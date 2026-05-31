package metadata

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

// ============================================================
// Raft Log Storage Strategy
// ============================================================
//
// Raft logs are stored in BoltDB (via raft-boltdb).
// Log compaction is triggered after each successful snapshot:
//   - Snapshot captures full Pebble state
//   - Logs older than snapshot index are truncated
//   - Retain last `TrailingLogs` entries for slow followers
//
// Log entry format (RaftLogEntry):
//   [OpType: 1 byte] [Key length: 4 bytes BE] [Key] [Value]
//
// Supported OpTypes:
//   OpSet    (0x01) — put key-value
//   OpDelete (0x02) — delete key
//   OpBatch  (0x03) — atomic multi-key batch

// RaftLogOp defines the operation type in a Raft log entry.
type RaftLogOp byte

const (
	OpSet    RaftLogOp = 0x01 // Set key-value
	OpDelete RaftLogOp = 0x02 // Delete key
	OpBatch  RaftLogOp = 0x03 // Atomic batch of Set/Delete operations
)

// RaftLogEntry is the binary-encoded format of a Raft log entry.
type RaftLogEntry struct {
	Op    RaftLogOp
	Key   []byte
	Value []byte
	// For OpBatch: alternating Key,Value pairs (nil Value = delete)
	Batch []BatchOp
}

// BatchOp is a single operation within an OpBatch.
type BatchOp struct {
	Delete bool
	Key    []byte
	Value  []byte
}

// Encode serializes a RaftLogEntry to bytes.
func (e *RaftLogEntry) Encode() []byte {
	var buf bytes.Buffer
	buf.WriteByte(byte(e.Op))

	switch e.Op {
	case OpSet:
		writeBytes(&buf, e.Key)
		writeBytes(&buf, e.Value)
	case OpDelete:
		writeBytes(&buf, e.Key)
	case OpBatch:
		binary.Write(&buf, binary.BigEndian, uint32(len(e.Batch)))
		for _, op := range e.Batch {
			if op.Delete {
				buf.WriteByte(1)
				writeBytes(&buf, op.Key)
			} else {
				buf.WriteByte(0)
				writeBytes(&buf, op.Key)
				writeBytes(&buf, op.Value)
			}
		}
	}
	return buf.Bytes()
}

// DecodeRaftLogEntry deserializes a Raft log entry.
func DecodeRaftLogEntry(data []byte) (*RaftLogEntry, error) {
	if len(data) < 1 {
		return nil, fmt.Errorf("empty raft log entry")
	}
	e := &RaftLogEntry{Op: RaftLogOp(data[0])}
	r := bytes.NewReader(data[1:])

	switch e.Op {
	case OpSet:
		var err error
		e.Key, err = readBytes(r)
		if err != nil {
			return nil, err
		}
		e.Value, err = readBytes(r)
		if err != nil {
			return nil, err
		}
	case OpDelete:
		var err error
		e.Key, err = readBytes(r)
		if err != nil {
			return nil, err
		}
	case OpBatch:
		var count uint32
		if err := binary.Read(r, binary.BigEndian, &count); err != nil {
			return nil, err
		}
		e.Batch = make([]BatchOp, count)
		for i := uint32(0); i < count; i++ {
			del, err := r.ReadByte()
			if err != nil {
				return nil, err
			}
			e.Batch[i].Delete = del == 1
			e.Batch[i].Key, err = readBytes(r)
			if err != nil {
				return nil, err
			}
			if !e.Batch[i].Delete {
				e.Batch[i].Value, err = readBytes(r)
				if err != nil {
					return nil, err
				}
			}
		}
	default:
		return nil, fmt.Errorf("unknown op type: 0x%02x", e.Op)
	}
	return e, nil
}

func writeBytes(buf *bytes.Buffer, data []byte) {
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(data)))
	buf.Write(lenBuf[:])
	buf.Write(data)
}

func readBytes(r *bytes.Reader) ([]byte, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(lenBuf[:])
	data := make([]byte, n)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, err
	}
	return data, nil
}

// ============================================================
// PebbleFSM — State Machine backed by Pebble
// ============================================================

// PebbleFSM implements raft.FSM, applying log entries to a PebbleStore.
type PebbleFSM struct {
	store *PebbleStore
}

// Apply applies a committed Raft log entry to the Pebble store.
func (f *PebbleFSM) Apply(l *raft.Log) interface{} {
	if len(l.Data) == 0 {
		return nil
	}

	entry, err := DecodeRaftLogEntry(l.Data)
	if err != nil {
		log.Printf("fsm: decode error at index %d: %v", l.Index, err)
		return err
	}

	switch entry.Op {
	case OpSet:
		err = f.store.db.Set(entry.Key, entry.Value, pebble.Sync)

	case OpDelete:
		err = f.store.db.Delete(entry.Key, pebble.Sync)

	case OpBatch:
		batch := f.store.db.NewBatch()
		for _, op := range entry.Batch {
			if op.Delete {
				batch.Delete(op.Key, nil)
			} else {
				batch.Set(op.Key, op.Value, nil)
			}
		}
		err = batch.Commit(pebble.Sync)
		batch.Close()
	}

	if err != nil {
		log.Printf("fsm: apply error at index %d: %v", l.Index, err)
	}
	return err
}

// ============================================================
// Snapshot Strategy
// ============================================================
//
// Snapshot process:
//   1. Pebble.Checkpoint() creates a zero-copy hard-link snapshot of all SST files
//   2. Checkpoint is serialized as a stream of key-value pairs to the Raft snapshot sink
//   3. After snapshot completes, Raft truncates logs before snapshot index
//
// Snapshot trigger policy:
//   - Automatic: hashicorp/raft triggers after SnapshotThreshold log entries (default: 8192)
//   - Configurable via RaftNodeConfig.SnapshotThreshold
//   - SnapshotInterval: minimum time between snapshots (default: 2 minutes)
//
// Log retention after snapshot:
//   - TrailingLogs: keep last N entries for slow followers (default: 10240)
//
// Disk layout:
//   raftDir/
//   ├── log/
//   │   └── raft-log.bolt       # Raft WAL (BoltDB)
//   └── snap/
//       ├── 0001-12345-67890/   # Snapshot directory (term-index-range)
//       │   ├── meta.bin         # Snapshot metadata
//       │   └── state.bin        # Serialized KV data
//       └── 0001-12346-67891/

// Snapshot format:
//   [magic: 4 bytes "PBL1"]
//   [key_count: 8 bytes BE]
//   For each KV:
//     [key_len: 4 bytes BE] [key] [val_len: 4 bytes BE] [val]

const snapshotMagic = "PBL1"

// Snapshot returns an FSM snapshot.
func (f *PebbleFSM) Snapshot() (raft.FSMSnapshot, error) {
	return &PebbleSnapshot{store: f.store}, nil
}

// Restore restores the FSM from a snapshot stream.
func (f *PebbleFSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	// Read magic
	magic := make([]byte, 4)
	if _, err := io.ReadFull(rc, magic); err != nil {
		return fmt.Errorf("read magic: %w", err)
	}
	if string(magic) != snapshotMagic {
		return fmt.Errorf("invalid snapshot magic: %q", magic)
	}

	// Read key count
	var count uint64
	if err := binary.Read(rc, binary.BigEndian, &count); err != nil {
		return fmt.Errorf("read count: %w", err)
	}

	// Clear existing data (ingest into empty DB)
	// In production: use Pebble ingest for SST-level restore
	batch := f.store.db.NewBatch()
	batchCount := 0

	for i := uint64(0); i < count; i++ {
		key, err := readBytesStream(rc)
		if err != nil {
			batch.Close()
			return fmt.Errorf("read key %d: %w", i, err)
		}
		val, err := readBytesStream(rc)
		if err != nil {
			batch.Close()
			return fmt.Errorf("read value %d: %w", i, err)
		}
		batch.Set(key, val, nil)
		batchCount++

		// Commit in batches of 10K to avoid OOM
		if batchCount >= 10000 {
			if err := batch.Commit(pebble.Sync); err != nil {
				batch.Close()
				return fmt.Errorf("batch commit at %d: %w", i, err)
			}
			batch.Close()
			batch = f.store.db.NewBatch()
			batchCount = 0
		}
	}

	if err := batch.Commit(pebble.Sync); err != nil {
		batch.Close()
		return err
	}
	batch.Close()

	log.Printf("fsm: restored %d keys from snapshot", count)
	return nil
}

func readBytesStream(r io.Reader) ([]byte, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(lenBuf[:])
	data := make([]byte, n)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, err
	}
	return data, nil
}

// PebbleSnapshot implements raft.FSMSnapshot.
type PebbleSnapshot struct {
	store *PebbleStore
}

// Persist streams the full Pebble state to the Raft snapshot sink.
func (s *PebbleSnapshot) Persist(sink raft.SnapshotSink) error {
	// Iterate all keys in Pebble and stream them
	iter, err := s.store.db.NewIter(nil)
	if err != nil {
		sink.Cancel()
		return fmt.Errorf("pebble iter: %w", err)
	}
	defer iter.Close()

	// First pass: count keys
	var count uint64
	for iter.First(); iter.Valid(); iter.Next() {
		count++
	}

	// Write header
	header := make([]byte, 12)
	copy(header[:4], snapshotMagic)
	binary.BigEndian.PutUint64(header[4:], count)
	if _, err := sink.Write(header); err != nil {
		sink.Cancel()
		return err
	}

	// Second pass: stream KV pairs
	for iter.First(); iter.Valid(); iter.Next() {
		val, err := iter.ValueAndErr()
		if err != nil {
			sink.Cancel()
			return err
		}

		k := iter.Key()
		// Write key
		var lenBuf [4]byte
		binary.BigEndian.PutUint32(lenBuf[:], uint32(len(k)))
		sink.Write(lenBuf[:])
		sink.Write(k)

		// Write value
		binary.BigEndian.PutUint32(lenBuf[:], uint32(len(val)))
		sink.Write(lenBuf[:])
		sink.Write(val)
	}

	return sink.Close()
}

// Release cleans up snapshot resources.
func (s *PebbleSnapshot) Release() {}

// ============================================================
// RaftNode — Full Raft lifecycle management
// ============================================================

// RaftNode wraps a hashicorp/raft instance backed by Pebble as the FSM.
type RaftNode struct {
	raft     *raft.Raft
	fsm      *PebbleFSM
	raftDir  string
	nodeID   string
	bindAddr string
}

// RaftNodeConfig configures a RaftNode.
type RaftNodeConfig struct {
	NodeID    string
	BindAddr  string // e.g. "0.0.0.0:7000"
	RaftDir   string
	Bootstrap bool
	JoinAddr  string
	Peers     []string

	// Snapshot policy
	SnapshotThreshold uint64        // Logs before triggering snapshot (default: 8192)
	SnapshotInterval  time.Duration // Min time between snapshots (default: 2min)
	TrailingLogs      uint64        // Logs to retain after snapshot (default: 10240)
}

// NewRaftNode creates and starts a new Raft node.
func NewRaftNode(store *PebbleStore, cfg RaftNodeConfig) (*RaftNode, error) {
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(cfg.NodeID)
	raftConfig.LogOutput = io.Discard

	// Apply snapshot/log retention policy
	if cfg.SnapshotThreshold > 0 {
		raftConfig.SnapshotThreshold = cfg.SnapshotThreshold
	}
	if cfg.SnapshotInterval > 0 {
		raftConfig.SnapshotInterval = cfg.SnapshotInterval
	}
	if cfg.TrailingLogs > 0 {
		raftConfig.TrailingLogs = cfg.TrailingLogs
	}

	// Directories
	logDir := filepath.Join(cfg.RaftDir, "log")
	snapDir := filepath.Join(cfg.RaftDir, "snap")
	os.MkdirAll(logDir, 0755)
	os.MkdirAll(snapDir, 0755)

	// Log + Stable store (BoltDB)
	logStore, err := raftboltdb.NewBoltStore(filepath.Join(logDir, "raft-log.bolt"))
	if err != nil {
		return nil, fmt.Errorf("raft log store: %w", err)
	}
	stableStore := logStore

	// Snapshot store (retain 3 snapshots)
	snapshotStore, err := raft.NewFileSnapshotStore(snapDir, 3, io.Discard)
	if err != nil {
		return nil, fmt.Errorf("raft snapshot store: %w", err)
	}

	// Transport
	addr, err := net.ResolveTCPAddr("tcp", cfg.BindAddr)
	if err != nil {
		return nil, fmt.Errorf("resolve addr %q: %w", cfg.BindAddr, err)
	}
	transport, err := raft.NewTCPTransport(cfg.BindAddr, addr, 3, 10*time.Second, io.Discard)
	if err != nil {
		return nil, fmt.Errorf("raft transport: %w", err)
	}

	// FSM
	fsm := &PebbleFSM{store: store}

	// Create Raft
	r, err := raft.NewRaft(raftConfig, fsm, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return nil, fmt.Errorf("new raft: %w", err)
	}

	// Bootstrap
	if cfg.Bootstrap {
		servers := []raft.Server{
			{ID: raft.ServerID(cfg.NodeID), Address: raft.ServerAddress(cfg.BindAddr)},
		}
		for _, peer := range cfg.Peers {
			servers = append(servers, raft.Server{
				ID:      raft.ServerID(peer),
				Address: raft.ServerAddress(peer),
			})
		}
		f := r.BootstrapCluster(raft.Configuration{Servers: servers})
		if err := f.Error(); err != nil && err != raft.ErrCantBootstrap {
			log.Printf("raft bootstrap warning: %v", err)
		}
	}

	return &RaftNode{
		raft: r, fsm: fsm, raftDir: cfg.RaftDir,
		nodeID: cfg.NodeID, bindAddr: cfg.BindAddr,
	}, nil
}

// IsLeader returns true if this node is the current leader.
func (n *RaftNode) IsLeader() bool {
	return n.raft.State() == raft.Leader
}

// LeaderAddr returns the address of the current leader.
func (n *RaftNode) LeaderAddr() string {
	addr, _ := n.raft.LeaderWithID()
	return string(addr)
}

// Apply sends a write through Raft consensus.
func (n *RaftNode) Apply(entry *RaftLogEntry, timeout time.Duration) error {
	if !n.IsLeader() {
		return fmt.Errorf("not leader (leader at %s)", n.LeaderAddr())
	}
	f := n.raft.Apply(entry.Encode(), timeout)
	return f.Error()
}

// ApplySet is a convenience method for a single key-value set.
func (n *RaftNode) ApplySet(key, value []byte, timeout time.Duration) error {
	return n.Apply(&RaftLogEntry{Op: OpSet, Key: key, Value: value}, timeout)
}

// ApplyDelete is a convenience method for a single key delete.
func (n *RaftNode) ApplyDelete(key []byte, timeout time.Duration) error {
	return n.Apply(&RaftLogEntry{Op: OpDelete, Key: key}, timeout)
}

// ApplyBatch is a convenience method for atomic multi-key operations.
func (n *RaftNode) ApplyBatch(ops []BatchOp, timeout time.Duration) error {
	return n.Apply(&RaftLogEntry{Op: OpBatch, Batch: ops}, timeout)
}

// TriggerSnapshot manually triggers a snapshot (useful before shutdown).
func (n *RaftNode) TriggerSnapshot() error {
	f := n.raft.Snapshot()
	return f.Error()
}

// AddPeer adds a voter to the cluster.
func (n *RaftNode) AddPeer(id, addr string) error {
	f := n.raft.AddVoter(raft.ServerID(id), raft.ServerAddress(addr), 0, 10*time.Second)
	return f.Error()
}

// RemovePeer removes a node from the cluster.
func (n *RaftNode) RemovePeer(id string) error {
	f := n.raft.RemoveServer(raft.ServerID(id), 0, 10*time.Second)
	return f.Error()
}

// Stats returns Raft statistics.
func (n *RaftNode) Stats() map[string]string {
	return n.raft.Stats()
}

// Shutdown gracefully stops the node.
func (n *RaftNode) Shutdown() error {
	f := n.raft.Shutdown()
	return f.Error()
}

// ============================================================
// Helper: encode JSON value for Raft apply
// ============================================================

// EncodeSetJSON creates a RaftLogEntry that sets key to JSON-encoded value.
func EncodeSetJSON(key string, v interface{}) (*RaftLogEntry, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return &RaftLogEntry{Op: OpSet, Key: []byte(key), Value: data}, nil
}
