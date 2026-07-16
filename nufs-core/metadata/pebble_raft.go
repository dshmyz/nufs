package metadata

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/klauspost/compress/zstd"
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
	OpCAS    RaftLogOp = 0x04 // Compare-and-swap inode update
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
	case OpCAS:
		// Key = Pebble key; Value = [8-byte expected version][new data]
		writeBytes(&buf, e.Key)
		writeBytes(&buf, e.Value)
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
	case OpCAS:
		var err error
		e.Key, err = readBytes(r)
		if err != nil {
			return nil, err
		}
		e.Value, err = readBytes(r)
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
//
// Write sync policy: Raft log entries are already durably written to the Raft WAL
// before they are committed and applied to the FSM. Therefore, the FSM does NOT
// call pebble.Sync on every write — this avoids fsync amplification. Instead,
// a background goroutine calls pebble.LogData (WAL sync) periodically, providing
// eventual durability without sacrificing throughput.
type PebbleFSM struct {
	store        *PebbleStore
	syncStopCh   chan struct{}
	syncInterval time.Duration
	syncMu       sync.Mutex
	syncWG       sync.WaitGroup
}

// Apply applies a committed Raft log entry to the Pebble store.
func (f *PebbleFSM) Apply(l *raft.Log) interface{} {
	if len(l.Data) == 0 {
		return nil
	}

	entry, err := DecodeRaftLogEntry(l.Data)
	if err != nil {
		slog.Error("fsm: decode error", "index", l.Index, "error", err)
		return err
	}

	switch entry.Op {
	case OpSet:
		err = f.store.db.Set(entry.Key, entry.Value, pebble.NoSync)

	case OpDelete:
		err = f.store.db.Delete(entry.Key, pebble.NoSync)

	case OpBatch:
		batch := f.store.db.NewBatch()
		for _, op := range entry.Batch {
			if op.Delete {
				batch.Delete(op.Key, nil)
			} else {
				batch.Set(op.Key, op.Value, nil)
			}
		}
		err = batch.Commit(pebble.NoSync)
		batch.Close()

	case OpCAS:
		// CAS: compare-and-swap inode update.
		// entry.Value = [8-byte expected version][new inode data]
		if len(entry.Value) < 8 {
			err = fmt.Errorf("fsm: opcas: value too short (%d bytes)", len(entry.Value))
			break
		}
		expectedVer := MVCCVersion(binary.BigEndian.Uint64(entry.Value[:8]))
		newData := entry.Value[8:]

		// Read current value
		currentVal, closer, getErr := f.store.db.Get(entry.Key)
		if getErr != nil {
			err = getErr
			break
		}
		currentCopy := make([]byte, len(currentVal))
		copy(currentCopy, currentVal)
		closer.Close()

		var current InodeWithVersion
		if err = unmarshalValue(currentCopy, &current); err != nil {
			break
		}

		if current.Version != expectedVer {
			err = ErrVersionConflict
			break
		}

		// Apply update
		err = f.store.db.Set(entry.Key, newData, pebble.NoSync)
	}

	if err != nil {
		slog.Error("fsm: apply error", "index", l.Index, "error", err)
	}
	return err
}

// StartSync starts a background goroutine that periodically syncs the Pebble
// WAL. Since FSM.Apply uses pebble.NoSync (durability is provided by Raft's own
// WAL), this ensures that data eventually reaches disk in case of a crash.
func (f *PebbleFSM) StartSync(interval time.Duration) {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	f.syncMu.Lock()
	defer f.syncMu.Unlock()
	if f.syncStopCh != nil {
		return
	}
	f.syncInterval = interval
	f.syncStopCh = make(chan struct{})
	stopCh := f.syncStopCh
	f.syncWG.Add(1)
	go func() {
		defer f.syncWG.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := f.logDataSync(); err != nil {
					slog.Error("fsm: WAL sync error", "error", err)
				}
			case <-stopCh:
				// Final sync before exit
				if err := f.logDataSync(); err != nil {
					slog.Error("fsm: final WAL sync error", "error", err)
				}
				return
			}
		}
	}()
	slog.Info("fsm: periodic WAL sync started", "interval", interval)
}

func (f *PebbleFSM) logDataSync() (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("pebble WAL sync panic: %v", r)
		}
	}()
	return f.store.db.LogData(nil, pebble.Sync)
}

// StopSync stops the background WAL sync goroutine.
func (f *PebbleFSM) StopSync() {
	f.syncMu.Lock()
	if f.syncStopCh != nil {
		close(f.syncStopCh)
		f.syncStopCh = nil
	}
	f.syncMu.Unlock()
	f.syncWG.Wait()
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

// Snapshot returns an FSM snapshot.
func (f *PebbleFSM) Snapshot() (raft.FSMSnapshot, error) {
	return &PebbleSnapshot{store: f.store}, nil
}

// Restore restores the FSM from a snapshot stream.
// Supports both PBL3 (checkpoint) and PBL1 (legacy KV stream) formats.
func (f *PebbleFSM) Restore(rc io.ReadCloser) error {
	return checkpointRestore(f, rc)
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

func writeBytesStream(w io.Writer, data []byte) error {
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(data)))
	if _, err := w.Write(lenBuf[:]); err != nil {
		return err
	}
	_, err := w.Write(data)
	return err
}

// PebbleSnapshot implements raft.FSMSnapshot.
type PebbleSnapshot struct {
	store *PebbleStore
}

// Persist creates a checkpoint snapshot (PBL3) or falls back to
// KV-stream (PBL1) for in-memory stores.
func (s *PebbleSnapshot) Persist(sink raft.SnapshotSink) error {
	if s.store.cfg.UseInMemory {
		return persistKVStream(s.store, sink)
	}

	// Flush memtable to SST before checkpointing so that NoSync-applied
	// entries are durable in the checkpoint, not just the WAL.
	if err := s.store.db.Flush(); err != nil {
		sink.Cancel()
		return fmt.Errorf("pebble flush before checkpoint: %w", err)
	}

	cpDir := filepath.Join(filepath.Dir(s.store.cfg.Dir), fmt.Sprintf("raft-checkpoint-%d", time.Now().UnixNano()))
	defer os.RemoveAll(cpDir)

	if err := s.store.db.Checkpoint(cpDir); err != nil {
		sink.Cancel()
		return fmt.Errorf("pebble checkpoint: %w", err)
	}

	if err := checkpointWriteDir(cpDir, sink); err != nil {
		sink.Cancel()
		return fmt.Errorf("checkpoint persist: %w", err)
	}

	return sink.Close()
}

// persistKVStream writes the legacy PBL1 KV-stream format.
func persistKVStream(store *PebbleStore, sink raft.SnapshotSink) error {
	if _, err := sink.Write([]byte("PBL1")); err != nil {
		sink.Cancel()
		return fmt.Errorf("write magic: %w", err)
	}

	zw, err := zstd.NewWriter(sink)
	if err != nil {
		sink.Cancel()
		return fmt.Errorf("zstd new: %w", err)
	}

	iter, err := store.db.NewIter(nil)
	if err != nil {
		zw.Close()
		sink.Cancel()
		return fmt.Errorf("pebble iter: %w", err)
	}

	var count uint64
	var iterErr error
	for iter.First(); iter.Valid(); iter.Next() {
		val, err := iter.ValueAndErr()
		if err != nil {
			iterErr = fmt.Errorf("value at %q: %w", iter.Key(), err)
			break
		}
		if err := writeBytesStream(zw, iter.Key()); err != nil {
			iterErr = fmt.Errorf("write key: %w", err)
			break
		}
		if err := writeBytesStream(zw, val); err != nil {
			iterErr = fmt.Errorf("write val: %w", err)
			break
		}
		count++
	}
	iter.Close()

	if iterErr != nil {
		zw.Close()
		sink.Cancel()
		return iterErr
	}

	var termBuf [4]byte
	for i := range termBuf {
		termBuf[i] = 0xFF
	}
	if _, err := zw.Write(termBuf[:]); err != nil {
		zw.Close()
		sink.Cancel()
		return fmt.Errorf("write term: %w", err)
	}
	var countBuf [8]byte
	binary.BigEndian.PutUint64(countBuf[:], count)
	if _, err := zw.Write(countBuf[:]); err != nil {
		zw.Close()
		sink.Cancel()
		return fmt.Errorf("write count: %w", err)
	}

	if err := zw.Close(); err != nil {
		sink.Cancel()
		return fmt.Errorf("zstd close: %w", err)
	}
	return sink.Close()
}

// Release cleans up snapshot resources.
func (s *PebbleSnapshot) Release() {}

// metaNodeOpsPrefix is the key prefix for storing each metad node's
// ops HTTP URL in the FSM, keyed by Raft server ID. Used by followers
// to look up the leader's ops address for auto-forwarding.
const metaNodeOpsPrefix = "/_raft/nodes/"

func metaNodeOpsKey(serverID string) string {
	return metaNodeOpsPrefix + serverID
}

// ============================================================
// RaftNode — Full Raft lifecycle management
// ============================================================

// RaftNode wraps a hashicorp/raft instance backed by Pebble as the FSM.
type RaftNode struct {
	raft         *raft.Raft
	fsm          *PebbleFSM
	raftDir      string
	nodeID       string
	bindAddr     string
	advertiseOps string            // Our own ops HTTP address
	peerOps      map[string]string // Raft node ID → ops HTTP address

	// BatchApply coalesces multiple write entries into a single Raft Apply,
	// reducing consensus overhead for metadata-intensive workloads.
	batch *raftBatchWriter
}

// RaftNodeConfig configures a RaftNode.
type RaftNodeConfig struct {
	NodeID   string
	BindAddr string // e.g. "0.0.0.0:7000"
	// AdvertiseAddr is the address peers use to reach this node. If empty,
	// BindAddr is used for both bind and advertise.
	AdvertiseAddr string
	RaftDir       string
	Bootstrap     bool
	JoinAddr      string
	Peers         []string
	// BootstrapPeers defines the initial Raft voter set as explicit
	// server ID/address pairs. Prefer this over Peers for production
	// deployments where server IDs differ from network addresses.
	BootstrapPeers []RaftPeer

	// AdvertiseOpsAddr is the HTTP ops URL (e.g. "http://10.0.0.1:8091")
	// that other metad nodes should use to reach this node's ops API.
	// Used by followers to redirect mutating write requests to the leader.
	AdvertiseOpsAddr string

	// PeerOpsURLs maps Raft node ID → ops HTTP URL for auto-forwarding.
	// Populated from config when each node's ops address is known.
	PeerOpsURLs map[string]string

	// Raft timing (zero value = use hashicorp/raft defaults: 1s, 1s, 500ms)
	HeartbeatTimeout   time.Duration
	ElectionTimeout    time.Duration
	LeaderLeaseTimeout time.Duration

	// Snapshot policy (zero value = sensible defaults)
	SnapshotThreshold uint64        // Logs before triggering snapshot (default: 8192)
	SnapshotInterval  time.Duration // Min time between snapshots (default: 2min)
	TrailingLogs      uint64        // Logs to retain after snapshot (default: 10240)

	// PreVote enables the Pre-Vote protocol extension (default: true).
	// When enabled, a partitioned node will not disrupt the current leader
	// by starting an election unless it can communicate with a majority.
	// This prevents election storms in network partitions.
	// Use a pointer so we can distinguish "not set" (nil → default true)
	// from "explicitly disabled" (false).
	PreVote *bool

	// ElectionPriority is a hint for leader election preference (0-255).
	// Higher values indicate a more preferred leader. This is used as a
	// tiebreaker: higher-priority nodes use shorter election timeouts,
	// making them more likely to win elections. Set to 0 (default) for no preference.
	ElectionPriority uint8
}

// RaftPeer is an explicit server ID/address pair for Raft membership.
type RaftPeer struct {
	ID      string
	Address string
}

// NewRaftNode creates and starts a new Raft node.
func NewRaftNode(store *PebbleStore, cfg RaftNodeConfig) (*RaftNode, error) {
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(cfg.NodeID)
	raftConfig.LogOutput = io.Discard

	// Apply Raft timing configuration
	if cfg.HeartbeatTimeout > 0 {
		raftConfig.HeartbeatTimeout = cfg.HeartbeatTimeout
	}
	if cfg.ElectionTimeout > 0 {
		raftConfig.ElectionTimeout = cfg.ElectionTimeout
	}
	if cfg.LeaderLeaseTimeout > 0 {
		raftConfig.LeaderLeaseTimeout = cfg.LeaderLeaseTimeout
	}

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

	// PreVote: enabled by default to prevent election storms in partitions.
	// A partitioned node will not disrupt the leader unless it can communicate
	// with a majority, avoiding unnecessary leader changes.
	// In hashicorp/raft v1.x, PreVote is enabled via ProtocolVersion >= 3.
	enablePreVote := true
	if cfg.PreVote != nil {
		enablePreVote = *cfg.PreVote
	}
	if enablePreVote {
		raftConfig.ProtocolVersion = 3
	}

	// Election priority: HashiCorp Raft does not natively support priority-based
	// leader election. We implement a practical workaround: higher-priority nodes
	// use shorter ElectionTimeout, making them more likely to initiate elections
	// first when the leader fails. This is a soft preference, not a guarantee.
	if cfg.ElectionPriority > 0 && cfg.ElectionTimeout == 0 {
		// Scale election timeout inversely with priority (0-255).
		// Priority 255 → ~50% of default timeout, Priority 1 → ~95% of default.
		factor := 0.5 + 0.5*float64(255-cfg.ElectionPriority)/255.0
		raftConfig.ElectionTimeout = time.Duration(float64(raftConfig.ElectionTimeout) * factor)
	}
	_ = cfg.ElectionPriority

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
	advertiseAddr := cfg.AdvertiseAddr
	if advertiseAddr == "" {
		advertiseAddr = cfg.BindAddr
	}
	addr, err := net.ResolveTCPAddr("tcp", advertiseAddr)
	if err != nil {
		return nil, fmt.Errorf("resolve addr %q: %w", advertiseAddr, err)
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

	// Start periodic WAL sync for FSM durability.
	// Raft logs are already durably written; this provides eventual
	// durability for the FSM's own WAL to survive crash without replaying
	// all Raft logs from last snapshot.
	fsm.StartSync(5 * time.Second)

	// Bootstrap
	if cfg.Bootstrap {
		servers := []raft.Server{
			{ID: raft.ServerID(cfg.NodeID), Address: raft.ServerAddress(advertiseAddr)},
		}
		for _, peer := range cfg.BootstrapPeers {
			if peer.ID == cfg.NodeID {
				continue
			}
			servers = append(servers, raft.Server{
				ID:      raft.ServerID(peer.ID),
				Address: raft.ServerAddress(peer.Address),
			})
		}
		for _, peer := range cfg.Peers {
			servers = append(servers, raft.Server{
				ID:      raft.ServerID(peer),
				Address: raft.ServerAddress(peer),
			})
		}
		f := r.BootstrapCluster(raft.Configuration{Servers: servers})
		if err := f.Error(); err != nil && err != raft.ErrCantBootstrap {
			slog.Warn("raft bootstrap warning", "error", err)
		}
	}

	peerOps := cfg.PeerOpsURLs
	if peerOps == nil {
		peerOps = make(map[string]string)
	}
	node := &RaftNode{
		raft: r, fsm: fsm, raftDir: cfg.RaftDir,
		nodeID: cfg.NodeID, bindAddr: cfg.BindAddr,
		advertiseOps: cfg.AdvertiseOpsAddr,
		peerOps:      peerOps,
	}
	store.SetRaftNode(node)
	return node, nil
}

// IsLeader returns true if this node is the current leader.
func (n *RaftNode) IsLeader() bool {
	return n.raft.State() == raft.Leader
}

// NodeID returns the Raft node ID of this node.
func (n *RaftNode) NodeID() string {
	return n.nodeID
}

// LeaderAddr returns the address of the current leader.
func (n *RaftNode) LeaderAddr() string {
	addr, _ := n.raft.LeaderWithID()
	return string(addr)
}

// LeaderOpsAddr returns the HTTP ops URL of the current leader,
// or empty string if this is a single-node deployment (Raft disabled).
//
// Resolution order:
//  1. If we are the leader, use our own ops address
//  2. Look up in the peerOps map (populated from --advertise-ops-addr flags)
//  3. FSM lookup: each metad node stores its ops URL under /_raft/nodes/<serverID>
//
// Returns empty string if the leader's ops address cannot be determined.
// Callers should handle empty strings as "leader unknown" rather than
// falling back to the Raft bind address, which may not be reachable
// on the ops HTTP port.
func (n *RaftNode) LeaderOpsAddr() string {
	addr, id := n.raft.LeaderWithID()
	if addr == "" {
		return ""
	}
	// If we are the leader, use our own ops address
	if n.IsLeader() {
		return n.advertiseOps
	}
	// Look up the leader's ops address from the peer map
	if ops, ok := n.peerOps[string(id)]; ok {
		return ops
	}
	// FSM lookup: each metad node stores its ops URL in the FSM on startup
	if n.fsm.store.db != nil {
		key := []byte(metaNodeOpsKey(string(id)))
		val, closer, err := n.fsm.store.db.Get(key)
		if err == nil {
			defer closer.Close()
			data := make([]byte, len(val))
			copy(data, val)
			return string(data)
		}
	}
	// Leader's ops address is not yet registered. Return empty to signal
	// "unknown" rather than guessing from the Raft bind address, which
	// is the internal Raft port and may not be reachable on the ops HTTP port.
	return ""
}

// StoreOpsURL submits this node's ops HTTP URL to the FSM via Raft,
// making it discoverable by other nodes for auto-forwarding.
// Uses ApplyAutoForward so it works on both leaders and followers.
func (n *RaftNode) StoreOpsURL(timeout time.Duration) error {
	if n.advertiseOps == "" {
		return nil
	}
	entry := &RaftLogEntry{
		Op:    OpSet,
		Key:   []byte(metaNodeOpsKey(n.nodeID)),
		Value: []byte(n.advertiseOps),
	}
	return n.ApplyAutoForward(entry, timeout)
}

// Apply sends a write through Raft consensus.
// Returns an error if this node is not the leader.
func (n *RaftNode) Apply(entry *RaftLogEntry, timeout time.Duration) error {
	if !n.IsLeader() {
		return fmt.Errorf("not leader (leader at %s)", n.LeaderAddr())
	}
	f := n.raft.Apply(entry.Encode(), timeout)
	return f.Error()
}

// ApplyAutoForward sends a write through Raft consensus, automatically
// forwarding to the leader if this node is a follower. This eliminates
// the need for clients to implement redirect/retry logic.
//
// If the leader's ops HTTP address is known, the entry is forwarded via
// an HTTP POST to the leader's /api/v1/raft/apply endpoint. Otherwise,
// it falls back to the standard Apply error ("not leader").
func (n *RaftNode) ApplyAutoForward(entry *RaftLogEntry, timeout time.Duration) error {
	// Fast path: we are the leader
	if n.IsLeader() {
		f := n.raft.Apply(entry.Encode(), timeout)
		return f.Error()
	}

	// Forward to leader via HTTP
	leaderOps := n.LeaderOpsAddr()
	if leaderOps == "" {
		return fmt.Errorf("not leader and leader address unknown")
	}

	return n.forwardToLeader(entry, leaderOps, timeout)
}

// ReadIndex performs a consistent read on a follower by verifying the
// node's log is caught up to the leader's commit index. This allows
// followers to serve read requests without forwarding to the leader,
// reducing leader load and cross-AZ latency.
//
// Returns an error if this node has been removed from the cluster or
// the verification times out.
func (n *RaftNode) ReadIndex(timeout time.Duration) error {
	future := n.raft.VerifyLeader()
	errCh := make(chan error, 1)
	go func() {
		errCh <- future.Error()
	}()
	select {
	case err := <-errCh:
		return err
	case <-time.After(timeout):
		return fmt.Errorf("read index: verify leader timeout after %v", timeout)
	}
}

// FollowerRead executes a read function with follower read consistency.
// It first verifies that this node's log is consistent with the leader,
// then executes the read function locally. If this node is the leader,
// the read proceeds immediately without verification.
func (n *RaftNode) FollowerRead(timeout time.Duration, fn func() error) error {
	if n.IsLeader() {
		return fn()
	}
	if err := n.ReadIndex(timeout); err != nil {
		return fmt.Errorf("follower read: %w", err)
	}
	return fn()
}

// forwardToLeader sends a Raft log entry to the leader's ops HTTP endpoint.
// When the leader returns a structured error (e.g. ErrVersionConflict),
// the response includes an X-Raft-Error header so the follower can
// reconstruct the correct sentinel error for errors.Is matching.
func (n *RaftNode) forwardToLeader(entry *RaftLogEntry, leaderOps string, timeout time.Duration) error {
	encoded := entry.Encode()

	// Use the internal raft apply endpoint on the leader node
	url := leaderOps + "/api/v1/raft/apply"

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(encoded))
	if err != nil {
		return fmt.Errorf("forward: create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("X-Forwarded-By", n.nodeID)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("forward to %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		// Check for structured error code in header — allows errors.Is to work
		// across HTTP boundaries by mapping known error codes to sentinel errors.
		if errCode := resp.Header.Get("X-Raft-Error"); errCode != "" {
			if mapped := mapRaftError(errCode); mapped != nil {
				return mapped
			}
		}
		return fmt.Errorf("forward to %s: status %d: %s", url, resp.StatusCode, string(body))
	}
	return nil
}

// mapRaftError maps a structured error code from the X-Raft-Error header
// to the corresponding sentinel error, enabling errors.Is matching across
// HTTP forwarding boundaries.
func mapRaftError(code string) error {
	switch code {
	case "ErrVersionConflict":
		return ErrVersionConflict
	case "ErrInodeNotFound":
		return ErrInodeNotFound
	case "ErrEntryExists":
		return ErrEntryExists
	case "ErrBucketNotFound":
		return ErrBucketNotFound
	case "ErrChunkNotFound":
		return ErrChunkNotFound
	case "ErrNodeNotFound":
		return ErrNodeNotFound
	default:
		return nil
	}
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
	if n.fsm != nil {
		n.fsm.StopSync()
	}
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

// ============================================================
// Raft Batch Writer — coalesces multiple writes into one Apply
// ============================================================

// raftBatchWriter collects write entries and flushes them as a single
// Raft Apply (OpBatch). This reduces consensus round-trips for
// metadata-intensive workloads like bulk file creation.
type raftBatchWriter struct {
	node    *RaftNode
	mu      sync.Mutex
	pending []BatchOp
	waiters []chan error
	timer   *time.Timer
	maxOps  int           // Flush when this many ops are collected
	maxWait time.Duration // Flush after this duration regardless
}

// newRaftBatchWriter creates a batch writer attached to the given RaftNode.
func newRaftBatchWriter(node *RaftNode, maxOps int, maxWait time.Duration) *raftBatchWriter {
	if maxOps <= 0 {
		maxOps = 64
	}
	if maxWait <= 0 {
		maxWait = 5 * time.Millisecond
	}
	return &raftBatchWriter{
		node:    node,
		maxOps:  maxOps,
		maxWait: maxWait,
	}
}

// ApplyBatched adds a write operation to the current batch. When the batch
// is full (maxOps) or the timer expires (maxWait), all pending ops are
// submitted as a single Raft Apply. The caller blocks until the batch
// containing their op is committed.
func (bw *raftBatchWriter) ApplyBatched(op BatchOp, timeout time.Duration) error {
	bw.mu.Lock()

	bw.pending = append(bw.pending, op)
	ch := make(chan error, 1)
	bw.waiters = append(bw.waiters, ch)

	// Start timer on first op in batch
	if len(bw.pending) == 1 {
		bw.timer = time.AfterFunc(bw.maxWait, func() { bw.flush() })
	}

	// Flush immediately if batch is full
	if len(bw.pending) >= bw.maxOps {
		if bw.timer != nil {
			bw.timer.Stop()
		}
		bw.mu.Unlock()
		bw.flush()
	} else {
		bw.mu.Unlock()
	}

	// Wait for result
	select {
	case err := <-ch:
		return err
	case <-time.After(timeout):
		return fmt.Errorf("batch apply timeout after %v", timeout)
	}
}

// flush sends all pending ops as a single Raft Apply and notifies waiters.
func (bw *raftBatchWriter) flush() {
	bw.mu.Lock()
	ops := bw.pending
	waiters := bw.waiters
	bw.pending = nil
	bw.waiters = nil
	bw.mu.Unlock()

	if len(ops) == 0 {
		return
	}

	entry := &RaftLogEntry{Op: OpBatch, Batch: ops}
	err := bw.node.Apply(entry, 10*time.Second)

	for _, ch := range waiters {
		ch <- err
	}
}

// StartBatchedApply enables batched write mode on the RaftNode.
// After calling this, use ApplyBatched for write operations to benefit
// from automatic coalescing.
func (n *RaftNode) StartBatchedApply(maxOps int, maxWait time.Duration) {
	n.batch = newRaftBatchWriter(n, maxOps, maxWait)
}

// ApplyBatched submits a single write operation through the batch writer.
// If batch mode is not enabled, falls back to a regular Apply.
func (n *RaftNode) ApplyBatched(op BatchOp, timeout time.Duration) error {
	if n.batch == nil {
		opType := OpSet
		if op.Delete {
			opType = OpDelete
		}
		return n.Apply(&RaftLogEntry{Op: opType, Key: op.Key, Value: op.Value}, timeout)
	}
	return n.batch.ApplyBatched(op, timeout)
}
