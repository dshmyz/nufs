package metadata

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
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
	// OpConditionalBatch preserves all existing wire values and adds a
	// versioned compare-and-mutate operation for ordered FSM invariants.
	OpConditionalBatch RaftLogOp = 0x05
)

const (
	conditionalBatchVersion               = uint8(1)
	conditionalBatchTermFencedVersion     = uint8(2)
	chunkTombstoneConditionalBatchVersion = uint8(3)
	chunkTombstoneFencedBatchVersion      = uint8(4)
	maxConditionalPreconditions           = uint32(1024)
	maxConditionalMutations               = uint32(4096)
	maxConditionalPrefixReplacements      = uint32(64)
	maxConditionalReplacementSets         = uint32(4096)
	maxConditionalKeyBytes                = 4 << 10
	maxConditionalValueBytes              = 4 << 20
	maxRaftLogEntryBytes                  = 8 << 20
	// Conditional metadata operations are low-throughput control-plane writes.
	// This bounds canceled callers whose accepted Raft futures are unresolved.
	conditionalFutureWaiterCapacity = 16
)

// MaxChunkAllocationBatch is the largest allocation batch accepted by the
// service API and the v4 conditional allocation log shape.
const MaxChunkAllocationBatch = 1024

var (
	// ErrRaftConditionalConflict is returned when an FSM precondition does not
	// match the state produced by all earlier committed Raft logs.
	ErrRaftConditionalConflict = errors.New("raft conditional conflict")
	// ErrRaftConditionalOutcomeUnknown means the caller stopped waiting after
	// Raft accepted a proposal. The log may still commit and callers must
	// reconcile durable state before retrying.
	ErrRaftConditionalOutcomeUnknown = errors.New("raft conditional outcome unknown")
	conditionalBatchCommit           = func(batch *pebble.Batch, sync *pebble.WriteOptions) error { return batch.Commit(sync) }
)

// RaftLogEntry is the binary-encoded format of a Raft log entry.
type RaftLogEntry struct {
	Op    RaftLogOp
	Key   []byte
	Value []byte
	// For OpBatch: alternating Key,Value pairs (nil Value = delete)
	Batch []BatchOp
	// Conditional is populated only for OpConditionalBatch.
	Conditional *ConditionalBatch
}

// BatchOp is a single operation within an OpBatch.
type BatchOp struct {
	Delete bool
	Key    []byte
	Value  []byte
}

// ConditionalPrecondition compares one raw Pebble value, or requires absence.
type ConditionalPrecondition struct {
	Key           []byte
	ExpectedValue []byte
	ExpectAbsent  bool
}

// ConditionalPrefixReplacement deletes every existing key under Prefix and
// installs Sets in the same Pebble batch.
type ConditionalPrefixReplacement struct {
	Prefix []byte
	Sets   []BatchOp
}

// ConditionalBatch is the versioned payload of OpConditionalBatch.
type ConditionalBatch struct {
	Version uint8
	// ExpectedRaftTerm is encoded only by the term-fenced payload version.
	// The FSM compares it with the committed raft.Log.Term before any write.
	ExpectedRaftTerm   uint64
	Preconditions      []ConditionalPrecondition
	Mutations          []BatchOp
	PrefixReplacements []ConditionalPrefixReplacement
}

// Encode serializes a RaftLogEntry to bytes.
func (e *RaftLogEntry) Encode() []byte {
	data, _ := e.EncodeChecked()
	return data
}

// EncodeChecked serializes a RaftLogEntry and reports invalid conditional
// payloads instead of emitting an ambiguous log entry.
func (e *RaftLogEntry) EncodeChecked() ([]byte, error) {
	if e == nil {
		return nil, fmt.Errorf("nil raft log entry")
	}
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
	case OpConditionalBatch:
		conditional, err := canonicalConditionalBatch(e.Conditional)
		if err != nil {
			return nil, err
		}
		buf.WriteByte(conditional.Version)
		if conditional.Version == conditionalBatchTermFencedVersion {
			if err := binary.Write(&buf, binary.BigEndian, conditional.ExpectedRaftTerm); err != nil {
				return nil, err
			}
		}
		writeUint32(&buf, uint32(len(conditional.Preconditions)))
		for _, precondition := range conditional.Preconditions {
			if precondition.ExpectAbsent {
				buf.WriteByte(1)
			} else {
				buf.WriteByte(0)
			}
			writeBytes(&buf, precondition.Key)
			if !precondition.ExpectAbsent {
				writeBytes(&buf, precondition.ExpectedValue)
			}
		}
		writeUint32(&buf, uint32(len(conditional.Mutations)))
		for _, mutation := range conditional.Mutations {
			writeBatchOp(&buf, mutation)
		}
		writeUint32(&buf, uint32(len(conditional.PrefixReplacements)))
		for _, replacement := range conditional.PrefixReplacements {
			writeBytes(&buf, replacement.Prefix)
			writeUint32(&buf, uint32(len(replacement.Sets)))
			for _, set := range replacement.Sets {
				writeBytes(&buf, set.Key)
				writeBytes(&buf, set.Value)
			}
		}
	default:
		return nil, fmt.Errorf("unknown op type: 0x%02x", e.Op)
	}
	if e.Op == OpConditionalBatch && buf.Len() > maxRaftLogEntryBytes {
		return nil, fmt.Errorf("raft log entry exceeds %d bytes", maxRaftLogEntryBytes)
	}
	return buf.Bytes(), nil
}

// DecodeRaftLogEntry deserializes a Raft log entry.
func DecodeRaftLogEntry(data []byte) (*RaftLogEntry, error) {
	if len(data) < 1 {
		return nil, fmt.Errorf("empty raft log entry")
	}
	if RaftLogOp(data[0]) == OpConditionalBatch && len(data) > maxRaftLogEntryBytes {
		return nil, fmt.Errorf("raft log entry exceeds %d bytes", maxRaftLogEntryBytes)
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
		// Every legacy batch item needs at least a kind byte and a four-byte
		// key length, so this prevents allocation from an impossible count
		// without imposing a new wire-level limit on valid historical logs.
		if uint64(count) > uint64(r.Len()/5) {
			return nil, fmt.Errorf("batch operation count %d exceeds encoded payload", count)
		}
		e.Batch = make([]BatchOp, count)
		for i := uint32(0); i < count; i++ {
			del, err := r.ReadByte()
			if err != nil {
				return nil, err
			}
			if del > 1 {
				return nil, fmt.Errorf("invalid batch operation kind %d", del)
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
	case OpConditionalBatch:
		conditional, err := decodeConditionalBatch(r)
		if err != nil {
			return nil, err
		}
		e.Conditional = conditional
	default:
		return nil, fmt.Errorf("unknown op type: 0x%02x", e.Op)
	}
	if r.Len() != 0 {
		return nil, fmt.Errorf("raft log entry has %d trailing bytes", r.Len())
	}
	return e, nil
}

func canonicalConditionalBatch(input *ConditionalBatch) (*ConditionalBatch, error) {
	if input == nil {
		return nil, fmt.Errorf("conditional batch is required")
	}
	if input.Version != conditionalBatchVersion &&
		input.Version != conditionalBatchTermFencedVersion &&
		input.Version != chunkTombstoneConditionalBatchVersion &&
		input.Version != chunkTombstoneFencedBatchVersion {
		return nil, fmt.Errorf("unsupported conditional batch version %d", input.Version)
	}
	preconditionLimit := maxConditionalPreconditions
	mutationLimit := maxConditionalMutations
	if input.Version == chunkTombstoneFencedBatchVersion && hasInodeAllocationPrecondition(input.Preconditions) {
		preconditionLimit = 2 + 2*MaxChunkAllocationBatch
		mutationLimit = 2 + MaxChunkAllocationBatch
	}
	if len(input.Preconditions) > int(preconditionLimit) {
		return nil, fmt.Errorf("conditional precondition count exceeds %d", preconditionLimit)
	}
	if len(input.Mutations) > int(mutationLimit) {
		return nil, fmt.Errorf("conditional mutation count exceeds %d", mutationLimit)
	}
	if len(input.PrefixReplacements) > int(maxConditionalPrefixReplacements) {
		return nil, fmt.Errorf("conditional prefix replacement count exceeds %d", maxConditionalPrefixReplacements)
	}

	out := &ConditionalBatch{
		Version:            input.Version,
		ExpectedRaftTerm:   input.ExpectedRaftTerm,
		Preconditions:      append([]ConditionalPrecondition(nil), input.Preconditions...),
		Mutations:          append([]BatchOp(nil), input.Mutations...),
		PrefixReplacements: make([]ConditionalPrefixReplacement, len(input.PrefixReplacements)),
	}
	for i, replacement := range input.PrefixReplacements {
		out.PrefixReplacements[i] = ConditionalPrefixReplacement{
			Prefix: append([]byte(nil), replacement.Prefix...),
			Sets:   append([]BatchOp(nil), replacement.Sets...),
		}
	}
	sort.Slice(out.Preconditions, func(i, j int) bool {
		return bytes.Compare(out.Preconditions[i].Key, out.Preconditions[j].Key) < 0
	})
	sort.Slice(out.Mutations, func(i, j int) bool {
		return bytes.Compare(out.Mutations[i].Key, out.Mutations[j].Key) < 0
	})
	sort.Slice(out.PrefixReplacements, func(i, j int) bool {
		return bytes.Compare(out.PrefixReplacements[i].Prefix, out.PrefixReplacements[j].Prefix) < 0
	})
	for i := range out.PrefixReplacements {
		sort.Slice(out.PrefixReplacements[i].Sets, func(a, b int) bool {
			return bytes.Compare(out.PrefixReplacements[i].Sets[a].Key, out.PrefixReplacements[i].Sets[b].Key) < 0
		})
	}
	if err := validateConditionalBatch(out); err != nil {
		return nil, err
	}
	return out, nil
}

func validateConditionalBatch(conditional *ConditionalBatch) error {
	if conditional == nil ||
		(conditional.Version != conditionalBatchVersion &&
			conditional.Version != conditionalBatchTermFencedVersion &&
			conditional.Version != chunkTombstoneConditionalBatchVersion &&
			conditional.Version != chunkTombstoneFencedBatchVersion) {
		return fmt.Errorf("unsupported conditional batch")
	}
	switch conditional.Version {
	case conditionalBatchVersion:
		if conditional.ExpectedRaftTerm != 0 {
			return fmt.Errorf("conditional batch v1 cannot carry an expected Raft term")
		}
	case conditionalBatchTermFencedVersion:
		if conditional.ExpectedRaftTerm == 0 {
			return fmt.Errorf("term-fenced conditional batch requires an expected Raft term")
		}
	}
	for i, precondition := range conditional.Preconditions {
		if err := validateConditionalKey("precondition key", precondition.Key); err != nil {
			return err
		}
		if precondition.ExpectAbsent && len(precondition.ExpectedValue) != 0 {
			return fmt.Errorf("absence precondition %q carries an expected value", precondition.Key)
		}
		if !precondition.ExpectAbsent && len(precondition.ExpectedValue) > maxConditionalValueBytes {
			return fmt.Errorf("conditional expected value exceeds %d bytes", maxConditionalValueBytes)
		}
		if i > 0 && bytes.Equal(conditional.Preconditions[i-1].Key, precondition.Key) {
			return fmt.Errorf("duplicate conditional precondition for %q", precondition.Key)
		}
	}
	for i, mutation := range conditional.Mutations {
		if err := validateConditionalKey("mutation key", mutation.Key); err != nil {
			return err
		}
		if mutation.Delete && len(mutation.Value) != 0 {
			return fmt.Errorf("delete mutation %q carries a value", mutation.Key)
		}
		if !mutation.Delete && len(mutation.Value) > maxConditionalValueBytes {
			return fmt.Errorf("conditional mutation value exceeds %d bytes", maxConditionalValueBytes)
		}
		if i > 0 && bytes.Equal(conditional.Mutations[i-1].Key, mutation.Key) {
			return fmt.Errorf("duplicate conditional mutation for %q", mutation.Key)
		}
	}
	for i, replacement := range conditional.PrefixReplacements {
		if err := validateConditionalReplacementPrefix(replacement.Prefix); err != nil {
			return err
		}
		if len(replacement.Sets) > int(maxConditionalReplacementSets) {
			return fmt.Errorf("conditional replacement set count exceeds %d", maxConditionalReplacementSets)
		}
		if i > 0 {
			previous := conditional.PrefixReplacements[i-1].Prefix
			if bytes.HasPrefix(replacement.Prefix, previous) {
				return fmt.Errorf("overlapping conditional prefix replacements %q and %q", previous, replacement.Prefix)
			}
		}
		for setIndex, set := range replacement.Sets {
			if set.Delete {
				return fmt.Errorf("prefix replacement %q contains delete set", replacement.Prefix)
			}
			if err := validateConditionalKey("replacement key", set.Key); err != nil {
				return err
			}
			if !bytes.HasPrefix(set.Key, replacement.Prefix) || len(set.Key) == len(replacement.Prefix) {
				return fmt.Errorf("replacement key %q is not under prefix %q", set.Key, replacement.Prefix)
			}
			if len(set.Value) > maxConditionalValueBytes {
				return fmt.Errorf("conditional replacement value exceeds %d bytes", maxConditionalValueBytes)
			}
			if setIndex > 0 && bytes.Equal(replacement.Sets[setIndex-1].Key, set.Key) {
				return fmt.Errorf("duplicate replacement key %q", set.Key)
			}
		}
		for _, mutation := range conditional.Mutations {
			if bytes.HasPrefix(mutation.Key, replacement.Prefix) {
				return fmt.Errorf("mutation key %q conflicts with prefix replacement %q", mutation.Key, replacement.Prefix)
			}
		}
	}
	if conditionalBatchEncodedSize(conditional) > maxRaftLogEntryBytes {
		return fmt.Errorf("raft log entry exceeds %d bytes", maxRaftLogEntryBytes)
	}
	if conditional.Version == conditionalBatchTermFencedVersion {
		if err := validateTermFencedBackupConditionalBatch(conditional); err != nil {
			return err
		}
	}
	if conditional.Version == chunkTombstoneConditionalBatchVersion {
		if err := validateChunkTombstoneConditionalBatch(conditional); err != nil {
			return err
		}
	}
	if conditional.Version == chunkTombstoneFencedBatchVersion {
		if err := validateChunkTombstoneFencedBatch(conditional); err != nil {
			return err
		}
	}
	return nil
}

func validateConditionalReplacementPrefix(prefix []byte) error {
	if !bytes.Equal(prefix, []byte(prefixBackupCatalog)) {
		return fmt.Errorf(
			"conditional prefix replacement %q is not an allowed durable namespace",
			prefix,
		)
	}
	return nil
}

func conditionalBatchEncodedSize(conditional *ConditionalBatch) uint64 {
	// Op, version, and the three top-level counts.
	size := uint64(2 + 3*4)
	if conditional.Version == conditionalBatchTermFencedVersion {
		size += 8
	}
	for _, precondition := range conditional.Preconditions {
		size += uint64(1 + 4 + len(precondition.Key))
		if !precondition.ExpectAbsent {
			size += uint64(4 + len(precondition.ExpectedValue))
		}
	}
	for _, mutation := range conditional.Mutations {
		size += uint64(1 + 4 + len(mutation.Key))
		if !mutation.Delete {
			size += uint64(4 + len(mutation.Value))
		}
	}
	for _, replacement := range conditional.PrefixReplacements {
		size += uint64(4 + len(replacement.Prefix) + 4)
		for _, set := range replacement.Sets {
			size += uint64(4 + len(set.Key) + 4 + len(set.Value))
		}
	}
	return size
}

func validateConditionalKey(field string, key []byte) error {
	if len(key) == 0 {
		return fmt.Errorf("%s is empty", field)
	}
	if len(key) > maxConditionalKeyBytes {
		return fmt.Errorf("%s exceeds %d bytes", field, maxConditionalKeyBytes)
	}
	return nil
}

func decodeConditionalBatch(r *bytes.Reader) (*ConditionalBatch, error) {
	version, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	conditional := &ConditionalBatch{Version: version}
	if version != conditionalBatchVersion &&
		version != conditionalBatchTermFencedVersion &&
		version != chunkTombstoneConditionalBatchVersion &&
		version != chunkTombstoneFencedBatchVersion {
		return nil, fmt.Errorf("unsupported conditional batch version %d", version)
	}
	if version == conditionalBatchTermFencedVersion {
		if err := binary.Read(r, binary.BigEndian, &conditional.ExpectedRaftTerm); err != nil {
			return nil, err
		}
	}
	preconditionLimit := maxConditionalPreconditions
	mutationLimit := maxConditionalMutations
	if version == chunkTombstoneFencedBatchVersion {
		preconditionLimit = 2 + 2*MaxChunkAllocationBatch
		mutationLimit = 2 + MaxChunkAllocationBatch
	}
	preconditionCount, err := readBoundedCount(r, "conditional preconditions", preconditionLimit)
	if err != nil {
		return nil, err
	}
	conditional.Preconditions = make([]ConditionalPrecondition, preconditionCount)
	for i := range conditional.Preconditions {
		kind, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		if kind > 1 {
			return nil, fmt.Errorf("invalid conditional precondition kind %d", kind)
		}
		conditional.Preconditions[i].ExpectAbsent = kind == 1
		conditional.Preconditions[i].Key, err = readBoundedBytes(r, maxConditionalKeyBytes)
		if err != nil {
			return nil, err
		}
		if !conditional.Preconditions[i].ExpectAbsent {
			conditional.Preconditions[i].ExpectedValue, err = readBoundedBytes(r, maxConditionalValueBytes)
			if err != nil {
				return nil, err
			}
		}
	}
	mutationCount, err := readBoundedCount(r, "conditional mutations", mutationLimit)
	if err != nil {
		return nil, err
	}
	conditional.Mutations = make([]BatchOp, mutationCount)
	for i := range conditional.Mutations {
		if err := readBatchOp(r, &conditional.Mutations[i]); err != nil {
			return nil, err
		}
	}
	replacementCount, err := readBoundedCount(r, "conditional prefix replacements", maxConditionalPrefixReplacements)
	if err != nil {
		return nil, err
	}
	conditional.PrefixReplacements = make([]ConditionalPrefixReplacement, replacementCount)
	for i := range conditional.PrefixReplacements {
		replacement := &conditional.PrefixReplacements[i]
		replacement.Prefix, err = readBoundedBytes(r, maxConditionalKeyBytes)
		if err != nil {
			return nil, err
		}
		setCount, err := readBoundedCount(r, "conditional replacement sets", maxConditionalReplacementSets)
		if err != nil {
			return nil, err
		}
		replacement.Sets = make([]BatchOp, setCount)
		for j := range replacement.Sets {
			replacement.Sets[j].Key, err = readBoundedBytes(r, maxConditionalKeyBytes)
			if err != nil {
				return nil, err
			}
			replacement.Sets[j].Value, err = readBoundedBytes(r, maxConditionalValueBytes)
			if err != nil {
				return nil, err
			}
		}
	}
	return canonicalConditionalBatch(conditional)
}

func writeUint32(buf *bytes.Buffer, value uint32) {
	var encoded [4]byte
	binary.BigEndian.PutUint32(encoded[:], value)
	buf.Write(encoded[:])
}

func writeBatchOp(buf *bytes.Buffer, op BatchOp) {
	if op.Delete {
		buf.WriteByte(1)
		writeBytes(buf, op.Key)
		return
	}
	buf.WriteByte(0)
	writeBytes(buf, op.Key)
	writeBytes(buf, op.Value)
}

func readBatchOp(r *bytes.Reader, op *BatchOp) error {
	kind, err := r.ReadByte()
	if err != nil {
		return err
	}
	if kind > 1 {
		return fmt.Errorf("invalid conditional mutation kind %d", kind)
	}
	op.Delete = kind == 1
	op.Key, err = readBoundedBytes(r, maxConditionalKeyBytes)
	if err != nil {
		return err
	}
	if !op.Delete {
		op.Value, err = readBoundedBytes(r, maxConditionalValueBytes)
	}
	return err
}

func readBoundedCount(r *bytes.Reader, field string, maximum uint32) (uint32, error) {
	var count uint32
	if err := binary.Read(r, binary.BigEndian, &count); err != nil {
		return 0, err
	}
	if count > maximum {
		return 0, fmt.Errorf("%s count %d exceeds %d", field, count, maximum)
	}
	return count, nil
}

func readBoundedBytes(r *bytes.Reader, maximum int) ([]byte, error) {
	var encoded [4]byte
	if _, err := io.ReadFull(r, encoded[:]); err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint32(encoded[:])
	if uint64(length) > uint64(maximum) {
		return nil, fmt.Errorf("encoded byte string length %d exceeds %d", length, maximum)
	}
	if uint64(length) > uint64(r.Len()) {
		return nil, io.ErrUnexpectedEOF
	}
	value := make([]byte, int(length))
	if _, err := io.ReadFull(r, value); err != nil {
		return nil, err
	}
	return value, nil
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
	store            *PebbleStore
	snapshotMu       sync.RWMutex
	lastAppliedIndex uint64
	lastAppliedTerm  uint64
	syncStopCh       chan struct{}
	syncInterval     time.Duration
	syncMu           sync.Mutex
	syncWG           sync.WaitGroup
}

// Apply applies a committed Raft log entry to the Pebble store.
func (f *PebbleFSM) Apply(l *raft.Log) interface{} {
	f.snapshotMu.RLock()
	defer f.snapshotMu.RUnlock()

	if len(l.Data) == 0 {
		f.recordApplied(l)
		return nil
	}

	entry, err := DecodeRaftLogEntry(l.Data)
	if err != nil {
		slog.Error("fsm: decode error", "index", l.Index, "error", err)
		return err
	}

	switch entry.Op {
	case OpSet:
		err = applyReferenceAwareBatch(f.store.db, []BatchOp{{Key: entry.Key, Value: entry.Value}}, pebble.NoSync)

	case OpDelete:
		err = applyReferenceAwareBatch(f.store.db, []BatchOp{{Delete: true, Key: entry.Key}}, pebble.NoSync)

	case OpBatch:
		err = applyReferenceAwareBatch(f.store.db, entry.Batch, pebble.NoSync)

	case OpConditionalBatch:
		if expected := entry.Conditional.ExpectedRaftTerm; expected != 0 && expected != l.Term {
			err = fmt.Errorf(
				"%w: expected Raft term %d, applied in term %d",
				ErrRaftConditionalConflict,
				expected,
				l.Term,
			)
			break
		}
		err = applyConditionalBatchWithHook(f.store.db, entry.Conditional, pebble.NoSync, f.store.conditionalBatchBeforeCommit)

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

		// Apply update through the same reference-aware path as OpSet/OpBatch.
		err = applyReferenceAwareBatch(f.store.db, []BatchOp{{Key: entry.Key, Value: newData}}, pebble.NoSync)
	}

	if err != nil && !errors.Is(err, ErrRaftConditionalConflict) {
		slog.Error("fsm: apply error", "index", l.Index, "error", err)
	}
	if err == nil {
		f.invalidateInodeCache(entry)
		f.recordApplied(l)
	}
	return err
}

func (f *PebbleFSM) recordApplied(l *raft.Log) {
	f.lastAppliedIndex = l.Index
	f.lastAppliedTerm = l.Term
}

func (f *PebbleFSM) invalidateInodeCache(entry *RaftLogEntry) {
	invalidate := func(key []byte) {
		if !validInodeMetadataKey(string(key)) {
			return
		}
		id, _ := strconv.ParseUint(strings.TrimPrefix(string(key), prefixInode), 10, 64)
		f.store.inCache.del(InodeID(id))
	}
	switch entry.Op {
	case OpSet, OpDelete, OpCAS:
		invalidate(entry.Key)
	case OpBatch:
		for _, op := range entry.Batch {
			invalidate(op.Key)
		}
	}
}

func applyConditionalBatch(db *pebble.DB, conditional *ConditionalBatch, sync *pebble.WriteOptions) error {
	return applyConditionalBatchWithHook(db, conditional, sync, nil)
}

func applyConditionalBatchWithHook(db *pebble.DB, conditional *ConditionalBatch, sync *pebble.WriteOptions, beforeCommit func() error) error {
	canonical, err := canonicalConditionalBatch(conditional)
	if err != nil {
		return err
	}
	for _, precondition := range canonical.Preconditions {
		current, closer, getErr := db.Get(precondition.Key)
		switch {
		case precondition.ExpectAbsent && getErr == nil:
			closer.Close()
			return ErrRaftConditionalConflict
		case precondition.ExpectAbsent && errors.Is(getErr, pebble.ErrNotFound):
			continue
		case precondition.ExpectAbsent:
			return getErr
		case errors.Is(getErr, pebble.ErrNotFound):
			return ErrRaftConditionalConflict
		case getErr != nil:
			return getErr
		}
		matches := bytes.Equal(current, precondition.ExpectedValue)
		closer.Close()
		if !matches {
			return ErrRaftConditionalConflict
		}
	}

	batch := db.NewBatch()
	defer batch.Close()
	for _, mutation := range canonical.Mutations {
		if mutation.Delete {
			if err := batch.Delete(mutation.Key, nil); err != nil {
				return err
			}
		} else if err := batch.Set(mutation.Key, mutation.Value, nil); err != nil {
			return err
		}
	}
	for _, replacement := range canonical.PrefixReplacements {
		iter, err := db.NewIter(&pebble.IterOptions{
			LowerBound: replacement.Prefix,
			UpperBound: prefixUpperBound(string(replacement.Prefix)),
		})
		if err != nil {
			return err
		}
		for iter.First(); iter.Valid(); iter.Next() {
			key := append([]byte(nil), iter.Key()...)
			if err := batch.Delete(key, nil); err != nil {
				iter.Close()
				return err
			}
		}
		if err := iter.Error(); err != nil {
			iter.Close()
			return err
		}
		if err := iter.Close(); err != nil {
			return err
		}
		for _, set := range replacement.Sets {
			if err := batch.Set(set.Key, set.Value, nil); err != nil {
				return err
			}
		}
	}
	if beforeCommit != nil {
		if err := beforeCommit(); err != nil {
			return err
		}
	}
	return conditionalBatchCommit(batch, sync)
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
	f.snapshotMu.Lock()
	defer f.snapshotMu.Unlock()

	if f.store.cfg.UseInMemory {
		var data bytes.Buffer
		if err := writeKVStream(f.store, &data); err != nil {
			return nil, err
		}
		return &PebbleSnapshot{pbl1Data: data.Bytes()}, nil
	}

	checkpointDir, err := createCheckpointDir(
		context.Background(),
		f.store,
		filepath.Dir(f.store.cfg.Dir),
		"raft-checkpoint-*",
	)
	if err != nil {
		return nil, err
	}
	return &PebbleSnapshot{checkpointDir: checkpointDir}, nil
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
	checkpointDir string
	pbl1Data      []byte
	persistMu     sync.RWMutex
	released      bool
	releaseOnce   sync.Once
}

// Persist writes the immutable state captured by PebbleFSM.Snapshot.
func (s *PebbleSnapshot) Persist(sink raft.SnapshotSink) error {
	s.persistMu.RLock()
	defer s.persistMu.RUnlock()
	if s.released {
		sink.Cancel()
		return fmt.Errorf("snapshot has been released")
	}

	var err error
	if s.checkpointDir != "" {
		err = checkpointWriteDir(s.checkpointDir, sink)
	} else {
		_, err = sink.Write(s.pbl1Data)
	}
	if err != nil {
		sink.Cancel()
		return fmt.Errorf("checkpoint persist: %w", err)
	}

	return sink.Close()
}

// writeKVStream writes the legacy PBL1 KV-stream format.
func writeKVStream(store *PebbleStore, dst io.Writer) error {
	if _, err := dst.Write([]byte("PBL1")); err != nil {
		return fmt.Errorf("write magic: %w", err)
	}

	zw, err := zstd.NewWriter(dst)
	if err != nil {
		return fmt.Errorf("zstd new: %w", err)
	}

	iter, err := store.db.NewIter(nil)
	if err != nil {
		zw.Close()
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
		return iterErr
	}

	var termBuf [4]byte
	for i := range termBuf {
		termBuf[i] = 0xFF
	}
	if _, err := zw.Write(termBuf[:]); err != nil {
		zw.Close()
		return fmt.Errorf("write term: %w", err)
	}
	var countBuf [8]byte
	binary.BigEndian.PutUint64(countBuf[:], count)
	if _, err := zw.Write(countBuf[:]); err != nil {
		zw.Close()
		return fmt.Errorf("write count: %w", err)
	}

	if err := zw.Close(); err != nil {
		return fmt.Errorf("zstd close: %w", err)
	}
	return nil
}

// Release cleans up snapshot resources.
func (s *PebbleSnapshot) Release() {
	s.releaseOnce.Do(func() {
		s.persistMu.Lock()
		defer s.persistMu.Unlock()
		s.released = true
		s.pbl1Data = nil
		if s.checkpointDir != "" {
			if err := os.RemoveAll(s.checkpointDir); err != nil {
				slog.Warn("failed to remove snapshot checkpoint", "path", s.checkpointDir, "error", err)
			}
		}
	})
}

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

	// Test hooks for deterministic checkpoint-boundary and leadership tests.
	backupCheckpointLockedHook     func()
	backupCheckpointFinalStateHook func() (bool, uint64, error)
	backupFutureOnce               sync.Once
	backupFutureSlot               chan struct{}

	conditionalFutureOnce  sync.Once
	conditionalFutureSlots chan struct{}

	// Test hooks for deterministic conditional-future lifecycle tests.
	conditionalLeaderHook func() bool
	conditionalApplyHook  func([]byte, time.Duration) raft.ApplyFuture

	// Test hooks for public submitter and auto-forward boundary tests.
	publicLeaderHook func() bool
	publicApplyHook  func(*RaftLogEntry, time.Duration) error
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
	// PreSeedBootstrapPeers seeds every BootstrapPeer into the initial Raft
	// configuration as a voter when true. Set it only when all peers start in
	// the same process at the same time and can form a quorum at boot — i.e.
	// the in-process multi-node integration harness. It must stay false (default)
	// for multi-process baremetal/helm deploys, where peer processes come up at
	// different times: pre-seeding a peer that is not yet up leaves the seed with
	// no reachable quorum and stalls election. Multi-process thus leaves this
	// false and relies on the leader-driven reconcile loop (RaftNode.EnsurePeers /
	// cmd/metad reconcileMetadPeers) to add each peer as a voter once it is up.
	PreSeedBootstrapPeers bool

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
		// cfg.Peers (legacy) is always seeded: it is used by the in-process test
		// harness where all nodes share one process and can form a quorum at boot.
		for _, peer := range cfg.Peers {
			servers = append(servers, raft.Server{
				ID:      raft.ServerID(peer),
				Address: raft.ServerAddress(peer),
			})
		}
		// cfg.BootstrapPeers (multi-process baremetal/helm) is seeded only when
		// PreSeedBootstrapPeers is true (in-process harness). For multi-process it
		// stays empty: peer processes may not be up yet, so pre-seeding them as
		// voters would leave the seed unable to reach a quorum and stall election.
		// Instead the seed starts with only itself as a voter, becomes leader, and
		// the leader-driven reconcile loop (cmd/metad reconcileMetadPeers) brings
		// each BootstrapPeer into the voting set with AddVoter as it comes up.
		if cfg.PreSeedBootstrapPeers {
			for _, peer := range cfg.BootstrapPeers {
				if peer.ID == cfg.NodeID {
					continue
				}
				servers = append(servers, raft.Server{
					ID:      raft.ServerID(peer.ID),
					Address: raft.ServerAddress(peer.Address),
				})
			}
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
	node.initBackupFutureLimiter()
	node.initConditionalFutureLimiter()
	store.SetRaftNode(node)
	return node, nil
}

// IsLeader returns true if this node is the current leader.
func (n *RaftNode) IsLeader() bool {
	if n.publicLeaderHook != nil {
		return n.publicLeaderHook()
	}
	return n.raft.State() == raft.Leader
}

// TransferLeadership transfers leadership to another node. If targetID
// is empty, the Raft library picks the best candidate. Returns an error
// if the transfer fails or times out.
func (n *RaftNode) TransferLeadership(targetID string) error {
	if n.raft == nil {
		return fmt.Errorf("raft not initialized")
	}
	var future raft.Future
	if targetID == "" {
		future = n.raft.LeadershipTransfer()
	} else {
		// Find the server address for the given ID.
		config := n.raft.GetConfiguration()
		if err := config.Error(); err != nil {
			return fmt.Errorf("get raft config: %w", err)
		}
		var found bool
		for _, srv := range config.Configuration().Servers {
			if string(srv.ID) == targetID {
				future = n.raft.LeadershipTransferToServer(srv.ID, srv.Address)
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("server %s not found in raft configuration", targetID)
		}
	}
	return future.Error()
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

// StoreOpsURL submits this node's ops HTTP URL to the FSM. It is leader-only:
// metadata request routing is responsible for directing follower callers.
func (n *RaftNode) StoreOpsURL(timeout time.Duration) error {
	if n.advertiseOps == "" {
		return nil
	}
	entry := &RaftLogEntry{
		Op:    OpSet,
		Key:   []byte(metaNodeOpsKey(n.nodeID)),
		Value: []byte(n.advertiseOps),
	}
	return n.applyTrusted(entry, timeout)
}

// Apply sends a write through Raft consensus.
// Returns an error if this node is not the leader.
func (n *RaftNode) Apply(entry *RaftLogEntry, timeout time.Duration) error {
	if err := rejectPublicRaftApply(entry); err != nil {
		return err
	}
	if n.publicApplyHook != nil {
		if !n.IsLeader() {
			if n.raft == nil {
				return fmt.Errorf("not leader")
			}
			return fmt.Errorf("not leader (leader at %s)", n.LeaderAddr())
		}
		return n.publicApplyHook(entry, timeout)
	}
	return n.applyTrusted(entry, timeout)
}

func (n *RaftNode) applyTrusted(entry *RaftLogEntry, timeout time.Duration) error {
	data, err := entry.EncodeChecked()
	if err != nil {
		return fmt.Errorf("raft apply: encode: %w", err)
	}
	if !n.IsLeader() {
		if n.raft == nil {
			return fmt.Errorf("not leader")
		}
		return fmt.Errorf("not leader (leader at %s)", n.LeaderAddr())
	}
	f := n.raft.Apply(data, timeout)
	return raftApplyFutureError(f)
}

// applyConditional submits a restricted conditional batch on the local leader.
// Once accepted by Raft, caller cancellation cannot cancel the log; cancellation
// therefore returns ErrRaftConditionalOutcomeUnknown and requires reconciliation.
func (n *RaftNode) applyConditional(ctx context.Context, entry *RaftLogEntry) error {
	if ctx == nil {
		return fmt.Errorf("conditional raft apply: nil context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !n.conditionalIsLeader() {
		return fmt.Errorf("not leader (leader at %s)", n.LeaderAddr())
	}
	if err := n.acquireConditionalFutureSlot(ctx); err != nil {
		return err
	}
	releaseSlot := true
	defer func() {
		if releaseSlot {
			n.releaseConditionalFutureSlot()
		}
	}()
	if err := ctx.Err(); err != nil {
		return err
	}
	data, err := entry.EncodeChecked()
	if err != nil {
		return fmt.Errorf("conditional raft apply: encode: %w", err)
	}
	timeout, err := conditionalRaftApplyTimeout(ctx)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	future := n.startConditionalApply(data, timeout)
	if future == nil {
		return fmt.Errorf("conditional raft apply returned nil future")
	}
	resultCh := make(chan error, 1)
	go func() {
		resultErr := raftApplyFutureError(future)
		n.releaseConditionalFutureSlot()
		resultCh <- resultErr
	}()
	releaseSlot = false
	select {
	case err := <-resultCh:
		return err
	case <-ctx.Done():
		select {
		case err := <-resultCh:
			return err
		default:
			return fmt.Errorf("%w: %w", ErrRaftConditionalOutcomeUnknown, ctx.Err())
		}
	}
}

func (n *RaftNode) applyConditionalAccepted(ctx context.Context, entry *RaftLogEntry, wait time.Duration) error {
	if ctx == nil {
		return fmt.Errorf("conditional raft apply: nil context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !n.conditionalIsLeader() {
		return fmt.Errorf("not leader (leader at %s)", n.LeaderAddr())
	}
	if err := n.acquireConditionalFutureSlot(ctx); err != nil {
		return err
	}
	releaseSlot := true
	defer func() {
		if releaseSlot {
			n.releaseConditionalFutureSlot()
		}
	}()
	if err := ctx.Err(); err != nil {
		return err
	}
	data, err := entry.EncodeChecked()
	if err != nil {
		return fmt.Errorf("conditional raft apply: encode: %w", err)
	}
	future := n.startConditionalApply(data, wait)
	if future == nil {
		return fmt.Errorf("conditional raft apply returned nil future")
	}
	resultCh := make(chan error, 1)
	go func() {
		resultErr := raftApplyFutureError(future)
		n.releaseConditionalFutureSlot()
		resultCh <- resultErr
	}()
	releaseSlot = false
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case err := <-resultCh:
		return err
	case <-timer.C:
		return ErrRaftConditionalOutcomeUnknown
	}
}

func (n *RaftNode) initConditionalFutureLimiter() {
	n.conditionalFutureOnce.Do(func() {
		n.conditionalFutureSlots = make(chan struct{}, conditionalFutureWaiterCapacity)
	})
}

func (n *RaftNode) acquireConditionalFutureSlot(ctx context.Context) error {
	n.initConditionalFutureLimiter()
	select {
	case n.conditionalFutureSlots <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (n *RaftNode) releaseConditionalFutureSlot() {
	<-n.conditionalFutureSlots
}

func (n *RaftNode) conditionalIsLeader() bool {
	if n.conditionalLeaderHook != nil {
		return n.conditionalLeaderHook()
	}
	return n.IsLeader()
}

func (n *RaftNode) startConditionalApply(data []byte, timeout time.Duration) raft.ApplyFuture {
	if n.conditionalApplyHook != nil {
		return n.conditionalApplyHook(data, timeout)
	}
	return n.raft.Apply(data, timeout)
}

func conditionalRaftApplyTimeout(ctx context.Context) (time.Duration, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	const defaultTimeout = 10 * time.Second
	deadline, ok := ctx.Deadline()
	if !ok {
		return defaultTimeout, nil
	}
	timeout := time.Until(deadline)
	if timeout <= 0 {
		return 0, context.DeadlineExceeded
	}
	return timeout, nil
}

// ApplyAutoForward is retained for API compatibility but is leader-only.
// Metadata HTTP request routing redirects followers before this boundary;
// raw Raft entries are never forwarded over an unauthenticated HTTP ingress.
func (n *RaftNode) ApplyAutoForward(entry *RaftLogEntry, timeout time.Duration) error {
	if err := rejectPublicRaftApply(entry); err != nil {
		return err
	}
	return n.applyTrusted(entry, timeout)
}

func (n *RaftNode) applyTrustedAutoForward(entry *RaftLogEntry, timeout time.Duration) error {
	return n.applyTrusted(entry, timeout)
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

func raftApplyFutureError(future raft.ApplyFuture) error {
	if future == nil {
		return fmt.Errorf("raft apply returned nil future")
	}
	if err := future.Error(); err != nil {
		return err
	}
	if responseErr, ok := future.Response().(error); ok && responseErr != nil {
		return responseErr
	}
	return nil
}

func rejectPublicRaftApply(entry *RaftLogEntry) error {
	if entry == nil {
		return fmt.Errorf("raft apply entry is required")
	}
	if entry.Op == OpConditionalBatch {
		return fmt.Errorf("conditional Raft operation rejected by generic apply; use the restricted metadata submitter")
	}
	keys := [][]byte{entry.Key}
	if entry.Op == OpBatch {
		keys = make([][]byte, 0, len(entry.Batch))
		for _, op := range entry.Batch {
			keys = append(keys, op.Key)
		}
	}
	for _, key := range keys {
		if isPublicRaftProtectedKey(string(key)) {
			return fmt.Errorf("protected metadata Raft operation rejected by generic apply")
		}
	}
	return nil
}

func isPublicRaftProtectedKey(key string) bool {
	return strings.HasPrefix(key, prefixChunk) ||
		strings.HasPrefix(key, prefixChunkTombstone) ||
		key == keyInodeReferenceEpoch ||
		strings.HasPrefix(key, prefixBackupTask) ||
		strings.HasPrefix(key, prefixBackupCatalog) ||
		key == keyBackupCatalog || key == keyClusterID || key == keyRestorePending
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

func checkpointBarrierTimeout(ctx context.Context) (time.Duration, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return 0, nil
	}
	timeout := time.Until(deadline)
	if timeout <= 0 {
		return 0, context.DeadlineExceeded
	}
	return timeout, nil
}

func (n *RaftNode) initBackupFutureLimiter() {
	n.backupFutureOnce.Do(func() {
		n.backupFutureSlot = make(chan struct{}, 1)
	})
}

func (n *RaftNode) waitBackupFuture(ctx context.Context, createFuture func() raft.Future) error {
	n.initBackupFutureLimiter()
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case n.backupFutureSlot <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}

	done := make(chan error, 1)
	go func() {
		defer func() {
			<-n.backupFutureSlot
		}()
		done <- createFuture().Error()
	}()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func parseCheckpointTerm(stats map[string]string) (uint64, error) {
	value, ok := stats["term"]
	if !ok || value == "" {
		return 0, fmt.Errorf("raft term is missing")
	}
	term, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse raft term %q: %w", value, err)
	}
	return term, nil
}

// CreateBackupCheckpoint creates a Leader-only immutable checkpoint at the
// Raft FSM's applied position.
func (n *RaftNode) CreateBackupCheckpoint(ctx context.Context, parentDir string) (*PortableCheckpoint, error) {
	if n == nil || n.raft == nil || n.fsm == nil {
		return nil, fmt.Errorf("raft node is not initialized")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !n.IsLeader() {
		return nil, fmt.Errorf("not leader (leader at %s)", n.LeaderAddr())
	}
	startTerm, err := parseCheckpointTerm(n.raft.Stats())
	if err != nil {
		return nil, fmt.Errorf("read initial raft term: %w", err)
	}
	if startTerm == 0 {
		return nil, fmt.Errorf("read initial raft term: zero term")
	}

	timeout, err := checkpointBarrierTimeout(ctx)
	if err != nil {
		return nil, err
	}
	if err := n.waitBackupFuture(ctx, func() raft.Future {
		return n.raft.Barrier(timeout)
	}); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, fmt.Errorf("raft barrier: %w", err)
	}
	if !n.IsLeader() {
		return nil, fmt.Errorf("not leader after barrier (leader at %s)", n.LeaderAddr())
	}

	timeout, err = checkpointBarrierTimeout(ctx)
	if err != nil {
		return nil, err
	}
	marker := &RaftLogEntry{Op: OpBatch}
	if err := n.waitBackupFuture(ctx, func() raft.Future {
		return n.raft.Apply(marker.Encode(), timeout)
	}); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, fmt.Errorf("raft checkpoint marker: %w", err)
	}
	if !n.IsLeader() {
		return nil, fmt.Errorf("not leader after checkpoint marker (leader at %s)", n.LeaderAddr())
	}

	if err := lockSnapshotWithContext(ctx, &n.fsm.snapshotMu); err != nil {
		return nil, err
	}
	if n.backupCheckpointLockedHook != nil {
		n.backupCheckpointLockedHook()
	}
	appliedIndex := n.fsm.lastAppliedIndex
	term := n.fsm.lastAppliedTerm
	if appliedIndex == 0 || term == 0 || term != startTerm {
		n.fsm.snapshotMu.Unlock()
		return nil, fmt.Errorf(
			"backup checkpoint leadership changed before checkpoint: started term %d, FSM boundary %d/%d",
			startTerm,
			appliedIndex,
			term,
		)
	}
	checkpointDir, err := createCheckpointDir(ctx, n.fsm.store, parentDir, "raft-backup-checkpoint-*")
	n.fsm.snapshotMu.Unlock()

	if err != nil {
		return nil, fmt.Errorf("create Raft backup checkpoint: %w", err)
	}
	checkpoint := &PortableCheckpoint{
		Dir:          checkpointDir,
		Term:         term,
		AppliedIndex: appliedIndex,
	}

	finalLeader := n.IsLeader()
	finalTerm, finalErr := parseCheckpointTerm(n.raft.Stats())
	if n.backupCheckpointFinalStateHook != nil {
		finalLeader, finalTerm, finalErr = n.backupCheckpointFinalStateHook()
	}
	if finalErr != nil {
		if cleanupErr := checkpoint.Release(); cleanupErr != nil {
			return nil, fmt.Errorf("read final raft term: %w (checkpoint cleanup: %v)", finalErr, cleanupErr)
		}
		return nil, fmt.Errorf("read final raft term: %w", finalErr)
	}
	if !finalLeader || finalTerm != startTerm || term != startTerm {
		leadershipErr := fmt.Errorf(
			"backup checkpoint leadership changed: leader=%t, started term %d, FSM term %d, current term %d",
			finalLeader,
			startTerm,
			term,
			finalTerm,
		)
		if cleanupErr := checkpoint.Release(); cleanupErr != nil {
			return nil, fmt.Errorf("%w (checkpoint cleanup: %v)", leadershipErr, cleanupErr)
		}
		return nil, leadershipErr
	}
	return checkpoint, nil
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

// EnsurePeers reconciles cluster membership so every requested peer (id, addr)
// is a voting member. It is leader-only and idempotent:
//
//   - A peer already in the configuration as a voter with the same address is
//     left untouched and counted under already.
//   - A peer that is absent, or present under a different address, is added (or
//     re-addressed) as a voter via AddVoter.
//
// The caller must drive this with retry: the method applies the config change
// on the leader (10s timeout) but cannot make a not-yet-listening peer appear
// in the cluster on its own — a peer whose transport is down will keep timing
// out until it is reachable.
func (n *RaftNode) EnsurePeers(peers []RaftPeer) (added, already int, err error) {
	if !n.IsLeader() {
		return 0, 0, fmt.Errorf("not leader")
	}
	cfgF := n.raft.GetConfiguration()
	if err := cfgF.Error(); err != nil {
		return 0, 0, fmt.Errorf("read raft configuration: %w", err)
	}
	members := make(map[string]string, len(cfgF.Configuration().Servers)) // id -> addr
	for _, s := range cfgF.Configuration().Servers {
		members[string(s.ID)] = string(s.Address)
	}
	for _, peer := range peers {
		if peer.ID == n.nodeID {
			// Never add ourselves to our own membership request; the local
			// server is already part of the bootstrap configuration.
			continue
		}
		if addr, ok := members[peer.ID]; ok && addr == peer.Address {
			already++
			continue
		}
		// Absent (or present under a stale address): add/re-address as a voter.
		// AddVoter is idempotent for an already-member id — it just updates the
		// address — so the same call covers both cases.
		f := n.raft.AddVoter(raft.ServerID(peer.ID), raft.ServerAddress(peer.Address), 0, 10*time.Second)
		if err := f.Error(); err != nil {
			return added, already, fmt.Errorf("add voter %s@%s: %w", peer.ID, peer.Address, err)
		}
		added++
		members[peer.ID] = peer.Address
	}
	return added, already, nil
}

// Stats returns Raft statistics.
func (n *RaftNode) Stats() map[string]string {
	return n.raft.Stats()
}

// Peers returns the current raft configuration membership as id -> address.
// It is useful for diagnostic/join logic.
func (n *RaftNode) Peers() map[string]string {
	cfgF := n.raft.GetConfiguration()
	if err := cfgF.Error(); err != nil {
		return nil
	}
	members := make(map[string]string, len(cfgF.Configuration().Servers))
	for _, s := range cfgF.Configuration().Servers {
		members[string(s.ID)] = string(s.Address)
	}
	return members
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
	op.Key = append([]byte(nil), op.Key...)
	op.Value = append([]byte(nil), op.Value...)
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
	if err := rejectPublicRaftApply(&RaftLogEntry{Op: OpBatch, Batch: []BatchOp{op}}); err != nil {
		return err
	}
	if n.batch == nil {
		opType := OpSet
		if op.Delete {
			opType = OpDelete
		}
		return n.Apply(&RaftLogEntry{Op: opType, Key: op.Key, Value: op.Value}, timeout)
	}
	return n.batch.ApplyBatched(op, timeout)
}
