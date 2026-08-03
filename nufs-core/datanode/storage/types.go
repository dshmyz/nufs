// Package storage implements the V2 DataNode storage engine: a
// segment/container-based extent store with a local persistent index,
// WAL, checkpoint, and bounded recovery. It replaces the legacy
// one-chunk-per-file layout (nufs-core/datanode/chunkstore.go).
//
// The package is structured so that no path builds an unbounded
// in-memory map of all extents, and startup recovery is bounded by
// work since the last checkpoint, never by total stored extents.
package storage

import (
	"errors"
	"fmt"
	"time"
)

// ========== Core identifiers ==========

// ExtentID uniquely identifies a logical extent in the cluster.
// It encodes the owning metadata shard in the high bits (16-bit owner
// shard ID per §11.4), so ownership is stable and explicit, not
// derived from a hash ring at operation time.
type ExtentID uint64

// SegmentID identifies a segment file on a single disk. Segment IDs
// and offsets are DataNode-local details (§11.3); they are never
// interpreted by the metadata layer.
type SegmentID uint64

// Generation is a per-extent monotonically increasing version used for
// generation fencing (§6.2): a stale write, overwrite, or delete with a
// lower generation is rejected.
type Generation uint64

// OwnerShard returns the 16-bit owning metadata shard encoded in the
// extent ID (§11.4).
func (id ExtentID) OwnerShard() uint16 {
	return uint16(id >> 48)
}

// ========== Format constants ==========

// Format version of the on-disk record/segment format. Bumped only on
// incompatible layout changes.
const FormatVersion = 3

// Record magic distinguishes record payload framing from arbitrary
// bytes in a segment.
const RecordMagic = 0x4E554653 // "NUFS"

// Magic constants for on-disk containers.
const (
	SuperblockMagic = 0x53555042 // "SUPB"
	SegmentMagic    = 0x53454744 // "SEGD"
	ManifestMagic   = 0x4D414E49 // "MANI"
	WalMagic        = 0x57414C46 // "WALF"
)

// WAL defaults (§7.1, §16).
const (
	DefaultWALSegmentSize          = 256 << 20       // 256 MiB
	DefaultWALRotateInterval       = 10 * 60 * 1e9   // 10 minutes in ns
	DefaultWALRetainMin            = 24 * 3600 * 1e9 // 24 hours in ns
	DefaultCheckpointInterval      = 5 * 60 * 1e9    // 5 minutes in ns
	DefaultCheckpointMaxWALRecords = 1000000
	DefaultCheckpointMaxWALBytes   = 2 << 30 // 2 GiB
	DefaultRetainCheckpoints       = 3
)

// Size limits from the design (§5.1, §16). The segment must never be
// smaller than the extent size.
const (
	// SegmentHeaderSize / SegmentFooterSize are the fixed on-disk sizes
	// of the segment header and footer containers. They live here (not
	// in the segment package) because the allocator reserves their
	// combined space at the head and tail of a segment.
	//
	// Header: magic(4)+version(1)+id(8)+class(1)+reserved(4)+crc(4) = 22
	// Footer: magic(4)+version(1)+record_count(8)+total_payload(8)
	//         +min(8)+max(8)+last_seq(8)+created(8)+sealed(8)+crc(4) = 65
	SegmentHeaderSize = 22
	SegmentFooterSize = 65

	// SmallFileThreshold is the maximum size of a single logical file
	// stored as one record in a small segment.
	SmallFileThreshold = 64 << 10 // 64 KiB
	// MaxInlineExtent is the largest extent stored inline.
	MaxInlineExtent = 16 << 20 // 16 MiB
	// MaxExtentSize is the fixed extent size for files split into
	// multiple extents (the final extent may be smaller).
	MaxExtentSize = 16 << 20 // 16 MiB
	// DefaultSmallSegmentSize is the sealed size limit for small segments.
	DefaultSmallSegmentSize = 1 << 30 // 1 GiB
	// DefaultDataSegmentSize is the sealed size limit for data segments.
	DefaultDataSegmentSize = 4 << 30 // 4 GiB
	// MaxRecordsPerSegment bounds records in one segment.
	MaxRecordsPerSegment = 1000000

	// Recovery bounds apply to the active segment tail on one disk. Keep
	// these in storage so segment parsing and recovery orchestration share
	// one policy without importing each other.
	MaxRecoveryRecords       uint64        = 100000
	MaxRecoveryReplayBytes   int64         = 256 << 20 // 256 MiB
	MaxRecoveryTrailingBytes int64         = 128 << 20 // 128 MiB
	RecoveryBudget           time.Duration = 30 * time.Second

	// CompressionNoCompressionThreshold: files below this are not
	// compressed by default (§9).
	CompressionNoCompressionThreshold = 4 << 10 // 4 KiB
	// CompressionMinSavingsRatio: sampled zstd is only used when it
	// saves at least this fraction.
	CompressionMinSavingsRatio = 0.10
)

// CompressionCodec identifies the record payload compression.
type CompressionCodec uint8

const (
	CompressionNone CompressionCodec = iota
	CompressionZstd
)

func (c CompressionCodec) String() string {
	switch c {
	case CompressionZstd:
		return "zstd"
	default:
		return "none"
	}
}

// SegmentClass distinguishes small-file segments from data segments
// (§5.1): small segments hold records ≤ 64 KiB, data segments hold
// extent records from 64 KiB up to the 16 MiB extent size.
type SegmentClass uint8

const (
	SegmentData SegmentClass = iota
	SegmentSmall
)

// ExtentState tracks the lifecycle of an extent on a local disk.
type ExtentState uint8

const (
	// ExtentDurable is the normal state: data + index entry are both
	// durable and consistent.
	ExtentDurable ExtentState = iota
	// ExtentTombstoned marks a generation-fenced delete whose physical
	// bytes await compaction.
	ExtentTombstoned
	// ExtentCorrupt marks an extent whose index entry points to invalid
	// data; the owning segment is quarantined.
	ExtentCorrupt
	// ExtentRelocating is a transient state during compaction; the new
	// location is not visible until RELOCATE commits.
	ExtentRelocating
)

// ========== Errors ==========

// StoreSink is the interface a disk store provides to the compactor:
// append live records into its active segment and relocate index
// entries atomically (§10.3).
type StoreSink interface {
	AppendRecord(extentID ExtentID, gen Generation, data []byte, codec CompressionCodec) (*Reloc, error)
	Relocate(relocs []Reloc) error
}

// Sentinel errors for the V2 storage path (§8). Checksum and decrypt
// failures never fall back to unverified bytes.
var (
	// ErrExtentNotFound is returned when no index entry exists for the
	// extent ID and generation.
	ErrExtentNotFound = errors.New("storage: extent not found")
	// ErrStaleGeneration is returned when a write/delete targets an
	// older generation than the stored one (generation fencing).
	ErrStaleGeneration = errors.New("storage: stale generation")
	// ErrUnsupportedRecordOperation marks a recognized durable operation whose
	// recovery semantics are not implemented. Recovery must fail closed rather
	// than treating it as a corrupt record or applying it as another operation.
	ErrUnsupportedRecordOperation = errors.New("storage: unsupported record operation")
	// ErrChecksumMismatch is returned when a record payload fails its
	// checksum. The bytes are never returned to the caller.
	ErrChecksumMismatch = errors.New("storage: checksum mismatch")
	// ErrSegmentUnavailable is returned when a segment file is missing
	// or unreadable.
	ErrSegmentUnavailable = errors.New("storage: segment unavailable")
	// ErrIndexCorrupt is returned when the local extent index is
	// inconsistent with a record.
	ErrIndexCorrupt = errors.New("storage: index corrupt")
	// ErrDecryptFailed is returned when record decryption fails.
	ErrDecryptFailed = errors.New("storage: decrypt failed")
	// ErrQuarantined is returned for reads of extents in a quarantined
	// segment (corruption detected).
	ErrQuarantined = errors.New("storage: segment quarantined")
	// ErrCapacity is returned when a write is rejected by the capacity
	// protection thresholds (§10.4).
	ErrCapacity = errors.New("storage: capacity protection")
	// ErrSegmentFull is returned when a segment cannot fit another
	// record and must be sealed first.
	ErrSegmentFull = errors.New("storage: segment full")
	// ErrRecoveryBudgetExceeded rejects startup recovery that exceeds one of
	// the bounded replay, tail, record, or elapsed-time limits.
	ErrRecoveryBudgetExceeded = errors.New("storage: recovery budget exceeded")
	// ErrStoreClosed rejects operations submitted once shutdown has begun.
	// Shutdown must fail these cleanly: touching a closed index panics.
	ErrStoreClosed = errors.New("storage: store closed")
	// ErrInvalidRange rejects a range read whose offset falls outside the
	// logical payload. Returning the whole extent instead would break the
	// §19 amplification bound and hand back unrequested bytes.
	ErrInvalidRange = errors.New("storage: invalid read range")
)

// ========== DurableReceipt ==========

// ========== Crash-point injection ==========

// Reloc is one record's new location after compaction (§10.3). It lives
// in the storage root so both the segment store and the maintenance
// compactor can reference it without an import cycle.
type Reloc struct {
	ExtentID   ExtentID
	Generation Generation
	SegmentID  SegmentID
	Offset     int64
	StoredLen  uint32
	LogicalLen uint32
}

// CrashPoint identifies a durable operation stage where a crash can be
// injected (V2.1 §18.2). The order mirrors the V2.1 write transaction
// (§6.1): a batch is appended, BatchCommit written, one sync executes,
// the overlay updates, and Pebble applies asynchronously.
type CrashPoint int

const (
	CrashBeforeBatchAppend CrashPoint = iota
	CrashAfterRecordAppend
	CrashAfterFrameIndex
	CrashAfterBatchCommitWrite
	CrashAfterBatchSync
	CrashBeforeOverlayApply
	CrashAfterOverlayApply
	CrashAfterAck // caller sees success, then async Pebble apply crashes
	CrashBeforeIndexApply
	CrashAfterIndexApply
	CrashBeforeIndexSafe
	CrashAfterIndexSafe
)

func (c CrashPoint) String() string {
	switch c {
	case CrashBeforeBatchAppend:
		return "before_batch_append"
	case CrashAfterRecordAppend:
		return "after_record_append"
	case CrashAfterFrameIndex:
		return "after_frame_index"
	case CrashAfterBatchCommitWrite:
		return "after_batchcommit_write"
	case CrashAfterBatchSync:
		return "after_batch_sync"
	case CrashBeforeOverlayApply:
		return "before_overlay_apply"
	case CrashAfterOverlayApply:
		return "after_overlay_apply"
	case CrashAfterAck:
		return "after_ack"
	case CrashBeforeIndexApply:
		return "before_index_apply"
	case CrashAfterIndexApply:
		return "after_index_apply"
	case CrashBeforeIndexSafe:
		return "before_index_safe"
	case CrashAfterIndexSafe:
		return "after_index_safe"
	default:
		return fmt.Sprintf("crash_point(%d)", int(c))
	}
}

// FaultHook is the interface a storage engine consults at each durable
// stage. Tests implement it to inject errors or simulate crashes.
type FaultHook interface {
	OnStage(point CrashPoint) error
}

// DurableReceipt is returned once both durability barriers pass
// (§6.1): the record payload is fdatasynced and the index mutation is
// fsynced in the WAL. Two receipts from distinct DataNodes constitute
// the two-replica acknowledgement required by §6.3.
type DurableReceipt struct {
	ExtentID   ExtentID
	Generation Generation
	SegmentID  SegmentID
	Offset     int64
	StoredLen  uint32
	LogicalLen uint32
	Seq        uint64 // index WAL sequence that made this durable
}

func (r *DurableReceipt) String() string {
	return fmt.Sprintf("receipt{extent=%d gen=%d seg=%d off=%d stored=%d log=%d seq=%d}",
		r.ExtentID, r.Generation, r.SegmentID, r.Offset, r.StoredLen, r.LogicalLen, r.Seq)
}
