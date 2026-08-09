package segment

import (
	"sync"

	"github.com/dshmyz/nufs/nufs-core/datanode/storage"
	"github.com/dshmyz/nufs/nufs-core/datanode/storage/journal"
)

// Allocator hands out append offsets within an active segment and
// decides when the segment must seal. It is the single writer for one
// disk (§6.4: "Each disk owns one append writer"), so its offset
// reservation is serialized behind a mutex.
type Allocator struct {
	mu sync.Mutex

	// Segment being written to.
	segmentID storage.SegmentID
	// Next append offset (start of the next record's header).
	offset int64
	// Bytes of payload written so far (for the footer total).
	payloadBytes int64
	// Number of records appended (for the footer count).
	recordCount uint64
	// First/last extent ID in this segment.
	minExtent storage.ExtentID
	maxExtent storage.ExtentID
	hasRecord bool

	// Size limits.
	maxSegmentBytes int64 // 4 GiB data / 1 GiB small
	maxRecords      uint64
	createdAt       int64
	class           storage.SegmentClass

	// lastCommitSeq is the last committed BatchCommit sequence for this
	// segment (persisted in the footer).
	lastCommitSeq uint64
}

// NewAllocator starts a fresh active segment.
func NewAllocator(segmentID storage.SegmentID, class storage.SegmentClass, maxSegmentBytes int64, createdAt int64) *Allocator {
	if maxSegmentBytes <= 0 {
		maxSegmentBytes = storage.DefaultDataSegmentSize
	}
	return &Allocator{
		segmentID:       segmentID,
		// Records start right after the segment header. The footer is
		// appended at seal time, not pre-reserved (V2.1 §7.2).
		offset:          int64(storage.SegmentHeaderSize),
		maxSegmentBytes: maxSegmentBytes,
		maxRecords:      storage.MaxRecordsPerSegment,
		createdAt:       createdAt,
		class:           class,
		minExtent:       0,
		maxExtent:       0,
	}
}

// Reserve allocates framingLen bytes for a record and records its
// extent/generation bounds. It returns the record's start offset, or an
// error if the segment cannot fit the record.
func (a *Allocator) Reserve(framingLen uint32, extent storage.ExtentID, storedLen uint32) (int64, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.recordCount >= a.maxRecords {
		return 0, ErrSegmentFull
	}
	if a.offset+int64(framingLen) > a.maxSegmentBytes {
		return 0, ErrSegmentFull
	}

	off := a.offset
	a.offset += int64(framingLen)
	a.payloadBytes += int64(storedLen)
	a.recordCount++
	if !a.hasRecord || extent < a.minExtent {
		a.minExtent = extent
	}
	if !a.hasRecord || extent > a.maxExtent {
		a.maxExtent = extent
	}
	a.hasRecord = true
	return off, nil
}

// CanReserveBatch reports whether the current segment can hold all bytes in
// a batch, including its single BatchCommit, without changing allocator
// state. Callers preflight the complete batch before reserving any record.
func (a *Allocator) CanReserveBatch(required int64) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if required < 0 || a.offset > a.maxSegmentBytes {
		return false
	}
	return required <= a.maxSegmentBytes-a.offset
}

// CanReserveRecords reports whether count more records fit under the
// per-segment record limit. It complements CanReserveBatch's byte check.
func (a *Allocator) CanReserveRecords(count int) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if count < 0 || a.recordCount > a.maxRecords {
		return false
	}
	return uint64(count) <= a.maxRecords-a.recordCount
}

// ReserveCommit reserves bytes for a BatchCommit at the current tail
// without counting a record. Returns the offset where the BatchCommit
// must be written.
func (a *Allocator) ReserveCommit(commitLen uint32) (int64, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.offset+int64(commitLen) > a.maxSegmentBytes {
		return 0, ErrSegmentFull
	}
	off := a.offset
	a.offset += int64(commitLen)
	return off, nil
}

// CurrentTail returns the current append offset (start of the next
// write).
func (a *Allocator) CurrentTail() (int64, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.offset+int64(journal.BatchCommitSize) > a.maxSegmentBytes {
		return 0, ErrSegmentFull
	}
	return a.offset, nil
}

// Consume advances the tail by n bytes (used after an append that
// bypassed Reserve, e.g. tombstone BatchCommit).
func (a *Allocator) Consume(n int64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.offset += n
}

// RecordCommit records a committed BatchCommit sequence for the footer.
func (a *Allocator) RecordCommit(seq uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.lastCommitSeq = seq
}

// State returns the current allocator state for checkpoint safe-offset
// persistence (§7.3).
func (a *Allocator) State() AllocatorState {
	a.mu.Lock()
	defer a.mu.Unlock()
	return AllocatorState{
		SegmentID:      a.segmentID,
		NextOffset:     a.offset,
		PayloadBytes:   a.payloadBytes,
		RecordCount:    a.recordCount,
		MinExtent:      a.minExtent,
		MaxExtent:      a.maxExtent,
		HasRecord:      a.hasRecord,
		CreatedAt:      a.createdAt,
		Class:          a.class,
		LastCommitSeq:  a.lastCommitSeq,
	}
}

// AllocatorState is a point-in-time snapshot persisted in a checkpoint.
type AllocatorState struct {
	SegmentID     storage.SegmentID
	NextOffset    int64
	PayloadBytes  int64
	RecordCount   uint64
	MinExtent     storage.ExtentID
	MaxExtent     storage.ExtentID
	HasRecord     bool
	CreatedAt     int64
	Class         storage.SegmentClass
	LastCommitSeq uint64
}

// ErrSegmentFull is returned when a segment has no room for a record
// and must be sealed first.
var ErrSegmentFull = storage.ErrSegmentFull
