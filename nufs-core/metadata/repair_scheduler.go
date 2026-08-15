package metadata

import (
	"fmt"
	"time"
)

// Large-scale repair is a Raft-persisted batch state machine keyed by
// PG, source epoch, target epoch, and a resumable inventory cursor
// (V2.1 §13.1). Each lease processes 512-4096 extents and persists
// aggregate progress. A node failure creates at most one initial batch
// task per affected PG; workers lazily advance through inventory
// partitions instead of pre-creating partition tasks. Individual extent
// tasks are reserved for sparse checksum corruption and exceptions.

// RepairBatchState is the batch state machine lifecycle (§13.1).
type RepairBatchState uint8

const (
	RepairQueued RepairBatchState = iota
	RepairLeased
	RepairCopying
	RepairVerifying
	RepairCommitted
	RepairRetryable
	RepairPermanentFailure
)

func (s RepairBatchState) String() string {
	switch s {
	case RepairQueued:
		return "queued"
	case RepairLeased:
		return "leased"
	case RepairCopying:
		return "copying"
	case RepairVerifying:
		return "verifying"
	case RepairCommitted:
		return "committed"
	case RepairRetryable:
		return "retryable"
	case RepairPermanentFailure:
		return "permanent_failure"
	default:
		return "unknown"
	}
}

// RepairPriority is the §13.1 priority order.
type RepairPriority uint8

const (
	RepairPriorityOneReplica RepairPriority = iota
	RepairPriorityUnsafeDomain
	RepairPriorityMissingReplica
	RepairPriorityChecksum
	RepairPriorityNodeDrain
	RepairPriorityPGRebalance
	RepairPriorityTierMigration
)

// RepairBatchTask is one resumable batch repair task (§13.1).
type RepairBatchTask struct {
	// BatchID uniquely identifies the task.
	BatchID uint64 `json:"batch_id"`
	// PGID and epochs scope the task.
	PGID        uint32 `json:"pg_id"`
	SourceEpoch uint64 `json:"source_epoch"`
	TargetEpoch uint64 `json:"target_epoch"`
	// InventoryPartition is the next partition to advance through
	// (workers lazy-advance, no pre-created partition tasks).
	InventoryPartition uint32 `json:"inventory_partition"`
	// Cursor is the position within the current partition.
	Cursor uint64 `json:"cursor"`
	// State is the batch state machine.
	State RepairBatchState `json:"state"`
	// Priority is the §13.1 order.
	Priority RepairPriority `json:"priority"`
	// LeaseExpiry fences the current worker lease.
	LeaseExpiry int64 `json:"lease_expiry"`
	// Attempts counts lease retries.
	Attempts uint32 `json:"attempts"`
	// CopiedBytes tracks progress.
	CopiedBytes int64 `json:"copied_bytes"`
	// CreatedAt is when the task was created.
	CreatedAt int64 `json:"created_at"`
	// PermanentError is set on permanent failure.
	PermanentError string `json:"permanent_error,omitempty"`
}

// Batch bounds (§13.1: each lease processes 512-4096 extents).
const (
	RepairBatchMinExtents = 512
	RepairBatchMaxExtents = 4096
)

// RepairBatchStore persists batch repair tasks through Raft.
type RepairBatchStore struct {
	store *PebbleStore
}

// NewRepairBatchStore creates the batch repair store.
func NewRepairBatchStore(store *PebbleStore) *RepairBatchStore {
	return &RepairBatchStore{store: store}
}

// repairBatchKey formats a batch task key.
func repairBatchKey(batchID uint64) string {
	return fmt.Sprintf("repair-batch/%020d", batchID)
}

// Get reads a batch task.
func (s *RepairBatchStore) Get(batchID uint64) (*RepairBatchTask, error) {
	var t RepairBatchTask
	exists, err := s.store.getValue(repairBatchKey(batchID), &t)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, nil
	}
	return &t, nil
}

// Put writes a batch task.
func (s *RepairBatchStore) Put(t *RepairBatchTask) error {
	return s.store.putMsgpack(repairBatchKey(t.BatchID), t)
}

// CreateBatch creates one batch task for an affected PG (§13.1: at
// most one initial batch task per PG).
func (s *RepairBatchStore) CreateBatch(pgID uint32, sourceEpoch, targetEpoch uint64, partition uint32, priority RepairPriority, batchID uint64) (*RepairBatchTask, error) {
	t := &RepairBatchTask{
		BatchID:            batchID,
		PGID:               pgID,
		SourceEpoch:        sourceEpoch,
		TargetEpoch:        targetEpoch,
		InventoryPartition: partition,
		State:              RepairQueued,
		Priority:           priority,
		CreatedAt:          time.Now().UnixNano(),
	}
	if err := s.Put(t); err != nil {
		return nil, err
	}
	return t, nil
}

// Lease acquires a batch for a worker with a lease expiry. Returns the
// task if acquirable.
func (s *RepairBatchStore) Lease(batchID uint64, leaseDuration time.Duration) (*RepairBatchTask, error) {
	t, err := s.Get(batchID)
	if err != nil {
		return nil, err
	}
	if t == nil {
		return nil, nil
	}
	now := time.Now().UnixNano()
	if t.State == RepairCommitted || t.State == RepairPermanentFailure {
		return nil, nil
	}
	// A lease is acquirable if it is queued or the previous lease
	// expired.
	if t.State != RepairQueued && t.LeaseExpiry > now {
		return nil, nil // held by another worker
	}
	t.State = RepairLeased
	t.LeaseExpiry = now + leaseDuration.Nanoseconds()
	t.Attempts++
	if err := s.Put(t); err != nil {
		return nil, err
	}
	return t, nil
}

// Advance persists progress after a lease processed a page of extents.
// Returns the next state (Copying → Verifying → Committed).
func (s *RepairBatchStore) Advance(batchID uint64, partition uint32, cursor uint64, copiedBytes int64) error {
	t, err := s.Get(batchID)
	if err != nil {
		return err
	}
	if t == nil {
		return fmt.Errorf("metadata: repair batch %d not found", batchID)
	}
	t.InventoryPartition = partition
	t.Cursor = cursor
	t.CopiedBytes += copiedBytes
	t.State = RepairCopying
	return s.Put(t)
}

// MarkVerifying transitions to verifying.
func (s *RepairBatchStore) MarkVerifying(batchID uint64) error {
	t, err := s.Get(batchID)
	if err != nil {
		return err
	}
	if t == nil {
		return nil
	}
	t.State = RepairVerifying
	return s.Put(t)
}

// Complete transitions to committed (target epoch proven complete by
// inventory reconciliation, §11.3).
func (s *RepairBatchStore) Complete(batchID uint64) error {
	t, err := s.Get(batchID)
	if err != nil {
		return err
	}
	if t == nil {
		return nil
	}
	t.State = RepairCommitted
	return s.Put(t)
}

// Fail marks a retryable or permanent failure.
func (s *RepairBatchStore) Fail(batchID uint64, permanent bool, reason string) error {
	t, err := s.Get(batchID)
	if err != nil {
		return err
	}
	if t == nil {
		return nil
	}
	if permanent {
		t.State = RepairPermanentFailure
		t.PermanentError = reason
	} else {
		t.State = RepairRetryable
	}
	return s.Put(t)
}

// ListActive returns queued/leased batches (paginated to avoid an
// unbounded listing, §21).
func (s *RepairBatchStore) ListActive(page int, pageSize int) ([]RepairBatchTask, error) {
	var out []RepairBatchTask
	prefixStr := "repair-batch/"
	prefix := []byte(prefixStr)
	iter, err := s.store.db.NewIter(nil)
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	iter.SeekGE(prefix)
	skip := page * pageSize
	count := 0
	for ; iter.Valid(); iter.Next() {
		if !hasPrefix(string(iter.Key()), prefixStr) {
			break
		}
		if skip > 0 {
			skip--
			continue
		}
		if count >= pageSize {
			break
		}
		var t RepairBatchTask
		if err := unmarshalValue(iter.Value(), &t); err != nil {
			return nil, err
		}
		out = append(out, t)
		count++
	}
	return out, nil
}
