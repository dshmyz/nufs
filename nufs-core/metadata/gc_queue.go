package metadata

import (
	"encoding/binary"
	"fmt"
	"time"
)

// GC uses time-bucketed batch queues (V2.1 §13.3):
//
//	/gc-bucket/{hour}/{logical_shard}/{batch_id}
//
// Each batch contains a compressed, checksummed list of extent ID /
// generation pairs and a cursor. Workers issue and acknowledge
// tombstones in pages of 512-4096 entries. GC cost is proportional to
// expired batches, not total inodes.

// GCRetention is the retention policy for different deletion causes
// (§13.3).
const (
	// GCRetentionOverwriteDelete: 24 hours for overwrite/delete.
	GCRetentionOverwriteDelete = 24 * time.Hour
	// GCRetentionAbandonedWrite: 1 hour for abandoned writes.
	GCRetentionAbandonedWrite = time.Hour
	// GCRetentionFailedRepair: 6 hours for failed-repair temp copies.
	GCRetentionFailedRepair = 6 * time.Hour
	// GCBatchPageSize is the tombstone page size (§13.3: 512-4096).
	GCBatchPageSize = 512
)

// GCBatch is one expired-extent batch in a time bucket.
type GCBatch struct {
	// BatchID uniquely identifies the batch.
	BatchID uint64 `json:"batch_id"`
	// DeleteAfter is the unix time (ns) after which the batch is
	// eligible for tombstoning.
	DeleteAfter int64 `json:"delete_after"`
	// LogicalShard is the owning logical partition (§11.4).
	LogicalShard uint16 `json:"logical_shard"`
	// Hour is the time bucket hour.
	Hour int64 `json:"hour"`
	// Extents is the compressed, checksummed list of extent/gen pairs.
	Extents []GCExtent `json:"extents"`
	// Cursor is the progress within the batch.
	Cursor uint32 `json:"cursor"`
	// State tracks lifecycle.
	State GCBatchState `json:"state"`
}

// GCExtent is one extent/generation pair scheduled for GC.
type GCExtent struct {
	ExtentID   uint64 `json:"extent_id"`
	Generation uint64 `json:"generation"`
}

// GCBatchState is the GC batch lifecycle (§13.3: verifies references,
// marks deleting, sends tombstones, waits for quorum, marks deleted).
type GCBatchState uint8

const (
	GCBatching GCBatchState = iota
	GCDeleting
	GCDeleted
)

func (s GCBatchState) String() string {
	switch s {
	case GCBatching:
		return "batching"
	case GCDeleting:
		return "deleting"
	case GCDeleted:
		return "deleted"
	default:
		return "unknown"
	}
}

// GCQueue persists GC batches.
type GCQueue struct {
	store *PebbleStore
}

// NewGCQueue creates the GC queue.
func NewGCQueue(store *PebbleStore) *GCQueue {
	return &GCQueue{store: store}
}

// gcKey formats a batch key under its hour bucket.
func gcKey(hour int64, shard uint16, batchID uint64) string {
	return fmt.Sprintf("%s%020d/%04x/%020d", prefixGCBucket, hour, shard, batchID)
}

// HourBucket returns the hour bucket for a time.
func HourBucket(now time.Time) int64 {
	return now.Truncate(time.Hour).UnixNano()
}

// Enqueue adds extents to a batch in the appropriate hour bucket, with
// the given retention.
func (s *GCQueue) Enqueue(now time.Time, shard uint16, batchID uint64, retention time.Duration, extents []GCExtent) error {
	hour := HourBucket(now)
	b := &GCBatch{
		BatchID:      batchID,
		DeleteAfter:  now.Add(retention).UnixNano(),
		LogicalShard: shard,
		Hour:         hour,
		Extents:      extents,
		State:        GCBatching,
	}
	return s.store.putMsgpack(gcKey(hour, shard, batchID), b)
}

// Get reads a batch.
func (s *GCQueue) Get(hour int64, shard uint16, batchID uint64) (*GCBatch, error) {
	var b GCBatch
	exists, err := s.store.getValue(gcKey(hour, shard, batchID), &b)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, nil
	}
	return &b, nil
}

// Put writes a batch (progress updates).
func (s *GCQueue) Put(b *GCBatch) error {
	return s.store.putMsgpack(gcKey(b.Hour, b.LogicalShard, b.BatchID), b)
}

// ExpiredBatches returns batches whose DeleteAfter has passed, in page
// order (cost proportional to expired batches, §13.3).
func (s *GCQueue) ExpiredBatches(now time.Time, page int, pageSize int) ([]GCBatch, error) {
	var out []GCBatch
	prefix := []byte(prefixGCBucket)
	iter, err := s.store.db.NewIter(nil)
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	iter.SeekGE(prefix)
	skip := page * pageSize
	count := 0
	for ; iter.Valid(); iter.Next() {
		if !hasPrefix(string(iter.Key()), prefixGCBucket) {
			break
		}
		var b GCBatch
		if err := unmarshalValue(iter.Value(), &b); err != nil {
			return nil, err
		}
		if b.DeleteAfter > now.UnixNano() || b.State == GCDeleted {
			continue
		}
		if skip > 0 {
			skip--
			continue
		}
		if count >= pageSize {
			break
		}
		out = append(out, b)
		count++
	}
	return out, nil
}

// NextPage returns the next page of extents to tombstone (512 entries,
// §13.3) and advances the cursor. Returns the page and whether more
// remain.
func (s *GCQueue) NextPage(b *GCBatch) ([]GCExtent, bool, error) {
	start := b.Cursor
	end := start + GCBatchPageSize
	if end > uint32(len(b.Extents)) {
		end = uint32(len(b.Extents))
	}
	page := b.Extents[start:end]
	b.Cursor = end
	more := end < uint32(len(b.Extents))
	if err := s.Put(b); err != nil {
		return nil, false, err
	}
	return page, more, nil
}

// MarkDeleting marks the batch as issuing tombstones.
func (s *GCQueue) MarkDeleting(b *GCBatch) error {
	b.State = GCDeleting
	return s.Put(b)
}

// MarkDeleted marks the batch deleted after quorum acknowledgement.
func (s *GCQueue) MarkDeleted(b *GCBatch) error {
	b.State = GCDeleted
	return s.Put(b)
}

var _ = binary.BigEndian
