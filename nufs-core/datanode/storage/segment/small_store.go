package segment

import (
	"context"

	"github.com/example/dfs/datanode/storage"
	"github.com/example/dfs/datanode/storage/encryption"
)

// SmallStore is the small-file commit stream (V2.1 §5.1/§9): 1 GiB
// segments holding records ≤ 64 KiB, one record per logical file. It is
// a Store with StreamID=0 and the small-segment size, enforcing the
// ≤ SmallFileThreshold record bound at the write boundary.
type SmallStore struct {
	*Store
}

// NewSmallStore opens the small-file commit stream on a disk.
func NewSmallStore(cfg Config) (*SmallStore, error) {
	if cfg.SegmentSize == 0 {
		cfg.SegmentSize = storage.DefaultSmallSegmentSize
	}
	if cfg.StreamID != 0 {
		cfg.StreamID = 0
	}
	st, err := New(cfg)
	if err != nil {
		return nil, err
	}
	return &SmallStore{Store: st}, nil
}

// WriteSmallFile writes a logical file (≤ SmallFileThreshold) as one
// record in the small segment, returning the durable receipt.
func (s *SmallStore) WriteSmallFile(req *storage.WriteRequest) (*storage.DurableReceipt, error) {
	if len(req.Data) > storage.SmallFileThreshold {
		return nil, storage.ErrCapacity // small stream rejects oversized records
	}
	// The small stream uses sampled compression in the 4-64 KiB band
	// (§9) — handled by the shared Write path.
	return s.Store.Write(context.Background(), req)
}

// Enc returns the store's encryption registry (nil when plaintext).
func (s *SmallStore) Enc() *encryption.KeyRegistry { return s.Store.enc }
