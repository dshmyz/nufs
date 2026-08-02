package index

import (
	"fmt"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"

	"github.com/example/dfs/datanode/storage"
)

// Index is the local persistent extent index (§5.4): the location
// authority for where a record lives. It is a Pebble database keyed by
// (extent_id, generation) with fixed-size values.
type Index struct {
	db         *pebble.DB
	dir        string
	persistent bool
}

// Options configures the index database.
type Options struct {
	// Dir is the on-disk directory for the Pebble database.
	Dir string
	// MemTableSize overrides the default memtable size.
	MemTableSize uint64
	// UseInMemory makes the DB purely in-memory (tests).
	UseInMemory bool
}

// Open opens (creating if needed) the extent index.
func Open(opts Options) (*Index, error) {
	po := &pebble.Options{
		MemTableSize:                64 << 20,
		MemTableStopWritesThreshold: 4,
	}
	if opts.MemTableSize > 0 {
		po.MemTableSize = opts.MemTableSize
	}
	if opts.UseInMemory {
		po.FS = vfs.NewMem()
	}
	db, err := pebble.Open(opts.Dir, po)
	if err != nil {
		return nil, fmt.Errorf("storage: open index: %w", err)
	}
	return &Index{db: db, dir: opts.Dir, persistent: !opts.UseInMemory}, nil
}

// Get returns the value for an exact (extent_id, generation) key.
func (ix *Index) Get(extentID storage.ExtentID, generation storage.Generation) (*Value, error) {
	raw, closer, err := ix.db.Get(Key(extentID, generation))
	if err == pebble.ErrNotFound {
		return nil, storage.ErrExtentNotFound
	}
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	var v Value
	if err := v.Decode(raw); err != nil {
		return nil, fmt.Errorf("%w: %v", storage.ErrIndexCorrupt, err)
	}
	return &v, nil
}

// Mutation is one async apply to the derived index.
type Mutation struct {
	ExtentID   storage.ExtentID
	Generation storage.Generation
	Value      Value
}

// PutBatch applies a mutation into a batch (used by async apply).
func (ix *Index) PutBatch(batch *pebble.Batch, extentID storage.ExtentID, generation storage.Generation, v *Value) error {
	buf := make([]byte, ValueLen)
	if err := v.Encode(buf); err != nil {
		return err
	}
	return batch.Set(Key(extentID, generation), buf, nil)
}

// Put applies a mutation in the same batch that the store uses for a
// write transaction. batch is optional: when nil a single-op batch is
// created and applied immediately.
func (ix *Index) Put(batch *pebble.Batch, extentID storage.ExtentID, generation storage.Generation, v *Value) error {
	buf := make([]byte, ValueLen)
	if err := v.Encode(buf); err != nil {
		return err
	}
	if batch != nil {
		return batch.Set(Key(extentID, generation), buf, nil)
	}
	return ix.db.Set(Key(extentID, generation), buf, pebble.Sync)
}

// Delete removes a key (generation-fenced delete).
func (ix *Index) Delete(batch *pebble.Batch, extentID storage.ExtentID, generation storage.Generation) error {
	if batch != nil {
		return batch.Delete(Key(extentID, generation), nil)
	}
	return ix.db.Delete(Key(extentID, generation), pebble.Sync)
}

// LatestGeneration returns the highest generation present for an
// extent, or (0, false) if none. Used by compaction and GC to resolve
// the live generation without loading all of them.
func (ix *Index) LatestGeneration(extentID storage.ExtentID) (storage.Generation, bool) {
	// Seek to the last key with this extent prefix. Extent keys sort by
	// (extent_id, generation), so the max generation is the last key
	// whose extent_id matches.
	prefix := Key(extentID, 0)
	lower := prefix
	upper := Key(extentID+1, 0)
	iter, err := ix.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return 0, false
	}
	defer iter.Close()
	if !iter.Last() {
		return 0, false
	}
	return GenerationFromKey(iter.Key()), true
}

// NewBatch creates a write batch.
func (ix *Index) NewBatch() *pebble.Batch { return ix.db.NewBatch() }

// ApplyBatch commits a batch durably.
func (ix *Index) ApplyBatch(batch *pebble.Batch) error {
	return batch.Commit(pebble.Sync)
}

// Close closes the index database.
func (ix *Index) Close() error { return ix.db.Close() }

// DB exposes the raw database (for checkpointing).
func (ix *Index) DB() *pebble.DB { return ix.db }
