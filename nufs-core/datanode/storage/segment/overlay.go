package segment

import (
	"encoding/binary"
	"sync"

	"github.com/example/dfs/datanode/storage"
	"github.com/example/dfs/datanode/storage/index"
)

// Overlay is the bounded in-memory committed-delta index (V2.1 §6.4).
// It is the read authority for freshly-committed extents before the
// async Pebble apply catches up: reads consult this overlay before the
// derived index. It holds only the delta since the last flush, so it is
// bounded by the flush-recovery budget, never by total stored extents.
type Overlay struct {
	mu      sync.RWMutex
	entries map[string]index.Value
}

// NewOverlay returns an empty overlay.
func NewOverlay() *Overlay {
	return &Overlay{entries: make(map[string]index.Value)}
}

// Put records a committed extent location in the overlay.
func (o *Overlay) Put(key []byte, v index.Value) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.entries[string(key)] = v
}

// Get returns the committed location if present.
func (o *Overlay) Get(key []byte) (index.Value, bool) {
	o.mu.RLock()
	defer o.mu.RUnlock()
	v, ok := o.entries[string(key)]
	return v, ok
}

// Delete removes an entry (after a generation-fenced delete commits).
func (o *Overlay) Delete(key []byte) {
	o.mu.Lock()
	defer o.mu.Unlock()
	delete(o.entries, string(key))
}

// Drain returns all entries and clears the overlay. Called when the
// async apply flushes them into Pebble, so the overlay stays bounded.
func (o *Overlay) Drain() []index.Mutation {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make([]index.Mutation, 0, len(o.entries))
	for k, v := range o.entries {
		kb := []byte(k)
		id := storage.ExtentID(binary.BigEndian.Uint64(kb[0:8]))
		gen := storage.Generation(binary.BigEndian.Uint64(kb[8:16]))
		out = append(out, index.Mutation{ExtentID: id, Generation: gen, Value: v})
	}
	o.entries = make(map[string]index.Value)
	return out
}

// Len returns the current overlay size.
func (o *Overlay) Len() int {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return len(o.entries)
}
