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
	// draining holds entries handed to an in-progress flush. They are no
	// longer part of the unflushed delta but are still read authority until
	// the flush confirms they are durable in Pebble, so no reader can observe
	// a committed extent as absent from both the overlay and the index.
	draining map[string]index.Value
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

// Get returns the committed location if present. Entries staged for an
// in-flight flush are still visible: they are durable in the segment log and
// not yet guaranteed present in Pebble, so dropping them from the read path
// would surface an acknowledged extent as ErrExtentNotFound.
func (o *Overlay) Get(key []byte) (index.Value, bool) {
	o.mu.RLock()
	defer o.mu.RUnlock()
	if v, ok := o.entries[string(key)]; ok {
		return v, true
	}
	v, ok := o.draining[string(key)]
	return v, ok
}

// Delete removes an entry (after a generation-fenced delete commits).
func (o *Overlay) Delete(key []byte) {
	o.mu.Lock()
	defer o.mu.Unlock()
	delete(o.entries, string(key))
	delete(o.draining, string(key))
}

// Drain moves all entries into the draining set and returns them. Drained
// entries remain readable via Get until DiscardDrained (flush succeeded, Pebble
// now owns them) or RestoreDrained (flush failed) resolves them, so the overlay
// stays bounded without ever opening a window where a committed extent is
// invisible in both the overlay and the index.
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
	// Merge into any previously-drained set rather than replacing it, so a
	// flush that fails and is retried cannot drop the earlier staged entries.
	if o.draining == nil {
		o.draining = make(map[string]index.Value, len(o.entries))
	}
	for k, v := range o.entries {
		o.draining[k] = v
	}
	o.entries = make(map[string]index.Value)
	return out
}

// DiscardDrained releases the staged set after a flush made those mutations
// durable in Pebble. Keys re-published into entries by a commit that raced the
// flush are left untouched: entries always wins in Get.
func (o *Overlay) DiscardDrained() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.draining = nil
}

// RestoreDrained moves the staged set back into the live delta after a failed
// flush, so the next flush retries exactly the same mutations.
func (o *Overlay) RestoreDrained() {
	o.mu.Lock()
	defer o.mu.Unlock()
	for k, v := range o.draining {
		// A commit that landed after the drain is newer; do not clobber it.
		if _, ok := o.entries[k]; !ok {
			o.entries[k] = v
		}
	}
	o.draining = nil
}

// Len returns the current unflushed delta size. Entries staged for an in-flight
// flush are excluded: they are accounted against that flush, not the delta.
func (o *Overlay) Len() int {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return len(o.entries)
}
