package metadata

import (
	"context"
	"errors"
	"time"
)

// This file is Program 6 Phase F4: stripe-orphan discovery for GC (§14). A
// failed conversion transaction (RollbackConversion) leaves partial shards
// scattered on datanode shard stores that no live chunk references — the
// metadata still points at the three replicas. These shards are reclaimable
// orphans, but they are not reclaimed immediately: a rolled-back stripe may
// briefly still be mid-repair (its partial shards re-referenced as the repair
// rebuilds from survivors), so reclamation defers until the stripe has been
// rolled back for a configured age. This file exposes the authoritative
// decision — "are this chunk's shards orphans?" — to the datanode-side GC
// pass (ECSelfHealer.ReclaimOrphans), which owns the actual DeleteShard.

// ListStripes returns every persisted EC stripe, live or rolled back. It is
// the enumeration primitive behind orphan discovery: the caller filters by
// state/age to decide what shard stores may reclaim.
func (s *ECStore) ListStripes() ([]*ECStripe, error) {
	var stripes []*ECStripe
	err := s.store.scanPrefix("ec-stripe/", func(_ []byte, value []byte) error {
		var st ECStripe
		if err := unmarshalValue(value, &st); err != nil {
			return err
		}
		stripe := st
		stripes = append(stripes, &stripe)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return stripes, nil
}

// ListOrphanStripes returns the stripes whose shards are eligible for
// reclamation: those rolled back at least `olderThan` ago. The age gate
// protects a stripe still mid-repair or just rolled back from being collected
// while an operator might still salvage it. A stripe is only ever orphaned in
// the rolled-back state — a Completed stripe is exclusively owned by a live
// chunk layout (SwitchChunkToEC), so it is never orphaned here.
func (s *ECStore) ListOrphanStripes(olderThan time.Duration) ([]*ECStripe, error) {
	if olderThan <= 0 {
		return nil, nil
	}
	cutoff := time.Now().Add(-olderThan).UnixNano()
	stripes, err := s.ListStripes()
	if err != nil {
		return nil, err
	}
	var orphans []*ECStripe
	for _, st := range stripes {
		if st.State == ECConversionRolledBack && st.RolledBackAt != 0 && st.RolledBackAt <= cutoff {
			orphans = append(orphans, st)
		}
	}
	return orphans, nil
}

// IsChunkShardsOrphaned answers whether the shards of the chunk `chunkID`
// (each stored as an extent in the dedicated shard stream) are reclaimable
// orphans. It is the authoritative decision the datanode GC pass consults per
// chunk before issuing any DeleteShard. The rules route every case:
//
//   - The chunk metadata is gone → its shards are orphans (a chunk is only
//     deleted after its layout is fully unreferenced). Unless every stripe
//     referencing that chunk ID is itself still live or too young (covered by
//     the stripe checks below), the shards are reclaimable.
//   - The stripe the chunk references is Complete → the chunk is served from
//     its 6+3 shards; they are live, not orphans.
//   - The stripe is rolled back and aged → the chunk's metadata was never
//     switched, the build failed, and its partial shards are orphans.
//   - The stripe is still in-flight (Preparing/Encoding/Syncing/Switching) or
//     rolled back but not yet aged → not reclaimable yet.
//   - No stripe reference at all → nothing authoritative says these shards
//     belong to a live layout, so they are orphans (a leaked shard store).
func (s *ECStore) IsChunkShardsOrphaned(ctx context.Context, chunkID ChunkID, olderThan time.Duration) (bool, error) {
	if olderThan <= 0 {
		return false, nil
	}
	// Resolve which stripe(s) this chunk ID could reference. The ECStripeID /
	// ECGroup.GroupID live on the chunk metadata; if the chunk is gone we can
	// only infer from stripes whose ExtentID == chunkID.
	var stripe *ECStripe
	stripes, err := s.ListStripes()
	if err != nil {
		return false, err
	}
	cutoff := time.Now().Add(-olderThan).UnixNano()
	for _, st := range stripes {
		if st.ExtentID != uint64(chunkID) {
			continue
		}
		// Prefer the chunk's own reference if it still exists; else any stripe
		// for this extent governs the shard's fate.
		if stripe == nil {
			stripe = st
		}
	}

	chunk, gerr := s.store.GetChunk(ctx, chunkID)
	if gerr != nil && !errors.Is(gerr, ErrChunkNotFound) {
		return false, gerr
	}
	if gerr == nil && chunk.ECStripeID != "" {
		// The chunk is switched to a stripe (V2.1 complete). Its shards are
		// live unless that stripe is itself rolled back.
		ref, rerr := s.GetStripe(chunk.ECStripeID)
		if rerr != nil {
			return false, rerr
		}
		if ref != nil && ref.State == ECConversionComplete {
			return false, nil
		}
	}

	// Chunk gone, or no live hit: decide from the governing stripe's state.
	if stripe == nil {
		// No stripe at all references this chunk ID → leaked shards, reclaim.
		return true, nil
	}
	if stripe.State == ECConversionComplete {
		return false, nil
	}
	if stripe.State == ECConversionRolledBack && stripe.RolledBackAt != 0 && stripe.RolledBackAt <= cutoff {
		return true, nil
	}
	// In-flight (Preparing/Encoding/Syncing/Switching) or young rolled-back →
	// not yet reclaimable.
	return false, nil
}
