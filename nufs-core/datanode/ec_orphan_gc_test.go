package datanode

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/example/dfs/metadata"
)

// Program 7: the production *metadata.HTTPClient must structurally satisfy both
// EC resolver seams so runDataNodeV21 can wire the real (remote-authority)
// metaStore as the healer's landing + orphan resolver over the metadata ops
// HTTP RPCs. These are compile-time proofs of the interface contracts.
var (
	_ ECLandingResolver = (*metadata.HTTPClient)(nil)
	_ ECOrphanResolver  = (*metadata.HTTPClient)(nil)
)

// This file is Program 6 Phase F4: the stripe-orphan GC pass. When a conversion
// fails, RollbackConversion leaves partial shards on the node's shard stores
// that no live chunk references (§14). ECSelfHealer.ReclaimOrphans, given an
// orphan resolver (the authoritative *metadata.ECStore.IsChunkShardsOrphaned),
// deletes those shards via DeleteShard — permanently freeing the space — while
// a live Completed stripe's shards are left completely untouched.

// buildOrphanAuthority returns a metadata ECStore whose IsChunkShardsOrphaned
// the healer can use as its orphan resolver. It persists the given stripes so
// the orphan decision is driven off authoritative durable metadata.
func buildOrphanAuthority(t *testing.T, stripes ...*metadata.ECStripe) *metadata.ECStore {
	t.Helper()
	pb, err := metadata.NewPebbleStore(metadata.PebbleStoreConfig{
		Dir: t.TempDir(), UseInMemory: true, UseBucketStats: false,
	})
	if err != nil {
		t.Fatalf("NewPebbleStore: %v", err)
	}
	t.Cleanup(func() { _ = pb.Close() })
	auth := metadata.NewECStore(pb)
	for _, st := range stripes {
		if err := auth.PutStripe(st); err != nil {
			t.Fatalf("PutStripe(%s): %v", st.StripeID, err)
		}
	}
	return auth
}

// TestECSelfHeal_ReclaimsRolledBackOrphans writes a full 6+3 stripe, then marks
// its governing stripe rolled back and aged (a failed conversion leaves these
// partial shards orphaned, §14). ReclaimOrphans reclaims every one of the nine
// shards; the chunk reads neither as a full stripe nor via any individual
// shard. A no-op re-scan reclaims nothing further (idempotent).
func TestECSelfHeal_ReclaimsRolledBackOrphans(t *testing.T) {
	v, _ := newTestShardMultiStore(t, 3)
	cid := metadata.ChunkID(40001)
	payload := bytes.Repeat([]byte("orphan-6+3-"), 700)
	placement := []int{0, 1, 2, 0, 1, 2, 0, 1, 2}
	if err := v.WriteChunkEC(cid, payload, placement); err != nil {
		t.Fatalf("WriteChunkEC: %v", err)
	}

	// The governing stripe was rolled back (conversion failed) and has aged
	// past the reclamation gate.
	rolledBack := &metadata.ECStripe{
		StripeID:     "stripe-orphan-40001",
		ExtentID:     uint64(cid),
		Generation:   1,
		State:        metadata.ECConversionRolledBack,
		RolledBackAt: time.Now().Add(-48 * time.Hour).UnixNano(),
	}
	auth := buildOrphanAuthority(t, rolledBack)

	h := NewECSelfHealer(v, nil, ECSelfHealConfig{})
	h.SetOrphanResolver(auth, 24*time.Hour)
	h.ReclaimOrphans(context.Background())

	// All nine shards reclaimed.
	if got := h.Reclaimed(); got != 9 {
		t.Fatalf("reclaimed = %d, want 9", got)
	}
	for idx := 0; idx < 9; idx++ {
		if _, _, err := v.ReadShard(cid, idx); err == nil {
			t.Fatalf("shard %d should have been reclaimed", idx)
		}
	}
	// Idempotent: a second pass reclaims nothing.
	h.ReclaimOrphans(context.Background())
	if got := h.Reclaimed(); got != 9 {
		t.Fatalf("reclaimed after re-scan = %d, want 9 (idempotent)", got)
	}
}

// TestECSelfHeal_LeavesLiveStripeAndYoungRollbackAlone pins the two negative
// cases: a Completed stripe's shards are live (never reclaimed), and a
// rolled-back-but-young stripe's shards are not yet reclaimable.
func TestECSelfHeal_LeavesLiveStripeAndYoungRollbackAlone(t *testing.T) {
	v, _ := newTestShardMultiStore(t, 3)

	// (1) Live completed stripe: shards stay.
	liveID := metadata.ChunkID(40002)
	payload := bytes.Repeat([]byte("live-stripe-"), 600)
	placement := []int{0, 1, 2, 0, 1, 2, 0, 1, 2}
	if err := v.WriteChunkEC(liveID, payload, placement); err != nil {
		t.Fatalf("WriteChunkEC live: %v", err)
	}
	live := &metadata.ECStripe{
		StripeID:   "stripe-live-40002",
		ExtentID:   uint64(liveID),
		Generation: 1,
		State:      metadata.ECConversionComplete,
	}

	// (2) Rolled back but too young: shards must be preserved (retry/salvage
	// window still open).
	youngID := metadata.ChunkID(40003)
	if err := v.WriteChunkEC(youngID, bytes.Repeat([]byte("young-6+3-"), 500), placement); err != nil {
		t.Fatalf("WriteChunkEC young: %v", err)
	}
	young := &metadata.ECStripe{
		StripeID:     "stripe-young-40003",
		ExtentID:     uint64(youngID),
		Generation:   1,
		State:        metadata.ECConversionRolledBack,
		RolledBackAt: time.Now().UnixNano(), // fresh roll back
	}

	auth := buildOrphanAuthority(t, live, young)
	h := NewECSelfHealer(v, nil, ECSelfHealConfig{})
	h.SetOrphanResolver(auth, 24*time.Hour)
	h.ReclaimOrphans(context.Background())

	if got := h.Reclaimed(); got != 0 {
		t.Fatalf("reclaimed = %d, want 0 (no orphans eligible)", got)
	}
	// Both stripes read back byte-exact — nothing was touched.
	data, _, err := v.ReadChunkEC(liveID, len(payload))
	if err != nil || !bytes.Equal(data, payload) {
		t.Fatalf("live stripe read = %v, %v", len(data), err)
	}
	youngPayload := bytes.Repeat([]byte("young-6+3-"), 500)
	data, _, err = v.ReadChunkEC(youngID, len(youngPayload))
	if err != nil || !bytes.Equal(data, youngPayload) {
		t.Fatalf("young stripe read = %v, %v", len(data), err)
	}
}

// TestECSelfHeal_EnumerationSkipsOrphanedChunk ensures a chunk whose shards
// are orphans is not repaired/re-referenced on the repair sweep: Enumerate must
// not run repair (or re-write shards) on it, leaving GC as sole owner.
func TestECSelfHeal_EnumerationSkipsOrphanedChunk(t *testing.T) {
	v, _ := newTestShardMultiStore(t, 3)
	cid := metadata.ChunkID(40004)
	payload := bytes.Repeat([]byte("orphan-skip-"), 800)
	placement := []int{0, 1, 2, 0, 1, 2, 0, 1, 2}
	if err := v.WriteChunkEC(cid, payload, placement); err != nil {
		t.Fatalf("WriteChunkEC: %v", err)
	}
	// Orphan it with a resolver: rolled back + aged.
	rolledBack := &metadata.ECStripe{
		StripeID:     "stripe-orphan-40004",
		ExtentID:     uint64(cid),
		Generation:   1,
		State:        metadata.ECConversionRolledBack,
		RolledBackAt: time.Now().Add(-48 * time.Hour).UnixNano(),
	}
	auth := buildOrphanAuthority(t, rolledBack)

	h := NewECSelfHealer(v, nil, ECSelfHealConfig{})
	h.SetOrphanResolver(auth, 24*time.Hour)
	h.Enumerate(context.Background())

	// Enumerate counts the chunk as scanned (discovery sees it) but must not
	// repair it — with no resolver, repair would have been skipped anyway, but
	// the orphan guard prevents the chunk from being treated as degraded. The
	// shards remain present because GC is the owner, and repair did not touch
	// them.
	for idx := 0; idx < 9; idx++ {
		if _, _, err := v.ReadShard(cid, idx); err != nil {
			t.Fatalf("shard %d should remain (untouched by Enumerate)", idx)
		}
	}
}
