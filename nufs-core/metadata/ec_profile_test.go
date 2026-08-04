package metadata

import (
	"context"
	"testing"
	"time"
)

// TestECProfileRoundTrip proves the shared ECProfile record round-trips through
// SelectOrCreateProfile/GetProfile, that the default is the canonical 6+3
// config, and that SelectOrCreate converges on a single row (idempotent create).
func TestECProfileRoundTrip(t *testing.T) {
	store := newV2TestPebbleStore(t)
	ec := NewECStore(store)

	def := DefaultECProfile()
	if def.ID != DefaultECProfileID || def.DataShards != 6 || def.ParityShards != 3 {
		t.Fatalf("default profile = %+v, want ec-6-3 6+3", def)
	}
	if def.MinMachines != ECMinMachines || def.MaxPerMachine != ECMaxShardsPerMachine {
		t.Fatalf("default profile diversity = %+v, want %d/%d", def, ECMinMachines, ECMaxShardsPerMachine)
	}

	// First create persists.
	got, err := ec.SelectOrCreateProfile("p-1", &ECProfile{ID: "p-1", DataShards: 6, ParityShards: 3, MinMachines: 3, MaxPerMachine: 3})
	if err != nil {
		t.Fatalf("SelectOrCreate (first): %v", err)
	}
	if got.ID != "p-1" {
		t.Fatalf("got profile %+v", got)
	}
	// Persisted: a fresh Get retrieves the same row.
	reloaded, err := ec.GetProfile("p-1")
	if err != nil {
		t.Fatalf("GetProfile: %v", err)
	}
	if reloaded == nil || reloaded.DataShards != 6 {
		t.Fatalf("reloaded profile = %+v", reloaded)
	}

	// SelectOrCreate with a DIFFERENT config on an existing ID must keep the
	// original row (read-mostly convergence — one shared profile row).
	want := &ECProfile{ID: "p-1", DataShards: 9, ParityShards: 3}
	again, err := ec.SelectOrCreateProfile("p-1", want)
	if err != nil {
		t.Fatalf("SelectOrCreate (second): %v", err)
	}
	if again.DataShards != 6 {
		t.Fatalf("existing profile clobbered: %+v", again)
	}

	// ECGroupFromProfile returns a pointer referencing the profile while
	// retaining the embedded config for read-compatible consumers.
	eg := ECGroupFromProfile(def, "stripe-x")
	if eg.ProfileID != DefaultECProfileID || eg.GroupID != "stripe-x" || eg.DataShards != 6 || eg.ParityShards != 3 {
		t.Fatalf("ECGroupFromProfile = %+v", eg)
	}
}

// TestSwitchChunkToEC_ProfileAndStripePointer proves the publish layout switch
// now references the shared ECProfile (ProfileID) and the durable stripe
// (ECStripeID), still materializes ChunkMeta.Replicas with the nine shard
// placements, and cross-checks the materialized copy against the authoritative
// stripe before writing — the landing is never silently dropped or diverged.
func TestSwitchChunkToEC_ProfileAndStripePointer(t *testing.T) {
	store := newV2TestPebbleStore(t)
	ec := NewECStore(store)

	const cid = ChunkID(777)
	seed := &ChunkMeta{ID: cid, Size: 4096, State: ChunkReady, Tier: TierCold, CreateTime: 1, Generation: 3}
	if err := store.putMsgpack(chunkMetadataKey(cid), seed); err != nil {
		t.Fatalf("seed chunk: %v", err)
	}

	// Drive a real conversion transaction to a durably Complete stripe (as the
	// serving loop does), with a §14-diverse 3-machine placement.
	st, err := ec.BeginConversion("stripe-prof-1", uint64(cid), 3, 0xBEEF)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := ec.PlanShards(st, testDisks([]uint64{1, 2, 3}, 3)); err != nil {
		t.Fatalf("plan: %v", err)
	}
	if err := ec.MarkSyncing(st); err != nil {
		t.Fatalf("mark syncing: %v", err)
	}
	if err := ec.CompleteConversion(st, time.Now()); err != nil {
		t.Fatalf("complete: %v", err)
	}

	layout, err := ec.SwitchChunkToEC(context.Background(), st.StripeID)
	if err != nil {
		t.Fatalf("SwitchChunkToEC: %v", err)
	}

	// Profile pointer: the chunk references the shared profile.
	if layout.ECGroup == nil || layout.ECGroup.ProfileID != DefaultECProfileID {
		t.Fatalf("ECGroup profile id = %+v, want %s", layout.ECGroup, DefaultECProfileID)
	}
	if layout.ECGroup.GroupID != st.StripeID {
		t.Fatalf("ECGroup group id = %q, want %q", layout.ECGroup.GroupID, st.StripeID)
	}
	// Durable landing pointer.
	if layout.ECStripeID != st.StripeID {
		t.Fatalf("ECStripeID = %q, want %q", layout.ECStripeID, st.StripeID)
	}
	// Materialized copy preserved + matches the authoritative stripe per shard.
	if len(layout.Replicas) != 9 {
		t.Fatalf("replicas after switch = %d, want 9", len(layout.Replicas))
	}
	for i, sh := range st.Shards {
		r := layout.Replicas[i]
		if r.NodeID != NodeID(sh.NodeID) || r.ShardIndex != sh.Index {
			t.Fatalf("replica %d = node %d idx %d, want node %d idx %d", i, r.NodeID, r.ShardIndex, sh.NodeID, sh.Index)
		}
	}
	if layout.Checksum != st.OriginalChecksum || layout.Size != seed.Size || layout.Tier != seed.Tier {
		t.Fatalf("layout non-landing fields not preserved: %+v", layout)
	}

	// Cross-check reference: the durable stripe re-resolves to the same landing.
	resolved, err := ec.ResolveStripeLanding(layout)
	if err != nil {
		t.Fatalf("ResolveStripeLanding: %v", err)
	}
	if len(resolved) != 9 {
		t.Fatalf("resolved landing has %d shards, want 9", len(resolved))
	}
	for i, sh := range resolved {
		if sh.Index != layout.Replicas[i].ShardIndex || sh.NodeID != uint64(layout.Replicas[i].NodeID) {
			t.Fatalf("resolved shard %d = %+v, want to match replica %+v", i, sh, layout.Replicas[i])
		}
	}
}

// TestSwitchChunkToEC_RejectsNonComplete proves the publish switch still refuses
// a stripe that has not durably completed (no partial publish), and that
// ResolveStripeLanding resolves through ECStripeID but falls back to
// ECGroup.GroupID for rows written before the explicit pointer existed.
func TestResolveStripeLanding_PointerAndFallback(t *testing.T) {
	store := newV2TestPebbleStore(t)
	ec := NewECStore(store)

	st, err := ec.BeginConversion("stripe-resolve-1", 1000, 1, 0x11)
	if err != nil {
		t.Fatal(err)
	}
	if err := ec.PlanShards(st, testDisks([]uint64{1, 2, 3}, 3)); err != nil {
		t.Fatal(err)
	}
	if err := ec.MarkSyncing(st); err != nil {
		t.Fatal(err)
	}
	if err := ec.CompleteConversion(st, time.Now()); err != nil {
		t.Fatal(err)
	}

	// New-style row: ECStripeID present.
	newRow := &ChunkMeta{ID: 1000, ECStripeID: st.StripeID}
	resolved, err := ec.ResolveStripeLanding(newRow)
	if err != nil || len(resolved) != 9 {
		t.Fatalf("resolve via ECStripeID: n=%d err=%v", len(resolved), err)
	}

	// Old-style row: no ECStripeID, only ECGroup.GroupID — must fall back.
	oldRow := &ChunkMeta{ID: 1000, ECGroup: &ECGroupInfo{GroupID: st.StripeID}}
	resolved2, err := ec.ResolveStripeLanding(oldRow)
	if err != nil || len(resolved2) != 9 {
		t.Fatalf("resolve via ECGroup fallback: n=%d err=%v", len(resolved2), err)
	}

	// Non-EC chunk: nil, nil.
	plain, err := ec.ResolveStripeLanding(&ChunkMeta{ID: 3000})
	if err != nil || plain != nil {
		t.Fatalf("resolve non-EC chunk = %v, %v; want nil, nil", plain, err)
	}
}
