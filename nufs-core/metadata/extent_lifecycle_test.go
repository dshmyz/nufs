package metadata

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

// ========== Roadmap §1.4: heartbeat degrade → extent Lifecycle ==========

// newExtentDegradeFixture seeds a sealed ChunkReady chunk, mirroring
// newDegradeFixture (pebble_store_test.go) but keeping the file inode so the
// test can promote the file to a V2 extent layout. layout is "inline",
// "pages", or "" (V1 ChunkMap, no promotion). Returns the store, file inode,
// and chunk.
func newExtentDegradeFixture(t *testing.T, rf int, layout string) (*PebbleStore, InodeID, *ChunkMeta) {
	t.Helper()
	store := newTestPebbleStore(t)
	ctx := context.Background()

	if err := store.CreateBucket(ctx, "deg-extent", PlacementPolicy{
		ID: "default", ReplicationFactor: rf, TopologySpread: SpreadRack,
	}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	bucket, err := store.GetBucket(ctx, "deg-extent")
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}
	f, err := store.CreateFile(ctx, bucket.RootInode, "d.bin", 0644)
	if err != nil {
		t.Fatalf("CreateFile: %v", err)
	}

	for i := NodeID(1); i <= NodeID(rf+1); i++ {
		if err := store.RegisterNode(ctx, &NodeInfo{
			ID: i, Addr: fmt.Sprintf("n%d:9100", i),
			Rack: fmt.Sprintf("r%d", i), Zone: "z1", Tier: TierHot,
		}); err != nil {
			t.Fatalf("RegisterNode: %v", err)
		}
	}

	chunk, err := store.AllocateChunk(ctx, f.ID, 0, PlacementPolicy{
		ReplicationFactor: rf, TopologySpread: SpreadRack,
	})
	if err != nil {
		t.Fatalf("AllocateChunk: %v", err)
	}
	if err := store.CommitChunk(ctx, chunk.ID, 0xABCDEF); err != nil {
		t.Fatalf("CommitChunk: %v", err)
	}
	if err := store.SealChunk(ctx, chunk.ID); err != nil {
		t.Fatalf("SealChunk: %v", err)
	}

	// Promote the file to the requested V2 layout. The extent ID mirrors the
	// chunk ID (extent==chunk-ID invariant), so the /extent-meta row is
	// co-located with the chunk row on the same store.
	ext := &ExtentMetaV2{ID: ExtentIDV2(chunk.ID), Generation: 1, LogicalLen: 4096}
	switch layout {
	case "inline":
		if err := store.SetInlineExtent(ctx, f.ID, ext, 4096); err != nil {
			t.Fatalf("SetInlineExtent: %v", err)
		}
	case "pages":
		if err := store.ReplaceExtents(ctx, f.ID, []ExtentWrite{{Extent: ext, Offset: 0}}, 4096); err != nil {
			t.Fatalf("ReplaceExtents: %v", err)
		}
	case "":
		// V1 ChunkMap layout stays; nothing to promote.
	default:
		t.Fatalf("unknown layout %q", layout)
	}
	return store, f.ID, chunk
}

// TestReportChunkState_DegradesExtentLifecycle verifies roadmap §1.4: when a
// heartbeat report fails a replica of a chunk that backs a V2 extent (inline
// or pages), the extent's Lifecycle mirrors the chunk's ChunkReady→ChunkDegraded
// transition Ready→ReadyDegraded. Without the hook in batchUpdateChunkStatesCtx
// the extent stays LifecycleReady and this test fails.
func TestReportChunkState_DegradesExtentLifecycle(t *testing.T) {
	for _, layout := range []string{"inline", "pages"} {
		t.Run(layout, func(t *testing.T) {
			const rf = 2
			store, _, chunk := newExtentDegradeFixture(t, rf, layout)
			ctx := context.Background()

			// Sanity: the V2 extent starts LifecycleReady.
			before, err := store.GetExtentMeta(ctx, ExtentIDV2(chunk.ID))
			if err != nil {
				t.Fatalf("GetExtentMeta before: %v", err)
			}
			if before.Lifecycle != LifecycleReady {
				t.Fatalf("extent %d lifecycle before = %d, want LifecycleReady", chunk.ID, before.Lifecycle)
			}

			// Degrade one replica → chunk degrades and the extent mirrors it.
			ready, err := store.GetChunk(ctx, chunk.ID)
			if err != nil {
				t.Fatalf("GetChunk: %v", err)
			}
			if ready.State != ChunkReady {
				t.Fatalf("fixture: chunk state = %d, want ChunkReady", ready.State)
			}
			failing := ready.Replicas[0].NodeID
			if err := store.ReportChunkState(ctx, failing, map[ChunkID]ReplicaState{chunk.ID: ReplicaFailed}); err != nil {
				t.Fatalf("ReportChunkState: %v", err)
			}

			deg, err := store.GetChunk(ctx, chunk.ID)
			if err != nil {
				t.Fatalf("GetChunk after: %v", err)
			}
			if deg.State != ChunkDegraded {
				t.Fatalf("chunk state = %d, want ChunkDegraded", deg.State)
			}
			after, err := store.GetExtentMeta(ctx, ExtentIDV2(chunk.ID))
			if err != nil {
				t.Fatalf("GetExtentMeta after: %v", err)
			}
			if after.Lifecycle != LifecycleReadyDegraded {
				t.Fatalf("extent lifecycle = %d, want LifecycleReadyDegraded", after.Lifecycle)
			}

			// Repeated failure is idempotent: chunk stays Degraded, the extent
			// mirror is a no-op, no error.
			if err := store.ReportChunkState(ctx, failing, map[ChunkID]ReplicaState{chunk.ID: ReplicaFailed}); err != nil {
				t.Fatalf("ReportChunkState repeat: %v", err)
			}
			idem, err := store.GetExtentMeta(ctx, ExtentIDV2(chunk.ID))
			if err != nil {
				t.Fatalf("GetExtentMeta idempotent: %v", err)
			}
			if idem.Lifecycle != LifecycleReadyDegraded {
				t.Fatalf("extent lifecycle after repeat = %d, want LifecycleReadyDegraded", idem.Lifecycle)
			}
		})
	}
}

// TestReportChunkState_DegradeSkipsV1 verifies the hook does not fabricate
// /extent-meta rows for V1 ChunkMap chunks: the chunk degrades, but no extent
// row appears (GetExtentMeta → ErrExtentNotFound).
func TestReportChunkState_DegradeSkipsV1(t *testing.T) {
	const rf = 2
	store, _, chunk := newExtentDegradeFixture(t, rf, "")
	ctx := context.Background()

	ready, err := store.GetChunk(ctx, chunk.ID)
	if err != nil {
		t.Fatalf("GetChunk: %v", err)
	}
	failing := ready.Replicas[0].NodeID
	if err := store.ReportChunkState(ctx, failing, map[ChunkID]ReplicaState{chunk.ID: ReplicaFailed}); err != nil {
		t.Fatalf("ReportChunkState: %v", err)
	}

	deg, err := store.GetChunk(ctx, chunk.ID)
	if err != nil {
		t.Fatalf("GetChunk after: %v", err)
	}
	if deg.State != ChunkDegraded {
		t.Fatalf("chunk state = %d, want ChunkDegraded", deg.State)
	}
	if _, err := store.GetExtentMeta(ctx, ExtentIDV2(chunk.ID)); !errors.Is(err, ErrExtentNotFound) {
		t.Fatalf("GetExtentMeta = %v, want ErrExtentNotFound (no fabricated V2 row)", err)
	}
}

// TestMarkExtentDegraded_Monotonic covers the row-level guard: only
// LifecycleReady extents degrade; EC-converting and missing rows are untouched,
// and the transition is idempotent.
func TestMarkExtentDegraded_Monotonic(t *testing.T) {
	store := newV2TestPebbleStore(t)
	ctx := context.Background()

	// A row pre-set to ECConverting must not be clobbered by the degrade hook.
	converting := &ExtentMetaV2{ID: 9001, Generation: 1, LogicalLen: 4096, Lifecycle: LifecycleECConverting}
	if err := store.putExtentMeta(converting); err != nil {
		t.Fatalf("putExtentMeta: %v", err)
	}
	if err := store.MarkExtentDegraded(ctx, 9001); err != nil {
		t.Fatalf("MarkExtentDegraded on converting: %v", err)
	}
	got, err := store.GetExtentMeta(ctx, 9001)
	if err != nil {
		t.Fatalf("GetExtentMeta: %v", err)
	}
	if got.Lifecycle != LifecycleECConverting {
		t.Fatalf("lifecycle = %d, want LifecycleECConverting preserved", got.Lifecycle)
	}

	// Missing extent-meta (V1 chunk) → no-op, no error, no row created.
	if err := store.MarkExtentDegraded(ctx, 9002); err != nil {
		t.Fatalf("MarkExtentDegraded on missing: %v", err)
	}
	if _, err := store.GetExtentMeta(ctx, 9002); !errors.Is(err, ErrExtentNotFound) {
		t.Fatalf("GetExtentMeta = %v, want ErrExtentNotFound", err)
	}

	// Ready → ReadyDegraded, then repeated calls stay put (idempotent).
	deg := &ExtentMetaV2{ID: 9003, Generation: 1, LogicalLen: 4096, Lifecycle: LifecycleReady}
	if err := store.putExtentMeta(deg); err != nil {
		t.Fatalf("putExtentMeta: %v", err)
	}
	if err := store.MarkExtentDegraded(ctx, 9003); err != nil {
		t.Fatalf("MarkExtentDegraded 1: %v", err)
	}
	if err := store.MarkExtentDegraded(ctx, 9003); err != nil {
		t.Fatalf("MarkExtentDegraded 2: %v", err)
	}
	got, err = store.GetExtentMeta(ctx, 9003)
	if err != nil {
		t.Fatalf("GetExtentMeta: %v", err)
	}
	if got.Lifecycle != LifecycleReadyDegraded {
		t.Fatalf("lifecycle = %d, want LifecycleReadyDegraded", got.Lifecycle)
	}
}
