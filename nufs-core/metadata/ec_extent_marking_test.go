package metadata

import (
	"context"
	"testing"
)

// TestCommitChunkRefsModelAware_MarksECExtents guards roadmap §1.3d: the
// write-path commit (CommitChunkRefsModelAware, shared by FUSE flush, S3 PUT
// and the write-attempt recovery worker) must mark extents of ECConfig-bucket
// chunks ColdEC with the chunk's stripe reference, so extent-level consumers
// (quota, scrub, repair, orphan GC) can tell EC extents from hot-replica ones.
//
// The chunk is the truth at commit time: buildAllocatedChunks stamps each
// chunk's ECGroup from the bucket's ECConfig, and the direct-EC write lifts
// the chunk to durable EC (RecordDirectEC sets ECStripeID = ECGroup.GroupID)
// before the ref is ever committed. The extent's ECStripeID falls back to
// ECGroup.GroupID while ECStripeID is not yet set — both spell the same
// "ec-<id>" value, so the pre-/post-RecordDirect windows agree. A chunk that
// cannot be read or is not EC must degrade to hot-replica defaults rather
// than fail the write.
func TestCommitChunkRefsModelAware_MarksECExtents(t *testing.T) {
	ctx := context.Background()
	store := newECAllocTestStore(t)

	// EC(4,2): chunks allocate with an ECGroup sized from the bucket config
	// (not the 6+3 default), spread across the 12 registered nodes.
	ecPolicy := PlacementPolicy{ECConfig: &ECConfig{DataShards: 4, ParityShards: 2}}
	// RF=3 without ECConfig: hot-replica chunks stay EC-less at allocation.
	hotPolicy := PlacementPolicy{ReplicationFactor: 3}

	var nextID InodeID = 9000
	newInode := func(t *testing.T) InodeID {
		t.Helper()
		nextID++
		if _, err := NewInodeStoreV2(store).CreateEmpty(nextID, FileRegular, 1, 0, 0, 0644); err != nil {
			t.Fatal(err)
		}
		return nextID
	}
	proj := func(t *testing.T, id InodeID) *InodeMeta {
		t.Helper()
		in, err := store.GetInode(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		return in
	}
	alloc := func(t *testing.T, id InodeID, policy PlacementPolicy, offs ...int64) []*ChunkMeta {
		t.Helper()
		chunks, err := store.AllocateChunksBatch(ctx, id, offs, policy)
		if err != nil {
			t.Fatal(err)
		}
		return chunks
	}
	extentMeta := func(t *testing.T, id ExtentIDV2) *ExtentMetaV2 {
		t.Helper()
		em, err := store.GetExtentMeta(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		return em
	}

	t.Run("inline EC chunk → ColdEC", func(t *testing.T) {
		id := newInode(t)
		chunks := alloc(t, id, ecPolicy, 0)
		chunk := chunks[0]
		if chunk.ECGroup == nil {
			t.Fatal("ECConfig bucket must allocate an EC-grouped chunk")
		}
		refs := []ChunkRef{{ID: chunk.ID, Offset: 0, Length: 4096}}
		if err := CommitChunkRefsModelAware(ctx, store, proj(t, id), refs, 4096); err != nil {
			t.Fatalf("commit: %v", err)
		}
		in, _ := NewInodeStoreV2(store).Get(id)
		if in.Layout != LayoutInlineExtent {
			t.Fatalf("single small EC chunk must land inline, got layout %d", in.Layout)
		}
		em := extentMeta(t, ExtentIDV2(chunk.ID))
		if em.StorageClass != StorageClassColdEC {
			t.Errorf("inline extent StorageClass = %d, want ColdEC", em.StorageClass)
		}
		if em.ECStripeID != chunk.ECGroup.GroupID {
			t.Errorf("inline extent ECStripeID = %q, want %q (chunk's stripe)", em.ECStripeID, chunk.ECGroup.GroupID)
		}
	})

	t.Run("pages EC chunks → ColdEC per ref", func(t *testing.T) {
		id := newInode(t)
		chunks := alloc(t, id, ecPolicy, 0, MaxChunkSize)
		if len(chunks) != 2 {
			t.Fatalf("allocated %d chunks, want 2", len(chunks))
		}
		refs := []ChunkRef{
			{ID: chunks[0].ID, Offset: 0, Length: 4096},
			{ID: chunks[1].ID, Offset: MaxChunkSize, Length: 4096},
		}
		if err := CommitChunkRefsModelAware(ctx, store, proj(t, id), refs, MaxChunkSize+4096); err != nil {
			t.Fatalf("commit: %v", err)
		}
		in, _ := NewInodeStoreV2(store).Get(id)
		if in.Layout != LayoutExtentPages {
			t.Fatalf("multi-chunk commit must land pages, got layout %d", in.Layout)
		}
		got, err := store.ResolveExtents(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 2 {
			t.Fatalf("resolved %d extents, want 2", len(got))
		}
		for i, r := range got {
			if r.ExtentID != ExtentIDV2(chunks[i].ID) {
				t.Fatalf("resolved extent[%d] = %d, want chunk %d", i, r.ExtentID, chunks[i].ID)
			}
			em := extentMeta(t, r.ExtentID)
			if em.StorageClass != StorageClassColdEC {
				t.Errorf("pages extent[%d] StorageClass = %d, want ColdEC", i, em.StorageClass)
			}
			if em.ECStripeID != chunks[i].ECGroup.GroupID {
				t.Errorf("pages extent[%d] ECStripeID = %q, want %q", i, em.ECStripeID, chunks[i].ECGroup.GroupID)
			}
		}
	})

	t.Run("hot bucket stays hot replica", func(t *testing.T) {
		id := newInode(t)
		chunks := alloc(t, id, hotPolicy, 0)
		chunk := chunks[0]
		if chunk.ECGroup != nil {
			t.Fatal("hot bucket must allocate EC-less chunks")
		}
		refs := []ChunkRef{{ID: chunk.ID, Offset: 0, Length: 4096}}
		if err := CommitChunkRefsModelAware(ctx, store, proj(t, id), refs, 4096); err != nil {
			t.Fatalf("commit: %v", err)
		}
		em := extentMeta(t, ExtentIDV2(chunk.ID))
		if em.StorageClass != StorageClassHotReplica {
			t.Errorf("hot extent StorageClass = %d, want HotReplica", em.StorageClass)
		}
		if em.ECStripeID != "" {
			t.Errorf("hot extent ECStripeID = %q, want empty", em.ECStripeID)
		}
	})

	t.Run("missing chunk degrades to hot replica", func(t *testing.T) {
		id := newInode(t)
		// Fake ref: no chunk 99901 exists on the store. The commit must still
		// succeed (degrading the extent to hot-replica defaults) rather than
		// fail on the class lookup — the extent's data stays reachable through
		// the chunk path regardless of class.
		refs := []ChunkRef{{ID: 99901, Offset: 0, Length: 4096}}
		if err := CommitChunkRefsModelAware(ctx, store, proj(t, id), refs, 4096); err != nil {
			t.Fatalf("commit: %v", err)
		}
		em := extentMeta(t, ExtentIDV2(99901))
		if em.StorageClass != StorageClassHotReplica {
			t.Errorf("degraded extent StorageClass = %d, want HotReplica", em.StorageClass)
		}
		if em.ECStripeID != "" {
			t.Errorf("degraded extent ECStripeID = %q, want empty", em.ECStripeID)
		}
	})
}
