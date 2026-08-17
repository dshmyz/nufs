package metadata

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// seedV2InlineInode creates a V2 inode with an inline extent and a backing
// chunk (no EC metadata). Returns the extent ID for convenience.
func seedV2InlineInode(t *testing.T, store *PebbleStore, inodeID InodeID, extID ExtentIDV2, size int64, extCreated int64) ExtentIDV2 {
	t.Helper()
	ins := NewInodeStoreV2(store)
	in, err := ins.CreateEmpty(inodeID, FileRegular, 1, 0, 0, 0644)
	if err != nil {
		t.Fatalf("CreateEmpty: %v", err)
	}
	// MTime is set on creation; override for tests that need it.
	in.MTime = extCreated
	if err := ins.Put(in); err != nil {
		t.Fatalf("Put inode: %v", err)
	}
	ext := &ExtentMetaV2{
		ID:           extID,
		Generation:   1,
		LogicalLen:   size,
		Lifecycle:    LifecycleReady,
		StorageClass: StorageClassHotReplica,
		CreatedAt:    extCreated,
	}
	if err := ins.SetInlineExtent(inodeID, ext, size); err != nil {
		t.Fatalf("SetInlineExtent: %v", err)
	}
	// Seed backing chunk (no EC metadata).
	if err := store.putMsgpack(
		fmt.Sprintf("%s%d", prefixChunk, ChunkID(extID)),
		&ChunkMeta{ID: ChunkID(extID), Size: int32(size), State: ChunkReady},
	); err != nil {
		t.Fatalf("seed chunk: %v", err)
	}
	return extID
}

func TestECConversionEligible_HappyPath(t *testing.T) {
	store := newV2TestPebbleStore(t)
	ctx := context.Background()

	old := time.Now().Add(-31 * 24 * time.Hour).UnixNano()
	seedV2InlineInode(t, store, 100, 0x10000000001, 4096, old)

	ins := NewInodeStoreV2(store)
	in, err := ins.Get(100)
	if err != nil {
		t.Fatal(err)
	}
	ext := in.InlineExtent

	elig, ok, err := store.ECConversionEligible(ctx, 100, in, ext)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected eligible, got not eligible")
	}
	if elig.Inode != 100 || elig.Extent != 0x10000000001 {
		t.Fatalf("eligibility mismatch: %+v", elig)
	}
}

func TestECConversionEligible_SkipRecent(t *testing.T) {
	store := newV2TestPebbleStore(t)
	ctx := context.Background()

	recent := time.Now().Add(-1 * 24 * time.Hour).UnixNano()
	seedV2InlineInode(t, store, 101, 0x10000000002, 4096, recent)

	ins := NewInodeStoreV2(store)
	in, _ := ins.Get(101)

	_, ok, err := store.ECConversionEligible(ctx, 101, in, in.InlineExtent)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected not eligible (too recent)")
	}
}

func TestECConversionEligible_SkipAlreadyEC(t *testing.T) {
	store := newV2TestPebbleStore(t)
	ctx := context.Background()

	old := time.Now().Add(-31 * 24 * time.Hour).UnixNano()
	seedV2InlineInode(t, store, 102, 0x10000000003, 4096, old)

	// Mark the extent as already EC.
	ins := NewInodeStoreV2(store)
	in, _ := ins.Get(102)
	in.InlineExtent.StorageClass = StorageClassColdEC
	in.InlineExtent.ECStripeID = "stripe-old"
	if err := ins.Put(in); err != nil {
		t.Fatal(err)
	}
	in2, _ := ins.Get(102)

	_, ok, err := store.ECConversionEligible(ctx, 102, in2, in2.InlineExtent)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected not eligible (already EC)")
	}
}

func TestECConversionEligible_SkipDegraded(t *testing.T) {
	store := newV2TestPebbleStore(t)
	ctx := context.Background()

	old := time.Now().Add(-31 * 24 * time.Hour).UnixNano()
	seedV2InlineInode(t, store, 103, 0x10000000004, 4096, old)

	ins := NewInodeStoreV2(store)
	in, _ := ins.Get(103)
	in.InlineExtent.Lifecycle = LifecycleReadyDegraded
	if err := ins.Put(in); err != nil {
		t.Fatal(err)
	}
	in2, _ := ins.Get(103)

	_, ok, err := store.ECConversionEligible(ctx, 103, in2, in2.InlineExtent)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected not eligible (degraded lifecycle)")
	}
}

func TestECConversionEligible_SkipChunkAlreadyEC(t *testing.T) {
	store := newV2TestPebbleStore(t)
	ctx := context.Background()

	old := time.Now().Add(-31 * 24 * time.Hour).UnixNano()
	seedV2InlineInode(t, store, 104, 0x10000000005, 4096, old)

	// Mark the chunk as already EC (direct-write path completed).
	chunkID := ChunkID(0x10000000005)
	chunk, err := store.GetChunk(ctx, chunkID)
	if err != nil {
		t.Fatal(err)
	}
	chunk.ECStripeID = "stripe-direct"
	if err := store.UpdateChunk(ctx, chunk); err != nil {
		t.Fatal(err)
	}

	ins := NewInodeStoreV2(store)
	in, _ := ins.Get(104)

	_, ok, err := store.ECConversionEligible(ctx, 104, in, in.InlineExtent)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected not eligible (chunk already has ECStripeID)")
	}
}

func TestSubmitConversion_Idempotent(t *testing.T) {
	store := newV2TestPebbleStore(t)
	ctx := context.Background()

	elig := &ConversionEligibility{
		Inode:         200,
		Extent:        0x20000000001,
		ExtentCreated: time.Now().Add(-31 * 24 * time.Hour).UnixNano(),
		Size:          4096,
	}

	if err := store.SubmitConversion(ctx, elig); err != nil {
		t.Fatal(err)
	}
	// Second submit: idempotent, no error, no duplicate task.
	if err := store.SubmitConversion(ctx, elig); err != nil {
		t.Fatal(err)
	}
	task, err := store.GetBackgroundTask(ctx, "ec-convert-2199023255553")
	if err != nil {
		t.Fatalf("GetBackgroundTask: %v", err)
	}
	if task.Type != TaskECConvert || task.State != TaskQueued {
		t.Fatalf("task mismatch: %+v", task)
	}
}

func TestECConversionEligible_OwnerNodesFromReplicas(t *testing.T) {
	store := newV2TestPebbleStore(t)
	ctx := context.Background()

	old := time.Now().Add(-31 * 24 * time.Hour).UnixNano()
	extID := seedV2InlineInode(t, store, 200, 0x10000000010, 4096, old)

	// Backing chunk with an RF=3 replica set plus a duplicate and a zero
	// (unassigned) entry to prove dedup + skip-0 on the OwnerNodes projection.
	if err := store.putMsgpack(
		fmt.Sprintf("%s%d", prefixChunk, ChunkID(extID)),
		&ChunkMeta{ID: ChunkID(extID), Size: 4096, State: ChunkReady,
			Replicas: []ReplicaInfo{
				{NodeID: 1}, {NodeID: 2}, {NodeID: 1}, {NodeID: 0},
			}},
	); err != nil {
		t.Fatal(err)
	}

	ins := NewInodeStoreV2(store)
	in, err := ins.Get(200)
	if err != nil {
		t.Fatal(err)
	}
	elig, ok, err := store.ECConversionEligible(ctx, 200, in, in.InlineExtent)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected eligible")
	}
	want := []uint64{1, 2}
	if len(elig.OwnerNodes) != len(want) || elig.OwnerNodes[0] != 1 || elig.OwnerNodes[1] != 2 {
		t.Fatalf("OwnerNodes = %v, want %v", elig.OwnerNodes, want)
	}

	// SubmitConversion propagates OwnerNodes onto the task so the datanode
	// worker's owner-routed lease can restrict it to replica holders.
	if err := store.SubmitConversion(ctx, elig); err != nil {
		t.Fatal(err)
	}
	task, err := store.GetBackgroundTask(ctx, fmt.Sprintf("ec-convert-%d", uint64(elig.Extent)))
	if err != nil {
		t.Fatalf("GetBackgroundTask: %v", err)
	}
	if len(task.OwnerNodes) != 2 || task.OwnerNodes[0] != 1 || task.OwnerNodes[1] != 2 {
		t.Fatalf("task OwnerNodes = %v, want %v", task.OwnerNodes, want)
	}
}

func TestECConversionScheduler_RunOnce(t *testing.T) {
	store := newV2TestPebbleStore(t)
	ctx := context.Background()

	old := time.Now().Add(-31 * 24 * time.Hour).UnixNano()
	recent := time.Now().Add(-1 * 24 * time.Hour).UnixNano()

	// Inode 300: eligible (old + Ready + HotReplica + backing chunk).
	seedV2InlineInode(t, store, 300, 0x30000000001, 4096, old)
	// Inode 301: not eligible (too recent).
	seedV2InlineInode(t, store, 301, 0x30000000002, 4096, recent)
	// Inode 302: not eligible (already EC on extent).
	seedV2InlineInode(t, store, 302, 0x30000000003, 4096, old)
	ins := NewInodeStoreV2(store)
	in302, _ := ins.Get(302)
	in302.InlineExtent.StorageClass = StorageClassColdEC
	in302.InlineExtent.ECStripeID = "stripe-302"
	_ = ins.Put(in302)

	sched := NewECConversionScheduler(store)
	// Run once directly (no goroutine) for test determinism.
	sched.run()

	queue, err := store.ConversionQueue(ctx)
	if err != nil {
		t.Fatalf("ConversionQueue: %v", err)
	}
	if len(queue) != 1 {
		t.Fatalf("queue length = %d, want 1", len(queue))
	}
	wantTaskID := "ec-convert-" + fmt.Sprintf("%d", uint64(0x30000000001))
	if queue[0].ID != wantTaskID {
		t.Fatalf("task ID = %q, want %q", queue[0].ID, wantTaskID)
	}
}

func TestScanV2Inlines_SkipsV1(t *testing.T) {
	store := newV2TestPebbleStore(t)

	// Seed a V1 inode (directly write InodeMeta — no Layout field, which
	// decodes as LayoutEmpty when unmarshalled as InodeMetaV2).
	if err := store.putMsgpack(
		fmt.Sprintf("%s%d", prefixInode, InodeID(500)),
		&InodeMeta{ID: 500, Type: FileRegular, Size: 4096},
	); err != nil {
		t.Fatal(err)
	}

	var seen []InodeID
	err := store.ScanV2Inlines(context.Background(), func(id InodeID, _ *InodeMetaV2, _ *ExtentMetaV2) error {
		seen = append(seen, id)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(seen) != 0 {
		t.Fatalf("ScanV2Inlines called for V1 inodes: %v", seen)
	}
}

func TestConversionQueue_Empty(t *testing.T) {
	store := newV2TestPebbleStore(t)
	queue, err := store.ConversionQueue(context.Background())
	if err != nil {
		t.Fatalf("ConversionQueue: %v", err)
	}
	if len(queue) != 0 {
		t.Fatalf("queue length = %d, want 0", len(queue))
	}
}
