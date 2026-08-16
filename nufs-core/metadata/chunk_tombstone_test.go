package metadata

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/hashicorp/raft"
)

func TestChunkTombstonePurgeEligibilityAtExactBoundary(t *testing.T) {
	store := newTestPebbleStore(t)
	ctx := context.Background()
	deletedAt := time.Date(2026, time.July, 28, 12, 0, 0, 0, time.UTC)
	tombstone := ChunkTombstone{
		ChunkID:     41,
		Size:        128,
		Reason:      "test orphan",
		DeletedAt:   deletedAt,
		DeleteAfter: deletedAt.Add(chunkTombstoneQuarantine),
	}

	if eligible, err := store.CanPurgeChunk(ctx, tombstone, tombstone.DeleteAfter.Add(-time.Second)); err != nil || eligible {
		t.Fatalf("before delete-after = (%v, %v), want (false, nil)", eligible, err)
	}

	putCommittedCatalog(t, store, deletedAt.Add(-time.Second), tombstone.DeleteAfter)
	if eligible, err := store.CanPurgeChunk(ctx, tombstone, tombstone.DeleteAfter); err != nil || eligible {
		t.Fatalf("backup older than deletion = (%v, %v), want (false, nil)", eligible, err)
	}

	putCommittedCatalog(t, store, deletedAt.Add(time.Second), tombstone.DeleteAfter)
	if eligible, err := store.CanPurgeChunk(ctx, tombstone, tombstone.DeleteAfter); err != nil || !eligible {
		t.Fatalf("all backups newer than deletion = (%v, %v), want (true, nil)", eligible, err)
	}
}

func TestChunkTombstonePurgeRejectsUncertainCatalog(t *testing.T) {
	store := newTestPebbleStore(t)
	ctx := context.Background()
	now := time.Date(2026, time.July, 29, 13, 0, 0, 0, time.UTC)
	tombstone := ChunkTombstone{
		ChunkID:     42,
		Size:        128,
		Reason:      "test orphan",
		DeletedAt:   now.Add(-chunkTombstoneQuarantine),
		DeleteAfter: now,
	}

	if eligible, err := store.CanPurgeChunk(ctx, tombstone, now); err == nil || eligible {
		t.Fatalf("missing catalog = (%v, %v), want error", eligible, err)
	}

	putCommittedCatalog(t, store, tombstone.DeletedAt.Add(time.Second), now.Add(-chunkCatalogMaxAge-time.Second))
	if eligible, err := store.CanPurgeChunk(ctx, tombstone, now); err == nil || eligible {
		t.Fatalf("stale catalog = (%v, %v), want error", eligible, err)
	}

	putCommittedCatalog(t, store, tombstone.DeletedAt.Add(time.Second), now.Add(time.Second))
	if eligible, err := store.CanPurgeChunk(ctx, tombstone, now); err == nil || eligible {
		t.Fatalf("future catalog = (%v, %v), want error", eligible, err)
	}
}

func TestChunkTombstonePurgeRejectsEmptyMalformedAndInconsistentCatalog(t *testing.T) {
	store := newTestPebbleStore(t)
	ctx := context.Background()
	now := time.Date(2026, time.July, 29, 13, 0, 0, 0, time.UTC)
	tombstone := ChunkTombstone{
		ChunkID:     421,
		Size:        128,
		Reason:      "test orphan",
		DeletedAt:   now.Add(-chunkTombstoneQuarantine),
		DeleteAfter: now,
	}
	if err := store.ReplaceCommittedBackupCatalog(ctx, nil, now); err != nil {
		t.Fatalf("store empty catalog: %v", err)
	}
	if eligible, err := store.CanPurgeChunk(ctx, tombstone, now); err == nil || eligible {
		t.Fatalf("empty catalog = (%v, %v), want error", eligible, err)
	}

	if err := store.putMsgpack(keyBackupCatalog, "malformed"); err != nil {
		t.Fatalf("corrupt catalog state: %v", err)
	}
	if eligible, err := store.CanPurgeChunk(ctx, tombstone, now); err == nil || eligible {
		t.Fatalf("malformed catalog = (%v, %v), want error", eligible, err)
	}

	putCommittedCatalog(t, store, tombstone.DeletedAt.Add(time.Second), now)
	if err := store.putMsgpack(prefixBackupCatalog+"20260728T120000Z-000000000002", &CommittedBackup{
		ID:              "20260728T120000Z-000000000002",
		SourceClusterID: "cluster-a",
		CreatedAt:       tombstone.DeletedAt.Add(time.Second),
		RaftTerm:        1,
		AppliedIndex:    2,
	}); err != nil {
		t.Fatalf("corrupt catalog index: %v", err)
	}
	if eligible, err := store.CanPurgeChunk(ctx, tombstone, now); err == nil || eligible {
		t.Fatalf("inconsistent catalog = (%v, %v), want error", eligible, err)
	}
}

func TestChunkTombstoneSnapshotsLiveChunkWithoutResettingQuarantine(t *testing.T) {
	store := newTestPebbleStore(t)
	ctx := context.Background()
	chunkID := ChunkID(43)
	chunk := &ChunkMeta{
		ID:       chunkID,
		Size:     256,
		State:    ChunkReady,
		Replicas: []ReplicaInfo{{NodeID: 7, Addr: "node-7:9001", State: ReplicaReady}},
	}
	if err := store.putMsgpack(fmt.Sprintf("%s%d", prefixChunk, chunkID), chunk); err != nil {
		t.Fatalf("seed chunk: %v", err)
	}

	if err := store.TombstoneChunk(ctx, chunkID, "test orphan"); err != nil {
		t.Fatalf("tombstone chunk: %v", err)
	}
	first, err := store.ListChunkTombstones(ctx, 1)
	if err != nil || len(first) != 1 {
		t.Fatalf("list first tombstone = (%v, %v), want one", first, err)
	}
	if first[0].Size != int64(chunk.Size) || len(first[0].Replicas) != 1 || first[0].Replicas[0] != chunk.Replicas[0] {
		t.Fatalf("tombstone snapshot = %+v, want chunk snapshot", first[0])
	}
	if first[0].DeleteAfter.Sub(first[0].DeletedAt) != chunkTombstoneQuarantine {
		t.Fatalf("quarantine = %s, want %s", first[0].DeleteAfter.Sub(first[0].DeletedAt), chunkTombstoneQuarantine)
	}
	if _, err := store.GetChunk(ctx, chunkID); err != nil {
		t.Fatalf("chunk must remain available during quarantine: %v", err)
	}

	if err := store.TombstoneChunk(ctx, chunkID, "different reason"); err != nil {
		t.Fatalf("repeat tombstone: %v", err)
	}
	second, err := store.ListChunkTombstones(ctx, 1)
	if err != nil || len(second) != 1 {
		t.Fatalf("list second tombstone = (%v, %v), want one", second, err)
	}
	if !second[0].DeletedAt.Equal(first[0].DeletedAt) || !second[0].DeleteAfter.Equal(first[0].DeleteAfter) || second[0].Reason != first[0].Reason {
		t.Fatalf("repeat tombstone changed durable snapshot: first=%+v second=%+v", first[0], second[0])
	}
}

func TestChunkTombstonePurgeRemovesBothRecordsAndIsIdempotent(t *testing.T) {
	store := newTestPebbleStore(t)
	ctx := context.Background()
	chunkID := ChunkID(44)
	deletedAt := time.Now().UTC().Add(-chunkTombstoneQuarantine).Round(0)
	seedTombstonedChunk(t, store, chunkID, 64, deletedAt)
	putCommittedCatalog(t, store, deletedAt.Add(time.Second), time.Now().UTC().Round(0))
	if err := store.PurgeChunk(ctx, chunkID); err != nil {
		t.Fatalf("purge chunk: %v", err)
	}
	if _, err := store.GetChunk(ctx, chunkID); !errors.Is(err, ErrChunkNotFound) {
		t.Fatalf("chunk after purge = %v, want ErrChunkNotFound", err)
	}
	if tombstones, err := store.ListChunkTombstones(ctx, 1); err != nil || len(tombstones) != 0 {
		t.Fatalf("tombstones after purge = (%v, %v), want none", tombstones, err)
	}
	if err := store.PurgeChunk(ctx, chunkID); err != nil {
		t.Fatalf("repeat purge: %v", err)
	}
}

func TestChunkTombstoneRefusesReferencedChunk(t *testing.T) {
	store := newTestPebbleStore(t)
	ctx := context.Background()
	chunkID := ChunkID(441)
	seedLiveChunk(t, store, chunkID, 64)
	seedInodeReferencingChunk(t, store, 441, chunkID)

	if err := store.TombstoneChunk(ctx, chunkID, "test referenced chunk"); err != nil {
		t.Fatalf("tombstone referenced chunk: %v", err)
	}
	if tombstones, err := store.ListChunkTombstones(ctx, 0); err != nil || len(tombstones) != 0 {
		t.Fatalf("tombstones after referenced delete = (%v, %v), want none", tombstones, err)
	}
}

func TestInodeCannotAttachTombstonedOrPurgedChunk(t *testing.T) {
	store := newTestPebbleStore(t)
	ctx := context.Background()
	tombstonedID := ChunkID(442)
	seedLiveChunk(t, store, tombstonedID, 64)
	seedInodeReferencingChunk(t, store, 442, 0)
	if err := store.TombstoneChunk(ctx, tombstonedID, "test tombstone"); err != nil {
		t.Fatalf("tombstone chunk: %v", err)
	}

	inode, err := store.GetInode(ctx, 442)
	if err != nil {
		t.Fatalf("get inode: %v", err)
	}
	inode.ChunkMap = []ChunkRef{{ID: tombstonedID, Offset: 0, Length: 64}}
	if err := store.UpdateInode(ctx, inode); err == nil {
		t.Fatal("attach tombstoned chunk succeeded")
	}

	purgedID := ChunkID(443)
	deletedAt := time.Now().UTC().Add(-chunkTombstoneQuarantine).Round(0)
	seedTombstonedChunk(t, store, purgedID, 64, deletedAt)
	putCommittedCatalog(t, store, deletedAt.Add(time.Second), time.Now().UTC().Round(0))
	if err := store.PurgeChunk(ctx, purgedID); err != nil {
		t.Fatalf("purge chunk: %v", err)
	}
	inode.ChunkMap = []ChunkRef{{ID: purgedID, Offset: 0, Length: 64}}
	if err := store.UpdateInode(ctx, inode); err == nil {
		t.Fatal("attach purged chunk succeeded")
	}
}

func TestChunkPurgeRefusesReferencedTombstone(t *testing.T) {
	store := newTestPebbleStore(t)
	ctx := context.Background()
	chunkID := ChunkID(444)
	deletedAt := time.Now().UTC().Add(-chunkTombstoneQuarantine).Round(0)
	seedTombstonedChunk(t, store, chunkID, 64, deletedAt)
	seedLegacyInodeReferencingChunk(t, store, 444, chunkID)
	putCommittedCatalog(t, store, deletedAt.Add(time.Second), time.Now().UTC().Round(0))

	if err := store.PurgeChunk(ctx, chunkID); err == nil {
		t.Fatal("purge referenced tombstone succeeded")
	}
	if _, err := store.GetChunk(ctx, chunkID); err != nil {
		t.Fatalf("referenced chunk was removed: %v", err)
	}
	if tombstones, err := store.ListChunkTombstones(ctx, 0); err != nil || len(tombstones) != 1 {
		t.Fatalf("tombstones after refused purge = (%v, %v), want one", tombstones, err)
	}
}

func TestUpdateChunkRefusesTombstonedChunk(t *testing.T) {
	store := newTestPebbleStore(t)
	ctx := context.Background()
	chunkID := ChunkID(445)
	seedLiveChunk(t, store, chunkID, 64)
	if err := store.TombstoneChunk(ctx, chunkID, "test tombstone"); err != nil {
		t.Fatalf("tombstone chunk: %v", err)
	}
	chunk, err := store.GetChunk(ctx, chunkID)
	if err != nil {
		t.Fatalf("get chunk: %v", err)
	}
	chunk.State = ChunkSealed
	if err := store.UpdateChunk(ctx, chunk); err == nil {
		t.Fatal("update tombstoned chunk succeeded")
	}
}

func TestTombstoneEpochConflictLeavesChunkUntombstoned(t *testing.T) {
	t.Skip("epoch removed: per-inode CAS is sufficient")
	store := newTestPebbleStore(t)
	ctx := context.Background()
	chunkID := ChunkID(446)
	seedLiveChunk(t, store, chunkID, 64)
	seedInodeReferencingChunk(t, store, 446, 0)
	reached := make(chan struct{})
	release := make(chan struct{})
	store.chunkTombstoneBeforeConditional = func() {
		close(reached)
		<-release
	}

	result := make(chan error, 1)
	go func() { result <- store.TombstoneChunk(ctx, chunkID, "test epoch fence") }()
	<-reached
	inode, err := store.GetInode(ctx, 446)
	if err != nil {
		t.Fatalf("get inode: %v", err)
	}
	inode.ChunkMap = []ChunkRef{{ID: chunkID, Offset: 0, Length: 64}}
	if err := store.UpdateInode(ctx, inode); err != nil {
		t.Fatalf("attach before conditional: %v", err)
	}
	close(release)
	if err := <-result; !errors.Is(err, ErrBackupMetadataConflict) {
		t.Fatalf("tombstone after epoch conflict = %v, want conflict", err)
	}
	if _, err := store.GetChunk(ctx, chunkID); err != nil {
		t.Fatalf("epoch-conflicted tombstone removed chunk: %v", err)
	}
	if tombstones, err := store.ListChunkTombstones(ctx, 0); err != nil || len(tombstones) != 0 {
		t.Fatalf("epoch-conflicted tombstone side effects = (%v, %v), want none", tombstones, err)
	}
}

func TestPurgeWinsOverPausedUpdateChunkWithoutResurrection(t *testing.T) {
	store := newTestPebbleStore(t)
	defer store.Close()
	ctx := context.Background()
	chunkID := ChunkID(447)
	seedLiveChunk(t, store, chunkID, 64)

	update, err := store.GetChunk(ctx, chunkID)
	if err != nil {
		t.Fatalf("get chunk: %v", err)
	}
	update.State = ChunkSealed
	reached := make(chan struct{})
	release := make(chan struct{})
	store.chunkUpdateBeforeConditional = func() {
		close(reached)
		<-release
	}
	result := make(chan error, 1)
	go func() { result <- store.UpdateChunk(ctx, update) }()
	<-reached

	if err := store.TombstoneChunk(ctx, chunkID, "race test"); err != nil {
		t.Fatalf("tombstone: %v", err)
	}
	tombstoneRaw, found, err := store.readChunkTombstoneRaw(chunkTombstoneKey(chunkID))
	if err != nil || !found {
		t.Fatalf("read tombstone = (%v, %v)", found, err)
	}
	tombstone, err := decodeChunkTombstone(tombstoneRaw)
	if err != nil {
		t.Fatalf("decode tombstone: %v", err)
	}
	tombstone.DeletedAt = time.Now().UTC().Add(-chunkTombstoneQuarantine).Round(0)
	tombstone.DeleteAfter = tombstone.DeletedAt.Add(chunkTombstoneQuarantine)
	if err := store.putMsgpack(chunkTombstoneKey(chunkID), &tombstone); err != nil {
		t.Fatalf("age tombstone: %v", err)
	}
	putCommittedCatalog(t, store, tombstone.DeletedAt.Add(time.Second), time.Now().UTC().Round(0))
	if err := store.PurgeChunk(ctx, chunkID); err != nil {
		t.Fatalf("purge: %v", err)
	}
	close(release)
	if err := <-result; !errors.Is(err, ErrBackupMetadataConflict) {
		t.Fatalf("paused update after purge = %v, want conditional conflict", err)
	}
	if _, err := store.GetChunk(ctx, chunkID); !errors.Is(err, ErrChunkNotFound) {
		t.Fatalf("chunk resurrected after purge: %v", err)
	}
	assertRawMissing(t, store, chunkTombstoneKey(chunkID))
}

func TestConditionalCommitFailureLeavesChunkAndTombstoneIntact(t *testing.T) {
	store := newTestPebbleStore(t)
	defer store.Close()
	ctx := context.Background()
	chunkID := ChunkID(448)
	seedLiveChunk(t, store, chunkID, 64)
	commitErr := errors.New("injected pebble conditional commit failure")
	oldCommit := conditionalBatchCommit
	conditionalBatchCommit = func(*pebble.Batch, *pebble.WriteOptions) error { return commitErr }
	defer func() { conditionalBatchCommit = oldCommit }()

	if err := store.TombstoneChunk(ctx, chunkID, "commit failure"); !errors.Is(err, commitErr) {
		t.Fatalf("tombstone failure = %v, want injected commit error", err)
	}
	if _, err := store.GetChunk(ctx, chunkID); err != nil {
		t.Fatalf("chunk changed after failed conditional commit: %v", err)
	}
	assertRawMissing(t, store, chunkTombstoneKey(chunkID))
}

func TestPurgeConditionalCommitFailureLeavesExactPairIntact(t *testing.T) {
	store := newTestPebbleStore(t)
	defer store.Close()
	ctx := context.Background()
	chunkID := ChunkID(4490)
	deletedAt := time.Now().UTC().Add(-chunkTombstoneQuarantine).Round(0)
	seedTombstonedChunk(t, store, chunkID, 64, deletedAt)
	putCommittedCatalog(t, store, deletedAt.Add(time.Second), time.Now().UTC().Round(0))
	chunkRaw, _, _ := store.readChunkTombstoneRaw(chunkMetadataKey(chunkID))
	tombstoneRaw, _, _ := store.readChunkTombstoneRaw(chunkTombstoneKey(chunkID))
	commitErr := errors.New("injected purge batch commit failure")
	oldCommit := conditionalBatchCommit
	conditionalBatchCommit = func(*pebble.Batch, *pebble.WriteOptions) error { return commitErr }
	defer func() { conditionalBatchCommit = oldCommit }()
	if err := store.PurgeChunk(ctx, chunkID); !errors.Is(err, commitErr) {
		t.Fatalf("purge failure = %v, want injected commit error", err)
	}
	assertRawValue(t, store, chunkMetadataKey(chunkID), chunkRaw)
	assertRawValue(t, store, chunkTombstoneKey(chunkID), tombstoneRaw)
}

func TestAllocateChunkRetriesDurableIDCollisionWithoutOverwriting(t *testing.T) {
	store := newTestPebbleStore(t)
	defer store.Close()
	ctx := context.Background()
	store.RegisterNode(ctx, &NodeInfo{ID: 1, Addr: "node:9001", CapacityGB: 10, Tier: TierHot})
	if err := store.CreateBucket(ctx, "allocation-collision", PlacementPolicy{}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	bucket, _ := store.GetBucket(ctx, "allocation-collision")
	inode, err := store.CreateFile(ctx, bucket.RootInode, "file", 0644)
	if err != nil {
		t.Fatalf("create file: %v", err)
	}
	collisionID := ChunkID(7001)
	seedLiveChunk(t, store, collisionID, 17)
	before, found, err := store.readChunkTombstoneRaw(chunkMetadataKey(collisionID))
	if err != nil || !found {
		t.Fatalf("read collision chunk = (%v, %v)", found, err)
	}
	ids := []ChunkID{collisionID, 7002}
	store.chunkIDNext = func() ChunkID { id := ids[0]; ids = ids[1:]; return id }
	chunk, err := store.AllocateChunk(ctx, inode.ID, 0, PlacementPolicy{ReplicationFactor: 1})
	if err != nil || chunk.ID != 7002 {
		t.Fatalf("allocation after collision = (%+v, %v), want ID 7002", chunk, err)
	}
	assertRawValue(t, store, chunkMetadataKey(collisionID), before)
	fresh, err := store.GetInode(ctx, inode.ID)
	if err != nil || len(fresh.ChunkMap) != 1 || fresh.ChunkMap[0].ID != chunk.ID {
		t.Fatalf("inode after collision retry = (%+v, %v)", fresh, err)
	}
}

func TestAllocateChunksBatchRetriesDuplicateCandidateAndCollision(t *testing.T) {
	store := newTestPebbleStore(t)
	defer store.Close()
	ctx := context.Background()
	store.RegisterNode(ctx, &NodeInfo{ID: 1, Addr: "node:9001", CapacityGB: 10, Tier: TierHot})
	if err := store.CreateBucket(ctx, "allocation-batch-collision", PlacementPolicy{}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	bucket, _ := store.GetBucket(ctx, "allocation-batch-collision")
	inode, err := store.CreateFile(ctx, bucket.RootInode, "file", 0644)
	if err != nil {
		t.Fatalf("create file: %v", err)
	}
	seedLiveChunk(t, store, 7101, 23)
	ids := []ChunkID{7102, 7102, 7101, 7103, 7104, 7105}
	store.chunkIDNext = func() ChunkID { id := ids[0]; ids = ids[1:]; return id }
	chunks, err := store.AllocateChunksBatch(ctx, inode.ID, []int64{0, MaxChunkSize}, PlacementPolicy{ReplicationFactor: 1})
	if err != nil || len(chunks) != 2 || chunks[0].ID != 7104 || chunks[1].ID != 7105 {
		t.Fatalf("batch allocation after rollback/wrap candidates = (%+v, %v)", chunks, err)
	}
	old, err := store.GetChunk(ctx, 7101)
	if err != nil || old.Size != 23 {
		t.Fatalf("collision chunk changed = (%+v, %v)", old, err)
	}
	fresh, err := store.GetInode(ctx, inode.ID)
	if err != nil || len(fresh.ChunkMap) != 2 || fresh.ChunkMap[0].ID != 7104 || fresh.ChunkMap[1].ID != 7105 {
		t.Fatalf("inode after batch retry = (%+v, %v)", fresh, err)
	}
}

func TestAllocateChunksBatchRejectsOversizeBeforeGeneration(t *testing.T) {
	store := newTestPebbleStore(t)
	ctx := context.Background()
	if err := store.RegisterNode(ctx, &NodeInfo{ID: 1, Addr: "node:9001", CapacityGB: 10, Tier: TierHot}); err != nil {
		t.Fatalf("RegisterNode: %v", err)
	}
	if err := store.CreateBucket(ctx, "oversize", PlacementPolicy{ReplicationFactor: 1}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	bucket, err := store.GetBucket(ctx, "oversize")
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}
	inode, err := store.CreateFile(ctx, bucket.RootInode, "file", 0644)
	if err != nil {
		t.Fatalf("CreateFile: %v", err)
	}
	var generated atomic.Int32
	store.chunkIDNext = func() ChunkID {
		generated.Add(1)
		return ChunkID(1000 + generated.Load())
	}

	offsets := make([]int64, 1025)
	for i := range offsets {
		offsets[i] = int64(i) * MaxChunkSize
	}
	if _, err := store.AllocateChunksBatch(ctx, inode.ID, offsets, PlacementPolicy{ReplicationFactor: 1}); err == nil ||
		!strings.Contains(err.Error(), "max chunk allocation batch") {
		t.Fatalf("AllocateChunksBatch oversize error = %v, want max batch rejection", err)
	}
	if got := generated.Load(); got != 0 {
		t.Fatalf("chunk ID generator called %d times before oversize rejection", got)
	}
	refs, err := store.ListChunks(ctx, inode.ID)
	if err != nil {
		t.Fatalf("ListChunks: %v", err)
	}
	if len(refs) != 0 {
		t.Fatalf("inode refs = %d, want zero side effects", len(refs))
	}
}

func TestAllocateChunkRejectsReadOnlyBeforeRaftSubmit(t *testing.T) {
	store, inode := newAllocationTestStore(t, "readonly")
	store.degradation.transition(DegStateReadOnly)
	var applyCalls atomic.Int32
	store.SetRaftNode(&RaftNode{
		conditionalLeaderHook: func() bool { return true },
		conditionalApplyHook: func([]byte, time.Duration) raft.ApplyFuture {
			applyCalls.Add(1)
			f := newControlledConditionalFuture()
			f.Resolve(nil, nil)
			return f
		},
	})
	t.Cleanup(func() { store.raft = nil })

	_, err := store.AllocateChunk(context.Background(), inode.ID, 0, PlacementPolicy{ReplicationFactor: 1})
	if !errors.Is(err, ErrServiceClosed) {
		t.Fatalf("AllocateChunk read-only error = %v, want ErrServiceClosed", err)
	}
	if got := applyCalls.Load(); got != 0 {
		t.Fatalf("read-only allocation submitted %d raft entries", got)
	}
}

func TestAllocateChunkConditionalFutureSlotsAreBoundedAndReleased(t *testing.T) {
	store, inode := newAllocationTestStore(t, "slots")
	store.chunkIDNext = sequentialChunkIDGenerator(1000)
	accepted := make(chan *controlledConditionalFuture, conditionalFutureWaiterCapacity+1)
	var applyCalls atomic.Int32
	store.SetRaftNode(&RaftNode{
		conditionalLeaderHook: func() bool { return true },
		conditionalApplyHook: func([]byte, time.Duration) raft.ApplyFuture {
			applyCalls.Add(1)
			f := newControlledConditionalFuture()
			accepted <- f
			return f
		},
	})
	t.Cleanup(func() { store.raft = nil })

	results := make([]chan error, conditionalFutureWaiterCapacity)
	futures := make([]*controlledConditionalFuture, 0, conditionalFutureWaiterCapacity)
	for i := 0; i < conditionalFutureWaiterCapacity; i++ {
		results[i] = make(chan error, 1)
		offset := int64(i) * MaxChunkSize
		go func() {
			_, err := store.AllocateChunk(context.Background(), inode.ID, offset, PlacementPolicy{ReplicationFactor: 1})
			results[i] <- err
		}()
		futures = append(futures, <-accepted)
	}
	blockedCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := store.AllocateChunk(blockedCtx, inode.ID, 99*MaxChunkSize, PlacementPolicy{ReplicationFactor: 1})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("slot-saturated allocation error = %v, want deadline", err)
	}
	if got := applyCalls.Load(); got != int32(conditionalFutureWaiterCapacity) {
		t.Fatalf("slot saturation apply calls = %d, want %d", got, conditionalFutureWaiterCapacity)
	}
	applyErr := errors.New("apply failed")
	for _, future := range futures {
		future.Resolve(nil, applyErr)
	}
	for i, ch := range results {
		if err := <-ch; !errors.Is(err, applyErr) {
			t.Fatalf("allocation %d error = %v, want apply failure after release", i, err)
		}
	}
}

func TestAllocateChunkAcceptedFutureIgnoresCallerCancelAndRecordsMetricsOnce(t *testing.T) {
	store, inode := newAllocationTestStore(t, "accepted")
	metrics := NewMetrics()
	store.SetMetrics(metrics)
	store.chunkIDNext = sequentialChunkIDGenerator(5000)
	submitted := make(chan []byte, 1)
	future := newControlledConditionalFuture()
	store.SetRaftNode(&RaftNode{
		conditionalLeaderHook: func() bool { return true },
		conditionalApplyHook: func(data []byte, _ time.Duration) raft.ApplyFuture {
			submitted <- append([]byte(nil), data...)
			return future
		},
	})
	t.Cleanup(func() { store.raft = nil })

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan struct {
		chunk *ChunkMeta
		err   error
	}, 1)
	go func() {
		chunk, err := store.AllocateChunk(ctx, inode.ID, 0, PlacementPolicy{ReplicationFactor: 1})
		result <- struct {
			chunk *ChunkMeta
			err   error
		}{chunk: chunk, err: err}
	}()
	data := <-submitted
	cancel()
	entry, err := DecodeRaftLogEntry(data)
	if err != nil {
		t.Fatalf("DecodeRaftLogEntry: %v", err)
	}
	if err := applyConditionalBatch(store.db, entry.Conditional, pebble.NoSync); err != nil {
		t.Fatalf("applyConditionalBatch: %v", err)
	}
	future.Resolve(nil, nil)
	got := <-result
	if got.err != nil {
		t.Fatalf("AllocateChunk after caller cancel: %v", got.err)
	}
	refs, err := store.ListChunks(context.Background(), inode.ID)
	if err != nil {
		t.Fatalf("ListChunks: %v", err)
	}
	if len(refs) != 1 || refs[0].ID != got.chunk.ID {
		t.Fatalf("refs = %+v, want exactly committed chunk %d", refs, got.chunk.ID)
	}
	if writes := metrics.WriteOps.Load(); writes != 1 {
		t.Fatalf("write metrics = %d, want exactly one", writes)
	}
}

func newAllocationTestStore(t *testing.T, bucketName string) (*PebbleStore, *InodeMeta) {
	t.Helper()
	store := newTestPebbleStore(t)
	ctx := context.Background()
	if err := store.RegisterNode(ctx, &NodeInfo{ID: 1, Addr: "node:9001", CapacityGB: 10, Tier: TierHot}); err != nil {
		t.Fatalf("RegisterNode: %v", err)
	}
	if err := store.CreateBucket(ctx, bucketName, PlacementPolicy{ReplicationFactor: 1}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	bucket, err := store.GetBucket(ctx, bucketName)
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}
	inode, err := store.CreateFile(ctx, bucket.RootInode, "file", 0644)
	if err != nil {
		t.Fatalf("CreateFile: %v", err)
	}
	return store, inode
}

func sequentialChunkIDGenerator(start ChunkID) func() ChunkID {
	var next atomic.Uint64
	next.Store(uint64(start))
	return func() ChunkID {
		return ChunkID(next.Add(1) - 1)
	}
}

func TestPurgeEpochAndCatalogConflictsLeaveDurablePairIntact(t *testing.T) {
	store := newTestPebbleStore(t)
	ctx := context.Background()
	deletedAt := time.Now().UTC().Add(-chunkTombstoneQuarantine).Round(0)
	chunkID := ChunkID(447)
	otherChunkID := ChunkID(448)
	seedTombstonedChunk(t, store, chunkID, 64, deletedAt)
	seedLiveChunk(t, store, otherChunkID, 64)
	seedInodeReferencingChunk(t, store, 447, 0)
	putCommittedCatalog(t, store, deletedAt.Add(time.Second), time.Now().UTC().Round(0))
	reached := make(chan struct{})
	release := make(chan struct{})
	store.chunkPurgeBeforeConditional = func() {
		close(reached)
		<-release
	}

	result := make(chan error, 1)
	go func() { result <- store.PurgeChunk(ctx, chunkID) }()
	<-reached
	inode, err := store.GetInode(ctx, 447)
	if err != nil {
		t.Fatalf("get inode: %v", err)
	}
	inode.ChunkMap = []ChunkRef{{ID: otherChunkID, Offset: 0, Length: 64}}
	if err := store.UpdateInode(ctx, inode); err != nil {
		t.Fatalf("advance reference epoch: %v", err)
	}
	putCommittedCatalog(t, store, deletedAt.Add(2*time.Second), time.Now().UTC().Round(0))
	close(release)
	if err := <-result; !errors.Is(err, ErrBackupMetadataConflict) {
		t.Fatalf("purge after epoch/catalog conflict = %v, want conflict", err)
	}
	if _, err := store.GetChunk(ctx, chunkID); err != nil {
		t.Fatalf("conflicted purge removed chunk: %v", err)
	}
	if tombstones, err := store.ListChunkTombstones(ctx, 0); err != nil || len(tombstones) != 1 {
		t.Fatalf("conflicted purge tombstones = (%v, %v), want one", tombstones, err)
	}
}

func TestChunkTombstoneDurableHalfStatesFailClosed(t *testing.T) {
	store := newTestPebbleStore(t)
	ctx := context.Background()
	deletedAt := time.Now().UTC().Add(-chunkTombstoneQuarantine).Round(0)

	wrongKeyID := ChunkID(449)
	seedLiveChunk(t, store, wrongKeyID, 64)
	wrongKeyTombstone := ChunkTombstone{ChunkID: wrongKeyID, Size: 64, Reason: "test", DeletedAt: deletedAt, DeleteAfter: deletedAt.Add(chunkTombstoneQuarantine)}
	if err := store.putMsgpack(chunkTombstoneKey(wrongKeyID+1), &wrongKeyTombstone); err != nil {
		t.Fatalf("seed wrong-key tombstone: %v", err)
	}
	if _, err := store.ListChunkTombstones(ctx, 0); err == nil {
		t.Fatal("wrong-key tombstone listed successfully")
	}

	tombstoneOnlyID := ChunkID(450)
	tombstoneOnly := ChunkTombstone{ChunkID: tombstoneOnlyID, Size: 64, Reason: "test", DeletedAt: deletedAt, DeleteAfter: deletedAt.Add(chunkTombstoneQuarantine)}
	if err := store.putMsgpack(chunkTombstoneKey(tombstoneOnlyID), &tombstoneOnly); err != nil {
		t.Fatalf("seed tombstone-only state: %v", err)
	}
	if err := store.PurgeChunk(ctx, tombstoneOnlyID); err == nil {
		t.Fatal("purge tombstone-only state succeeded")
	}

	chunkOnlyID := ChunkID(451)
	seedLiveChunk(t, store, chunkOnlyID, 64)
	if err := store.PurgeChunk(ctx, chunkOnlyID); err == nil {
		t.Fatal("purge chunk-only state succeeded")
	}
}

func TestTombstonedChunkRemainsVisibleToRepairAndRebalanceScans(t *testing.T) {
	store := newTestPebbleStore(t)
	ctx := context.Background()
	chunkID := ChunkID(452)
	chunk := &ChunkMeta{ID: chunkID, Size: 64, State: ChunkReady, Replicas: []ReplicaInfo{{NodeID: 1, Addr: "node-1", State: ReplicaReady}}}
	if err := store.putMsgpack(chunkMetadataKey(chunkID), chunk); err != nil {
		t.Fatalf("seed chunk: %v", err)
	}
	if err := store.RegisterNode(ctx, &NodeInfo{ID: 2, Addr: "node-2", State: NodeOnline}); err != nil {
		t.Fatalf("register rebalance target: %v", err)
	}
	if err := store.TombstoneChunk(ctx, chunkID, "test"); err != nil {
		t.Fatalf("tombstone: %v", err)
	}

	seen := 0
	if err := store.ScanAllChunks(ctx, func(scanned *ChunkMeta) error {
		if scanned.ID == chunkID {
			seen++
		}
		return nil
	}); err != nil {
		t.Fatalf("scan all chunks: %v", err)
	}
	if seen != 1 {
		t.Fatalf("tombstoned chunk scan count = %d, want 1", seen)
	}
	byNode, err := store.ChunksByNode(ctx, 1)
	if err != nil || len(byNode) != 1 || byNode[0].ID != chunkID {
		t.Fatalf("chunks by source node = (%v, %v), want tombstoned chunk", byNode, err)
	}
	if err := store.MigrateChunkReplica(ctx, chunkID, 1, 2); err == nil {
		t.Fatal("rebalance mutated tombstoned chunk")
	}
}

func TestChunkGCTombstonesOrphansBeforePhysicalPurge(t *testing.T) {
	store := newTestPebbleStore(t)
	ctx := context.Background()
	chunkID := ChunkID(45)
	if err := store.putMsgpack(fmt.Sprintf("%s%d", prefixChunk, chunkID), &ChunkMeta{ID: chunkID, Size: 512, State: ChunkReady}); err != nil {
		t.Fatalf("seed orphan: %v", err)
	}

	result, err := NewChunkGC(store, nil, nil, false).Scan(ctx)
	if err != nil {
		t.Fatalf("GC scan: %v", err)
	}
	if result.TombstonesCreated != 1 || result.ChunksPurged != 0 || result.DeletedChunks != 0 {
		t.Fatalf("first GC result = %+v, want one tombstone and no purge", result)
	}
	if _, err := store.GetChunk(ctx, chunkID); err != nil {
		t.Fatalf("orphan metadata must remain through quarantine: %v", err)
	}
	if tombstones, err := store.ListChunkTombstones(ctx, 1); err != nil || len(tombstones) != 1 {
		t.Fatalf("tombstones after phase A = (%v, %v), want one", tombstones, err)
	}
}

// TestChunkGC_KeepsV2ExtentBackedChunks pins the roadmap §1.4 orphan-GC fix:
// V2 layout inodes reference their data through /extent-meta and COW
// /extent-page rows, not a V1 ChunkMap. The GC reference snapshot must
// recognize those references or every V2-backed chunk is misjudged as an
// orphan and tombstoned (then purged after quarantine) — data loss on the
// production-default V2 write path (§1.3c). Without the fix the V2 chunk is
// tombstoned alongside the true orphan (TombstonesCreated==2 instead of 1),
// so the assertions fail directly.
func TestChunkGC_KeepsV2ExtentBackedChunks(t *testing.T) {
	ctx := context.Background()
	orphanID := ChunkID(5099)
	cases := []struct {
		name string
		seed func(t *testing.T, store *PebbleStore, id InodeID, chunk *ChunkMeta)
	}{
		{
			name: "inline extent",
			seed: func(t *testing.T, store *PebbleStore, id InodeID, chunk *ChunkMeta) {
				if err := store.SetInlineExtent(ctx, id, &ExtentMetaV2{ID: ExtentIDV2(chunk.ID), Generation: 1, LogicalLen: 4096}, 4096); err != nil {
					t.Fatalf("SetInlineExtent: %v", err)
				}
			},
		},
		{
			name: "extent pages",
			seed: func(t *testing.T, store *PebbleStore, id InodeID, chunk *ChunkMeta) {
				if err := store.ReplaceExtents(ctx, id, []ExtentWrite{{Extent: &ExtentMetaV2{ID: ExtentIDV2(chunk.ID), Generation: 1, LogicalLen: 4096}, Offset: 0}}, 4096); err != nil {
					t.Fatalf("ReplaceExtents: %v", err)
				}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := newResolveTestStore(t)
			id := InodeID(2001)
			newResolveTestInode(t, store, id)

			// The V2 layout's backing chunk: a real allocated chunk row whose
			// numeric ID mirrors the extent ID (extent==chunk-ID invariant),
			// plus a true orphan chunk (directly in Pebble, no inode ref).
			chunk, err := store.AllocateChunk(ctx, id, 0, v2ResolveTestPolicy)
			if err != nil {
				t.Fatalf("AllocateChunk: %v", err)
			}
			tc.seed(t, store, id, chunk)
			if err := store.putMsgpack(fmt.Sprintf("%s%d", prefixChunk, orphanID), &ChunkMeta{ID: orphanID, Size: 512, State: ChunkReady}); err != nil {
				t.Fatalf("seed orphan: %v", err)
			}

			result, err := NewChunkGC(store, nil, nil, false).Scan(ctx)
			if err != nil {
				t.Fatalf("GC scan: %v", err)
			}
			// Only the true orphan is tombstoned; the V2-backed chunk survives.
			if result.OrphanChunks != 1 {
				t.Fatalf("OrphanChunks = %d, want 1", result.OrphanChunks)
			}
			if result.TombstonesCreated != 1 {
				t.Fatalf("TombstonesCreated = %d, want 1 (V2-backed chunk must not be orphaned)", result.TombstonesCreated)
			}
			if result.ChunksPurged != 0 || result.DeletedChunks != 0 {
				t.Fatalf("GC result = %+v, want no purges/deletes this cycle", result)
			}
			if _, err := store.GetChunk(ctx, chunk.ID); err != nil {
				t.Fatalf("V2-backed chunk %d must survive GC: %v", chunk.ID, err)
			}
			if _, err := store.GetChunk(ctx, orphanID); err != nil {
				t.Fatalf("orphan must remain readable through quarantine: %v", err)
			}
			tombstones, err := store.ListChunkTombstones(ctx, 0)
			if err != nil {
				t.Fatalf("ListChunkTombstones: %v", err)
			}
			if len(tombstones) != 1 || tombstones[0].ChunkID != orphanID {
				t.Fatalf("tombstones = %+v, want only the true orphan", tombstones)
			}
		})
	}
}

// TestChunkGCOrphanLifecycleEndToEnd pins the full orphan chunk lifecycle in a
// single non-dry-run pass: a chunk that was allocated and committed but never
// attached to any inode (the FUSE flush crash-between-write-and-UpdateInode
// signature, and the S3 abandoned-allocated case) must be (1) caught and
// tombstoned by the first Scan, and (2) physically purged by a later Scan once
// the quarantine window has elapsed and a newer backup frees the payload. This
// closes the gap between TestChunkGCTombstonesOrphansBeforePhysicalPurge (stops
// at tombstone) and TestChunkGCPurgesEligibleTombstonesBeyondPublicListLimit
// (starts from a pre-seeded tombstone): the GC-produced tombstone is never
// exercised through to purge, which is exactly the path that reclaims a FUSE
// orphan's storage.
func TestChunkGCOrphanLifecycleEndToEnd(t *testing.T) {
	store := newTestPebbleStore(t)
	defer store.Close()
	ctx := context.Background()

	// A raw orphan: chunk metadata exists, no inode references it, and it was
	// never DeleteChunk'd — the crash-leftover profile.
	chunkID := ChunkID(46)
	if err := store.putMsgpack(fmt.Sprintf("%s%d", prefixChunk, chunkID), &ChunkMeta{ID: chunkID, Size: 512, State: ChunkReady}); err != nil {
		t.Fatalf("seed orphan: %v", err)
	}

	// First GC cycle: the orphan must be tombstoned, but retained (metadata +
	// payload) through the quarantine window.
	first, err := NewChunkGC(store, nil, nil, false).Scan(ctx)
	if err != nil {
		t.Fatalf("first GC scan: %v", err)
	}
	if first.TombstonesCreated != 1 || first.ChunksPurged != 0 || first.DeletedChunks != 0 {
		t.Fatalf("first GC result = %+v, want one tombstone and no purge", first)
	}
	if _, err := store.GetChunk(ctx, chunkID); err != nil {
		t.Fatalf("orphan must not be purged in the same cycle that tombstones it: %v", err)
	}
	tombstones, err := store.ListChunkTombstones(ctx, 1)
	if err != nil || len(tombstones) != 1 {
		t.Fatalf("tombstones after first cycle = (%v, %v), want one", tombstones, err)
	}

	// Advance the tombstone past quarantine and record a backup newer than the
	// deletion, so the next cycle is allowed to physically purge the payload.
	deletedAt := tombstones[0].DeletedAt
	aged := ChunkTombstone{
		ChunkID:     chunkID,
		Replicas:    tombstones[0].Replicas,
		Size:        tombstones[0].Size,
		Reason:      tombstones[0].Reason,
		DeletedAt:   deletedAt.Add(-chunkTombstoneQuarantine - time.Hour),
		DeleteAfter: deletedAt.Add(-time.Hour),
	}
	if err := store.putMsgpack(chunkTombstoneKey(chunkID), &aged); err != nil {
		t.Fatalf("age tombstone: %v", err)
	}
	putCommittedCatalog(t, store, aged.DeletedAt.Add(time.Second), time.Now().UTC().Round(0))

	// Second GC cycle: the orphan (now tombstoned + eligible) is purged.
	second, err := NewChunkGC(store, nil, nil, false).Scan(ctx)
	if err != nil {
		t.Fatalf("second GC scan: %v", err)
	}
	if second.ChunksPurged != 1 || second.DeletedChunks != 1 || second.FreedBytes != 512 {
		t.Fatalf("second GC result = %+v, want one chunk purged, 512 bytes freed", second)
	}
	if _, err := store.GetChunk(ctx, chunkID); !errors.Is(err, ErrChunkNotFound) {
		t.Fatalf("chunk after purge = %v, want ErrChunkNotFound", err)
	}
	if tombstones, err := store.ListChunkTombstones(ctx, 0); err != nil || len(tombstones) != 0 {
		t.Fatalf("tombstones after purge = (%v, %v), want none", tombstones, err)
	}
}

func TestChunkGCPurgesEligibleTombstonesBeyondPublicListLimit(t *testing.T) {
	store := newTestPebbleStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Round(0)
	deletedAt := now.Add(-chunkTombstoneQuarantine)
	putCommittedCatalog(t, store, deletedAt.Add(time.Second), now)

	for _, chunkID := range []ChunkID{51, 52, 53} {
		seedTombstonedChunk(t, store, chunkID, 1024, deletedAt)
	}
	if tombstones, err := store.ListChunkTombstones(ctx, 1); err != nil || len(tombstones) != 1 {
		t.Fatalf("public tombstone list = (%v, %v), want one limited result", tombstones, err)
	}

	result, err := NewChunkGC(store, nil, nil, false).Scan(ctx)
	if err != nil {
		t.Fatalf("GC scan: %v", err)
	}
	if result.ChunksPurged != 3 || result.DeletedChunks != 3 || result.FreedBytes != 3*1024 {
		t.Fatalf("eligible purge result = %+v, want all three chunks purged", result)
	}
	if tombstones, err := store.ListChunkTombstones(ctx, 0); err != nil || len(tombstones) != 0 {
		t.Fatalf("tombstones after purge = (%v, %v), want none", tombstones, err)
	}
}

func TestChunkGCDryRunDoesNotCreateOrPurgeTombstones(t *testing.T) {
	store := newTestPebbleStore(t)
	ctx := context.Background()
	chunkID := ChunkID(54)
	if err := store.putMsgpack(fmt.Sprintf("%s%d", prefixChunk, chunkID), &ChunkMeta{ID: chunkID, Size: 256, State: ChunkReady}); err != nil {
		t.Fatalf("seed orphan: %v", err)
	}

	result, err := NewChunkGC(store, nil, nil, true).Scan(ctx)
	if err != nil {
		t.Fatalf("dry-run GC scan: %v", err)
	}
	if result.TombstonesCreated != 0 || result.ChunksPurged != 0 || result.DeletedChunks != 0 {
		t.Fatalf("dry-run result = %+v, want zero durable mutations", result)
	}
	if _, err := store.GetChunk(ctx, chunkID); err != nil {
		t.Fatalf("dry-run removed chunk metadata: %v", err)
	}
	if tombstones, err := store.ListChunkTombstones(ctx, 0); err != nil || len(tombstones) != 0 {
		t.Fatalf("dry-run tombstones = (%v, %v), want none", tombstones, err)
	}
}

func TestChunkGCDryRunReportsExistingTombstoneBacklogWithoutWrites(t *testing.T) {
	store := newTestPebbleStore(t)
	defer store.Close()
	ctx := context.Background()
	chunkID := ChunkID(55)
	seedTombstonedChunk(t, store, chunkID, 64, time.Now().UTC().Add(-chunkTombstoneQuarantine).Round(0))

	result, err := NewChunkGC(store, nil, nil, true).Scan(ctx)
	if err != nil {
		t.Fatalf("dry-run GC scan: %v", err)
	}
	if result.TombstonesCreated != 0 || result.ChunksPurged != 0 || result.DeletedChunks != 0 {
		t.Fatalf("dry-run result = %+v, want zero durable mutations", result)
	}
	if result.TombstonesRetained != 1 || result.RetainedBytes != 64 {
		t.Fatalf("dry-run retained = (%d, %d), want (1, 64)", result.TombstonesRetained, result.RetainedBytes)
	}
	if _, err := store.GetChunk(ctx, chunkID); err != nil {
		t.Fatalf("dry-run purged metadata: %v", err)
	}
	if tombstones, err := store.ListChunkTombstones(ctx, 0); err != nil || len(tombstones) != 1 {
		t.Fatalf("dry-run tombstones = (%v, %v), want one retained tombstone", tombstones, err)
	}
}

func putCommittedCatalog(t *testing.T, store *PebbleStore, createdAt, reconciledAt time.Time) {
	t.Helper()
	if err := store.ReplaceCommittedBackupCatalog(context.Background(), []CommittedBackup{{
		ID:              "20260728T120000Z-000000000001",
		SourceClusterID: "cluster-a",
		CreatedAt:       createdAt,
		RaftTerm:        1,
		AppliedIndex:    1,
	}}, reconciledAt); err != nil {
		t.Fatalf("put committed catalog: %v", err)
	}
}

func seedTombstonedChunk(t *testing.T, store *PebbleStore, chunkID ChunkID, size int32, deletedAt time.Time) {
	t.Helper()
	if err := store.putMsgpack(fmt.Sprintf("%s%d", prefixChunk, chunkID), &ChunkMeta{ID: chunkID, Size: size, State: ChunkReady}); err != nil {
		t.Fatalf("seed chunk %d: %v", chunkID, err)
	}
	tombstone := ChunkTombstone{
		ChunkID:     chunkID,
		Size:        int64(size),
		Reason:      "orphaned by chunk GC",
		DeletedAt:   deletedAt,
		DeleteAfter: deletedAt.Add(chunkTombstoneQuarantine),
	}
	if err := store.putMsgpack(chunkTombstoneKey(chunkID), &tombstone); err != nil {
		t.Fatalf("seed tombstone %d: %v", chunkID, err)
	}
}

func seedLiveChunk(t *testing.T, store *PebbleStore, chunkID ChunkID, size int32) {
	t.Helper()
	if err := store.putMsgpack(chunkMetadataKey(chunkID), &ChunkMeta{ID: chunkID, Size: size, State: ChunkReady}); err != nil {
		t.Fatalf("seed live chunk %d: %v", chunkID, err)
	}
}

func seedInodeReferencingChunk(t *testing.T, store *PebbleStore, inodeID InodeID, chunkID ChunkID) {
	t.Helper()
	meta := &InodeMeta{ID: inodeID}
	if chunkID != 0 {
		meta.ChunkMap = []ChunkRef{{ID: chunkID, Offset: 0, Length: 64}}
	}
	if err := store.putMsgpack(fmt.Sprintf("%s%d", prefixInode, inodeID), meta); err != nil {
		t.Fatalf("seed inode %d: %v", inodeID, err)
	}
}

func seedLegacyInodeReferencingChunk(t *testing.T, store *PebbleStore, inodeID InodeID, chunkID ChunkID) {
	t.Helper()
	data, err := marshalValue(&InodeMeta{ID: inodeID, ChunkMap: []ChunkRef{{ID: chunkID, Offset: 0, Length: 64}}}, codecMsgpack)
	if err != nil {
		t.Fatalf("encode legacy inode %d: %v", inodeID, err)
	}
	if err := store.db.Set([]byte(fmt.Sprintf("%s%d", prefixInode, inodeID)), data, pebble.Sync); err != nil {
		t.Fatalf("seed legacy inode %d: %v", inodeID, err)
	}
}

func TestFSMInodeReferenceEpochTracksSetBatchAndCAS(t *testing.T) {
	t.Skip("epoch removed: per-inode CAS is sufficient")
	store := newTestPebbleStore(t)
	defer store.Close()
	fsm := &PebbleFSM{store: store}
	chunkID := ChunkID(9911)
	chunkRaw, err := marshalValue(&ChunkMeta{ID: chunkID, Size: 64, State: ChunkReady}, codecMsgpack)
	if err != nil {
		t.Fatalf("encode chunk: %v", err)
	}
	if err := store.db.Set([]byte(chunkMetadataKey(chunkID)), chunkRaw, pebble.Sync); err != nil {
		t.Fatalf("seed chunk: %v", err)
	}
	initialEpoch := readInodeReferenceEpochForTest(t, store)

	inodeKey := []byte("/inode/9911")
	first := &InodeMeta{ID: 9911, ChunkMap: []ChunkRef{{ID: chunkID, Length: 64}}}
	firstRaw, _ := marshalValue(first, codecMsgpack)
	if response := fsm.Apply(&raft.Log{Index: 1, Data: (&RaftLogEntry{Op: OpSet, Key: inodeKey, Value: firstRaw}).Encode()}); response != nil {
		t.Fatalf("OpSet response = %#v", response)
	}
	assertInodeReferenceEpoch(t, store, initialEpoch+1)

	second := &InodeMeta{ID: 9911}
	secondRaw, _ := marshalValue(second, codecMsgpack)
	if response := fsm.Apply(&raft.Log{Index: 2, Data: (&RaftLogEntry{Op: OpBatch, Batch: []BatchOp{{Key: inodeKey, Value: secondRaw}}}).Encode()}); response != nil {
		t.Fatalf("OpBatch response = %#v", response)
	}
	assertInodeReferenceEpoch(t, store, initialEpoch+2)

	versioned := &InodeWithVersion{InodeMeta: InodeMeta{ID: 9911}, Version: 7}
	versionedRaw, _ := marshalValue(versioned, codecMsgpack)
	if err := store.db.Set(inodeKey, versionedRaw, pebble.Sync); err != nil {
		t.Fatalf("seed versioned inode: %v", err)
	}
	updated := &InodeWithVersion{InodeMeta: InodeMeta{ID: 9911, ChunkMap: []ChunkRef{{ID: chunkID, Length: 64}}}, Version: 8}
	updatedRaw, _ := marshalValue(updated, codecMsgpack)
	casValue := make([]byte, 8+len(updatedRaw))
	binary.BigEndian.PutUint64(casValue, 7)
	copy(casValue[8:], updatedRaw)
	if response := fsm.Apply(&raft.Log{Index: 3, Data: (&RaftLogEntry{Op: OpCAS, Key: inodeKey, Value: casValue}).Encode()}); response != nil {
		t.Fatalf("OpCAS response = %#v", response)
	}
	assertInodeReferenceEpoch(t, store, initialEpoch+3)
}

func TestFSMRejectsInodeAttachmentAfterTombstoneAndPurge(t *testing.T) {
	store := newTestPebbleStore(t)
	defer store.Close()
	ctx := context.Background()
	fsm := &PebbleFSM{store: store}
	chunkID := ChunkID(9912)
	seedLiveChunk(t, store, chunkID, 64)
	if err := store.TombstoneChunk(ctx, chunkID, "test"); err != nil {
		t.Fatalf("tombstone: %v", err)
	}
	attachRaw, _ := marshalValue(&InodeMeta{ID: 9912, ChunkMap: []ChunkRef{{ID: chunkID, Length: 64}}}, codecMsgpack)
	response := fsm.Apply(&raft.Log{Index: 4, Data: (&RaftLogEntry{Op: OpSet, Key: []byte("/inode/9912"), Value: attachRaw}).Encode()})
	if response == nil {
		t.Fatal("FSM attached tombstoned chunk")
	}

	purgedID := ChunkID(9913)
	deletedAt := time.Now().UTC().Add(-chunkTombstoneQuarantine).Round(0)
	seedTombstonedChunk(t, store, purgedID, 64, deletedAt)
	putCommittedCatalog(t, store, deletedAt.Add(1), deletedAt.Add(chunkTombstoneQuarantine))
	if err := store.PurgeChunk(ctx, purgedID); err != nil {
		t.Fatalf("purge: %v", err)
	}
	purgedRaw, _ := marshalValue(&InodeMeta{ID: 9913, ChunkMap: []ChunkRef{{ID: purgedID, Length: 64}}}, codecMsgpack)
	response = fsm.Apply(&raft.Log{Index: 5, Data: (&RaftLogEntry{Op: OpSet, Key: []byte("/inode/9913"), Value: purgedRaw}).Encode()})
	if response == nil {
		t.Fatal("FSM attached purged chunk")
	}
}

func assertInodeReferenceEpoch(t *testing.T, store *PebbleStore, want uint64) {
	t.Helper()
	raw, found, err := store.readChunkTombstoneRaw(keyInodeReferenceEpoch)
	if err != nil || !found || len(raw) != 8 || binary.BigEndian.Uint64(raw) != want {
		t.Fatalf("inode reference epoch = (%x, %v, %v), want %d", raw, found, err, want)
	}
}

func readInodeReferenceEpochForTest(t *testing.T, store *PebbleStore) uint64 {
	t.Helper()
	raw, found, err := store.readChunkTombstoneRaw(keyInodeReferenceEpoch)
	if err != nil {
		t.Fatalf("read initial inode reference epoch: %v", err)
	}
	if !found {
		return 0
	}
	if len(raw) != 8 {
		t.Fatalf("initial inode reference epoch is malformed: %x", raw)
	}
	return binary.BigEndian.Uint64(raw)
}

func TestRaftCallerReceivesFSMRejectedInodeMutation(t *testing.T) {
	store, node := newCheckpointRaftNode(t, true)
	defer node.Shutdown()
	defer store.Close()
	if leader := waitForLeader([]*RaftNode{node}, 5*time.Second); leader == nil {
		t.Fatal("node did not become leader")
	}
	ctx := context.Background()
	inode, err := store.GetInode(ctx, RootInodeID)
	if err != nil {
		t.Fatalf("get root inode: %v", err)
	}
	inode.ChunkMap = []ChunkRef{{ID: 999999, Length: 64}}
	if err := store.UpdateInode(ctx, inode); err == nil {
		t.Fatal("Raft caller accepted FSM-rejected inode attachment")
	}
	fresh, err := store.GetInode(ctx, RootInodeID)
	if err != nil {
		t.Fatalf("read root after rejected mutation: %v", err)
	}
	if len(fresh.ChunkMap) != 0 {
		t.Fatalf("rejected inode mutation changed durable state: %+v", fresh.ChunkMap)
	}
}

func TestFSMInvalidatesPrimedInodeCacheAfterApply(t *testing.T) {
	store := newTestPebbleStore(t)
	defer store.Close()
	ctx := context.Background()
	before, err := store.GetInode(ctx, RootInodeID)
	if err != nil {
		t.Fatalf("prime inode cache: %v", err)
	}
	updated := *before
	updated.Mode = 0600
	raw, err := marshalValue(&updated, codecMsgpack)
	if err != nil {
		t.Fatalf("encode inode: %v", err)
	}
	fsm := &PebbleFSM{store: store}
	if response := fsm.Apply(&raft.Log{Index: 801, Term: 4, Data: (&RaftLogEntry{Op: OpSet, Key: []byte(fmt.Sprintf("%s%d", prefixInode, RootInodeID)), Value: raw}).Encode()}); response != nil {
		t.Fatalf("FSM update: %#v", response)
	}
	after, err := store.GetInode(ctx, RootInodeID)
	if err != nil {
		t.Fatalf("read updated inode: %v", err)
	}
	if after.Mode != updated.Mode {
		t.Fatalf("cached inode mode = %#o, want %#o", after.Mode, updated.Mode)
	}
}
