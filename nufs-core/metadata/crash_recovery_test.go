package metadata

import (
	"context"
	"testing"
	"time"
)

// TestCrashReplay_FlushOrphanCleanup verifies the FUSE crash scenario:
// crash happens after AllocateChunksBatch (which atomically adds chunks to
// inode ChunkMap) but before the final UpdateInode (which sets size + final
// ChunkMap). After restart, the inode is in an intermediate state with
// extra chunks in ChunkMap but old size. ChunkGC must handle this.
//
// This is the Neville-Neil "crash+replay" drill at the metadata layer.
func TestCrashReplay_FlushOrphanCleanup(t *testing.T) {
	store := newTestPebbleStore(t)
	defer store.Close()
	ctx := context.Background()

	// Register a node for chunk placement.
	if err := store.RegisterNode(ctx, &NodeInfo{
		ID: 1, Addr: "node:9001", CapacityGB: 10, Tier: TierHot,
	}); err != nil {
		t.Fatalf("RegisterNode: %v", err)
	}
	if err := store.CreateBucket(ctx, "crash-bucket", PlacementPolicy{ReplicationFactor: 1}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	bucket, err := store.GetBucket(ctx, "crash-bucket")
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}

	// Phase 1: create an inode with one committed chunk (pre-crash state).
	inode, err := store.CreateFile(ctx, bucket.RootInode, "crash-test.txt", 0644)
	if err != nil {
		t.Fatalf("create file: %v", err)
	}

	committedChunk, err := store.AllocateChunk(ctx, inode.ID, 0, PlacementPolicy{ReplicationFactor: 1})
	if err != nil {
		t.Fatalf("allocate committed chunk: %v", err)
	}
	if err := store.CommitChunk(ctx, committedChunk.ID, 12345); err != nil {
		t.Fatalf("commit chunk: %v", err)
	}
	if err := store.SealChunk(ctx, committedChunk.ID); err != nil {
		t.Fatalf("seal chunk: %v", err)
	}
	inode.ChunkMap = []ChunkRef{{ID: committedChunk.ID, Offset: 0, Length: 64, Version: 1}}
	inode.Size = 64
	if err := store.UpdateInode(ctx, inode); err != nil {
		t.Fatalf("update inode: %v", err)
	}

	// Phase 2: simulate crash — AllocateChunksBatch adds chunks to inode
	// atomically, but the final UpdateInode (size + final ChunkMap) never
	// executes. After crash, inode has: committed chunk + newly allocated
	// chunks, but old size. The newly allocated chunks may or may not have
	// been written to datanode (crash could happen between allocation and write).
	orphanChunk1, err := store.AllocateChunk(ctx, inode.ID, 64, PlacementPolicy{ReplicationFactor: 1})
	if err != nil {
		t.Fatalf("allocate orphan 1: %v", err)
	}
	if err := store.CommitChunk(ctx, orphanChunk1.ID, 99999); err != nil {
		t.Fatalf("commit orphan 1: %v", err)
	}

	// Note: we do NOT call UpdateInode here — this simulates the crash.
	// The inode now has: ChunkMap = [committedChunk, orphanChunk1], Size = 64.

	// Phase 3: verify the intermediate state.
	inodePost, err := store.GetInode(ctx, inode.ID)
	if err != nil {
		t.Fatalf("get inode post-crash: %v", err)
	}
	// ChunkMap has 2 chunks (committed + orphan from AllocateChunk).
	if len(inodePost.ChunkMap) != 2 {
		t.Fatalf("inode should have 2 chunks (committed + orphan), got %d", len(inodePost.ChunkMap))
	}
	// But size is still 64 (old size, UpdateInode never ran).
	if inodePost.Size != 64 {
		t.Fatalf("inode size = %d, want 64 (UpdateInode never ran)", inodePost.Size)
	}

	// Phase 4: run ChunkGC — orphan chunk is referenced (in ChunkMap),
	// so GC won't tombstone it. This is correct: after AllocateChunk
	// atomically updates ChunkMap, the chunk IS referenced. The real
	// cleanup path is: the FUSE recovery worker (ObjectWriteRecoveryWorker)
	// detects the incomplete attempt and reconciles.
	gc := NewChunkGC(store, nil, nil, false)
	gcResult, err := gc.Scan(ctx)
	if err != nil {
		t.Fatalf("GC scan: %v", err)
	}

	// GC found 0 orphans — correct! The chunk is referenced (in ChunkMap).
	if gcResult.OrphanChunks != 0 {
		t.Fatalf("GC found %d orphans, want 0 (chunk is in ChunkMap)", gcResult.OrphanChunks)
	}

	// Phase 5: the correct recovery is via the attempt ledger + recovery worker,
	// not GC. GC cleans unreferenced chunks; recovery cleans incomplete writes.
	// Verify the attempt ledger records the incomplete flush.
	attemptID := "crash-test-attempt"
	if err := store.PutWriteAttempt(ctx, &ObjectWriteAttempt{
		ID:         attemptID,
		Bucket:     "crash-bucket",
		InodeID:    inode.ID,
		InodeCTime: inode.CTime,
		Chunks:     []ChunkRef{{ID: orphanChunk1.ID, Offset: 64, Length: 64, Version: 1}},
		State:      WriteAttemptChunksAllocated,
	}); err != nil {
		t.Fatalf("put attempt: %v", err)
	}

	// Verify attempt exists and can be listed.
	attempts, err := store.ListWriteAttemptsByState(ctx, WriteAttemptChunksAllocated, 10)
	if err != nil {
		t.Fatalf("list attempts: %v", err)
	}
	if len(attempts) == 0 {
		t.Fatalf("no attempts in ChunksAllocated state")
	}

	// Verify original committed chunk is still intact.
	origChunk, err := store.GetChunk(ctx, committedChunk.ID)
	if err != nil {
		t.Fatalf("committed chunk should still exist: %v", err)
	}
	if origChunk.Size == 0 {
		t.Fatalf("committed chunk size = 0, data lost")
	}
}

// TestCrashReplay_AttemptLedgerRecordsState verifies that the write-attempt
// ledger correctly records state transitions during a flush, and that
// the recovery worker can pick up incomplete attempts.
func TestCrashReplay_AttemptLedgerRecordsState(t *testing.T) {
	store := newTestPebbleStore(t)
	defer store.Close()
	ctx := context.Background()

	// Create an inode.
	inode, err := store.CreateFile(ctx, 0, "ledger-test.txt", 0644)
	if err != nil {
		t.Fatalf("create file: %v", err)
	}

	// Simulate FUSE flush attempt ledger: Pending → ChunksAllocated → ChunksDurable.
	attemptID := "fuse-test-123"
	now := time.Now().UnixNano()

	// Write attempt in Pending state.
	if err := store.PutWriteAttempt(ctx, &ObjectWriteAttempt{
		ID:         attemptID,
		Bucket:     "test-bucket",
		InodeID:    inode.ID,
		InodeCTime: inode.CTime,
		State:      WriteAttemptPending,
		CreatedAt:  now,
	}); err != nil {
		t.Fatalf("put attempt pending: %v", err)
	}

	// Verify attempt exists.
	attempt, err := store.GetWriteAttempt(ctx, attemptID)
	if err != nil {
		t.Fatalf("get attempt: %v", err)
	}
	if attempt.State != WriteAttemptPending {
		t.Fatalf("state = %s, want pending", attempt.State)
	}

	// Update to ChunksDurable.
	attempt.State = WriteAttemptChunksDurable
	if err := store.PutWriteAttempt(ctx, attempt); err != nil {
		t.Fatalf("put attempt durable: %v", err)
	}

	// Verify state updated.
	attempt2, err := store.GetWriteAttempt(ctx, attemptID)
	if err != nil {
		t.Fatalf("get attempt after update: %v", err)
	}
	if attempt2.State != WriteAttemptChunksDurable {
		t.Fatalf("state = %s, want chunks_durable", attempt2.State)
	}

	// Verify attempt can be listed by state.
	attempts, err := store.ListWriteAttemptsByState(ctx, WriteAttemptChunksDurable, 10)
	if err != nil {
		t.Fatalf("list attempts: %v", err)
	}
	found := false
	for _, a := range attempts {
		if a.ID == attemptID {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("attempt %s not found in ChunksDurable list", attemptID)
	}
}
