package metadata

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
)

// TestInodeIDNoReuseAfterStoreReopen is the decisive regression test for the
// inode-ID-reuse durability bug (multi-metad leader-failover drill: 36/99
// objects corrupted): inode IDs were minted by a process-local inodeSeq counter
// reset to RootInodeID at every store construction, with no memory of IDs
// already committed to the store. After a process restart (or a newly elected
// raft leader), the fresh counter re-minted IDs 2,3,4,... and silently
// overwrote the inode rows of unrelated live objects — the old objects' name
// entries kept pointing at the same IDs, which now held the new objects'
// extents.
//
// The fix cold-scans the committed /inode/ keys once (ensureInodeIDMax) and
// raises inodeSeq strictly above the largest committed inode ID before the
// first mint. This non-raft test reproduces the exact mechanism: create under
// a store, close and reopen (fresh counter), create again, and assert zero
// reuse with strict monotonicity — plus a future-dated committed row to prove
// the scan reads committed keys, not process memory.
func TestInodeIDNoReuseAfterStoreReopen(t *testing.T) {
	dir := t.TempDir()
	fresh := func() *PebbleStore {
		st, err := NewPebbleStore(PebbleStoreConfig{Dir: dir, NodeID: 1})
		if err != nil {
			t.Fatalf("store: %v", err)
		}
		return st
	}

	ctx := context.Background()
	setup := fresh()
	if err := setup.CreateBucket(ctx, "fs", PlacementPolicy{ReplicationFactor: 1}); err != nil {
		setup.Close()
		t.Fatalf("create bucket: %v", err)
	}
	bucket, err := setup.GetBucket(ctx, "fs")
	if err != nil {
		setup.Close()
		t.Fatalf("get bucket: %v", err)
	}
	if _, err := setup.CreateFile(ctx, bucket.RootInode, "f.bin", 0o644); err != nil {
		setup.Close()
		t.Fatalf("create file: %v", err)
	}

	// Seed a committed inode row whose ID lies in the FUTURE relative to the
	// restarted counter. This models the real hazard deterministically:
	// without the fix, a freshly restarted counter re-mints 2,3,4,... and
	// collides with (overwrites) committed rows; with the fix, the restart
	// scans committed keys and stays strictly above them.
	futureID := InodeID(999)
	if err := setup.putMsgpack(fmt.Sprintf("%s%d", prefixInode, futureID),
		&InodeMeta{ID: futureID, Type: FileRegular, NLink: 1}); err != nil {
		setup.Close()
		t.Fatalf("seed future inode row: %v", err)
	}
	if err := setup.Close(); err != nil {
		t.Fatalf("close setup store: %v", err)
	}

	// Restart: fresh process, fresh counter (inodeSeq = RootInodeID again).
	create := func(name string) InodeID {
		t.Helper()
		st := fresh()
		defer st.Close()
		bx, err := st.GetBucket(ctx, "fs")
		if err != nil {
			t.Fatalf("GetBucket after reopen: %v", err)
		}
		f, err := st.CreateFile(ctx, bx.RootInode, name, 0o644)
		if err != nil {
			t.Fatalf("CreateFile after reopen: %v", err)
		}
		return f.ID
	}

	// The first post-restart mint must land strictly above the committed max
	// (999), not restart from 2.
	first := create("post-1.bin")
	if first <= futureID {
		t.Fatalf("post-restart inode ID %d is not strictly greater than committed future row %d (reuse would overwrite it)", first, futureID)
	}
	second := create("post-2.bin")
	if second <= first {
		t.Fatalf("post-restart inode IDs not monotonic: %d then %d", first, second)
	}
	t.Logf("OK: post-restart inode IDs %d, %d — strictly above committed max %d, zero reuse", first, second, futureID)
}

// TestRaftClusterInodeIDNoReuseAcrossFailover is the raft-level proof of the
// same fix: a freshly elected leader must not re-issue inode IDs a previous
// leader already committed. The new leader's inodeSeq is process-local and
// reset, so without ensureInodeIDMax it re-mints 2,3,4,... and overwrites the
// first leader's inode rows.
func TestRaftClusterInodeIDNoReuseAcrossFailover(t *testing.T) {
	if testing.Short() {
		t.Skip("real raft integration test")
	}

	cluster := startRealRaftTestCluster(t, 3)
	defer cluster.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	leader := cluster.CreateBucketOnLeader(t, ctx, "fs", PlacementPolicy{ReplicationFactor: 1})
	bucket, err := leader.Store.GetBucket(ctx, "fs")
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}

	// Phase 1: create a batch of files under the initial leader (A).
	var pre []InodeID
	for i := 0; i < 8; i++ {
		f, err := leader.Store.CreateFile(ctx, bucket.RootInode, fmt.Sprintf("pre-%d.bin", i), 0o644)
		if err != nil {
			t.Fatalf("CreateFile (pre-failover): %v", err)
		}
		pre = append(pre, f.ID)
	}
	leaderID := leader.ID
	t.Logf("created %d inode IDs under leader %s: %v", len(pre), leaderID, pre)

	// Phase 2: kill the current leader and fail over to a new one (B).
	cluster.StopNode(t, leaderID)
	failoverCtx, cancelFO := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancelFO()
	newLeader := cluster.WaitForLeader(t, failoverCtx)
	if newLeader.ID == leaderID {
		t.Fatal("expected a new leader after failover")
	}
	t.Logf("failover: leader %s -> %s", leaderID, newLeader.ID)

	// Phase 3: create more files under the new leader and assert strict,
	// collision-free monotonicity across the leadership boundary.
	newBucket, err := newLeader.Store.GetBucket(ctx, "fs")
	if err != nil {
		t.Fatalf("GetBucket on new leader: %v", err)
	}
	for i := 0; i < 6; i++ {
		f, err := newLeader.Store.CreateFile(ctx, newBucket.RootInode, fmt.Sprintf("post-%d.bin", i), 0o644)
		if err != nil {
			t.Fatalf("CreateFile (post-failover): %v", err)
		}
		for _, prev := range pre {
			if f.ID == prev {
				t.Fatalf("inode ID %d reused across leader failover (%s -> %s): the new inode silently overwrites another object's row", f.ID, leaderID, newLeader.ID)
			}
			if f.ID <= prev {
				t.Fatalf("post-failover inode ID %d is not strictly greater than pre-failover ID %d", f.ID, prev)
			}
		}
	}

	t.Logf("OK: %d pre-failover + 6 post-failover inode IDs, zero reuse, strictly monotonic", len(pre))
}

// TestInodeIDGuard_RejectsStaleReuse proves the create-path inode-key
// precondition turns any stale-ID reuse into an explicit ErrEntryExists at
// apply time instead of silently overwriting the live inode row. This is the
// defense-in-depth layer behind the cold-scan seeding: even if a future
// regression ever mints a colliding ID (e.g. during the cold-scan seeding
// window), the create fails loudly and the pre-existing object's row is
// provably untouched.
func TestInodeIDGuard_RejectsStaleReuse(t *testing.T) {
	s := newTestPebbleStore(t)
	ctx := context.Background()

	// A live inode row that a stale ID would collide with.
	const staleID = InodeID(7)
	seed := &InodeMeta{ID: staleID, Type: FileRegular, NLink: 1, Size: 12345}
	if err := s.putMsgpack(fmt.Sprintf("%s%d", prefixInode, staleID), seed); err != nil {
		t.Fatalf("seed inode row: %v", err)
	}

	// A pseudo-create whose nsKey is absent (so the name precondition would
	// pass) but whose inode key collides with the live row — the exact stale-ID
	// reuse the guard must catch.
	nsKey := prefixNS + "5/stale-reuse.bin"
	inodeKey := fmt.Sprintf("%s%d", prefixInode, staleID)
	ops := []batchOp{
		{Key: inodeKey, Value: seed},
		{Key: nsKey, Value: &DirEntry{InodeID: staleID, Type: FileRegular, Name: "stale-reuse.bin"}},
	}
	cond, err := buildNamespaceConditionalWithInodeGuard(nsKey, inodeKey, ops)
	if err != nil {
		t.Fatalf("build guard batch: %v", err)
	}

	err = s.applyNamespaceConditional(ctx, cond, ErrEntryExists)
	if !errors.Is(err, ErrEntryExists) {
		t.Fatalf("stale-ID create = %v, want ErrEntryExists (must not silently overwrite)", err)
	}

	// The live row must be byte-for-byte untouched.
	var got InodeMeta
	found, err := s.getValue(inodeKey, &got)
	if err != nil || !found {
		t.Fatalf("live inode row missing after rejected create: found=%v err=%v", found, err)
	}
	if got.Size != seed.Size || got.NLink != seed.NLink {
		t.Fatalf("live inode row mutated by rejected create: %+v (want size=%d nlink=%d)", got, seed.Size, seed.NLink)
	}
}

// TestInodeIDGuard_RaftWireRoundTrip proves the two-precondition create batch
// round-trips through the raft log codec intact (wire format is
// count-generic, so adding a precondition needs no version bump) and that the
// FSM apply path checks each precondition in turn — a live inode row yields
// ErrRaftConditionalConflict, exactly like an existing chunk in the
// chunk-allocation batch.
func TestInodeIDGuard_RaftWireRoundTrip(t *testing.T) {
	nsKey := prefixNS + "5/wire.bin"
	inodeKey := prefixInode + "7"
	meta := &InodeMeta{ID: 7, Type: FileRegular, NLink: 1}
	cond, err := buildNamespaceConditionalWithInodeGuard(nsKey, inodeKey, []batchOp{
		{Key: inodeKey, Value: meta},
		{Key: nsKey, Value: &DirEntry{InodeID: 7, Type: FileRegular, Name: "wire.bin"}},
	})
	if err != nil {
		t.Fatalf("build guard batch: %v", err)
	}

	data, err := (&RaftLogEntry{Op: OpConditionalBatch, Conditional: cond}).EncodeChecked()
	if err != nil {
		t.Fatalf("encode guard batch: %v", err)
	}
	decoded, err := DecodeRaftLogEntry(data)
	if err != nil {
		t.Fatalf("decode guard batch: %v", err)
	}
	if len(decoded.Conditional.Preconditions) != 2 {
		t.Fatalf("round-trip preconditions = %d, want 2 (nsKey + inodeKey)", len(decoded.Conditional.Preconditions))
	}

	// FSM apply path: seed a live inode row, then apply — each precondition is
	// checked in turn, so the inode guard conflicts and nothing applies.
	db, err := pebble.Open(t.TempDir(), &pebble.Options{})
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	defer db.Close()
	raw, err := marshalValue(meta, codecMsgpack)
	if err != nil {
		t.Fatalf("marshal seed: %v", err)
	}
	if err := db.Set([]byte(inodeKey), raw, pebble.NoSync); err != nil {
		t.Fatalf("seed live inode row: %v", err)
	}
	if err := applyConditionalBatch(db, decoded.Conditional, pebble.NoSync); !errors.Is(err, ErrRaftConditionalConflict) {
		t.Fatalf("apply with live inode row = %v, want ErrRaftConditionalConflict", err)
	}
}

// TestInodeIDFreeListReuseAfterReopen covers the recycling path that the
// cold-scan seeding and the inode-key guard must not break: a deleted inode's
// ID enters the (raft-replicated) free list and is reused on the next create.
// The recycled ID's row was deleted by the same committed unlink, so both the
// monotonic counter and the guard allow it — reuse is by design, and the new
// object must be readable through its name entry.
func TestInodeIDFreeListReuseAfterReopen(t *testing.T) {
	dir := t.TempDir()
	fresh := func() *PebbleStore {
		st, err := NewPebbleStore(PebbleStoreConfig{Dir: dir, NodeID: 1})
		if err != nil {
			t.Fatalf("store: %v", err)
		}
		return st
	}

	ctx := context.Background()
	setup := fresh()
	if err := setup.CreateBucket(ctx, "fs", PlacementPolicy{ReplicationFactor: 1}); err != nil {
		setup.Close()
		t.Fatalf("create bucket: %v", err)
	}
	bucket, err := setup.GetBucket(ctx, "fs")
	if err != nil {
		setup.Close()
		t.Fatalf("get bucket: %v", err)
	}
	del, err := setup.CreateFile(ctx, bucket.RootInode, "del.bin", 0o644)
	if err != nil {
		setup.Close()
		t.Fatalf("create file: %v", err)
	}
	recycledID := del.ID
	if err := setup.Unlink(ctx, bucket.RootInode, "del.bin"); err != nil {
		setup.Close()
		t.Fatalf("unlink: %v", err)
	}
	if err := setup.Close(); err != nil {
		t.Fatalf("close setup store: %v", err)
	}

	// Reopen: restoreFreeList reloads the recycled ID; the next create reuses
	// it (its row is gone), and the guard does not false-positive.
	st := fresh()
	defer st.Close()
	b2, err := st.GetBucket(ctx, "fs")
	if err != nil {
		t.Fatalf("GetBucket after reopen: %v", err)
	}
	f2, err := st.CreateFile(ctx, b2.RootInode, "reuse.bin", 0o644)
	if err != nil {
		t.Fatalf("CreateFile after reopen: %v", err)
	}
	if f2.ID != recycledID {
		t.Fatalf("reopen create ID = %d, want recycled %d", f2.ID, recycledID)
	}
	got, err := st.Lookup(ctx, b2.RootInode, "reuse.bin")
	if err != nil {
		t.Fatalf("Lookup recycled-name: %v", err)
	}
	if got.ID != recycledID {
		t.Fatalf("recycled name entry points at inode %d, want %d", got.ID, recycledID)
	}
	t.Logf("OK: recycled inode ID %d reused after reopen, new object readable", recycledID)
}

// TestCreateFile_ReseedOnInodeKeyConflict proves the re-seed-on-conflict
// retry: a create whose first attempt mints a stale inode ID (under-seeded
// cold-cache scan right after leader election — the FSM had not yet replayed a
// prior leader's inodes) hits the inode-key ExpectAbsent guard, then re-seeds
// the inode high-water mark and succeeds on the second attempt with a fresh ID.
//
// The test forces the under-seeded state directly: inode rows 3 and 5 are
// committed (as if by a prior leader) while inodeSeq is pinned low AND marked
// initialized (as if the cold scan ran before those rows were applied). The
// first CreateFile would mint 3 and conflict on the inode key; the retry must
// mint 6 and succeed.
func TestCreateFile_ReseedOnInodeKeyConflict(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	if err := store.CreateBucket(ctx, "fs", PlacementPolicy{ReplicationFactor: 1}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	bucket, err := store.GetBucket(ctx, "fs")
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}

	// Commit inode rows 3 and 5 directly (a prior leader's already-committed
	// files whose rows this process's cold scan has not yet seen).
	seed := func(id InodeID) {
		now := time.Now().UnixNano()
		if err := store.putMsgpack(fmt.Sprintf("%s%d", prefixInode, id), &InodeMeta{
			ID: id, Type: FileRegular, Mode: 0o644, NLink: 1,
			CTime: now, MTime: now, ATime: now,
		}); err != nil {
			t.Fatalf("seed inode %d: %v", id, err)
		}
	}
	seed(3)
	seed(5)

	// Simulate the under-seeded state: counter below the committed max, but the
	// one-time scan already ran (so ensureInodeIDMax is a no-op until reseeded).
	store.inodeSeq.Store(2)
	store.inodeIDMaxInit.Store(true)

	f, err := store.CreateFile(ctx, bucket.RootInode, "fresh.bin", 0o644)
	if err != nil {
		t.Fatalf("CreateFile (should re-seed and retry): %v", err)
	}
	if f.ID != 6 {
		t.Fatalf("CreateFile returned inode %d, want 6 (above the committed max 5)", f.ID)
	}
	if got := store.inodeSeq.Load(); got != 6 {
		t.Fatalf("inodeSeq = %d, want 6", got)
	}

	// The committed row is really there (the second attempt's mutation landed).
	got, err := store.GetInode(ctx, 6)
	if err != nil || got == nil || got.ID != 6 {
		t.Fatalf("GetInode(6) = %+v, %v", got, err)
	}
}

// TestCreateFile_ReseedDoesNotMaskGenuineSameNameConflict proves the retry
// never converts a real ErrEntryExists (the file name already exists — nsKey
// precondition) into a success: the re-seeded second attempt fails on the same
// nsKey and the original error surfaces.
func TestCreateFile_ReseedDoesNotMaskGenuineSameNameConflict(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	if err := store.CreateBucket(ctx, "fs", PlacementPolicy{ReplicationFactor: 1}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	bucket, err := store.GetBucket(ctx, "fs")
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}
	if _, err := store.CreateFile(ctx, bucket.RootInode, "dup.bin", 0o644); err != nil {
		t.Fatalf("first CreateFile: %v", err)
	}

	// Pin the counter below the committed max with the scan "done" — a
	// same-name create will now conflict on the nsKey first, reseed, and still
	// conflict. It must keep returning ErrEntryExists.
	store.inodeSeq.Store(2)
	store.inodeIDMaxInit.Store(true)

	if _, err := store.CreateFile(ctx, bucket.RootInode, "dup.bin", 0o644); !errors.Is(err, ErrEntryExists) {
		t.Fatalf("CreateFile(dup) = %v, want ErrEntryExists (retry must not mask it)", err)
	}
}
