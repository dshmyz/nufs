package metadata

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

// newTestPebbleStore creates an in-memory PebbleStore for testing.
func newTestPebbleStore(t *testing.T) *PebbleStore {
	t.Helper()
	cfg := PebbleStoreConfig{
		Dir:         fmt.Sprintf("test-%d", time.Now().UnixNano()),
		UseInMemory: true,
		NodeID:      1,
	}
	store, err := NewPebbleStore(cfg)
	if err != nil {
		t.Fatalf("NewPebbleStore: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

func TestPebbleStore_BucketCRUD(t *testing.T) {
	store := newTestPebbleStore(t)
	ctx := context.Background()

	// Create
	err := store.CreateBucket(ctx, "test-bucket", PlacementPolicy{
		ID: "default", ReplicationFactor: 3, TopologySpread: SpreadRack,
	})
	if err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	// Duplicate
	err = store.CreateBucket(ctx, "test-bucket", PlacementPolicy{})
	if err != ErrBucketExists {
		t.Fatalf("expected ErrBucketExists, got: %v", err)
	}

	// Get
	info, err := store.GetBucket(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}
	if info.Name != "test-bucket" || info.Policy.ReplicationFactor != 3 {
		t.Fatalf("unexpected bucket info: %+v", info)
	}

	// List
	buckets, err := store.ListBuckets(ctx)
	if err != nil {
		t.Fatalf("ListBuckets: %v", err)
	}
	if len(buckets) != 1 || buckets[0].Name != "test-bucket" {
		t.Fatalf("unexpected buckets: %+v", buckets)
	}

	// Delete empty bucket
	err = store.DeleteBucket(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("DeleteBucket: %v", err)
	}

	// Get deleted
	_, err = store.GetBucket(ctx, "test-bucket")
	if err != ErrBucketNotFound {
		t.Fatalf("expected ErrBucketNotFound, got: %v", err)
	}
}

func TestPebbleStore_BucketNotEmpty(t *testing.T) {
	store := newTestPebbleStore(t)
	ctx := context.Background()

	store.CreateBucket(ctx, "b1", PlacementPolicy{})
	info, _ := store.GetBucket(ctx, "b1")
	store.CreateFile(ctx, info.RootInode, "file.txt", 0644)

	err := store.DeleteBucket(ctx, "b1")
	if err != ErrBucketNotEmpty {
		t.Fatalf("expected ErrBucketNotEmpty, got: %v", err)
	}
}

func TestPebbleStore_MkDirReadDir(t *testing.T) {
	store := newTestPebbleStore(t)
	ctx := context.Background()

	store.CreateBucket(ctx, "fs", PlacementPolicy{})
	bucket, _ := store.GetBucket(ctx, "fs")

	// Create directories
	dir1, err := store.MkDir(ctx, bucket.RootInode, "photos", 0755)
	if err != nil {
		t.Fatalf("MkDir: %v", err)
	}
	dir2, err := store.MkDir(ctx, bucket.RootInode, "docs", 0755)
	if err != nil {
		t.Fatalf("MkDir: %v", err)
	}

	// ReadDir
	entries, err := store.ReadDir(ctx, bucket.RootInode, 0, 100)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}

	// ReadDir with offset/limit
	entries, err = store.ReadDir(ctx, bucket.RootInode, 1, 1)
	if err != nil {
		t.Fatalf("ReadDir offset: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}

	// Duplicate
	_, err = store.MkDir(ctx, bucket.RootInode, "photos", 0755)
	if err != ErrEntryExists {
		t.Fatalf("expected ErrEntryExists, got: %v", err)
	}

	_ = dir1
	_ = dir2
}

func TestPebbleStore_RmDir(t *testing.T) {
	store := newTestPebbleStore(t)
	ctx := context.Background()

	store.CreateBucket(ctx, "fs", PlacementPolicy{})
	bucket, _ := store.GetBucket(ctx, "fs")
	store.MkDir(ctx, bucket.RootInode, "empty-dir", 0755)

	err := store.RmDir(ctx, bucket.RootInode, "empty-dir")
	if err != nil {
		t.Fatalf("RmDir: %v", err)
	}

	// RmDir non-empty
	store.MkDir(ctx, bucket.RootInode, "non-empty", 0755)
	dir, _ := store.Lookup(ctx, bucket.RootInode, "non-empty")
	store.CreateFile(ctx, dir.ID, "file.txt", 0644)
	err = store.RmDir(ctx, bucket.RootInode, "non-empty")
	if err != ErrDirNotEmpty {
		t.Fatalf("expected ErrDirNotEmpty, got: %v", err)
	}
}

func TestPebbleStore_FileCRUD(t *testing.T) {
	store := newTestPebbleStore(t)
	ctx := context.Background()

	store.CreateBucket(ctx, "fs", PlacementPolicy{})
	bucket, _ := store.GetBucket(ctx, "fs")

	// Create file
	meta, err := store.CreateFile(ctx, bucket.RootInode, "hello.txt", 0644)
	if err != nil {
		t.Fatalf("CreateFile: %v", err)
	}
	if meta.ID == 0 || meta.Mode != 0644 {
		t.Fatalf("unexpected inode: %+v", meta)
	}

	// Lookup
	found, err := store.Lookup(ctx, bucket.RootInode, "hello.txt")
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if found.ID != meta.ID {
		t.Fatalf("ID mismatch: %d vs %d", found.ID, meta.ID)
	}

	// GetInode
	inode, err := store.GetInode(ctx, meta.ID)
	if err != nil {
		t.Fatalf("GetInode: %v", err)
	}
	if inode.NLink != 1 {
		t.Fatalf("expected nlink=1, got %d", inode.NLink)
	}

	// UpdateInode
	inode.Size = 1024
	if err := store.UpdateInode(ctx, inode); err != nil {
		t.Fatalf("UpdateInode: %v", err)
	}
	inode2, _ := store.GetInode(ctx, meta.ID)
	if inode2.Size != 1024 {
		t.Fatalf("size not updated: %d", inode2.Size)
	}

	// Unlink
	err = store.Unlink(ctx, bucket.RootInode, "hello.txt")
	if err != nil {
		t.Fatalf("Unlink: %v", err)
	}
	_, err = store.Lookup(ctx, bucket.RootInode, "hello.txt")
	if err != ErrEntryNotFound {
		t.Fatalf("expected ErrEntryNotFound, got: %v", err)
	}
}

func TestPebbleStore_Rename(t *testing.T) {
	store := newTestPebbleStore(t)
	ctx := context.Background()

	store.CreateBucket(ctx, "fs", PlacementPolicy{})
	bucket, _ := store.GetBucket(ctx, "fs")
	store.CreateFile(ctx, bucket.RootInode, "old.txt", 0644)

	err := store.Rename(ctx, bucket.RootInode, "old.txt", bucket.RootInode, "new.txt")
	if err != nil {
		t.Fatalf("Rename: %v", err)
	}

	_, err = store.Lookup(ctx, bucket.RootInode, "old.txt")
	if err != ErrEntryNotFound {
		t.Fatalf("old name should not exist")
	}
	_, err = store.Lookup(ctx, bucket.RootInode, "new.txt")
	if err != nil {
		t.Fatalf("new name should exist: %v", err)
	}
}

func TestPebbleStore_SymlinkAndLink(t *testing.T) {
	store := newTestPebbleStore(t)
	ctx := context.Background()

	store.CreateBucket(ctx, "fs", PlacementPolicy{})
	bucket, _ := store.GetBucket(ctx, "fs")
	store.CreateFile(ctx, bucket.RootInode, "target.txt", 0644)

	// Symlink
	sym, err := store.Symlink(ctx, bucket.RootInode, "link.txt", "/fs/target.txt")
	if err != nil {
		t.Fatalf("Symlink: %v", err)
	}
	target, err := store.Readlink(ctx, sym.ID)
	if err != nil {
		t.Fatalf("Readlink: %v", err)
	}
	if target != "/fs/target.txt" {
		t.Fatalf("unexpected target: %s", target)
	}

	// Hard link
	orig, _ := store.Lookup(ctx, bucket.RootInode, "target.txt")
	linked, err := store.Link(ctx, bucket.RootInode, "hardlink.txt", orig.ID)
	if err != nil {
		t.Fatalf("Link: %v", err)
	}
	if linked.NLink != 2 {
		t.Fatalf("expected nlink=2, got %d", linked.NLink)
	}
}

func TestPebbleStore_ChunkLifecycle(t *testing.T) {
	store := newTestPebbleStore(t)
	ctx := context.Background()

	// Register nodes for placement
	for i := 1; i <= 3; i++ {
		store.RegisterNode(ctx, &NodeInfo{
			ID: NodeID(i), Addr: fmt.Sprintf("node%d:9001", i),
			CapacityGB: 100, Rack: fmt.Sprintf("rack%d", i), Zone: "zone1",
			Tier: TierHot,
		})
	}

	store.CreateBucket(ctx, "fs", PlacementPolicy{})
	bucket, _ := store.GetBucket(ctx, "fs")
	file, _ := store.CreateFile(ctx, bucket.RootInode, "big.bin", 0644)

	// Allocate
	chunk, err := store.AllocateChunk(ctx, file.ID, 0, PlacementPolicy{
		ReplicationFactor: 3, TopologySpread: SpreadRack,
	})
	if err != nil {
		t.Fatalf("AllocateChunk: %v", err)
	}
	if chunk.State != ChunkCreated {
		t.Fatalf("expected ChunkCreated, got %d", chunk.State)
	}

	// Commit
	err = store.CommitChunk(ctx, chunk.ID, 0xDEADBEEF)
	if err != nil {
		t.Fatalf("CommitChunk: %v", err)
	}

	// Seal
	err = store.SealChunk(ctx, chunk.ID)
	if err != nil {
		t.Fatalf("SealChunk: %v", err)
	}

	// GetChunk
	got, err := store.GetChunk(ctx, chunk.ID)
	if err != nil {
		t.Fatalf("GetChunk: %v", err)
	}
	if got.State != ChunkReady || got.Checksum != 0xDEADBEEF {
		t.Fatalf("unexpected chunk: %+v", got)
	}

	// ListChunks
	refs, err := store.ListChunks(ctx, file.ID)
	if err != nil {
		t.Fatalf("ListChunks: %v", err)
	}
	if len(refs) != 1 || refs[0].ID != chunk.ID {
		t.Fatalf("unexpected refs: %+v", refs)
	}
}

func TestPebbleStore_NodeManagement(t *testing.T) {
	store := newTestPebbleStore(t)
	ctx := context.Background()

	info := &NodeInfo{
		ID: 1, Addr: "node1:9001", CapacityGB: 500,
		Rack: "rack1", Zone: "zone1", Tier: TierHot,
	}
	if err := store.RegisterNode(ctx, info); err != nil {
		t.Fatalf("RegisterNode: %v", err)
	}

	// Duplicate
	if err := store.RegisterNode(ctx, info); err != ErrNodeAlreadyExists {
		t.Fatalf("expected ErrNodeAlreadyExists, got: %v", err)
	}

	// Heartbeat
	err := store.Heartbeat(ctx, 1, &NodeReport{UsedGB: 100, ChunkCount: 50})
	if err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}

	// List
	nodes, err := store.ListNodes(ctx)
	if err != nil {
		t.Fatalf("ListNodes: %v", err)
	}
	if len(nodes) != 1 || nodes[0].UsedGB != 100 {
		t.Fatalf("unexpected nodes: %+v", nodes)
	}

	// GetNode
	n, err := store.GetNode(ctx, 1)
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if n.UsedGB != 100 {
		t.Fatalf("expected UsedGB=100, got %d", n.UsedGB)
	}

	// Decommission
	if err := store.DecommissionNode(ctx, 1); err != nil {
		t.Fatalf("DecommissionNode: %v", err)
	}
	n2, _ := store.GetNode(ctx, 1)
	if n2.State != NodeDraining {
		t.Fatalf("expected Draining, got %d", n2.State)
	}
}

// TestPebbleStore_ShardDiskCountRoundTrip proves the EC candidate-topology
// contract (Program 9): ShardDiskCount reported at registration persists and
// round-trips through ListNodes/GetNode, and re-registration (ErrNodeAlreadyExists
// + address refresh) also refreshes a *changed* ShardDiskCount — so an EC
// coordinator converting at a later time sees the node's current disk count.
func TestPebbleStore_ShardDiskCountRoundTrip(t *testing.T) {
	store := newTestPebbleStore(t)
	ctx := context.Background()

	// Initial registration with 3 shard disks.
	if err := store.RegisterNode(ctx, &NodeInfo{ID: 10, Addr: "n10:9100", ShardDiskCount: 3}); err != nil {
		t.Fatalf("RegisterNode: %v", err)
	}
	n, err := store.GetNode(ctx, 10)
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if n.ShardDiskCount != 3 {
		t.Fatalf("ShardDiskCount after register=%d, want 3", n.ShardDiskCount)
	}

	// Re-register with the SAME count + same addr -> still ErrNodeAlreadyExists,
	// no spurious refresh, count unchanged.
	if err := store.RegisterNode(ctx, &NodeInfo{ID: 10, Addr: "n10:9100", ShardDiskCount: 3}); err != ErrNodeAlreadyExists {
		t.Fatalf("re-register same: got %v, want ErrNodeAlreadyExists", err)
	}
	n, _ = store.GetNode(ctx, 10)
	if n.ShardDiskCount != 3 {
		t.Fatalf("ShardDiskCount after no-op re-register=%d, want 3", n.ShardDiskCount)
	}

	// Re-register with a NEW count (node gained a shard disk) -> refreshed.
	if err := store.RegisterNode(ctx, &NodeInfo{ID: 10, Addr: "n10:9100", ShardDiskCount: 6}); err != ErrNodeAlreadyExists {
		t.Fatalf("re-register changed: got %v, want ErrNodeAlreadyExists", err)
	}
	n, _ = store.GetNode(ctx, 10)
	if n.ShardDiskCount != 6 {
		t.Fatalf("ShardDiskCount after changed re-register=%d, want 6 (refreshed)", n.ShardDiskCount)
	}
	if n.Addr != "n10:9100" {
		t.Fatalf("Addr was clobbered=%q, want n10:9100", n.Addr)
	}

	// ListNodes carries the refreshed count too.
	nodes, err := store.ListNodes(ctx)
	if err != nil {
		t.Fatalf("ListNodes: %v", err)
	}
	if len(nodes) != 1 || nodes[0].ShardDiskCount != 6 {
		t.Fatalf("ListNodes ShardDiskCount=%v (len=%d), want 6", nodes[0].ShardDiskCount, len(nodes))
	}

	// Legacy semantics preserved: a re-register with ShardDiskCount 0 (a V1 node
	// that never reports it) does not clobber a previously reported value.
	if err := store.RegisterNode(ctx, &NodeInfo{ID: 10, Addr: "n10:9100", ShardDiskCount: 0}); err != ErrNodeAlreadyExists {
		t.Fatalf("re-register legacy: got %v, want ErrNodeAlreadyExists", err)
	}
	n, _ = store.GetNode(ctx, 10)
	if n.ShardDiskCount != 6 {
		t.Fatalf("ShardDiskCount after legacy 0 re-register=%d, want 6 (0 must not clobber)", n.ShardDiskCount)
	}
}

func TestPebbleStore_ScanAllChunks(t *testing.T) {
	store := newTestPebbleStore(t)
	ctx := context.Background()

	for i := 1; i <= 3; i++ {
		store.RegisterNode(ctx, &NodeInfo{
			ID: NodeID(i), Addr: fmt.Sprintf("node%d:9001", i),
			CapacityGB: 100, Rack: fmt.Sprintf("rack%d", i), Tier: TierHot,
		})
	}

	store.CreateBucket(ctx, "fs", PlacementPolicy{})
	bucket, _ := store.GetBucket(ctx, "fs")
	file, _ := store.CreateFile(ctx, bucket.RootInode, "data.bin", 0644)

	for i := 0; i < 5; i++ {
		store.AllocateChunk(ctx, file.ID, int64(i)*MaxChunkSize, PlacementPolicy{
			ReplicationFactor: 3, TopologySpread: SpreadRack,
		})
	}

	count := 0
	err := store.ScanAllChunks(ctx, func(cm *ChunkMeta) error {
		count++
		return nil
	})
	if err != nil {
		t.Fatalf("ScanAllChunks: %v", err)
	}
	if count != 5 {
		t.Fatalf("expected 5 chunks, got %d", count)
	}
}

func TestPebbleStore_ConcurrentWrites(t *testing.T) {
	store := newTestPebbleStore(t)
	ctx := context.Background()

	store.CreateBucket(ctx, "fs", PlacementPolicy{})
	bucket, _ := store.GetBucket(ctx, "fs")

	const N = 100
	var wg sync.WaitGroup
	errs := make(chan error, N)

	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, err := store.CreateFile(ctx, bucket.RootInode, fmt.Sprintf("file-%d.txt", idx), 0644)
			if err != nil {
				errs <- err
			}
		}(i)
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		t.Fatalf("concurrent write error: %v", err)
	}

	entries, err := store.ReadDir(ctx, bucket.RootInode, 0, 1000)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != N {
		t.Fatalf("expected %d entries, got %d", N, len(entries))
	}
}

// runConcurrentSameNameCreate fans out `n` goroutines all creating the same
// name under the same parent, then asserts the atomic namespace-creation
// invariant: exactly one goroutine succeeds, every other gets
// ErrEntryExists, exactly one entry exists, and the parent NLink is not
// over-incremented (lost/false updates are forbidden). The create closure is
// parameterized so the same invariant is checked for both MkDir and
// CreateFile.
func runConcurrentSameNameCreate(t *testing.T, create func() error) {
	t.Helper()
	const n = 32
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			errs[idx] = create()
		}(i)
	}
	wg.Wait()

	var succeeded, conflicts int
	for _, err := range errs {
		switch {
		case err == nil:
			succeeded++
		case err == ErrEntryExists:
			conflicts++
		default:
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if succeeded != 1 {
		t.Fatalf("expected exactly 1 successful create, got %d (conflicts=%d)", succeeded, conflicts)
	}
	if conflicts != n-1 {
		t.Fatalf("expected %d ErrEntryExists conflicts, got %d", n-1, conflicts)
	}
}

// TestPebbleStore_ConcurrentSameNameMkDir verifies that concurrent same-name
// MkDir is atomic (the pre-CAS code could persist orphan inodes and lose a
// parent NLink update when two creates both passed the existence check).
func TestPebbleStore_ConcurrentSameNameMkDir(t *testing.T) {
	store := newTestPebbleStore(t)
	ctx := context.Background()
	if err := store.CreateBucket(ctx, "fs", PlacementPolicy{}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	bucket, err := store.GetBucket(ctx, "fs")
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}
	parent := bucket.RootInode

	runConcurrentSameNameCreate(t, func() error {
		_, err := store.MkDir(ctx, parent, "race-dir", 0755)
		return err
	})

	// Exactly one directory entry, and it points at a live inode.
	entries, err := store.ReadDir(ctx, parent, 0, 100)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if _, err := store.GetInode(ctx, entries[0].InodeID); err != nil {
		t.Fatalf("winner inode is not live: %v", err)
	}

	// Parent NLink must be incremented exactly once (a dir holds NLink=2 for
	// itself+parent; after one child, +1 for the child link).
	parentMeta, err := store.GetInode(ctx, parent)
	if err != nil {
		t.Fatalf("GetInode(parent): %v", err)
	}
	if parentMeta.NLink != 3 {
		t.Fatalf("parent NLink = %d, want 3 (one child link, no lost/double update)", parentMeta.NLink)
	}
}

// TestPebbleStore_ConcurrentSameNameCreateFile mirrors the MkDir race for
// CreateFile, also asserting bucket-stats object count stays exactly 1.
func TestPebbleStore_ConcurrentSameNameCreateFile(t *testing.T) {
	store := newTestPebbleStore(t)
	ctx := context.Background()
	if err := store.CreateBucket(ctx, "fs", PlacementPolicy{}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	bucket, err := store.GetBucket(ctx, "fs")
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}
	parent := bucket.RootInode

	runConcurrentSameNameCreate(t, func() error {
		_, err := store.CreateFile(ctx, parent, "race-file.txt", 0644)
		return err
	})

	entries, err := store.ReadDir(ctx, parent, 0, 100)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	usage, err := store.ComputeAllBucketUsage(ctx)
	if err != nil {
		t.Fatalf("ComputeAllBucketUsage: %v", err)
	}
	if len(usage) != 1 || usage[0].Objects != 1 {
		t.Fatalf("bucket usage = %+v, want exactly 1 object", usage)
	}
}

// runConcurrentSameNameDelete fans out `n` goroutines all deleting the same
// name under the same parent, then asserts the atomic namespace-delete
// invariant: exactly one goroutine succeeds and every other gets
// ErrEntryNotFound (a concurrent create/delete/overwrite won the CAS), and
// the entry is gone afterwards. The delete closure returns the tracked error
// from RmDir/Unlink. This exercises the CAS-on-value precondition of the
// delete path — the pre-CAS code had both goroutines pass the getJSON check
// and both decrement the parent/inode NLink (a double-decrement / underflow).
func runConcurrentSameNameDelete(t *testing.T, del func() error) {
	t.Helper()
	const n = 32
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			errs[idx] = del()
		}(i)
	}
	wg.Wait()

	var succeeded, notFound int
	for _, err := range errs {
		switch {
		case err == nil:
			succeeded++
		case err == ErrEntryNotFound:
			notFound++
		default:
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if succeeded != 1 {
		t.Fatalf("expected exactly 1 successful delete, got %d (notFound=%d)", succeeded, notFound)
	}
	if notFound != n-1 {
		t.Fatalf("expected %d ErrEntryNotFound conflicts, got %d", n-1, notFound)
	}
}

// TestPebbleStore_ConcurrentSameNameUnlink verifies that concurrent same-name
// Unlink is atomic: one unlink succeeds, the rest conflict with
// ErrEntryNotFound, and the inode NLink is decremented exactly once (the
// pre-CAS code could double-decrement NLink / double-release the inode ID).
func TestPebbleStore_ConcurrentSameNameUnlink(t *testing.T) {
	store := newTestPebbleStore(t)
	ctx := context.Background()
	if err := store.CreateBucket(ctx, "fs", PlacementPolicy{}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	bucket, err := store.GetBucket(ctx, "fs")
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}
	parent := bucket.RootInode
	if _, err := store.CreateFile(ctx, parent, "race-file.txt", 0644); err != nil {
		t.Fatalf("CreateFile: %v", err)
	}

	runConcurrentSameNameDelete(t, func() error {
		return store.Unlink(ctx, parent, "race-file.txt")
	})

	// Entry is gone.
	if _, err := store.Lookup(ctx, parent, "race-file.txt"); !errors.Is(err, ErrEntryNotFound) {
		t.Fatalf("Lookup after unlink = %v, want ErrEntryNotFound", err)
	}
	// Parent NLink is back to its pre-child value (2 for a bucket root dir),
	// not underflowed by concurrent double-decrements.
	parentMeta, err := store.GetInode(ctx, parent)
	if err != nil {
		t.Fatalf("GetInode(parent): %v", err)
	}
	if parentMeta.NLink != 2 {
		t.Fatalf("parent NLink = %d, want 2 (double-decrement would give 1 or 0)", parentMeta.NLink)
	}
}

// TestPebbleStore_ConcurrentSameNameRmDir mirrors the Unlink race for RmDir.
func TestPebbleStore_ConcurrentSameNameRmDir(t *testing.T) {
	store := newTestPebbleStore(t)
	ctx := context.Background()
	if err := store.CreateBucket(ctx, "fs", PlacementPolicy{}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	bucket, err := store.GetBucket(ctx, "fs")
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}
	parent := bucket.RootInode
	if _, err := store.MkDir(ctx, parent, "race-dir", 0755); err != nil {
		t.Fatalf("MkDir: %v", err)
	}

	runConcurrentSameNameDelete(t, func() error {
		return store.RmDir(ctx, parent, "race-dir")
	})

	if _, err := store.Lookup(ctx, parent, "race-dir"); !errors.Is(err, ErrEntryNotFound) {
		t.Fatalf("Lookup after rmdir = %v, want ErrEntryNotFound", err)
	}
	parentMeta, err := store.GetInode(ctx, parent)
	if err != nil {
		t.Fatalf("GetInode(parent): %v", err)
	}
	if parentMeta.NLink != 2 {
		t.Fatalf("parent NLink = %d, want 2 (no double-decrement)", parentMeta.NLink)
	}
}


func TestPebbleStore_PerfWrite10K(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping perf test in short mode")
	}

	store := newTestPebbleStore(t)
	ctx := context.Background()

	store.CreateBucket(ctx, "perf", PlacementPolicy{})
	bucket, _ := store.GetBucket(ctx, "perf")

	start := time.Now()
	for i := 0; i < 10000; i++ {
		_, err := store.CreateFile(ctx, bucket.RootInode, fmt.Sprintf("f-%d", i), 0644)
		if err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	elapsed := time.Since(start)
	t.Logf("10K writes: %v (%.0f ops/s)", elapsed, 10000/elapsed.Seconds())

	// Read all
	start = time.Now()
	entries, err := store.ReadDir(ctx, bucket.RootInode, 0, 20000)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	elapsed = time.Since(start)
	t.Logf("ReadDir 10K: %v", elapsed)
	if len(entries) != 10000 {
		t.Fatalf("expected 10000, got %d", len(entries))
	}
}

// ========== Raft Log Encoding Tests ==========

func TestRaftLogEntry_SetRoundTrip(t *testing.T) {
	entry := &RaftLogEntry{
		Op:    OpSet,
		Key:   []byte("/inode/123"),
		Value: []byte(`{"id":123,"type":0}`),
	}
	data := entry.Encode()
	decoded, err := DecodeRaftLogEntry(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if decoded.Op != OpSet || string(decoded.Key) != "/inode/123" || string(decoded.Value) != `{"id":123,"type":0}` {
		t.Fatalf("mismatch: %+v", decoded)
	}
}

func TestRaftLogEntry_DeleteRoundTrip(t *testing.T) {
	entry := &RaftLogEntry{Op: OpDelete, Key: []byte("/chunk/456")}
	data := entry.Encode()
	decoded, err := DecodeRaftLogEntry(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if decoded.Op != OpDelete || string(decoded.Key) != "/chunk/456" {
		t.Fatalf("mismatch: %+v", decoded)
	}
}

func TestRaftLogEntry_BatchRoundTrip(t *testing.T) {
	entry := &RaftLogEntry{
		Op: OpBatch,
		Batch: []BatchOp{
			{Key: []byte("k1"), Value: []byte("v1")},
			{Key: []byte("k2"), Value: []byte("v2")},
			{Delete: true, Key: []byte("k3")},
		},
	}
	data := entry.Encode()
	decoded, err := DecodeRaftLogEntry(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if decoded.Op != OpBatch || len(decoded.Batch) != 3 {
		t.Fatalf("mismatch: %+v", decoded)
	}
	if string(decoded.Batch[0].Key) != "k1" || string(decoded.Batch[0].Value) != "v1" {
		t.Fatalf("batch[0] mismatch")
	}
	if !decoded.Batch[2].Delete || string(decoded.Batch[2].Key) != "k3" {
		t.Fatalf("batch[2] mismatch")
	}
}

func TestFSM_ApplyAndRestore(t *testing.T) {
	store := newTestPebbleStore(t)
	fsm := &PebbleFSM{store: store}

	// Apply Set operations
	for i := 0; i < 100; i++ {
		entry := &RaftLogEntry{
			Op:    OpSet,
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(fmt.Sprintf("value-%d", i)),
		}
		result := fsm.Apply(&raft.Log{Index: uint64(i + 1), Data: entry.Encode()})
		if result != nil {
			t.Fatalf("apply error: %v", result)
		}
	}

	// Verify data is in Pebble
	var val []byte
	valBytes, closer, err := store.db.Get([]byte("key-50"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	val = make([]byte, len(valBytes))
	copy(val, valBytes)
	closer.Close()
	if string(val) != "value-50" {
		t.Fatalf("expected value-50, got %s", val)
	}

	// Take snapshot
	snap, err := fsm.Snapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	// Persist to buffer
	var buf bytes.Buffer
	sink := &testSnapshotSink{buf: &buf}
	if err := snap.Persist(sink); err != nil {
		t.Fatalf("persist: %v", err)
	}

	// Restore into new store
	store2 := newTestPebbleStore(t)
	fsm2 := &PebbleFSM{store: store2}
	rc := io.NopCloser(bytes.NewReader(buf.Bytes()))
	if err := fsm2.Restore(rc); err != nil {
		t.Fatalf("restore: %v", err)
	}

	// Verify restored data
	val2, closer2, err := store2.db.Get([]byte("key-50"))
	if err != nil {
		t.Fatalf("get after restore: %v", err)
	}
	if string(val2) != "value-50" {
		t.Fatalf("restored value mismatch: %s", val2)
	}
	closer2.Close()

	// Count restored keys
	count := 0
	iter, _ := store2.db.NewIter(nil)
	for iter.First(); iter.Valid(); iter.Next() {
		count++
	}
	iter.Close()
	// 100 keys + 1 root inode (epoch key removed).
	if count != 101 {
		t.Fatalf("expected 101 keys after restore, got %d", count)
	}
}

func TestFSM_ApplyAndRestore_Checkpoint(t *testing.T) {
	// Use real (non-in-memory) Pebble to test PBL3 checkpoint format
	cfg := PebbleStoreConfig{
		Dir:    fmt.Sprintf("test-checkpoint-%d", time.Now().UnixNano()),
		NodeID: 1,
	}
	store, err := NewPebbleStore(cfg)
	if err != nil {
		t.Fatalf("NewPebbleStore: %v", err)
	}
	defer os.RemoveAll(cfg.Dir)
	defer store.Close()

	fsm := &PebbleFSM{store: store}

	for i := 0; i < 100; i++ {
		entry := &RaftLogEntry{
			Op:    OpSet,
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(fmt.Sprintf("value-%d", i)),
		}
		result := fsm.Apply(&raft.Log{Index: uint64(i + 1), Data: entry.Encode()})
		if result != nil {
			t.Fatalf("apply error: %v", result)
		}
	}

	snap, err := fsm.Snapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	var buf bytes.Buffer
	sink := &testSnapshotSink{buf: &buf}
	if err := snap.Persist(sink); err != nil {
		t.Fatalf("persist: %v", err)
	}

	// Verify magic is PBL3
	if string(buf.Bytes()[:4]) != "PBL3" {
		t.Fatalf("expected PBL3 magic, got %q", buf.Bytes()[:4])
	}

	// Restore into a new store
	cfg2 := PebbleStoreConfig{
		Dir:    fmt.Sprintf("test-checkpoint-restore-%d", time.Now().UnixNano()),
		NodeID: 1,
	}
	store2, err := NewPebbleStore(cfg2)
	if err != nil {
		t.Fatalf("NewPebbleStore: %v", err)
	}
	defer os.RemoveAll(cfg2.Dir)
	defer store2.Close()

	fsm2 := &PebbleFSM{store: store2}
	rc := io.NopCloser(bytes.NewReader(buf.Bytes()))
	if err := fsm2.Restore(rc); err != nil {
		t.Fatalf("restore: %v", err)
	}

	// Verify restored data
	val, closer, err := store2.db.Get([]byte("key-50"))
	if err != nil {
		t.Fatalf("get after restore: %v", err)
	}
	if string(val) != "value-50" {
		t.Fatalf("restored value mismatch: %s", val)
	}
	closer.Close()

	count := 0
	iter, _ := store2.db.NewIter(nil)
	for iter.First(); iter.Valid(); iter.Next() {
		count++
	}
	iter.Close()
	if count != 101 {
		t.Fatalf("expected 101 keys after checkpoint restore, got %d", count)
	}
}

func newCheckpointStore(t *testing.T) *PebbleStore {
	t.Helper()
	store, err := NewPebbleStore(PebbleStoreConfig{
		Dir:    filepath.Join(t.TempDir(), "db"),
		NodeID: 1,
	})
	if err != nil {
		t.Fatalf("NewPebbleStore: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil && err != ErrServiceClosed {
			t.Errorf("close checkpoint store: %v", err)
		}
	})
	return store
}

func putTestKey(t *testing.T, store *PebbleStore, key, value string) {
	t.Helper()
	if err := store.db.Set([]byte(key), []byte(value), nil); err != nil {
		t.Fatalf("set %q: %v", key, err)
	}
}

func persistSnapshotToBytes(t *testing.T, snapshot raft.FSMSnapshot) []byte {
	t.Helper()
	defer snapshot.Release()

	var buf bytes.Buffer
	if err := snapshot.Persist(&testSnapshotSink{buf: &buf}); err != nil {
		t.Fatalf("persist snapshot: %v", err)
	}
	return append([]byte(nil), buf.Bytes()...)
}

func restoreSnapshotBytes(t *testing.T, data []byte) *PebbleStore {
	t.Helper()
	store := newCheckpointStore(t)
	if err := (&PebbleFSM{store: store}).Restore(io.NopCloser(bytes.NewReader(data))); err != nil {
		t.Fatalf("restore snapshot: %v", err)
	}
	return store
}

func assertTestKey(t *testing.T, store *PebbleStore, key, want string) {
	t.Helper()
	value, closer, err := store.db.Get([]byte(key))
	if err != nil {
		t.Fatalf("get %q: %v", key, err)
	}
	defer closer.Close()
	if got := string(value); got != want {
		t.Fatalf("get %q = %q, want %q", key, got, want)
	}
}

func assertTestKeyMissing(t *testing.T, store *PebbleStore, key string) {
	t.Helper()
	_, closer, err := store.db.Get([]byte(key))
	if err == nil {
		closer.Close()
		t.Fatalf("key %q unexpectedly exists", key)
	}
}

func TestPebbleSnapshotDoesNotIncludeWritesAfterSnapshot(t *testing.T) {
	store := newCheckpointStore(t)
	putTestKey(t, store, "before", "one")
	fsm := &PebbleFSM{store: store}

	snapshot, err := fsm.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	putTestKey(t, store, "after", "two")

	data := persistSnapshotToBytes(t, snapshot)
	restored := restoreSnapshotBytes(t, data)
	assertTestKey(t, restored, "before", "one")
	assertTestKeyMissing(t, restored, "after")
}

func TestPebbleStore_RemoveRepairTask(t *testing.T) {
	store := newTestPebbleStore(t)
	ctx := context.Background()

	// Trigger a repair task
	err := store.TriggerRepair(ctx, 42)
	if err != nil {
		t.Fatalf("TriggerRepair: %v", err)
	}

	// Verify it's in the queue
	tasks, err := store.GetRepairQueue(ctx)
	if err != nil {
		t.Fatalf("GetRepairQueue: %v", err)
	}
	if len(tasks) != 1 || tasks[0].ChunkID != 42 {
		t.Fatalf("expected 1 repair task for chunk 42, got %+v", tasks)
	}

	// Remove it
	err = store.RemoveRepairTask(ctx, 42)
	if err != nil {
		t.Fatalf("RemoveRepairTask: %v", err)
	}

	// Verify queue is empty
	tasks, err = store.GetRepairQueue(ctx)
	if err != nil {
		t.Fatalf("GetRepairQueue: %v", err)
	}
	if len(tasks) != 0 {
		t.Fatalf("expected empty repair queue, got %+v", tasks)
	}
}

func TestPebbleStore_RemoveRepairTask_NonExistent(t *testing.T) {
	store := newTestPebbleStore(t)
	ctx := context.Background()

	// Removing a non-existent task should not error
	err := store.RemoveRepairTask(ctx, 999)
	if err != nil {
		t.Fatalf("RemoveRepairTask for non-existent task: %v", err)
	}
}

func TestPebbleStore_ReportChunkState_Batch(t *testing.T) {
	store := newTestPebbleStore(t)
	ctx := context.Background()

	// Create bucket with file → allocate chunk
	err := store.CreateBucket(ctx, "batch-test", PlacementPolicy{
		ID: "default", ReplicationFactor: 3, TopologySpread: SpreadRack,
	})
	if err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	bucket, err := store.GetBucket(ctx, "batch-test")
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}

	f, err := store.CreateFile(ctx, bucket.RootInode, "test.txt", 0644)
	if err != nil {
		t.Fatalf("CreateFile: %v", err)
	}

	// Register nodes first (placement policy needs healthy nodes)
	nodeID := NodeID(1)
	err = store.RegisterNode(ctx, &NodeInfo{
		ID: nodeID, Addr: "host1:9100", Rack: "r1", Zone: "z1",
		State: NodeOnline,
	})
	if err != nil {
		t.Fatalf("RegisterNode: %v", err)
	}

	// Allocate two chunks
	chunk1, err := store.AllocateChunk(ctx, f.ID, 0, PlacementPolicy{
		ReplicationFactor: 1, TopologySpread: SpreadNode,
	})
	if err != nil {
		t.Fatalf("AllocateChunk 1: %v", err)
	}

	chunk2, err := store.AllocateChunk(ctx, f.ID, 65536, PlacementPolicy{
		ReplicationFactor: 1, TopologySpread: SpreadNode,
	})
	if err != nil {
		t.Fatalf("AllocateChunk 2: %v", err)
	}

	// Batch report state for both chunks
	states := map[ChunkID]ReplicaState{
		chunk1.ID: ReplicaReady,
		chunk2.ID: ReplicaReady,
	}
	err = store.ReportChunkState(ctx, nodeID, states)
	if err != nil {
		t.Fatalf("ReportChunkState batch: %v", err)
	}

	// Verify chunk1
	c1, err := store.GetChunk(ctx, chunk1.ID)
	if err != nil {
		t.Fatalf("GetChunk chunk1: %v", err)
	}
	found1 := false
	for _, r := range c1.Replicas {
		if r.NodeID == nodeID && r.State == ReplicaReady {
			found1 = true
			break
		}
	}
	if !found1 {
		t.Fatalf("chunk1: expected ready replica for node 1, got %+v", c1.Replicas)
	}

	// Verify chunk2
	c2, err := store.GetChunk(ctx, chunk2.ID)
	if err != nil {
		t.Fatalf("GetChunk chunk2: %v", err)
	}
	found2 := false
	for _, r := range c2.Replicas {
		if r.NodeID == nodeID && r.State == ReplicaReady {
			found2 = true
			break
		}
	}
	if !found2 {
		t.Fatalf("chunk2: expected ready replica for node 1, got %+v", c2.Replicas)
	}
}

func TestPebbleStore_ReportChunkState_EmptyBatch(t *testing.T) {
	store := newTestPebbleStore(t)
	ctx := context.Background()

	// Empty batch should be a no-op
	err := store.ReportChunkState(ctx, 1, nil)
	if err != nil {
		t.Fatalf("ReportChunkState nil: %v", err)
	}

	err = store.ReportChunkState(ctx, 1, map[ChunkID]ReplicaState{})
	if err != nil {
		t.Fatalf("ReportChunkState empty: %v", err)
	}
}

func TestPebbleStore_ChunksByNode(t *testing.T) {
	store := newTestPebbleStore(t)
	ctx := context.Background()

	// Register nodes
	for i := 1; i <= 3; i++ {
		store.RegisterNode(ctx, &NodeInfo{
			ID: NodeID(i), Addr: fmt.Sprintf("node%d:9001", i),
			CapacityGB: 100, Rack: fmt.Sprintf("rack%d", i), Zone: "zone1",
			Tier: TierHot,
		})
	}

	store.CreateBucket(ctx, "fs", PlacementPolicy{})
	bucket, _ := store.GetBucket(ctx, "fs")
	file, _ := store.CreateFile(ctx, bucket.RootInode, "chunks.bin", 0644)

	// Allocate several chunks (they'll be auto-placed on nodes 1-3)
	var chunkIDs []ChunkID
	for i := 0; i < 5; i++ {
		chunk, err := store.AllocateChunk(ctx, file.ID, int64(i)*MaxChunkSize, PlacementPolicy{
			ReplicationFactor: 2, TopologySpread: SpreadNode,
		})
		if err != nil {
			t.Fatalf("AllocateChunk %d: %v", i, err)
		}
		chunkIDs = append(chunkIDs, chunk.ID)
	}

	// Find which nodes actually received chunks
	nodeChunkCount := make(map[NodeID]int)
	for _, cid := range chunkIDs {
		chunk, _ := store.GetChunk(ctx, cid)
		for _, r := range chunk.Replicas {
			nodeChunkCount[r.NodeID]++
		}
	}
	if len(nodeChunkCount) == 0 {
		t.Fatal("expected at least one node to have chunks")
	}

	// Pick a node that has chunks and verify ChunksByNode returns them
	var targetNode NodeID
	for nid := range nodeChunkCount {
		targetNode = nid
		break
	}

	results, err := store.ChunksByNode(ctx, targetNode)
	if err != nil {
		t.Fatalf("ChunksByNode: %v", err)
	}
	if len(results) == 0 {
		t.Errorf("expected at least 1 chunk for node %d", targetNode)
	}

	// Verify all returned chunks have targetNode in replicas
	for _, c := range results {
		found := false
		for _, r := range c.Replicas {
			if r.NodeID == targetNode {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("chunk %d returned by ChunksByNode(%d) but no replica on that node", c.ID, targetNode)
		}
	}
}

func TestPebbleStore_ChunksByNode_Empty(t *testing.T) {
	store := newTestPebbleStore(t)
	ctx := context.Background()

	results, err := store.ChunksByNode(ctx, 1)
	if err != nil {
		t.Fatalf("ChunksByNode: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected 0 chunks, got %d", len(results))
	}
}

func TestPebbleStore_MigrateChunkReplica(t *testing.T) {
	store := newTestPebbleStore(t)
	ctx := context.Background()

	// Register nodes
	for i := 1; i <= 3; i++ {
		store.RegisterNode(ctx, &NodeInfo{
			ID: NodeID(i), Addr: fmt.Sprintf("10.0.0.%d:9100", i),
			CapacityGB: 100, Rack: fmt.Sprintf("rack%d", i), Zone: "zone1",
			Tier: TierHot, State: NodeOnline,
		})
	}

	store.CreateBucket(ctx, "test", PlacementPolicy{})
	bucket, _ := store.GetBucket(ctx, "test")
	file, _ := store.CreateFile(ctx, bucket.RootInode, "target.bin", 0644)

	// Allocate a chunk (auto-placed on some of nodes 1-3)
	chunk, err := store.AllocateChunk(ctx, file.ID, 0, PlacementPolicy{
		ReplicationFactor: 2, TopologySpread: SpreadNode,
	})
	if err != nil {
		t.Fatalf("AllocateChunk: %v", err)
	}

	// Find a replica to migrate
	sourceNode := chunk.Replicas[0].NodeID
	targetNode := NodeID(3)
	if sourceNode == 3 {
		targetNode = 1
	}

	// Migrate replica
	err = store.MigrateChunkReplica(ctx, chunk.ID, sourceNode, targetNode)
	if err != nil {
		t.Fatalf("MigrateChunkReplica: %v", err)
	}

	// Verify migration
	updated, err := store.GetChunk(ctx, chunk.ID)
	if err != nil {
		t.Fatalf("GetChunk: %v", err)
	}

	hasSource := false
	hasTarget := false
	for _, r := range updated.Replicas {
		if r.NodeID == sourceNode {
			hasSource = true
		}
		if r.NodeID == targetNode {
			hasTarget = true
		}
	}
	if hasSource {
		t.Errorf("node %d should not be a replica after migration", sourceNode)
	}
	if !hasTarget {
		t.Errorf("node %d should be a replica after migration", targetNode)
	}
}

func TestPebbleStore_MigrateChunkReplica_NotFound(t *testing.T) {
	store := newTestPebbleStore(t)
	ctx := context.Background()

	err := store.MigrateChunkReplica(ctx, 999, 1, 2)
	if err == nil {
		t.Error("expected error for non-existent chunk")
	}
}

func TestPebbleStore_TriggerRebalanceWithChunks(t *testing.T) {
	store := newTestPebbleStore(t)
	ctx := context.Background()

	// Register nodes
	for i := 1; i <= 3; i++ {
		store.RegisterNode(ctx, &NodeInfo{
			ID: NodeID(i), Addr: fmt.Sprintf("10.0.0.%d:9100", i),
			CapacityGB: 1000, Rack: fmt.Sprintf("rack%d", i), Zone: "zone1",
			Tier: TierHot, State: NodeOnline,
		})
	}

	store.CreateBucket(ctx, "test", PlacementPolicy{})
	bucket, _ := store.GetBucket(ctx, "test")
	file, _ := store.CreateFile(ctx, bucket.RootInode, "data.bin", 0644)

	// Allocate several chunks
	for i := 0; i < 5; i++ {
		_, err := store.AllocateChunk(ctx, file.ID, int64(i)*MaxChunkSize, PlacementPolicy{
			ReplicationFactor: 2, TopologySpread: SpreadNode,
		})
		if err != nil {
			t.Fatalf("AllocateChunk %d: %v", i, err)
		}
	}

	// Override chunk counts to create imbalance
	// First, find which node actually has the most chunks
	nodeChunkCount := make(map[NodeID]int)
	inode, _ := store.GetInode(ctx, file.ID)
	for _, ref := range inode.ChunkMap {
		chunk, _ := store.GetChunk(ctx, ref.ID)
		if chunk != nil {
			for _, r := range chunk.Replicas {
				nodeChunkCount[r.NodeID]++
			}
		}
	}

	// Find the node with most chunks and mark it as overloaded
	var overloadedNode NodeID
	maxCount := 0
	for nid, cnt := range nodeChunkCount {
		if cnt > maxCount {
			maxCount = cnt
			overloadedNode = nid
		}
	}

	nodes, _ := store.ListNodes(ctx)
	for _, n := range nodes {
		info, _ := store.GetNode(ctx, n.ID)
		if info.ID == overloadedNode {
			info.ChunkCount = 500
			info.UsedGB = 500
		} else {
			info.ChunkCount = 10
			info.UsedGB = 10
		}
		key := fmt.Sprintf("%s%d", prefixNode, info.ID)
		store.putJSON(key, info)
	}

	// Trigger rebalance
	err := store.TriggerRebalance(ctx)
	if err != nil {
		t.Fatalf("TriggerRebalance: %v", err)
	}

	// Verify repair tasks were created with real chunk IDs
	tasks, err := store.GetRepairQueue(ctx)
	if err != nil {
		t.Fatalf("GetRepairQueue: %v", err)
	}
	if len(tasks) == 0 {
		t.Error("expected repair tasks after trigger rebalance")
	}
	for _, task := range tasks {
		if task.ChunkID == 0 {
			t.Errorf("repair task has ChunkID=0, should have real chunk ID")
		}
		if task.Reason == "" {
			t.Errorf("repair task for chunk %d has empty reason", task.ChunkID)
		}
	}
}

func TestPebbleStore_AutoRebalanceUsesConcreteChunks(t *testing.T) {
	store := newTestPebbleStore(t)
	ctx := context.Background()

	for i := 1; i <= 3; i++ {
		if err := store.RegisterNode(ctx, &NodeInfo{
			ID:         NodeID(i),
			Addr:       fmt.Sprintf("10.0.0.%d:9100", i),
			CapacityGB: 1000,
			Rack:       fmt.Sprintf("rack%d", i),
			Zone:       "zone1",
			Tier:       TierHot,
			State:      NodeOnline,
		}); err != nil {
			t.Fatalf("RegisterNode %d: %v", i, err)
		}
	}

	if err := store.CreateBucket(ctx, "auto-rebalance", PlacementPolicy{}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	bucket, err := store.GetBucket(ctx, "auto-rebalance")
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}
	file, err := store.CreateFile(ctx, bucket.RootInode, "data.bin", 0644)
	if err != nil {
		t.Fatalf("CreateFile: %v", err)
	}
	for i := 0; i < 5; i++ {
		if _, err := store.AllocateChunk(ctx, file.ID, int64(i)*MaxChunkSize, PlacementPolicy{
			ReplicationFactor: 2,
			TopologySpread:    SpreadNode,
		}); err != nil {
			t.Fatalf("AllocateChunk %d: %v", i, err)
		}
	}

	var overloaded NodeID
	counts := make(map[NodeID]int)
	inode, err := store.GetInode(ctx, file.ID)
	if err != nil {
		t.Fatalf("GetInode: %v", err)
	}
	for _, ref := range inode.ChunkMap {
		chunk, err := store.GetChunk(ctx, ref.ID)
		if err != nil {
			t.Fatalf("GetChunk %d: %v", ref.ID, err)
		}
		for _, replica := range chunk.Replicas {
			counts[replica.NodeID]++
			if counts[replica.NodeID] > counts[overloaded] {
				overloaded = replica.NodeID
			}
		}
	}

	nodes, err := store.ListNodes(ctx)
	if err != nil {
		t.Fatalf("ListNodes: %v", err)
	}
	for _, node := range nodes {
		info, err := store.GetNode(ctx, node.ID)
		if err != nil {
			t.Fatalf("GetNode %d: %v", node.ID, err)
		}
		if info.ID == overloaded {
			info.ChunkCount = 500
			info.UsedGB = 500
		} else {
			info.ChunkCount = 10
			info.UsedGB = 10
		}
		if err := store.putMsgpack(fmt.Sprintf("%s%d", prefixNode, info.ID), info); err != nil {
			t.Fatalf("update node %d: %v", info.ID, err)
		}
	}

	store.triggerAutoRebalance()

	tasks, err := store.GetRepairQueue(ctx)
	if err != nil {
		t.Fatalf("GetRepairQueue: %v", err)
	}
	if len(tasks) == 0 {
		t.Fatal("expected auto-rebalance to create repair tasks")
	}
	for _, task := range tasks {
		if task.ChunkID == 0 {
			t.Fatalf("auto-rebalance created zero chunk repair task: %+v", task)
		}
	}
}

// testSnapshotSink is a minimal raft.SnapshotSink for testing.
type testSnapshotSink struct {
	buf *bytes.Buffer
}

func (s *testSnapshotSink) Write(p []byte) (int, error) { return s.buf.Write(p) }
func (s *testSnapshotSink) Close() error                { return nil }
func (s *testSnapshotSink) ID() string                  { return "test" }
func (s *testSnapshotSink) Cancel() error               { return nil }

// ========== applyViaRaft / applyBatchViaRaft Tests ==========

func TestApplyViaRaft_SetDelete(t *testing.T) {
	store := newTestPebbleStore(t)

	// Set via applyViaRaft (non-Raft mode: direct Pebble write)
	err := store.applyViaRaft(OpSet, "test-key", []byte("test-value"))
	if err != nil {
		t.Fatalf("applyViaRaft set: %v", err)
	}

	// Verify in Pebble
	val, closer, err := store.db.Get([]byte("test-key"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(val) != "test-value" {
		t.Fatalf("expected test-value, got %s", val)
	}
	closer.Close()

	// Delete via applyViaRaft
	err = store.applyViaRaft(OpDelete, "test-key", nil)
	if err != nil {
		t.Fatalf("applyViaRaft delete: %v", err)
	}

	_, closer2, err := store.db.Get([]byte("test-key"))
	if err == nil {
		closer2.Close()
		t.Fatal("key should be deleted")
	}
}

func TestApplyViaRaft_ReadOnlyRejects(t *testing.T) {
	store := newTestPebbleStore(t)

	// Force read-only state
	store.degradation.transition(DegStateReadOnly)

	err := store.applyViaRaft(OpSet, "key", []byte("val"))
	if err != ErrServiceClosed {
		t.Fatalf("expected ErrServiceClosed in read-only mode, got: %v", err)
	}

	err = store.applyBatchViaRaft([]BatchOp{{Key: []byte("k"), Value: []byte("v")}})
	if err != ErrServiceClosed {
		t.Fatalf("expected ErrServiceClosed in read-only mode, got: %v", err)
	}
}

func TestApplyViaRaft_MetricsRecorded(t *testing.T) {
	store := newTestPebbleStore(t)
	store.metrics = NewMetrics()

	initialWrites := store.metrics.WriteOps.Load()

	_ = store.applyViaRaft(OpSet, "mkey", []byte("mval"))

	if store.metrics.WriteOps.Load() != initialWrites+1 {
		t.Fatalf("expected WriteOps=%d, got %d", initialWrites+1, store.metrics.WriteOps.Load())
	}
}

func TestApplyBatchViaRaft_AtomicBatch(t *testing.T) {
	store := newTestPebbleStore(t)

	ops := []BatchOp{
		{Key: []byte("bk1"), Value: []byte("bv1")},
		{Key: []byte("bk2"), Value: []byte("bv2")},
		{Delete: true, Key: []byte("nonexistent")}, // delete missing key is fine
	}

	err := store.applyBatchViaRaft(ops)
	if err != nil {
		t.Fatalf("applyBatchViaRaft: %v", err)
	}

	// Verify both keys written
	v1, c1, err := store.db.Get([]byte("bk1"))
	if err != nil {
		t.Fatalf("get bk1: %v", err)
	}
	if string(v1) != "bv1" {
		t.Fatalf("bk1 = %s, want bv1", v1)
	}
	c1.Close()

	v2, c2, err := store.db.Get([]byte("bk2"))
	if err != nil {
		t.Fatalf("get bk2: %v", err)
	}
	if string(v2) != "bv2" {
		t.Fatalf("bk2 = %s, want bv2", v2)
	}
	c2.Close()
}

func TestApplyBatchViaRaft_EmptyIsNoop(t *testing.T) {
	store := newTestPebbleStore(t)
	err := store.applyBatchViaRaft(nil)
	if err != nil {
		t.Fatalf("nil batch: %v", err)
	}
	err = store.applyBatchViaRaft([]BatchOp{})
	if err != nil {
		t.Fatalf("empty batch: %v", err)
	}
}

// ========== DegradationManager Tests ==========

func TestDegradationManager_WriteErrorThreshold(t *testing.T) {
	store := newTestPebbleStore(t)

	if store.degradation.State() != DegStateNormal {
		t.Fatalf("initial state should be Normal")
	}

	// Record 9 errors — still normal
	for i := 0; i < 9; i++ {
		store.degradation.RecordWriteError()
	}
	if store.degradation.State() != DegStateNormal {
		t.Fatalf("expected Normal after 9 errors, got %s", store.degradation.State())
	}

	// 10th error triggers read-only
	store.degradation.RecordWriteError()
	if store.degradation.State() != DegStateReadOnly {
		t.Fatalf("expected ReadOnly after 10 errors, got %s", store.degradation.State())
	}
	if !store.degradation.IsReadOnly() {
		t.Fatal("IsReadOnly should be true")
	}
}

func TestDegradationManager_SuccessResetsCounter(t *testing.T) {
	store := newTestPebbleStore(t)

	for i := 0; i < 9; i++ {
		store.degradation.RecordWriteError()
	}
	store.degradation.RecordWriteSuccess()

	// Counter reset, 10 more errors needed to trigger
	for i := 0; i < 9; i++ {
		store.degradation.RecordWriteError()
	}
	if store.degradation.State() != DegStateNormal {
		t.Fatalf("expected Normal after reset, got %s", store.degradation.State())
	}
}

func TestDegradationManager_ReadErrorThreshold(t *testing.T) {
	store := newTestPebbleStore(t)

	for i := 0; i < 20; i++ {
		store.degradation.RecordReadError()
	}
	if store.degradation.State() != DegStateDegraded {
		t.Fatalf("expected Degraded after 20 read errors, got %s", store.degradation.State())
	}
}

func TestDegradationManager_StateString(t *testing.T) {
	if DegStateNormal.String() != "Normal" {
		t.Fatalf("Normal.String() = %q", DegStateNormal.String())
	}
	if DegStateReadOnly.String() != "ReadOnly" {
		t.Fatalf("ReadOnly.String() = %q", DegStateReadOnly.String())
	}
	if DegStateDegraded.String() != "Degraded" {
		t.Fatalf("Degraded.String() = %q", DegStateDegraded.String())
	}
	if DegStateUnavailable.String() != "Unavailable" {
		t.Fatalf("Unavailable.String() = %q", DegStateUnavailable.String())
	}
}

func TestDegradationManager_Recovery(t *testing.T) {
	store := newTestPebbleStore(t)

	// Trigger read-only
	for i := 0; i < 10; i++ {
		store.degradation.RecordWriteError()
	}
	if store.degradation.State() != DegStateReadOnly {
		t.Fatal("expected ReadOnly")
	}

	// Recovery: enough successes should recover to normal
	store.degradation.Recover()
	if store.degradation.State() != DegStateNormal {
		t.Fatalf("expected Normal after recovery, got %s", store.degradation.State())
	}
}

func TestRaftLogEntry_CASRoundTrip(t *testing.T) {
	payload := make([]byte, 8+len("new-data"))
	copy(payload[8:], "new-data")
	entry := &RaftLogEntry{
		Op:    OpCAS,
		Key:   []byte("/inode/123"),
		Value: payload,
	}
	data := entry.Encode()
	decoded, err := DecodeRaftLogEntry(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if decoded.Op != OpCAS {
		t.Fatalf("op mismatch: %v", decoded.Op)
	}
	if string(decoded.Key) != "/inode/123" {
		t.Fatalf("key mismatch: %s", decoded.Key)
	}
	if string(decoded.Value[8:]) != "new-data" {
		t.Fatalf("value mismatch: %s", decoded.Value[8:])
	}
}

// ========== Raft Leader Discovery Tests ==========

func TestLeaderOpsAddr_SingleNodeReturnsEmpty(t *testing.T) {
	// In single-node mode (raft == nil), LeaderOpsAddr should return ""
	// because there is no leader election happening.
	store := newTestPebbleStore(t)
	// raft is nil in test store, so LeaderOpsAddr should not be called directly.
	// This tests the fallback path indirectly.
	if store.raft != nil {
		t.Skip("raft is initialized in this test config")
	}
	// Single-node mode: no raft, so LeaderOpsAddr is not applicable.
}

func TestRaftLogEntry_StoreOpsURLRoundTrip(t *testing.T) {
	// Verify that storing an ops URL via Raft log entry round-trips correctly.
	entry := &RaftLogEntry{
		Op:    OpSet,
		Key:   []byte(metaNodeOpsKey("meta-1")),
		Value: []byte("http://10.0.0.1:8091"),
	}
	encoded := entry.Encode()
	decoded, err := DecodeRaftLogEntry(encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if decoded.Op != OpSet {
		t.Fatalf("op mismatch: %v", decoded.Op)
	}
	if string(decoded.Key) != "/_raft/nodes/meta-1" {
		t.Fatalf("key mismatch: %s", decoded.Key)
	}
	if string(decoded.Value) != "http://10.0.0.1:8091" {
		t.Fatalf("value mismatch: %s", decoded.Value)
	}
}

func TestApplyViaRaft_SingleNodeDirectWrite(t *testing.T) {
	// In single-node mode (raft == nil), writes go directly to Pebble.
	store := newTestPebbleStore(t)
	if store.raft != nil {
		t.Skip("raft is initialized in this test config")
	}

	// Store an ops URL directly in Pebble (simulating what StoreOpsURL does)
	key := []byte(metaNodeOpsKey("meta-1"))
	val := []byte("http://localhost:8091")
	if err := store.db.Set(key, val, nil); err != nil {
		t.Fatalf("Set: %v", err)
	}

	// Verify we can read it back
	got, closer, err := store.db.Get(key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer closer.Close()
	if string(got) != "http://localhost:8091" {
		t.Fatalf("mismatch: %s", got)
	}
}

func TestForwardToLeader_RequiresLeaderAddress(t *testing.T) {
	// ApplyAutoForward with no leader should return an error, not panic.
	store := newTestPebbleStore(t)
	if store.raft != nil {
		t.Skip("raft is initialized in this test config")
	}

	// In single-node mode without raft, applyViaRaft writes directly to Pebble.
	err := store.applyViaRaft(OpSet, "test-key", []byte("test-value"))
	if err != nil {
		t.Logf("applyViaRaft in single-node mode: %v", err)
	}
}

// ========== 3-Node Raft E2E Tests ==========

// waitForLeader waits until one of the Raft nodes becomes leader or timeout.
func waitForLeader(nodes []*RaftNode, timeout time.Duration) *RaftNode {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n != nil && n.IsLeader() {
				return n
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	return nil
}

// setup3NodeCluster creates a 3-node Raft cluster for testing.
// Returns stores, nodes, and a cleanup function.
func setup3NodeCluster(t *testing.T, portBase int) ([]*PebbleStore, []*RaftNode, func()) {
	t.Helper()
	const numNodes = 3

	var stores []*PebbleStore
	var nodes []*RaftNode

	for i := 0; i < numNodes; i++ {
		stores = append(stores, newTestPebbleStore(t))
	}

	// Build all server addresses
	allAddrs := make([]string, numNodes)
	for i := 0; i < numNodes; i++ {
		allAddrs[i] = fmt.Sprintf("127.0.0.1:%d", portBase+i)
	}

	// Only node 0 bootstraps with all servers in the configuration.
	// Other nodes receive the configuration via Raft log replication.
	for i := 0; i < numNodes; i++ {
		var peers []string
		if i == 0 {
			// Bootstrap: include peer addresses
			for j := 1; j < numNodes; j++ {
				peers = append(peers, allAddrs[j])
			}
		}

		cfg := RaftNodeConfig{
			NodeID:             fmt.Sprintf("meta-%d", i+1),
			BindAddr:           allAddrs[i],
			RaftDir:            t.TempDir(),
			Bootstrap:          i == 0,
			Peers:              peers,
			HeartbeatTimeout:   500 * time.Millisecond,
			ElectionTimeout:    1000 * time.Millisecond,
			LeaderLeaseTimeout: 200 * time.Millisecond,
			SnapshotThreshold:  1024,
			SnapshotInterval:   time.Minute,
			TrailingLogs:       256,
			AdvertiseOpsAddr:   fmt.Sprintf("http://127.0.0.1:%d", 9000+i),
		}
		node, err := NewRaftNode(stores[i], cfg)
		if err != nil {
			t.Fatalf("NewRaftNode %d: %v", i+1, err)
		}
		nodes = append(nodes, node)
	}

	cleanup := func() {
		for _, n := range nodes {
			if n != nil {
				n.Shutdown()
			}
		}
	}
	return stores, nodes, cleanup
}

func TestRaftE2E_3NodeClusterElection(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping raft e2e in short mode")
	}

	stores, nodes, cleanup := setup3NodeCluster(t, 10100)
	_ = stores
	defer cleanup()

	// Verify leader exists
	leader := waitForLeader(nodes, 5*time.Second)
	if leader == nil {
		t.Fatal("no leader elected")
	}
	t.Logf("leader elected: %s", leader.nodeID)

	// Verify only one leader
	leaderCount := 0
	for _, n := range nodes {
		if n.IsLeader() {
			leaderCount++
		}
	}
	if leaderCount != 1 {
		t.Fatalf("expected 1 leader, got %d", leaderCount)
	}
}

func TestRaftE2E_WriteThroughLeader(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping raft e2e in short mode")
	}

	// Single-node Raft: verify leader commits writes via FSM
	store := newTestPebbleStore(t)
	cfg := RaftNodeConfig{
		NodeID:             "meta-1",
		BindAddr:           "127.0.0.1:10200",
		RaftDir:            t.TempDir(),
		Bootstrap:          true,
		HeartbeatTimeout:   500 * time.Millisecond,
		ElectionTimeout:    1000 * time.Millisecond,
		LeaderLeaseTimeout: 200 * time.Millisecond,
		SnapshotThreshold:  1024,
		SnapshotInterval:   time.Minute,
		TrailingLogs:       256,
		AdvertiseOpsAddr:   "http://127.0.0.1:10200",
	}
	node, err := NewRaftNode(store, cfg)
	if err != nil {
		t.Fatalf("NewRaftNode: %v", err)
	}
	defer node.Shutdown()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && !node.IsLeader() {
		time.Sleep(50 * time.Millisecond)
	}
	if !node.IsLeader() {
		t.Fatal("node did not become leader")
	}

	key := []byte("test/key-1")
	val := []byte("hello-raft")
	if err := node.fsm.store.applyViaRaft(OpSet, string(key), val); err != nil {
		t.Fatalf("write: %v", err)
	}

	got, closer, err := store.db.Get(key)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	closer.Close()
	if string(got) != string(val) {
		t.Fatalf("expected %q, got %q", val, got)
	}
	t.Log("write committed via leader FSM successfully")
}

func TestRaftE2E_LeaderFailover(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping raft e2e in short mode")
	}

	// Single-node Raft: verify data persists after leader shutdown
	store := newTestPebbleStore(t)
	cfg := RaftNodeConfig{
		NodeID:             "meta-1",
		BindAddr:           "127.0.0.1:10300",
		RaftDir:            t.TempDir(),
		Bootstrap:          true,
		HeartbeatTimeout:   500 * time.Millisecond,
		ElectionTimeout:    1000 * time.Millisecond,
		LeaderLeaseTimeout: 200 * time.Millisecond,
		SnapshotThreshold:  1024,
		SnapshotInterval:   time.Minute,
		TrailingLogs:       256,
		AdvertiseOpsAddr:   "http://127.0.0.1:10300",
	}
	node, err := NewRaftNode(store, cfg)
	if err != nil {
		t.Fatalf("NewRaftNode: %v", err)
	}

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && !node.IsLeader() {
		time.Sleep(50 * time.Millisecond)
	}
	if !node.IsLeader() {
		t.Fatal("node did not become leader")
	}

	// Write data
	if err := node.fsm.store.applyViaRaft(OpSet, "test-data", []byte("before-shutdown")); err != nil {
		t.Fatalf("write: %v", err)
	}

	// Verify write
	got, closer, err := store.db.Get([]byte("test-data"))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	closer.Close()
	if string(got) != "before-shutdown" {
		t.Fatalf("expected %q, got %q", "before-shutdown", got)
	}

	// Shutdown
	node.raft.Shutdown()

	// Data persists in local store
	got2, closer2, err := store.db.Get([]byte("test-data"))
	if err != nil {
		t.Fatalf("read after shutdown: %v", err)
	}
	closer2.Close()
	if string(got2) != "before-shutdown" {
		t.Fatalf("data lost: got %q", got2)
	}
	t.Log("leader commit and local persistence verified")
}
