package metadata

import (
	"bytes"
	"context"
	"fmt"
	"io"
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
	if chunk.State != ChunkSealing {
		t.Fatalf("expected Sealing, got %d", chunk.State)
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
	// 100 keys + 1 root inode
	if count != 101 {
		t.Fatalf("expected 101 keys after restore, got %d", count)
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
