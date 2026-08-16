package metadata

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
)

var _ BucketQuotaService = (*ShardedStore)(nil)

func TestPebbleStoreBucketQuotaRoundTrip(t *testing.T) {
	ctx := context.Background()
	store := newQuotaTestStore(t)
	if err := store.CreateBucket(ctx, "photos", PlacementPolicy{ReplicationFactor: 1}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	if err := store.SetBucketQuota(ctx, "photos", &BucketQuota{MaxSizeBytes: 1024, MaxObjects: 2}); err != nil {
		t.Fatalf("SetBucketQuota: %v", err)
	}
	got, err := store.GetBucketQuota(ctx, "photos")
	if err != nil {
		t.Fatalf("GetBucketQuota: %v", err)
	}
	if got == nil || got.MaxSizeBytes != 1024 || got.MaxObjects != 2 {
		t.Fatalf("quota = %+v", got)
	}
	if err := store.DeleteBucketQuota(ctx, "photos"); err != nil {
		t.Fatalf("DeleteBucketQuota: %v", err)
	}
	got, err = store.GetBucketQuota(ctx, "photos")
	if err != nil {
		t.Fatalf("GetBucketQuota after delete: %v", err)
	}
	if got != nil {
		t.Fatalf("quota after delete = %+v, want nil", got)
	}
}

func TestPebbleStoreBucketQuotaRejectsInvalidAndMissingBucket(t *testing.T) {
	ctx := context.Background()
	store := newQuotaTestStore(t)
	if err := store.SetBucketQuota(ctx, "missing", &BucketQuota{MaxSizeBytes: 1}); err != ErrBucketNotFound {
		t.Fatalf("SetBucketQuota missing = %v, want %v", err, ErrBucketNotFound)
	}
	if err := store.CreateBucket(ctx, "photos", PlacementPolicy{ReplicationFactor: 1}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	if err := store.SetBucketQuota(ctx, "photos", nil); err == nil || !strings.Contains(err.Error(), "required") {
		t.Fatalf("SetBucketQuota nil = %v, want required validation error", err)
	}
	if err := store.SetBucketQuota(ctx, "photos", &BucketQuota{MaxSizeBytes: -1}); err == nil || !strings.Contains(err.Error(), "negative") {
		t.Fatalf("SetBucketQuota negative = %v, want negative validation error", err)
	}
}

func TestPebbleStoreCheckBucketQuotaUsesDeltas(t *testing.T) {
	ctx := context.Background()
	store := newQuotaTestStore(t)
	if err := store.CreateBucket(ctx, "photos", PlacementPolicy{ReplicationFactor: 1}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	if err := store.SetBucketQuota(ctx, "photos", &BucketQuota{MaxSizeBytes: 100, MaxObjects: 1}); err != nil {
		t.Fatalf("SetBucketQuota: %v", err)
	}
	bucket, err := store.GetBucket(ctx, "photos")
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}
	file, err := store.CreateFile(ctx, bucket.RootInode, "existing", 0644)
	if err != nil {
		t.Fatalf("CreateFile: %v", err)
	}
	file.Size = 80
	if err := store.UpdateInode(ctx, file); err != nil {
		t.Fatalf("UpdateInode: %v", err)
	}
	usage, err := store.GetBucketUsage(ctx, "photos")
	if err != nil {
		t.Fatalf("GetBucketUsage: %v", err)
	}
	if usage.UsedBytes != 80 || usage.Objects != 1 {
		t.Fatalf("usage = %+v, want 80 bytes and 1 object", usage)
	}
	if err := store.CheckBucketQuota(ctx, "photos", 10, 0); err != nil {
		t.Fatalf("CheckBucketQuota small delta: %v", err)
	}
	if err := store.CheckBucketQuota(ctx, "photos", 25, 0); err == nil ||
		!errors.Is(err, ErrQuotaExceeded) || !strings.Contains(err.Error(), "size") {
		t.Fatalf("CheckBucketQuota bytes = %v, want size quota error", err)
	}
	if err := store.CheckBucketQuota(ctx, "photos", 0, 1); err == nil ||
		!errors.Is(err, ErrQuotaExceeded) || !strings.Contains(err.Error(), "object") {
		t.Fatalf("CheckBucketQuota objects = %v, want object quota error", err)
	}
}

func TestShardedStoreBucketQuotaForwards(t *testing.T) {
	ctx := context.Background()
	ring := NewHashRing(1)
	ring.AddShard(ShardInfo{ID: 1})
	ring.AddShard(ShardInfo{ID: 2})
	sharded := NewShardedStore(ring)
	first := newQuotaTestStore(t)
	second := newQuotaTestStore(t)
	sharded.AddShard(1, first)
	sharded.AddShard(2, second)

	quotaManager := NewQuotaManager()
	sharded.SetQuotaManager(quotaManager)
	if first.quota != quotaManager || second.quota != quotaManager {
		t.Fatal("SetQuotaManager did not share the quota manager with every shard")
	}
	if err := sharded.CreateBucket(ctx, "photos", PlacementPolicy{ReplicationFactor: 1}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	if err := sharded.SetBucketQuota(ctx, "photos", &BucketQuota{MaxSizeBytes: 1024, MaxObjects: 2}); err != nil {
		t.Fatalf("SetBucketQuota: %v", err)
	}
	for shardID, store := range sharded.AllShards() {
		quota, err := store.GetBucketQuota(ctx, "photos")
		if err != nil {
			t.Fatalf("GetBucketQuota on shard %d: %v", shardID, err)
		}
		if quota == nil || quota.MaxSizeBytes != 1024 || quota.MaxObjects != 2 {
			t.Fatalf("quota on shard %d = %+v", shardID, quota)
		}
	}
	quota, err := sharded.GetBucketQuota(ctx, "photos")
	if err != nil {
		t.Fatalf("GetBucketQuota: %v", err)
	}
	if quota == nil || quota.MaxSizeBytes != 1024 || quota.MaxObjects != 2 {
		t.Fatalf("sharded quota = %+v", quota)
	}
	if err := sharded.DeleteBucketQuota(ctx, "photos"); err != nil {
		t.Fatalf("DeleteBucketQuota: %v", err)
	}
	for shardID, store := range sharded.AllShards() {
		quota, err := store.GetBucketQuota(ctx, "photos")
		if err != nil {
			t.Fatalf("GetBucketQuota after delete on shard %d: %v", shardID, err)
		}
		if quota != nil {
			t.Fatalf("quota after delete on shard %d = %+v, want nil", shardID, quota)
		}
	}
}

func TestShardedStoreBucketQuotaPersistsAndDeletesOnEveryShard(t *testing.T) {
	ctx := context.Background()
	dirs := []string{t.TempDir(), t.TempDir()}
	openStore := func(dir string, nodeID uint64) *PebbleStore {
		t.Helper()
		store, err := NewPebbleStore(PebbleStoreConfig{
			Dir:            dir,
			NodeID:         nodeID,
			UseBucketStats: true,
		})
		if err != nil {
			t.Fatalf("NewPebbleStore(%q): %v", dir, err)
		}
		t.Cleanup(func() { _ = store.Close() })
		return store
	}
	newShardedStore := func(stores []*PebbleStore) *ShardedStore {
		t.Helper()
		ring := NewHashRing(1)
		sharded := NewShardedStore(ring)
		for i, store := range stores {
			shardID := ShardID(i + 1)
			ring.AddShard(ShardInfo{ID: shardID})
			sharded.AddShard(shardID, store)
		}
		sharded.SetQuotaManager(NewQuotaManager())
		return sharded
	}
	reopenAll := func() []*PebbleStore {
		t.Helper()
		return []*PebbleStore{
			openStore(dirs[0], 1),
			openStore(dirs[1], 2),
		}
	}

	initial := []*PebbleStore{
		openStore(dirs[0], 1),
		openStore(dirs[1], 2),
	}
	sharded := newShardedStore(initial)
	if err := sharded.CreateBucket(ctx, "photos", PlacementPolicy{ReplicationFactor: 1}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	if err := sharded.SetBucketQuota(ctx, "photos", &BucketQuota{MaxSizeBytes: 1024, MaxObjects: 2}); err != nil {
		t.Fatalf("SetBucketQuota: %v", err)
	}
	if err := sharded.Close(); err != nil {
		t.Fatalf("Close after set: %v", err)
	}

	afterSet := reopenAll()
	for i, store := range afterSet {
		store.SetQuotaManager(NewQuotaManager())
		quota, err := store.GetBucketQuota(ctx, "photos")
		if err != nil {
			t.Fatalf("GetBucketQuota after set on shard %d: %v", i+1, err)
		}
		if quota == nil || quota.MaxSizeBytes != 1024 || quota.MaxObjects != 2 {
			t.Fatalf("quota after set on shard %d = %+v", i+1, quota)
		}
	}

	sharded = newShardedStore(afterSet)
	if err := sharded.DeleteBucketQuota(ctx, "photos"); err != nil {
		t.Fatalf("DeleteBucketQuota: %v", err)
	}
	if err := sharded.Close(); err != nil {
		t.Fatalf("Close after delete: %v", err)
	}

	afterDelete := reopenAll()
	for i, store := range afterDelete {
		store.SetQuotaManager(NewQuotaManager())
		quota, err := store.GetBucketQuota(ctx, "photos")
		if err != nil {
			t.Fatalf("GetBucketQuota after delete on shard %d: %v", i+1, err)
		}
		if quota != nil {
			t.Fatalf("quota after delete on shard %d = %+v, want nil", i+1, quota)
		}
	}
}

func TestPebbleStoreGetBucketQuotaReadsPersistedValueWithoutQuotaManager(t *testing.T) {
	ctx := context.Background()
	cfg := PebbleStoreConfig{
		Dir:            t.TempDir(),
		NodeID:         1,
		UseBucketStats: true,
	}
	store, err := NewPebbleStore(cfg)
	if err != nil {
		t.Fatalf("NewPebbleStore: %v", err)
	}
	if err := store.CreateBucket(ctx, "photos", PlacementPolicy{ReplicationFactor: 1}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	if err := store.SetBucketQuota(ctx, "photos", &BucketQuota{MaxSizeBytes: 1024, MaxObjects: 2}); err != nil {
		t.Fatalf("SetBucketQuota: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := NewPebbleStore(cfg)
	if err != nil {
		t.Fatalf("NewPebbleStore after reopen: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	quota, err := reopened.GetBucketQuota(ctx, "photos")
	if err != nil {
		t.Fatalf("GetBucketQuota without quota manager: %v", err)
	}
	if quota == nil || quota.MaxSizeBytes != 1024 || quota.MaxObjects != 2 {
		t.Fatalf("persisted quota = %+v", quota)
	}
	if err := reopened.CheckBucketQuota(ctx, "photos", 1025, 0); !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("CheckBucketQuota after reopen = %v, want ErrQuotaExceeded", err)
	}
}

func TestPebbleStoreDeleteBucketRemovesPersistedQuota(t *testing.T) {
	ctx := context.Background()
	store := newQuotaTestStore(t)
	if err := store.CreateBucket(ctx, "photos", PlacementPolicy{ReplicationFactor: 1}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	if err := store.SetBucketQuota(ctx, "photos", &BucketQuota{MaxSizeBytes: 1024}); err != nil {
		t.Fatalf("SetBucketQuota: %v", err)
	}
	if err := store.DeleteBucket(ctx, "photos"); err != nil {
		t.Fatalf("DeleteBucket: %v", err)
	}
	if err := store.CreateBucket(ctx, "photos", PlacementPolicy{ReplicationFactor: 1}); err != nil {
		t.Fatalf("CreateBucket after delete: %v", err)
	}
	quota, err := store.GetBucketQuota(ctx, "photos")
	if err != nil {
		t.Fatalf("GetBucketQuota after recreate: %v", err)
	}
	if quota != nil {
		t.Fatalf("quota after recreate = %+v, want nil", quota)
	}
}

func TestQuotaManagerCheckWriteDeltaRejectsOverflowAndPermitsNegativeDelta(t *testing.T) {
	manager := NewQuotaManager()
	if err := manager.SetQuota("photos", &BucketQuota{MaxSizeBytes: math.MaxInt64, MaxObjects: math.MaxInt64}); err != nil {
		t.Fatalf("SetQuota: %v", err)
	}
	if err := manager.UpdateUsage("photos", &BucketUsage{Name: "photos", UsedBytes: math.MaxInt64, Objects: math.MaxInt64}); err != nil {
		t.Fatalf("UpdateUsage: %v", err)
	}
	if err := manager.CheckWriteDelta("photos", 1, 0); err == nil ||
		!errors.Is(err, ErrQuotaExceeded) || !strings.Contains(err.Error(), "size") {
		t.Fatalf("CheckWriteDelta overflowing bytes = %v, want size quota error", err)
	}
	if err := manager.CheckWriteDelta("photos", 0, 1); err == nil ||
		!errors.Is(err, ErrQuotaExceeded) || !strings.Contains(err.Error(), "object") {
		t.Fatalf("CheckWriteDelta overflowing objects = %v, want object quota error", err)
	}
	if err := manager.CheckWriteDelta("photos", -1, -1); err != nil {
		t.Fatalf("CheckWriteDelta negative deltas = %v", err)
	}
}

type failingQuotaStore struct {
	saveQuotaErr   error
	saveUsageErr   error
	deleteQuotaErr error
}

func (s *failingQuotaStore) SaveQuota(string, *BucketQuota) error {
	return s.saveQuotaErr
}

func (s *failingQuotaStore) SaveUsage(string, *BucketUsage) error {
	return s.saveUsageErr
}

func (s *failingQuotaStore) DeleteQuota(string) error {
	return s.deleteQuotaErr
}

func TestQuotaManagerPersistenceFailureLeavesMemoryUnchanged(t *testing.T) {
	manager := NewQuotaManager()
	manager.LoadQuota("photos", &BucketQuota{MaxSizeBytes: 100})
	manager.LoadUsage("photos", &BucketUsage{Name: "photos", UsedBytes: 40, Objects: 1})

	store := &failingQuotaStore{saveQuotaErr: fmt.Errorf("disk unavailable")}
	manager.SetStore(store)
	if err := manager.SetQuota("photos", &BucketQuota{MaxSizeBytes: 200}); err == nil {
		t.Fatal("SetQuota succeeded despite persistence failure")
	}
	if got := manager.GetQuota("photos"); got == nil || got.MaxSizeBytes != 100 {
		t.Fatalf("quota after failed SetQuota = %+v, want original quota", got)
	}

	store.saveQuotaErr = nil
	store.deleteQuotaErr = fmt.Errorf("disk unavailable")
	if err := manager.DeleteQuota("photos"); err == nil {
		t.Fatal("DeleteQuota succeeded despite persistence failure")
	}
	if got := manager.GetQuota("photos"); got == nil || got.MaxSizeBytes != 100 {
		t.Fatalf("quota after failed DeleteQuota = %+v, want original quota", got)
	}

	store.deleteQuotaErr = nil
	store.saveUsageErr = fmt.Errorf("disk unavailable")
	if err := manager.UpdateUsage("photos", &BucketUsage{Name: "photos", UsedBytes: 90, Objects: 1}); err == nil {
		t.Fatal("UpdateUsage succeeded despite persistence failure")
	}
	if err := manager.CheckWriteDelta("photos", 20, 0); err != nil {
		t.Fatalf("failed UpdateUsage changed in-memory usage: %v", err)
	}
}

func TestPebbleStoreBucketQuotaAndUsageSurviveCloseAndReopen(t *testing.T) {
	ctx := context.Background()
	cfg := PebbleStoreConfig{
		Dir:            t.TempDir(),
		NodeID:         1,
		UseBucketStats: true,
	}
	store, err := NewPebbleStore(cfg)
	if err != nil {
		t.Fatalf("NewPebbleStore: %v", err)
	}
	if err := store.CreateBucket(ctx, "photos", PlacementPolicy{ReplicationFactor: 1}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	bucket, err := store.GetBucket(ctx, "photos")
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}
	file, err := store.CreateFile(ctx, bucket.RootInode, "existing", 0644)
	if err != nil {
		t.Fatalf("CreateFile: %v", err)
	}
	file.Size = 80
	if err := store.UpdateInode(ctx, file); err != nil {
		t.Fatalf("UpdateInode: %v", err)
	}
	if err := store.SetBucketQuota(ctx, "photos", &BucketQuota{MaxSizeBytes: 100, MaxObjects: 2}); err != nil {
		t.Fatalf("SetBucketQuota: %v", err)
	}
	if err := store.CheckBucketQuota(ctx, "photos", 0, 0); err != nil {
		t.Fatalf("CheckBucketQuota: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := NewPebbleStore(cfg)
	if err != nil {
		t.Fatalf("NewPebbleStore after reopen: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	quota, err := reopened.GetBucketQuota(ctx, "photos")
	if err != nil {
		t.Fatalf("GetBucketQuota after reopen: %v", err)
	}
	if quota == nil || quota.MaxSizeBytes != 100 || quota.MaxObjects != 2 {
		t.Fatalf("reloaded quota = %+v", quota)
	}
	usage, err := reopened.GetBucketUsage(ctx, "photos")
	if err != nil {
		t.Fatalf("GetBucketUsage after reopen: %v", err)
	}
	if usage == nil || usage.UsedBytes != 80 || usage.Objects != 1 {
		t.Fatalf("reloaded usage = %+v", usage)
	}
}

func TestPebbleStoreBackfillsBucketStatsWhenFastCountersAreEnabled(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := NewPebbleStore(PebbleStoreConfig{Dir: dir, NodeID: 1})
	if err != nil {
		t.Fatalf("NewPebbleStore: %v", err)
	}
	if err := store.CreateBucket(ctx, "photos", PlacementPolicy{ReplicationFactor: 1}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	bucket, err := store.GetBucket(ctx, "photos")
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}
	file, err := store.CreateFile(ctx, bucket.RootInode, "existing", 0644)
	if err != nil {
		t.Fatalf("CreateFile: %v", err)
	}
	file.Size = 80
	if err := store.UpdateInode(ctx, file); err != nil {
		t.Fatalf("UpdateInode: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := NewPebbleStore(PebbleStoreConfig{
		Dir:            dir,
		NodeID:         1,
		UseBucketStats: true,
	})
	if err != nil {
		t.Fatalf("NewPebbleStore with bucket stats: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	usage, err := reopened.GetBucketUsage(ctx, "photos")
	if err != nil {
		t.Fatalf("GetBucketUsage: %v", err)
	}
	if usage.UsedBytes != 80 || usage.Objects != 1 {
		t.Fatalf("usage after backfill = %+v, want 80 bytes and 1 object", usage)
	}
}

func TestPebbleStoreAllocateChunkDoesNotUseMaxChunkSizeForQuota(t *testing.T) {
	ctx, store, file, policy := newQuotaAllocationFile(t)
	if _, err := store.AllocateChunk(ctx, file.ID, 0, policy); err != nil {
		t.Fatalf("AllocateChunk with a later actual object delta: %v", err)
	}
}

func TestPebbleStoreAllocateChunksBatchDoesNotUseMaxChunkSizeForQuota(t *testing.T) {
	ctx, store, file, policy := newQuotaAllocationFile(t)
	chunks, err := store.AllocateChunksBatch(ctx, file.ID, []int64{0, MaxChunkSize}, policy)
	if err != nil {
		t.Fatalf("AllocateChunksBatch with later actual object deltas: %v", err)
	}
	if len(chunks) != 2 {
		t.Fatalf("allocated chunks = %d, want 2", len(chunks))
	}
}

func newQuotaAllocationFile(t *testing.T) (context.Context, *PebbleStore, *InodeMeta, PlacementPolicy) {
	t.Helper()
	ctx := context.Background()
	store := newQuotaTestStore(t)
	policy := PlacementPolicy{ReplicationFactor: 1}
	if err := store.CreateBucket(ctx, "photos", policy); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	bucket, err := store.GetBucket(ctx, "photos")
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}
	// The quota allocation path scans /bucket rows by root inode (format-agnostic
	// via unmarshalValue), so exercise the quota estimate against a full on-disk
	// bucket row (msgpack since the stage-4 write-surface convergence).
	if err := store.putMsgpack(prefixBucket+"photos", bucket); err != nil {
		t.Fatalf("write bucket record: %v", err)
	}
	file, err := store.CreateFile(ctx, bucket.RootInode, "new-object", 0644)
	if err != nil {
		t.Fatalf("CreateFile: %v", err)
	}
	if err := store.RegisterNode(ctx, &NodeInfo{
		ID:         1,
		Addr:       "127.0.0.1:9100",
		Rack:       "rack-1",
		Zone:       "zone-1",
		CapacityGB: 100,
	}); err != nil {
		t.Fatalf("RegisterNode: %v", err)
	}
	if err := store.SetBucketQuota(ctx, "photos", &BucketQuota{MaxSizeBytes: 1}); err != nil {
		t.Fatalf("SetBucketQuota: %v", err)
	}
	if err := store.CheckBucketQuota(ctx, "photos", 0, 0); err != nil {
		t.Fatalf("CheckBucketQuota: %v", err)
	}
	return ctx, store, file, policy
}

func newQuotaTestStore(t *testing.T) *PebbleStore {
	t.Helper()
	store, err := NewPebbleStore(PebbleStoreConfig{
		Dir:            t.TempDir(),
		UseInMemory:    true,
		NodeID:         1,
		UseBucketStats: true,
	})
	if err != nil {
		t.Fatalf("NewPebbleStore: %v", err)
	}
	store.SetQuotaManager(NewQuotaManager())
	t.Cleanup(func() { _ = store.Close() })
	return store
}
