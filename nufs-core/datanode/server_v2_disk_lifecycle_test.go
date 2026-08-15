package datanode

// Disk-lifecycle tests for the V2.1 V2Store (DiskLifecycleOps parity with the
// legacy ChunkStore): adopt (AddDisk), retire (RemoveDisk), and the
// migrate+retire decommission composite.

import (
	"testing"

	"github.com/dshmyz/nufs/nufs-core/datanode/storage"
	"github.com/dshmyz/nufs/nufs-core/datanode/storage/segment"
	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// newDiskFactory builds a V2.1-style disk factory that constructs the paired
// data (StreamID 1), EC-shard (StreamID 2) and small-file (StreamID 0) segment
// stores for a dir, the same shape runDataNodeV21 wires via SetDiskFactory.
func newDiskFactory(t *testing.T) func(dir string) (data, shard, small storage.Store, err error) {
	return func(dir string) (storage.Store, storage.Store, storage.Store, error) {
		data, err := segment.New(segment.Config{Dir: dir, UseMemIndex: true, StreamID: 1})
		if err != nil {
			return nil, nil, nil, err
		}
		shard, err := segment.New(segment.Config{Dir: dir, UseMemIndex: true, StreamID: 2})
		if err != nil {
			_ = data.Close()
			return nil, nil, nil, err
		}
		small, err := segment.NewSmallStore(segment.Config{Dir: dir, UseMemIndex: true})
		if err != nil {
			_ = data.Close()
			_ = shard.Close()
			return nil, nil, nil, err
		}
		return data, shard, small, nil
	}
}

func writeChunks(t *testing.T, v *V2Store, base metadata.ChunkID, n int) [][]byte {
	t.Helper()
	payloads := make([][]byte, n)
	for i := 0; i < n; i++ {
		p := []byte("payload-" + string(rune('a'+i)) + "-xxxxxxxxxx")
		payloads[i] = p
		if err := v.Write(base+metadata.ChunkID(i), p); err != nil {
			t.Fatalf("write chunk %d: %v", i, err)
		}
	}
	return payloads
}

func TestV2StoreMigrateDiskMovesAllExtents(t *testing.T) {
	v, _ := newTestMultiStore(t, 2)
	base := metadata.ChunkID(1)
	writeChunks(t, v, base, 8)

	// Record how many chunks initially live on disk 0 — those are what
	// MigrateDisk(0) must relocate off it.
	before := 0
	for i := 0; i < 8; i++ {
		if v.loc(base+metadata.ChunkID(i)).disk == 0 {
			before++
		}
	}
	if before == 0 {
		t.Fatal("no chunks initially placed on disk 0; placement unexpectedly skipped it")
	}

	migrated, err := v.MigrateDisk(0)
	if err != nil {
		t.Fatalf("MigrateDisk: %v", err)
	}
	if migrated != before {
		t.Fatalf("MigrateDisk migrated %d, want %d (chunks on disk 0)", migrated, before)
	}

	// No chunk may still resolve to disk 0, and disk 0 holds no live extents.
	for i := 0; i < 8; i++ {
		if loc := v.loc(base + metadata.ChunkID(i)); loc.disk == 0 {
			t.Fatalf("chunk %d still lives on disk %d after migrate", i, loc.disk)
		}
	}
	if got := v.disks[0].extCount.Load(); got != 0 {
		t.Fatalf("disk 0 extent count after migrate = %d, want 0", got)
	}

	// Reads still return the original payloads from the new home.
	for i := 0; i < 8; i++ {
		data, _, err := v.Read(base+metadata.ChunkID(i), 0, 1<<20)
		if err != nil {
			t.Fatalf("read chunk %d after migrate: %v", i, err)
		}
		if string(data) != "payload-"+string(rune('a'+i))+"-xxxxxxxxxx" {
			t.Fatalf("chunk %d payload corrupted after migrate: %q", i, data)
		}
	}
}

// TestV2StoreMigrateDiskPhysicallyDrainsSource proves MigrateDisk truly empties
// the source disk, not just its accounting. After the move the source disk's
// lister must enumerate NO live extents (so the freed space is real and a later
// migrate cannot get stuck re-attempting stale source extents whose locOf now
// points at the target). Reads then succeed from the target.
func TestV2StoreMigrateDiskPhysicallyDrainsSource(t *testing.T) {
	v, _ := newTestMultiStore(t, 2)
	base := metadata.ChunkID(1)
	writeChunks(t, v, base, 8)

	if _, err := v.MigrateDisk(0); err != nil {
		t.Fatalf("MigrateDisk: %v", err)
	}
	// The source disk's lister must report zero live extents after migration.
	if lister := v.disks[0].lister; lister != nil {
		exts, err := lister.ListExtents()
		if err != nil {
			t.Fatalf("source lister: %v", err)
		}
		if n := len(exts); n != 0 {
			t.Fatalf("source disk 0 still holds %d live extents after migrate (physically not drained): %v", n, exts)
		}
	}
	// A second migrate must be a no-op (0 moved), not re-stick or error.
	n, err := v.MigrateDisk(0)
	if err != nil {
		t.Fatalf("second MigrateDisk: %v", err)
	}
	if n != 0 {
		t.Fatalf("second MigrateDisk moved %d, want 0 (source already drained)", n)
	}
	// Reads still resolve to the target with the original payloads.
	for i := 0; i < 8; i++ {
		data, _, err := v.Read(base+metadata.ChunkID(i), 0, 1<<20)
		if err != nil {
			t.Fatalf("read chunk %d after migrate: %v", i, err)
		}
		if string(data) != "payload-"+string(rune('a'+i))+"-xxxxxxxxxx" {
			t.Fatalf("chunk %d payload %q, want original", i, data)
		}
	}
}

func TestV2StoreMigrateDiskRejectsOnlyDisk(t *testing.T) {
	v, _ := newTestMultiStore(t, 1)
	if _, err := v.MigrateDisk(0); err == nil {
		t.Fatal("MigrateDisk on the only disk should error")
	}
	if _, err := v.MigrateDisk(5); err == nil {
		t.Fatal("MigrateDisk on out-of-range index should error")
	}
}

func TestV2StoreRemoveDiskMarksFailedAndExcludesPlacement(t *testing.T) {
	v, _ := newTestMultiStore(t, 2)
	base := metadata.ChunkID(100)

	if err := v.RemoveDisk(0); err != nil {
		t.Fatalf("RemoveDisk: %v", err)
	}
	if !v.diskFailed(0) {
		t.Fatal("disk 0 should be FAILED after RemoveDisk")
	}
	// The index slot is preserved (reads to it still resolve).
	if got := len(v.disks); got != 2 {
		t.Fatalf("RemoveDisk changed disk count to %d, want 2 (slot preserved)", got)
	}

	// New writes must be placed on disk 1, never the retired disk 0.
	writeChunks(t, v, base, 4)
	for i := 0; i < 4; i++ {
		if loc := v.loc(base + metadata.ChunkID(i)); loc.disk != 1 {
			t.Fatalf("chunk %d placed on retired disk %d, want 1", i, loc.disk)
		}
	}
	if got := v.disks[0].extCount.Load(); got != 0 {
		t.Fatalf("retired disk 0 received %d extents, want 0", got)
	}

	// RemoveDisk on the only disk is rejected; RemoveDisk twice is idempotent.
	if err := v.RemoveDisk(1); err != nil {
		t.Fatalf("RemoveDisk(1): %v", err)
	}
	if err := v.RemoveDisk(7); err == nil {
		t.Fatal("RemoveDisk on out-of-range index should error")
	}
}

func TestV2StoreAddDiskGrowsStoreAndRoutesNewChunks(t *testing.T) {
	v, _ := newTestMultiStore(t, 2)
	v.SetDiskFactory(newDiskFactory(t))

	// Seed some data so disks 0 and 1 carry bytes; after AddDisk the fresh,
	// empty disk will be the strict least-used target for the next write.
	writeChunks(t, v, metadata.ChunkID(400), 6)

	dir := t.TempDir()
	idx, err := v.AddDisk(dir, 8, 8)
	if err != nil {
		t.Fatalf("AddDisk: %v", err)
	}
	if idx != 2 {
		t.Fatalf("AddDisk returned index %d, want 2", idx)
	}
	if got := len(v.disks); got != 3 {
		t.Fatalf("disks = %d after AddDisk, want 3", got)
	}
	if got := len(v.shards); got != 1 {
		t.Fatalf("shards = %d after AddDisk, want 1 (one shard store appended per add)", got)
	}
	if got := len(v.caps); got != 3 {
		t.Fatalf("caps = %d after AddDisk, want 3", got)
	}

	// New chunks go to the fresh, empty disk 2 (least-used) — the first
	// post-adopt write must land there, proving the adopted disk is eligible.
	base := metadata.ChunkID(500)
	writeChunks(t, v, base, 1)
	if loc := v.loc(base); loc.disk != 2 {
		t.Fatalf("first post-adopt chunk placed on disk %d, want 2", loc.disk)
	}
	// The adopted disk shows in DiskInfos with data.
	var found bool
	for _, di := range v.DiskInfos() {
		if di.Index == 2 && di.Dir == dir {
			found = true
		}
	}
	if !found {
		t.Fatalf("adopted disk not present in DiskInfos (dir %s): %+v", dir, v.DiskInfos())
	}
}

func TestV2StoreAddDiskWithoutFactoryDegrades(t *testing.T) {
	v, _ := newTestMultiStore(t, 1)
	if _, err := v.AddDisk(t.TempDir(), 8, 8); err == nil {
		t.Fatal("AddDisk without a disk factory should degrade to unsupported")
	}
}

// TestV2StoreECShardChunksSkipsRetiredShardStore guards the EC self-heal
// discovery path against a retired shard store: after RemoveDisk retires a disk
// whose shard backend is subsequently closed (re-adopt tears the old backend
// down to release its index flock), ListExtents on that closed store errors.
// ECShardChunks must SKIP the retired shard store — not abort discovery for the
// whole node — or the EC reaper logs "discovery failed" every sweep and never
// self-heals the healthy disks until the disk is re-adopted.
func TestV2StoreECShardChunksSkipsRetiredShardStore(t *testing.T) {
	v, _ := newTestShardMultiStore(t, 2)

	// A shard each on disk 0 (chunk 1) and disk 1 (chunk 2).
	if err := v.WriteShard(metadata.ChunkID(1), 0, []byte("s0")); err != nil {
		t.Fatalf("WriteShard(1): %v", err)
	}
	if err := v.WriteShard(metadata.ChunkID(2), 0, []byte("s1")); err != nil {
		t.Fatalf("WriteShard(2): %v", err)
	}

	// Both chunks discovered while both disks are healthy.
	all, err := v.ECShardChunks()
	if err != nil {
		t.Fatalf("ECShardChunks before retire: %v", err)
	}
	if !all[1] || !all[2] {
		t.Fatalf("ECShardChunks before retire = %v, want chunks 1 and 2", all)
	}

	// Retire disk 1, then close its (now unneeded) shard backend — the exact
	// state re-adopt of the same dir leaves behind (AddDisk closes the retired
	// data+shard backends to release their index flocks).
	if err := v.RemoveDisk(1); err != nil {
		t.Fatalf("RemoveDisk(1): %v", err)
	}
	closeStore(v.shards[1].store)

	// Discovery must still succeed and report only the healthy disk's shard.
	after, err := v.ECShardChunks()
	if err != nil {
		t.Fatalf("ECShardChunks with retired shard store errored (discovery aborts): %v", err)
	}
	if !after[1] {
		t.Fatal("healthy disk 0 shard (chunk 1) not discovered after retining disk 1")
	}
	if after[2] {
		t.Fatal("retired disk 1 shard (chunk 2) should be excluded from discovery")
	}
}

// TestV2StoreReAdoptSameDirAfterRetire proves retire → re-adopt the SAME dir is
// a reversible round-trip on the V2.1 engine. This uses on-disk (Pebble) segment
// stores so the retired backend actually holds its index flock — the exact
// failure the E2E hit ("open index: lock held by current process") — and the
// fix (AddDisk tearing down the retired backend before reopening) must clear it.
func TestV2StoreReAdoptSameDirAfterRetire(t *testing.T) {
	target := t.TempDir()
	// A factory that builds real on-disk segment stores (StreamID 1 data,
	// StreamID 2 shard) so each holds a real index flock. The shard index is
	// placed under dir/index-ecshard (mirroring runDataNodeV21.newDiskStores)
	// so the data and shard stores of the same dir don't collide on one lock.
	factory := func(dir string) (storage.Store, storage.Store, storage.Store, error) {
		data, err := segment.New(segment.Config{Dir: dir, UseMemIndex: false, StreamID: 1})
		if err != nil {
			return nil, nil, nil, err
		}
		shard, err := segment.New(segment.Config{Dir: dir, IndexDir: dir + "/index-ecshard", UseMemIndex: false, StreamID: 2})
		if err != nil {
			_ = data.Close()
			return nil, nil, nil, err
		}
		small, err := segment.NewSmallStore(segment.Config{Dir: dir, IndexDir: dir + "/index-small", UseMemIndex: false})
		if err != nil {
			_ = data.Close()
			_ = shard.Close()
			return nil, nil, nil, err
		}
		return data, shard, small, nil
	}

	// Seed TWO real on-disk disks so RemoveDisk on a third (adopted) disk isn't
	// rejected as "the only disk".
	seed := func(dir string) (*segment.Store, *segment.Store) {
		data, err := segment.New(segment.Config{Dir: dir, UseMemIndex: false, StreamID: 1})
		if err != nil {
			t.Fatalf("seed data store %s: %v", dir, err)
		}
		shard, err := segment.New(segment.Config{Dir: dir, IndexDir: dir + "/index-ecshard", UseMemIndex: false, StreamID: 2})
		if err != nil {
			t.Fatalf("seed shard store %s: %v", dir, err)
		}
		return data, shard
	}
	seed1, seed1s := seed(t.TempDir())
	seed2, seed2s := seed(t.TempDir())
	t.Cleanup(func() { seed1.Close(); seed1s.Close(); seed2.Close(); seed2s.Close() })
	v := NewMultiV2Store([]storage.Store{seed1, seed2}, "" /* dirs omitted */)
	// Attach the seeds' EC-shard stores so the shard slice stays index-aligned
	// with the data disks, exactly as runDataNodeV21's AttachShardStores does.
	if err := v.AttachShardStores([]storage.Store{seed1s, seed2s}); err != nil {
		t.Fatalf("attach shard stores: %v", err)
	}
	v.SetDiskFactory(factory)
	// Close every store V2Store adopts (including the re-adopted one) so their
	// segment background goroutines (compactor/WAL) stop before the t.TempDir()
	// cleanup removes the index dirs — otherwise a leaked goroutine races temp
	// cleanup and crashes a LATER test with "001/index: no such file or
	// directory". Close is idempotent, so this is safe alongside the seed
	// t.Cleanup above.
	t.Cleanup(func() {
		for _, b := range v.disks {
			closeStore(b.store)
		}
		for _, b := range v.shards {
			closeStore(b.store)
		}
	})

	// Give the seed disks some bytes so the freshly re-adopted (empty) disk is
	// the strict least-used placement target.
	if err := v.Write(metadata.ChunkID(1), []byte("seed-1")); err != nil {
		t.Fatalf("seed write 1: %v", err)
	}
	if err := v.Write(metadata.ChunkID(2), []byte("seed-2")); err != nil {
		t.Fatalf("seed write 2: %v", err)
	}

	// Adopt a temp dir → disk 2, then retire it, then re-adopt the SAME dir.
	// RemoveDisk preserves the slot and leaves the retired backend's flock on
	// the dir; the re-adopt must tear that backend down first (the fix) so it
	// can reopen the same dir instead of failing with "lock held by current
	// process".
	idx, err := v.AddDisk(target, 8, 8)
	if err != nil {
		t.Fatalf("adopt: %v", err)
	}
	if idx != 2 {
		t.Fatalf("adopt index = %d, want 2", idx)
	}
	if err := v.RemoveDisk(idx); err != nil {
		t.Fatalf("retire: %v", err)
	}
	if !v.diskFailed(idx) {
		t.Fatalf("retired disk %d should be FAILED", idx)
	}
	// The retired backend for the target dir must be locatable so the re-adopt
	// can tear it down; if not, its flock is never released and re-adopt fails.
	if got := v.retiredDiskIndexFor(target); got != idx {
		t.Fatalf("retiredDiskIndexFor(%s) = %d, want %d", target, got, idx)
	}
	reIdx, err := v.AddDisk(target, 8, 8)
	if err != nil {
		t.Fatalf("re-adopt same dir after retire: %v", err)
	}
	if reIdx != 3 {
		t.Fatalf("re-adopted disk index = %d, want 3 (appended after preserved slot)", reIdx)
	}
	if v.diskFailed(reIdx) {
		t.Fatalf("re-adopted disk %d should be healthy, not failed", reIdx)
	}

	// A fresh chunk must land on the healthy re-adopted disk (disk 3 is the
	// strictly least-used non-failed one) and read back.
	base := metadata.ChunkID(900)
	if err := v.Write(base, []byte("post-readopt")); err != nil {
		t.Fatalf("write post re-adopt: %v", err)
	}
	if loc := v.loc(base); loc.disk != reIdx {
		t.Fatalf("post-re-adopt chunk placed on disk %d, want %d", loc.disk, reIdx)
	}
	if got, _, err := v.Read(base, 0, 1<<20); err != nil || string(got) != "post-readopt" {
		t.Fatalf("read post-re-adopt: got=%q err=%v", got, err)
	}
}

// TestV2StoreDecommissionComposite exercises the ops-server decommission path:
// migrate the disk's data onto the remaining healthy disk, then retire it.
func TestV2StoreDecommissionComposite(t *testing.T) {
	v, _ := newTestMultiStore(t, 2)
	base := metadata.ChunkID(700)
	writeChunks(t, v, base, 6)

	migrated, err := v.MigrateDisk(0)
	if err != nil {
		t.Fatalf("MigrateDisk: %v", err)
	}
	if migrated < 1 {
		t.Fatal("MigrateDisk migrated nothing, expected to drain disk 0")
	}
	if err := v.RemoveDisk(0); err != nil {
		t.Fatalf("RemoveDisk: %v", err)
	}
	if v.diskFailed(0) != true {
		t.Fatal("decommissioned disk should be FAILED")
	}
	// Chunks survived on the survivor disk (none remains on the retired disk).
	for i := 0; i < 6; i++ {
		loc := v.loc(base + metadata.ChunkID(i))
		if loc.disk == 0 {
			t.Fatalf("chunk %d still claims retired disk 0 after decommission", i)
		}
		if _, _, err := v.Read(base+metadata.ChunkID(i), 0, 1<<20); err != nil {
			t.Fatalf("read chunk %d after decommission: %v", i, err)
		}
	}
}

// TestV2StoreDiskLifecycleSurvivesRestart verifies AddDisk's on-disk
// enumeration: a new disk is reconstructed into locOf so its data is readable
// after a fresh open of the same dirs.
func TestV2StoreDiskLifecycleSurvivesRestart(t *testing.T) {
	dir0, dir1, dir2 := t.TempDir(), t.TempDir(), t.TempDir()
	dirs := []string{dir0, dir1}
	backends := make([]*segment.Store, 2)
	for i, d := range dirs {
		s, err := segment.New(segment.Config{Dir: d, UseMemIndex: true, StreamID: 1})
		if err != nil {
			t.Fatalf("segment.New %d: %v", i, err)
		}
		backends[i] = s
	}
	stores := []storage.Store{backends[0], backends[1]}
	v := NewMultiV2Store(stores, dirs...)
	defer backends[0].Close()
	defer backends[1].Close()

	// Adopt a third disk and write to it in-session. The store has no small
	// streams attached (this test predates them), so AddDisk skips the small
	// store; nil is the honest factory return (reusing the data store as
	// "small" would have AddDisk close the live data store on discard).
	v.SetDiskFactory(func(dir string) (storage.Store, storage.Store, storage.Store, error) {
		s, err := segment.New(segment.Config{Dir: dir, UseMemIndex: true, StreamID: 1})
		return s, s, nil, err
	})
	if _, err := v.AddDisk(dir2, 8, 8); err != nil {
		t.Fatalf("AddDisk: %v", err)
	}
	// Record where each chunk lives before the simulated restart.
	base := metadata.ChunkID(900)
	want := make([]int, 3)
	for i := 0; i < 3; i++ {
		if err := v.Write(base+metadata.ChunkID(i), []byte("payload-"+string(rune('a'+i))+"-xxxxxxxxxx")); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
		want[i] = v.loc(base + metadata.ChunkID(i)).disk
	}

	// Simulate a restart: build a fresh V2Store over the same three dirs. The
	// adopted disk's data must enumerate back into locOf at the same disk.
	restartDirs := []string{dir0, dir1, dir2}
	restartStores := make([]storage.Store, 3)
	for i, d := range restartDirs {
		s, err := segment.New(segment.Config{Dir: d, UseMemIndex: true, StreamID: 1})
		if err != nil {
			t.Fatalf("restart segment.New %d: %v", i, err)
		}
		defer s.Close()
		restartStores[i] = s
	}
	rv := NewMultiV2Store(restartStores, restartDirs...)
	for i := 0; i < 3; i++ {
		if loc := rv.loc(base + metadata.ChunkID(i)); loc.disk != want[i] {
			t.Fatalf("after restart chunk %d lives on disk %d, want %d (pre-restart home)", i, loc.disk, want[i])
		}
		if _, _, err := rv.Read(base+metadata.ChunkID(i), 0, 1<<20); err != nil {
			t.Fatalf("read chunk %d after restart: %v", i, err)
		}
	}
}
