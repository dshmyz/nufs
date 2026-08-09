package datanode

import (
	"testing"

	"github.com/dshmyz/nufs/nufs-core/datanode/storage"
	"github.com/dshmyz/nufs/nufs-core/datanode/storage/maintenance"
	"github.com/dshmyz/nufs/nufs-core/datanode/storage/segment"
)

// haveSegmentStore builds a small real segment store, the backing a V2Store
// normally wraps.
func haveSegmentStore(t *testing.T, dir string) storage.Store {
	s, err := segment.New(segment.Config{
		Dir:         dir,
		UseMemIndex: true,
		StreamID:    1,
		SegmentSize: 64 << 10,
	})
	if err != nil {
		t.Fatalf("segment.New: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// fakeGuard returns a CapacityGuard with deterministic FreeBytes/TotalBytes
// so the write-path admission test does not depend on the host filesystem.
func fakeGuard(total, free int64) *maintenance.CapacityGuard {
	g := maintenance.NewCapacityGuard()
	g.TotalBytes = func() int64 { return total }
	g.FreeBytes = func() int64 { return free }
	return g
}

// TestCapacityGuard_RejectsAtLowWatermark wires a V2Store whose disk can only
// hold a tiny fraction of the write; the write path must reject with
// storage.ErrCapacity before the disk actually fills.
func TestCapacityGuard_RejectsAtLowWatermark(t *testing.T) {
	v := NewV2Store(haveSegmentStore(t, t.TempDir()), t.TempDir())
	// free=4 of total=100 => 4% free, below ForceReadOnlyFreePct(5%).
	v.caps[0] = fakeGuard(100, 4)

	err := v.Write(777, make([]byte, 1024))
	if err != storage.ErrCapacity {
		t.Fatalf("expected storage.ErrCapacity at 4%% free, got %v", err)
	}
}

// TestCapacityGuard_RejectsAtRejectWritesWatermark: 5..10% free band also
// rejects ordinary writes with storage.ErrCapacity.
func TestCapacityGuard_RejectsAtRejectWritesWatermark(t *testing.T) {
	v := NewV2Store(haveSegmentStore(t, t.TempDir()), t.TempDir())
	// free=7 of total=100 => 7% free: above ForceReadOnly(5), below
	// RejectWrites(10) => ordinary writes rejected.
	v.caps[0] = fakeGuard(100, 7)

	err := v.Write(778, make([]byte, 1024))
	if err != storage.ErrCapacity {
		t.Fatalf("expected storage.ErrCapacity at 7%% free, got %v", err)
	}
}

// TestCapacityGuard_AllowsAtHealthyWatermark verifies a comfortably-full disk
// accepts writes through the same public path.
func TestCapacityGuard_AllowsAtHealthyWatermark(t *testing.T) {
	v := NewV2Store(haveSegmentStore(t, t.TempDir()), t.TempDir())
	v.caps[0] = fakeGuard(100, 50) // 50% free → allowed

	if err := v.Write(779, make([]byte, 1024)); err != nil {
		t.Fatalf("healthy disk rejected write: %v", err)
	}
}

// TestCapacityGuard_ShardWriteRejected verifies the EC-shard write path
// (writeShardAt) applies the same capacity admission: a shard write onto a
// full physical disk is rejected with storage.ErrCapacity.
func TestCapacityGuard_ShardWriteRejected(t *testing.T) {
	var dirs []string
	var dataStores, shardStores []storage.Store
	for i := 0; i < 2; i++ {
		d := t.TempDir()
		dirs = append(dirs, d)
		ds, err := segment.New(segment.Config{Dir: d, UseMemIndex: true, StreamID: 1, SegmentSize: 256 << 10})
		if err != nil {
			t.Fatalf("segment.New data %d: %v", i, err)
		}
		ss, err := segment.New(segment.Config{Dir: d, UseMemIndex: true, StreamID: 2, SegmentSize: 256 << 10})
		if err != nil {
			t.Fatalf("segment.New shard %d: %v", i, err)
		}
		dataStores = append(dataStores, ds)
		shardStores = append(shardStores, ss)
	}
	v := NewMultiV2Store(dataStores, dirs...)
	if err := v.AttachShardStores(shardStores); err != nil {
		t.Fatalf("AttachShardStores: %v", err)
	}
	t.Cleanup(func() {
		for _, s := range shardStores {
			_ = s.(interface{ Close() error }).Close()
		}
		for _, s := range dataStores {
			_ = s.(interface{ Close() error }).Close()
		}
	})

	// Drive disk 1 (the shard write target) to 4% free: shard index 1 writes
	// land on shards[1], whose physical root shares disks[1].dir == dirs[1].
	v.caps[1] = fakeGuard(100, 4)

	err := v.WriteShardAtDisk(999, 1, 1, make([]byte, 1024))
	if err != storage.ErrCapacity {
		t.Fatalf("expected storage.ErrCapacity for shard write on full disk, got %v", err)
	}
}

// TestCapacityGuard_NilGuardIsNoOp ensures that when capacity protection is
// absent (caps[i]==nil, e.g. unknown filesystem root) the write path is
// completely unchanged — the pre-existing behavior.
func TestCapacityGuard_NilGuardIsNoOp(t *testing.T) {
	v := NewV2Store(haveSegmentStore(t, t.TempDir())) // no dir → capacityForDisk returns nil
	if v.caps[0] != nil {
		t.Fatalf("expected nil guard without a dir, got %#v", v.caps[0])
	}
	if err := v.Write(780, make([]byte, 1024)); err != nil {
		t.Fatalf("write with nil guard failed: %v", err)
	}
}
