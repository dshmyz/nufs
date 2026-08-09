package datanode

import (
	"github.com/dshmyz/nufs/nufs-core/datanode/storage"
	"github.com/dshmyz/nufs/nufs-core/datanode/storage/maintenance"
	"golang.org/x/sys/unix"
)

// capacityForDisk builds a CapacityGuard (§10.4) for one physical disk
// root. dir is the disk's filesystem root (the segment store's Config.Dir).
// The guard rejects new ordinary writes before the disk actually fills
// (hard watermarks), preventing ENOSPC. WriteRate/ReclaimRate are left
// nil so admission control (time_to_full vs time_to_reclaim) is skipped —
// the fixed reject/read-only watermarks alone gate writes, which is all
// the serving path needs without a background reclaim-accounting loop.
func capacityForDisk(dir string) *maintenance.CapacityGuard {
	if dir == "" {
		return nil // no filesystem root known; leave capacity protection off
	}
	g := maintenance.NewCapacityGuard()
	g.TotalBytes = func() int64 { return detectCapacityBytes(dir) }
	g.FreeBytes = func() int64 { return detectFreeBytes(dir) }
	return g
}

// detectFreeBytes returns the filesystem free bytes for dir via Statfs, or
// 0 if it cannot be determined.
func detectFreeBytes(dir string) int64 {
	var s unix.Statfs_t
	if err := unix.Statfs(dir, &s); err != nil {
		return 0
	}
	return int64(s.Bavail) * int64(s.Bsize)
}

// admitDiskWrite applies capacity admission control for a write destined
// for disk. It is a no-op when the disk has no guard (capacity protection
// disabled / unknown filesystem root), preserving pre-existing behavior.
// It returns storage.ErrCapacity when the disk is too full to admit the
// write, so the write is rejected before it can trip ENOSPC.
func (v *V2Store) admitDiskWrite(disk int, sizeBytes int64) error {
	if disk < 0 || disk >= len(v.caps) {
		return nil
	}
	g := v.caps[disk]
	if g == nil {
		return nil
	}
	return g.AdmitWrite(sizeBytes)
}

// compile-time check that we surface the guard's capacity sentinel.
var _ = storage.ErrCapacity
