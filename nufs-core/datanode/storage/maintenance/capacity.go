package maintenance

import (
	"github.com/dshmyz/nufs/nufs-core/datanode/storage"
)

// CapacityGuard enforces the §10.4 capacity protection thresholds and
// the §10.4 admission control (time_to_full vs time_to_reclaim).
type CapacityGuard struct {
	// CompactionReservePct reserves disk for emergency compaction (§16).
	CompactionReservePct float64
	// MetadataReservePct reserves for commit-log/checkpoint/manifest/index.
	MetadataReservePct float64
	// RejectWritesFreePct: below this, reject new ordinary writes.
	RejectWritesFreePct float64
	// ForceReadOnlyFreePct: below this, force protective read-only.
	ForceReadOnlyFreePct float64
	// WarnFreePct: below this, admission control (time_to_full vs
	// time_to_reclaim) gates writes (§10.4 "before the fixed watermarks").
	WarnFreePct float64

	// Stats hook, injected by the store so the guard stays testable.
	FreeBytes func() int64
	TotalBytes func() int64
	// WriteRate returns bytes/sec of recent allocation (for time_to_full).
	WriteRate func() float64
	// ReclaimRate returns bytes/sec of recent reclaim (for time_to_reclaim).
	ReclaimRate func() float64
}

// NewCapacityGuard creates a guard with §16 defaults.
func NewCapacityGuard() *CapacityGuard {
	return &CapacityGuard{
		CompactionReservePct: 10,
		MetadataReservePct:   5,
		RejectWritesFreePct:  10,
		ForceReadOnlyFreePct: 5,
		WarnFreePct:          15,
	}
}

// FreePct returns the current free-space percentage.
func (g *CapacityGuard) FreePct() float64 {
	if g.FreeBytes == nil || g.TotalBytes == nil {
		return 100
	}
	total := g.TotalBytes()
	if total <= 0 {
		return 100
	}
	return float64(g.FreeBytes()) / float64(total) * 100
}

// AdmitWrite returns nil if a write of sizeBytes is allowed, or a typed
// error if capacity protection rejects it (§10.4).
func (g *CapacityGuard) AdmitWrite(sizeBytes int64) error {
	freePct := g.FreePct()
	// Hard threshold: below force-read-only, reject everything.
	if freePct < g.ForceReadOnlyFreePct {
		return storage.ErrCapacity
	}
	// Below reject-writes threshold, reject ordinary writes.
	if freePct < g.RejectWritesFreePct {
		return storage.ErrCapacity
	}
	// Admission control: only once below the warn watermark do we gate
	// on time_to_full vs time_to_reclaim (§10.4 "rejects or throttles
	// writes before the fixed watermarks").
	if freePct < g.WarnFreePct && g.WriteRate != nil && g.ReclaimRate != nil {
		wr := g.WriteRate()
		rr := g.ReclaimRate()
		if wr > 0 && rr >= 0 {
			timeToFull := float64(g.FreeBytes()) / wr
			timeToReclaim := 0.0
			if rr > 0 {
				timeToReclaim = float64(g.ReclaimableBytes()) / rr
			}
			// Reject when we fill up before reclaim completes (reclaim
			// cannot keep pace) — the §10.4 admission-control gate.
			if timeToReclaim > timeToFull {
				return storage.ErrCapacity
			}
		}
	}
	_ = sizeBytes
	return nil
}

// ReclaimableBytes is the space recoverable by compaction (bounded by
// the compaction reserve). The guard uses it for admission control.
func (g *CapacityGuard) ReclaimableBytes() int64 {
	if g.FreeBytes == nil || g.TotalBytes == nil {
		return 0
	}
	total := g.TotalBytes()
	reserve := int64(float64(total) * g.CompactionReservePct / 100)
	free := g.FreeBytes()
	if free > reserve {
		return free - reserve
	}
	return 0
}

// ReserveBytes returns the total protected capacity (compaction +
// metadata reserves).
func (g *CapacityGuard) ReserveBytes() int64 {
	if g.TotalBytes == nil {
		return 0
	}
	total := g.TotalBytes()
	return int64((g.CompactionReservePct + g.MetadataReservePct) / 100 * float64(total))
}
