package maintenance

import (
	"testing"

	"github.com/example/dfs/datanode/storage"
)

func TestCapacityGuard_Thresholds(t *testing.T) {
	g := NewCapacityGuard()
	g.TotalBytes = func() int64 { return 100 }
	g.FreeBytes = func() int64 { return 60 }
	if err := g.AdmitWrite(10); err != nil {
		t.Fatalf("60%% free should admit, got %v", err)
	}

	// Below reject threshold (10%): reject ordinary writes.
	g.FreeBytes = func() int64 { return 8 }
	if err := g.AdmitWrite(10); err != storage.ErrCapacity {
		t.Fatalf("8%% free should reject, got %v", err)
	}

	// Below force-read-only (5%): reject everything.
	g.FreeBytes = func() int64 { return 4 }
	if err := g.AdmitWrite(1); err != storage.ErrCapacity {
		t.Fatalf("4%% free should force read-only, got %v", err)
	}
}

func TestCapacityGuard_AdmissionControl(t *testing.T) {
	g := NewCapacityGuard()
	g.TotalBytes = func() int64 { return 100 }
	g.FreeBytes = func() int64 { return 12 } // below warn (15%), above reject (10%)
	// Fast allocation, slow reclaim → reject before the hard watermark.
	g.WriteRate = func() float64 { return 100 }  // 100 B/s allocation
	g.ReclaimRate = func() float64 { return 10 } // 10 B/s reclaim
	if err := g.AdmitWrite(10); err != storage.ErrCapacity {
		t.Fatalf("reclaim cannot keep pace, should reject, got %v", err)
	}
	// Reclaim keeps pace → admit.
	g.WriteRate = func() float64 { return 10 }
	g.ReclaimRate = func() float64 { return 100 }
	if err := g.AdmitWrite(10); err != nil {
		t.Fatalf("reclaim keeps pace, should admit, got %v", err)
	}
}

func TestCapacityGuard_Reserve(t *testing.T) {
	g := NewCapacityGuard()
	g.TotalBytes = func() int64 { return 1000 }
	if got := g.ReserveBytes(); got != int64(0.15*1000) {
		t.Fatalf("reserve = %d, want 150", got)
	}
	// Reclaimable is bounded by free minus the compaction reserve.
	g.FreeBytes = func() int64 { return 300 }
	if got := g.ReclaimableBytes(); got != 300-100 {
		t.Fatalf("reclaimable = %d, want 200", got)
	}
}
