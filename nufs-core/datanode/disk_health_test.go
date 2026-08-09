package datanode

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dshmyz/nufs/nufs-core/datanode/storage"
	"github.com/dshmyz/nufs/nufs-core/datanode/storage/segment"
	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// ============ Program 8 V1-c: proactive disk-health monitor ============
//
// The reactive health path (Program 4 V1-c) only escalates failCount on WRITE
// failures. Program 8 adds a proactive probe so an idle or read-wedged disk —
// one that would otherwise sit at failCount 0 forever — is pushed to
// degraded/failed by a background monitor. Recovery stays write-path only:
// probe success NEVER lowers failCount; only a real write via writeTo
// (failCount.Store(0)) recovers a disk.

// statReadyStore wraps a real storage.Store and lets the test flip Stat
// between failing (a real I/O error the probe escalates) and responsive
// (returns ErrExtentNotFound — the sentinel probe extent never exists, so this
// is the "healthy" answer a real store gives). All other methods forward to
// the wrapped backend.
type statReadyStore struct {
	storage.Store
	real     storage.Store
	failStat bool
	stats    atomic.Int64 // count of Stat probes (monitor ticks observed)
}

func (s *statReadyStore) Stat(ctx context.Context, req *storage.StatRequest) (*storage.StatResult, error) {
	s.stats.Add(1)
	if s.failStat {
		return nil, storage.ErrSegmentUnavailable
	}
	return nil, storage.ErrExtentNotFound
}

// newProbeFixture builds a 2-disk V2Store where disk 1's backend is wrapped in
// a controllable statReadyStore (disk 0 is a plain real store used as a healthy
// placement target). Returns the store and the wrapper so the test can flip
// probe success/failure.
type probeFixture struct {
	v    *V2Store
	wrap *statReadyStore
}

func newProbeFixture(t *testing.T) *probeFixture {
	t.Helper()
	dirs := []string{t.TempDir(), t.TempDir()}
	backends := make([]*segment.Store, 2)
	wraps := make([]storage.Store, 2)
	for i := range dirs {
		s, err := segment.New(segment.Config{Dir: dirs[i], UseMemIndex: true, StreamID: 1})
		if err != nil {
			t.Fatalf("segment.New disk %d: %v", i, err)
		}
		backends[i] = s
	}
	wraps[0] = backends[0]
	// Disk 1's wrapper embeds the real store (so Write/Read/Delete still work)
	// and overrides only Stat for probe control.
	wrap := &statReadyStore{Store: backends[1], real: backends[1]}
	wraps[1] = wrap

	v := NewMultiV2Store(wraps)
	t.Cleanup(func() {
		for _, s := range backends {
			s.Close()
		}
	})
	return &probeFixture{v: v, wrap: wrap}
}

// TestV2StoreDiskMonitor_ProbeFailureDegradesIdleDisk proves the proactive
// monitor escalates a disk that has NO writes (idle) purely from probe failure:
// failCount climbs, the disk crosses into Degraded then Failed, and DiskInfos
// reflects it — closing the "idle disk never leaves online" gap that the
// reactive-only write path left open.
func TestV2StoreDiskMonitor_ProbeFailureDegradesIdleDisk(t *testing.T) {
	fx := newProbeFixture(t)
	v := fx.v

	// Disk 1 starts online and idle (no writes landed on it).
	if st := v.diskState(1); st != DiskOnline {
		t.Fatalf("diskState(1) initially=%v, want online", st)
	}

	// A single probe failure pushes failCount 0 -> 1 (Degraded).
	fx.wrap.failStat = true
	v.probeDisk(1)
	if fc := v.disks[1].failCount.Load(); fc != 1 {
		t.Fatalf("failCount after 1 failed probe=%d, want 1", fc)
	}
	if st := v.diskState(1); st != DiskDegraded {
		t.Fatalf("diskState after 1 probe fail=%v, want degraded", st)
	}

	// Crossing the >=5 threshold via repeated probe failures -> Failed.
	for i := 0; i < 4; i++ {
		v.probeDisk(1)
	}
	if st := v.diskState(1); st != DiskFailed {
		t.Fatalf("diskState after 5 probe fails=%v, want failed", st)
	}
	if !v.DiskInfos()[1].Failed {
		t.Fatalf("DiskInfos[1].Failed=false, want true after probe-driven failure")
	}
}

// TestV2StoreDiskMonitor_ProbeNoRecovery proves recovery is write-path only:
// probe success on a degraded disk does NOT lower failCount (it stays
// degraded), and only a real write via writeTo (failCount.Store(0)) recovers
// the disk back to online.
func TestV2StoreDiskMonitor_ProbeNoRecovery(t *testing.T) {
	fx := newProbeFixture(t)
	v := fx.v

	// Seed disk 0 so disk 1 is the least-used placement target (both start
	// empty, index 0 wins the tie).
	if err := v.Write(metadata.ChunkID(7777), []byte("seed-on-disk-zero")); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	v.mu.RLock()
	seedLoc := v.locOf[metadata.ChunkID(7777)]
	v.mu.RUnlock()
	if seedLoc.disk != 0 {
		t.Fatalf("seed chunk on disk %d, want disk 0", seedLoc.disk)
	}

	// Degrade disk 1 with probe failures.
	fx.wrap.failStat = true
	for i := 0; i < 2; i++ {
		v.probeDisk(1)
	}
	if st := v.diskState(1); st != DiskDegraded {
		t.Fatalf("diskState start=%v, want degraded", st)
	}

	// Probe success (disk responsive again) must NOT lower failCount —
	// recovery is exclusively the write path.
	fx.wrap.failStat = false
	v.probeDisk(1)
	if fc := v.disks[1].failCount.Load(); fc != 2 {
		t.Fatalf("failCount after probe success=%d, want unchanged 2 (no probe recovery)", fc)
	}
	if st := v.diskState(1); st != DiskDegraded {
		t.Fatalf("diskState after probe recovery attempt=%v, want still degraded", st)
	}

	// A REAL write that lands on disk 1 clears the streak -> recovered to online.
	// Disk 1 is the least-used target (disk 0 already has the seed bytes), so a
	// new chunk's nextLoc sends it to disk 1 — exercising the writeTo success
	// path that Store(0)s failCount.
	if err := v.Write(metadata.ChunkID(8888), []byte("real-write-clears-streak")); err != nil {
		t.Fatalf("write: %v", err)
	}
	v.mu.RLock()
	loc := v.locOf[metadata.ChunkID(8888)]
	v.mu.RUnlock()
	if loc.disk != 1 {
		t.Fatalf("recovery write landed on disk %d, want disk 1", loc.disk)
	}
	if fc := v.disks[1].failCount.Load(); fc != 0 {
		t.Fatalf("failCount after real write=%d, want 0 (write-path recovery)", fc)
	}
	if st := v.diskState(1); st != DiskOnline {
		t.Fatalf("diskState after real write=%v, want online", st)
	}
}

// TestV2StoreDiskMonitor_HealthyDiskUnaffected proves no false positives: on an
// all-healthy multi-disk store, running many probe ticks leaves every disk
// online with failCount 0.
func TestV2StoreDiskMonitor_HealthyDiskUnaffected(t *testing.T) {
	v, _ := newTestMultiStore(t, 2)

	for i := 0; i < 20; i++ {
		for d := 0; d < 2; d++ {
			// Responsive (ErrExtentNotFound) probes are not failures.
			v.probeDisk(d)
		}
	}
	for d := 0; d < 2; d++ {
		if fc := v.disks[d].failCount.Load(); fc != 0 {
			t.Fatalf("healthy disk %d failCount=%d, want 0", d, fc)
		}
		if st := v.diskState(d); st != DiskOnline {
			t.Fatalf("healthy disk %d state=%v, want online", d, st)
		}
	}
}

// TestV2StoreDiskMonitor_StartStopIdempotent proves StartDiskMonitor is
// idempotent (repeat calls don't double-spawn) and StopDiskMonitor halts the
// ticker without racing (-race). Disk 1's Stat always fails, so had two monitor
// goroutines both been ticking, failCount would climb past what a single
// monitor produces in the observation window — the idempotence guard keeps it
// bounded.
func TestV2StoreDiskMonitor_StartStopIdempotent(t *testing.T) {
	fx := newProbeFixture(t)
	v := fx.v
	fx.wrap.failStat = true

	// Shrink the probe cadence so the monitor actually ticks within the test.
	v.diskInterval = time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	v.StartDiskMonitor(ctx)
	v.StartDiskMonitor(ctx) // idempotent: no-op second start
	v.StartDiskMonitor(ctx) // and again

	// Let the monitor run several ticks. A double-spawn would drive roughly
	// twice the probe count; assert the observed probe count is consistent with
	// a single monitor goroutine, not two.
	time.Sleep(50 * time.Millisecond)
	v.StopDiskMonitor()

	probes := fx.wrap.stats.Load()
	if probes < 10 {
		t.Fatalf("monitor drove %d probes, want >=10 (a single monitor ticked repeatedly)", probes)
	}
	if probes > 120 {
		t.Fatalf("monitor drove %d probes, want <=120 (a single 1ms monitor does ~50 in 50ms; %d suggests a double-spawn)", probes, probes)
	}

	// Stop is idempotent and leaves the monitor stopped.
	v.StopDiskMonitor()
	v.StopDiskMonitor()
}
