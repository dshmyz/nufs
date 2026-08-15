package maintenance

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/dshmyz/nufs/nufs-core/datanode/storage"
	"github.com/dshmyz/nufs/nufs-core/datanode/storage/segment"
)

// diskCompactStore is the per-disk segment.Store surface the compaction
// worker drives: enumerate sealed segments, act as the compactor sink,
// answer liveness via Stat, and enumerate live extents so the worker can
// distinguish the newest generation of an extent from older (dead) ones.
// *segment.Store satisfies it.
type diskCompactStore interface {
	// SealedSegments lists this disk's sealed data-stream segments.
	SealedSegments() ([]segment.SealedSegment, error)
	// ListExtents enumerates each extent's single live (newest) generation.
	ListExtents() ([]segment.LiveExtent, error)
	// StoreSink is the append + relocate path the compactor writes to.
	storage.StoreSink
	// Stat answers the read-authority location of one record, used to
	// decide whether the record's newest generation still lives at the
	// source segment.
	Stat(ctx context.Context, req *storage.StatRequest) (*storage.StatResult, error)
}

// CompressionWorker runs the background compaction+reclaim loop for the
// V2.1 engine (§10.3). Segments only rotate on write-full; without this
// loop the physical on-disk footprint grows without bound as overwritten
// generations leave dead bytes in sealed segments. It is a local,
// per-disk transaction — no cross-node coordination.
type CompressionWorker struct {
	stores     []diskCompactStore
	sched      *Scheduler
	interval   time.Duration
	maxPerTick int
}

// CompressionWorkerOption customizes a CompressionWorker.
type CompressionWorkerOption func(*CompressionWorker)

// WithCompactionInterval overrides the default 30s scan cadence.
func WithCompactionInterval(d time.Duration) CompressionWorkerOption {
	return func(w *CompressionWorker) { w.interval = d }
}

// NewCompressionWorker creates a compaction worker over the given
// per-disk segment stores (from V2Store.DataStores). Only stores that
// surface the compactable segment surface (*segment.Store) are driven;
// EC shard stores (which do not) are skipped. Each tick the worker scans
// every store's sealed segments, scores them, and compacts the eligible
// highest-value segment(s), removing reclaimed source files so their dead
// bytes return to the filesystem.
func NewCompressionWorker(stores []storage.Store, opts ...CompressionWorkerOption) *CompressionWorker {
	w := &CompressionWorker{
		sched:      NewScheduler(),
		interval:   30 * time.Second,
		maxPerTick: 1, // one segment per disk per tick keeps impact bounded
	}
	for _, st := range stores {
		if ds, ok := st.(diskCompactStore); ok {
			w.stores = append(w.stores, ds)
		}
	}
	for _, o := range opts {
		o(w)
	}
	return w
}

// Run drives the loop until ctx is cancelled. It compacts at most one
// segment per disk per tick so a burst of newly-stale segments is
// drained gradually (bounded write amplification, §13.1).
func (w *CompressionWorker) Run(ctx context.Context) {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	w.onePass()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.onePass()
		}
	}
}

// RunOnce performs a single compaction pass synchronously (used by tests
// and for a warmup scan at startup).
func (w *CompressionWorker) RunOnce() {
	w.onePass()
}

func (w *CompressionWorker) onePass() {
	for _, st := range w.stores {
		w.compactOne(st)
	}
}

// compactOne scans one disk's sealed segments and compacts the highest
// scoring eligible one (if any).
func (w *CompressionWorker) compactOne(st diskCompactStore) {
	sealed, err := st.SealedSegments()
	if err != nil {
		slog.Error("compaction: list sealed segments", "error", err)
		return
	}
	if len(sealed) == 0 {
		return
	}

	// Score each sealed segment from a scan of its records. A record is
	// dead when the store's read authority no longer points it at this
	// source segment (superseded by a newer generation elsewhere) or the
	// extent is tombstoned.
	var candidates []CompactionCandidate
	for _, seg := range sealed {
		c, ok := w.score(st, seg)
		if !ok {
			continue // torn/unreadable; leave it for a later pass
		}
		candidates = append(candidates, c)
	}
	if len(candidates) == 0 {
		return
	}

	selected := w.sched.Select(candidates, w.maxPerTick, 1, 1, 1)
	for _, cand := range selected {
		w.compactSegment(st, cand)
	}
}

// score scans a sealed segment and builds its compaction candidate.
func (w *CompressionWorker) score(st diskCompactStore, seg segment.SealedSegment) (CompactionCandidate, bool) {
	rd, err := segment.OpenReader(seg.Path)
	if err != nil {
		return CompactionCandidate{}, false
	}
	defer rd.Close()

	records, err := segment.ScanSegmentRecords(seg.Path, rd)
	if err != nil {
		return CompactionCandidate{}, false
	}

	// A scanned record is live only if it belongs to its extent's newest
	// generation AND that newest generation still lives at the source
	// segment. Anything older, or relocated/tombstoned, is reclaimable
	// dead bytes. Build the newest-generation map once per segment scan.
	latest := w.latestGenerations(st)
	live := func(ext storage.ExtentID, gen storage.Generation) bool {
		lg, ok := latest[ext]
		if !ok || lg != gen {
			return false
		}
		return w.isLive(st, seg.SegmentID, ext, gen)
	}

	var deadBytes, liveBytes int64
	var deadRecs, total uint64
	for _, rec := range records {
		total++
		if live(rec.ExtentID, rec.Generation) {
			liveBytes += int64(rec.StoredLen)
		} else {
			deadBytes += int64(rec.StoredLen)
			deadRecs++
		}
	}
	deadRatio := 0.0
	if total > 0 {
		deadRatio = float64(deadRecs) / float64(total)
	}
	c := CompactionCandidate{
		SegmentID:       seg.SegmentID,
		Path:            seg.Path,
		RecordCount:     total,
		DeadBytes:       deadBytes,
		LiveBytes:       liveBytes,
		DeadRecordRatio: deadRatio,
	}
	if total > 0 {
		c.ScoreWith(1, 1, 1)
	}
	return c, true
}

// compactSegment runs the §10.3 compaction transaction for a selected
// segment and, on success, removes the source file so its dead bytes are
// reclaimed. Failure leaves the source intact for a later pass.
func (w *CompressionWorker) compactSegment(st diskCompactStore, cand CompactionCandidate) {
	w.sched.Track(cand.SegmentID)
	defer w.sched.Release(cand.SegmentID)

	rd, err := segment.OpenReader(cand.Path)
	if err != nil {
		slog.Error("compaction: open source", "segment", cand.SegmentID, "error", err)
		return
	}
	records, err := segment.ScanSegmentRecords(cand.Path, rd)
	rd.Close()
	if err != nil {
		slog.Error("compaction: scan source", "segment", cand.SegmentID, "error", err)
		return
	}

	compactor := NewCompactor(st, nil) // finishSealed: destination appends reuse the store's active segment
	latest := w.latestGenerations(st)
	isLive := func(ext storage.ExtentID, gen storage.Generation) bool {
		lg, ok := latest[ext]
		if !ok || lg != gen {
			return false // older generation → dead
		}
		return w.isLive(st, cand.SegmentID, ext, gen)
	}
	copied, err := compactor.Compact(cand.SegmentID, toCompactorRecords(records), isLive)
	if err != nil {
		slog.Error("compaction: compact", "segment", cand.SegmentID, "error", err)
		return
	}

	if err := os.Remove(cand.Path); err != nil {
		slog.Error("compaction: remove source", "segment", cand.SegmentID, "error", err)
		return
	}
	slog.Info("compaction: reclaimed sealed segment", "segment", cand.SegmentID,
		"live_records", copied, "dead_bytes", cand.DeadBytes)
}

// latestGenerations returns, per extent, its newest (live) generation as
// seen in the read-authority view. Older generations of the same extent
// are dead bytes regardless of where they physically live.
func (w *CompressionWorker) latestGenerations(st diskCompactStore) map[storage.ExtentID]storage.Generation {
	exts, err := st.ListExtents()
	if err != nil {
		return map[storage.ExtentID]storage.Generation{}
	}
	out := make(map[storage.ExtentID]storage.Generation, len(exts))
	for _, e := range exts {
		out[e.ExtentID] = e.Generation
	}
	return out
}

// isLive reports whether the extent's read-authority location still
// points at srcID (i.e. the source segment still owns this generation).
// Any tombstone, corruption, or a newer location elsewhere makes it dead.
func (w *CompressionWorker) isLive(st diskCompactStore, srcID storage.SegmentID, ext storage.ExtentID, gen storage.Generation) bool {
	res, err := st.Stat(context.Background(), &storage.StatRequest{ExtentID: ext, Generation: gen})
	if err != nil {
		return false
	}
	if res.State == storage.ExtentTombstoned || res.State == storage.ExtentCorrupt {
		return false
	}
	return res.SegmentID == srcID
}

// toCompactorRecords converts a segment scan into the compactor's record
// shape. The two types are structurally identical; only the declaration
// lives in a different package.
func toCompactorRecords(in []segment.ScannedRecord) []ScannedRecord {
	out := make([]ScannedRecord, len(in))
	for i, r := range in {
		out[i] = ScannedRecord{
			ExtentID:    r.ExtentID,
			Generation:  r.Generation,
			StoredLen:   r.StoredLen,
			LogicalLen:  r.LogicalLen,
			Codec:       r.Codec,
			ReadPayload: r.ReadPayload,
		}
	}
	return out
}
