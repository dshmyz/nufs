package metadata

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"time"
)

// EC conversion scheduler (§14 background pipeline): automatically discovers
// eligible replicated data and enqueues it for conversion to 6+3 erasure
// coding. This is the metad-side discovery half — the actual conversion
// execution is driven by the datanode's ECService.ConvertToEC (which owns the
// 5-step §14 transaction), consuming the BackgroundTasks this scheduler
// produces.
//
// Eligibility criteria (§14 prerequisites):
//   - Inline extent layout (single-extent files ≤ 16 MiB — the EC demographic)
//   - Idle for ≥ ECConversionIdleAge (30 days) since extent creation
//   - Not already EC (StorageClass != ColdEC, no ECStripeID)
//   - Lifecycle == Ready (degraded/migrating/deleting/converting skipped)
//   - Backing chunk exists and has no EC metadata (chunk.ECGroup == nil)
//
// Defers:
//   - Immutable flag (no filesystem-level immutable; 30-day idle is sufficient)
//   - Scrubbed timestamp (Lifecycle==Ready implies scrubber has processed it)
//   - Fault-domain diversity (checked at PlanShards execution, not at scan time)
//   - datanode-side ConversionWorker (consumes BackgroundTasks, separate knife)

// ConversionEligibility carries the fields needed to submit a conversion task
// without re-reading the inode or extent metadata.
type ConversionEligibility struct {
	// Inode is the owning inode.
	Inode InodeID
	// Extent is the inline extent to convert.
	Extent ExtentIDV2
	// ExtentCreated is the extent's creation timestamp (unix nanos).
	ExtentCreated int64
	// Size is the logical file size (from the inline extent's LogicalLen).
	Size int64
}

// ScanV2Inlines iterates over all inodes that have a V2 inline extent layout.
// V1 inodes (Layout==LayoutEmpty when decoded as InodeMetaV2) and multi-extent
// inodes (LayoutExtentPages) are silently skipped. The callback receives the
// inode ID, the full V2 inode, and its inline extent metadata.
func (s *PebbleStore) ScanV2Inlines(ctx context.Context, fn func(InodeID, *InodeMetaV2, *ExtentMetaV2) error) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}
	return s.scanPrefix(prefixInode, func(key, val []byte) error {
		var in InodeMetaV2
		if err := unmarshalValue(val, &in); err != nil {
			// Corrupted or V1-only row: V1 rows decode with Layout==LayoutEmpty
			// when unmarshalled as InodeMetaV2 (no Layout field in msgpack →
			// zero value). Skip silently — not an error, just not V2.
			return nil
		}
		if in.Layout != LayoutInlineExtent || in.InlineExtent == nil {
			return nil
		}
		return fn(in.ID, &in, in.InlineExtent)
	})
}

// ECConversionEligible checks the four §14 prerequisites for automatic
// replicas→EC conversion. Returns (*ConversionEligibility, true, nil) when
// eligible, (nil, false, nil) when not, or (nil, false, err) on store error.
//
// Eligibility criteria and their implementation rationale are documented inline.
func (s *PebbleStore) ECConversionEligible(ctx context.Context, id InodeID, in *InodeMetaV2, ext *ExtentMetaV2) (*ConversionEligibility, bool, error) {
	// 1. Idle for ≥ ECConversionIdleAge (30 days, §14).
	//
	// Use extent CreatedAt (last data-write timestamp) rather than inode MTime,
	// because MTime is updated by non-data operations (link, chown, xattr) that
	// do not invalidate the data content eligible for conversion. Fallback to
	// MTime when CreatedAt is unset (pre-migration extents).
	created := ext.CreatedAt
	if created == 0 {
		created = in.MTime
	}
	idle := time.Since(time.Unix(0, created))
	if idle < ECConversionIdleAge {
		return nil, false, nil
	}

	// 2. Not already EC: StorageClass must be HotReplica and ECStripeID unset.
	if ext.StorageClass == StorageClassColdEC || ext.ECStripeID != "" {
		return nil, false, nil
	}

	// 3. Lifecycle == Ready: degraded/migrating/deleting/converting extents are
	// skipped. LifecycleReady implies the scrubber has processed this extent
	// (ReadyDegraded/Unhealthy are set by ExtentScrubber on detection), so this
	// doubles as the "scrubbed" prerequisite from §14 without tracking a
	// separate per-extent scrub timestamp.
	if ext.Lifecycle != LifecycleReady {
		return nil, false, nil
	}

	// 4. Backing chunk exists and has no EC metadata on the chunk side.
	//
	// The chunk-side EC check catches the race where the direct-EC write path
	// (PlanECWrite → RecordDirectEC) has switched the chunk to EC but the
	// extent-side mark (MarkExtentColdEC) has not yet landed. Without this
	// cross-check the scheduler would re-enqueue an already-converted chunk.
	cid := ChunkID(ext.ID)
	chunk, err := s.GetChunk(ctx, cid)
	if err != nil {
		// Chunk not found or store error: skip (the chunk may have been GC'd).
		return nil, false, nil
	}
	if chunk.ECGroup != nil || chunk.ECStripeID != "" {
		return nil, false, nil
	}

	return &ConversionEligibility{
		Inode:         id,
		Extent:        ext.ID,
		ExtentCreated: created,
		Size:          ext.LogicalLen,
	}, true, nil
}

// SubmitConversion enqueues one EC conversion background task for an eligible
// extent. The task ID is deterministic ("ec-convert-{extentID}"), so repeated
// calls for the same extent are idempotent — the existing task is silently
// preserved regardless of its state.
func (s *PebbleStore) SubmitConversion(ctx context.Context, elig *ConversionEligibility) error {
	taskID := "ec-convert-" + strconv.FormatUint(uint64(elig.Extent), 10)

	// Idempotency: a task with this ID already exists. Preserve it regardless
	// of state (queued → will be processed; leased/running → in progress;
	// succeeded → done; dead_letter → operator decision).
	if existing, err := s.GetBackgroundTask(ctx, taskID); err == nil && existing != nil {
		return nil
	} else if err != nil && err != ErrEntryNotFound {
		return err
	}

	now := time.Now().UnixNano()
	task := &BackgroundTask{
		ID:        taskID,
		Type:      TaskECConvert,
		State:     TaskQueued,
		Target:    taskID,
		NextRunAt: now,
		CreatedAt: now,
	}
	return s.PutBackgroundTask(ctx, task)
}

// ECConversionScheduler is a background worker that periodically scans all
// V2 inline-extent inodes for EC conversion eligibility and enqueues those
// that meet the §14 prerequisites. It follows the same Start/Stop lifecycle
// as ScrubWorker, ChunkGC, and ExtentScrubber.
type ECConversionScheduler struct {
	store    *PebbleStore
	interval time.Duration
	stop     chan struct{}
	stopped  chan struct{}
}

// NewECConversionScheduler creates a conversion scheduler over the given store.
// Call Start to begin periodic scanning.
func NewECConversionScheduler(store *PebbleStore) *ECConversionScheduler {
	return &ECConversionScheduler{
		store:   store,
		stop:    make(chan struct{}),
		stopped: make(chan struct{}),
	}
}

// Start begins periodic scanning at the given interval. Each pass walks all
// V2 inodes, checks eligibility, and submits background tasks for eligible
// extents. The first pass runs immediately on the calling goroutine to seed
// the queue without waiting for the first tick.
func (s *ECConversionScheduler) Start(interval time.Duration) {
	s.interval = interval
	go s.loop()
}

// Stop terminates the scheduler. It blocks until the current pass (if any)
// finishes.
func (s *ECConversionScheduler) Stop() {
	close(s.stop)
	<-s.stopped
}

func (s *ECConversionScheduler) loop() {
	defer close(s.stopped)
	// First pass runs immediately to seed the queue without waiting for the
	// first ticker fire. Subsequent passes run on the ticker.
	s.run()
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-ticker.C:
			s.run()
		}
	}
}

func (s *ECConversionScheduler) run() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	var scanned, eligible, submitted, skipped int
	err := s.store.ScanV2Inlines(ctx, func(id InodeID, in *InodeMetaV2, ext *ExtentMetaV2) error {
		scanned++
		elig, ok, err := s.store.ECConversionEligible(ctx, id, in, ext)
		if err != nil {
			slog.Error("ec conversion scheduler: eligibility check failed",
				"inode", id, "extent", uint64(ext.ID), "error", err)
			return nil // continue scanning
		}
		if !ok {
			skipped++
			return nil
		}
		eligible++
		if err := s.store.SubmitConversion(ctx, elig); err != nil {
			slog.Error("ec conversion scheduler: submit failed",
				"inode", id, "extent", uint64(ext.ID), "error", err)
			return nil
		}
		submitted++
		return nil
	})
	if err != nil {
		slog.Error("ec conversion scheduler: scan failed", "error", err)
		return
	}
	slog.Info("ec conversion scheduler: scan complete",
		"scanned", scanned,
		"eligible", eligible,
		"submitted", submitted,
		"skipped", skipped,
	)
}

// ConversionQueue returns all queued EC conversion background tasks (for the
// ops queue endpoint).
func (s *PebbleStore) ConversionQueue(ctx context.Context) ([]BackgroundTask, error) {
	if s.closed.Load() {
		return nil, ErrServiceClosed
	}
	var tasks []BackgroundTask
	prefix := backgroundTaskQueuePrefix(TaskECConvert, TaskQueued)
	err := s.scanPrefix(prefix, func(key, value []byte) error {
		_, id, err := backgroundTaskFromQueueKey(string(key))
		if err != nil {
			return nil
		}
		task, err := s.GetBackgroundTask(ctx, id)
		if err != nil {
			return nil
		}
		tasks = append(tasks, *task)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("ec conversion queue: %w", err)
	}
	return tasks, nil
}
