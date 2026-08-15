package datanode

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dshmyz/nufs/nufs-core/datanode/storage"
	"github.com/dshmyz/nufs/nufs-core/datanode/storage/journal"
	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// ============================================================
// ScrubWorker — automatic background data integrity scan
// ============================================================
//
// ScrubWorker periodically reads every chunk on the node and verifies
// its CRC32C checksum, detecting silent corruption (bitrot) that
// occurred after the chunk was written. When corruption is found, an
// EventScrubFinding is appended to the change journal so the heartbeat
// ships it to the metadata authority for reconciliation.
//
// This is the V2.1 replacement for the V1 AntiEntropy scanner.

const (
	scrubDefaultInterval   = 6 * time.Hour
	scrubDefaultBatchSize  = 100
	scrubDefaultBatchDelay = 10 * time.Millisecond
)

// ScrubStats returns live scrub worker counters.
type ScrubStats struct {
	Scanned int64 `json:"scanned"` // chunks successfully verified (pass + fail)
	Corrupt int64 `json:"corrupt"` // chunks with CRC mismatch
	Skipped int64 `json:"skipped"` // chunks with no stored checksum (skipped)
	Failed  int64 `json:"failed"`  // chunks that errored on read (I/O, not corruption)
}

// ScrubWorker is a background goroutine that periodically scans all local
// chunks and verifies their data integrity.
type ScrubWorker struct {
	store   *V2Store
	journal *journal.ChangeJournal // optional; nil = count-only, no journal writes

	interval   time.Duration
	batchSize  int
	batchDelay time.Duration

	stopCh  chan struct{}
	running bool
	mu      sync.Mutex
	wg      sync.WaitGroup

	scanned atomic.Int64
	corrupt atomic.Int64
	skipped atomic.Int64
	failed  atomic.Int64
}

// ScrubOption configures a ScrubWorker.
type ScrubOption func(*ScrubWorker)

// WithScrubInterval sets the scan interval. Default: 6h.
func WithScrubInterval(d time.Duration) ScrubOption {
	return func(s *ScrubWorker) { s.interval = d }
}

// WithScrubBatchSize sets how many chunks to verify per batch. Default: 100.
func WithScrubBatchSize(n int) ScrubOption {
	return func(s *ScrubWorker) { s.batchSize = n }
}

// WithScrubBatchDelay sets the sleep between batches. Default: 10ms.
func WithScrubBatchDelay(d time.Duration) ScrubOption {
	return func(s *ScrubWorker) { s.batchDelay = d }
}

// NewScrubWorker creates a scrub worker for the given V2Store.
func NewScrubWorker(store *V2Store, opts ...ScrubOption) *ScrubWorker {
	s := &ScrubWorker{
		store:      store,
		interval:   scrubDefaultInterval,
		batchSize:  scrubDefaultBatchSize,
		batchDelay: scrubDefaultBatchDelay,
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

// SetChangeJournal attaches a change journal. When set, scrub findings
// (corrupt chunks) are appended as EventScrubFinding so the heartbeat
// ships them to the metadata authority.
func (s *ScrubWorker) SetChangeJournal(j *journal.ChangeJournal) {
	s.journal = j
}

// Start begins the background scrub loop. It is idempotent — calling
// Start on an already-running worker is a no-op.
func (s *ScrubWorker) Start(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.running {
		return
	}
	s.running = true
	stopCh := make(chan struct{})
	s.stopCh = stopCh

	s.wg.Add(1)
	go func(stop <-chan struct{}) {
		defer s.wg.Done()
		// First scan starts immediately, then every interval.
		s.doScrub(ctx)
		ticker := time.NewTicker(s.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-stop:
				return
			case <-ticker.C:
				s.doScrub(ctx)
			}
		}
	}(stopCh)
	slog.Info("scrub worker started", "interval", s.interval, "batch_size", s.batchSize)
}

// Stop halts the background scrub loop and waits for it to exit.
func (s *ScrubWorker) Stop() {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return
	}
	s.running = false
	close(s.stopCh)
	s.mu.Unlock()
	s.wg.Wait()
	slog.Info("scrub worker stopped", "scanned", s.scanned.Load(), "corrupt", s.corrupt.Load())
}

// Stats returns live counters.
func (s *ScrubWorker) Stats() ScrubStats {
	return ScrubStats{
		Scanned: s.scanned.Load(),
		Corrupt: s.corrupt.Load(),
		Skipped: s.skipped.Load(),
		Failed:  s.failed.Load(),
	}
}

// doScrub runs one full scan of all local chunks.
func (s *ScrubWorker) doScrub(ctx context.Context) {
	chunks := s.store.ListChunks()
	if len(chunks) == 0 {
		return
	}
	slog.Info("scrub scan starting", "chunks", len(chunks))

	var scanned, corrupt, skipped, failed int64
	for i, ci := range chunks {
		// Rate-limit: pause between batches to avoid I/O storms.
		if i > 0 && i%s.batchSize == 0 {
			time.Sleep(s.batchDelay)
			if ctx.Err() != nil {
				slog.Info("scrub scan interrupted", "scanned", scanned, "remaining", len(chunks)-i)
				break
			}
		}

		// Skip chunks with no stored checksum (e.g. zero-length or legacy).
		if ci.Checksum == 0 {
			skipped++
			continue
		}

		ok, _, err := s.store.VerifyChunkData(ci.ChunkID)
		if err != nil {
			// I/O error (not corruption) — the chunk may have been
			// deleted between ListChunks and now, or the disk is failing.
			failed++
			continue
		}
		scanned++

		if !ok {
			corrupt++
			slog.Warn("scrub: corruption detected",
				"chunk_id", ci.ChunkID, "disk", ci.DiskIndex,
				"expected_checksum", ci.Checksum)
			s.appendScrubFinding(ci.ChunkID)
		}
	}

	// Publish counters atomically.
	s.scanned.Add(scanned)
	s.corrupt.Add(corrupt)
	s.skipped.Add(skipped)
	s.failed.Add(failed)

	if corrupt > 0 {
		slog.Warn("scrub scan complete", "scanned", scanned, "corrupt", corrupt, "skipped", skipped, "failed", failed)
	} else {
		slog.Info("scrub scan complete", "scanned", scanned, "skipped", skipped, "failed", failed)
	}
}

// appendScrubFinding writes an EventScrubFinding to the change journal.
// The heartbeat's Pending() path will ship it to the metadata authority.
func (s *ScrubWorker) appendScrubFinding(chunkID metadata.ChunkID) {
	if s.journal == nil {
		return
	}
	loc := s.store.loc(chunkID)
	if _, err := s.journal.Append(
		journal.EventScrubFinding,
		storage.ExtentID(chunkID),
		loc.gen,
		0, // no segment-level association
		"crc_mismatch",
	); err != nil {
		slog.Error("scrub: failed to append journal event", "chunk_id", chunkID, "error", err)
	}
}
