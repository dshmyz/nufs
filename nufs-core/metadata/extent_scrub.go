package metadata

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// extent_scrub.go — roadmap §1.4: Scrubber extent 版. The V1 Scrubber
// (production.go) verifies chunk metadata; the ExtentScrubber verifies the
// V2 extent model (ExtentMetaV2.Lifecycle distribution + backing-chunk
// health) and heals the one transition the heartbeat path deliberately
// never performs — returning a ReadyDegraded extent (and its fully recovered
// chunk) to Ready. The chunk upgrade is the scrubber's job per the note next
// to batchUpdateChunkStatesCtx (pebble_store.go): the heartbeat path only
// sees per-replica deltas and only degrades; the scrubber has the whole
// replica set in hand.
//
// Recovery is keyed on replica health, not chunk.State: the repair flow
// (datanode repairByAddingReplica → ReportChunkState(target, ReplicaReady))
// never flips ChunkDegraded back to ChunkReady, so a repaired chunk sits
// with all replicas Ready while State stays Degraded. The scrubber detects
// exactly that state and restores chunk + extent together.

// ExtentScrubResult holds the result of one extent scrub pass.
type ExtentScrubResult struct {
	ExtentsScanned int
	Ready          int // LifecycleReady
	ReadyDegraded  int // LifecycleReadyDegraded
	Other          int // any other Lifecycle value
	Dangling       int // /extent-meta row whose backing chunk row is gone
	Unhealthy      int // backing chunk present but no healthy replica
	Recovered      int // ReadyDegraded extents restored to Ready
	ScanDuration   time.Duration
}

// ExtentScrubber periodically scans V2 extent metadata for lifecycle
// consistency and heals fully-recovered degraded extents. Started from the
// ServiceBundle under the same ScrubInterval gate as the V1 Scrubber.
type ExtentScrubber struct {
	store   *PebbleStore
	stopCh  chan struct{}
	wg      sync.WaitGroup
	running atomic.Bool
}

// NewExtentScrubber creates an extent scrubber.
func NewExtentScrubber(store *PebbleStore) *ExtentScrubber {
	return &ExtentScrubber{
		store:  store,
		stopCh: make(chan struct{}),
	}
}

// extentBackingChunkHealth resolves the chunk backing a V2 extent (extent ID
// == chunk ID invariant) and reports whether the row exists and, if so,
// whether it is healthy. EC chunks always report healthy: their shard health
// lives in the EC stripe registry, not the replica list, and recovery is the
// EC healer's domain.
func (s *PebbleStore) extentBackingChunkHealth(ext *ExtentMetaV2) (exists, healthy bool) {
	chunk, err := s.GetChunk(context.Background(), ChunkID(ext.ID))
	if err != nil {
		return false, false
	}
	if chunk.ECGroup != nil {
		return true, true
	}
	return true, chunkHasHealthyReplica(chunk)
}

// chunkHasHealthyReplica reports whether the chunk has at least one replica
// in ReplicaReady or ReplicaSyncing (the same healthy set the V1 Scrubber
// uses in VerifyChunkChecksum).
func chunkHasHealthyReplica(chunk *ChunkMeta) bool {
	for _, r := range chunk.Replicas {
		if r.State == ReplicaReady || r.State == ReplicaSyncing {
			return true
		}
	}
	return false
}

// Scan performs one full extent scrub pass: count Lifecycle distribution,
// validate each extent's backing chunk, and heal ReadyDegraded extents whose
// chunk has fully recovered.
func (s *ExtentScrubber) Scan(ctx context.Context) (*ExtentScrubResult, error) {
	start := time.Now()
	result := &ExtentScrubResult{}

	err := s.store.ScanExtents(ctx, func(ext *ExtentMetaV2) error {
		result.ExtentsScanned++
		switch ext.Lifecycle {
		case LifecycleReady:
			result.Ready++
		case LifecycleReadyDegraded:
			result.ReadyDegraded++
		default:
			result.Other++
		}

		// Validate the backing chunk. The chunk row is authoritative; a
		// missing one means an orphan /extent-meta row (an interrupted
		// write, or the chunk was purged after file deletion) — a benign
		// observability signal, not data loss.
		chunk, err := s.store.GetChunk(ctx, ChunkID(ext.ID))
		if err != nil {
			if errors.Is(err, ErrChunkNotFound) {
				result.Dangling++
				return nil
			}
			return err
		}

		// EC chunks: shard health is the EC healer's domain; count
		// Lifecycle only, never flag or recover.
		if chunk.ECGroup != nil {
			return nil
		}

		if !chunkHasHealthyReplica(chunk) {
			result.Unhealthy++
			return nil
		}

		// Recovery: a ReadyDegraded extent whose backing chunk has fully
		// recovered (every replica Ready) is restored to Ready. Chunk and
		// extent are restored together so the two models cannot diverge;
		// the transition is idempotent and converges even if the process
		// dies between the two writes (the next pass re-syncs).
		if ext.Lifecycle != LifecycleReadyDegraded {
			return nil
		}
		allReady := len(chunk.Replicas) > 0
		for _, r := range chunk.Replicas {
			if r.State != ReplicaReady {
				allReady = false
				break
			}
		}
		if !allReady {
			return nil
		}
		if chunk.State != ChunkReady {
			chunk.State = ChunkReady
			if err := s.store.UpdateChunk(ctx, chunk); err != nil {
				return err
			}
		}
		ext.Lifecycle = LifecycleReady
		if err := s.store.putExtentMeta(ext); err != nil {
			return err
		}
		result.Recovered++
		result.Ready++
		result.ReadyDegraded--
		return nil
	})

	result.ScanDuration = time.Since(start)
	return result, err
}

// Start runs the extent scrubber periodically, mirroring Scrubber.
func (s *ExtentScrubber) Start(interval time.Duration) {
	if s.running.Swap(true) {
		return
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				result, err := s.Scan(context.Background())
				if err != nil {
					slog.Error("extent scrub: error", "error", err)
				} else {
					slog.Info("extent scrub: completed",
						"scanned", result.ExtentsScanned,
						"ready", result.Ready,
						"degraded", result.ReadyDegraded,
						"other", result.Other,
						"dangling", result.Dangling,
						"unhealthy", result.Unhealthy,
						"recovered", result.Recovered,
						"duration", result.ScanDuration)
				}
			case <-s.stopCh:
				return
			}
		}
	}()
}

// Stop terminates the extent scrubber and waits for the goroutine to exit.
func (s *ExtentScrubber) Stop() {
	if s.running.Swap(false) {
		close(s.stopCh)
	}
	s.wg.Wait()
}
