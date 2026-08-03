package segment

import (
	"log/slog"
	"time"

	"github.com/example/dfs/datanode/storage/index"
	"github.com/example/dfs/datanode/storage/journal"
)

// Flush limits (§7.4). The index (Pebble) may lag the committed
// sequence but never lead it. A flush persists the overlay to Pebble,
// syncs the SSTs, and publishes an INDEX_SAFE record carrying the safe
// sequence, so recovery replays only committed records after it.
const (
	// flushMaxMutationsDefault is the §7.4 initial guardrail of 100000
	// committed mutations.
	flushMaxMutationsDefault = 100000
	// flushMaxIntervalDefault is the §7.4 two-second foreground trigger.
	flushMaxIntervalDefault = 2 * time.Second
)

// flushLoop periodically applies committed mutations to Pebble and
// publishes INDEX_SAFE (§7.4). It fires when the committed-mutation
// count reaches the guardrail, or when the interval elapses with
// pending mutations. The recovery-cost estimator (replay time) is the
// third trigger; we approximate it by the interval+count guards per the
// §7.4 initial hard caps.
func (s *Store) flushLoop() {
	defer s.flushWG.Done()
	maxMutations := s.cfgFlushMaxMutations()
	interval := s.cfgFlushInterval()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-s.stopCh:
			s.flush()
			return
		case <-ticker.C:
			// Interval trigger: flush if there are pending mutations.
			if s.flushMutations.Load() > 0 {
				if err := s.flush(); err != nil {
					slog.Error("storage: flush failed", "error", err)
				}
			}
		}
		// Mutation-count trigger: if the committed-but-unflushed count
		// reached the guardrail, flush eagerly (§7.4 hard cap).
		if s.flushMutations.Load() >= maxMutations {
			if err := s.flush(); err != nil {
				slog.Error("storage: flush failed (mutations)", "error", err)
			}
		}
	}
}

// flush applies all committed mutations to Pebble, syncs, and publishes an
// INDEX_SAFE record for the current safe sequence. It holds s.mu from overlay
// drain through sidecar publication and counter reconciliation. commitBatch
// holds that same lock through record sync and overlay publication, so no
// durable batch can land in the marker's physical prefix without also being
// present in the index snapshot it certifies.
func (s *Store) flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Snapshot the overlay-published watermark and pending count while commits
	// are excluded. A successful checkpoint subtracts this exact count instead
	// of resetting it, so post-checkpoint publications cannot be erased.
	safe := s.flushSeq.Load()
	pending := s.flushMutations.Load()
	muts := s.overlay.Drain()
	if len(muts) == 0 {
		return nil
	}
	reinsert := func() {
		// Every error after Drain leaves the durable log as recovery authority.
		// Restore the overlay as well so the next flush retries the same
		// checkpoint instead of dropping its pending count or publishing a
		// later sidecar without these mutations.
		s.overlay.RestoreDrained()
	}
	if s.flushCheckpointHook != nil {
		s.flushCheckpointHook()
	}
	// Apply to Pebble (the overlay is now the delta-since-flush).
	apply := s.applyNow
	if s.flushApply != nil {
		apply = s.flushApply
	}
	if err := apply(muts); err != nil {
		reinsert()
		return err
	}
	// Sync Pebble SSTs so the index entries are durable (§7.4: "After
	// flush and SST sync, an INDEX_SAFE record ... publishes the safe
	// sequence").
	if err := s.index.DB().Flush(); err != nil {
		reinsert()
		return err
	}
	// Publish INDEX_SAFE with the safe sequence, then atomically persist the
	// matching marker-end offset in the index-owned recovery checkpoint. The
	// ordering is intentional: the sidecar is never durable unless both the
	// index and marker it certifies are already durable.
	checkpoint, err := s.writeIndexSafeLocked(safe)
	if err != nil {
		reinsert()
		return err
	}
	if err := index.StoreRecoveryCheckpoint(s.index, checkpoint); err != nil {
		reinsert()
		return err
	}
	// The checkpoint is durable; discard the staged entries so the overlay
	// stays bounded. Keys committed after Drain are already in entries and
	// are unaffected by this discard.
	s.overlay.DiscardDrained()
	s.safeSeq.Store(safe)
	s.flushMutations.Add(-pending)
	slog.Debug("storage: index flushed", "safe_seq", safe, "mutations", len(muts))
	return nil
}

// writeIndexSafeLocked appends an INDEX_SAFE record to the commit log and
// syncs it, making the safe sequence durable in the segment. The caller must
// hold s.mu for the entire checkpoint transaction.
func (s *Store) writeIndexSafeLocked(safeSeq uint64) (index.RecoveryCheckpoint, error) {
	// The INDEX_SAFE record carries the safe sequence in its Seq field
	// (§7.1 OpIndexSafe). It is written into the active segment so
	// recovery reads it during segment-log replay.
	rec := &journal.CommitRecord{
		Seq: safeSeq,
		Op:  journal.OpIndexSafe,
	}
	// Serialize into the segment tail.
	body := make([]byte, journal.CommitRecordSize)
	if err := rec.Encode(body); err != nil {
		return index.RecoveryCheckpoint{}, err
	}
	tail, err := s.alloc.CurrentTail()
	if err != nil {
		return index.RecoveryCheckpoint{}, err
	}
	if _, err := s.writer.WriteAt(body, tail); err != nil {
		return index.RecoveryCheckpoint{}, err
	}
	if err := s.writer.Sync(); err != nil {
		return index.RecoveryCheckpoint{}, err
	}
	s.alloc.Consume(int64(journal.CommitRecordSize))
	return index.RecoveryCheckpoint{
		StreamID:   s.streamID,
		SegmentID:  s.alloc.State().SegmentID,
		SafeOffset: tail + int64(journal.CommitRecordSize),
		SafeSeq:    safeSeq,
	}, nil
}

// cfgFlushMaxMutations returns the flush mutation guardrail.
func (s *Store) cfgFlushMaxMutations() int64 {
	return flushMaxMutationsDefault
}

// cfgFlushInterval returns the flush interval.
func (s *Store) cfgFlushInterval() time.Duration {
	if s.flushInterval <= 0 {
		return flushMaxIntervalDefault
	}
	return s.flushInterval
}

// SafeSeq returns the last sequence covered by an INDEX_SAFE record.
func (s *Store) SafeSeq() uint64 {
	return s.safeSeq.Load()
}
