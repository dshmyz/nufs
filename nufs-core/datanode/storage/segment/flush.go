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

// flush applies all committed mutations to Pebble, syncs, and publishes
// an INDEX_SAFE record for the current safe sequence.
func (s *Store) flush() error {
	muts := s.overlay.Drain()
	if len(muts) == 0 {
		return nil
	}
	// Apply to Pebble (the overlay is now the delta-since-flush).
	if err := s.applyNow(muts); err != nil {
		// Re-insert into the overlay so nothing is lost: the committed
		// records are still the durability authority, and reads consult
		// the overlay first.
		for _, m := range muts {
			s.overlay.Put(indexKeyFor(m), m.Value)
		}
		return err
	}
	// Sync Pebble SSTs so the index entries are durable (§7.4: "After
	// flush and SST sync, an INDEX_SAFE record ... publishes the safe
	// sequence").
	if err := s.index.DB().Flush(); err != nil {
		return err
	}
	// Publish INDEX_SAFE with the safe sequence.
	safe := s.flushSeq.Load()
	if err := s.writeIndexSafe(safe); err != nil {
		return err
	}
	s.safeSeq.Store(safe)
	s.flushMutations.Store(0)
	slog.Debug("storage: index flushed", "safe_seq", safe, "mutations", len(muts))
	return nil
}

// writeIndexSafe appends an INDEX_SAFE record to the commit log and
// syncs it, making the safe sequence durable in the segment.
func (s *Store) writeIndexSafe(safeSeq uint64) error {
	// The INDEX_SAFE record carries the safe sequence in its Seq field
	// (§7.1 OpIndexSafe). It is written into the active segment so
	// recovery reads it during segment-log replay.
	s.mu.Lock()
	defer s.mu.Unlock()
	rec := &journal.CommitRecord{
		Seq: safeSeq,
		Op:  journal.OpIndexSafe,
	}
	// Serialize into the segment tail.
	body := make([]byte, journal.CommitRecordSize)
	if err := rec.Encode(body); err != nil {
		return err
	}
	tail, err := s.alloc.CurrentTail()
	if err != nil {
		return err
	}
	if _, err := s.writer.WriteAt(body, tail); err != nil {
		return err
	}
	if err := s.writer.Sync(); err != nil {
		return err
	}
	s.alloc.Consume(int64(journal.CommitRecordSize))
	return nil
}

// cfgFlushMaxMutations returns the flush mutation guardrail.
func (s *Store) cfgFlushMaxMutations() int64 {
	return flushMaxMutationsDefault
}

// indexKeyFor builds the extent index key for a mutation (for overlay
// re-insertion on a failed flush).
func indexKeyFor(m index.Mutation) []byte {
	return index.Key(m.ExtentID, m.Generation)
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
