package maintenance

import (
	"sort"

	"github.com/example/dfs/datanode/storage"
)

// Scheduler selects compaction candidates and enforces the §10.2
// resource budget: steady-state write amplification ≤ 2.5, background
// compaction bandwidth ≤ 10% of disk.
type Scheduler struct {
	// MaxWriteAmp caps steady-state write amplification (§16).
	MaxWriteAmp float64
	// MaxBandwidthPct is the compaction disk bandwidth share (§13.1).
	MaxBandwidthPct float64
	// queued tracks in-flight compaction candidates.
	queued []storage.SegmentID
}

// NewScheduler creates a scheduler with §16 defaults.
func NewScheduler() *Scheduler {
	return &Scheduler{
		MaxWriteAmp:     2.5,
		MaxBandwidthPct: 10,
	}
}

// Select returns the highest-value candidates within the budget,
// applying the §10.2 scoring formula. Candidates already queued are
// excluded. Returns at most maxCandidates.
func (s *Scheduler) Select(candidates []CompactionCandidate, maxCandidates int, ageFactor, spacePressure, mediaHealth float64) []CompactionCandidate {
	sort.Slice(candidates, func(i, j int) bool {
		candidates[i].ScoreWith(ageFactor, spacePressure, mediaHealth)
		candidates[j].ScoreWith(ageFactor, spacePressure, mediaHealth)
		return candidates[i].Score > candidates[j].Score
	})
	var out []CompactionCandidate
	queuedSet := make(map[storage.SegmentID]struct{}, len(s.queued))
	for _, q := range s.queued {
		queuedSet[q] = struct{}{}
	}
	for _, c := range candidates {
		if len(out) >= maxCandidates {
			break
		}
		if !c.Eligible() {
			continue
		}
		if _, ok := queuedSet[c.SegmentID]; ok {
			continue
		}
		out = append(out, c)
	}
	return out
}

// Track marks a candidate as queued (in-flight).
func (s *Scheduler) Track(segID storage.SegmentID) {
	s.queued = append(s.queued, segID)
}

// Release removes a candidate from the in-flight set.
func (s *Scheduler) Release(segID storage.SegmentID) {
	for i, q := range s.queued {
		if q == segID {
			s.queued = append(s.queued[:i], s.queued[i+1:]...)
			return
		}
	}
}

// AllowAdmission returns true if a compaction of the given live bytes
// stays within the write-amplification budget given bytes written so
// far. This is a heuristic gate; the full budget is enforced by the
// bandwidth limiter.
func (s *Scheduler) AllowAdmission(liveBytes, totalWritten int64) bool {
	if totalWritten <= 0 {
		return true
	}
	amp := float64(totalWritten) / float64(maxInt64(liveBytes, 1))
	return amp <= s.MaxWriteAmp
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
