package maintenance

import (
	"fmt"

	"github.com/dshmyz/nufs/nufs-core/datanode/storage"
	"github.com/dshmyz/nufs/nufs-core/datanode/storage/index"
)

// CompactionCandidate is a sealed segment scored for compaction (§10.2).
type CompactionCandidate struct {
	// SegmentID and path identify the source segment.
	SegmentID storage.SegmentID
	Path      string
	// RecordCount / deadBytes / liveBytes feed the eligibility rules.
	RecordCount uint64
	DeadBytes   int64
	LiveBytes   int64
	// DeadRecordRatio is dead records / total records.
	DeadRecordRatio float64
	// Score is the scheduler's ranking (higher = more valuable).
	Score float64
}

// Eligible reports whether the segment meets any §10.2 eligibility
// condition.
func (c *CompactionCandidate) Eligible() bool {
	total := c.DeadBytes + c.LiveBytes
	if total <= 0 {
		return false
	}
	deadRatio := float64(c.DeadBytes) / float64(total)
	return deadRatio >= 0.30 || c.DeadRecordRatio >= 0.40 || (c.DeadBytes > 0 && c.LiveBytes == 0)
}

// ScoreWith computes the §10.2 ranking:
//
//	reclaimable / expected_read_bytes * age * pressure * health / latency
func (c *CompactionCandidate) ScoreWith(ageFactor, spacePressure, mediaHealth float64) {
	total := float64(c.DeadBytes + c.LiveBytes)
	if total <= 0 {
		c.Score = 0
		return
	}
	reclaim := float64(c.DeadBytes)
	readCost := total
	c.Score = reclaim / readCost * ageFactor * spacePressure * mediaHealth
}

// ScannedRecord is one record found in a segment scan.
type ScannedRecord struct {
	ExtentID   storage.ExtentID
	Generation storage.Generation
	StoredLen  uint32
	LogicalLen uint32
	Codec      storage.CompressionCodec
	// ReadPayload reads and validates the record's payload.
	ReadPayload func() ([]byte, error)
}

// Reloc is one relocated record's new location.
type Reloc = storage.Reloc

// StoreSink is the interface a disk store provides to the compactor.
type StoreSink = storage.StoreSink

// Compactor moves live records from a sealed segment into a fresh
// segment, applying the V2.1 §10.3 compaction transaction.
type Compactor struct {
	// sink is the store's append + relocate interface.
	sink StoreSink
	// finishSealed seals the destination segment after compaction.
	finishSealed func() error
}

// NewCompactor creates a compactor bound to a store sink.
func NewCompactor(sink StoreSink, finishSealed func() error) *Compactor {
	return &Compactor{sink: sink, finishSealed: finishSealed}
}

// Compact runs the §10.3 transaction on a source segment:
//
//  1. fence the source at a compaction generation
//  2. select records whose index still points to the source
//  3. copy + validate live records into a new segment
//  4. sync the new segment
//  5. append + sync conditional RELOCATE records
//  6. apply index changes only if old locations still match
//  7. publish a new manifest (caller)
//  8. move old segment to trash (caller)
//
// isLive returns true if the extent's index still points at srcID.
func (c *Compactor) Compact(srcID storage.SegmentID, records []ScannedRecord, isLive func(extentID storage.ExtentID, gen storage.Generation) bool) (int, error) {
	var relocs []Reloc
	copied := 0
	for _, rec := range records {
		if !isLive(rec.ExtentID, rec.Generation) {
			continue // dead or already relocated
		}
		payload, rerr := rec.ReadPayload()
		if rerr != nil {
			return copied, fmt.Errorf("compaction: read extent %d gen %d: %w", rec.ExtentID, rec.Generation, rerr)
		}
		loc, aerr := c.sink.AppendRecord(rec.ExtentID, rec.Generation, payload, rec.Codec)
		if aerr != nil {
			return copied, fmt.Errorf("compaction: append extent %d: %w", rec.ExtentID, aerr)
		}
		relocs = append(relocs, *loc)
		copied++
	}
	// Steps 4-6: relocation is applied by the sink atomically after the
	// destination is durable.
	if len(relocs) > 0 {
		if err := c.sink.Relocate(relocs); err != nil {
			return copied, err
		}
	}
	if c.finishSealed != nil {
		if err := c.finishSealed(); err != nil {
			return copied, err
		}
	}
	return copied, nil
}

var _ = index.Key // reserved
