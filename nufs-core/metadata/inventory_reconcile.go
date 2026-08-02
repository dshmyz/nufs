package metadata

import (
	"encoding/binary"
	"fmt"
)

// InventoryPartition holds fixed-size commutative summaries for one of
// the 65536 inventory partitions (V2.1 §12). The foreground write path
// updates only these summaries — never a full Merkle path — so the
// update cost is O(1) per write.
//
// The summaries are commutative (order-independent) so two nodes can
// compare partitions cheaply: identical sets produce identical
// summaries; a mismatch means the partition differs and triggers a
// Merkle-based narrowing.
type InventoryPartition struct {
	// Partition is the partition index (extent-ID hash).
	Partition uint32 `json:"partition"`
	// Count is the number of live extents.
	Count uint64 `json:"count"`
	// LiveBytes is the total live logical bytes.
	LiveBytes uint64 `json:"live_bytes"`
	// XORHash is the bitwise XOR of extent ID + generation hashes.
	XORHash uint64 `json:"xor_hash"`
	// SumHash is the modular sum of extent hashes.
	SumHash uint64 `json:"sum_hash"`
	// MaxGeneration is the maximum generation seen.
	MaxGeneration uint64 `json:"max_generation"`
}

// hashExtent derives a fixed hash from an extent ID and generation.
func hashExtent(id uint64, gen uint64) uint64 {
	// FNV-1a over the concatenated 16 bytes.
	h := uint64(14695981039346656037)
	for _, b := range []byte{
		byte(id >> 56), byte(id >> 48), byte(id >> 40), byte(id >> 32),
		byte(id >> 24), byte(id >> 16), byte(id >> 8), byte(id),
		byte(gen >> 56), byte(gen >> 48), byte(gen >> 40), byte(gen >> 32),
		byte(gen >> 24), byte(gen >> 16), byte(gen >> 8), byte(gen),
	} {
		h ^= uint64(b)
		h *= 1099511628211
	}
	return h
}

// Add records a live extent into the partition (commutative update).
func (p *InventoryPartition) Add(id uint64, gen uint64, bytes int64) {
	h := hashExtent(id, gen)
	p.Count++
	p.LiveBytes += uint64(bytes)
	p.XORHash ^= h
	p.SumHash += h
	if gen > p.MaxGeneration {
		p.MaxGeneration = gen
	}
}

// Remove records a deletion (commutative inverse; Count/LiveBytes may
// need reconciliation if the partition was never observed before).
func (p *InventoryPartition) Remove(id uint64, gen uint64, bytes int64) {
	h := hashExtent(id, gen)
	if p.Count > 0 {
		p.Count--
	}
	if p.LiveBytes >= uint64(bytes) {
		p.LiveBytes -= uint64(bytes)
	}
	p.XORHash ^= h
	p.SumHash -= h
}

// Equal compares two partition summaries for reconciliation.
func (p *InventoryPartition) Equal(o *InventoryPartition) bool {
	return p.Count == o.Count &&
		p.LiveBytes == o.LiveBytes &&
		p.XORHash == o.XORHash &&
		p.SumHash == o.SumHash &&
		p.MaxGeneration == o.MaxGeneration
}

// PartitionSummaryKey formats the on-disk key for a partition summary.
func PartitionSummaryKey(partition uint32) string {
	return fmt.Sprintf("inv/summary/%08x", partition)
}

// InventoryStore persists the partition summaries.
type InventoryStore struct {
	store *PebbleStore
}

// NewInventoryStore creates the inventory store.
func NewInventoryStore(store *PebbleStore) *InventoryStore {
	return &InventoryStore{store: store}
}

// NumInventoryPartitions is the fixed partition count (§12).
const NumInventoryPartitions = 65536

// PartitionFor returns the partition index for an extent (extent-ID
// hash mod 65536, §12).
func PartitionFor(extentID uint64) uint32 {
	return uint32(hashExtent(extentID, 0) % NumInventoryPartitions)
}

// Get reads a partition summary (zero-value if absent).
func (s *InventoryStore) Get(partition uint32) (*InventoryPartition, error) {
	var p InventoryPartition
	exists, err := s.store.getValue(PartitionSummaryKey(partition), &p)
	if err != nil {
		return nil, err
	}
	if !exists {
		return &InventoryPartition{Partition: partition}, nil
	}
	return &p, nil
}

// Put writes a partition summary.
func (s *InventoryStore) Put(p *InventoryPartition) error {
	return s.store.putMsgpack(PartitionSummaryKey(p.Partition), p)
}

// RecordAdd updates the summary for a committed extent (called on the
// write path; O(1), no Merkle update — §12).
func (s *InventoryStore) RecordAdd(extentID uint64, gen uint64, bytes int64) error {
	part := PartitionFor(extentID)
	p, err := s.Get(part)
	if err != nil {
		return err
	}
	p.Add(extentID, gen, bytes)
	return s.Put(p)
}

// RecordRemove updates the summary for a deletion.
func (s *InventoryStore) RecordRemove(extentID uint64, gen uint64, bytes int64) error {
	part := PartitionFor(extentID)
	p, err := s.Get(part)
	if err != nil {
		return err
	}
	p.Remove(extentID, gen, bytes)
	return s.Put(p)
}

// GlobalDigest is the fixed-size cluster-wide inventory digest
// (commutative sum of all partitions). Compared every 6 hours (§22).
type GlobalDigest struct {
	Count     uint64 `json:"count"`
	LiveBytes uint64 `json:"live_bytes"`
	XORHash   uint64 `json:"xor_hash"`
	SumHash   uint64 `json:"sum_hash"`
}

// Global returns the cluster-wide digest (computed by folding all
// partition summaries; the store scans them in pages).
func (s *InventoryStore) Global() (*GlobalDigest, error) {
	d := &GlobalDigest{}
	// Scan partitions in pages to avoid an unbounded in-memory list
	// (§3: no unbounded map).
	const pageSize = 4096
	for start := uint32(0); start < NumInventoryPartitions; start += pageSize {
		end := start + pageSize
		if end > NumInventoryPartitions {
			end = NumInventoryPartitions
		}
		for p := start; p < end; p++ {
			part, err := s.Get(p)
			if err != nil {
				return nil, err
			}
			if part == nil {
				continue
			}
			d.Count += part.Count
			d.LiveBytes += part.LiveBytes
			d.XORHash ^= part.XORHash
			d.SumHash += part.SumHash
		}
	}
	return d, nil
}

var _ = binary.BigEndian
