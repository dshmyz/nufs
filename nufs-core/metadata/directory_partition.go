package metadata

import (
	"encoding/binary"
	"fmt"
	"sort"
)

// Directory partitioning (V2.1 §11.5): normal directory entries remain
// colocated with their directory inode. A directory becomes
// range-partitioned when it exceeds the proactive thresholds, and must
// complete before the hard thresholds. A versioned directory map assigns
// ordered name ranges to metadata shards; lookups target one range,
// enumeration walks ranges in lexical order.
//
// Thresholds (§16 / §11.5):
//
//	proactive: 500000 entries / 128 MiB values / 2500 wps / 60% CPU
//	hard:      1000000 entries / 256 MiB values / 5000 wps / 70% CPU

const (
	DirPartitionProactiveEntries = 500000
	DirPartitionHardEntries      = 1000000
	DirPartitionProactiveBytes   = 128 << 20 // 128 MiB
	DirPartitionHardBytes        = 256 << 20 // 256 MiB
	DirPartitionProactiveWPS     = 2500
	DirPartitionHardWPS          = 5000
	DirPartitionProactiveCPU     = 60.0
	DirPartitionHardCPU          = 70.0
)

// DirectoryRange is one ordered name range assigned to a shard.
type DirectoryRange struct {
	// Start/End are the lexicographic name bounds ("" = unbounded).
	Start string `json:"start"`
	End   string `json:"end"`
	// ShardID is the metadata shard owning this range.
	ShardID uint32 `json:"shard_id"`
}

// DirectoryMap is the versioned partition map for a directory (§11.5).
type DirectoryMap struct {
	// DirInode is the directory being partitioned.
	DirInode InodeID `json:"dir_inode"`
	// Version increments on every split; stale versions trigger a
	// routing refresh rather than speculative writes.
	Version uint64 `json:"version"`
	// Ranges are sorted by Start.
	Ranges []DirectoryRange `json:"ranges"`
	// Partitioned is true once the directory uses ranged lookup.
	Partitioned bool `json:"partitioned"`
}

// DirectoryPartitionStore manages the partition maps.
type DirectoryPartitionStore struct {
	store *PebbleStore
}

// NewDirectoryPartitionStore creates the directory partition store.
func NewDirectoryPartitionStore(store *PebbleStore) *DirectoryPartitionStore {
	return &DirectoryPartitionStore{store: store}
}

// dmKey formats a directory map key.
func dmKey(dir InodeID) string {
	return fmt.Sprintf("%s%d", prefixDirectoryMap, dir)
}

// GetMap reads the partition map for a directory (nil if not
// partitioned).
func (s *DirectoryPartitionStore) GetMap(dir InodeID) (*DirectoryMap, error) {
	var dm DirectoryMap
	exists, err := s.store.getValue(dmKey(dir), &dm)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, nil
	}
	return &dm, nil
}

// PutMap writes a directory map.
func (s *DirectoryPartitionStore) PutMap(dm *DirectoryMap) error {
	return s.store.putMsgpack(dmKey(dm.DirInode), dm)
}

// ShouldPartition evaluates the §11.5 thresholds. Returns proactive and
// hard flags.
func ShouldPartition(entryCount int64, bytes int64, writesPerSec int64, cpuPct float64) (proactive, hard bool) {
	proactive = entryCount >= DirPartitionProactiveEntries ||
		bytes >= DirPartitionProactiveBytes ||
		writesPerSec >= DirPartitionProactiveWPS ||
		cpuPct >= DirPartitionProactiveCPU
	hard = entryCount >= DirPartitionHardEntries ||
		bytes >= DirPartitionHardBytes ||
		writesPerSec >= DirPartitionHardWPS ||
		cpuPct >= DirPartitionHardCPU
	return
}

// SplitRange splits one range into two at a split point, producing a new
// DirectoryMap version. Monotonic-name workloads use adaptive split
// points (§11.5); here a mid-range split is the default.
func (s *DirectoryPartitionStore) SplitRange(dir InodeID, rangeIdx int, splitPoint string, shardIDs []uint32) (*DirectoryMap, error) {
	dm, err := s.GetMap(dir)
	if err != nil {
		return nil, err
	}
	if dm == nil {
		// Initial partition: the whole namespace is one range.
		dm = &DirectoryMap{DirInode: dir, Version: 1, Ranges: []DirectoryRange{
			{Start: "", End: "", ShardID: shardIDs[0]},
		}}
	}
	if rangeIdx < 0 || rangeIdx >= len(dm.Ranges) {
		return nil, fmt.Errorf("metadata: range %d out of bounds", rangeIdx)
	}
	old := dm.Ranges[rangeIdx]
	// New map version: split [Start, End) at splitPoint.
	left := DirectoryRange{Start: old.Start, End: splitPoint, ShardID: old.ShardID}
	right := DirectoryRange{Start: splitPoint, End: old.End, ShardID: shardIDs[1%len(shardIDs)]}
	newRanges := make([]DirectoryRange, 0, len(dm.Ranges)+1)
	newRanges = append(newRanges, dm.Ranges[:rangeIdx]...)
	newRanges = append(newRanges, left, right)
	newRanges = append(newRanges, dm.Ranges[rangeIdx+1:]...)
	sort.Slice(newRanges, func(i, j int) bool { return newRanges[i].Start < newRanges[j].Start })
	dm.Ranges = newRanges
	dm.Version++
	dm.Partitioned = true
	if err := s.PutMap(dm); err != nil {
		return nil, err
	}
	return dm, nil
}

// Lookup returns the shard owning the name and the map version (for
// stale-version refresh checks). If the directory is not partitioned, it
// returns the directory's home shard (0 = colocated).
func (s *DirectoryPartitionStore) Lookup(dir InodeID, name string) (shardID uint32, version uint64, partitioned bool, err error) {
	dm, err := s.GetMap(dir)
	if err != nil {
		return 0, 0, false, err
	}
	if dm == nil || !dm.Partitioned {
		return 0, 0, false, nil
	}
	for _, r := range dm.Ranges {
		if (r.Start == "" || name >= r.Start) && (r.End == "" || name < r.End) {
			return r.ShardID, dm.Version, true, nil
		}
	}
	return 0, dm.Version, true, fmt.Errorf("metadata: no range for name %q", name)
}

// EnumRanges returns the ranges in lexical order for enumeration.
func (s *DirectoryPartitionStore) EnumRanges(dir InodeID) ([]DirectoryRange, error) {
	dm, err := s.GetMap(dir)
	if err != nil {
		return nil, err
	}
	if dm == nil {
		return nil, nil
	}
	return dm.Ranges, nil
}

var _ = binary.BigEndian
