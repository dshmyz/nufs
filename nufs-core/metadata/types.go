// Package metadata provides core data structures and interfaces for the distributed storage metadata layer.
package metadata

import (
	"fmt"
	"time"
)

// ========== Basic Types ==========

// FileType represents the type of a filesystem entry.
type FileType uint8

const (
	FileRegular   FileType = iota // Regular file
	FileDirectory                 // Directory
	FileSymlink                   // Symbolic link
)

// InodeID is a unique identifier for an inode in the namespace tree.
type InodeID uint64

// ChunkID is a unique identifier for a data chunk (Snowflake-style 64-bit).
type ChunkID uint64

// NodeID is a unique identifier for a data node in the cluster.
type NodeID uint64

// ========== Namespace & Inode ==========

// InodeMeta represents metadata for a file or directory.
// Stored at key: /inode/{inode_id}
type InodeMeta struct {
	ID         InodeID  `json:"id"`
	Type       FileType `json:"type"`
	Size       int64    `json:"size"`  // Total file size in bytes
	NLink      uint32   `json:"nlink"` // Hard link count
	BucketRoot InodeID  `json:"bucket_root,omitempty"` // Root inode of containing bucket
	UID        uint32   `json:"uid"`
	GID        uint32   `json:"gid"`
	Mode       uint32   `json:"mode"`  // POSIX permission bits
	CTime      int64    `json:"ctime"` // Change time (unix nanoseconds)
	MTime      int64    `json:"mtime"` // Modification time
	ATime      int64    `json:"atime"` // Access time

	// File-specific fields
	ChunkMap []ChunkRef `json:"chunks,omitempty"`  // Ordered chunk list
	Symlink  string     `json:"symlink,omitempty"` // Symlink target path

	// Extended attributes
	XAttrs map[string][]byte `json:"xattrs,omitempty"`
}

// DirEntry represents a directory entry (child pointer).
// Stored at key: /ns/{parent_inode}/{name}
type DirEntry struct {
	InodeID InodeID  `json:"inode"`
	Type    FileType `json:"type"`
	Name    string   `json:"name"`
}

// BucketInfo represents S3 bucket metadata.
type BucketInfo struct {
	Name         string          `json:"name"`
	RootInode    InodeID         `json:"root_inode"`
	Policy       PlacementPolicy `json:"policy"`
	CreationDate time.Time       `json:"creation_date"`
}

// ========== Chunk Layer ==========

// ChunkRef is a reference to a chunk within a file.
type ChunkRef struct {
	ID      ChunkID `json:"id"`
	Offset  int64   `json:"offset"`  // Byte offset within the file
	Length  int32   `json:"length"`  // Actual data length in this chunk
	Version int64   `json:"version"` // MVCC version for read-your-writes
}

// ChunkMeta represents metadata for a data chunk.
// Stored at key: /chunk/{chunk_id}
type ChunkMeta struct {
	ID         ChunkID       `json:"id"`
	Size       int32         `json:"size"` // Max 64MB per chunk
	State      ChunkState    `json:"state"`
	Replicas   []ReplicaInfo `json:"replicas"` // Ordered: [primary, secondary, ...]
	ECGroup    *ECGroupInfo  `json:"ec_group,omitempty"`
	Tier       StorageTier   `json:"tier"`       // Target storage tier for this chunk
	CreateTime int64         `json:"create_time"`
	Checksum   uint32        `json:"checksum"` // CRC32C of chunk data
}

// ChunkState represents the lifecycle state of a chunk.
type ChunkState uint8

const (
	ChunkCreated ChunkState = iota // Allocated, not yet committed
	ChunkSealed                    // Commit received, replicating
	ChunkReady                     // All replicas confirmed
	ChunkDegraded                   // Replica lost, repairing
	ChunkOrphan                     // No inode references (GC candidate)
)

// ReplicaInfo describes a replica location.
type ReplicaInfo struct {
	NodeID   NodeID       `json:"node_id"`
	Addr       string       `json:"addr"` // Data node address (host:port)
	State      ReplicaState `json:"state"`
	DiskPath   string       `json:"disk_path"`   // Local storage path on data node
	ShardIndex int          `json:"shard_index"` // EC shard index (0=first data shard, K=first parity)
}

// ReplicaState represents the sync state of a replica.
type ReplicaState uint8

const (
	ReplicaSyncing ReplicaState = iota
	ReplicaReady
	ReplicaStale
	ReplicaFailed
)

// ECGroupInfo describes erasure coding group membership.
type ECGroupInfo struct {
	GroupID      string `json:"group_id"`
	DataShards   int    `json:"data_shards"`
	ParityShards int    `json:"parity_shards"`
	ShardIndex   int    `json:"shard_index"` // This chunk's shard index
}

// ========== Node & Cluster ==========

// NodeInfo represents a data node in the cluster.
// Registered with lease (auto-expire on heartbeat loss).
// Stored at key: /node/{node_id}
type NodeInfo struct {
	ID         NodeID      `json:"id"`
	Addr       string      `json:"addr"`
	DataDir    string      `json:"data_dir"`
	Rack       string      `json:"rack"`
	Zone       string      `json:"zone"`
	MachineID  string      `json:"machine_id"`
	Tier       StorageTier `json:"tier"`
	CapacityGB int64       `json:"capacity_gb"`
	UsedGB     int64       `json:"used_gb"`
	ChunkCount int64       `json:"chunk_count"`
	State      NodeState   `json:"state"`
	LastSeen   int64       `json:"last_seen"`
}

// NodeState represents the operational state of a node.
type NodeState uint8

const (
	NodeOnline    NodeState = iota
	NodeDraining             // Being decommissioned
	NodeMaint                // Under maintenance (rolling upgrade)
	NodeOffline
	NodeFailed
)

func (s NodeState) String() string {
	switch s {
	case NodeOnline:
		return "online"
	case NodeDraining:
		return "draining"
	case NodeMaint:
		return "maintenance"
	case NodeOffline:
		return "offline"
	case NodeFailed:
		return "failed"
	default:
		return fmt.Sprintf("unknown(%d)", s)
	}
}

// NodeReport is sent by data nodes during heartbeat.
type NodeReport struct {
	UsedGB      int64                    `json:"used_gb"`
	ChunkCount  int64                    `json:"chunk_count"`
	DiskIO      float64                  `json:"disk_io"` // 0.0 - 1.0 utilization
	ChunkStates map[ChunkID]ReplicaState `json:"chunk_states"`
}

// RepairTask represents a pending repair operation.
type RepairTask struct {
	ChunkID   ChunkID   `json:"chunk_id"`
	Reason    string    `json:"reason"`
	Priority  int       `json:"priority"`
	CreatedAt time.Time `json:"created_at"`
}

// ========== Placement Policy ==========

// PlacementPolicy defines data placement rules for a bucket.
// Stored at key: /policy/{bucket_name}
type PlacementPolicy struct {
	ID                string         `json:"id"`
	ReplicationFactor int            `json:"replication_factor"` // e.g., 3
	ECConfig          *ECConfig      `json:"ec_config,omitempty"`
	TopologySpread    TopologySpread `json:"topology_spread"`
	StorageTier       StorageTier    `json:"storage_tier"`
	// WriteQuorum controls how many replicas must acknowledge a write
	// before returning success. 0 = auto (safe default).
	// 1 = async (fire-and-forget, risk of data loss on primary failure).
	// For 3-replication, default is 2 (majority).
	// For EC(K,M), default is K (all data shards must land).
	WriteQuorum int `json:"write_quorum,omitempty"`
	// CrossZoneReplication enables async replication to a remote zone.
	CrossZoneReplication *CrossZoneConfig `json:"cross_zone_replication,omitempty"`
}

// CrossZoneConfig defines cross-zone (cross-DC) replication settings.
type CrossZoneConfig struct {
	RemoteZone    string `json:"remote_zone"`     // Target zone name
	ReplicaFactor int    `json:"replica_factor"`  // Number of remote replicas (default: 1)
	AsyncMode     bool   `json:"async_mode"`      // true=async, false=sync
	BandwidthMBps int    `json:"bandwidth_mbps"`  // Bandwidth limit in MB/s (0=unlimited)
}

// ECConfig defines erasure coding parameters.
type ECConfig struct {
	DataShards   int `json:"data_shards"`   // e.g., 4
	ParityShards int `json:"parity_shards"` // e.g., 2
}

// TotalShards returns the total number of shards (data + parity).
func (ec ECConfig) TotalShards() int {
	return ec.DataShards + ec.ParityShards
}

// StorageOverhead returns the space amplification factor.
// EC(4+2) = 6/4 = 1.5x overhead vs 3x for 3-replication.
func (ec ECConfig) StorageOverhead() float64 {
	if ec.DataShards == 0 {
		return 0
	}
	return float64(ec.DataShards+ec.ParityShards) / float64(ec.DataShards)
}

// MaxFailures returns the maximum number of shard failures that
// can be tolerated without data loss (equals ParityShards).
func (ec ECConfig) MaxFailures() int {
	return ec.ParityShards
}

// TopologySpread defines the fault domain isolation level.
type TopologySpread uint8

const (
	SpreadNode    TopologySpread = iota // Replicas on different nodes (disks)
	SpreadMachine                       // Replicas on different machines
	SpreadRack                          // Replicas on different racks
	SpreadZone                          // Replicas on different AZs/DCs
)

// EffectiveWriteQuorum returns the minimum number of replica acknowledgements
// required before a write is considered successful.
// Default: min(ReplicationFactor, 2) for replication, DataShards for EC.
func (p PlacementPolicy) EffectiveWriteQuorum() int {
	if p.WriteQuorum > 0 {
		return p.WriteQuorum
	}
	// EC: all data shards must be written
	if p.ECConfig != nil && p.ECConfig.DataShards > 0 {
		return p.ECConfig.DataShards
	}
	// Replication: majority (at least 2 for 3-replication)
	if p.ReplicationFactor <= 1 {
		return 1
	}
	if p.ReplicationFactor == 2 {
		return 2
	}
	// 3+: use majority (N/2 + 1)
	return p.ReplicationFactor/2 + 1
}

// EffectiveReadQuorum returns the minimum number of replicas to read
// to guarantee seeing the latest write. Ensures W + R > N.
func (p PlacementPolicy) EffectiveReadQuorum() int {
	n := p.ReplicationFactor
	if p.ECConfig != nil && p.ECConfig.DataShards > 0 {
		n = p.ECConfig.TotalShards()
	}
	if n <= 0 {
		return 1
	}
	w := p.EffectiveWriteQuorum()
	r := n - w + 1
	if r < 1 {
		r = 1
	}
	return r
}

// IsSyncWrite returns true if writes require more than 1 replica ACK.
func (p PlacementPolicy) IsSyncWrite() bool {
	return p.EffectiveWriteQuorum() > 1
}

// StorageTier defines the storage performance tier.
type StorageTier uint8

const (
	StorageTierAny StorageTier = 0 // No tier preference (used as default/unset)
	TierHot        StorageTier = 1 // NVMe
	TierWarm       StorageTier = 2 // SSD
	TierCold       StorageTier = 3 // HDD
	TierArchive    StorageTier = 4 // Tape / remote cold storage
)
