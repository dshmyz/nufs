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

// InodeMeta represents metadata for a file or directory (legacy V1
// layout, still used until V2.1 reaches feature parity).
// Stored at key: /inode/{inode_id}
type InodeMeta struct {
	ID         InodeID  `json:"id"`
	Type       FileType `json:"type"`
	Size       int64    `json:"size"`                  // Total file size in bytes
	NLink      uint32   `json:"nlink"`                 // Hard link count
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

// InodeLayout describes how a file's extents are stored (V2.1 §11.1).
type InodeLayout uint8

const (
	LayoutEmpty InodeLayout = iota
	LayoutInlineExtent
	LayoutExtentPages
)

// InodeMetaV2 is the V2.1 fixed-attribute inode (§11.1). Unlike the
// legacy InodeMeta (which embeds an unbounded ChunkMap), it holds only
// fixed attributes plus a single inline extent reference or an extent
// page root. Multi-extent files use copy-on-write extent pages under
// /extent-page/{inode_id}/{extent_root}/{page_no}.
type InodeMetaV2 struct {
	ID         InodeID  `json:"id"`
	Type       FileType `json:"type"`
	Size       int64    `json:"size"`
	NLink      uint32   `json:"nlink"`
	BucketRoot InodeID  `json:"bucket_root,omitempty"`
	UID        uint32   `json:"uid"`
	GID        uint32   `json:"gid"`
	Mode       uint32   `json:"mode"`
	CTime      int64    `json:"ctime"`
	MTime      int64    `json:"mtime"`
	ATime      int64    `json:"atime"`

	// Layout is Empty, InlineExtent, or ExtentPages.
	Layout InodeLayout `json:"layout"`

	// InlineExtent is set when Layout==InlineExtent (single-extent files).
	InlineExtent *ExtentMetaV2 `json:"inline_extent,omitempty"`

	// ExtentRoot is the versioned COW root for Layout==ExtentPages.
	ExtentRoot uint64 `json:"extent_root,omitempty"`
	// ExtentPageCount is the number of pages under the current root.
	ExtentPageCount uint32 `json:"extent_page_count,omitempty"`
	// ExtentRootVersion distinguishes COW roots (old roots enter GC).
	ExtentRootVersion uint64 `json:"extent_root_version,omitempty"`

	Symlink string            `json:"symlink,omitempty"`
	XAttrs  map[string][]byte `json:"xattrs,omitempty"`
}

// ExtentMetaV2 is one extent's metadata (V2.1 §11.2). It does not
// repeat complete DataNode addresses; placement is resolved through the
// placement group.
type ExtentMetaV2 struct {
	ID         ExtentIDV2 `json:"id"`
	Generation uint64     `json:"generation"`
	LogicalLen int64      `json:"logical_len"`
	Checksum   uint32     `json:"checksum"`
	// PGID selects the placement group (§11.3).
	PGID uint32 `json:"pg_id"`
	// PlacementEpoch is the PG epoch this extent was placed under.
	PlacementEpoch uint64 `json:"placement_epoch"`
	// Lifecycle state (Ready/Degraded/etc).
	Lifecycle ExtentLifecycle `json:"lifecycle"`
	// StorageClass distinguishes hot replica from cold EC.
	StorageClass StorageClass `json:"storage_class"`
	// ECStripeID is set when StorageClass==ColdEC.
	ECStripeID string `json:"ec_stripe_id,omitempty"`
	CreatedAt  int64  `json:"created_at"`
}

// ExtentIDV2 identifies an extent in the V2 metadata layer. The high
// 16 bits encode the owning logical partition (§11.4).
type ExtentIDV2 uint64

// OwnerPartition returns the 16-bit logical partition encoded in the ID.
func (id ExtentIDV2) OwnerPartition() uint16 {
	return uint16(id >> 48)
}

// ExtentLifecycle is the lifecycle state of an extent.
type ExtentLifecycle uint8

const (
	LifecycleReady ExtentLifecycle = iota
	LifecycleReadyDegraded
	LifecycleMigrating
	LifecycleDeleting
	LifecycleDeleted
	LifecycleECConverting
)

// StorageClass distinguishes replica (hot) from EC (cold) storage.
type StorageClass uint8

const (
	StorageClassHotReplica StorageClass = iota
	StorageClassColdEC
)

// ExtentPage holds up to MaxExtentsPerPage extent references (V2.1
// §11.1). Stored at /extent-page/{inode_id}/{extent_root}/{page_no}.
type ExtentPage struct {
	InodeID InodeID     `json:"inode_id"`
	PageNo  uint32      `json:"page_no"`
	Extents []ExtentRef `json:"extents"`
}

// MaxExtentsPerPage is the page capacity (§11.1): 256 references →
// 4 GiB at a 16 MiB extent size.
const MaxExtentsPerPage = 256

// ExtentRef references an extent within a file's logical byte range.
type ExtentRef struct {
	ExtentID ExtentIDV2 `json:"extent_id"`
	// LogicalOffset is the byte offset of this extent in the file.
	LogicalOffset int64 `json:"logical_offset"`
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
	Tier       StorageTier   `json:"tier"` // Target storage tier for this chunk
	CreateTime int64         `json:"create_time"`
	Checksum   uint32        `json:"checksum"` // CRC32C of chunk data

	// PGID/Epoch record the placement group this chunk is placed under
	// (Metadata V2 serving path, Task #56 Phase A). When set, Replicas are
	// resolved from the PG's replica set at the recorded epoch; the fields
	// are zero for chunks placed through the legacy per-chunk
	// PlacementEngine path (V1, no placement groups).
	PGID  uint32 `json:"pg_id,omitempty"`
	Epoch uint64 `json:"epoch,omitempty"`
	// Generation is the metadata-issued write generation for this chunk
	// (Metadata V2 fencing, Task #56 Phase C / A2). The metadata service is
	// the authority that hands each write its generation, so an overwrite
	// chains the metadata generation rather than each datanode locally
	// bumping its own counter — this keeps all replicas on the same
	// generation and fences stale/duplicate writes deterministically. Zero
	// for the legacy per-chunk path (V1), where datanodes keep their own
	// local generation.
	Generation uint64 `json:"generation,omitempty"`
}

// ChunkState represents the lifecycle state of a chunk.
type ChunkState uint8

const (
	ChunkCreated  ChunkState = iota // Allocated, not yet committed
	ChunkSealed                     // Commit received, replicating
	ChunkReady                      // All replicas confirmed
	ChunkDegraded                   // Replica lost, repairing
	ChunkOrphan                     // No inode references (GC candidate)
)

// ReplicaInfo describes a replica location.
type ReplicaInfo struct {
	NodeID     NodeID       `json:"node_id"`
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

	// ChangeAck is the highest change-journal sequence the metadata authority
	// has reconciled for this node (persisted, monotonic). The node polls it
	// via the change-ack RPC and advances its local journal Ack once metadata
	// catches up, guaranteeing no un-reconciled event is dropped (§12).
	ChangeAck uint64 `json:"change_ack,omitempty"`
}

// NodeState represents the operational state of a node.
type NodeState uint8

const (
	NodeOnline   NodeState = iota
	NodeDraining           // Being decommissioned
	NodeMaint              // Under maintenance (rolling upgrade)
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
	UsedGB         int64                    `json:"used_gb"`
	ChunkCount     int64                    `json:"chunk_count"`
	DiskIO         float64                  `json:"disk_io"`          // 0.0 - 1.0 utilization
	WriteErrorRate float64                  `json:"write_error_rate"` // 0.0 - 1.0, rolling write failure ratio
	ChunkStates    map[ChunkID]ReplicaState `json:"chunk_states"`
	DiskStats      []DiskReport             `json:"disk_stats,omitempty"` // per-disk breakdown (JBOD)

	// ChangeEvents carries the node's locally-detected async changes
	// (corruption, disk/segment loss — thing the delta ChunkStates does not
	// convey because a corrupt-but-present extent still looks "present").
	// Rides on the heartbeat so the metadata authority can reconcile the
	// affected replicas (mark failed / trigger repair). V1 nodes send none.
	// §12.
	ChangeEvents []ChangeEventRecord `json:"change_events,omitempty"`
}

// ChangeEventKind enumerates the async datanode change-journal event types
// surfaced to the metadata authority (§12). It mirrors the datanode-side
// journal's representation; the datanode reconciler converts its local
// journal events into these records before shipping them on the heartbeat.
type ChangeEventKind uint8

const (
	ChangeCorrupt ChangeEventKind = iota
	ChangeDiskLost
	ChangeSegmentLost
	ChangeRelocated
	ChangeThirdReplicaComplete
	ChangeRepairCreated
	ChangeScrubFinding
	ChangeDeleteComplete
)

func (k ChangeEventKind) String() string {
	switch k {
	case ChangeCorrupt:
		return "corrupt"
	case ChangeDiskLost:
		return "disk_lost"
	case ChangeSegmentLost:
		return "segment_lost"
	case ChangeRelocated:
		return "relocated"
	case ChangeThirdReplicaComplete:
		return "third_replica_complete"
	case ChangeRepairCreated:
		return "repair_created"
	case ChangeScrubFinding:
		return "scrub_finding"
	case ChangeDeleteComplete:
		return "delete_complete"
	default:
		return fmt.Sprintf("unknown(%d)", k)
	}
}

// ChangeEventRecord is the metadata-side representation of one datanode
// change-journal event. ExtentID/Generation identify the affected extent
// (0 for disk/segment-level events); SegmentID is set for those. It is
// shipped inside NodeReport.ChangeEvents and reconciled by the metadata
// service, which then acknowledges the watermark back so the datanode can
// advance its journal Ack.
type ChangeEventRecord struct {
	Seq        uint64          `json:"seq"`
	Kind       ChangeEventKind `json:"kind"`
	ExtentID   uint64          `json:"extent_id,omitempty"`
	Generation uint64          `json:"generation,omitempty"`
	SegmentID  uint64          `json:"segment_id,omitempty"`
	Reason     string          `json:"reason,omitempty"`
	AtUnix     int64           `json:"at_unix,omitempty"`
}

// DiskReport holds per-disk statistics for the heartbeat, allowing the
// metadata service to track per-disk usage and health for placement.
type DiskReport struct {
	Index      int   `json:"index"`
	UsedBytes  int64 `json:"used_bytes"`
	ChunkCount int64 `json:"chunk_count"`
	Failed     bool  `json:"failed"`
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
	// PlacementGroups opts this bucket's chunks into the Metadata V2
	// placement-group serving path (Task #56 Phase A): replicas are
	// resolved from a durable PlacementGroup (PGID/Epoch recorded on each
	// ChunkMeta) instead of a fresh per-chunk PlacementEngine decision.
	// Default false = legacy per-chunk placement (V1), zero behavior change.
	PlacementGroups bool `json:"placement_groups,omitempty"`
}

// CrossZoneConfig defines cross-zone (cross-DC) replication settings.
type CrossZoneConfig struct {
	RemoteZone    string `json:"remote_zone"`    // Target zone name
	ReplicaFactor int    `json:"replica_factor"` // Number of remote replicas (default: 1)
	AsyncMode     bool   `json:"async_mode"`     // true=async, false=sync
	BandwidthMBps int    `json:"bandwidth_mbps"` // Bandwidth limit in MB/s (0=unlimited)
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
