// Package datanode implements the chunk storage engine and data node server
// for the distributed storage system. Each data node manages local disk storage
// for data chunks, handles read/write requests, and participates in replication.
package datanode

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/sys/unix"

	"github.com/dshmyz/nufs/nufs-core/internal/tlsutil"
	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// ========== Configuration ==========

// Config holds the data node configuration.
type Config struct {
	// NodeID is the unique identifier for this data node.
	NodeID metadata.NodeID

	// ListenAddr is the TCP address for the data node server (e.g., "0.0.0.0:9100").
	ListenAddr string

	// RegisterAddr is the address registered with the metadata service
	// as the reachable endpoint for this node. In containerized/multi-
	// host deployments this must be a routable host:port (e.g.
	// "datanode-1:9100"), NOT the bind address (0.0.0.0) — peers connect
	// to this address, so 0.0.0.0 would make them dial themselves.
	// Empty = ListenAddr.
	RegisterAddr string

	// OpsListenAddr is the HTTP address for management API (e.g., "0.0.0.0:8091").
	OpsListenAddr string

	// DataDir is the root directory for chunk storage on local disk.
	// Used for single-disk deployments. Ignored when DataDirs is non-empty.
	DataDir string

	// DataDirs is the list of root directories for JBOD multi-disk storage.
	// When set, the datanode manages all disks in one process (writes spread
	// across disks, single-disk failure is isolated). Empty = single disk
	// (use DataDir).
	DataDirs []string

	// MetadataAddr is the address of the metadata service (e.g., "localhost:8091").
	MetadataAddr string

	// MetadataCacheDir is the directory for the local Pebble cache.
	MetadataCacheDir string

	// HeartbeatInterval is the interval between heartbeat reports to the metadata service.
	HeartbeatInterval time.Duration

	// MaxConcurrentWrites limits simultaneous chunk write operations.
	MaxConcurrentWrites int

	// MaxConcurrentReads limits simultaneous chunk read operations.
	MaxConcurrentReads int

	// Rack is the rack identifier for topology-aware placement.
	Rack string

	// Zone is the availability zone identifier.
	Zone string

	// MachineID is the physical machine identifier for topology placement.
	MachineID string

	// Tier is the storage tier (NVMe/SSD/HDD).
	Tier metadata.StorageTier

	// CapacityGB is the total storage capacity of this node in gigabytes.
	CapacityGB int64

	// ClientTimeout is the default timeout for inter-node TCP operations.
	ClientTimeout time.Duration

	// MaxConnections limits the number of concurrent TCP connections.
	// Default: 256. Set to 0 for unlimited.
	MaxConnections int

	// RequestTimeout is the per-request timeout for TCP operations.
	// Slow clients that exceed this timeout will be disconnected.
	// Default: 30s.
	RequestTimeout time.Duration

	// TLS configures TLS for both the chunk TCP server and the
	// ops HTTP server. When CertFile and KeyFile are empty, both
	// run in plain-text mode.
	TLS tlsutil.Config

	// MetadataAuthToken is sent as a bearer token to the metadata service.
	MetadataAuthToken string

	// OpsAuthToken protects the datanode HTTP operations API when set.
	OpsAuthToken string

	// EnablePprof exposes /debug/pprof on the operations API.
	EnablePprof bool

	// TraceEnabled enables OpenTelemetry tracing provider initialization.
	TraceEnabled bool

	// TraceEndpoint is the OTLP gRPC endpoint for tracing export.
	TraceEndpoint string

	// TraceInsecure uses insecure OTLP transport when tracing is enabled.
	TraceInsecure bool

	// EncryptAtRest enables AES-256-GCM encryption for chunk data
	// stored on local disk. When true, data is encrypted before
	// writing and decrypted after reading.
	EncryptAtRest bool

	// AllowLocalKMS permits the in-memory development KMS for at-rest
	// encryption. It is not production safe because keys are lost on restart.
	AllowLocalKMS bool

	// KMSKeyFile / KMSKeyEnv / KMSKeyHex configure a production FileKMS
	// (envelope encryption: KEK in a file, env var, or hex string; DEKs
	// persisted on disk so they survive restart). When any is set and
	// EncryptAtRest is true, the FileKMS is used instead of LocalKMS and
	// AllowLocalKMS is not required.
	KMSKeyFile string
	KMSKeyEnv  string
	KMSKeyHex  string

	// AllowInsecureDev permits running the datanode without TLS (dev only).
	// Without it and without --tls-cert, the datanode refuses to start so
	// it cannot silently listen in plaintext in a production setting.
	AllowInsecureDev bool

	// AlertWebhook is an optional URL that receives capacity-alert events as a
	// JSON POST (async, non-blocking). Empty disables webhook delivery; alerts
	// still land in the admin ring buffer + logs.
	AlertWebhook string

	// SegmentSize is the V2.1 per-stream segment size in bytes. 0 keeps the
	// storage default (4GiB); a smaller value seals segments sooner so the
	// compaction worker reclaims superseded bytes faster (demos/CI).
	SegmentSize int64

	// CompactionInterval is the background compaction scan cadence for the
	// V2.1 worker. 0 keeps the maintenance default (30s).
	CompactionInterval time.Duration

	// GCScanInterval is the cadence for the background orphan-chunk GC scan
	// on the ops server. 0 disables the background scan (leaving only the
	// manual POST /api/v1/gc/scan endpoint). Orphan scan deletes local chunks
	// absent from metadata, so it is bounded by GCGraceWindow to avoid
	// deleting in-flight writes whose metadata commit has not landed yet.
	GCScanInterval time.Duration

	// GCGraceWindow is how long a locally-written chunk must exist before the
	// orphan GC scan will consider deleting it. Chunks this young are presumed
	// to be in the "written locally but not yet committed to metadata"
	// window, during which GetChunk legitimately reports not-found.
	GCGraceWindow time.Duration

	// ScrubInterval is the cadence for the background data integrity scan
	// (scrub worker). The scrub reads every local chunk and verifies its
	// CRC32C checksum; corruption is reported via the change journal so the
	// heartbeat ships it to the metadata authority. 0 disables the scan.
	// Default in the scrub worker: 6h.
	ScrubInterval time.Duration

	// ECConvertInterval is the cadence for the background EC conversion worker
	// (consumes metad TaskECConvert background tasks whose chunk replica lives
	// on this node and runs the §14 replication→EC transaction). Default in
	// the worker: 30s.
	ECConvertInterval time.Duration

	// LogLevel is the initial log level (debug/info/warn/error).
	// Can be changed at runtime via SIGHUP signal.
	LogLevel string
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() Config {
	return Config{
		ListenAddr:          "0.0.0.0:9100",
		OpsListenAddr:       "0.0.0.0:8091",
		DataDir:             "/var/lib/dfs/data",
		MetadataAddr:        "localhost:8091",
		MetadataCacheDir:    "",
		HeartbeatInterval:   10 * time.Second,
		MaxConcurrentWrites: 64,
		MaxConcurrentReads:  256,
		Tier:                metadata.TierHot,
		CapacityGB:          1000,
		ClientTimeout:       30 * time.Second,
		// Background orphan GC is off by default; operators opt in via the
		// --gc-scan-interval flag. When enabled, default to a 10-minute grace
		// window so in-flight writes are never swept.
		GCScanInterval: 0,
		GCGraceWindow:  10 * time.Minute,
	}
}

// ========== Wire Protocol ==========

// RequestType identifies the type of data node RPC request.
type RequestType uint8

const (
	ReqWriteChunk       RequestType = iota // Write chunk data
	ReqReadChunk                           // Read chunk data
	ReqDeleteChunk                         // Delete a chunk
	ReqReplicateChunk                      // Replicate chunk from peer
	ReqChunkInfo                           // Get chunk metadata
	ReqListChunks                          // List local chunks
	ReqHealth                              // Health check
	ReqReplicateECShard                    // Replicate a single EC shard to a peer datanode
	ReqReadECShard                         // Read a single EC shard from a peer datanode
)

// Header is the wire protocol header for all data node messages.
// Wire format: [4-byte header_len][Header JSON][body]
type Header struct {
	Type      RequestType      `json:"type"`
	ChunkID   metadata.ChunkID `json:"chunk_id"`
	Offset    int64            `json:"offset"`     // byte offset within chunk
	Length    int32            `json:"length"`     // data length (0 = entire chunk)
	Checksum  uint32           `json:"checksum"`   // CRC32C of body data
	RequestID uint64           `json:"request_id"` // for request/response correlation
	// Generation is the metadata-issued write generation (Metadata V2
	// fencing). 0 = unspecified → the receiving datanode keeps its own local
	// generation (legacy V1 behavior). Obsolete (idempotent) writes are fenced
	// on receipt.
	Generation uint64 `json:"generation,omitempty"`
	// ShardIndex is the EC shard index within a stripe, used for EC shard
	// replication (ReqReplicateECShard). 0 = unspecified (whole-chunk op).
	ShardIndex int               `json:"shard_index,omitempty"`
	Extra      map[string]string `json:"extra,omitempty"`
}

// Response is the standard response from a data node.
type Response struct {
	RequestID uint64         `json:"request_id"`
	Status    ResponseStatus `json:"status"`
	Error     string         `json:"error,omitempty"`
	Data      []byte         `json:"data,omitempty"`
	Length    int32          `json:"length"`
	Checksum  uint32         `json:"checksum"`
}

// ResponseStatus indicates the result of a data node operation.
type ResponseStatus uint8

const (
	StatusOK ResponseStatus = iota
	StatusError
	StatusNotFound
	StatusFull
	StatusBusy
)

// ========== Chunk Disk Layout ==========
//
// Each chunk is stored as a single file on local disk:
//   {DataDir}/chunks/{shard}/{chunk_id}.dat
//
// Sharding: chunk_id % 256 → 256 subdirectories (shard 0x00 - 0xFF)
//   This avoids too many files in a single directory.
//
// Chunk file format:
//   [4-byte magic "DFS\x01"]
//   [8-byte chunk_id (big-endian)]
//   [4-byte data_length (big-endian)]
//   [4-byte crc32c checksum (big-endian)]
//   [N bytes chunk data]
//
// Metadata sidecar (optional, for fast startup):
//   {DataDir}/chunks/{shard}/{chunk_id}.meta
//   JSON file with ChunkMeta snapshot

const (
	// ChunkFileMagic is the magic bytes at the start of every chunk file.
	ChunkFileMagic = "DFS\x01"
	// ChunkFileHeaderSize is the fixed-size header in bytes (4 + 8 + 4 + 4 = 20).
	ChunkFileHeaderSize = 20
	// MaxShards is the number of shard directories for chunk distribution.
	MaxShards = 256
)

// ========== Local Chunk State ==========

// LocalChunkInfo tracks the local state of a chunk on this data node.
type LocalChunkInfo struct {
	ChunkID     metadata.ChunkID     `json:"chunk_id"`
	Size        int64                `json:"size"` // actual bytes stored
	Checksum    uint32               `json:"checksum"`
	State       LocalChunkState      `json:"state"`
	Tier        metadata.StorageTier `json:"tier"`
	WrittenAt   time.Time            `json:"written_at"`
	LastAccess  time.Time            `json:"last_access"`
	AccessCount int64                `json:"access_count"`
	// DiskIndex identifies which disk in the JBOD set holds this chunk.
	// Reconstructed at startup from which disk's directory the file is
	// scanned from; not persisted in the chunk file header.
	DiskIndex int `json:"disk_index,omitempty"`
}

// LocalChunkState tracks the write lifecycle on the local node.
type LocalChunkState uint8

const (
	LocalWriting LocalChunkState = iota // Write in progress
	LocalWritten                        // Write complete, pending seal
	LocalSealed                         // Sealed and verified
	LocalCorrupt                        // Checksum mismatch detected
)

// ========== Shared types (extracted from deleted V1 files) ==========

// DiskState represents the operational health of a disk.
type DiskState int

const (
	DiskOnline   DiskState = iota // Healthy
	DiskDegraded                  // I/O errors detected, below threshold
	DiskFailed                    // Too many I/O errors, read-only
)

func (s DiskState) String() string {
	switch s {
	case DiskOnline:
		return "online"
	case DiskDegraded:
		return "degraded"
	case DiskFailed:
		return "failed"
	default:
		return "unknown"
	}
}

// AlertLevel represents capacity alert severity.
type AlertLevel int32

const (
	AlertNone     AlertLevel = iota // No alert
	AlertWarn                       // Usage above warn threshold
	AlertCritical                   // Usage above critical threshold
)

func (l AlertLevel) String() string {
	switch l {
	case AlertWarn:
		return "warn"
	case AlertCritical:
		return "critical"
	default:
		return "none"
	}
}

// ChunkStorePerfSnapshot holds performance counters for the store.
type ChunkStorePerfSnapshot struct {
	WriteSemWaitNs     int64 `json:"write_sem_wait_ns"`
	ReadSemWaitNs      int64 `json:"read_sem_wait_ns"`
	FsyncNs            int64 `json:"fsync_ns"`
	FsyncCount         int64 `json:"fsync_count"`
	ReadRequestedBytes int64 `json:"read_requested_bytes"`
	ReadAmplifiedBytes int64 `json:"read_amplified_bytes"`
	FdCacheHits        int64 `json:"fd_cache_hits"`
	FdCacheMisses      int64 `json:"fd_cache_misses"`
	FdCacheEvictions   int64 `json:"fd_cache_evictions"`
	ListChunksNs       int64 `json:"list_chunks_ns"`
	ListChunksCalls    int64 `json:"list_chunks_calls"`
	ListChunksItems    int64 `json:"list_chunks_items"`
}

// DiskStatsItem holds per-disk usage and health, used by the heartbeat
// reporter and the management interface.
type DiskStatsItem struct {
	Index      int
	UsedBytes  int64
	TotalBytes int64
	// OnDiskBytes is the physical bytes occupied by the store's data files
	// on this disk, including superseded record generations not yet
	// reclaimed by seal+compaction. Distinct from UsedBytes (logical live
	// bytes); a console can show both honestly.
	OnDiskBytes int64
	ChunkCount  int64
	Failed      bool
	// State is the derived 3-tier health (DiskOnline/DiskDegraded/DiskFailed).
	State DiskState
}

// DiskInfo is a read-only snapshot of one disk shard's metadata, returned
// by DiskInfos for the management interface.
type DiskInfo struct {
	Index       int
	Dir         string
	UsedBytes   int64
	OnDiskBytes int64
	ChunkCount  int64
	Failed      bool
	// State is the derived 3-tier health (DiskOnline/DiskDegraded/DiskFailed).
	// Zero value = DiskOnline.
	State DiskState
}

// DiskIndexByDir resolves a directory to its disk index, preferring the first
// HEALTHY (non-Failed) entry that claims that dir and falling back to a Failed
// entry only when no healthy one matches.
func DiskIndexByDir(infos []DiskInfo, dir string) int {
	firstAny := -1
	for _, d := range infos {
		if d.Dir != dir {
			continue
		}
		if firstAny < 0 {
			firstAny = d.Index
		}
		if !d.Failed {
			return d.Index
		}
	}
	return firstAny
}

// DiskStatsSnapshot is a point-in-time copy of DiskStats without atomic fields
// (safe to copy).
type DiskStatsSnapshot struct {
	TotalBytes  int64     `json:"total_bytes"`
	UsedBytes   int64     `json:"used_bytes"`
	OnDiskBytes int64     `json:"on_disk_bytes,omitempty"`
	AvailBytes  int64     `json:"avail_bytes"`
	UsagePct    float64   `json:"usage_pct"`
	ChunkCount  int64     `json:"chunk_count"`
	ReadIOPS    int64     `json:"read_iops"`
	WriteIOPS   int64     `json:"write_iops"`
	ReadBytes   int64     `json:"read_bytes"`
	WriteBytes  int64     `json:"write_bytes"`
	IOErrors    int64     `json:"io_errors"`
	DiskState   string    `json:"disk_state"`
	LastUpdated time.Time `json:"last_updated"`
}

// detectCapacityBytes returns the filesystem total bytes for dir via Statfs,
// or 0 if it cannot be determined.
func detectCapacityBytes(dir string) int64 {
	var s unix.Statfs_t
	if err := unix.Statfs(dir, &s); err != nil {
		return 0
	}
	return int64(s.Blocks) * int64(s.Bsize)
}

// dataFilesOnDiskBytes sums the logical sizes of all files under root whose
// name ends in one of the given extensions.
func dataFilesOnDiskBytes(root string, exts ...string) int64 {
	if root == "" {
		return 0
	}
	var total int64
	filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		found := false
		for _, e := range exts {
			if strings.HasSuffix(d.Name(), e) {
				found = true
				break
			}
		}
		if !found {
			return nil
		}
		if fi, err := os.Stat(path); err == nil {
			total += fi.Size()
		}
		return nil
	})
	return total
}

// ========== Metadata interfaces (shared by V2.1 heartbeat/repair) ==========

// HeartbeatMeta is the metadata surface used by HeartbeatReporter.
type HeartbeatMeta interface {
	Heartbeat(ctx context.Context, nodeID metadata.NodeID, report *metadata.NodeReport) error
	AckChangeEvents(ctx context.Context, nodeID metadata.NodeID, seq uint64) (uint64, error)
}

// RepairMeta is the metadata surface used by RepairWorker.
type RepairMeta interface {
	GetChunk(ctx context.Context, id metadata.ChunkID) (*metadata.ChunkMeta, error)
	UpdateChunk(ctx context.Context, chunk *metadata.ChunkMeta) error
	GetRepairQueue(ctx context.Context) ([]metadata.RepairTask, error)
	RemoveRepairTask(ctx context.Context, chunkID metadata.ChunkID) error
	ReportChunkState(ctx context.Context, nodeID metadata.NodeID, states map[metadata.ChunkID]metadata.ReplicaState) error
	ListNodes(ctx context.Context) ([]metadata.NodeInfo, error)
	ChunksByNode(ctx context.Context, nodeID metadata.NodeID) ([]metadata.ChunkMeta, error)
	TriggerRepair(ctx context.Context, chunkID metadata.ChunkID) error
}

var (
	_ HeartbeatMeta = (*metadata.PebbleStore)(nil)
	_ RepairMeta    = (*metadata.PebbleStore)(nil)
)
