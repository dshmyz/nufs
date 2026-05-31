// Package datanode implements the chunk storage engine and data node server
// for the distributed storage system. Each data node manages local disk storage
// for data chunks, handles read/write requests, and participates in replication.
package datanode

import (
	"time"

	"github.com/example/dfs/metadata"
)

// ========== Configuration ==========

// Config holds the data node configuration.
type Config struct {
	// NodeID is the unique identifier for this data node.
	NodeID metadata.NodeID

	// ListenAddr is the TCP address for the data node server (e.g., "0.0.0.0:9100").
	ListenAddr string

	// OpsListenAddr is the HTTP address for management API (e.g., "0.0.0.0:8091").
	OpsListenAddr string

	// DataDir is the root directory for chunk storage on local disk.
	DataDir string

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

	// Tier is the storage tier (NVMe/SSD/HDD).
	Tier metadata.StorageTier

	// CapacityGB is the total storage capacity of this node in gigabytes.
	CapacityGB int64
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
	}
}

// ========== Wire Protocol ==========

// RequestType identifies the type of data node RPC request.
type RequestType uint8

const (
	ReqWriteChunk     RequestType = iota // Write chunk data
	ReqReadChunk                         // Read chunk data
	ReqDeleteChunk                       // Delete a chunk
	ReqReplicateChunk                    // Replicate chunk from peer
	ReqChunkInfo                         // Get chunk metadata
	ReqListChunks                        // List local chunks
	ReqHealth                            // Health check
)

// Header is the wire protocol header for all data node messages.
// Wire format: [4-byte header_len][Header JSON][body]
type Header struct {
	Type      RequestType       `json:"type"`
	ChunkID   metadata.ChunkID  `json:"chunk_id"`
	Offset    int64             `json:"offset"`     // byte offset within chunk
	Length    int32             `json:"length"`     // data length (0 = entire chunk)
	Checksum  uint32            `json:"checksum"`   // CRC32C of body data
	RequestID uint64            `json:"request_id"` // for request/response correlation
	Extra     map[string]string `json:"extra,omitempty"`
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
}

// LocalChunkState tracks the write lifecycle on the local node.
type LocalChunkState uint8

const (
	LocalWriting LocalChunkState = iota // Write in progress
	LocalWritten                        // Write complete, pending seal
	LocalSealed                         // Sealed and verified
	LocalCorrupt                        // Checksum mismatch detected
)
