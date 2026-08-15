package datanode

import (
	"context"

	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// OpsStore is the read/inspect/verify/status subset of the storage backend
// that the management channel (OpsServer HTTP + management unix socket)
// requires.
//
// Replication / anti-entropy / repair are NOT part of this interface — those
// sit on the Metadata V2 placement/EC serving path (task #56) and have their
// own wiring.
type OpsStore interface {
	Stats() (totalBytes int64, chunkCount int64)
	ListChunks() []LocalChunkInfo
	// DiskInfos returns per-disk metadata (Index/Dir/UsedBytes/ChunkCount/Failed).
	DiskInfos() []DiskInfo
	// VerifyChunkData re-reads a chunk and reports whether its on-disk content
	// still matches the recorded checksum (integrity check).
	VerifyChunkData(chunkID metadata.ChunkID) (bool, uint32, error)
	// PerfSnapshot returns engine performance counters.
	PerfSnapshot() ChunkStorePerfSnapshot
	Delete(chunkID metadata.ChunkID) error
}

// DiskLifecycleOps is the optional hot-add/remove/migrate capability of a
// storage backend. V2Store implements it; the management servers advertise
// these commands only when the backend implements the capability.
// V2Store.AddDisk additionally requires a disk factory (SetDiskFactory) to
// have been injected by the datanode main — without one it degrades to an
// "unsupported" error.
type DiskLifecycleOps interface {
	AddDisk(dir string, maxWrites, maxReads int) (int, error)
	RemoveDisk(idx int) error
	MigrateDisk(srcIdx int) (int, error)
}

// DrainOps is the optional graceful-shutdown drain capability. V2Store
// exposes it; the management/ops channel advertises /drain when a backend
// implements the capability. V2Store's DrainWrites delegates to its internal
// QuiesceWrites write barrier.
type DrainOps interface {
	DrainWrites(ctx context.Context) (func(), error)
}

// Compile-time: V2Store satisfies all ops interfaces.
var _ OpsStore = (*V2Store)(nil)
var _ DiskLifecycleOps = (*V2Store)(nil)
var _ DrainOps = (*V2Store)(nil)
