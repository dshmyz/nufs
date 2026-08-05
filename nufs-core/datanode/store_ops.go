package datanode

import (
	"context"

	"github.com/example/dfs/metadata"
)

// OpsStore is the read/inspect/verify/status subset of the storage backend
// that the management channel (OpsServer HTTP + management unix socket)
// requires. Both the legacy ChunkStore and the V2.1 V2Store implement it, so
// either engine can drive the same operational API surface.
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
	// PerfSnapshot returns engine performance counters (V2.1 reports a zero
	// snapshot for the V1-only fsync/semaphore metrics).
	PerfSnapshot() ChunkStorePerfSnapshot
	Delete(chunkID metadata.ChunkID) error
}

// DiskLifecycleOps is the optional hot-add/remove/migrate capability of a
// storage backend (V1 ChunkStore has it; V2.1 V2Store does not yet). The
// management servers advertise these commands only when the backend
// implements the capability.
type DiskLifecycleOps interface {
	AddDisk(dir string, maxWrites, maxReads int, wal *WriteAheadLog) (int, error)
	RemoveDisk(idx int) error
	MigrateDisk(srcIdx int) (int, error)
}

// DrainOps is the optional graceful-shutdown drain capability (V1 ChunkStore
// has it). V2.1 does NOT advertise it to the ops channel: V2Store's internal
// shutdown barrier is named QuiesceWrites (not DrainWrites), so it does not
// structurally satisfy DrainOps and the management/ops channel keeps
// returning "drain unsupported by this engine" for V2.1 (option A), even
// though the internal §4 shutdown drain is real and wired in runDataNodeV21.
type DrainOps interface {
	DrainWrites(ctx context.Context) (func(), error)
}

// Compile-time: both storage engines satisfy the OpsStore operations surface.
var _ OpsStore = (*ChunkStore)(nil)
var _ OpsStore = (*V2Store)(nil)

// Compile-time: only the legacy ChunkStore exposes the disk-lifecycle and
// drain capabilities. V2.1 gates both out of the management/ops channel.
var _ DiskLifecycleOps = (*ChunkStore)(nil)
var _ DrainOps = (*ChunkStore)(nil)
