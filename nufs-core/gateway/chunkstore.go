package gateway

import (
	"context"

	"github.com/example/dfs/metadata"
)

// ChunkStore is the data path used by every gateway (FUSE, S3,
// future HTTP) to read and write chunk payloads. It is defined
// here, in the package above the per-protocol gateways, so that
// fusegw and s3gw can share the same production implementation
// (DatanodeChunkStore) and the same in-memory test double without
// either package depending on the other.
//
// Implementations:
//   - *s3.DatanodeChunkStore  — production: talks to datanode daemons
//   - *s3.MemoryChunkStore   — tests: in-memory map
//
// The S3 package re-exports this interface as s3.ChunkStore for
// backwards compatibility with code that already references it.
type ChunkStore interface {
	// WriteChunk writes the given data to all replicas for the chunk.
	// It returns nil only when at least the chunk store's
	// MinReplicasPerWrite replicas have acknowledged the write.
	WriteChunk(ctx context.Context, chunk *metadata.ChunkMeta, data []byte) error

	// ReadChunk reads the chunk payload from the first healthy replica.
	// The returned slice may be larger than the requested range; callers
	// are responsible for trimming to the bytes they need.
	ReadChunk(ctx context.Context, chunk *metadata.ChunkMeta) ([]byte, error)
}
