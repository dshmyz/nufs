// Package chunkstore provides the distributed storage engine for nufs.
// Any service (gateway, FUSE, CLI, external) can import this package
// to read and write chunks across a cluster of datanodes.
//
// The ChunkStore interface abstracts over two data path strategies:
//   - Replication: same data sent to all replicas (WritePipeline fan-out)
//   - Erasure coding: data split into K+M shards, each shard sent to a different datanode
//
// The caller does not need to know which strategy is active — the
// implementation inspects chunk.ECGroup and routes accordingly.
package chunkstore

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/example/dfs/datanode"
	"github.com/example/dfs/internal/tlsutil"
	"github.com/example/dfs/metadata"
)

// ChunkStore is the distributed storage interface.
// All callers (gateway, fuse, external services) program against this.
type ChunkStore interface {
	WriteChunk(ctx context.Context, chunk *metadata.ChunkMeta, data []byte) error
	ReadChunk(ctx context.Context, chunk *metadata.ChunkMeta) ([]byte, error)
	ReadChunkRange(ctx context.Context, chunk *metadata.ChunkMeta, offset int64, length int32) ([]byte, error)
}

// DatanodeChunkStore is the production ChunkStore implementation.
// It talks to datanode daemons over TCP using a connection pool and
// supports both replication and erasure coding transparently.
type DatanodeChunkStore struct {
	pool     *datanode.ClientPool
	pipeline *datanode.WritePipeline
	tlsCfg   tlsutil.Config
	// MinReplicasPerWrite is the floor on the number of replicas that
	// must acknowledge a write. <= 0 means "all replicas must succeed".
	MinReplicasPerWrite int
}

// NewDatanodeChunkStore returns a ChunkStore that dials datanode daemons
// over TCP using a connection pool.
func NewDatanodeChunkStore() *DatanodeChunkStore {
	pool := datanode.NewClientPool(4, 30*time.Second, 10*time.Second)
	pipeline := datanode.NewWritePipeline(pool, 30*time.Second)
	return &DatanodeChunkStore{pool: pool, pipeline: pipeline}
}

// SetTLS configures TLS for connections to datanode daemons.
func (s *DatanodeChunkStore) SetTLS(cfg tlsutil.Config) {
	s.tlsCfg = cfg
	s.pool.SetTLS(cfg)
}

// Close releases the connection pool. After Close returns the store
// can no longer be used for I/O. Callers that construct a ChunkStore
// for the lifetime of a process should defer Close to let the
// underlying datanode connections shut down promptly.
func (s *DatanodeChunkStore) Close() {
	if s.pool != nil {
		s.pool.CloseAll()
	}
}

// WriteChunk writes data to the chunk's assigned datanodes.
// For replicated chunks (chunk.ECGroup == nil), the same data is
// sent to all replicas via WritePipeline.
// For EC chunks (chunk.ECGroup != nil), data is encoded into K+M
// shards and each shard is written to its assigned datanode.
func (s *DatanodeChunkStore) WriteChunk(ctx context.Context, chunk *metadata.ChunkMeta, data []byte) error {
	if chunk == nil {
		return fmt.Errorf("chunkstore: nil chunk")
	}
	if len(chunk.Replicas) == 0 {
		return fmt.Errorf("chunkstore: chunk %d has no replicas", chunk.ID)
	}

	if chunk.ECGroup != nil {
		return s.writeECChunk(ctx, chunk, data)
	}

	// Replication path: fan out the same data to all replicas
	required := s.requiredReplicas(len(chunk.Replicas))
	// Carry the metadata-issued generation (Metadata V2 fencing) so every
	// replica lands on the same authoritative generation. Zero (legacy V1
	// chunks) leaves each datanode to its own local generation.
	pp := datanode.NewWritePipeline(s.pool, 30*time.Second,
		datanode.WithQuorum(required), datanode.WithGeneration(chunk.Generation))
	return pp.Write(ctx, chunk.ID, data, chunk.Replicas)
}

// ReadChunk reads the full chunk data.
func (s *DatanodeChunkStore) ReadChunk(ctx context.Context, chunk *metadata.ChunkMeta) ([]byte, error) {
	return s.ReadChunkRange(ctx, chunk, 0, 0)
}

// ReadChunkRange reads a subrange [offset, offset+length) from the chunk.
// offset=0, length=0 reads the entire chunk.
//
// For replicated chunks, reads from the first healthy replica.
// For EC chunks, reads K+M shards in parallel and decodes.
func (s *DatanodeChunkStore) ReadChunkRange(ctx context.Context, chunk *metadata.ChunkMeta, offset int64, length int32) ([]byte, error) {
	if chunk == nil {
		return nil, fmt.Errorf("chunkstore: nil chunk")
	}
	if len(chunk.Replicas) == 0 {
		return nil, fmt.Errorf("chunkstore: chunk %d has no replicas", chunk.ID)
	}

	if chunk.ECGroup != nil {
		return s.readECChunk(ctx, chunk, offset, length)
	}

	// Replication path: read from first healthy replica
	return s.readReplicaChunk(ctx, chunk, offset, length)
}

// requiredReplicas maps MinReplicasPerWrite to the actual floor for this chunk.
func (s *DatanodeChunkStore) requiredReplicas(total int) int {
	if s.MinReplicasPerWrite <= 0 {
		return total
	}
	if s.MinReplicasPerWrite > total {
		return total
	}
	return s.MinReplicasPerWrite
}

// readReplicaChunk reads from the first healthy replica (replication mode).
func (s *DatanodeChunkStore) readReplicaChunk(ctx context.Context, chunk *metadata.ChunkMeta, offset int64, length int32) ([]byte, error) {
	replicas := sortReplicasForRead(chunk.Replicas)
	var lastErr error
	for _, rep := range replicas {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if rep.Addr == "" {
			lastErr = fmt.Errorf("replica on node %d has empty addr", rep.NodeID)
			continue
		}

		client, err := s.pool.Get(rep.Addr)
		if err != nil {
			lastErr = fmt.Errorf("connect to %s: %w", rep.Addr, err)
			slog.Warn("chunkstore: connect failed", "addr", rep.Addr, "chunk", chunk.ID, "error", err)
			continue
		}
		resp, err := client.ReadChunk(chunk.ID, offset, length)
		s.pool.Put(rep.Addr, client)
		if err != nil {
			lastErr = fmt.Errorf("read from %s: %w", rep.Addr, err)
			slog.Warn("chunkstore: read failed", "addr", rep.Addr, "chunk", chunk.ID, "error", err)
			continue
		}
		if resp.Status != datanode.StatusOK {
			lastErr = fmt.Errorf("datanode %s status=%d: %s", rep.Addr, resp.Status, resp.Error)
			slog.Warn("chunkstore: non-OK status", "addr", rep.Addr, "chunk", chunk.ID, "status", resp.Status)
			continue
		}
		return resp.Data, nil
	}
	if lastErr != nil {
		return nil, fmt.Errorf("chunkstore: all %d replicas failed for chunk %d, last error: %w", len(replicas), chunk.ID, lastErr)
	}
	return nil, fmt.Errorf("chunkstore: all %d replicas failed for chunk %d", len(replicas), chunk.ID)
}

// sortReplicasForRead orders replicas by health: Ready > Syncing > Stale > Failed.
func sortReplicasForRead(replicas []metadata.ReplicaInfo) []metadata.ReplicaInfo {
	sorted := append([]metadata.ReplicaInfo(nil), replicas...)
	sort.SliceStable(sorted, func(i, j int) bool {
		return replicaReadPriority(sorted[i].State) < replicaReadPriority(sorted[j].State)
	})
	return sorted
}

func replicaReadPriority(s metadata.ReplicaState) int {
	switch s {
	case metadata.ReplicaReady:
		return 0
	case metadata.ReplicaSyncing:
		return 1
	case metadata.ReplicaStale:
		return 2
	case metadata.ReplicaFailed:
		return 3
	default:
		return 2
	}
}
