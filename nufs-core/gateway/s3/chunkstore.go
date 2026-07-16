package s3

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/example/dfs/datanode"
	"github.com/example/dfs/gateway"
	"github.com/example/dfs/internal/tlsutil"
	"github.com/example/dfs/metadata"
)

// ChunkStore is the data path used by the S3 gateway to read and write
// chunk payloads. It is a type alias for gateway.ChunkStore, defined
// here so existing callers that reference s3.ChunkStore keep working
// after the interface was lifted into the parent gateway package.
//
// Production uses DatanodeChunkStore; tests can inject
// MemoryChunkStore to avoid requiring a real datanode.
type ChunkStore = gateway.ChunkStore

// Compile-time guards: the two implementations below must satisfy
// both gateway.ChunkStore (the canonical interface) and the
// s3.ChunkStore alias (the legacy name).
var (
	_ ChunkStore         = (*DatanodeChunkStore)(nil)
	_ ChunkStore         = (*MemoryChunkStore)(nil)
	_ gateway.ChunkStore = (*DatanodeChunkStore)(nil)
	_ gateway.ChunkStore = (*MemoryChunkStore)(nil)
)

// DatanodeChunkStore is the production ChunkStore implementation: it
// talks to datanode daemons over TCP using a connection pool for
// efficient reuse and a write pipeline for parallel replication.
type DatanodeChunkStore struct {
	pool     *datanode.ClientPool
	pipeline *datanode.WritePipeline
	tlsCfg   tlsutil.Config
	// MinReplicasPerWrite is the floor on the number of replicas that
	// must acknowledge a write. <= 0 (the default) means "all replicas
	// must succeed", which is the production durability contract. Set
	// to 1 in tests that bring up a single-node cluster.
	MinReplicasPerWrite int
}

// NewDatanodeChunkStore returns a ChunkStore that dials datanode daemons
// over TCP using a connection pool. The pool reuses connections across
// operations, eliminating per-request TCP handshake overhead.
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

// WriteChunk writes data to every replica using the write pipeline.
// The write is considered successful only when at least
// s.requiredReplicas(chunk) replicas return OK; for production
// deployments that is "all of N". The pipeline dispatches writes
// to all replicas in parallel, reducing latency from O(N*RTT) to O(RTT).
func (s *DatanodeChunkStore) WriteChunk(ctx context.Context, chunk *metadata.ChunkMeta, data []byte) error {
	if chunk == nil {
		return fmt.Errorf("chunkstore: nil chunk")
	}
	if len(chunk.Replicas) == 0 {
		return fmt.Errorf("chunkstore: chunk %d has no replicas", chunk.ID)
	}

	required := s.requiredReplicas(len(chunk.Replicas))
	pp := datanode.NewWritePipeline(s.pool, 30*time.Second, datanode.WithQuorum(required))
	return pp.Write(ctx, chunk.ID, data, chunk.Replicas)
}

// requiredReplicas maps MinReplicasPerWrite to the actual floor for
// this chunk. A non-positive value is "all replicas".
func (s *DatanodeChunkStore) requiredReplicas(total int) int {
	if s.MinReplicasPerWrite <= 0 {
		return total
	}
	if s.MinReplicasPerWrite > total {
		return total
	}
	return s.MinReplicasPerWrite
}

// ReadChunk reads chunk data from the first replica that responds OK.
func (s *DatanodeChunkStore) ReadChunk(ctx context.Context, chunk *metadata.ChunkMeta) ([]byte, error) {
	return s.ReadChunkRange(ctx, chunk, 0, 0)
}

// ReadChunkRange reads a subrange [offset, offset+length) from the chunk
// via the datanode TCP protocol. offset=0, length=0 reads the entire chunk.
// Connections are sourced from the pool and returned after use.
func (s *DatanodeChunkStore) ReadChunkRange(ctx context.Context, chunk *metadata.ChunkMeta, offset int64, length int32) ([]byte, error) {
	if chunk == nil {
		return nil, fmt.Errorf("chunkstore: nil chunk")
	}
	if len(chunk.Replicas) == 0 {
		return nil, fmt.Errorf("chunkstore: chunk %d has no replicas", chunk.ID)
	}

	var lastErr error
	for _, rep := range chunk.Replicas {
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
			log.Printf("s3gw: connect to %s: %v", rep.Addr, err)
			continue
		}
		// Read the specified range; offset=0, length=0 means full chunk.
		resp, err := client.ReadChunk(chunk.ID, offset, length)
		s.pool.Put(rep.Addr, client)
		if err != nil {
			lastErr = fmt.Errorf("read from %s: %w", rep.Addr, err)
			log.Printf("s3gw: read from %s: %v", rep.Addr, err)
			continue
		}
		if resp.Status != datanode.StatusOK {
			lastErr = fmt.Errorf("datanode %s status=%d: %s", rep.Addr, resp.Status, resp.Error)
			continue
		}
		return resp.Data, nil
	}
	if lastErr != nil {
		return nil, fmt.Errorf("chunkstore: all replicas failed, last error: %w", lastErr)
	}
	return nil, fmt.Errorf("chunkstore: all replicas failed")
}

// MemoryChunkStore is an in-process ChunkStore used by tests. It keeps
// chunk payloads in a map and does not require any datanode.
type MemoryChunkStore struct {
	mu     sync.RWMutex
	chunks map[metadata.ChunkID][]byte
	// WriteHook, if set, is called on every WriteChunk. Tests can use
	// this to assert or simulate failures.
	WriteHook func(chunkID metadata.ChunkID, data []byte) error
	// ReadHook, if set, is called on every ReadChunk.
	ReadHook func(chunkID metadata.ChunkID) error
	// MinReplicasPerWrite mirrors DatanodeChunkStore.MinReplicasPerWrite
	// so the gateway can be tested with the same durability contract.
	// The MemoryChunkStore only ever has one logical copy, so the
	// meaningful values are 0 (default; "succeed" without checking,
	// matches old test behaviour) and 1 (require the write hook to
	// return nil).
	MinReplicasPerWrite int
}

// NewMemoryChunkStore returns an empty in-memory ChunkStore.
func NewMemoryChunkStore() *MemoryChunkStore {
	return &MemoryChunkStore{chunks: make(map[metadata.ChunkID][]byte)}
}

// WriteChunk stores data for the chunk, recording one copy per replica
// so a subsequent read on any replica succeeds. Hook errors are
// propagated. With MinReplicasPerWrite == 1 the hook must succeed for
// the call to be considered durable.
func (m *MemoryChunkStore) WriteChunk(_ context.Context, chunk *metadata.ChunkMeta, data []byte) error {
	if m.MinReplicasPerWrite > 0 && m.WriteHook == nil {
		return fmt.Errorf("chunkstore: MinReplicasPerWrite=1 requires a non-nil WriteHook")
	}
	if m.WriteHook != nil {
		if err := m.WriteHook(chunk.ID, data); err != nil {
			return err
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	// Copy the data so subsequent caller mutations don't affect us.
	buf := make([]byte, len(data))
	copy(buf, data)
	m.chunks[chunk.ID] = buf
	return nil
}

// ReadChunk returns the previously written payload for chunk.ID.
func (m *MemoryChunkStore) ReadChunk(ctx context.Context, chunk *metadata.ChunkMeta) ([]byte, error) {
	return m.ReadChunkRange(ctx, chunk, 0, 0)
}

// ReadChunkRange reads a subrange from the in-memory chunk data.
func (m *MemoryChunkStore) ReadChunkRange(_ context.Context, chunk *metadata.ChunkMeta, offset int64, length int32) ([]byte, error) {
	if m.ReadHook != nil {
		if err := m.ReadHook(chunk.ID); err != nil {
			return nil, err
		}
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	data, ok := m.chunks[chunk.ID]
	if !ok {
		return nil, fmt.Errorf("chunkstore: chunk %d not found", chunk.ID)
	}
	if length <= 0 || offset < 0 || offset+int64(length) > int64(len(data)) {
		// Return full data
		out := make([]byte, len(data))
		copy(out, data)
		return out, nil
	}
	out := make([]byte, length)
	copy(out, data[offset:offset+int64(length)])
	return out, nil
}

// Get returns the stored payload for a chunk (used by tests to verify
// what was written).
func (m *MemoryChunkStore) Get(chunkID metadata.ChunkID) ([]byte, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	data, ok := m.chunks[chunkID]
	if !ok {
		return nil, false
	}
	out := make([]byte, len(data))
	copy(out, data)
	return out, true
}

// dialClient creates a datanode.Client connected to the given address,
// using TLS when configured. DEPRECATED: use s.pool.Get instead.
func (s *DatanodeChunkStore) dialClient(addr string) (*datanode.Client, error) {
	return s.pool.Get(addr)
}
