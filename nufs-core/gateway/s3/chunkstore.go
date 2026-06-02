package s3

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/example/dfs/datanode"
	"github.com/example/dfs/metadata"
)

// ChunkStore is the data path used by the S3 gateway to read and write
// chunk payloads. Production uses DatanodeChunkStore; tests can inject
// MemoryChunkStore to avoid requiring a real datanode.
type ChunkStore interface {
	// WriteChunk writes the given data to all replicas for the chunk.
	// It returns nil only when at least minReplicas replicas have
	// acknowledged the write.
	WriteChunk(ctx context.Context, chunk *metadata.ChunkMeta, data []byte) error

	// ReadChunk reads the chunk payload from the first healthy replica.
	// The returned slice may be larger than the requested range; callers
	// are responsible for trimming.
	ReadChunk(ctx context.Context, chunk *metadata.ChunkMeta) ([]byte, error)
}

// DatanodeChunkStore is the production ChunkStore implementation: it
// talks to datanode daemons over TCP. The previous in-process behaviour
// (commit chunk without writing data) is replaced by an actual data path
// that goes through datanode.Client.
type DatanodeChunkStore struct {
	mu          sync.Mutex
	dialTimeout time.Duration
	// MinReplicasPerWrite is the floor on the number of replicas that
	// must acknowledge a write. <= 0 (the default) means "all replicas
	// must succeed", which is the production durability contract. Set
	// to 1 in tests that bring up a single-node cluster.
	MinReplicasPerWrite int
}

// NewDatanodeChunkStore returns a ChunkStore that dials datanode daemons
// over TCP. dialTimeout applies to the connect step; each request then
// uses datanode.Client's own per-request deadline.
func NewDatanodeChunkStore() *DatanodeChunkStore {
	return &DatanodeChunkStore{dialTimeout: 10 * time.Second}
}

// WriteChunk writes data to every replica. The write is considered
// successful only when at least s.requiredReplicas(chunk) replicas
// return OK; for production deployments that is "all of N" (i.e. a
// non-zero MinReplicasPerWrite means "at least this many, capped by
// the total"). The default (MinReplicasPerWrite == 0) means the write
// must reach every replica before it is considered durable.
func (s *DatanodeChunkStore) WriteChunk(ctx context.Context, chunk *metadata.ChunkMeta, data []byte) error {
	if chunk == nil {
		return fmt.Errorf("chunkstore: nil chunk")
	}
	if len(chunk.Replicas) == 0 {
		return fmt.Errorf("chunkstore: chunk %d has no replicas", chunk.ID)
	}

	required := s.requiredReplicas(len(chunk.Replicas))
	successes := 0
	var lastErr error
	for _, rep := range chunk.Replicas {
		if rep.Addr == "" {
			lastErr = fmt.Errorf("replica on node %d has empty addr", rep.NodeID)
			log.Printf("s3gw: skip replica on node %d: empty addr", rep.NodeID)
			continue
		}
		if err := ctx.Err(); err != nil {
			return err
		}

		client := datanode.NewClient(rep.Addr)
		client.SetTimeout(30 * time.Second)
		if err := client.Connect(); err != nil {
			lastErr = fmt.Errorf("connect to %s: %w", rep.Addr, err)
			log.Printf("s3gw: connect to datanode %s: %v", rep.Addr, err)
			continue
		}
		resp, err := client.WriteChunk(chunk.ID, data)
		client.Close()
		if err != nil {
			lastErr = fmt.Errorf("write to %s: %w", rep.Addr, err)
			log.Printf("s3gw: write to datanode %s: %v", rep.Addr, err)
			continue
		}
		if resp.Status != datanode.StatusOK {
			lastErr = fmt.Errorf("datanode %s status=%d: %s", rep.Addr, resp.Status, resp.Error)
			log.Printf("s3gw: datanode %s returned status %d: %s", rep.Addr, resp.Status, resp.Error)
			continue
		}
		successes++
	}

	if successes < required {
		if lastErr != nil {
			return fmt.Errorf("chunkstore: only %d of %d required replicas wrote chunk %d, last error: %w",
				successes, required, chunk.ID, lastErr)
		}
		return fmt.Errorf("chunkstore: only %d of %d required replicas wrote chunk %d",
			successes, required, chunk.ID)
	}
	return nil
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

		client := datanode.NewClient(rep.Addr)
		client.SetTimeout(30 * time.Second)
		if err := client.Connect(); err != nil {
			lastErr = fmt.Errorf("connect to %s: %w", rep.Addr, err)
			log.Printf("s3gw: connect to %s: %v", rep.Addr, err)
			continue
		}
		// Read the full chunk; callers trim to the requested range.
		resp, err := client.ReadChunk(chunk.ID, 0, 0)
		client.Close()
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
func (m *MemoryChunkStore) ReadChunk(_ context.Context, chunk *metadata.ChunkMeta) ([]byte, error) {
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
	out := make([]byte, len(data))
	copy(out, data)
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
