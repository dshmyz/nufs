package datanode

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// ============================================================
// WritePipeline — parallel chunk replication
// ============================================================
//
// Instead of the serial chain write (Head → Middle → Tail),
// WritePipeline dispatches writes to all replicas concurrently.
// This reduces latency from O(N * RTT) to O(RTT + max_slow_replica).
//
// Quorum controls durability: the write returns success once the
// quorum number of replicas acknowledge. Remaining replicas are
// best-effort.

// PipelineConfig holds configuration for WritePipeline.
type PipelineConfig struct {
	Quorum     int    // number of replicas that must succeed (0 = all)
	Generation uint64 // metadata-issued generation (0 = legacy local-gen)
}

// PipelineOption configures a PipelineConfig.
type PipelineOption func(*PipelineConfig)

// WithQuorum sets the minimum number of replicas that must succeed.
func WithQuorum(n int) PipelineOption {
	return func(cfg *PipelineConfig) {
		cfg.Quorum = n
	}
}

// WithGeneration sets the metadata-issued write generation carried to every
// replica (Metadata V2 fencing). 0 leaves each datanode to its own local
// generation (legacy behavior).
func WithGeneration(g uint64) PipelineOption {
	return func(cfg *PipelineConfig) {
		cfg.Generation = g
	}
}

// WritePipeline dispatches chunk writes to multiple replicas in parallel.
type WritePipeline struct {
	pool    *ClientPool
	timeout time.Duration
	quorum  int    // 0 means all replicas must succeed
	gen     uint64 // metadata-issued generation (0 = legacy)
}

// NewWritePipeline creates a write pipeline backed by the given connection pool.
func NewWritePipeline(pool *ClientPool, timeout time.Duration, opts ...PipelineOption) *WritePipeline {
	cfg := PipelineConfig{}
	for _, opt := range opts {
		opt(&cfg)
	}
	return &WritePipeline{
		pool:    pool,
		timeout: timeout,
		quorum:  cfg.Quorum,
		gen:     cfg.Generation,
	}
}

// Write sends data to all replicas concurrently and waits for quorum
// acknowledgements. If quorum == 0, all replicas must succeed.
func (pp *WritePipeline) Write(ctx context.Context, chunkID metadata.ChunkID, data []byte, replicas []metadata.ReplicaInfo) error {
	if len(replicas) == 0 {
		return fmt.Errorf("pipeline: no replicas for chunk %d", chunkID)
	}

	required := pp.quorum
	if required <= 0 {
		required = len(replicas)
	}
	if required > len(replicas) {
		required = len(replicas)
	}

	type result struct {
		nodeID metadata.NodeID
		err    error
	}

	results := make(chan result, len(replicas))

	for _, rep := range replicas {
		go func(r metadata.ReplicaInfo) {
			client, err := pp.pool.Get(r.Addr)
			if err != nil {
				results <- result{nodeID: r.NodeID, err: fmt.Errorf("connect %s: %w", r.Addr, err)}
				return
			}
			resp, err := client.ReplicateChunkGen(chunkID, pp.gen, data)
			pp.pool.Put(r.Addr, client)

			if err != nil {
				results <- result{nodeID: r.NodeID, err: fmt.Errorf("write %s: %w", r.Addr, err)}
				return
			}
			if resp.Status != StatusOK {
				results <- result{nodeID: r.NodeID, err: fmt.Errorf("node %d status=%d: %s", r.NodeID, resp.Status, resp.Error)}
				return
			}
			results <- result{nodeID: r.NodeID}
		}(rep)
	}

	successes := 0
	var lastErr error
	for i := 0; i < len(replicas); i++ {
		select {
		case r := <-results:
			if r.err != nil {
				lastErr = r.err
				slog.Warn("pipeline: replica write failed", "chunkID", chunkID, "nodeID", r.nodeID, "error", r.err)
			} else {
				successes++
			}
		case <-ctx.Done():
			return fmt.Errorf("pipeline: context cancelled: %w", ctx.Err())
		}
	}

	if successes < required {
		if lastErr != nil {
			return fmt.Errorf("pipeline: only %d/%d replicas succeeded for chunk %d: %w",
				successes, required, chunkID, lastErr)
		}
		return fmt.Errorf("pipeline: only %d/%d replicas succeeded for chunk %d",
			successes, required, chunkID)
	}

	return nil
}
