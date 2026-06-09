package datanode

import (
	"context"
	"fmt"
	"hash/crc32"
	"log/slog"
	"sync"
	"time"

	"github.com/example/dfs/internal/tlsutil"
	"github.com/example/dfs/metadata"
)

// ReplicationTask describes a pending chunk replication operation.
type ReplicationTask struct {
	ChunkID    metadata.ChunkID
	SourceAddr string // data node to read chunk from
	TargetAddr string // data node to replicate to
	Retries    int
	CreatedAt  time.Time

	// done is an optional 1-buffered channel signalled by the worker
	// once replicate(task) returns. nil means fire-and-forget.
	done chan error
}

// Replicator manages async chunk replication between data nodes.
type Replicator struct {
	localAddr string
	taskCh    chan ReplicationTask
	wg        sync.WaitGroup
	workers   int
	ctx       context.Context
	cancel    context.CancelFunc
	tlsCfg    tlsutil.Config // TLS config for inter-node connections
}

// NewReplicator creates a new replication engine with the specified concurrency.
func NewReplicator(localAddr string, workers int) *Replicator {
	if workers <= 0 {
		workers = 4
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Replicator{
		localAddr: localAddr,
		taskCh:    make(chan ReplicationTask, 1024),
		workers:   workers,
		ctx:       ctx,
		cancel:    cancel,
	}
}

// SetTLS configures TLS for inter-node replication connections.
func (r *Replicator) SetTLS(cfg tlsutil.Config) {
	r.tlsCfg = cfg
}

// Start launches replication worker goroutines.
func (r *Replicator) Start() {
	for i := 0; i < r.workers; i++ {
		r.wg.Add(1)
		go r.worker(i)
	}
	slog.Info("datanode: replicator started", "workers", r.workers)
}

// Stop gracefully shuts down the replicator.
func (r *Replicator) Stop() {
	r.cancel()
	close(r.taskCh)
	r.wg.Wait()
	slog.Info("datanode: replicator stopped")
}

// Submit adds a replication task to the queue.
func (r *Replicator) Submit(task ReplicationTask) error {
	defer func() {
		if recover() != nil {
			// Channel closed, ignore
		}
	}()
	select {
	case r.taskCh <- task:
		return nil
	case <-r.ctx.Done():
		return fmt.Errorf("datanode: replicator shut down")
	default:
		return fmt.Errorf("datanode: replication queue full")
	}
}

// SubmitWait enqueues a task and blocks until the worker reports the
// final result (or the timeout elapses, or the replicator is stopped).
// It is the synchronous counterpart of Submit; callers should use it
// when they need to know the copy actually completed before they
// update metadata or commit the chunk.
func (r *Replicator) SubmitWait(task ReplicationTask, timeout time.Duration) error {
	task.done = make(chan error, 1)
	if err := r.Submit(task); err != nil {
		return err
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case err := <-task.done:
		return err
	case <-timer.C:
		return fmt.Errorf("datanode: replication task for chunk %d timed out after %v", task.ChunkID, timeout)
	case <-r.ctx.Done():
		return fmt.Errorf("datanode: replicator shut down while waiting for chunk %d", task.ChunkID)
	}
}

// SubmitReplication creates replication tasks for a chunk to all target replicas.
func (r *Replicator) SubmitReplication(chunkID metadata.ChunkID, sourceAddr string, targets []metadata.ReplicaInfo) {
	for _, target := range targets {
		if target.Addr == sourceAddr {
			continue // skip self
		}
		task := ReplicationTask{
			ChunkID:    chunkID,
			SourceAddr: sourceAddr,
			TargetAddr: target.Addr,
			CreatedAt:  time.Now(),
		}
		if err := r.Submit(task); err != nil {
			slog.Warn("datanode: failed to submit replication", "chunkID", task.ChunkID, "target", task.TargetAddr, "error", err)
		}
	}
}

func (r *Replicator) worker(id int) {
	defer r.wg.Done()

	for task := range r.taskCh {
		select {
		case <-r.ctx.Done():
			if task.done != nil {
				task.done <- fmt.Errorf("datanode: replicator shut down")
			}
			return
		default:
		}

		err := r.replicate(task)
		if err != nil {
			slog.Error("datanode: replication failed",
				"worker", id, "chunkID", task.ChunkID, "source", task.SourceAddr, "target", task.TargetAddr, "error", err)

			// Retry with exponential backoff (max 3 retries). We only
			// retry fire-and-forget tasks; synchronous ones are
			// surfaced to the caller immediately so it can decide.
			if task.done == nil && task.Retries < 3 {
				task.Retries++
				backoff := time.Duration(1<<uint(task.Retries)) * time.Second
				time.AfterFunc(backoff, func() {
					_ = r.Submit(task)
				})
			} else if task.done == nil {
				slog.Warn("datanode: giving up on chunk after retries", "worker", id, "chunkID", task.ChunkID, "retries", task.Retries)
			}
		} else {
			slog.Info("datanode: replicated chunk", "worker", id, "chunkID", task.ChunkID, "target", task.TargetAddr)
		}

		// Signal any synchronous waiter exactly once. We deliver the
		// final result of this attempt, not the eventual retry result,
		// because the caller wants to know whether the copy landed.
		if task.done != nil {
			task.done <- err
		}
	}
}

// replicate performs the actual chunk replication:
// 1. Read chunk from source node
// 2. Write chunk to target node
// 3. Verify checksum
func (r *Replicator) replicate(task ReplicationTask) error {
	// Connect to source
	srcClient, err := r.dialClient(task.SourceAddr)
	if err != nil {
		return fmt.Errorf("connect source: %w", err)
	}
	defer srcClient.Close()

	// Read chunk from source
	resp, err := srcClient.ReadChunk(task.ChunkID, 0, 0)
	if err != nil {
		return fmt.Errorf("read from source: %w", err)
	}
	if resp.Status != StatusOK {
		return fmt.Errorf("source read failed: %s", resp.Error)
	}

	data := resp.Data
	checksum := crc32.ChecksumIEEE(data)

	// Connect to target
	tgtClient, err := r.dialClient(task.TargetAddr)
	if err != nil {
		return fmt.Errorf("connect target: %w", err)
	}
	defer tgtClient.Close()

	// Write chunk to target
	resp, err = tgtClient.ReplicateChunk(task.ChunkID, data)
	if err != nil {
		return fmt.Errorf("write to target: %w", err)
	}
	if resp.Status != StatusOK {
		return fmt.Errorf("target write failed: %s", resp.Error)
	}

	// Verify checksum
	if resp.Checksum != 0 && resp.Checksum != checksum {
		return fmt.Errorf("checksum mismatch: expected %d, got %d", checksum, resp.Checksum)
	}

	return nil
}

// dialClient creates a Client connected to the given address, using
// TLS when configured.
func (r *Replicator) dialClient(addr string) (*Client, error) {
	if r.tlsCfg.Enabled() {
		c, err := NewTLSClient(addr, r.tlsCfg)
		if err != nil {
			return nil, err
		}
		if err := c.Connect(); err != nil {
			return nil, err
		}
		return c, nil
	}
	c := NewClient(addr)
	if err := c.Connect(); err != nil {
		return nil, err
	}
	return c, nil
}

// RepairTask represents a chunk repair operation (re-replicate from surviving copy).
type ChunkRepairTask struct {
	ChunkID       metadata.ChunkID
	SurvivingAddr string // address of node with valid copy
	NewTargetAddr string // address of new replica node
}

// Repair initiates a chunk repair from a surviving replica.
func (r *Replicator) Repair(task ChunkRepairTask) error {
	return r.replicate(ReplicationTask{
		ChunkID:    task.ChunkID,
		SourceAddr: task.SurvivingAddr,
		TargetAddr: task.NewTargetAddr,
		CreatedAt:  time.Now(),
	})
}
