package datanode

import (
	"context"
	"fmt"
	"hash/crc32"
	"log/slog"
	"sync"
	"sync/atomic"
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

	// taskMu serializes channel sends (Submit) against the channel close in
	// Stop. A worker's retry AfterFunc can fire concurrently with Stop, and
	// close(taskCh) racing a chansend is a data race even when the send's
	// panic is recovered; the mutex makes the close/send pair atomic so
	// `-race` never observes them overlapping.
	taskMu sync.Mutex

	// Connection pool — avoids re-dialing for every replication task.
	// Each addr has a stack of idle *Client connections; workers pop
	// one off, use it, and push it back. If the stack is empty, a new
	// connection is dialed.
	pool        *connPool
	poolDialCount  atomic.Int64 // total dials (for testing/diagnostics)
	poolOpenConns  atomic.Int64 // currently open connections in pool
}

// connPool maintains per-address idle connection stacks.
type connPool struct {
	mu    sync.Mutex
	idle  map[string][]*Client // addr → idle connections (LIFO stack)
	tls   tlsutil.Config
	dials *atomic.Int64
	open  *atomic.Int64
}

func newConnPool(tls tlsutil.Config, dials, open *atomic.Int64) *connPool {
	return &connPool{
		idle:  make(map[string][]*Client),
		tls:   tls,
		dials: dials,
		open:  open,
	}
}

// get returns an idle connection for addr, dialing a new one if none idle.
func (p *connPool) get(addr string) (*Client, error) {
	p.mu.Lock()
	stack := p.idle[addr]
	if len(stack) > 0 {
		c := stack[len(stack)-1]
		p.idle[addr] = stack[:len(stack)-1]
		p.mu.Unlock()
		return c, nil
	}
	p.mu.Unlock()

	// Dial new connection
	p.dials.Add(1)
	c := NewClient(addr)
	if p.tls.Enabled() {
		tc, err := NewTLSClient(addr, p.tls)
		if err != nil {
			return nil, err
		}
		c = tc
	}
	if err := c.Connect(); err != nil {
		return nil, err
	}
	p.open.Add(1)
	return c, nil
}

// put returns a connection to the idle pool. If the connection is
// closed, it is discarded instead of recycled.
func (p *connPool) put(addr string, c *Client) {
	if c.IsClosed() {
		p.open.Add(-1)
		return
	}
	p.mu.Lock()
	p.idle[addr] = append(p.idle[addr], c)
	p.mu.Unlock()
}

// closeAll closes every idle connection and clears the pool.
func (p *connPool) closeAll() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for addr, stack := range p.idle {
		for _, c := range stack {
			c.Close()
			p.open.Add(-1)
		}
		delete(p.idle, addr)
	}
}

// NewReplicator creates a new replication engine with the specified concurrency.
func NewReplicator(localAddr string, workers int) *Replicator {
	if workers <= 0 {
		workers = 4
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &Replicator{
		localAddr: localAddr,
		taskCh:    make(chan ReplicationTask, 1024),
		workers:   workers,
		ctx:       ctx,
		cancel:    cancel,
	}
	r.pool = newConnPool(r.tlsCfg, &r.poolDialCount, &r.poolOpenConns)
	return r
}

// SetTLS configures TLS for inter-node replication connections.
func (r *Replicator) SetTLS(cfg tlsutil.Config) {
	r.tlsCfg = cfg
	// Recreate pool with updated TLS config
	r.pool = newConnPool(r.tlsCfg, &r.poolDialCount, &r.poolOpenConns)
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
	// Close under taskMu so a concurrent send from a retry AfterFunc cannot
	// race the close. Workers drain taskCh until it is closed, then exit.
	r.taskMu.Lock()
	close(r.taskCh)
	r.taskMu.Unlock()
	r.wg.Wait()
	r.pool.closeAll()
	slog.Info("datanode: replicator stopped")
}

// closeAllPooledConns closes all idle pooled connections (for testing).
func (r *Replicator) closeAllPooledConns() {
	r.pool.closeAll()
}

// Submit adds a replication task to the queue.
func (r *Replicator) Submit(task ReplicationTask) error {
	defer func() {
		if recover() != nil {
			// Channel closed, ignore
		}
	}()
	// Send under taskMu so we never race Stop's close. Blocking on a full
	// buffer with no reader would deadlock a worker's retry path, so keep
	// the send non-blocking exactly as before (a requeue that finds the
	// buffer full is dropped and logged by the caller).
	r.taskMu.Lock()
	defer r.taskMu.Unlock()
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
// 2. Throttle to background bandwidth limit (shared with anti-entropy and repair)
// 3. Write chunk to target node
// 4. Verify checksum
func (r *Replicator) replicate(task ReplicationTask) error {
	// Get source connection from pool
	srcClient, err := r.pool.get(task.SourceAddr)
	if err != nil {
		return fmt.Errorf("connect source: %w", err)
	}

	// Read chunk from source
	resp, err := srcClient.ReadChunk(task.ChunkID, 0, 0)
	if err != nil {
		// Connection may be broken — discard it
		srcClient.Close()
		r.poolOpenConns.Add(-1)
		return fmt.Errorf("read from source: %w", err)
	}
	// Return source connection to pool for reuse
	r.pool.put(task.SourceAddr, srcClient)

	if resp.Status != StatusOK {
		return fmt.Errorf("source read failed: %s", resp.Error)
	}

	// Throttle the background data copy so it doesn't starve
	// foreground client reads/writes. We back-pay since we only
	// know the data length after ReadChunk returns.
	if len(resp.Data) > 0 {
		if err := ThrottleRead(context.Background(), len(resp.Data)); err != nil {
			slog.Warn("replicator: bandwidth throttle cancelled",
				"chunkID", task.ChunkID, "bytes", len(resp.Data), "error", err)
		}
	}

	data := resp.Data
	checksum := crc32.ChecksumIEEE(data)

	// Get target connection from pool
	tgtClient, err := r.pool.get(task.TargetAddr)
	if err != nil {
		return fmt.Errorf("connect target: %w", err)
	}

	// Write chunk to target
	resp, err = tgtClient.ReplicateChunk(task.ChunkID, data)
	if err != nil {
		tgtClient.Close()
		r.poolOpenConns.Add(-1)
		return fmt.Errorf("write to target: %w", err)
	}
	// Return target connection to pool for reuse
	r.pool.put(task.TargetAddr, tgtClient)

	if resp.Status != StatusOK {
		return fmt.Errorf("target write failed: %s", resp.Error)
	}

	// Verify checksum
	if resp.Checksum != 0 && resp.Checksum != checksum {
		return fmt.Errorf("checksum mismatch: expected %d, got %d", checksum, resp.Checksum)
	}

	return nil
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
