package segment

import (
	"sync"
	"time"

	"github.com/example/dfs/datanode/storage"
	"github.com/example/dfs/datanode/storage/journal"
)

// groupCommitConfig holds the §6.4 batch limits.
type groupCommitConfig struct {
	// MaxBatch is the maximum requests per batch (256).
	MaxBatch int
	// MaxWait is the batch close timeout (2ms).
	MaxWait time.Duration
}

func defaultGroupCommitConfig() groupCommitConfig {
	return groupCommitConfig{MaxBatch: 256, MaxWait: 2 * time.Millisecond}
}

// pendingWrite is one request submitted to the coordinator. The record
// is fully built by the submitter; the leader appends the single
// BatchCommit and does one sync for the whole batch.
type pendingWrite struct {
	extentID   storage.ExtentID
	generation storage.Generation
	header     *RecordHeader
	idxBuf     []byte
	stored     []byte
	frameSize  int
	storedLen  uint32
	logicalLen uint32
	payloadCRC uint32

	// Set by the coordinator.
	segID     storage.SegmentID
	offset    int64
	streamSeq uint64
	err       error
	done      chan struct{}
	doneOnce  sync.Once // guards done close (leader vs close() race)
}

// groupCommitCoordinator batches writes to one active segment and
// issues ONE fdatasync per batch (§6.4). It uses the leader-follower
// pattern: the first submitter becomes the batch leader, collects
// followers until MaxBatch or MaxWait, writes the single BatchCommit,
// syncs once, then wakes every request in the batch.
//
// Correctness invariants:
//
//  1. A DurableReceipt is returned only after the batch sync completes.
//  2. A batch never spans two segment files: the commit callback seals
//     on overflow, and the batch is committed atomically before any
//     seal.
//  3. A sync failure is propagated to EVERY request in the batch.
type groupCommitCoordinator struct {
	cfg groupCommitConfig

	mu       sync.Mutex
	cond     *sync.Cond
	pending  []*pendingWrite // followers awaiting the current leader
	leader   *pendingWrite   // non-nil while a batch is being led
	closed   bool
}

func newGroupCommitCoordinator(cfg groupCommitConfig) *groupCommitCoordinator {
	if cfg.MaxBatch <= 0 {
		cfg.MaxBatch = 256
	}
	if cfg.MaxWait <= 0 {
		cfg.MaxWait = 2 * time.Millisecond
	}
	c := &groupCommitCoordinator{cfg: cfg}
	c.cond = sync.NewCond(&c.mu)
	return c
}

// finish wakes a pending write with its result. Safe against a
// concurrent close() because it uses doneOnce.
func (pw *pendingWrite) finish(err error) {
	pw.err = err
	pw.doneOnce.Do(func() { close(pw.done) })
}

// Submit enqueues a pending write. If this writer is the batch leader it
// collects followers for up to MaxWait, commits (appending all records +
// one BatchCommit + one sync via the commit callback), and wakes
// everyone. If a leader already exists, this writer becomes a follower
// and waits.
func (c *groupCommitCoordinator) Submit(pw *pendingWrite, commit func(batch []*pendingWrite) error) error {
	pw.done = make(chan struct{})
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return storage.ErrCapacity
	}

	if c.leader == nil {
		// I am the leader. Take any already-pending followers.
		batch := append(c.pending, pw)
		c.pending = nil
		c.leader = pw
		deadline := time.Now().Add(c.cfg.MaxWait)

		// Collect followers until full or deadline.
		for {
			if len(batch) >= c.cfg.MaxBatch {
				break
			}
			remaining := time.Until(deadline)
			if remaining <= 0 {
				break
			}
			if len(c.pending) == 0 {
				waitFor := remaining
				if waitFor > time.Millisecond {
					waitFor = time.Millisecond // re-check to stay responsive
				}
				t := time.AfterFunc(waitFor, func() { c.cond.Broadcast() })
				c.cond.Wait()
				t.Stop()
			}
			// Absorb any new followers.
			batch = append(batch, c.pending...)
			c.pending = nil
		}

		c.leader = nil
		err := commit(batch)
		for _, p := range batch {
			p.finish(err)
		}
		c.cond.Broadcast()
		c.mu.Unlock()
		return err
	}

	// Follower: append to pending and wait for the leader to commit.
	c.pending = append(c.pending, pw)
	c.cond.Broadcast() // wake the leader
	c.mu.Unlock()
	<-pw.done
	return pw.err
}

// close shuts down the coordinator, waking any queued followers with an
// error. The in-flight leader (if any) finishes its own batch; the
// doneOnce guards prevent a double-close.
func (c *groupCommitCoordinator) close() {
	c.mu.Lock()
	c.closed = true
	for _, p := range c.pending {
		p.finish(storage.ErrCapacity)
	}
	c.pending = nil
	c.cond.Broadcast()
	c.mu.Unlock()
}

var _ = journal.BatchCommitSize
