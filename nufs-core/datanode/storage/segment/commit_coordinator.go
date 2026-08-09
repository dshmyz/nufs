package segment

import (
	"sync"
	"time"

	"github.com/dshmyz/nufs/nufs-core/datanode/storage"
	"github.com/dshmyz/nufs/nufs-core/datanode/storage/index"
	"github.com/dshmyz/nufs/nufs-core/datanode/storage/journal"
)

// groupCommitConfig holds the §6.4 batch limits.
type groupCommitConfig struct {
	// MaxBatch is the maximum requests per batch (256).
	MaxBatch int
	// MaxWait is the batch close timeout (2ms).
	MaxWait time.Duration
	// beforeWait is a package-private test hook invoked immediately
	// before the coordinator waits for a follower or timeout.
	beforeWait func()
	// afterWake is a package-private test hook invoked after the batch
	// timer makes its wake-up available to the coordinator loop.
	afterWake func()
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
	// publishedValue overrides the normal durable-record location for
	// tombstones, which retain the prior location while changing state.
	publishedValue *index.Value

	// Set by the coordinator.
	segID     storage.SegmentID
	offset    int64
	streamSeq uint64
	// recoveryBatch coordinates publication of every member of one durable
	// BatchCommit into the overlay before that batch can advance SafeSeq.
	recoveryBatch *recoveryPublishBatch
	err           error
	done          chan struct{}
}

type commitRequest struct {
	write  *pendingWrite
	commit func([]*pendingWrite) error
}

// groupCommitCoordinator batches writes to one active segment and
// issues ONE fdatasync per batch (§6.4). A dedicated goroutine owns batch
// collection and its timer; submitters only enqueue and await their own
// completion channel.
//
// Correctness invariants:
//
//  1. A DurableReceipt is returned only after the batch sync completes.
//  2. A batch never spans two segment files: the commit callback seals
//     on overflow, and the batch is committed atomically before any
//     seal.
//  3. A sync failure is propagated to EVERY request in the batch.
type groupCommitCoordinator struct {
	cfg  groupCommitConfig
	reqs chan commitRequest
	stop chan struct{}
	done chan struct{}
	once sync.Once
}

func newGroupCommitCoordinator(cfg groupCommitConfig) *groupCommitCoordinator {
	if cfg.MaxBatch <= 0 {
		cfg.MaxBatch = 256
	}
	if cfg.MaxWait <= 0 {
		cfg.MaxWait = 2 * time.Millisecond
	}
	c := &groupCommitCoordinator{
		cfg:  cfg,
		reqs: make(chan commitRequest),
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}
	go c.loop()
	return c
}

// finish wakes a pending write with its result. The coordinator loop owns
// accepted requests and calls finish exactly once for each one.
func (pw *pendingWrite) finish(err error) {
	pw.err = err
	close(pw.done)
}

// Submit enqueues a pending write and waits for the coordinator to complete
// its batch. Once the request is accepted, the coordinator always finishes
// it exactly once, including when shutdown races with batch collection.
func (c *groupCommitCoordinator) Submit(pw *pendingWrite, commit func(batch []*pendingWrite) error) error {
	pw.done = make(chan struct{})
	select {
	case c.reqs <- commitRequest{write: pw, commit: commit}:
	case <-c.stop:
		return storage.ErrCapacity
	}
	<-pw.done
	return pw.err
}

func (c *groupCommitCoordinator) loop() {
	defer close(c.done)
	for {
		select {
		case <-c.stop:
			return
		default:
		}

		select {
		case <-c.stop:
			return
		case first := <-c.reqs:
			if c.stopped() {
				first.write.finish(storage.ErrCapacity)
				return
			}
			if !c.collectAndCommit(first) {
				return
			}
		}
	}
}

// collectAndCommit owns the timer and all accepted requests in one batch.
// It returns false when shutdown interrupts collection.
func (c *groupCommitCoordinator) collectAndCommit(first commitRequest) bool {
	requests := make([]commitRequest, 1, c.cfg.MaxBatch)
	requests[0] = first
	if c.cfg.MaxBatch > 1 {
		wake := make(chan struct{}, 1)
		timer := time.AfterFunc(c.cfg.MaxWait, func() {
			wake <- struct{}{}
			if c.cfg.afterWake != nil {
				c.cfg.afterWake()
			}
		})
		if c.cfg.beforeWait != nil {
			c.cfg.beforeWait()
		}

	collect:
		for len(requests) < c.cfg.MaxBatch {
			select {
			case <-c.stop:
				timer.Stop()
				for _, req := range requests {
					req.write.finish(storage.ErrCapacity)
				}
				return false
			case req := <-c.reqs:
				requests = append(requests, req)
			case <-wake:
				break collect
			}
		}
		timer.Stop()
	}

	batch := make([]*pendingWrite, len(requests))
	for i, req := range requests {
		batch[i] = req.write
	}
	err := first.commit(batch)
	for _, req := range requests {
		req.write.finish(err)
	}
	return true
}

func (c *groupCommitCoordinator) stopped() bool {
	select {
	case <-c.stop:
		return true
	default:
		return false
	}
}

// close shuts down the coordinator and waits until its loop has resolved
// every request it accepted.
func (c *groupCommitCoordinator) close() {
	c.once.Do(func() { close(c.stop) })
	<-c.done
}

var _ = journal.BatchCommitSize
