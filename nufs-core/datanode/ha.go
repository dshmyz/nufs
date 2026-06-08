package datanode

import (
	"context"
	"fmt"
	"hash/crc32"
	"log"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/example/dfs/metadata"
)

// ============================================================
// Chain Replication — Synchronous write pipeline
// ============================================================
//
// Write path (sync replication):
//   Client → Head Node → Node2 → Node3 (Tail) → ACK
//
// Read path (nearest replica):
//   Client → nearest alive replica (Tail preferred for consistency)
//
// Failure handling:
//   - Head fails: client retries to new head (first alive in chain)
//   - Middle fails: chain bypasses dead node, continues
//   - Tail fails: previous node becomes new tail

// ReplicationChain represents an ordered list of replicas for a chunk.
type ReplicationChain struct {
	ChunkID metadata.ChunkID
	Nodes   []ChainNode // ordered: [head, ..., tail]
}

// ChainNode is a node in the replication chain.
type ChainNode struct {
	NodeID metadata.NodeID
	Addr   string
	State  ChainNodeState
}

// ChainNodeState represents the health of a chain node.
type ChainNodeState uint8

const (
	ChainAlive   ChainNodeState = iota // Node is healthy
	ChainSyncing                       // Replication in progress
	ChainFailed                        // Node is down/unreachable
)

// ParallelReplicator manages synchronous chain replication.
type ParallelReplicator struct {
	localAddr   string
	localID     metadata.NodeID
	timeout     time.Duration
	localWriter func(chunkID metadata.ChunkID, data []byte) error

	// Active chains per chunk
	mu     sync.RWMutex
	chains map[metadata.ChunkID]*ReplicationChain

	// Stats
	writeCount   atomic.Int64
	writeErrors  atomic.Int64
	writeLatency atomic.Int64 // microseconds
}

// NewParallelReplicator creates a chain replication manager.
func NewParallelReplicator(localAddr string, localID metadata.NodeID, timeout time.Duration, opts ...func(*ParallelReplicatorConfig)) *ParallelReplicator {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	cfg := &ParallelReplicatorConfig{}
	for _, opt := range opts {
		opt(cfg)
	}
	return &ParallelReplicator{
		localAddr:   localAddr,
		localID:     localID,
		timeout:     timeout,
		chains:      make(map[metadata.ChunkID]*ReplicationChain),
		localWriter: cfg.LocalWriter,
	}
}

// BuildChain creates a replication chain for a chunk.
func (cr *ParallelReplicator) BuildChain(chunkID metadata.ChunkID, replicas []metadata.ReplicaInfo) *ReplicationChain {
	chain := &ReplicationChain{
		ChunkID: chunkID,
		Nodes:   make([]ChainNode, 0, len(replicas)),
	}

	// Order: local node first (head), then by NodeID for determinism
	sort.Slice(replicas, func(i, j int) bool {
		if replicas[i].NodeID == cr.localID {
			return true
		}
		if replicas[j].NodeID == cr.localID {
			return false
		}
		return replicas[i].NodeID < replicas[j].NodeID
	})

	for _, r := range replicas {
		chain.Nodes = append(chain.Nodes, ChainNode{
			NodeID: r.NodeID,
			Addr:   r.Addr,
			State:  ChainAlive,
		})
	}

	cr.mu.Lock()
	cr.chains[chunkID] = chain
	cr.mu.Unlock()

	return chain
}

// Head returns the head node of the chain (first alive).
func (cr *ParallelReplicator) Head(chunkID metadata.ChunkID) *ChainNode {
	cr.mu.RLock()
	defer cr.mu.RUnlock()

	chain, ok := cr.chains[chunkID]
	if !ok {
		return nil
	}
	for i := range chain.Nodes {
		if chain.Nodes[i].State != ChainFailed {
			return &chain.Nodes[i]
		}
	}
	return nil
}

// Tail returns the tail node (last alive, most consistent).
func (cr *ParallelReplicator) Tail(chunkID metadata.ChunkID) *ChainNode {
	cr.mu.RLock()
	defer cr.mu.RUnlock()

	chain, ok := cr.chains[chunkID]
	if !ok {
		return nil
	}
	for i := len(chain.Nodes) - 1; i >= 0; i-- {
		if chain.Nodes[i].State != ChainFailed {
			return &chain.Nodes[i]
		}
	}
	return nil
}

// AliveReplicas returns all non-failed replicas.
func (cr *ParallelReplicator) AliveReplicas(chunkID metadata.ChunkID) []ChainNode {
	cr.mu.RLock()
	defer cr.mu.RUnlock()

	chain, ok := cr.chains[chunkID]
	if !ok {
		return nil
	}
	var alive []ChainNode
	for _, n := range chain.Nodes {
		if n.State != ChainFailed {
			alive = append(alive, n)
		}
	}
	return alive
}

// MarkFailed marks a node as failed in the chain.
func (cr *ParallelReplicator) MarkFailed(chunkID metadata.ChunkID, nodeID metadata.NodeID) {
	cr.mu.Lock()
	defer cr.mu.Unlock()

	chain, ok := cr.chains[chunkID]
	if !ok {
		return
	}
	for i := range chain.Nodes {
		if chain.Nodes[i].NodeID == nodeID {
			chain.Nodes[i].State = ChainFailed
			log.Printf("chain: chunk %d node %d marked failed", chunkID, nodeID)
			break
		}
	}
}

// Stats returns replication statistics.
func (cr *ParallelReplicator) Stats() (writes, errors int64, avgLatencyUs int64) {
	w := cr.writeCount.Load()
	e := cr.writeErrors.Load()
	l := cr.writeLatency.Load()
	if w > 0 {
		return w, e, l / w
	}
	return 0, e, 0
}

// WriteToChain performs a synchronous chain-replicated write.
// Returns the response from the tail node, which is the most consistent.
func (cr *ParallelReplicator) WriteToChain(ctx context.Context, chunkID metadata.ChunkID, data []byte) (*Response, error) {
	chain := cr.Head(chunkID)
	if chain == nil {
		return nil, fmt.Errorf("chain for chunk %d has no alive head", chunkID)
	}

	// Propagate write through the chain: Head -> Middle -> Tail
	nodes := cr.AliveReplicas(chunkID)
	if len(nodes) == 0 {
		return nil, fmt.Errorf("chain for chunk %d has no alive replicas", chunkID)
	}

	start := time.Now()
	var lastResp *Response
	var lastErr error

	for i := 0; i < len(nodes); i++ {
		node := nodes[i]
		if node.Addr == cr.localAddr {
			// Write locally
			if cr.localWriter != nil {
				if err := cr.localWriter(chunkID, data); err != nil {
					log.Printf("chain: local write chunk %d failed: %v", chunkID, err)
					cr.MarkFailed(chunkID, node.NodeID)
					return nil, fmt.Errorf("local write: %w", err)
				}
			}
			continue
		}

		client := NewClient(node.Addr)
		if err := client.Connect(); err != nil {
			log.Printf("chain: connect to %s for chunk %d failed: %v", node.Addr, chunkID, err)
			cr.MarkFailed(chunkID, node.NodeID)
			lastErr = fmt.Errorf("connect %s: %w", node.Addr, err)
			continue
		}

		resp, err := client.ReplicateChunk(chunkID, data)
		client.Close()
		if err != nil {
			log.Printf("chain: write to %s for chunk %d failed: %v", node.Addr, chunkID, err)
			cr.MarkFailed(chunkID, node.NodeID)
			lastErr = fmt.Errorf("write %s: %w", node.Addr, err)
			continue
		}
		lastResp = resp
	}

	cr.writeCount.Add(1)
	cr.writeLatency.Add(time.Since(start).Microseconds())

	if lastResp == nil && nodes[len(nodes)-1].Addr == cr.localAddr {
		// All remote replicas failed, but local write succeeded
		cr.writeErrors.Add(int64(len(nodes) - 1))
		return &Response{Status: ResponseStatus(1), Data: data}, nil
	}

	if lastResp == nil && lastErr != nil {
		cr.writeErrors.Add(int64(len(nodes)))
		return nil, fmt.Errorf("chain write failed: %w", lastErr)
	}

	return lastResp, nil
}

// ParallelReplicatorConfig holds configuration for chain replication.
type ParallelReplicatorConfig struct {
	LocalWriter func(chunkID metadata.ChunkID, data []byte) error
}

// WithLocalWriter sets the local write function for chain replication.
func WithLocalWriter(writer func(chunkID metadata.ChunkID, data []byte) error) func(*ParallelReplicatorConfig) {
	return func(cfg *ParallelReplicatorConfig) {
		cfg.LocalWriter = writer
	}
}





// ============================================================
// Minimal Interfaces — Interface Segregation for Datanode
// ============================================================
//
// Each component only depends on the metadata methods it actually
// uses, reducing coupling and making testing easier.

// HeartbeatMeta is the minimal metadata interface for HeartbeatReporter.
type HeartbeatMeta interface {
	Heartbeat(ctx context.Context, nodeID metadata.NodeID, report *metadata.NodeReport) error
}

// AntiEntropyMeta is the minimal metadata interface for AntiEntropy.
type AntiEntropyMeta interface {
	GetChunk(ctx context.Context, id metadata.ChunkID) (*metadata.ChunkMeta, error)
	ReportChunkState(ctx context.Context, nodeID metadata.NodeID, states map[metadata.ChunkID]metadata.ReplicaState) error
	TriggerRepair(ctx context.Context, chunkID metadata.ChunkID) error
}

// RepairMeta is the minimal metadata interface for RepairWorker.
type RepairMeta interface {
	GetChunk(ctx context.Context, id metadata.ChunkID) (*metadata.ChunkMeta, error)
	UpdateChunk(ctx context.Context, chunk *metadata.ChunkMeta) error
	GetRepairQueue(ctx context.Context) ([]metadata.RepairTask, error)
	RemoveRepairTask(ctx context.Context, chunkID metadata.ChunkID) error
	ReportChunkState(ctx context.Context, nodeID metadata.NodeID, states map[metadata.ChunkID]metadata.ReplicaState) error
	ListNodes(ctx context.Context) ([]metadata.NodeInfo, error)
	ChunksByNode(ctx context.Context, nodeID metadata.NodeID) ([]metadata.ChunkMeta, error)
	TriggerRepair(ctx context.Context, chunkID metadata.ChunkID) error
}

// Compile-time interface checks: PebbleStore satisfies all minimal interfaces.
var (
	_ HeartbeatMeta   = (*metadata.PebbleStore)(nil)
	_ AntiEntropyMeta = (*metadata.PebbleStore)(nil)
	_ RepairMeta      = (*metadata.PebbleStore)(nil)
)

// ============================================================
// Nearest-Replica Read — Read from closest alive replica
// ============================================================

// ReadStrategy determines which replica to read from.
type ReadStrategy struct {
	localAddr string
	// Prefer tail for strong consistency (tail has all committed writes)
	// Prefer nearest for low latency
	PreferConsistency bool
}

// NewReadStrategy creates a read routing strategy.
func NewReadStrategy(localAddr string, preferConsistency bool) *ReadStrategy {
	return &ReadStrategy{
		localAddr:         localAddr,
		PreferConsistency: preferConsistency,
	}
}

// SelectReplica picks the best replica for a read request.
func (rs *ReadStrategy) SelectReplica(chain *ReplicationChain) *ChainNode {
	if chain == nil || len(chain.Nodes) == 0 {
		return nil
	}

	if rs.PreferConsistency {
		// Tail has the most up-to-date data
		for i := len(chain.Nodes) - 1; i >= 0; i-- {
			if chain.Nodes[i].State != ChainFailed {
				return &chain.Nodes[i]
			}
		}
		return nil
	}

	// Prefer local node (zero network latency)
	for i := range chain.Nodes {
		if chain.Nodes[i].Addr == rs.localAddr && chain.Nodes[i].State != ChainFailed {
			return &chain.Nodes[i]
		}
	}

	// Fall back to first alive replica
	for i := range chain.Nodes {
		if chain.Nodes[i].State != ChainFailed {
			return &chain.Nodes[i]
		}
	}
	return nil
}

// ============================================================
// Anti-Entropy — Background consistency repair
// ============================================================

// AntiEntropy periodically compares chunk checksums between replicas
// and triggers repair when inconsistencies are detected.
type AntiEntropy struct {
	store   *ChunkStore
	meta    AntiEntropyMeta
	localID metadata.NodeID
	stopCh  chan struct{}
	running atomic.Bool
	wg      sync.WaitGroup // WaitGroup for graceful goroutine shutdown

	// Stats
	scanned    atomic.Int64
	mismatches atomic.Int64
	repaired   atomic.Int64
}

// NewAntiEntropy creates an anti-entropy repair engine.
func NewAntiEntropy(store *ChunkStore, meta AntiEntropyMeta, localID metadata.NodeID) *AntiEntropy {
	return &AntiEntropy{
		store:   store,
		meta:    meta,
		localID: localID,
		stopCh:  make(chan struct{}),
	}
}

// ScrubResult holds the result of an anti-entropy pass.
type AntiEntropyResult struct {
	ChunksScanned int
	Mismatches    int
	Repaired      int
	Duration      time.Duration
}

// Scan performs one anti-entropy pass: compare local chunks with metadata.
func (ae *AntiEntropy) Scan(ctx context.Context) (*AntiEntropyResult, error) {
	start := time.Now()
	result := &AntiEntropyResult{}

	ae.store.mu.RLock()
	localChunks := make(map[metadata.ChunkID]*LocalChunkInfo, len(ae.store.chunks))
	for k, v := range ae.store.chunks {
		localChunks[k] = v
	}
	ae.store.mu.RUnlock()

	for chunkID, local := range localChunks {
		if ctx.Err() != nil {
			break
		}
		result.ChunksScanned++
		ae.scanned.Add(1)

		// Get authoritative metadata
		meta, err := ae.meta.GetChunk(ctx, chunkID)
		if err != nil {
			continue // chunk might have been deleted
		}

		// Verify: local checksum matches metadata checksum
		if meta.Checksum != 0 && local.Checksum != meta.Checksum {
			result.Mismatches++
			ae.mismatches.Add(1)
			log.Printf("anti-entropy: chunk %d checksum mismatch: local=0x%08x meta=0x%08x",
				chunkID, local.Checksum, meta.Checksum)

			// Trigger repair from a healthy replica
			if err := ae.repairFromPeer(ctx, chunkID, meta); err != nil {
				log.Printf("anti-entropy: repair chunk %d failed: %v", chunkID, err)
			} else {
				result.Repaired++
				ae.repaired.Add(1)
			}
		}
	}

	result.Duration = time.Since(start)
	return result, nil
}

func (ae *AntiEntropy) repairFromPeer(ctx context.Context, chunkID metadata.ChunkID, meta *metadata.ChunkMeta) error {
	// Find a healthy peer (not self) and fetch chunk data via TCP
	for _, r := range meta.Replicas {
		if r.NodeID == ae.localID {
			continue
		}
		if r.State != metadata.ReplicaReady && r.State != metadata.ReplicaSyncing {
			continue
		}

		log.Printf("anti-entropy: repairing chunk %d from node %d (%s)", chunkID, r.NodeID, r.Addr)

		err := ae.fetchAndRepairLocal(chunkID, r.Addr)
		if err != nil {
			log.Printf("anti-entropy: repair chunk %d from %s failed: %v, trying next peer", chunkID, r.Addr, err)
			continue
		}

		// Report local replica as ready to metadata
		if stateErr := ae.meta.ReportChunkState(ctx, ae.localID, map[metadata.ChunkID]metadata.ReplicaState{
			chunkID: metadata.ReplicaReady,
		}); stateErr != nil {
			log.Printf("anti-entropy: failed to report chunk %d state: %v", chunkID, stateErr)
		}

		log.Printf("anti-entropy: chunk %d repaired successfully from node %d", chunkID, r.NodeID)
		return nil
	}

	// No healthy peer available — fall back to metadata-level repair queue
	log.Printf("anti-entropy: no healthy peer for chunk %d, queuing metadata repair", chunkID)
	return ae.meta.TriggerRepair(ctx, chunkID)
}

// fetchAndRepairLocal reads a chunk from a remote peer via TCP and overwrites the local copy.
func (ae *AntiEntropy) fetchAndRepairLocal(chunkID metadata.ChunkID, peerAddr string) error {
	client := NewClient(peerAddr)
	if err := client.Connect(); err != nil {
		return fmt.Errorf("connect %s: %w", peerAddr, err)
	}
	defer client.Close()

	resp, err := client.ReadChunk(chunkID, 0, 0)
	if err != nil {
		return fmt.Errorf("read from %s: %w", peerAddr, err)
	}
	if resp.Status != StatusOK {
		return fmt.Errorf("read from %s: status %d", peerAddr, resp.Status)
	}

	if err := ae.store.Write(chunkID, resp.Data); err != nil {
		return fmt.Errorf("local write: %w", err)
	}
	if _, err := ae.store.Seal(chunkID); err != nil {
		return fmt.Errorf("local seal: %w", err)
	}
	return nil
}

// Start runs anti-entropy periodically.
func (ae *AntiEntropy) Start(interval time.Duration) {
	if ae.running.Swap(true) {
		return
	}
	ae.wg.Add(1)
	go func() {
		defer ae.wg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				result, err := ae.Scan(context.Background())
				if err != nil {
					log.Printf("anti-entropy: scan error: %v", err)
				} else if result.Mismatches > 0 {
					log.Printf("anti-entropy: scanned=%d mismatches=%d repaired=%d in %v",
						result.ChunksScanned, result.Mismatches, result.Repaired, result.Duration)
				}
			case <-ae.stopCh:
				return
			}
		}
	}()
}

// Stop terminates anti-entropy and waits for the goroutine to exit.
func (ae *AntiEntropy) Stop() {
	if ae.running.Swap(false) {
		close(ae.stopCh)
	}
	ae.wg.Wait()
}

// Stats returns cumulative anti-entropy statistics.
func (ae *AntiEntropy) Stats() (scanned, mismatches, repaired int64) {
	return ae.scanned.Load(), ae.mismatches.Load(), ae.repaired.Load()
}

// ============================================================
// CRC32C verification for data integrity
// ============================================================

// VerifyChunkData reads a chunk from disk and verifies its checksum.
func (cs *ChunkStore) VerifyChunkData(chunkID metadata.ChunkID) (bool, uint32, error) {
	cs.mu.RLock()
	info, ok := cs.chunks[chunkID]
	cs.mu.RUnlock()
	if !ok {
		return false, 0, fmt.Errorf("chunk %d not found locally", chunkID)
	}

	data, _, err := cs.Read(chunkID, 0, int32(info.Size))
	if err != nil {
		return false, 0, err
	}

	computed := crc32.ChecksumIEEE(data)
	return computed == info.Checksum, computed, nil
}
