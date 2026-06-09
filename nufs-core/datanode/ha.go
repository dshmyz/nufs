package datanode

import (
	"context"
	"fmt"
	"hash/crc32"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/example/dfs/internal/tlsutil"
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
	tlsCfg      tlsutil.Config

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

// SetTLS configures TLS for inter-node chain replication connections.
func (cr *ParallelReplicator) SetTLS(cfg tlsutil.Config) {
	cr.tlsCfg = cfg
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
			slog.Warn("chain: node marked failed", "chunkID", chunkID, "nodeID", nodeID)
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
					slog.Error("chain: local write failed", "chunkID", chunkID, "error", err)
					cr.MarkFailed(chunkID, node.NodeID)
					return nil, fmt.Errorf("local write: %w", err)
				}
			}
			continue
		}

		client, err := cr.dialClient(node.Addr)
		if err != nil {
			slog.Error("chain: connect failed", "addr", node.Addr, "chunkID", chunkID, "error", err)
			cr.MarkFailed(chunkID, node.NodeID)
			lastErr = fmt.Errorf("connect %s: %w", node.Addr, err)
			continue
		}

		resp, err := client.ReplicateChunk(chunkID, data)
		client.Close()
		if err != nil {
			slog.Error("chain: write failed", "addr", node.Addr, "chunkID", chunkID, "error", err)
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
	UpdateChunk(ctx context.Context, chunk *metadata.ChunkMeta) error
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
	tlsCfg  tlsutil.Config

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

// SetTLS configures TLS for inter-node anti-entropy connections.
func (ae *AntiEntropy) SetTLS(cfg tlsutil.Config) {
	ae.tlsCfg = cfg
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

		// Check 1: Find local replica state in metadata
		var localReplicaState metadata.ReplicaState
		var hasLocalReplica bool
		for _, r := range meta.Replicas {
			if r.NodeID == ae.localID {
				localReplicaState = r.State
				hasLocalReplica = true
				break
			}
		}

		// Check 2: Checksum mismatch — local data differs from metadata
		if meta.Checksum != 0 && local.Checksum != meta.Checksum {
			result.Mismatches++
			ae.mismatches.Add(1)
			slog.Warn("anti-entropy: checksum mismatch",
				"chunkID", chunkID, "local", fmt.Sprintf("0x%08x", local.Checksum), "meta", fmt.Sprintf("0x%08x", meta.Checksum))

			// Trigger repair from a healthy replica
			if err := ae.repairFromPeer(ctx, chunkID, meta); err != nil {
				slog.Error("anti-entropy: repair chunk failed", "chunkID", chunkID, "error", err)
			} else {
				result.Repaired++
				ae.repaired.Add(1)
			}
			continue
		}

		// Check 3: Local data is correct but metadata says replica is Stale/Failed
		// This means a previous repair wrote the data but didn't update the metadata state.
		if hasLocalReplica && (localReplicaState == metadata.ReplicaStale || localReplicaState == metadata.ReplicaFailed) {
			slog.Info("anti-entropy: local data correct but replica state is stale/failed, promoting to ready",
				"chunkID", chunkID, "currentState", localReplicaState)

			if stateErr := ae.meta.ReportChunkState(ctx, ae.localID, map[metadata.ChunkID]metadata.ReplicaState{
				chunkID: metadata.ReplicaReady,
			}); stateErr != nil {
				slog.Warn("anti-entropy: failed to promote replica state", "chunkID", chunkID, "error", stateErr)
			} else {
				result.Repaired++
				ae.repaired.Add(1)
			}
			continue
		}

		// Check 4: Local data is correct but metadata checksum is zero or outdated.
		// Sync the local checksum back to metadata so other replicas can verify.
		if meta.Checksum == 0 && local.Checksum != 0 {
			meta.Checksum = local.Checksum
			if updateErr := ae.meta.UpdateChunk(ctx, meta); updateErr != nil {
				slog.Warn("anti-entropy: failed to sync checksum to metadata", "chunkID", chunkID, "error", updateErr)
			} else {
				slog.Info("anti-entropy: synced checksum to metadata", "chunkID", chunkID, "checksum", fmt.Sprintf("0x%08x", local.Checksum))
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

		slog.Info("anti-entropy: repairing chunk from peer", "chunkID", chunkID, "nodeID", r.NodeID, "addr", r.Addr)

		err := ae.fetchAndRepairLocal(chunkID, r.Addr)
		if err != nil {
			slog.Warn("anti-entropy: repair from peer failed, trying next", "chunkID", chunkID, "addr", r.Addr, "error", err)
			continue
		}

		// After repair, sync the new checksum back to metadata.
		// The local Seal() recomputed the checksum, which may differ
		// from the stale value in metadata.
		ae.store.mu.RLock()
		localInfo, ok := ae.store.chunks[chunkID]
		ae.store.mu.RUnlock()
		if ok && localInfo.Checksum != 0 {
			meta.Checksum = localInfo.Checksum
			if updateErr := ae.meta.UpdateChunk(ctx, meta); updateErr != nil {
				slog.Warn("anti-entropy: failed to update chunk checksum after repair",
					"chunkID", chunkID, "error", updateErr)
			}
		}

		// Report local replica as ready to metadata
		if stateErr := ae.meta.ReportChunkState(ctx, ae.localID, map[metadata.ChunkID]metadata.ReplicaState{
			chunkID: metadata.ReplicaReady,
		}); stateErr != nil {
			slog.Warn("anti-entropy: failed to report chunk state", "chunkID", chunkID, "error", stateErr)
		}

		slog.Info("anti-entropy: chunk repaired successfully", "chunkID", chunkID, "nodeID", r.NodeID)
		return nil
	}

	// No healthy peer available — fall back to metadata-level repair queue
	slog.Warn("anti-entropy: no healthy peer for chunk, queuing metadata repair", "chunkID", chunkID)
	return ae.meta.TriggerRepair(ctx, chunkID)
}

// fetchAndRepairLocal reads a chunk from a remote peer via TCP and overwrites the local copy.
func (ae *AntiEntropy) fetchAndRepairLocal(chunkID metadata.ChunkID, peerAddr string) error {
	client, err := ae.dialClient(peerAddr)
	if err != nil {
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
					slog.Error("anti-entropy: scan error", "error", err)
				} else if result.Mismatches > 0 {
					slog.Info("anti-entropy: scan complete",
						"scanned", result.ChunksScanned, "mismatches", result.Mismatches,
						"repaired", result.Repaired, "duration", result.Duration)
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

// dialClient creates a Client connected to the given address, using
// TLS when configured. Shared helper for ParallelReplicator and AntiEntropy.
func (cr *ParallelReplicator) dialClient(addr string) (*Client, error) {
	if cr.tlsCfg.Enabled() {
		c, err := NewTLSClient(addr, cr.tlsCfg)
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

func (ae *AntiEntropy) dialClient(addr string) (*Client, error) {
	if ae.tlsCfg.Enabled() {
		c, err := NewTLSClient(addr, ae.tlsCfg)
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

// ============================================================
// Chunk Integrity Checker — Cross-node data verification
// ============================================================

// ChunkIntegrityChecker periodically verifies that chunk replicas
// across different datanodes have identical data. It computes a
// checksum locally and compares it with checksums fetched from
// peer nodes holding the same chunk.
type ChunkIntegrityChecker struct {
	store   *ChunkStore
	meta    IntegrityMeta
	localID metadata.NodeID
	tlsCfg  tlsutil.Config

	stopCh  chan struct{}
	running atomic.Bool
	wg      sync.WaitGroup

	// Stats
	verified   atomic.Int64
	mismatches atomic.Int64
}

// IntegrityMeta provides the metadata needed to find peer replicas.
type IntegrityMeta interface {
	// GetChunk returns chunk metadata including replica locations.
	GetChunk(ctx context.Context, id metadata.ChunkID) (*metadata.ChunkMeta, error)
}

// IntegrityCheckResult holds the outcome of a single integrity check pass.
type IntegrityCheckResult struct {
	ChunksVerified int
	Mismatches     int
	Duration       time.Duration
}

// NewChunkIntegrityChecker creates a new integrity checker.
func NewChunkIntegrityChecker(store *ChunkStore, meta IntegrityMeta, localID metadata.NodeID) *ChunkIntegrityChecker {
	return &ChunkIntegrityChecker{
		store:   store,
		meta:    meta,
		localID: localID,
		stopCh:  make(chan struct{}),
	}
}

// SetTLS configures TLS for inter-node connections.
func (ic *ChunkIntegrityChecker) SetTLS(cfg tlsutil.Config) {
	ic.tlsCfg = cfg
}

// Start begins the periodic integrity check loop.
func (ic *ChunkIntegrityChecker) Start(interval time.Duration) {
	if ic.running.Swap(true) {
		return
	}
	ic.wg.Add(1)
	go func() {
		defer ic.wg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				result, err := ic.Check(context.Background())
				if err != nil {
					slog.Error("integrity check failed", "error", err)
				} else if result.Mismatches > 0 {
					slog.Warn("integrity check found mismatches",
						"verified", result.ChunksVerified,
						"mismatches", result.Mismatches)
				} else {
					slog.Info("integrity check passed", "verified", result.ChunksVerified)
				}
			case <-ic.stopCh:
				return
			}
		}
	}()
}

// Stop terminates the integrity checker.
func (ic *ChunkIntegrityChecker) Stop() {
	if ic.running.Swap(false) {
		close(ic.stopCh)
	}
	ic.wg.Wait()
}

// Check performs one integrity verification pass over all local chunks.
// For each chunk, it computes the local checksum and compares it with
// checksums from peer replicas.
func (ic *ChunkIntegrityChecker) Check(ctx context.Context) (*IntegrityCheckResult, error) {
	start := time.Now()
	result := &IntegrityCheckResult{}

	// Collect local chunk IDs
	ic.store.mu.RLock()
	chunkIDs := make([]metadata.ChunkID, 0, len(ic.store.chunks))
	for id := range ic.store.chunks {
		chunkIDs = append(chunkIDs, id)
	}
	ic.store.mu.RUnlock()

	for _, chunkID := range chunkIDs {
		select {
		case <-ctx.Done():
			return result, ctx.Err()
		default:
		}

		// Compute local checksum
		ok, localCksum, err := ic.store.VerifyChunkData(chunkID)
		if err != nil {
			slog.Debug("integrity: skip chunk (local read error)", "chunkID", chunkID, "error", err)
			continue
		}
		if !ok {
			slog.Warn("integrity: local checksum mismatch", "chunkID", chunkID, "checksum", localCksum)
			result.Mismatches++
			ic.mismatches.Add(1)
		}

		// Fetch metadata to find peer replicas
		chunkMeta, err := ic.meta.GetChunk(ctx, chunkID)
		if err != nil || chunkMeta == nil {
			continue
		}

		// Compare with each peer replica
		for _, replica := range chunkMeta.Replicas {
			if replica.NodeID == ic.localID {
				continue // skip self
			}

			peerCksum, err := ic.fetchPeerChecksum(ctx, replica.Addr, chunkID)
			if err != nil {
				slog.Debug("integrity: failed to fetch peer checksum",
					"chunkID", chunkID, "peer", replica.Addr, "error", err)
				continue
			}

			if peerCksum != localCksum {
				slog.Warn("integrity: cross-node checksum mismatch",
					"chunkID", chunkID,
					"local", localCksum,
					"peer", peerCksum,
					"peerAddr", replica.Addr)
				result.Mismatches++
				ic.mismatches.Add(1)
			}
		}

		result.ChunksVerified++
		ic.verified.Add(1)
	}

	result.Duration = time.Since(start)
	return result, nil
}

// fetchPeerChecksum connects to a peer datanode, reads the chunk data,
// and computes the CRC32 checksum for comparison.
func (ic *ChunkIntegrityChecker) fetchPeerChecksum(ctx context.Context, addr string, chunkID metadata.ChunkID) (uint32, error) {
	client, err := ic.dialClient(addr)
	if err != nil {
		return 0, fmt.Errorf("connect to peer %s: %w", addr, err)
	}
	defer client.Close()

	// Read full chunk from peer
	resp, err := client.ReadChunk(chunkID, 0, 0) // offset=0, length=0 means read all
	if err != nil {
		return 0, fmt.Errorf("read chunk from %s: %w", addr, err)
	}
	if resp.Status != StatusOK {
		return 0, fmt.Errorf("read chunk status %d from %s", resp.Status, addr)
	}
	return crc32.ChecksumIEEE(resp.Data), nil
}

func (ic *ChunkIntegrityChecker) dialClient(addr string) (*Client, error) {
	if ic.tlsCfg.Enabled() {
		c, err := NewTLSClient(addr, ic.tlsCfg)
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

// Stats returns the cumulative integrity check statistics.
func (ic *ChunkIntegrityChecker) Stats() (verified, mismatches int64) {
	return ic.verified.Load(), ic.mismatches.Load()
}
