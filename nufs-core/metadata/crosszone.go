package metadata

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// maxReplicationRetries is the maximum number of times a chunk will be
// re-enqueued for cross-zone replication before being discarded.
const maxReplicationRetries = 10

// maxPendingAge is the maximum age of a pending replication before it's discarded.
// This implements the 72h retention policy for cross-zone replication logs.
const maxPendingAge = 72 * time.Hour

// pendingChunk holds the context needed to replicate a chunk to a remote zone.
type pendingChunk struct {
	bucket   string
	inodeID  InodeID
	offset   int64
	retries  int
	enqueued time.Time // Track when this was enqueued for 72h retention
}

// CrossZoneReplicator manages async replication of chunks to a remote zone.
// It monitors local writes and replicates them to the remote zone according
// to the bucket's PlacementPolicy.CrossZoneReplication config.
//
// The replication process has two phases:
//  1. Metadata: allocate a corresponding chunk on the remote zone's metadata service
//  2. Data: trigger the local datanode to transfer chunk data to the remote datanode
type CrossZoneReplicator struct {
	store     MetadataService
	localZone string

	mu      sync.Mutex
	running bool
	stopCh  chan struct{}
	wg      sync.WaitGroup

	// pending tracks chunk IDs awaiting remote replication.
	pending map[ChunkID]pendingChunk

	// remoteClients holds metadata clients for each remote zone.
	remoteClients map[string]MetadataService

	// dataTransferer triggers datanode-to-datanode chunk data transfer.
	// When nil, only metadata is replicated (legacy behavior).
	dataTransferer DataTransferer

	// wal provides persistent storage for pending replications.
	// When nil, pending queue is only in-memory (lost on restart).
	wal WALWriter
}

// WALWriter defines the interface for persisting pending replications.
// Implementations should write to durable storage (WAL, local file, etc.)
// to survive restarts.
type WALWriter interface {
	// WritePending persists a pending chunk entry.
	WritePending(chunkID ChunkID, info pendingChunk) error
	// ReadPending returns all persisted pending chunks for recovery.
	ReadPending() (map[ChunkID]pendingChunk, error)
	// DeletePending removes a persisted pending chunk after successful replication.
	DeletePending(chunkID ChunkID) error
}

// DataTransferer triggers the actual chunk data transfer between datanodes
// across zones. The metadata service calls this after allocating the remote
// chunk, so the datanode layer knows where to send the data.
type DataTransferer interface {
	// TransferChunk tells the local datanode holding chunkID to replicate
	// its data to the remote datanode at remoteAddr.
	TransferChunk(ctx context.Context, chunkID ChunkID, remoteAddr string) error
}

// NewCrossZoneReplicator creates a new cross-zone replicator.
func NewCrossZoneReplicator(store MetadataService, localZone string) *CrossZoneReplicator {
	return &CrossZoneReplicator{
		store:         store,
		localZone:     localZone,
		pending:       make(map[ChunkID]pendingChunk),
		remoteClients: make(map[string]MetadataService),
	}
}

// RegisterRemoteZone adds a metadata client for a remote zone.
func (r *CrossZoneReplicator) RegisterRemoteZone(zone string, client MetadataService) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.remoteClients[zone] = client
}

// SetDataTransferer configures the data transfer mechanism for cross-zone
// chunk replication. Without this, only metadata is replicated.
func (r *CrossZoneReplicator) SetDataTransferer(dt DataTransferer) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.dataTransferer = dt
}

// SetWAL configures persistent storage for pending replications.
// If a WAL is configured, pending replications survive restarts.
// Must be called before Start().
func (r *CrossZoneReplicator) SetWAL(wal WALWriter) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.wal = wal
}

// Enqueue adds a chunk to the replication queue for the given bucket.
// The bucket's PlacementPolicy.CrossZoneReplication determines the target zone.
// inodeID and offset are required so the remote zone can allocate the chunk
// in the correct file context.
func (r *CrossZoneReplicator) Enqueue(chunkID ChunkID, bucket string, inodeID InodeID, offset int64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	info := pendingChunk{
		bucket:   bucket,
		inodeID:  inodeID,
		offset:   offset,
		enqueued: time.Now(),
	}
	r.pending[chunkID] = info

	// Persist to WAL if configured
	if r.wal != nil {
		if err := r.wal.WritePending(chunkID, info); err != nil {
			slog.Warn("cross-zone: failed to persist pending chunk to WAL",
				"chunkID", chunkID, "error", err)
		}
	}
}

// Start begins the background replication loop.
func (r *CrossZoneReplicator) Start() {
	r.mu.Lock()
	if r.running {
		r.mu.Unlock()
		return
	}
	r.running = true
	r.stopCh = make(chan struct{})

	// Recover pending queue from WAL if configured
	if r.wal != nil {
		if recovered, err := r.wal.ReadPending(); err != nil {
			slog.Warn("cross-zone: failed to recover pending from WAL", "error", err)
		} else if len(recovered) > 0 {
			slog.Info("cross-zone: recovered pending chunks from WAL", "count", len(recovered))
			for chunkID, info := range recovered {
				// Check if the pending chunk has exceeded max age
				if time.Since(info.enqueued) > maxPendingAge {
					// Delete expired entry from WAL
					if delErr := r.wal.DeletePending(chunkID); delErr != nil {
						slog.Warn("cross-zone: failed to delete expired pending from WAL",
							"chunkID", chunkID, "error", delErr)
					}
					continue
				}
				r.pending[chunkID] = info
			}
		}
	}

	r.mu.Unlock()

	r.wg.Add(1)
	go r.replicationLoop()
	slog.Info("cross-zone replicator started", "localZone", r.localZone)
}

// Stop terminates the replication loop.
func (r *CrossZoneReplicator) Stop() {
	r.mu.Lock()
	if !r.running {
		r.mu.Unlock()
		return
	}
	r.running = false
	close(r.stopCh)
	r.mu.Unlock()
	r.wg.Wait()
	slog.Info("cross-zone replicator stopped")
}

func (r *CrossZoneReplicator) replicationLoop() {
	defer r.wg.Done()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			r.drainQueue()
		case <-r.stopCh:
			return
		}
	}
}

// drainQueue processes all pending chunk replications.
func (r *CrossZoneReplicator) drainQueue() {
	r.mu.Lock()
	batch := make(map[ChunkID]pendingChunk, len(r.pending))
	for k, v := range r.pending {
		batch[k] = v
	}
	r.pending = make(map[ChunkID]pendingChunk)
	clients := make(map[string]MetadataService)
	for k, v := range r.remoteClients {
		clients[k] = v
	}
	dt := r.dataTransferer
	wal := r.wal
	r.mu.Unlock()

	if len(batch) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Group chunks by target zone based on bucket's PlacementPolicy
	type zoneChunk struct {
		chunkID ChunkID
		info    pendingChunk
	}
	zoneChunks := make(map[string][]zoneChunk)
	for chunkID, info := range batch {
		// Check if the pending chunk has exceeded max age (72h)
		if time.Since(info.enqueued) > maxPendingAge {
			slog.Warn("cross-zone: discarding chunk after max age (72h)",
				"chunkID", chunkID, "enqueued", info.enqueued)
			// Clean up WAL entry
			if wal != nil {
				if err := wal.DeletePending(chunkID); err != nil {
					slog.Warn("cross-zone: failed to delete expired WAL entry",
						"chunkID", chunkID, "error", err)
				}
			}
			continue
		}

		bi, err := r.store.GetBucket(ctx, info.bucket)
		if err != nil || bi == nil {
			continue
		}
		cfg := bi.Policy.CrossZoneReplication
		if cfg == nil {
			continue
		}
		zoneChunks[cfg.RemoteZone] = append(zoneChunks[cfg.RemoteZone], zoneChunk{
			chunkID: chunkID,
			info:    info,
		})
	}

	// Replicate to each remote zone
	for zone, chunks := range zoneChunks {
		client, ok := clients[zone]
		if !ok {
			slog.Warn("cross-zone: no client for remote zone", "zone", zone)
			ids := make([]ChunkID, len(chunks))
			for i, zc := range chunks {
				ids[i] = zc.chunkID
			}
			r.reEnqueue(ids, batch)
			continue
		}

		for _, zc := range chunks {
			if err := r.replicateChunk(ctx, client, dt, zc.chunkID, zc.info); err != nil {
				slog.Error("cross-zone: replication failed",
					"chunkID", zc.chunkID, "zone", zone, "error", err)
				r.reEnqueueOne(zc.chunkID, zc.info)
			} else {
				// Replication succeeded - delete WAL entry
				if wal != nil {
					if err := wal.DeletePending(zc.chunkID); err != nil {
						slog.Warn("cross-zone: failed to delete WAL entry after success",
							"chunkID", zc.chunkID, "error", err)
					}
				}
			}
		}
	}

	slog.Info("cross-zone: batch processed",
		"total", len(batch), "zones", len(zoneChunks))
}

// reEnqueue puts chunks back into the pending queue, incrementing the retry
// counter. Chunks that exceed maxReplicationRetries are discarded with a warning.
func (r *CrossZoneReplicator) reEnqueue(chunkIDs []ChunkID, batch map[ChunkID]pendingChunk) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, id := range chunkIDs {
		r.reEnqueueLocked(id, batch[id])
	}
}

func (r *CrossZoneReplicator) reEnqueueOne(chunkID ChunkID, info pendingChunk) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reEnqueueLocked(chunkID, info)
}

func (r *CrossZoneReplicator) reEnqueueLocked(chunkID ChunkID, info pendingChunk) {
	info.retries++
	if info.retries > maxReplicationRetries {
		slog.Warn("cross-zone: discarding chunk after max retries",
			"chunkID", chunkID, "retries", info.retries)
		// Clean up WAL entry since we're discarding this chunk
		if r.wal != nil {
			if err := r.wal.DeletePending(chunkID); err != nil {
				slog.Warn("cross-zone: failed to delete WAL entry",
					"chunkID", chunkID, "error", err)
			}
		}
		return
	}
	r.pending[chunkID] = info
}

// replicateChunk allocates a corresponding chunk on the remote zone's metadata
// service and then triggers the actual data transfer if a DataTransferer is
// configured. Without a DataTransferer, only metadata is replicated.
func (r *CrossZoneReplicator) replicateChunk(ctx context.Context, remote MetadataService, dt DataTransferer, chunkID ChunkID, info pendingChunk) error {
	// Look up the bucket's policy to determine replication factor
	replFactor := 1
	if bucket, err := r.store.GetBucket(ctx, info.bucket); err == nil && bucket != nil {
		if bucket.Policy.ReplicationFactor > 0 {
			replFactor = bucket.Policy.ReplicationFactor
		}
	}
	remotePolicy := PlacementPolicy{
		ReplicationFactor: replFactor,
		TopologySpread:    SpreadZone,
	}
	remoteMeta, err := remote.AllocateChunk(ctx, info.inodeID, info.offset, remotePolicy)
	if err != nil {
		return fmt.Errorf("allocate remote chunk: %w", err)
	}

	// Phase 2: trigger actual data transfer if DataTransferer is configured
	if dt != nil && len(remoteMeta.Replicas) > 0 {
		remoteAddr := remoteMeta.Replicas[0].Addr
		if err := dt.TransferChunk(ctx, chunkID, remoteAddr); err != nil {
			return fmt.Errorf("transfer chunk data to %s: %w", remoteAddr, err)
		}
		slog.Info("cross-zone: chunk data replicated",
			"chunkID", chunkID, "remoteChunkID", remoteMeta.ID,
			"remoteAddr", remoteAddr, "zone", info.bucket)
	} else {
		slog.Debug("cross-zone: chunk metadata replicated (no data transferer)",
			"inodeID", info.inodeID, "offset", info.offset, "remoteChunkID", remoteMeta.ID)
	}

	return nil
}

// PendingCount returns the number of chunks awaiting replication.
func (r *CrossZoneReplicator) PendingCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.pending)
}
