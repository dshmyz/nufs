package datanode

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/example/dfs/metadata"
)

// HeartbeatReporter periodically sends node status and chunk state
// to the metadata service.
//
// To avoid sending the full chunk state map on every heartbeat
// (which is O(total_chunks) and grows linearly with disk usage),
// the reporter tracks the last known state of each chunk and only
// sends deltas: chunks whose state has changed since the last
// heartbeat. A full sync is performed on the first heartbeat, on
// explicit ForceFullSync (e.g., after reconnect), or periodically
// to self-heal any missed deltas (P2.9).
type HeartbeatReporter struct {
	cfg        Config
	meta       HeartbeatMeta
	chunkSt    *ChunkStore
	interval   time.Duration
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	lastDiskIO float64

	// Disk I/O sampling: track bytes since last heartbeat to compute
	// utilization as (delta_bytes / interval) normalized to 0.0-1.0.
	lastIOBytes int64

	// Delta tracking: lastKnownState maps ChunkID → state the metadata
	// service has acknowledged (advanced only on a successful send).
	// On each heartbeat we diff the true state against it and send only
	// the changes. Guarded by stateMu.
	stateMu        sync.Mutex
	lastKnownState map[metadata.ChunkID]metadata.ReplicaState
	forceFullSync  bool

	// lastSnapshot caches the last true-state snapshot (what the store
	// reported), with the store version it was built from. On ticks
	// where the store is unchanged it is reused to re-derive an
	// unacknowledged delta without an O(N) rescan. Guarded by stateMu.
	lastSnapshot      map[metadata.ChunkID]metadata.ReplicaState
	lastSnapshotVer   uint64
	lastFullSyncVer   uint64 // store version at the last successful full sync

	// fullSyncCounter counts heartbeats since the last full sync.
	// Every fullSyncInterval heartbeats, we do a full sync to
	// self-heal any missed deltas (e.g., if a previous heartbeat
	// was lost).
	fullSyncCounter  int
	fullSyncInterval int
}

const defaultFullSyncInterval = 6 // every 6th heartbeat (~1 min at 10s interval)

// NewHeartbeatReporter creates a new heartbeat reporter.
func NewHeartbeatReporter(cfg Config, metaStore HeartbeatMeta, chunkStore *ChunkStore) *HeartbeatReporter {
	ctx, cancel := context.WithCancel(context.Background())
	return &HeartbeatReporter{
		cfg:              cfg,
		meta:             metaStore,
		chunkSt:          chunkStore,
		interval:         cfg.HeartbeatInterval,
		ctx:              ctx,
		cancel:           cancel,
		lastKnownState:   make(map[metadata.ChunkID]metadata.ReplicaState),
		fullSyncInterval: defaultFullSyncInterval,
	}
}

// Start begins the periodic heartbeat loop.
func (h *HeartbeatReporter) Start() {
	h.wg.Add(1)
	go h.loop()
	slog.Info("datanode: heartbeat reporter started", "interval", h.interval)
}

// Stop halts the heartbeat reporter.
func (h *HeartbeatReporter) Stop() {
	h.cancel()
	h.wg.Wait()
	slog.Info("datanode: heartbeat reporter stopped")
}

func (h *HeartbeatReporter) loop() {
	defer h.wg.Done()

	// Send initial heartbeat immediately
	h.send()

	ticker := time.NewTicker(h.interval)
	defer ticker.Stop()

	for {
		select {
		case <-h.ctx.Done():
			return
		case <-ticker.C:
			h.send()
		}
	}
}

func (h *HeartbeatReporter) send() {
	totalBytes, chunkCount := h.chunkSt.Stats()
	usedGB := totalBytes / (1024 * 1024 * 1024)

	// Sample disk I/O utilization from DiskManager counters.
	// Computes bytes/sec since last heartbeat, normalized to 0.0-1.0
	// where 1.0 = 200 MB/s (a reasonable SSD sustained throughput).
	if dm := h.chunkSt.DiskManager(); dm != nil {
		stats := dm.Stats()
		currentIO := stats.ReadBytes + stats.WriteBytes
		delta := currentIO - h.lastIOBytes
		h.lastIOBytes = currentIO
		bytesPerSec := float64(delta) / h.interval.Seconds()
		util := bytesPerSec / (200 * 1024 * 1024) // 200 MB/s = 1.0
		if util > 1.0 {
			util = 1.0
		}
		h.lastDiskIO = util
	}

	// Build the current replica-state snapshot only when the store
	// reports a state change since the last snapshot. In steady state
	// the store version is unchanged, so we reuse the cached snapshot
	// and skip the O(N) ChunkStateSnapshot scan entirely.
	storeVersion := h.chunkSt.StateVersion()

	h.stateMu.Lock()
	needSnapshot := h.lastSnapshot == nil || storeVersion != h.lastSnapshotVer
	h.stateMu.Unlock()
	if needSnapshot {
		// Build outside the lock: ChunkStateSnapshot takes the store
		// RLock, and holding stateMu while doing so risks a lock-order
		// inversion if a writer ever takes them in the other order.
		snapshot := h.chunkSt.ChunkStateSnapshot()
		h.stateMu.Lock()
		h.lastSnapshot = snapshot
		h.lastSnapshotVer = storeVersion
		h.stateMu.Unlock()
	}

	h.stateMu.Lock()
	forceFull := h.forceFullSync
	h.forceFullSync = false
	known := h.lastKnownState
	snapshot := h.lastSnapshot
	// Periodic full-sync backstop: fire only when the counter elapsed
	// AND the state-set changed since the last full sync, so a quiet
	// cluster never pays the O(N) full-send cost.
	periodicFull := h.fullSyncCounter >= h.fullSyncInterval && storeVersion != h.lastFullSyncVer
	needFullSync := forceFull || len(known) == 0 || periodicFull
	h.stateMu.Unlock()

	// Compute the delta against the acknowledged state. On a store-
	// unchanged tick with a previously failed send, snapshot == the
	// unacknowledged changes, so they are re-derived and re-sent.
	chunkStates := make(map[metadata.ChunkID]metadata.ReplicaState)
	deleted := make(map[metadata.ChunkID]struct{})
	for id, st := range snapshot {
		if prev, ok := known[id]; !ok || prev != st {
			chunkStates[id] = st
		}
	}
	if !needFullSync {
		// Deleted chunks: present in known but gone from the snapshot.
		for id := range known {
			if _, ok := snapshot[id]; !ok {
				chunkStates[id] = metadata.ReplicaFailed // mark as gone
				deleted[id] = struct{}{}
			}
		}
	}

	report := &metadata.NodeReport{
		UsedGB:         usedGB,
		ChunkCount:     chunkCount,
		DiskIO:         h.lastDiskIO,
		WriteErrorRate: h.chunkSt.WriteErrorRate(),
		ChunkStates:    chunkStates,
	}

	// Per-disk stats for JBOD multi-disk deployments.
	if ds := h.chunkSt.DiskStats(); len(ds) > 1 {
		diskReports := make([]metadata.DiskReport, len(ds))
		for i, d := range ds {
			diskReports[i] = metadata.DiskReport{
				Index:      d.Index,
				UsedBytes:  d.UsedBytes,
				ChunkCount: d.ChunkCount,
				Failed:     d.Failed,
			}
		}
		report.DiskStats = diskReports
	}

	if err := h.meta.Heartbeat(h.ctx, h.cfg.NodeID, report); err != nil {
		// Do NOT advance lastKnownState: the changes were not
		// acknowledged, so the next tick re-derives the same delta
		// from the cached snapshot (O(1) in steady state).
		slog.Error("datanode: heartbeat failed", "error", err)
		return
	}

	// Send succeeded: snapshot is now acknowledged. For a full sync the
	// whole set is acked; for a delta only the changed chunks, and the
	// unchanged rest were already acked by construction. Chunks that
	// were deleted locally are dropped from the known set (a corrupt
	// chunk still present in the snapshot stays, so ReplicaFailed for
	// it is not re-sent every tick).
	h.stateMu.Lock()
	if needFullSync {
		h.lastKnownState = make(map[metadata.ChunkID]metadata.ReplicaState, len(snapshot))
		for id, st := range snapshot {
			h.lastKnownState[id] = st
		}
		h.lastFullSyncVer = storeVersion
		h.fullSyncCounter = 0
	} else {
		for id, st := range chunkStates {
			h.lastKnownState[id] = st
		}
		for id := range deleted {
			delete(h.lastKnownState, id)
		}
		h.fullSyncCounter++
	}
	h.stateMu.Unlock()
}

// ForceFullSync forces the next heartbeat to send the full chunk
// state map instead of a delta. Should be called after reconnecting
// to the metadata service or when the reporter suspects its delta
// state is out of sync.
func (h *HeartbeatReporter) ForceFullSync() {
	h.stateMu.Lock()
	defer h.stateMu.Unlock()
	h.forceFullSync = true
	// Clear lastKnownState so the next send() sees len==0 and does
	// a full sync even if forceFullSync was already consumed by a
	// concurrent send().
	h.lastKnownState = make(map[metadata.ChunkID]metadata.ReplicaState)
	// Invalidate the snapshot cache so the next send() rebuilds it.
	h.lastSnapshot = nil
}

// SetDiskIO updates the disk IO utilization metric (0.0 - 1.0).
// Called by external monitoring or the server itself.
func (h *HeartbeatReporter) SetDiskIO(utilization float64) {
	h.lastDiskIO = utilization
}
