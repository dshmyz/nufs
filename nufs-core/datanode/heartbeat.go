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

	// Delta tracking: lastKnownState maps ChunkID → state we last
	// reported to the metadata service. On each heartbeat, we compute
	// the diff between current state and lastKnownState; only changed
	// chunks go into the report. Guarded by stateMu.
	stateMu        sync.Mutex
	lastKnownState map[metadata.ChunkID]metadata.ReplicaState
	forceFullSync  bool

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

	// Collect current chunk states
	currentStates := make(map[metadata.ChunkID]metadata.ReplicaState)
	for _, info := range h.chunkSt.ListChunks() {
		switch info.State {
		case LocalSealed:
			currentStates[info.ChunkID] = metadata.ReplicaReady
		case LocalWritten:
			currentStates[info.ChunkID] = metadata.ReplicaSyncing
		case LocalCorrupt:
			currentStates[info.ChunkID] = metadata.ReplicaFailed
		default:
			currentStates[info.ChunkID] = metadata.ReplicaSyncing
		}
	}

	// Determine whether to do a full sync or a delta sync.
	h.stateMu.Lock()
	needFullSync := h.forceFullSync ||
		len(h.lastKnownState) == 0 ||
		h.fullSyncCounter >= h.fullSyncInterval
	h.forceFullSync = false
	h.stateMu.Unlock()

	var chunkStates map[metadata.ChunkID]metadata.ReplicaState
	if needFullSync {
		// Full sync: send all current states
		chunkStates = currentStates
		h.stateMu.Lock()
		h.lastKnownState = make(map[metadata.ChunkID]metadata.ReplicaState, len(currentStates))
		for id, st := range currentStates {
			h.lastKnownState[id] = st
		}
		h.fullSyncCounter = 0
		h.stateMu.Unlock()
	} else {
		// Delta sync: only send chunks whose state changed
		h.stateMu.Lock()
		chunkStates = make(map[metadata.ChunkID]metadata.ReplicaState)
		// New or changed chunks
		for id, st := range currentStates {
			if prev, ok := h.lastKnownState[id]; !ok || prev != st {
				chunkStates[id] = st
				h.lastKnownState[id] = st
			}
		}
		// Deleted chunks (present in lastKnown but not in current)
		for id := range h.lastKnownState {
			if _, ok := currentStates[id]; !ok {
				chunkStates[id] = metadata.ReplicaFailed // mark as gone
				delete(h.lastKnownState, id)
			}
		}
		h.fullSyncCounter++
		h.stateMu.Unlock()
	}

	report := &metadata.NodeReport{
		UsedGB:      usedGB,
		ChunkCount:  chunkCount,
		DiskIO:      h.lastDiskIO,
		ChunkStates: chunkStates,
	}

	if err := h.meta.Heartbeat(h.ctx, h.cfg.NodeID, report); err != nil {
		slog.Error("datanode: heartbeat failed", "error", err)
	}
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
}

// SetDiskIO updates the disk IO utilization metric (0.0 - 1.0).
// Called by external monitoring or the server itself.
func (h *HeartbeatReporter) SetDiskIO(utilization float64) {
	h.lastDiskIO = utilization
}
