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
type HeartbeatReporter struct {
	cfg        Config
	meta       HeartbeatMeta
	chunkSt    *ChunkStore
	interval   time.Duration
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	lastDiskIO float64
}

// NewHeartbeatReporter creates a new heartbeat reporter.
func NewHeartbeatReporter(cfg Config, metaStore HeartbeatMeta, chunkStore *ChunkStore) *HeartbeatReporter {
	ctx, cancel := context.WithCancel(context.Background())
	return &HeartbeatReporter{
		cfg:      cfg,
		meta:     metaStore,
		chunkSt:  chunkStore,
		interval: cfg.HeartbeatInterval,
		ctx:      ctx,
		cancel:   cancel,
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

	// Collect chunk states for reporting
	chunkStates := make(map[metadata.ChunkID]metadata.ReplicaState)
	for _, info := range h.chunkSt.ListChunks() {
		switch info.State {
		case LocalSealed:
			chunkStates[info.ChunkID] = metadata.ReplicaReady
		case LocalWritten:
			chunkStates[info.ChunkID] = metadata.ReplicaSyncing
		case LocalCorrupt:
			chunkStates[info.ChunkID] = metadata.ReplicaFailed
		default:
			chunkStates[info.ChunkID] = metadata.ReplicaSyncing
		}
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

// SetDiskIO updates the disk IO utilization metric (0.0 - 1.0).
// Called by external monitoring or the server itself.
func (h *HeartbeatReporter) SetDiskIO(utilization float64) {
	h.lastDiskIO = utilization
}
