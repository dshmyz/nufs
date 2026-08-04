package datanode

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/example/dfs/datanode/storage/journal"
	"github.com/example/dfs/metadata"
)

// HeartbeatStore is the subset of ChunkStore the HeartbeatReporter
// needs. It is an interface so the V2.1 engine (via V2Store) can also be
// heartbeated and stay online for placement selection.
type HeartbeatStore interface {
	Stats() (totalBytes int64, chunkCount int64)
	DiskManager() *DiskManager
	ChunkStateSnapshot() map[metadata.ChunkID]metadata.ReplicaState
	StateVersion() uint64
	DiskStats() []DiskStatsItem
	WriteErrorRate() float64
}

// diskIOProvider is an optional capability a store may expose to feed the
// heartbeat's DiskIO utilization sample with real served bytes. The legacy
// ChunkStore never feeds its DiskManager byte counters on the serving path
// (RecordRead/RecordWrite have no production callers), so its DiskIO is
// always 0; V2Store implements this to produce a live metric. The heartbeat
// samples a store that satisfies this interface ahead of the DiskManager
// fallback.
type diskIOProvider interface {
	ReadWriteBytes() (readBytes int64, writeBytes int64)
}

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
	chunkSt    HeartbeatStore
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
	lastSnapshot    map[metadata.ChunkID]metadata.ReplicaState
	lastSnapshotVer uint64
	lastFullSyncVer uint64 // store version at the last successful full sync

	// fullSyncCounter counts heartbeats since the last full sync.
	// Every fullSyncInterval heartbeats, we do a full sync to
	// self-heal any missed deltas (e.g., if a previous heartbeat
	// was lost).
	fullSyncCounter  int
	fullSyncInterval int

	// changeJournal, when non-nil, is the node's async change journal
	// (§12) whose Pending() events ride on the heartbeat so metadata can
	// reconcile corruption/disk-loss that the ChunkStates delta misses.
	// After a successful heartbeat the reporter polls AckChangeEvents and
	// advances the journal past sequences metadata has actually consumed.
	changeJournal *journal.ChangeJournal
}

const defaultFullSyncInterval = 6 // every 6th heartbeat (~1 min at 10s interval)

// NewHeartbeatReporter creates a new heartbeat reporter.
func NewHeartbeatReporter(cfg Config, metaStore HeartbeatMeta, chunkStore HeartbeatStore) *HeartbeatReporter {
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

// SetChangeJournal attaches the node's async change journal (§12) so its
// Pending() events ride on heartbeats for metadata reconciliation. Nil
// disables change-event shipping.
func (h *HeartbeatReporter) SetChangeJournal(j *journal.ChangeJournal) {
	h.changeJournal = j
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

	// Sample disk I/O utilization from the store's served-byte counters,
	// storing the windowed utilization for the report.
	_ = h.sampleDiskIO()

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

	// Pull the node's pending change-journal events (bounded by the journal's
	// own heartbeat caps) onto the heartbeat so metadata can reconcile async
	// corruption/storage-loss (§12). The events are not acked here — Acking
	// happens only after metadata confirms consumption (below).
	var pendingAck uint64
	hadEvents := false
	if j := h.changeJournal; j != nil {
		if evs, nextAck := j.Pending(j.MaxPerHeartbeat, j.MaxHeartbeatBytes); len(evs) > 0 {
			report.ChangeEvents = changeEventsToReport(evs, make([]metadata.ChangeEventRecord, 0, len(evs)))
			pendingAck = nextAck
			hadEvents = true
		}
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

	// Heartbeat succeeded. If we shipped change events, ask the metadata
	// authority for its persisted reconciled watermark and advance the local
	// journal Ack only past sequences metadata has in fact consumed. On any
	// error the un-acked events are simply reshipped next tick — idempotent
	// reconcile makes this safe (§12).
	if hadEvents && h.changeJournal != nil {
		if acked, err := h.meta.AckChangeEvents(h.ctx, h.cfg.NodeID, pendingAck); err != nil {
			slog.Warn("datanode: change-ack query failed", "error", err)
		} else if acked > 0 {
			h.changeJournal.Ack(acked)
		}
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

// sampleDiskIO samples the store's served-byte counters, computes
// bytes/sec since the last sample normalized to 0.0-1.0 (1.0 = 200 MB/s,
// a reasonable SSD sustained throughput), stores it as the report's DiskIO
// figure, and returns it. A store exposing diskIOProvider (V2Store) yields
// a live metric; the legacy ChunkStore falls back to its DiskManager
// counters, which are never fed on the serving path, so V1 DiskIO remains 0.
func (h *HeartbeatReporter) sampleDiskIO() float64 {
	currentIO := int64(0)
	if p, ok := h.chunkSt.(diskIOProvider); ok {
		r, w := p.ReadWriteBytes()
		currentIO = r + w
	} else if dm := h.chunkSt.DiskManager(); dm != nil {
		stats := dm.Stats()
		currentIO = stats.ReadBytes + stats.WriteBytes
	}
	delta := currentIO - h.lastIOBytes
	h.lastIOBytes = currentIO
	bytesPerSec := float64(delta) / h.interval.Seconds()
	util := bytesPerSec / (200 * 1024 * 1024) // 200 MB/s = 1.0
	if util > 1.0 {
		util = 1.0
	}
	h.lastDiskIO = util
	return util
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

// changeEventKindToReport maps a datanode journal event kind to its
// metadata-side representation (§12).
func changeEventKindToReport(k journal.ChangeEventKind) metadata.ChangeEventKind {
	switch k {
	case journal.EventCorrupt:
		return metadata.ChangeCorrupt
	case journal.EventDiskLost:
		return metadata.ChangeDiskLost
	case journal.EventSegmentLost:
		return metadata.ChangeSegmentLost
	case journal.EventRelocated:
		return metadata.ChangeRelocated
	case journal.EventThirdReplicaComplete:
		return metadata.ChangeThirdReplicaComplete
	case journal.EventRepairCreated:
		return metadata.ChangeRepairCreated
	case journal.EventScrubFinding:
		return metadata.ChangeScrubFinding
	case journal.EventDeleteComplete:
		return metadata.ChangeDeleteComplete
	default:
		return metadata.ChangeCorrupt // safe fallback; unknown kinds are rare
	}
}

// changeEventsToReport converts datanode journal events to metadata
// ChangeEventRecords for shipping on the heartbeat.
func changeEventsToReport(evs []journal.ChangeEvent, out []metadata.ChangeEventRecord) []metadata.ChangeEventRecord {
	for _, ev := range evs {
		out = append(out, metadata.ChangeEventRecord{
			Seq:        ev.Seq,
			Kind:       changeEventKindToReport(ev.Kind),
			ExtentID:   uint64(ev.ExtentID),
			Generation: uint64(ev.Generation),
			SegmentID:  uint64(ev.SegmentID),
			Reason:     ev.Reason,
			AtUnix:     ev.AtUnix,
		})
	}
	return out
}
