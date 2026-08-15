package datanode

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dshmyz/nufs/nufs-core/internal/httputil"
	"github.com/dshmyz/nufs/nufs-core/internal/tlsutil"
	"github.com/dshmyz/nufs/nufs-core/internal/version"
	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// OpsMetadata is the narrow interface OpsServer needs from the metadata layer.
// Define this instead of depending on the full MetadataService to keep the
// datanode package loosely coupled and testable with fakes.
type OpsMetadata interface {
	ListNodes(ctx context.Context) ([]metadata.NodeInfo, error)
	DecommissionNode(ctx context.Context, nodeID metadata.NodeID) error
	ListBuckets(ctx context.Context) ([]metadata.BucketInfo, error)
	GetChunk(ctx context.Context, chunkID metadata.ChunkID) (*metadata.ChunkMeta, error)
	GetRepairQueue(ctx context.Context) ([]metadata.RepairTask, error)
}

// ============================================================
// Operations API — Production Cluster Management HTTP Interface
// ============================================================

// OpsServer exposes HTTP endpoints for cluster operations. It is
// engine-agnostic: store is the OpsStore subset both V1 ChunkStore and V2.1
// V2Store satisfy, so the same HTTP surface serves either engine. The V1-only
// subsystems (disk manager, replicator, anti-entropy, repair worker) are
// optional and nil-guarded; V2.1 passes nil for all of them.
type OpsServer struct {
	cfg        Config
	store      OpsStore
	meta       OpsMetadata
	repair     *RepairWorker
	ec         *ECService
	listener   *http.Server
	running    atomic.Bool
	shutdownWg sync.WaitGroup

	// Instance-scoped capacity-alert state (admin ring + webhook). Lived on the
	// server so multiple OpsServers (e.g. across tests) stay isolated.
	alertWebhook   string
	alertMu        sync.RWMutex
	alertRing      []capacityAlertEvent
	lastAlertLevel atomic.Int64
	// alertDispatch is a FIFO queue of alert events awaiting webhook delivery.
	// A single worker goroutine drains it so POSTs arrive in notification order
	// (independent goroutines would race and could reorder them). Lazy-started
	// on first use so servers that never set a webhook don't leak a worker.
	alertDispatch    chan capacityAlertEvent
	alertDispatchOne sync.Once

	// Background orphan GC ticker (enabled when cfg.GCScanInterval > 0).
	// Lived on the server so both the V1 and V2.1 engines get it for free
	// through Start()/Stop() with zero main.go changes.
	gcStop chan struct{}
	gcWg   sync.WaitGroup
}

// NewOpsServer creates the operations HTTP server.
func NewOpsServer(cfg Config, store OpsStore, meta OpsMetadata) *OpsServer {
	return NewOpsServerWithRepair(cfg, store, meta, nil)
}

// NewOpsServerWithRepair creates an ops server with repair worker integration.
func NewOpsServerWithRepair(cfg Config, store OpsStore, meta OpsMetadata, repair *RepairWorker) *OpsServer {
	mux := http.NewServeMux()

	s := &OpsServer{
		cfg:    cfg,
		store:  store,
		meta:   meta,
		repair: repair,
	}


	// Cluster status
	// Disk lifecycle management (HTTP endpoints)
	mux.HandleFunc("/api/v1/disks", s.handleDisks)
	mux.HandleFunc("/api/v1/disks/adopt", s.handleHTTPAdopt)
	mux.HandleFunc("/api/v1/disks/retire", s.handleHTTPRetire)
	mux.HandleFunc("/api/v1/disks/decommission", s.handleHTTPDecommission)
	mux.HandleFunc("/api/v1/disks/migrate", s.handleHTTPMigrate)
	mux.HandleFunc("/api/v1/disks/verify", s.handleHTTPVerifyDisk)
	mux.HandleFunc("/api/v1/disks/drain", s.handleHTTPDrain)
	mux.HandleFunc("/api/v1/config", s.handleHTTPConfig)

	mux.HandleFunc("/api/v1/cluster/status", s.handleClusterStatus)

	// Node management
	mux.HandleFunc("/api/v1/nodes", s.handleNodes)
	mux.HandleFunc("/api/v1/nodes/{id}/decommission", s.handleDecommission)

	// Bucket operations
	mux.HandleFunc("/api/v1/buckets", s.handleBuckets)

	// Chunk operations
	mux.HandleFunc("/api/v1/chunks/{id}/verify", s.handleVerifyChunk)

	// EC (V2.1 6+3) operations
	mux.HandleFunc("/api/v1/ec/convert", s.handleECConvert)

	// Repair operations
	mux.HandleFunc("/api/v1/repair/queue", s.handleRepairQueue)
	mux.HandleFunc("/api/v1/gc/scan", s.handleGCScan)

	// Metrics
	mux.HandleFunc("/metrics", s.handlePrometheusMetrics)
	mux.HandleFunc("/api/v1/metrics", s.handleMetrics)
	mux.HandleFunc("/api/v1/health", s.handleHealth)
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/healthz", s.handleHealth)
	mux.HandleFunc("/ready", s.handleHealth)
	mux.HandleFunc("/api/v1/capacity/alerts", s.handleCapacityAlerts)

	// pprof — runtime profiling endpoints, disabled by default.
	if cfg.EnablePprof {
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	}

	// Version
	mux.HandleFunc("/version", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(version.Info())
	})

	// Datanode admin web view + capacity-alert feed.
	s.registerAdminRoutes(mux)

	if cfg.OpsAuthToken != "" {
		public := map[string]struct{}{
			"/api/v1/health": {},
			"/metrics":       {},
			"/health":        {},
			"/healthz":       {},
			"/ready":         {},
			"/version":       {},
		}
		s.listener = &http.Server{
			Addr:    cfg.OpsListenAddr,
			Handler: httputil.BearerAuth(cfg.OpsAuthToken, nil, public, mux),
		}
	} else {
		s.listener = &http.Server{
			Addr:    cfg.OpsListenAddr,
			Handler: mux,
		}
	}
	return s
}

// SetECService attaches the V2.1 EC conversion driver to the ops server. It is
// optional and nil-guarded (V1 leaves it unset; V2.1 wires the ECService built
// by runDataNodeV21), enabling the /api/v1/ec/convert control-plane endpoint.
func (s *OpsServer) SetECService(ec *ECService) {
	s.ec = ec
}

// Start begins listening for operations requests.
// It blocks until the HTTP listener is ready to accept connections,
// eliminating the race between Start() returning and the server being
// available for requests. When Config.TLS is enabled, the listener
// is wrapped with TLS.
func (s *OpsServer) Start() error {
	if !s.running.Swap(true) {
		// Use a channel to wait for the listener to be ready.
		ready := make(chan error, 1)

		go func() {
			// Create a net.Listener first, then signal readiness.
			ln, err := net.Listen("tcp", s.cfg.OpsListenAddr)
			if err != nil {
				s.running.Store(false)
				ready <- fmt.Errorf("ops: listen %s: %w", s.cfg.OpsListenAddr, err)
				return
			}

			// Wrap with TLS if configured.
			if s.cfg.TLS.Enabled() {
				tlsCfg, err := tlsutil.ServerConfig(s.cfg.TLS)
				if err != nil {
					ln.Close()
					s.running.Store(false)
					ready <- fmt.Errorf("ops: TLS config: %w", err)
					return
				}
				ln = tls.NewListener(ln, tlsCfg)
			}

			ready <- nil // Signal that the listener is ready.

			if err := s.listener.Serve(ln); err != nil && err != http.ErrServerClosed {
				slog.Error("ops: HTTP server error", "error", err)
			}
		}()

		if err := <-ready; err != nil {
			return err
		}
		scheme := "http"
		if s.cfg.TLS.Enabled() {
			scheme = "https"
		}
		slog.Info("ops: management API ready", "addr", s.cfg.OpsListenAddr, "scheme", scheme)

		// Start the optional background orphan GC scan.
		if s.cfg.GCScanInterval > 0 {
			s.startGCScanner(s.cfg.GCScanInterval)
		}
	}
	return nil
}

// startGCScanner launches the background orphan-chunk GC ticker with the
// configured cadence and grace window. Stopped by Stop(). Safe to call once
// per Start (guarded by running flag in the caller).
func (s *OpsServer) startGCScanner(interval time.Duration) {
	s.gcStop = make(chan struct{})
	s.gcWg.Add(1)
	grace := s.cfg.GCGraceWindow
	go func() {
		defer s.gcWg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if _, err := s.triggerGCScan(context.Background(), grace); err != nil {
					slog.Warn("ops: background gc scan error", "error", err)
				}
			case <-s.gcStop:
				return
			}
		}
	}()
	slog.Info("ops: background orphan scan started",
		"interval", interval, "grace", grace)
}

// Stop shuts down the operations server.
func (s *OpsServer) Stop() {
	if s.running.Swap(false) {
		if s.gcStop != nil {
			close(s.gcStop)
			s.gcWg.Wait()
			s.gcStop = nil
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s.listener.Shutdown(ctx)
		slog.Info("ops: management API stopped")
	}
}

// --- Handlers ---

// replStats returns replication counters (V1 removed, always zero).
func (s *OpsServer) replStats() (writes, errors, avgLatency int64) {
	return 0, 0, 0
}

// aeStats returns anti-entropy counters (V1 removed, always zero).
func (s *OpsServer) aeStats() (scanned, mismatches, repaired int64) {
	return 0, 0, 0
}

// diskStats returns the disk-capacity/IO snapshot for the ops dashboard.
//
// V1 path (s.disk set): hands through to the legacy DiskManager snapshot.
//
// V2.1 path (s.disk nil): there is no DiskManager, so synthesize the snapshot
// from the engine's per-disk DiskInfos + aggregate Stats (and ReadWriteBytes
// cumulative counters when the store exposes them). Without this branch the
// V2.1 metrics/admin telemetry would report all-zero capacity/chunk-count/IO.
// diskStateName returns the string form of a DiskState for the metrics snapshot.
func diskStateName(s DiskState) string {
	switch s {
	case DiskDegraded:
		return "degraded"
	case DiskFailed:
		return "failed"
	default:
		return "online"
	}
}

// stateRank orders disk states so the worst health wins when aggregating a
// multi-disk node's state (failed > degraded > online).
func stateRank(name string) int {
	switch name {
	case "failed":
		return 2
	case "degraded":
		return 1
	default:
		return 0
	}
}

func (s *OpsServer) diskStats() DiskStatsSnapshot {
	totalBytes, chunkCount := s.store.Stats()
	var readBytes, writeBytes int64
	if rw, ok := s.store.(interface{ ReadWriteBytes() (int64, int64) }); ok {
		readBytes, writeBytes = rw.ReadWriteBytes()
	}
	var used, total, onDisk int64
	state := "online"
	for _, d := range s.store.DiskInfos() {
		used += d.UsedBytes
		onDisk += d.OnDiskBytes
		if cap := detectCapacityBytes(d.Dir); cap > 0 {
			total += cap
		}
		if st := diskStateName(d.State); stateRank(st) > stateRank(state) {
			state = st
		}
	}
	if totalBytes > used {
		used = totalBytes
	}
	usage := 0.0
	if total > 0 {
		usage = float64(used) / float64(total)
	}
	return DiskStatsSnapshot{
		TotalBytes:  total,
		UsedBytes:   used,
		OnDiskBytes: onDisk,
		AvailBytes:  total - used,
		UsagePct:    usage,
		ChunkCount:  chunkCount,
		ReadBytes:   readBytes,
		WriteBytes:  writeBytes,
		DiskState:   state,
		LastUpdated: time.Now(),
	}
}

// diskAlertLevel returns the current alert level (V1 disk manager removed).
func (s *OpsServer) diskAlertLevel() AlertLevel {
	return AlertNone
}

type ClusterStatus struct {
	NodeID      uint64             `json:"node_id"`
	State       metadata.NodeState `json:"state"`
	Addr        string             `json:"addr"`
	DiskStats   DiskStatsSnapshot  `json:"disk_stats"`
	Replication struct {
		Writes     int64 `json:"writes"`
		Errors     int64 `json:"errors"`
		AvgLatency int64 `json:"avg_latency_us"`
	} `json:"replication"`
	AntiEntropy struct {
		Scanned    int64 `json:"scanned"`
		Mismatches int64 `json:"mismatches"`
		Repaired   int64 `json:"repaired"`
	} `json:"anti_entropy"`
}

func (s *OpsServer) handleClusterStatus(w http.ResponseWriter, r *http.Request) {
	writes, errors, avgLatency := s.replStats()
	scanned, mismatches, repaired := s.aeStats()

	status := ClusterStatus{
		NodeID:    uint64(s.cfg.NodeID),
		State:     metadata.NodeOnline,
		Addr:      s.cfg.ListenAddr,
		DiskStats: s.diskStats(),
	}
	status.Replication.Writes = writes
	status.Replication.Errors = errors
	status.Replication.AvgLatency = avgLatency
	status.AntiEntropy.Scanned = scanned
	status.AntiEntropy.Mismatches = mismatches
	status.AntiEntropy.Repaired = repaired

	s.writeJSON(w, status)
}

func (s *OpsServer) handleNodes(w http.ResponseWriter, r *http.Request) {
	nodes, err := s.meta.ListNodes(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.writeJSON(w, nodes)
}

func (s *OpsServer) handleDecommission(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	idStr := r.PathValue("id")
	var nodeID metadata.NodeID
	fmt.Sscanf(idStr, "%d", &nodeID)
	err := s.meta.DecommissionNode(r.Context(), nodeID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.writeJSON(w, map[string]string{"status": "decommissioned", "node_id": idStr})
}

func (s *OpsServer) handleBuckets(w http.ResponseWriter, r *http.Request) {
	buckets, err := s.meta.ListBuckets(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.writeJSON(w, buckets)
}

type VerifyResult struct {
	ChunkID  uint64 `json:"chunk_id"`
	Valid    bool   `json:"valid"`
	Checksum uint32 `json:"checksum"`
	Local    uint32 `json:"local_checksum"`
}

func (s *OpsServer) handleVerifyChunk(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	var chunkID metadata.ChunkID
	fmt.Sscanf(idStr, "%d", &chunkID)

	valid, local, err := s.store.VerifyChunkData(chunkID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	meta, err := s.meta.GetChunk(r.Context(), chunkID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	s.writeJSON(w, VerifyResult{
		ChunkID:  uint64(chunkID),
		Valid:    valid,
		Checksum: meta.Checksum,
		Local:    local,
	})
}

func (s *OpsServer) handleRepairQueue(w http.ResponseWriter, r *http.Request) {
	tasks, err := s.meta.GetRepairQueue(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.writeJSON(w, tasks)
}

func (s *OpsServer) handleGCScan(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Manual scan is immediate: no grace window (the operator is explicitly
	// asking for it). The background ticker applies cfg.GCGraceWindow.
	orphanCount, err := s.triggerGCScan(r.Context(), 0)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.writeJSON(w, map[string]interface{}{
		"orphan_chunks": orphanCount,
		"status":        "scan_complete",
	})
}

// triggerGCScan deletes local chunks that no longer exist in metadata.
// grace is a minimum local chunk age before it may be deleted; a chunk this
// young (WrittenAt within grace) is presumed to be an in-flight write whose
// metadata commit has not landed yet — GetChunk legitimately reports
// not-found for it, and deleting it would destroy valid data. Pass 0 to
// disable the guard (manual operator trigger).
func (s *OpsServer) triggerGCScan(ctx context.Context, grace time.Duration) (int, error) {
	// Iterate local chunks and check if each exists in metadata.
	// Chunks not found in metadata are orphans and should be deleted.
	localChunks := s.store.ListChunks()
	orphanCount := 0

	for _, lc := range localChunks {
		if ctx.Err() != nil {
			break
		}
		_, err := s.meta.GetChunk(ctx, lc.ChunkID)
		if err == nil {
			continue // chunk exists in metadata
		}
		// Only delete if the error is specifically "chunk not found".
		// Transient errors (network timeout, metadata unavailable) must NOT
		// trigger deletion — that would destroy valid data.
		if !isChunkNotFound(err) {
			slog.Warn("gc: skipping chunk due to metadata error", "chunkID", lc.ChunkID, "error", err)
			continue
		}
		// Grace window: a very recently written local chunk is likely an
		// in-flight write (data on disk, metadata commit pending), for which
		// not-found is expected and NOT an orphan signal. Skip it rather than
		// racing the writer to delete data it still needs.
		if grace > 0 && time.Since(lc.WrittenAt) < grace {
			slog.Debug("gc: skipping young chunk within grace window",
				"chunkID", lc.ChunkID, "writtenAt", lc.WrittenAt)
			continue
		}
		slog.Info("gc: orphan chunk not in metadata, deleting", "chunkID", lc.ChunkID, "size", lc.Size)
		if delErr := s.store.Delete(lc.ChunkID); delErr != nil {
			slog.Error("gc: failed to delete orphan chunk", "chunkID", lc.ChunkID, "error", delErr)
		} else {
			orphanCount++
		}
	}

	if orphanCount > 0 {
		slog.Info("gc: scan complete", "deleted", orphanCount, "total", len(localChunks))
	}
	return orphanCount, nil
}

type OpsMetrics struct {
	Disk  DiskStatsSnapshot `json:"disk"`
	Cache struct {
		ChunkCount int64 `json:"chunk_count"`
		UsedBytes  int64 `json:"used_bytes"`
	} `json:"cache"`
	Perf        ChunkStorePerfSnapshot `json:"perf"`
	Replication struct {
		Writes     int64 `json:"writes"`
		Errors     int64 `json:"errors"`
		AvgLatency int64 `json:"avg_latency_us"`
	} `json:"replication"`
	AntiEntropy struct {
		Scanned    int64 `json:"scanned"`
		Mismatches int64 `json:"mismatches"`
		Repaired   int64 `json:"repaired"`
	} `json:"anti_entropy"`
}

func (s *OpsServer) handlePrometheusMetrics(w http.ResponseWriter, r *http.Request) {
	stats := s.diskStats()
	writes, replErrors, avgLatency := s.replStats()
	scanned, mismatches, repaired := s.aeStats()
	totalBytes, chunkCount := s.store.Stats()
	perf := s.store.PerfSnapshot()

	state := strings.ToLower(stats.DiskState)
	if state == "" {
		state = "unknown"
	}

	var sb strings.Builder
	sb.WriteString("# HELP nufs_datanode_chunks_total Local chunks stored on this datanode\n")
	sb.WriteString("# TYPE nufs_datanode_chunks_total gauge\n")
	fmt.Fprintf(&sb, "nufs_datanode_chunks_total %d\n", chunkCount)

	sb.WriteString("# HELP nufs_datanode_bytes_total Local plaintext chunk bytes stored on this datanode\n")
	sb.WriteString("# TYPE nufs_datanode_bytes_total gauge\n")
	fmt.Fprintf(&sb, "nufs_datanode_bytes_total %d\n", totalBytes)

	sb.WriteString("# HELP nufs_disk_capacity_bytes Configured disk capacity in bytes\n")
	sb.WriteString("# TYPE nufs_disk_capacity_bytes gauge\n")
	fmt.Fprintf(&sb, "nufs_disk_capacity_bytes %d\n", stats.TotalBytes)

	sb.WriteString("# HELP nufs_disk_used_bytes Disk bytes used by local chunks\n")
	sb.WriteString("# TYPE nufs_disk_used_bytes gauge\n")
	fmt.Fprintf(&sb, "nufs_disk_used_bytes %d\n", stats.UsedBytes)

	sb.WriteString("# HELP nufs_disk_available_bytes Disk bytes available before configured capacity\n")
	sb.WriteString("# TYPE nufs_disk_available_bytes gauge\n")
	fmt.Fprintf(&sb, "nufs_disk_available_bytes %d\n", stats.AvailBytes)

	sb.WriteString("# HELP nufs_disk_io_errors_total Consecutive disk I/O error count\n")
	sb.WriteString("# TYPE nufs_disk_io_errors_total gauge\n")
	fmt.Fprintf(&sb, "nufs_disk_io_errors_total %d\n", stats.IOErrors)

	sb.WriteString("# HELP nufs_disk_state Disk state as labeled gauge (1 for current state)\n")
	sb.WriteString("# TYPE nufs_disk_state gauge\n")
	fmt.Fprintf(&sb, "nufs_disk_state{state=%q} 1\n", state)

	sb.WriteString("# HELP nufs_datanode_replication_writes_total Replication writes performed\n")
	sb.WriteString("# TYPE nufs_datanode_replication_writes_total counter\n")
	fmt.Fprintf(&sb, "nufs_datanode_replication_writes_total %d\n", writes)

	sb.WriteString("# HELP nufs_datanode_replication_errors_total Replication errors observed\n")
	sb.WriteString("# TYPE nufs_datanode_replication_errors_total counter\n")
	fmt.Fprintf(&sb, "nufs_datanode_replication_errors_total %d\n", replErrors)

	sb.WriteString("# HELP nufs_datanode_replication_avg_latency_us Average replication latency in microseconds\n")
	sb.WriteString("# TYPE nufs_datanode_replication_avg_latency_us gauge\n")
	fmt.Fprintf(&sb, "nufs_datanode_replication_avg_latency_us %d\n", avgLatency)

	sb.WriteString("# HELP nufs_datanode_antientropy_scanned_total Anti-entropy chunks scanned\n")
	sb.WriteString("# TYPE nufs_datanode_antientropy_scanned_total counter\n")
	fmt.Fprintf(&sb, "nufs_datanode_antientropy_scanned_total %d\n", scanned)

	sb.WriteString("# HELP nufs_datanode_antientropy_mismatches_total Anti-entropy mismatches found\n")
	sb.WriteString("# TYPE nufs_datanode_antientropy_mismatches_total counter\n")
	fmt.Fprintf(&sb, "nufs_datanode_antientropy_mismatches_total %d\n", mismatches)

	sb.WriteString("# HELP nufs_datanode_antientropy_repaired_total Anti-entropy repairs completed\n")
	sb.WriteString("# TYPE nufs_datanode_antientropy_repaired_total counter\n")
	fmt.Fprintf(&sb, "nufs_datanode_antientropy_repaired_total %d\n", repaired)

	sb.WriteString("# HELP nufs_datanode_read_requested_bytes_total Bytes requested by read calls after range clipping\n")
	sb.WriteString("# TYPE nufs_datanode_read_requested_bytes_total counter\n")
	fmt.Fprintf(&sb, "nufs_datanode_read_requested_bytes_total %d\n", perf.ReadRequestedBytes)

	sb.WriteString("# HELP nufs_datanode_read_amplified_bytes_total Bytes read from chunk payloads before range slicing\n")
	sb.WriteString("# TYPE nufs_datanode_read_amplified_bytes_total counter\n")
	fmt.Fprintf(&sb, "nufs_datanode_read_amplified_bytes_total %d\n", perf.ReadAmplifiedBytes)

	sb.WriteString("# HELP nufs_datanode_fsync_seconds_total Total time spent in chunk file fsync\n")
	sb.WriteString("# TYPE nufs_datanode_fsync_seconds_total counter\n")
	fmt.Fprintf(&sb, "nufs_datanode_fsync_seconds_total %g\n", float64(perf.FsyncNs)/1e9)

	sb.WriteString("# HELP nufs_datanode_fsync_total Chunk file fsync count\n")
	sb.WriteString("# TYPE nufs_datanode_fsync_total counter\n")
	fmt.Fprintf(&sb, "nufs_datanode_fsync_total %d\n", perf.FsyncCount)

	sb.WriteString("# HELP nufs_datanode_fd_cache_hits_total File descriptor cache hits\n")
	sb.WriteString("# TYPE nufs_datanode_fd_cache_hits_total counter\n")
	fmt.Fprintf(&sb, "nufs_datanode_fd_cache_hits_total %d\n", perf.FdCacheHits)

	sb.WriteString("# HELP nufs_datanode_fd_cache_misses_total File descriptor cache misses\n")
	sb.WriteString("# TYPE nufs_datanode_fd_cache_misses_total counter\n")
	fmt.Fprintf(&sb, "nufs_datanode_fd_cache_misses_total %d\n", perf.FdCacheMisses)

	sb.WriteString("# HELP nufs_datanode_fd_cache_evictions_total File descriptor cache evictions\n")
	sb.WriteString("# TYPE nufs_datanode_fd_cache_evictions_total counter\n")
	fmt.Fprintf(&sb, "nufs_datanode_fd_cache_evictions_total %d\n", perf.FdCacheEvictions)

	sb.WriteString("# HELP nufs_datanode_list_chunks_seconds_total Total time spent copying local chunk lists\n")
	sb.WriteString("# TYPE nufs_datanode_list_chunks_seconds_total counter\n")
	fmt.Fprintf(&sb, "nufs_datanode_list_chunks_seconds_total %g\n", float64(perf.ListChunksNs)/1e9)

	sb.WriteString("# HELP nufs_datanode_list_chunks_total ListChunks call count\n")
	sb.WriteString("# TYPE nufs_datanode_list_chunks_total counter\n")
	fmt.Fprintf(&sb, "nufs_datanode_list_chunks_total %d\n", perf.ListChunksCalls)

	sb.WriteString("# HELP nufs_datanode_write_semaphore_wait_seconds_total Total time waiting for write semaphore\n")
	sb.WriteString("# TYPE nufs_datanode_write_semaphore_wait_seconds_total counter\n")
	fmt.Fprintf(&sb, "nufs_datanode_write_semaphore_wait_seconds_total %g\n", float64(perf.WriteSemWaitNs)/1e9)

	sb.WriteString("# HELP nufs_datanode_read_semaphore_wait_seconds_total Total time waiting for read semaphore\n")
	sb.WriteString("# TYPE nufs_datanode_read_semaphore_wait_seconds_total counter\n")
	fmt.Fprintf(&sb, "nufs_datanode_read_semaphore_wait_seconds_total %g\n", float64(perf.ReadSemWaitNs)/1e9)

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	_, _ = w.Write([]byte(sb.String()))
}

func (s *OpsServer) handleMetrics(w http.ResponseWriter, r *http.Request) {
	writes, errors, avgLatency := s.replStats()
	scanned, mismatches, repaired := s.aeStats()

	m := OpsMetrics{}
	m.Disk = s.diskStats()
	tb, cc := s.store.Stats()
	m.Cache.ChunkCount = cc
	m.Cache.UsedBytes = tb
	m.Perf = s.store.PerfSnapshot()
	m.Replication.Writes = writes
	m.Replication.Errors = errors
	m.Replication.AvgLatency = avgLatency
	m.AntiEntropy.Scanned = scanned
	m.AntiEntropy.Mismatches = mismatches
	m.AntiEntropy.Repaired = repaired

	s.writeJSON(w, m)
}

type HealthStatus struct {
	Status string `json:"status"`
	NodeID uint64 `json:"node_id"`
	DiskOK bool   `json:"disk_ok"`
	MetaOK bool   `json:"meta_ok"`
}

func (s *OpsServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	disk := s.diskStats()
	healthy := disk.UsagePct < 0.95

	status := HealthStatus{
		Status: "healthy",
		NodeID: uint64(s.cfg.NodeID),
		DiskOK: healthy,
		MetaOK: true,
	}
	if !healthy {
		status.Status = "degraded"
	}

	code := http.StatusOK
	if status.Status == "degraded" {
		code = http.StatusServiceUnavailable
	}
	s.writeJSON(w, status, code)
}

func (s *OpsServer) handleCapacityAlerts(w http.ResponseWriter, r *http.Request) {
	ov := s.capacityOverview()
	level := s.currentAlertLevel()
	s.writeJSON(w, map[string]interface{}{
		"alert_level": level.String(),
		"usage_pct":   fmt.Sprintf("%.1f%%", ov.UsagePct*100),
		"used_bytes":  ov.UsedBytes,
		"total_bytes": ov.TotalBytes,
		"avail_bytes": ov.TotalBytes - ov.UsedBytes,
	})
}

func (s *OpsServer) writeJSON(w http.ResponseWriter, v interface{}, status ...int) {
	w.Header().Set("Content-Type", "application/json")
	if len(status) > 0 {
		w.WriteHeader(status[0])
	} else {
		w.WriteHeader(http.StatusOK)
	}
	json.NewEncoder(w).Encode(v)
}

// ============================================================
// Config extension for Ops Server
// ============================================================

// OpsListenAddr is added to Config
func init() {
	// Default ops port is 8091
}

// ============================================================
// Disk lifecycle management — HTTP endpoints
// ============================================================

func (s *OpsServer) handleDisks(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	totalBytes, chunkCount := s.store.Stats()
	infos := s.store.DiskInfos()

	type diskJSON struct {
		Index       int    `json:"index"`
		Dir         string `json:"dir"`
		Failed      bool   `json:"failed"`
		State       string `json:"state"`
		Chunks      int64  `json:"chunks"`
		Bytes       int64  `json:"bytes"`
		OnDiskBytes int64  `json:"on_disk_bytes,omitempty"`
		TotalBytes  int64  `json:"total_bytes"`
	}

	chunksPerDisk := make(map[int]struct{ count, bytes int64 })
	for _, info := range s.store.ListChunks() {
		v := chunksPerDisk[info.DiskIndex]
		v.count++
		v.bytes += info.Size
		chunksPerDisk[info.DiskIndex] = v
	}

	disks := make([]diskJSON, 0, len(infos))
	for _, di := range infos {
		v := chunksPerDisk[di.Index]
		disks = append(disks, diskJSON{
			Index: di.Index, Dir: di.Dir, Failed: di.Failed,
			State: di.State.String(), Chunks: v.count, Bytes: v.bytes,
			OnDiskBytes: di.OnDiskBytes, TotalBytes: detectCapacityBytes(di.Dir),
		})
	}

	writeJSON(w, map[string]interface{}{
		"disks": disks, "total_chunks": chunkCount, "total_bytes": totalBytes,
	})
}

func (s *OpsServer) handleHTTPAdopt(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	dir := r.URL.Query().Get("dir")
	if dir == "" {
		writeJSONError(w, http.StatusBadRequest, "path required")
		return
	}
	lc, ok := s.store.(DiskLifecycleOps)
	if !ok {
		writeJSONError(w, http.StatusNotImplemented, "disk lifecycle unsupported by this engine")
		return
	}
	idx, err := lc.AddDisk(dir, 8, 8)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, map[string]interface{}{"dir": dir, "index": idx})
}

func (s *OpsServer) handleHTTPRetire(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	dir := r.URL.Query().Get("dir")
	if dir == "" {
		writeJSONError(w, http.StatusBadRequest, "path required")
		return
	}
	lc, ok := s.store.(DiskLifecycleOps)
	if !ok {
		writeJSONError(w, http.StatusNotImplemented, "disk lifecycle unsupported by this engine")
		return
	}
	idx := DiskIndexByDir(s.store.DiskInfos(), dir)
	if idx < 0 {
		writeJSONError(w, http.StatusNotFound, "dir not found")
		return
	}
	for _, di := range s.store.DiskInfos() {
		if di.Index != idx {
			continue
		}
		if di.Failed {
			writeJSONError(w, http.StatusConflict, "disk already retired")
			return
		}
		if err := lc.RemoveDisk(di.Index); err != nil {
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, map[string]interface{}{"dir": dir})
		return
	}
	writeJSONError(w, http.StatusNotFound, "dir not found")
}

func (s *OpsServer) handleHTTPDecommission(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	dir := r.URL.Query().Get("dir")
	if dir == "" {
		writeJSONError(w, http.StatusBadRequest, "path required")
		return
	}
	lc, ok := s.store.(DiskLifecycleOps)
	if !ok {
		writeJSONError(w, http.StatusNotImplemented, "disk lifecycle unsupported by this engine")
		return
	}
	idx := DiskIndexByDir(s.store.DiskInfos(), dir)
	if idx < 0 {
		writeJSONError(w, http.StatusNotFound, "dir not found")
		return
	}
	for _, di := range s.store.DiskInfos() {
		if di.Index != idx {
			continue
		}
		if di.Failed {
			writeJSONError(w, http.StatusConflict, "disk already retired")
			return
		}
		migrated, migErr := lc.MigrateDisk(di.Index)
		if migErr != nil {
			writeJSONError(w, http.StatusInternalServerError,
				fmt.Sprintf("disk may be unreadable; use retire instead (migrated %d, error: %v)", migrated, migErr))
			return
		}
		if err := lc.RemoveDisk(di.Index); err != nil {
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, map[string]interface{}{"dir": dir, "migrated": migrated})
		return
	}
	writeJSONError(w, http.StatusNotFound, "dir not found")
}

func (s *OpsServer) handleHTTPMigrate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	dir := r.URL.Query().Get("dir")
	if dir == "" {
		writeJSONError(w, http.StatusBadRequest, "path required")
		return
	}
	lc, ok := s.store.(DiskLifecycleOps)
	if !ok {
		writeJSONError(w, http.StatusNotImplemented, "disk lifecycle unsupported by this engine")
		return
	}
	idx := DiskIndexByDir(s.store.DiskInfos(), dir)
	if idx < 0 {
		writeJSONError(w, http.StatusNotFound, "dir not found")
		return
	}
	for _, di := range s.store.DiskInfos() {
		if di.Index != idx {
			continue
		}
		migrated, err := lc.MigrateDisk(di.Index)
		if err != nil {
			writeJSONError(w, http.StatusInternalServerError,
				fmt.Sprintf("error: %v (migrated %d)", err, migrated))
			return
		}
		writeJSON(w, map[string]interface{}{"dir": dir, "migrated": migrated})
		return
	}
	writeJSONError(w, http.StatusNotFound, "dir not found")
}

// handleECConvert converts one replicated chunk to a completed 6+3 EC stripe
// on the V2.1 serving path, driving the injected ECService's authority
// transaction (§14). V1 (no ECService wired) answers "unsupported", the same
// capability-gating the other V2.1 engine-specific endpoints use.
//
//	POST /api/v1/ec/convert?chunk_id=<id>[&generation=<gen>]
//
// On success returns the completed stripe (state, shard count, original
// checksum). On failure the transaction is rolled back and the chunk remains a
// readable replica.
func (s *OpsServer) handleECConvert(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.ec == nil {
		writeJSONError(w, http.StatusNotImplemented, "EC conversion unsupported by this engine")
		return
	}
	idStr := r.URL.Query().Get("chunk_id")
	if idStr == "" {
		writeJSONError(w, http.StatusBadRequest, "chunk_id required")
		return
	}
	var chunkID metadata.ChunkID
	if _, err := fmt.Sscanf(idStr, "%d", &chunkID); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid chunk_id")
		return
	}
	var gen uint64
	fmt.Sscanf(r.URL.Query().Get("generation"), "%d", &gen)

	st, err := s.ec.ConvertToEC(r.Context(), chunkID, gen)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError,
			fmt.Sprintf("ec convert: %v", err))
		return
	}
	writeJSON(w, map[string]interface{}{
		"stripe_id":         st.StripeID,
		"state":             st.State.String(),
		"converted_at":      st.ConvertedAt,
		"original_checksum": st.OriginalChecksum,
		"shards":            len(st.Shards),
	})
}

func writeJSON(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

func writeJSONError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

// handleHTTPDrain stops accepting new writes and waits for in-flight
// writes to complete, then resumes writes. A point-in-time quiesce: it
// confirms the moment in-flight writes have drained (for a rolling restart,
// the operator restarts the process right after; the barrier must NOT stay
// held or the store would be permanently write-blocked). The returned
// release is invoked once "drained" has been reported so writes resume.
func (s *OpsServer) handleHTTPDrain(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	drain, ok := s.store.(DrainOps)
	if !ok {
		writeJSONError(w, http.StatusNotImplemented, "drain unsupported by this engine")
		return
	}
	release, err := drain.DrainWrites(ctx)
	if err != nil {
		writeJSONError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	// Report drained first, then hand the barrier back so a concurrent write
	// never observes the response before writes are permitted again. If a
	// rolling restart follows, the process exits before this matters.
	writeJSON(w, map[string]interface{}{"status": "drained"})
	if release != nil {
		release()
	}
}

// handleHTTPVerifyDisk verifies checksums of all chunks on a specific disk.
func (s *OpsServer) handleHTTPVerifyDisk(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	dir := r.URL.Query().Get("dir")
	if dir == "" {
		writeJSONError(w, http.StatusBadRequest, "path required")
		return
	}
	targetIdx := DiskIndexByDir(s.store.DiskInfos(), dir)
	if targetIdx < 0 {
		writeJSONError(w, http.StatusNotFound, "dir not found")
		return
	}

	type chunkResult struct {
		ChunkID metadata.ChunkID `json:"chunk_id"`
		Valid   bool             `json:"valid"`
		Error   string           `json:"error,omitempty"`
	}

	var results []chunkResult
	var verified, corrupted, failed int
	for _, info := range s.store.ListChunks() {
		if info.DiskIndex != targetIdx {
			continue
		}
		valid, _, err := s.store.VerifyChunkData(info.ChunkID)
		if err != nil {
			failed++
			results = append(results, chunkResult{ChunkID: info.ChunkID, Valid: false, Error: err.Error()})
		} else if valid {
			verified++
			results = append(results, chunkResult{ChunkID: info.ChunkID, Valid: true})
		} else {
			corrupted++
			results = append(results, chunkResult{ChunkID: info.ChunkID, Valid: false, Error: "checksum mismatch"})
		}
	}

	writeJSON(w, map[string]interface{}{
		"dir":       dir,
		"total":     verified + corrupted + failed,
		"verified":  verified,
		"corrupted": corrupted,
		"failed":    failed,
		"chunks":    results,
	})
}

// handleHTTPConfig reads DynamicConfig at runtime (read-only).
// Config updates go through the admin API which calls the metadata
// service's HTTP endpoint directly.
func (s *OpsServer) handleHTTPConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "use admin API to update config", http.StatusMethodNotAllowed)
		return
	}
	// Return a snapshot of current placement config from the store's
	// placement engine. The full DynamicConfig lives on the metadata
	// service; this endpoint returns the locally visible config.
	writeJSON(w, map[string]interface{}{
		"message": "config is managed by the metadata service",
		"hint":    "use admin API to update config at runtime",
	})
}
