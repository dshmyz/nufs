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

	"github.com/example/dfs/internal/httputil"
	"github.com/example/dfs/internal/tlsutil"
	"github.com/example/dfs/internal/version"
	"github.com/example/dfs/metadata"
)

// ============================================================
// Operations API — Production Cluster Management HTTP Interface
// ============================================================

// OpsServer exposes HTTP endpoints for cluster operations.
type OpsServer struct {
	cfg        Config
	store      *ChunkStore
	meta       metadata.MetadataService
	disk       *DiskManager
	repl       *ParallelReplicator
	ae         *AntiEntropy
	repair     *RepairWorker
	listener   *http.Server
	running    atomic.Bool
	shutdownWg sync.WaitGroup
}

// NewOpsServer creates the operations HTTP server.
func NewOpsServer(cfg Config, store *ChunkStore, meta metadata.MetadataService,
	disk *DiskManager, repl *ParallelReplicator, ae *AntiEntropy) *OpsServer {
	return NewOpsServerWithRepair(cfg, store, meta, disk, repl, ae, nil)
}

// NewOpsServerWithRepair creates an ops server with repair worker integration.
func NewOpsServerWithRepair(cfg Config, store *ChunkStore, meta metadata.MetadataService,
	disk *DiskManager, repl *ParallelReplicator, ae *AntiEntropy, repair *RepairWorker) *OpsServer {
	mux := http.NewServeMux()

	s := &OpsServer{
		cfg:    cfg,
		store:  store,
		meta:   meta,
		disk:   disk,
		repl:   repl,
		ae:     ae,
		repair: repair,
	}

	// Wire disk failure callback to trigger repair for chunks on failed disk
	if repair != nil {
		disk.SetOnDiskFailed(func(diskID string) {
			slog.Warn("ops: disk failed, triggering chunk repairs", "diskID", diskID)
			if err := repair.RepairChunksForDiskFailure(context.Background(), diskID); err != nil {
				slog.Error("ops: disk failure repair failed", "error", err)
			}
		})
	}

	// Cluster status
	mux.HandleFunc("/api/v1/cluster/status", s.handleClusterStatus)

	// Node management
	mux.HandleFunc("/api/v1/nodes", s.handleNodes)
	mux.HandleFunc("/api/v1/nodes/{id}/decommission", s.handleDecommission)

	// Bucket operations
	mux.HandleFunc("/api/v1/buckets", s.handleBuckets)

	// Chunk operations
	mux.HandleFunc("/api/v1/chunks/{id}/verify", s.handleVerifyChunk)

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
			Handler: httputil.BearerAuth(cfg.OpsAuthToken, public, mux),
		}
	} else {
		s.listener = &http.Server{
			Addr:    cfg.OpsListenAddr,
			Handler: mux,
		}
	}
	return s
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
	}
	return nil
}

// Stop shuts down the operations server.
func (s *OpsServer) Stop() {
	if s.running.Swap(false) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s.listener.Shutdown(ctx)
		slog.Info("ops: management API stopped")
	}
}

// --- Handlers ---

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
	writes, errors, avgLatency := s.repl.Stats()
	scanned, mismatches, repaired := s.ae.Stats()

	status := ClusterStatus{
		NodeID:    uint64(s.cfg.NodeID),
		State:     metadata.NodeOnline,
		Addr:      s.cfg.ListenAddr,
		DiskStats: s.disk.Stats(),
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

	// Trigger orphan chunk scan
	orphanCount, err := s.triggerGCScan(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.writeJSON(w, map[string]interface{}{
		"orphan_chunks": orphanCount,
		"status":        "scan_complete",
	})
}

func (s *OpsServer) triggerGCScan(ctx context.Context) (int, error) {
	// Iterate local chunks and check if each exists in metadata.
	// Chunks not found in metadata are orphans and should be deleted.
	localChunks := s.store.ListChunks()
	orphanCount := 0

	for _, lc := range localChunks {
		if ctx.Err() != nil {
			break
		}
		_, err := s.meta.GetChunk(ctx, lc.ChunkID)
		if err != nil {
			// Chunk not in metadata — it's an orphan
			slog.Info("gc: orphan chunk not in metadata, deleting", "chunkID", lc.ChunkID, "size", lc.Size)
			if delErr := s.store.Delete(lc.ChunkID); delErr != nil {
				slog.Error("gc: failed to delete orphan chunk", "chunkID", lc.ChunkID, "error", delErr)
			} else {
				orphanCount++
			}
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
	stats := s.disk.Stats()
	writes, replErrors, avgLatency := s.repl.Stats()
	scanned, mismatches, repaired := s.ae.Stats()
	totalBytes, chunkCount := s.store.Stats()

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

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	_, _ = w.Write([]byte(sb.String()))
}

func (s *OpsServer) handleMetrics(w http.ResponseWriter, r *http.Request) {
	writes, errors, avgLatency := s.repl.Stats()
	scanned, mismatches, repaired := s.ae.Stats()

	m := OpsMetrics{}
	m.Disk = s.disk.Stats()
	m.Cache.ChunkCount = s.store.chunkCount.Load()
	m.Cache.UsedBytes = s.store.totalBytes.Load()
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
	disk := s.disk.Stats()
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
	stats := s.disk.Stats()
	alertLevel := AlertLevel(s.disk.alertFired.Load())
	s.writeJSON(w, map[string]interface{}{
		"alert_level": alertLevel.String(),
		"usage_pct":   fmt.Sprintf("%.1f%%", stats.UsagePct*100),
		"used_bytes":  stats.UsedBytes,
		"total_bytes": stats.TotalBytes,
		"avail_bytes": stats.AvailBytes,
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
