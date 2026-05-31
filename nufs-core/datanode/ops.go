package datanode

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/example/dfs/metadata"
)

// ============================================================
// Operations API — Production Cluster Management HTTP Interface
// ============================================================

// OpsServer exposes HTTP endpoints for cluster operations.
type OpsServer struct {
	cfg      Config
	store    *ChunkStore
	meta     metadata.MetadataService
	disk     *DiskManager
	repl     *ChainReplicator
	ae       *AntiEntropy
	listener *http.Server
	running  atomic.Bool
}

// NewOpsServer creates the operations HTTP server.
func NewOpsServer(cfg Config, store *ChunkStore, meta metadata.MetadataService,
	disk *DiskManager, repl *ChainReplicator, ae *AntiEntropy) *OpsServer {
	mux := http.NewServeMux()

	s := &OpsServer{
		cfg:   cfg,
		store: store,
		meta:  meta,
		disk:  disk,
		repl:  repl,
		ae:    ae,
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
	mux.HandleFunc("/api/v1/metrics", s.handleMetrics)
	mux.HandleFunc("/api/v1/health", s.handleHealth)

	s.listener = &http.Server{
		Addr:    cfg.OpsListenAddr,
		Handler: mux,
	}
	return s
}

// Start begins listening for operations requests.
func (s *OpsServer) Start() error {
	if !s.running.Swap(true) {
		log.Printf("ops: starting management API on %s", s.cfg.OpsListenAddr)
		go func() {
			if err := s.listener.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Printf("ops: HTTP server error: %v", err)
			}
		}()
	}
	return nil
}

// Stop shuts down the operations server.
func (s *OpsServer) Stop() {
	if s.running.Swap(false) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s.listener.Shutdown(ctx)
		log.Printf("ops: management API stopped")
	}
}

// --- Handlers ---

type ClusterStatus struct {
	NodeID      uint64             `json:"node_id"`
	State       metadata.NodeState `json:"state"`
	Addr        string             `json:"addr"`
	DiskStats   DiskStatsSnapshot    `json:"disk_stats"`
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
			log.Printf("gc: orphan chunk %d (%d bytes) not in metadata, deleting", lc.ChunkID, lc.Size)
			if delErr := s.store.Delete(lc.ChunkID); delErr != nil {
				log.Printf("gc: failed to delete orphan chunk %d: %v", lc.ChunkID, delErr)
			} else {
				orphanCount++
			}
		}
	}

	if orphanCount > 0 {
		log.Printf("gc: scan complete, deleted %d orphan chunks out of %d local chunks", orphanCount, len(localChunks))
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
