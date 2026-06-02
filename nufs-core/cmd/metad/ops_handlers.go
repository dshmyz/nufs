package main

import (
	"encoding/json"
	"net/http"

	"github.com/example/dfs/metadata"
)

// opsHandlers holds the dependencies for the HTTP ops API. Methods on
// this type are defined across ops_handlers.go (this file),
// ops_buckets.go, ops_nodes.go, ops_chunks.go, ops_namespace.go and
// ops_repair.go, grouped by resource domain.
type opsHandlers struct {
	store  *metadata.PebbleStore
	bundle *metadata.ServiceBundle
}

// registerOpsHandlers wires every endpoint in the ops API. The list
// is grouped by resource domain so that adding a new endpoint means
// adding the route here and the handler in the matching ops_*.go
// file.
func registerOpsHandlers(mux *http.ServeMux, store *metadata.PebbleStore, bundle *metadata.ServiceBundle) {
	s := &opsHandlers{store: store, bundle: bundle}

	// Health & cluster
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/ready", s.handleReady)
	mux.HandleFunc("/api/v1/cluster/status", s.handleClusterStatus)
	mux.HandleFunc("/api/v1/metrics", s.handleMetrics)

	// Buckets
	mux.HandleFunc("/api/v1/buckets", s.handleBuckets)
	mux.HandleFunc("/api/v1/buckets/", s.handleBucketByID)

	// Nodes
	mux.HandleFunc("/api/v1/nodes", s.handleNodes)
	mux.HandleFunc("/api/v1/nodes/", s.handleNodesByID)

	// Chunks
	mux.HandleFunc("/api/v1/chunks", s.handleChunks)
	mux.HandleFunc("/api/v1/chunks/", s.handleChunksByID)
	mux.HandleFunc("/api/v1/chunks/migrate-replica", s.handleMigrateReplica)
	mux.HandleFunc("/api/v1/chunks/report-state", s.handleReportChunkState)

	// Namespace + inodes
	mux.HandleFunc("/api/v1/namespace/mkdir", s.handleMkDir)
	mux.HandleFunc("/api/v1/namespace/rmdir", s.handleRmDir)
	mux.HandleFunc("/api/v1/namespace/readdir", s.handleReadDir)
	mux.HandleFunc("/api/v1/namespace/createfile", s.handleCreateFile)
	mux.HandleFunc("/api/v1/namespace/unlink", s.handleUnlink)
	mux.HandleFunc("/api/v1/namespace/lookup", s.handleLookup)
	mux.HandleFunc("/api/v1/namespace/rename", s.handleRename)
	mux.HandleFunc("/api/v1/namespace/symlink", s.handleSymlink)
	mux.HandleFunc("/api/v1/namespace/readlink", s.handleReadlink)
	mux.HandleFunc("/api/v1/namespace/link", s.handleLink)
	mux.HandleFunc("/api/v1/inodes/", s.handleInodesByID)

	// Repair + rebalance
	mux.HandleFunc("/api/v1/repair/queue", s.handleRepairQueue)
	mux.HandleFunc("/api/v1/repair/trigger", s.handleTriggerRepair)
	mux.HandleFunc("/api/v1/repair/", s.handleRepairByID)
	mux.HandleFunc("/api/v1/rebalance/trigger", s.handleTriggerRebalance)
}

// --- Health, cluster, metrics ---

func (h *opsHandlers) handleHealth(w http.ResponseWriter, _ *http.Request) {
	if h.bundle.IsReady() {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"healthy"}`))
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte(`{"status":"initializing"}`))
	}
}

func (h *opsHandlers) handleReady(w http.ResponseWriter, _ *http.Request) {
	if h.bundle.IsReady() {
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
}

func (h *opsHandlers) handleClusterStatus(w http.ResponseWriter, _ *http.Request) {
	status := map[string]interface{}{
		"is_leader":  h.store.IsLeader(),
		"version":    "0.2.0",
		"leader_uri": h.store.LeaderAddr(),
	}
	writeJSON(w, status)
}

func (h *opsHandlers) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	if h.bundle.Metrics != nil {
		writeJSON(w, h.bundle.Metrics.Snapshot())
	} else {
		writeJSON(w, map[string]string{"status": "no metrics"})
	}
}

// --- JSON helpers ---

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.Encode(v)
}

func writeJSONError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
