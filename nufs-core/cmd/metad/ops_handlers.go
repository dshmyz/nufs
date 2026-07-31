package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"

	"github.com/example/dfs/internal/version"
	"github.com/example/dfs/metadata"
)

// opsHandlers holds the dependencies for the HTTP ops API. Methods on
// this type are defined across ops_handlers.go (this file),
// ops_buckets.go, ops_nodes.go, ops_chunks.go, ops_namespace.go and
// ops_repair.go, grouped by resource domain.
type opsHandlers struct {
	store  *metadata.PebbleStore
	bundle *metadata.ServiceBundle

	backupCoordinator backupOpsCoordinator
	backupRepository  backupVerifierRepository

	// advertiseOpsAddr is our own ops HTTP URL (e.g. "http://10.0.0.1:8091").
	// Followers that receive mutating requests return 307 to the leader's
	// ops address so the caller can retry on the correct node.
	advertiseOpsAddr string
}

// requireLeader checks if this node is the Raft leader. If not, it
// sends an HTTP 307 redirect to the leader's ops address and returns
// false. Callers (mutating handlers) should return immediately after
// this returns false. Read-only handlers can skip the check.
func (h *opsHandlers) requireLeader(w http.ResponseWriter, r *http.Request) bool {
	if h.store.IsLeader() {
		return true
	}
	leaderAddr := h.store.LeaderOpsAddr()
	if leaderAddr == "" {
		writeJSONError(w, http.StatusServiceUnavailable, "no leader available")
		return false
	}
	target := leaderRedirectTarget(leaderAddr, r.URL)
	http.Redirect(w, r, target, http.StatusTemporaryRedirect)
	return false
}

func leaderRedirectTarget(leaderAddr string, requestURL *url.URL) string {
	target := leaderAddr + requestURL.EscapedPath()
	if requestURL.RawQuery != "" {
		target += "?" + requestURL.RawQuery
	}
	return target
}

// registerOpsHandlers wires every endpoint in the ops API. The list
// is grouped by resource domain so that adding a new endpoint means
// adding the route here and the handler in the matching ops_*.go
// file.
func registerOpsHandlers(mux *http.ServeMux, store *metadata.PebbleStore, bundle *metadata.ServiceBundle, advertiseOpsAddr string, backupDeps ...backupOpsDependency) {
	s := &opsHandlers{store: store, bundle: bundle, advertiseOpsAddr: advertiseOpsAddr}
	if len(backupDeps) > 0 {
		s.backupCoordinator = backupDeps[0].coordinator
		s.backupRepository = backupDeps[0].repository
	}

	// helper: wrap a handler with leader check
	mut := func(fn http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if !s.requireLeader(w, r) {
				return
			}
			fn(w, r)
		}
	}

	// Health & cluster — always served, no leader check
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/healthz", s.handleHealth)
	mux.HandleFunc("/ready", s.handleReady)
	mux.HandleFunc("/api/v1/health", s.handleHealth)
	mux.HandleFunc("/api/v1/cluster/status", s.handleClusterStatus)
	mux.HandleFunc("/api/v1/cluster/readiness", s.handleClusterReadiness)
	mux.HandleFunc("/api/v1/cluster/balance", s.handleClusterBalance)
	mux.HandleFunc("/api/v1/metrics", s.handleMetrics)

	// Admin — read-only, no leader check
	mux.HandleFunc("/api/v1/admin/bucket-usage", s.handleComputeAllBucketUsage)

	// Backups (GET routes work on followers; POST routes enter the leader boundary)
	mux.HandleFunc("/api/v1/backups/status", s.handleBackupStatus)
	mux.HandleFunc("/api/v1/backups/prune", mut(s.handleBackupPrune))
	mux.HandleFunc("/api/v1/backups", s.handleBackups)
	mux.HandleFunc("/api/v1/backups/", mut(s.handleBackupByID))

	// Buckets (mutating: POST/PUT/DELETE; read: GET uses same handler that method-switches)
	mux.HandleFunc("/api/v1/buckets", mut(s.handleBuckets))
	mux.HandleFunc("/api/v1/buckets/", mut(s.handleBucketByID))

	// Nodes (register/heartbeat/decommission are mutating)
	mux.HandleFunc("/api/v1/nodes", mut(s.handleNodes))
	mux.HandleFunc("/api/v1/nodes/", mut(s.handleNodesByID))

	// Chunks
	mux.HandleFunc("/api/v1/chunks", mut(s.handleChunks))
	mux.HandleFunc("/api/v1/chunks/batch", mut(s.handleChunksBatch))
	mux.HandleFunc("/api/v1/chunks/", mut(s.handleChunksByID))
	mux.HandleFunc("/api/v1/chunks/migrate-replica", mut(s.handleMigrateReplica))
	mux.HandleFunc("/api/v1/chunks/report-state", mut(s.handleReportChunkState))

	// Watch — long-poll SSE stream of metadata change events
	// (doesn't need leader check; followers already consume from their local EventBus)
	mux.HandleFunc("/api/v1/watch", s.handleWatch)

	// Namespace — readdir/lookup/readlink are read-only, no leader check
	mux.HandleFunc("/api/v1/namespace/mkdir", mut(s.handleMkDir))
	mux.HandleFunc("/api/v1/namespace/rmdir", mut(s.handleRmDir))
	mux.HandleFunc("/api/v1/namespace/readdir", s.handleReadDir)
	mux.HandleFunc("/api/v1/namespace/createfile", mut(s.handleCreateFile))
	mux.HandleFunc("/api/v1/namespace/unlink", mut(s.handleUnlink))
	mux.HandleFunc("/api/v1/namespace/lookup", s.handleLookup)
	mux.HandleFunc("/api/v1/namespace/rename", mut(s.handleRename))
	mux.HandleFunc("/api/v1/namespace/symlink", mut(s.handleSymlink))
	mux.HandleFunc("/api/v1/namespace/readlink", s.handleReadlink)
	mux.HandleFunc("/api/v1/namespace/link", mut(s.handleLink))
	mux.HandleFunc("/api/v1/inodes/", s.handleInodesByID) // read-only

	// Repair + rebalance (all mutating)
	mux.HandleFunc("/api/v1/repair/queue", s.handleRepairQueue) // read-only
	mux.HandleFunc("/api/v1/repair/trigger", mut(s.handleTriggerRepair))
	mux.HandleFunc("/api/v1/repair/", mut(s.handleRepairByID))
	mux.HandleFunc("/api/v1/rebalance/trigger", mut(s.handleTriggerRebalance))

	// Object write recovery state and background task coordination
	mux.HandleFunc("/api/v1/write-attempts", s.handleWriteAttempts)
	mux.HandleFunc("/api/v1/write-attempts/", mut(s.handleWriteAttemptByID))
	mux.HandleFunc("/api/v1/write-ops/status", s.handleWriteOpsStatus)
	mux.HandleFunc("/api/v1/background-tasks/lease", mut(s.handleLeaseBackgroundTask))
	mux.HandleFunc("/api/v1/background-tasks/", mut(s.handleBackgroundTaskByID))

	// Advisory file locks (acquire/release are mutating, list is read-only)
	mux.HandleFunc("/api/v1/locks/acquire", mut(s.handleAdvisoryAcquire))
	mux.HandleFunc("/api/v1/locks/release", mut(s.handleAdvisoryRelease))
	mux.HandleFunc("/api/v1/locks", s.handleAdvisoryList)

	// Scrub — data consistency check
	mux.HandleFunc("/api/v1/scrub", s.handleScrub)

	// Audit — query audit trail
	mux.HandleFunc("/api/v1/audit", s.handleAudit)
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
		"version":    version.Version,
		"leader_uri": h.store.LeaderAddr(),
	}
	writeJSON(w, status)
}

// handleClusterReadiness returns a comprehensive cluster readiness report
// that answers: can the cluster serve writes, how many chunks are
// under-replicated, and is the leader stable?
func (h *opsHandlers) handleClusterReadiness(w http.ResponseWriter, _ *http.Request) {
	if h.bundle.Health == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "health checker not configured")
		return
	}
	r := h.bundle.Health.ComputeClusterReadiness()
	w.Header().Set("Content-Type", "application/json")
	if r.Status == "not_ready" {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	json.NewEncoder(w).Encode(r)
}

// handleClusterBalance returns per-node and per-disk capacity utilization,
// plus an aggregate imbalance score. This lets operators see at a glance
// whether rebalancing is needed.
func (h *opsHandlers) handleClusterBalance(w http.ResponseWriter, _ *http.Request) {
	ctx := context.Background()
	nodes, err := h.store.ListNodes(ctx)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if len(nodes) == 0 {
		writeJSON(w, map[string]interface{}{"nodes": []interface{}{}, "imbalance": 0})
		return
	}

	type nodeBalance struct {
		ID       metadata.NodeID `json:"id"`
		Addr     string          `json:"addr"`
		CapGB    int64           `json:"capacity_gb"`
		UsedGB   int64           `json:"used_gb"`
		UsedPct  float64         `json:"used_pct"`
		Tier     string          `json:"tier"`
		Online   bool            `json:"online"`
	}

	var totalUsed, totalCap int64
	var minPct, maxPct float64
	minPct = 1.0
	nodeBalances := make([]nodeBalance, 0, len(nodes))

	for _, n := range nodes {
		capGB := n.CapacityGB
		usedGB := n.UsedGB
		var usedPct float64
		if capGB > 0 {
			usedPct = float64(usedGB) / float64(capGB)
		}
		totalUsed += usedGB
		totalCap += capGB
		if usedPct < minPct {
			minPct = usedPct
		}
		if usedPct > maxPct {
			maxPct = usedPct
		}
		nodeBalances = append(nodeBalances, nodeBalance{
			ID: n.ID, Addr: n.Addr, CapGB: capGB, UsedGB: usedGB,
			UsedPct: usedPct, Tier: tierName(n.Tier), Online: n.State == metadata.NodeOnline,
		})
	}

	imbalance := maxPct - minPct
	var recommendation string
	switch {
	case imbalance < 0.10:
		recommendation = "balanced"
	case imbalance < 0.25:
		recommendation = "slightly imbalanced"
	case imbalance < 0.50:
		recommendation = "imbalanced — consider rebalancing"
	default:
		recommendation = "severely imbalanced — rebalancing recommended"
	}

	var totalPct float64
	if totalCap > 0 {
		totalPct = float64(totalUsed) / float64(totalCap)
	}

	writeJSON(w, map[string]interface{}{
		"nodes":         nodeBalances,
		"total_used_gb": totalUsed,
		"total_cap_gb":  totalCap,
		"total_used_pct": totalPct,
		"imbalance":     imbalance,
		"min_used_pct":  minPct,
		"max_used_pct":  maxPct,
		"recommendation": recommendation,
	})
}

func (h *opsHandlers) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if h.bundle.Metrics != nil {
		data, _ := json.Marshal(h.bundle.Metrics.Snapshot())
		var metrics map[string]interface{}
		if err := json.Unmarshal(data, &metrics); err != nil {
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
		metrics["object_write_ops"] = h.writeOpsStatus(r.Context())
		writeJSON(w, metrics)
	} else {
		writeJSON(w, map[string]string{"status": "no metrics"})
	}
}

// handleComputeAllBucketUsage computes per-bucket usage server-side.
func (h *opsHandlers) handleComputeAllBucketUsage(w http.ResponseWriter, r *http.Request) {
	usage, err := h.store.ComputeAllBucketUsage(r.Context())
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, usage)
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

// writeJSONErrorC is like writeJSONError but includes a machine-readable
// "code" field so callers can match on it instead of fragile strings.
func writeJSONErrorC(w http.ResponseWriter, code int, errCode, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": msg, "code": errCode})
}

func tierName(t metadata.StorageTier) string {
	switch t {
	case metadata.TierHot:
		return "hot"
	case metadata.TierWarm:
		return "warm"
	case metadata.TierCold:
		return "cold"
	case metadata.TierArchive:
		return "archive"
	default:
		return "any"
	}
}
