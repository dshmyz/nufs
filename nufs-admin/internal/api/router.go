// Package api provides HTTP handlers and routing.
package api

import (
	"net/http"
	"strings"

	"github.com/dshmyz/nufs/nufs-admin/internal/auth"
	"github.com/dshmyz/nufs/nufs-admin/internal/cluster"
	"github.com/dshmyz/nufs/nufs-admin/internal/proxy"
)

// Router sets up HTTP routing with middleware.
type Router struct {
	proxy      *proxy.Proxy
	aggregator *proxy.Aggregator
	jwt        *auth.JWTManager
	users      *auth.UserStore
	registry   *cluster.Registry
}

// NewRouter creates a router with dependencies.
func NewRouter(proxy *proxy.Proxy, aggregator *proxy.Aggregator, jwt *auth.JWTManager, users *auth.UserStore, registry *cluster.Registry) *Router {
	return &Router{
		proxy:      proxy,
		aggregator: aggregator,
		jwt:        jwt,
		users:      users,
		registry:   registry,
	}
}

// Setup registers all routes on a mux.
func (r *Router) Setup(mux *http.ServeMux) {
	// Auth endpoints (no JWT required)
	mux.HandleFunc("/api/v1/auth/login", r.handleLogin)

	// Protected API endpoints
	protected := auth.Middleware(r.jwt)

	// Cluster management (CRUD via UI)
	mux.Handle("/api/v1/admin/clusters", protected(http.HandlerFunc(r.handleClusterManage)))
	mux.Handle("/api/v1/admin/clusters/audit", protected(http.HandlerFunc(r.handleClusterAuditLogs)))

	// Cluster list and global overview
	mux.Handle("/api/v1/clusters", protected(http.HandlerFunc(r.handleListClusters)))
	mux.Handle("/api/v1/clusters/all/overview", protected(http.HandlerFunc(r.handleGlobalOverview)))

	// Single cluster endpoints (includes /api/v1/admin/clusters/{id} for DELETE/PUT)
	mux.Handle("/api/v1/admin/clusters/", protected(http.HandlerFunc(r.handleAdminClusterRoutes)))
	mux.Handle("/api/v1/clusters/", protected(http.HandlerFunc(r.handleClusterRoutes)))
}

// handleAdminClusterRoutes dispatches admin operations on a specific cluster.
// Path format: /api/v1/admin/clusters/{cluster-id}
func (r *Router) handleAdminClusterRoutes(w http.ResponseWriter, req *http.Request) {
	path := req.URL.Path
	// /api/v1/admin/clusters/{id}
	trimmed := strings.TrimPrefix(path, "/api/v1/admin/clusters/")
	if trimmed == "" || trimmed == path {
		http.Error(w, "invalid path", http.StatusBadRequest)
		return
	}

	clusterID := trimmed
	r.handleClusterItem(w, req, clusterID)
}

// handleClusterRoutes dispatches to cluster-specific handlers.
func (r *Router) handleClusterRoutes(w http.ResponseWriter, req *http.Request) {
	// Path format: /api/v1/clusters/{cluster-id}/{resource}
	path := req.URL.Path
	parts := splitPath(path)

	if len(parts) < 4 {
		http.Error(w, "invalid path", http.StatusBadRequest)
		return
	}

	clusterID := parts[3]

	// Dispatch based on remaining path
	if len(parts) == 4 {
		// /api/v1/clusters/{id}
		r.handleClusterOverview(w, req, clusterID)
		return
	}

	resource := parts[4]
	switch resource {
	case "nodes":
		r.handleNodes(w, req, clusterID, parts[5:])
	case "datanode":
		r.handleDatanodeOps(w, req, clusterID, parts[5:])
	case "buckets":
		r.handleBuckets(w, req, clusterID, parts[5:])
	case "chunks":
		r.handleChunks(w, req, clusterID, parts[5:])
	case "repair":
		r.handleRepair(w, req, clusterID, parts[5:])
	case "write-ops":
		r.handleWriteOps(w, req, clusterID, parts[5:])
	case "gc":
		r.handleGC(w, req, clusterID, parts[5:])
	case "rebalance":
		r.handleRebalance(w, req, clusterID, parts[5:])
	case "raft":
		r.handleRaft(w, req, clusterID, parts[5:])
	case "readiness":
		r.handleClusterReadiness(w, req, clusterID)
	case "audit":
		r.handleAudit(w, req, clusterID, parts[5:])
	default:
		http.Error(w, "unknown resource", http.StatusNotFound)
	}
}

func splitPath(path string) []string {
	var parts []string
	for _, p := range split(path, '/') {
		if p != "" {
			parts = append(parts, p)
		}
	}
	return parts
}

func split(s string, sep byte) []string {
	var parts []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == sep {
			parts = append(parts, s[start:i])
			start = i + 1
		}
	}
	parts = append(parts, s[start:])
	return parts
}
