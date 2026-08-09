// Package api provides HTTP handlers and routing.
package api

import (
	"encoding/json"
	"net/http"

	"github.com/dshmyz/nufs/nufs-admin/internal/auth"
	"github.com/dshmyz/nufs/nufs-admin/internal/cluster"
	"github.com/dshmyz/nufs/nufs-admin/internal/store"
)

// handleClusterManage handles cluster CRUD (add/remove/update dynamic clusters).
// This is registered on /api/v1/admin/clusters.
func (r *Router) handleClusterManage(w http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case http.MethodGet:
		r.handleListClusters(w, req)
	case http.MethodPost:
		r.handleAddCluster(w, req)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleAddCluster adds a new dynamic cluster via UI.
func (r *Router) handleAddCluster(w http.ResponseWriter, req *http.Request) {
	var body struct {
		ID          string `json:"id"`
		Region      string `json:"region"`
		MetadOpsURL string `json:"metad_ops_url"`
		Description string `json:"description"`
	}

	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if body.ID == "" || body.MetadOpsURL == "" {
		http.Error(w, "id and metad_ops_url are required", http.StatusBadRequest)
		return
	}

	// Get operator from JWT claims
	claims := auth.GetClaims(req.Context())
	operator := "unknown"
	if claims != nil {
		operator = claims.Username
	}

	rec := store.ClusterRecord{
		ID:          body.ID,
		Region:      body.Region,
		MetadOpsURL: body.MetadOpsURL,
		Description: body.Description,
	}

	if err := r.registry.AddDynamic(req.Context(), rec, operator); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{
		"status": "created",
		"id":     body.ID,
	})
}

// handleClusterItem handles operations on a specific cluster (remove/update).
// Registered on /api/v1/admin/clusters/{id}.
func (r *Router) handleClusterItem(w http.ResponseWriter, req *http.Request, clusterID string) {
	switch req.Method {
	case http.MethodDelete:
		r.handleRemoveCluster(w, req, clusterID)
	case http.MethodPut:
		r.handleUpdateCluster(w, req, clusterID)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleRemoveCluster removes a dynamic cluster via UI.
func (r *Router) handleRemoveCluster(w http.ResponseWriter, req *http.Request, clusterID string) {
	claims := auth.GetClaims(req.Context())
	operator := "unknown"
	if claims != nil {
		operator = claims.Username
	}

	if err := r.registry.RemoveDynamic(req.Context(), clusterID, operator); err != nil {
		if err == cluster.ErrStoreNotConfigured {
			http.Error(w, err.Error(), http.StatusNotImplemented)
			return
		}
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status": "deleted",
		"id":     clusterID,
	})
}

// handleUpdateCluster updates a dynamic cluster via UI.
func (r *Router) handleUpdateCluster(w http.ResponseWriter, req *http.Request, clusterID string) {
	var body struct {
		Region      string `json:"region"`
		MetadOpsURL string `json:"metad_ops_url"`
		Description string `json:"description"`
	}

	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	claims := auth.GetClaims(req.Context())
	operator := "unknown"
	if claims != nil {
		operator = claims.Username
	}

	rec := store.ClusterRecord{
		ID:          clusterID,
		Region:      body.Region,
		MetadOpsURL: body.MetadOpsURL,
		Description: body.Description,
	}

	if err := r.registry.UpdateDynamic(req.Context(), rec, operator); err != nil {
		if err == cluster.ErrStoreNotConfigured {
			http.Error(w, err.Error(), http.StatusNotImplemented)
			return
		}
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status": "updated",
		"id":     clusterID,
	})
}

// handleClusterAuditLogs returns cluster change audit logs.
func (r *Router) handleClusterAuditLogs(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	limit := 50
	offset := 0

	logs, err := r.registry.ListAuditLogs(req.Context(), limit, offset)
	if err != nil {
		if err == cluster.ErrStoreNotConfigured {
			http.Error(w, err.Error(), http.StatusNotImplemented)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(logs)
}