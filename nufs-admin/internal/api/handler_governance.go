// Package api provides HTTP handlers and routing.
package api

import (
	"encoding/json"
	"net/http"
)

// handleRaft handles Raft status endpoints.
func (r *Router) handleRaft(w http.ResponseWriter, req *http.Request, clusterID string, subpath []string) {
	switch {
	case len(subpath) == 1 && subpath[0] == "status":
		// GET /clusters/{id}/raft/status
		if req.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var status map[string]interface{}
		if err := r.proxy.Get(req.Context(), clusterID, "/api/v1/cluster/status", &status); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}

		status["cluster"] = clusterID

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(status)

	default:
		http.Error(w, "invalid raft path", http.StatusBadRequest)
	}
}

// handleAudit handles audit log endpoints.
func (r *Router) handleAudit(w http.ResponseWriter, req *http.Request, clusterID string, subpath []string) {
	switch {
	case len(subpath) == 0:
		// GET /clusters/{id}/audit
		if req.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// Forward query parameters
		query := req.URL.Query().Encode()
		path := "/api/v1/audit"
		if query != "" {
			path += "?" + query
		}

		var logs []map[string]interface{}
		if err := r.proxy.Get(req.Context(), clusterID, path, &logs); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}

		for _, log := range logs {
			log["cluster"] = clusterID
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(logs)

	default:
		http.Error(w, "invalid audit path", http.StatusBadRequest)
	}
}