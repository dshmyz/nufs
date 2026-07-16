// Package api provides HTTP handlers and routing.
package api

import (
	"encoding/json"
	"net/http"
)

// handleChunks handles /clusters/{id}/chunks endpoints.
func (r *Router) handleChunks(w http.ResponseWriter, req *http.Request, clusterID string, subpath []string) {
	switch {
	case len(subpath) == 2 && subpath[1] == "verify":
		// POST /clusters/{id}/chunks/{chunk-id}/verify
		if req.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		chunkID := subpath[0]
		path := "/api/v1/chunks/" + chunkID + "/verify"

		if err := r.proxy.Post(req.Context(), clusterID, path, req.Body, nil); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}

		w.WriteHeader(http.StatusOK)

	case len(subpath) == 1:
		// GET /clusters/{id}/chunks/{chunk-id}
		if req.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		chunkID := subpath[0]
		path := "/api/v1/chunks/" + chunkID

		var chunk map[string]interface{}
		if err := r.proxy.Get(req.Context(), clusterID, path, &chunk); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}

		chunk["cluster"] = clusterID

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(chunk)

	default:
		http.Error(w, "invalid chunks path", http.StatusBadRequest)
	}
}