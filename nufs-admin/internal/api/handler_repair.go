// Package api provides HTTP handlers and routing.
package api

import (
	"encoding/json"
	"net/http"
)

// handleRepair handles repair, GC, and rebalance endpoints.
func (r *Router) handleRepair(w http.ResponseWriter, req *http.Request, clusterID string, subpath []string) {
	switch {
	case len(subpath) == 1 && subpath[0] == "trigger":
		// POST /clusters/{id}/repair/trigger
		if req.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		if err := r.proxy.Post(req.Context(), clusterID, "/api/v1/repair/trigger", req.Body, nil); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}

		w.WriteHeader(http.StatusOK)

	case len(subpath) == 1 && subpath[0] == "queue":
		// GET /clusters/{id}/repair/queue
		if req.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var queue map[string]interface{}
		if err := r.proxy.Get(req.Context(), clusterID, "/api/v1/repair/queue", &queue); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}

		queue["cluster"] = clusterID

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(queue)

	default:
		http.Error(w, "invalid repair path", http.StatusBadRequest)
	}
}

// handleGC handles GC scan endpoints.
func (r *Router) handleGC(w http.ResponseWriter, req *http.Request, clusterID string, subpath []string) {
	switch {
	case len(subpath) == 1 && subpath[0] == "scan":
		// POST /clusters/{id}/gc/scan
		if req.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		if err := r.proxy.Post(req.Context(), clusterID, "/api/v1/gc/scan", req.Body, nil); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}

		w.WriteHeader(http.StatusOK)

	default:
		http.Error(w, "invalid gc path", http.StatusBadRequest)
	}
}

// handleRebalance handles rebalance endpoints.
func (r *Router) handleRebalance(w http.ResponseWriter, req *http.Request, clusterID string, subpath []string) {
	switch {
	case len(subpath) == 1 && subpath[0] == "trigger":
		// POST /clusters/{id}/rebalance/trigger
		if req.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		if err := r.proxy.Post(req.Context(), clusterID, "/api/v1/rebalance/trigger", req.Body, nil); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}

		w.WriteHeader(http.StatusOK)

	default:
		http.Error(w, "invalid rebalance path", http.StatusBadRequest)
	}
}