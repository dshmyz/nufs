// Package api provides HTTP handlers and routing.
package api

import (
	"encoding/json"
	"net/http"
)

// handleBuckets handles /clusters/{id}/buckets endpoints.
func (r *Router) handleBuckets(w http.ResponseWriter, req *http.Request, clusterID string, subpath []string) {
	switch {
	case len(subpath) == 0:
		// GET /clusters/{id}/buckets - list buckets
		if req.Method == http.MethodGet {
			var buckets []map[string]interface{}
			if err := r.proxy.Get(req.Context(), clusterID, "/api/v1/buckets", &buckets); err != nil {
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				return
			}

			for _, bucket := range buckets {
				bucket["cluster"] = clusterID
			}

			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(buckets)
			return
		}

		// POST /clusters/{id}/buckets - create bucket
		if req.Method == http.MethodPost {
			if err := r.proxy.Post(req.Context(), clusterID, "/api/v1/buckets", req.Body, nil); err != nil {
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusOK)
			return
		}

		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)

	case len(subpath) == 1:
		// DELETE /clusters/{id}/buckets/{name}
		if req.Method == http.MethodDelete {
			bucketName := subpath[0]
			path := "/api/v1/buckets/" + bucketName

			if err := r.proxy.Delete(req.Context(), clusterID, path); err != nil {
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				return
			}

			w.WriteHeader(http.StatusOK)
			return
		}

		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)

	default:
		http.Error(w, "invalid buckets path", http.StatusBadRequest)
	}
}