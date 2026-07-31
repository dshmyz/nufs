package api

import (
	"encoding/json"
	"net/http"
)

func (r *Router) handleWriteOps(w http.ResponseWriter, req *http.Request, clusterID string, subpath []string) {
	switch {
	case len(subpath) == 1 && subpath[0] == "status":
		if req.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var status map[string]interface{}
		if err := r.proxy.Get(req.Context(), clusterID, "/api/v1/write-ops/status", &status); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		status["cluster"] = clusterID
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(status)
	default:
		http.Error(w, "invalid write-ops path", http.StatusBadRequest)
	}
}
