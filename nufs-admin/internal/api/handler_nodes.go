// Package api provides HTTP handlers and routing.
package api

import (
	"encoding/json"
	"net/http"
)

// handleNodes handles /clusters/{id}/nodes endpoints.
func (r *Router) handleNodes(w http.ResponseWriter, req *http.Request, clusterID string, subpath []string) {
	switch {
	case len(subpath) == 0:
		// GET /clusters/{id}/nodes - list nodes
		if req.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var nodes []map[string]interface{}
		if err := r.proxy.Get(req.Context(), clusterID, "/api/v1/nodes", &nodes); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}

		// Add cluster field + normalize state → status (metad returns numeric
		// NodeState: 0=online, 1=offline; the console reads a string status).
		for _, node := range nodes {
			node["cluster"] = clusterID
			switch st, _ := node["state"].(float64); st {
			case 0:
				node["status"] = "online"
			case 1:
				node["status"] = "offline"
			default:
				node["status"] = "unknown"
			}
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(nodes)

	case len(subpath) == 2 && subpath[1] == "decommission":
		// POST /clusters/{id}/nodes/{node-id}/decommission
		if req.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		nodeID := subpath[0]
		path := "/api/v1/nodes/" + nodeID + "/decommission"

		if err := r.proxy.Post(req.Context(), clusterID, path, req.Body, nil); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}

		w.WriteHeader(http.StatusOK)

	default:
		http.Error(w, "invalid nodes path", http.StatusBadRequest)
	}
}