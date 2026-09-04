// Package api provides HTTP handlers and routing.
package api

import (
	"encoding/json"
	"fmt"
	"net/http"
)

// handleLogin authenticates user and returns JWT.
func (r *Router) handleLogin(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var creds struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}

	if err := json.NewDecoder(req.Body).Decode(&creds); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if err := r.users.Authenticate(creds.Username, creds.Password); err != nil {
		http.Error(w, "invalid credentials", http.StatusUnauthorized)
		return
	}

	token, err := r.jwt.GenerateToken(creds.Username)
	if err != nil {
		http.Error(w, "token generation failed", http.StatusInternalServerError)
		return
	}

	resp := struct {
		Token string `json:"token"`
	}{Token: token}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// handleListClusters returns all registered clusters with health status.
func (r *Router) handleListClusters(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	clusters := r.proxy.Registry.List()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(clusters)
}

// handleGlobalOverview aggregates overview from all clusters.
func (r *Router) handleGlobalOverview(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	result := r.aggregator.FetchAll(req.Context(), "/api/v1/overview")

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

// handleClusterOverview returns overview for a single cluster, aggregated from
// real metad endpoints (nodes/buckets/repair queue) — the metad has no
// /api/v1/overview endpoint, so this no longer proxies a nonexistent path.
func (r *Router) handleClusterOverview(w http.ResponseWriter, req *http.Request, clusterID string) {
	if req.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	ctx := req.Context()
	out := map[string]interface{}{
		"cluster":       clusterID,
		"nodes":         0,
		"capacityTotal": 0,
		"capacityUsed":  0,
		"buckets":       0,
		"repairQueue":   0,
	}

	var nodes []map[string]interface{}
	if err := r.proxy.Get(ctx, clusterID, "/api/v1/nodes", &nodes); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	var capTotal, capUsed float64
	for _, n := range nodes {
		capTotal += asFloat(n["capacity_gb"])
		capUsed += asFloat(n["used_gb"])
	}
	out["nodes"] = len(nodes)
	out["capacityTotal"] = int(capTotal)
	out["capacityUsed"] = int(capUsed)

	var buckets []interface{}
	if err := r.proxy.Get(ctx, clusterID, "/api/v1/buckets", &buckets); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	out["buckets"] = len(buckets)

	var repairQueue json.RawMessage
	if err := r.proxy.Get(ctx, clusterID, "/api/v1/repair/queue", &repairQueue); err == nil {
		var arr []interface{}
		if err := json.Unmarshal(repairQueue, &arr); err == nil {
			out["repairQueue"] = len(arr)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(out)
}

// asFloat tolerantly converts a JSON number/string to float64.
func asFloat(v interface{}) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case int:
		return float64(x)
	case int64:
		return float64(x)
	case string:
		var f float64
		fmt.Sscanf(x, "%f", &f)
		return f
	}
	return 0
}

// handleClusterReadiness proxies the cluster readiness check to the metad.
func (r *Router) handleClusterReadiness(w http.ResponseWriter, req *http.Request, clusterID string) {
	if req.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var readiness map[string]interface{}
	if err := r.proxy.Get(req.Context(), clusterID, "/api/v1/cluster/readiness", &readiness); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}

	readiness["cluster"] = clusterID
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(readiness)
}