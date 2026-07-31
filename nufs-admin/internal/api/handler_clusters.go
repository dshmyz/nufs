// Package api provides HTTP handlers and routing.
package api

import (
	"encoding/json"
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

// handleClusterOverview returns overview for a single cluster.
func (r *Router) handleClusterOverview(w http.ResponseWriter, req *http.Request, clusterID string) {
	if req.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var overview map[string]interface{}
	if err := r.proxy.Get(req.Context(), clusterID, "/api/v1/overview", &overview); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}

	// Add cluster metadata
	overview["cluster"] = clusterID

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(overview)
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