package s3

import (
	"encoding/json"
	"net/http"

	"github.com/example/dfs/metadata"
)

// ============================================================
// Native cluster stats response type (GET /admin/cluster/stats)
// ============================================================

type clusterStatsResponse struct {
	Mode         string `json:"mode"`
	Buckets      int    `json:"buckets"`
	TotalCapacityGB int64 `json:"total_capacity_gb"`
	UsedGB       int64  `json:"used_gb"`
	FreeGB       int64  `json:"free_gb"`
	UsagePct     float64 `json:"usage_pct"`
	NodesOnline  int    `json:"nodes_online"`
	NodesTotal   int    `json:"nodes_total"`
}

// handleClusterStats handles GET /admin/cluster/stats
func (gw *Gateway) handleClusterStats(w http.ResponseWriter, r *http.Request) {
	if gw.creds.HasCredentials() {
		_, err := gw.creds.VerifySignatureV4(r)
		if err != nil {
			requestID := w.Header().Get("x-amz-request-id")
			WriteXMLError(w, http.StatusForbidden, ErrCodeAccessDenied,
				err.Error(), r.URL.Path, requestID)
			return
		}
	}

	nodes, err := gw.meta.ListNodes(r.Context())
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	buckets, err := gw.meta.ListBuckets(r.Context())
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	var totalCap, usedCap int64
	var online int
	for _, n := range nodes {
		totalCap += n.CapacityGB
		usedCap += n.UsedGB
		if n.State == metadata.NodeOnline {
			online++
		}
	}

	freeGB := totalCap - usedCap
	pct := 0.0
	if totalCap > 0 {
		pct = float64(usedCap) / float64(totalCap) * 100
	}

	resp := clusterStatsResponse{
		Mode:           "online",
		Buckets:        len(buckets),
		TotalCapacityGB: totalCap,
		UsedGB:         usedCap,
		FreeGB:         freeGB,
		UsagePct:       pct,
		NodesOnline:    online,
		NodesTotal:     len(nodes),
	}

	writeJSON(w, http.StatusOK, resp)
}

// handleAdminBuckets handles GET /admin/buckets
func (gw *Gateway) handleAdminBuckets(w http.ResponseWriter, r *http.Request) {
	if gw.creds.HasCredentials() {
		_, err := gw.creds.VerifySignatureV4(r)
		if err != nil {
			requestID := w.Header().Get("x-amz-request-id")
			WriteXMLError(w, http.StatusForbidden, ErrCodeAccessDenied,
				err.Error(), r.URL.Path, requestID)
			return
		}
	}

	usage, err := gw.meta.ComputeAllBucketUsage(r.Context())
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, usage)
}

// writeJSON writes a JSON response.
func writeJSON(w http.ResponseWriter, statusCode int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(v)
}

// writeJSONError writes a JSON error response.
func writeJSONError(w http.ResponseWriter, statusCode int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
