package s3

import (
	"encoding/json"
	"net/http"

	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// ============================================================
// Native cluster stats response type (GET /admin/cluster/stats)
// ============================================================

type clusterStatsResponse struct {
	Mode            string  `json:"mode"`
	Buckets         int     `json:"buckets"`
	TotalCapacityGB int64   `json:"total_capacity_gb"`
	UsedGB          int64   `json:"used_gb"`
	FreeGB          int64   `json:"free_gb"`
	UsagePct        float64 `json:"usage_pct"`
	NodesOnline     int     `json:"nodes_online"`
	NodesTotal      int     `json:"nodes_total"`
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
		Mode:            "online",
		Buckets:         len(buckets),
		TotalCapacityGB: totalCap,
		UsedGB:          usedCap,
		FreeGB:          freeGB,
		UsagePct:        pct,
		NodesOnline:     online,
		NodesTotal:      len(nodes),
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

// ============================================================
// RBAC — Bucket Policy Management API
// ============================================================

// handleGetBucketPolicy handles GET /admin/policy/{bucket}
func (gw *Gateway) handleGetBucketPolicy(w http.ResponseWriter, r *http.Request) {
	if gw.creds.HasCredentials() {
		_, err := gw.creds.VerifySignatureV4(r)
		if err != nil {
			writeJSONError(w, http.StatusForbidden, err.Error())
			return
		}
	}

	bucket := r.URL.Query().Get("bucket")
	if bucket == "" {
		writeJSONError(w, http.StatusBadRequest, "missing bucket parameter")
		return
	}

	policy, err := gw.meta.GetBucketPolicy(r.Context(), bucket)
	if err != nil {
		if err == metadata.ErrAccessDenied {
			writeJSONError(w, http.StatusNotFound, "no policy found for bucket")
			return
		}
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, policy)
}

// handleSetBucketPolicy handles PUT /admin/policy/{bucket}
func (gw *Gateway) handleSetBucketPolicy(w http.ResponseWriter, r *http.Request) {
	if gw.creds.HasCredentials() {
		accessKey, err := gw.creds.VerifySignatureV4(r)
		if err != nil {
			writeJSONError(w, http.StatusForbidden, err.Error())
			return
		}
		// Only bucket owner or admin can set policy
		bucket := r.URL.Query().Get("bucket")
		if bucket == "" {
			writeJSONError(w, http.StatusBadRequest, "missing bucket parameter")
			return
		}
		owner := gw.acl.OwnerOf(bucket)
		if owner != "" && owner != accessKey {
			// Check if the user has admin permission
			if err := gw.acl.CheckAccess(bucket, metadata.Principal(accessKey), metadata.PermAdmin); err != nil {
				writeJSONError(w, http.StatusForbidden, "only bucket owner or admin can set policy")
				return
			}
		}
	}

	var policy metadata.BucketPolicy
	if err := json.NewDecoder(r.Body).Decode(&policy); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid policy JSON: "+err.Error())
		return
	}

	bucket := r.URL.Query().Get("bucket")
	policy.Bucket = bucket

	if err := gw.meta.SetBucketPolicy(r.Context(), bucket, policy); err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	gw.acl.SetPolicy(bucket, &policy)
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleDeleteBucketPolicy handles DELETE /admin/policy/{bucket}
func (gw *Gateway) handleDeleteBucketPolicy(w http.ResponseWriter, r *http.Request) {
	if gw.creds.HasCredentials() {
		accessKey, err := gw.creds.VerifySignatureV4(r)
		if err != nil {
			writeJSONError(w, http.StatusForbidden, err.Error())
			return
		}
		bucket := r.URL.Query().Get("bucket")
		owner := gw.acl.OwnerOf(bucket)
		if owner != "" && owner != accessKey {
			if err := gw.acl.CheckAccess(bucket, metadata.Principal(accessKey), metadata.PermAdmin); err != nil {
				writeJSONError(w, http.StatusForbidden, "only bucket owner or admin can delete policy")
				return
			}
		}
	}

	bucket := r.URL.Query().Get("bucket")
	if bucket == "" {
		writeJSONError(w, http.StatusBadRequest, "missing bucket parameter")
		return
	}

	if err := gw.meta.DeleteBucketPolicy(r.Context(), bucket); err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	gw.acl.DeletePolicy(bucket)
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}
