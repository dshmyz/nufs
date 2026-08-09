package main

import (
	"encoding/json"
	"net/http"

	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// --- Bucket handlers ---

func (h *opsHandlers) handleBuckets(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		buckets, err := h.dataStore.ListBuckets(r.Context())
		if err != nil {
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, buckets)
	case http.MethodPost:
		var req struct {
			Name   string                   `json:"name"`
			Policy metadata.PlacementPolicy `json:"policy"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid request body")
			return
		}
		if err := h.dataStore.CreateBucket(r.Context(), req.Name, req.Policy); err != nil {
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
		w.WriteHeader(http.StatusCreated)
		writeJSON(w, map[string]string{"status": "created"})
	default:
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (h *opsHandlers) handleBucketByID(w http.ResponseWriter, r *http.Request) {
	if name, isQuotaPath := bucketNameAndQuotaPath(r.URL.Path); isQuotaPath {
		if name == "" {
			writeJSONError(w, http.StatusBadRequest, "bucket name required")
			return
		}
		h.handleBucketQuota(w, r, name)
		return
	}

	name := r.URL.Path[len("/api/v1/buckets/"):]
	if name == "" {
		writeJSONError(w, http.StatusBadRequest, "bucket name required")
		return
	}
	switch r.Method {
	case http.MethodGet:
		bucket, err := h.dataStore.GetBucket(r.Context(), name)
		if err != nil {
			writeJSONError(w, http.StatusNotFound, err.Error())
			return
		}
		writeJSON(w, bucket)
	case http.MethodDelete:
		if err := h.dataStore.DeleteBucket(r.Context(), name); err != nil {
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, map[string]string{"status": "deleted"})
	default:
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}
