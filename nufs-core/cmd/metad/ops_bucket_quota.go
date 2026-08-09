package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/dshmyz/nufs/nufs-core/metadata"
)

type bucketQuotaStatus struct {
	Bucket string                `json:"bucket"`
	Quota  *metadata.BucketQuota `json:"quota"`
	Usage  metadata.BucketUsage  `json:"usage"`
	Ratios bucketQuotaRatios     `json:"ratios"`
}

type bucketQuotaRatios struct {
	Bytes   float64 `json:"bytes"`
	Objects float64 `json:"objects"`
}

// rejectEmptyBucketQuotaPath runs before http.ServeMux, whose path cleaning
// would otherwise redirect this malformed quota route to a different bucket.
func rejectEmptyBucketQuotaPath(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.EscapedPath() == "/api/v1/buckets//quota" {
			writeJSONError(w, http.StatusBadRequest, "bucket name required")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (h *opsHandlers) handleBucketQuota(w http.ResponseWriter, r *http.Request, bucket string) {
	if strings.HasSuffix(r.URL.Path, "/quota/check") {
		if r.Method != http.MethodPost {
			writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		h.checkBucketQuota(w, r, bucket)
		return
	}

	switch r.Method {
	case http.MethodGet:
		h.getBucketQuota(w, r, bucket)
	case http.MethodPut:
		h.putBucketQuota(w, r, bucket)
	case http.MethodDelete:
		if err := h.dataStore.DeleteBucketQuota(r.Context(), bucket); err != nil {
			writeBucketQuotaError(w, err)
			return
		}
		writeJSON(w, map[string]string{"status": "deleted"})
	default:
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (h *opsHandlers) getBucketQuota(w http.ResponseWriter, r *http.Request, bucket string) {
	quota, err := h.dataStore.GetBucketQuota(r.Context(), bucket)
	if err != nil {
		writeBucketQuotaError(w, err)
		return
	}
	usage, err := h.dataStore.GetBucketUsage(r.Context(), bucket)
	if err != nil {
		writeBucketQuotaError(w, err)
		return
	}
	writeJSON(w, newBucketQuotaStatus(bucket, quota, usage))
}

func (h *opsHandlers) putBucketQuota(w http.ResponseWriter, r *http.Request, bucket string) {
	var quota metadata.BucketQuota
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&quota); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := quota.Validate(); err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := h.dataStore.SetBucketQuota(r.Context(), bucket, &quota); err != nil {
		writeBucketQuotaError(w, err)
		return
	}
	usage, err := h.dataStore.GetBucketUsage(r.Context(), bucket)
	if err != nil {
		writeBucketQuotaError(w, err)
		return
	}
	writeJSON(w, newBucketQuotaStatus(bucket, &quota, usage))
}

func (h *opsHandlers) checkBucketQuota(w http.ResponseWriter, r *http.Request, bucket string) {
	var req struct {
		AdditionalBytes   int64 `json:"additional_bytes"`
		AdditionalObjects int64 `json:"additional_objects"`
	}
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := h.dataStore.CheckBucketQuota(r.Context(), bucket, req.AdditionalBytes, req.AdditionalObjects); err != nil {
		writeBucketQuotaError(w, err)
		return
	}
	writeJSON(w, map[string]string{"status": "allowed"})
}

func newBucketQuotaStatus(bucket string, quota *metadata.BucketQuota, usage *metadata.BucketUsage) bucketQuotaStatus {
	status := bucketQuotaStatus{Bucket: bucket, Quota: quota}
	if usage != nil {
		status.Usage = *usage
	}
	if quota != nil {
		if quota.MaxSizeBytes > 0 {
			status.Ratios.Bytes = float64(status.Usage.UsedBytes) / float64(quota.MaxSizeBytes)
		}
		if quota.MaxObjects > 0 {
			status.Ratios.Objects = float64(status.Usage.Objects) / float64(quota.MaxObjects)
		}
	}
	return status
}

func bucketNameAndQuotaPath(path string) (string, bool) {
	trimmed := strings.TrimPrefix(path, "/api/v1/buckets/")
	if strings.HasSuffix(trimmed, "/quota/check") {
		return strings.TrimSuffix(trimmed, "/quota/check"), true
	}
	if !strings.HasSuffix(trimmed, "/quota") {
		return "", false
	}
	return strings.TrimSuffix(trimmed, "/quota"), true
}

func writeBucketQuotaError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, metadata.ErrBucketNotFound):
		writeJSONError(w, http.StatusNotFound, err.Error())
	case errors.Is(err, metadata.ErrQuotaExceeded):
		writeJSONErrorC(w, http.StatusConflict, "quota_exceeded", err.Error())
	default:
		writeJSONError(w, http.StatusInternalServerError, err.Error())
	}
}
