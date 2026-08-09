// Package api provides HTTP handlers and routing.
package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/dshmyz/nufs/nufs-admin/internal/cluster"
)

const bucketQuotaBodyLimit = 64 << 10

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
			bucketName, err := decodeBucketPathSegment(req, subpath[0])
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			path := "/api/v1/buckets/" + escapePathSegment(bucketName)

			if err := r.proxy.Delete(req.Context(), clusterID, path); err != nil {
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				return
			}

			w.WriteHeader(http.StatusOK)
			return
		}

		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)

	case len(subpath) == 2 && subpath[1] == "quota":
		bucketName, err := decodeBucketPathSegment(req, subpath[0])
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		r.handleBucketQuota(w, req, clusterID, bucketName)

	case len(subpath) == 2:
		http.Error(w, "unknown bucket resource", http.StatusNotFound)

	default:
		http.Error(w, "invalid buckets path", http.StatusBadRequest)
	}
}

func (r *Router) handleBucketQuota(w http.ResponseWriter, req *http.Request, clusterID, bucket string) {
	path := "/api/v1/buckets/" + escapePathSegment(bucket) + "/quota"

	switch req.Method {
	case http.MethodGet:
		var response json.RawMessage
		if err := r.proxy.GetUncached(req.Context(), clusterID, path, &response); err != nil {
			writeBucketQuotaProxyError(w, err)
			return
		}
		writeBucketQuotaJSON(w, response)

	case http.MethodPut:
		body := http.MaxBytesReader(w, req.Body, bucketQuotaBodyLimit)
		defer body.Close()
		payload, err := io.ReadAll(body)
		if err != nil {
			var maxBytesError *http.MaxBytesError
			if errors.As(err, &maxBytesError) {
				http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
				return
			}
			http.Error(w, "invalid request body", http.StatusBadRequest)
			return
		}

		var response json.RawMessage
		if err := r.proxy.Put(req.Context(), clusterID, path, bytes.NewReader(payload), &response); err != nil {
			writeBucketQuotaProxyError(w, err)
			return
		}
		writeBucketQuotaJSON(w, response)

	case http.MethodDelete:
		if err := r.proxy.Delete(req.Context(), clusterID, path); err != nil {
			writeBucketQuotaProxyError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)

	default:
		w.Header().Set("Allow", "GET, PUT, DELETE")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func escapePathSegment(segment string) string {
	switch segment {
	case ".":
		return "%2E"
	case "..":
		return "%2E%2E"
	default:
		return url.PathEscape(segment)
	}
}

func decodeBucketPathSegment(req *http.Request, segment string) (string, error) {
	switch req.URL.Query().Get("bucket_path") {
	case "":
		return segment, nil
	case "dot":
		if strings.EqualFold(segment, "%2E") {
			return ".", nil
		}
	case "dotdot":
		if strings.EqualFold(segment, "%2E%2E") {
			return "..", nil
		}
	}
	return "", errors.New("invalid bucket path encoding")
}

func writeBucketQuotaJSON(w http.ResponseWriter, response json.RawMessage) {
	if len(response) == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(response)
}

func writeBucketQuotaProxyError(w http.ResponseWriter, err error) {
	if errors.Is(err, cluster.ErrClusterNotFound) {
		http.Error(w, "cluster not found", http.StatusNotFound)
		return
	}

	var upstreamError *cluster.UpstreamHTTPError
	if !errors.As(err, &upstreamError) {
		http.Error(w, "upstream service unavailable", http.StatusServiceUnavailable)
		return
	}

	contentType := upstreamError.ContentType
	if strings.HasPrefix(strings.ToLower(contentType), "application/json") && json.Valid(upstreamError.Body) {
		w.Header().Set("Content-Type", contentType)
		w.WriteHeader(upstreamError.StatusCode)
		_, _ = w.Write(upstreamError.Body)
		return
	}

	http.Error(w, http.StatusText(upstreamError.StatusCode), upstreamError.StatusCode)
}
