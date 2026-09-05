// Package api provides HTTP handlers and routing.
package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
			if buckets == nil {
				buckets = make([]map[string]interface{}, 0) // 空桶给 []，前端 .map 依赖
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

	case len(subpath) == 2 && subpath[1] == "objects":
		bucketName, err := decodeBucketPathSegment(req, subpath[0])
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		r.handleBucketObjects(w, req, clusterID, bucketName)

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

// handleBucketObjects lists a bucket's contents by walking the metadata
// namespace (metad readdir on the bucket root inode) — no S3 protocol, no
// SigV4, just the metad ops token. path is a "/"-joined directory path
// inside the bucket; each level is resolved to its inode via readdir.
// GET /clusters/{id}/buckets/{bucket}/objects?path=a/b
func (r *Router) handleBucketObjects(w http.ResponseWriter, req *http.Request, clusterID, bucket string) {
	if req.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	ctx := req.Context()
	path := strings.Trim(req.URL.Query().Get("path"), "/")

	// 找桶 root inode
	var buckets []map[string]interface{}
	if err := r.proxy.Get(ctx, clusterID, "/api/v1/buckets", &buckets); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	var rootInode int64 = -1
	for _, b := range buckets {
		if b["name"] == bucket {
			if v, ok := b["root_inode"].(float64); ok {
				rootInode = int64(v)
			}
			break
		}
	}
	if rootInode < 0 {
		http.Error(w, "bucket not found", http.StatusNotFound)
		return
	}

	// 按 path 逐段解析 inode
	parent := rootInode
	if path != "" {
		for _, seg := range strings.Split(path, "/") {
			entries, err := r.metadReadDir(ctx, clusterID, parent)
			if err != nil {
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				return
			}
			var found bool
			for _, e := range entries {
				if e["name"] == seg {
					if v, ok := e["inode"].(float64); ok {
						parent = int64(v)
					}
					found = true
					break
				}
			}
			if !found {
				http.Error(w, "path not found: "+path, http.StatusNotFound)
				return
			}
		}
	}

	entries, err := r.metadReadDir(ctx, clusterID, parent)
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	for _, e := range entries {
		name, _ := e["name"].(string)
		if path == "" {
			e["path"] = name
		} else {
			e["path"] = path + "/" + name
		}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"path": path, "entries": entries})
}

func (r *Router) metadReadDir(ctx context.Context, clusterID string, parent int64) ([]map[string]interface{}, error) {
	var entries []map[string]interface{}
	p := fmt.Sprintf("/api/v1/namespace/readdir?parent=%d&offset=0&limit=1000", parent)
	if err := r.proxy.GetUncached(ctx, clusterID, p, &entries); err != nil {
		return nil, err
	}
	return entries, nil
}
