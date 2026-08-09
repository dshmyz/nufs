package s3

import (
	"errors"
	"net/http"
	"time"

	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// handleListBuckets handles GET /
func (gw *Gateway) handleListBuckets(w http.ResponseWriter, r *http.Request, requestID string) {
	ctx := r.Context()
	buckets, err := gw.meta.ListBuckets(ctx)
	if err != nil {
		WriteXMLError(w, http.StatusInternalServerError, ErrCodeInternalError,
			err.Error(), "/", requestID)
		return
	}

	result := ListAllMyBucketsResult{
		Owner: Owner{
			ID:          "dfs",
			DisplayName: "DFS",
		},
	}
	for _, b := range buckets {
		result.Buckets = append(result.Buckets, BucketEntry{
			Name:         b.Name,
			CreationDate: FormatS3Time(b.CreationDate),
		})
	}
	WriteXML(w, http.StatusOK, result)
}

// handleCreateBucket handles PUT /{bucket}
func (gw *Gateway) handleCreateBucket(w http.ResponseWriter, r *http.Request, bucket, requestID string) {
	ctx := r.Context()

	policy := metadata.PlacementPolicy{
		ID:                "default",
		ReplicationFactor: 3,
		TopologySpread:    metadata.SpreadRack,
		StorageTier:       metadata.TierHot,
	}

	err := gw.meta.CreateBucket(ctx, bucket, policy)
	if err != nil {
		if errors.Is(err, metadata.ErrBucketExists) {
			WriteXMLError(w, http.StatusConflict, ErrCodeBucketAlreadyExists,
				"The requested bucket name is not available", "/"+bucket, requestID)
			return
		}
		WriteXMLError(w, http.StatusInternalServerError, ErrCodeInternalError,
			err.Error(), "/"+bucket, requestID)
		return
	}

	// Set default bucket policy: creator is the owner with full access
	owner := r.Header.Get("X-Owner")
	if owner == "" {
		owner = "anonymous"
	}
	defaultPolicy := metadata.BucketPolicy{
		Bucket: bucket,
		Owner:  owner,
		Statements: []metadata.Statement{
			{
				Effect:      "allow",
				Principal:   metadata.Principal(owner),
				Permissions: []metadata.Permission{metadata.PermRead, metadata.PermWrite, metadata.PermAdmin},
				Resource:    bucket,
			},
		},
		DefaultAccess: "deny",
	}
	gw.acl.SetPolicy(bucket, &defaultPolicy)
	_ = gw.meta.SetBucketPolicy(ctx, bucket, defaultPolicy)

	w.Header().Set("Location", "/"+bucket)
	w.WriteHeader(http.StatusOK)
}

// handleDeleteBucket handles DELETE /{bucket}
func (gw *Gateway) handleDeleteBucket(w http.ResponseWriter, r *http.Request, bucket, requestID string) {
	ctx := r.Context()

	err := gw.meta.DeleteBucket(ctx, bucket)
	if err != nil {
		if errors.Is(err, metadata.ErrBucketNotFound) {
			WriteXMLError(w, http.StatusNotFound, ErrCodeNoSuchBucket,
				"The specified bucket does not exist", "/"+bucket, requestID)
			return
		}
		if errors.Is(err, metadata.ErrBucketNotEmpty) {
			WriteXMLError(w, http.StatusConflict, ErrCodeBucketNotEmpty,
				"The bucket you tried to delete is not empty", "/"+bucket, requestID)
			return
		}
		WriteXMLError(w, http.StatusInternalServerError, ErrCodeInternalError,
			err.Error(), "/"+bucket, requestID)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// handleHeadBucket handles HEAD /{bucket}
func (gw *Gateway) handleHeadBucket(w http.ResponseWriter, r *http.Request, bucket, requestID string) {
	ctx := r.Context()

	_, err := gw.meta.GetBucket(ctx, bucket)
	if err != nil {
		if errors.Is(err, metadata.ErrBucketNotFound) {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Header().Set("x-amz-bucket-region", "us-east-1")
	w.WriteHeader(http.StatusOK)
}

// handleListObjects handles GET /{bucket}
func (gw *Gateway) handleListObjects(w http.ResponseWriter, r *http.Request, bucket, requestID string) {
	ctx := r.Context()
	q := r.URL.Query()

	prefix := q.Get("prefix")
	delimiter := q.Get("delimiter")
	marker := q.Get("marker")
	maxKeys := DefaultMaxKeys

	if v := q.Get("max-keys"); v != "" {
		if n, err := parseIntParam(v); err == nil && n > 0 {
			maxKeys = n
		}
	}

	// V2 API
	isV2 := q.Get("list-type") == "2"
	if isV2 {
		marker = q.Get("start-after")
		if ct := q.Get("continuation-token"); ct != "" {
			marker = ct
		}
	}

	// Get bucket info for root inode
	b, err := gw.meta.GetBucket(ctx, bucket)
	if err != nil {
		if errors.Is(err, metadata.ErrBucketNotFound) {
			WriteXMLError(w, http.StatusNotFound, ErrCodeNoSuchBucket,
				"The specified bucket does not exist", "/"+bucket, requestID)
			return
		}
		WriteXMLError(w, http.StatusInternalServerError, ErrCodeInternalError,
			err.Error(), "/"+bucket, requestID)
		return
	}

	// List directory entries under bucket root
	entries, err := gw.meta.ReadDir(ctx, b.RootInode, 0, maxKeys+1)
	if err != nil {
		WriteXMLError(w, http.StatusInternalServerError, ErrCodeInternalError,
			err.Error(), "/"+bucket, requestID)
		return
	}

	result := ListBucketResult{
		Name:        bucket,
		Prefix:      prefix,
		Marker:      marker,
		MaxKeys:     maxKeys,
		IsTruncated: len(entries) > maxKeys,
	}
	if isV2 {
		result.KeyCount = len(entries)
		if result.IsTruncated {
			result.KeyCount = maxKeys
		}
	}

	commonPrefixes := make(map[string]bool)
	for _, entry := range entries {
		if result.IsTruncated && len(result.Contents) >= maxKeys {
			break
		}

		name := entry.Name

		// Apply prefix filter
		if prefix != "" && !hasPrefix(name, prefix) {
			continue
		}

		// Apply marker filter
		if marker != "" && name <= marker {
			continue
		}

		// Apply delimiter for virtual directories
		if delimiter != "" {
			relative := trimPrefix(name, prefix)
			delimIdx := indexOf(relative, delimiter)
			if delimIdx >= 0 {
				cp := prefix + relative[:delimIdx+len(delimiter)]
				commonPrefixes[cp] = true
				continue
			}
		}

		// Get inode metadata for size/timestamp
		inode, err := gw.meta.GetInode(ctx, entry.InodeID)
		if err != nil {
			continue
		}

		obj := ObjectEntry{
			Key:          name,
			LastModified: FormatS3Time(unixNanoToTime(inode.MTime)),
			ETag:         FormatETag(0), // TODO: compute from chunk checksums
			Size:         inode.Size,
			StorageClass: "STANDARD",
		}
		result.Contents = append(result.Contents, obj)
	}

	for cp := range commonPrefixes {
		result.CommonPrefixes = append(result.CommonPrefixes, CommonPrefix{Prefix: cp})
	}

	WriteXML(w, http.StatusOK, result)
}

// handleBatchDelete handles POST /{bucket}?delete
func (gw *Gateway) handleBatchDelete(w http.ResponseWriter, r *http.Request, bucket, requestID string) {
	// TODO: Implement batch delete (parse XML body, delete multiple objects)
	w.WriteHeader(http.StatusOK)
}

// ========== Helpers ==========

func hasPrefix(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}

func trimPrefix(s, prefix string) string {
	if hasPrefix(s, prefix) {
		return s[len(prefix):]
	}
	return s
}

func indexOf(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}

func parseIntParam(s string) (int, error) {
	n := 0
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0, errors.New("not a number")
		}
		n = n*10 + int(c-'0')
	}
	return n, nil
}

func unixNanoToTime(nano int64) time.Time {
	return time.Unix(0, nano)
}
