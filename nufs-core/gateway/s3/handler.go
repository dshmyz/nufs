package s3

import (
	"context"
	"net/http"
	"strings"

	"github.com/example/dfs/metadata"
)

// Gateway is the S3-compatible HTTP handler that routes requests
// to the metadata service and data nodes.
type Gateway struct {
	meta       metadata.MetadataService
	creds      *CredentialStore
	chunkStore ChunkStore
	dataNodes  map[metadata.NodeID]string // nodeID -> data node address
	mux        *http.ServeMux
}

// GatewayConfig holds configuration for the S3 gateway.
type GatewayConfig struct {
	MetaService metadata.MetadataService
	Creds       *CredentialStore
	// ChunkStore is the data path used to read and write chunk payloads.
	// If nil, NewGateway installs a DatanodeChunkStore (the production
	// default). Tests typically inject a MemoryChunkStore.
	ChunkStore ChunkStore
}

// NewGateway creates a new S3 gateway handler.
func NewGateway(cfg GatewayConfig) *Gateway {
	gw := &Gateway{
		meta:       cfg.MetaService,
		creds:      cfg.Creds,
		chunkStore: cfg.ChunkStore,
		dataNodes:  make(map[metadata.NodeID]string),
		mux:        http.NewServeMux(),
	}

	if gw.creds == nil {
		gw.creds = NewCredentialStore() // anonymous mode
	}
	if gw.chunkStore == nil {
		gw.chunkStore = NewDatanodeChunkStore()
	}

	// Register routes — Go 1.22+ enhanced ServeMux patterns
	gw.mux.HandleFunc("/", gw.route)

	return gw
}

// Handler returns the fully wrapped HTTP handler with middleware chain.
func (gw *Gateway) Handler() http.Handler {
	return Chain(
		gw.mux,
		RecoveryMiddleware,
		RequestIDMiddleware,
		CORSMiddleware,
		LoggingMiddleware,
	)
}

// RefreshDataNodes fetches the current data node list from metadata service.
func (gw *Gateway) RefreshDataNodes(ctx context.Context) error {
	nodes, err := gw.meta.ListNodes(ctx)
	if err != nil {
		return err
	}
	for _, n := range nodes {
		if n.State == metadata.NodeOnline {
			gw.dataNodes[n.ID] = n.Addr
		}
	}
	return nil
}

// route is the main request dispatcher.
// S3 URL patterns:
//   - GET /                        -> ListBuckets
//   - PUT /{bucket}                -> CreateBucket
//   - DELETE /{bucket}             -> DeleteBucket
//   - HEAD /{bucket}               -> HeadBucket
//   - GET /{bucket}                -> ListObjects
//   - PUT /{bucket}/{key+}         -> PutObject
//   - GET /{bucket}/{key+}         -> GetObject
//   - DELETE /{bucket}/{key+}      -> DeleteObject
//   - HEAD /{bucket}/{key+}        -> HeadObject
func (gw *Gateway) route(w http.ResponseWriter, r *http.Request) {
	bucket, key := parsePath(r.URL.Path)
	requestID := w.Header().Get("x-amz-request-id")

	// Service-level operations
	if bucket == "" {
		switch r.Method {
		case http.MethodGet:
			gw.handleListBuckets(w, r, requestID)
		default:
			WriteXMLError(w, http.StatusMethodNotAllowed, ErrCodeMethodNotAllowed,
				"Method not allowed", r.URL.Path, requestID)
		}
		return
	}

	// Bucket-level operations (no key)
	if key == "" {
		switch r.Method {
		case http.MethodPut:
			gw.handleCreateBucket(w, r, bucket, requestID)
		case http.MethodDelete:
			gw.handleDeleteBucket(w, r, bucket, requestID)
		case http.MethodHead:
			gw.handleHeadBucket(w, r, bucket, requestID)
		case http.MethodGet:
			// Check for multipart list-parts
			if _, ok := r.URL.Query()["uploads"]; ok {
				gw.handleListMultipartUploads(w, r, bucket, requestID)
			} else {
				gw.handleListObjects(w, r, bucket, requestID)
			}
		case http.MethodPost:
			// Multipart delete (batch)
			if _, ok := r.URL.Query()["delete"]; ok {
				gw.handleBatchDelete(w, r, bucket, requestID)
			} else {
				WriteXMLError(w, http.StatusMethodNotAllowed, ErrCodeMethodNotAllowed,
					"Method not allowed", r.URL.Path, requestID)
			}
		default:
			WriteXMLError(w, http.StatusMethodNotAllowed, ErrCodeMethodNotAllowed,
				"Method not allowed", r.URL.Path, requestID)
		}
		return
	}

	// Object-level operations
	switch r.Method {
	case http.MethodPut:
		if r.URL.Query().Get("uploadId") != "" && r.URL.Query().Get("partNumber") != "" {
			gw.handleUploadPart(w, r, bucket, key, requestID)
		} else if r.Header.Get("X-Amz-Copy-Source") != "" {
			gw.handleCopyObject(w, r, bucket, key, requestID)
		} else {
			gw.handlePutObject(w, r, bucket, key, requestID)
		}
	case http.MethodGet:
		// Check for multipart download
		if r.URL.Query().Get("uploadId") != "" {
			gw.handleListParts(w, r, bucket, key, requestID)
		} else {
			gw.handleGetObject(w, r, bucket, key, requestID)
		}
	case http.MethodDelete:
		if r.URL.Query().Get("uploadId") != "" {
			gw.handleAbortMultipartUpload(w, r, bucket, key, requestID)
		} else {
			gw.handleDeleteObject(w, r, bucket, key, requestID)
		}
	case http.MethodHead:
		gw.handleHeadObject(w, r, bucket, key, requestID)
	case http.MethodPost:
		if _, ok := r.URL.Query()["uploads"]; ok {
			gw.handleInitiateMultipartUpload(w, r, bucket, key, requestID)
		} else if r.URL.Query().Get("uploadId") != "" {
			gw.handleCompleteMultipartUpload(w, r, bucket, key, requestID)
		} else {
			WriteXMLError(w, http.StatusMethodNotAllowed, ErrCodeMethodNotAllowed,
				"Method not allowed", r.URL.Path, requestID)
		}
	default:
		WriteXMLError(w, http.StatusMethodNotAllowed, ErrCodeMethodNotAllowed,
			"Method not allowed", r.URL.Path, requestID)
	}
}

// parsePath extracts bucket and key from the URL path.
// Path format: /{bucket}/{key...}
func parsePath(path string) (bucket, key string) {
	// Remove leading slash
	path = strings.TrimPrefix(path, "/")
	if path == "" {
		return "", ""
	}

	// Split on first slash
	idx := strings.Index(path, "/")
	if idx == -1 {
		return path, ""
	}
	return path[:idx], path[idx+1:]
}
