package s3

import (
	"context"
	"net/http"
	"strings"

	"github.com/dshmyz/nufs/nufs-core/chunkstore"
	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// Gateway is the S3-compatible HTTP handler that routes requests
// to the metadata service and data nodes.
type Gateway struct {
	meta       metadata.MetadataService
	creds      *CredentialStore
	chunkStore chunkstore.ChunkStore
	committer  ObjectCommitter
	dataNodes  map[metadata.NodeID]string // nodeID -> data node address
	mux        *http.ServeMux
	acl        *metadata.AccessController // RBAC access control

	// Configurable limits and health check, captured at construction.
	maxObjectSize       int64
	rejectEmptyReplicas bool
	health              HealthChecker
	ready               HealthChecker
	ratePool            *rateLimiterPool
	backgroundWorkers   ObjectWriteBackgroundWorkerConfig
}

// GatewayConfig holds configuration for the S3 gateway.
type GatewayConfig struct {
	MetaService metadata.MetadataService
	Creds       *CredentialStore
	// ChunkStore is the data path used to read and write chunk payloads.
	// If nil, NewGateway installs a DatanodeChunkStore (the production
	// default). Tests typically inject a MemoryChunkStore.
	ChunkStore chunkstore.ChunkStore
	// MaxObjectSize is the largest allowed PUT body, in bytes, for
	// single-shot PutObject and UploadPart. <= 0 means 5 GiB
	// (the S3 single-shot limit). Requests exceeding the cap get a
	// 413 EntityTooLarge before any metadata work happens.
	MaxObjectSize int64
	// RejectEmptyReplicas, when true, makes PutObject return 503 if
	// the placement policy produced an empty replica set. Production
	// deployments should set this to true so a degraded cluster
	// surfaces writes as ServiceUnavailable rather than accepting
	// them and losing data. Tests with in-memory chunk stores keep
	// the default (false) so they don't need to wire replicas.
	RejectEmptyReplicas bool
	// HealthCheck, if set, is invoked on GET /healthz. Returning a
	// non-nil error causes the probe to fail.
	HealthCheck HealthChecker
	// ReadyCheck, if set, is invoked on GET /readyz. Same semantics
	// as HealthCheck. /readyz should reflect whether the gateway
	// is connected to the metadata service and the chunk store is
	// usable; /healthz is the liveness probe.
	ReadyCheck HealthChecker
	// PartDir is the temp directory for multipart upload part data.
	// Empty means parts are stored in memory (not recommended for
	// production — restart loses in-progress uploads).
	PartDir string
	// RateLimit is the maximum requests/second per client IP.
	// 0 means unlimited.
	RateLimit float64
	// RateLimitBurst is the maximum burst size for the rate limiter.
	// Defaults to RateLimit if 0.
	RateLimitBurst int
	// BackgroundWorkers controls object write recovery and garbage
	// collection loops. Disabled by default for tests and embedded use.
	BackgroundWorkers ObjectWriteBackgroundWorkerConfig
}

// HealthChecker reports the state of a subsystem. Returning nil means
// the subsystem is up; returning a non-nil error means down.
type HealthChecker func(ctx context.Context) error

// DefaultMaxObjectSize is the S3 single-shot PUT limit (5 GiB).
const DefaultMaxObjectSize int64 = 5 * 1024 * 1024 * 1024

// NewGateway creates a new S3 gateway handler.
func NewGateway(cfg GatewayConfig) *Gateway {
	if err := activeUploads.init(cfg.PartDir); err != nil {
		// PartDir is advisory; silence error in production.
		// Callers can check logs for misconfiguration.
	}
	activeUploads.startCleanup(defaultUploadTTL)
	gw := &Gateway{
		meta:       cfg.MetaService,
		creds:      cfg.Creds,
		chunkStore: cfg.ChunkStore,
		dataNodes:  make(map[metadata.NodeID]string),
		mux:        http.NewServeMux(),
		acl:        metadata.NewAccessController(),
	}

	if gw.creds == nil {
		gw.creds = NewCredentialStore() // anonymous mode
	}
	if gw.chunkStore == nil {
		gw.chunkStore = chunkstore.NewDatanodeChunkStore()
	}
	// Wire the write-path direct-EC authority (Program 10, §14): a configured
	// DatanodeChunkStore over a metadata authority (HTTPClient in production,
	// or a PebbleStore — both structurally satisfy ECWriteAuthority) can
	// direct-write EC shards (encode K+M and push each to its owning node's
	// shard store) for ECConfig buckets. A store left unwired makes an ECConfig
	// write fail with ErrECUnavailable: V1 whole-shard EC is retired
	// (docs/v1-retirement-roadmap.md stage 3), so an ECConfig bucket must never
	// silently degrade to the replication path.
	if cs, ok := gw.chunkStore.(*chunkstore.DatanodeChunkStore); ok {
		if auth, ok := gw.meta.(chunkstore.ECWriteAuthority); ok {
			cs.SetECWriteAuthority(auth)
		}
	}
	gw.committer = newMetadataObjectCommitter(gw.meta, gw.chunkStore, cfg.RejectEmptyReplicas)
	if cfg.MaxObjectSize <= 0 {
		gw.maxObjectSize = DefaultMaxObjectSize
	} else {
		gw.maxObjectSize = cfg.MaxObjectSize
	}
	gw.rejectEmptyReplicas = cfg.RejectEmptyReplicas
	gw.health = cfg.HealthCheck
	if gw.health == nil {
		gw.health = func(_ context.Context) error { return nil }
	}
	gw.ready = cfg.ReadyCheck
	if gw.ready == nil {
		gw.ready = gw.health
	}
	gw.ratePool = newRateLimiterPool(cfg.RateLimit, cfg.RateLimitBurst)
	gw.backgroundWorkers = cfg.BackgroundWorkers

	// Register routes — Go 1.22+ enhanced ServeMux patterns
	gw.mux.HandleFunc("/", gw.route)
	gw.mux.HandleFunc("/healthz", gw.handleHealthz)
	gw.mux.HandleFunc("/readyz", gw.handleReadyz)
	gw.mux.HandleFunc("/admin/cluster/stats", gw.handleClusterStats)
	gw.mux.HandleFunc("/admin/buckets", gw.handleAdminBuckets)
	gw.mux.HandleFunc("/admin/policy", gw.handleGetBucketPolicy)           // GET ?bucket=xxx
	gw.mux.HandleFunc("/admin/policy/set", gw.handleSetBucketPolicy)       // PUT ?bucket=xxx + JSON body
	gw.mux.HandleFunc("/admin/policy/delete", gw.handleDeleteBucketPolicy) // DELETE ?bucket=xxx

	return gw
}

// handleHealthz is the liveness probe. It returns 200 as long as the
// process is responsive enough to serve the request; a custom
// HealthChecker can veto by returning an error.
func (gw *Gateway) handleHealthz(w http.ResponseWriter, r *http.Request) {
	if err := gw.health(r.Context()); err != nil {
		http.Error(w, "unhealthy: "+err.Error(), http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

// handleReadyz is the readiness probe. It is the same as healthz by
// default but the caller is expected to wire a stricter check (e.g.
// "metadata service is reachable") via GatewayConfig.ReadyCheck.
func (gw *Gateway) handleReadyz(w http.ResponseWriter, r *http.Request) {
	if err := gw.ready(r.Context()); err != nil {
		http.Error(w, "not ready: "+err.Error(), http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ready"}`))
}

// Handler returns the fully wrapped HTTP handler with middleware chain.
func (gw *Gateway) Handler() http.Handler {
	return Chain(
		gw.mux,
		RecoveryMiddleware,
		RequestIDMiddleware,
		CORSMiddleware,
		gw.ratePool.Middleware(),
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

// SetRateLimit updates the per-IP rate limiter settings at runtime.
// rps <= 0 disables rate limiting. This is safe for concurrent use and
// is intended to be called from a config-watch callback.
func (gw *Gateway) SetRateLimit(rps float64, burst int) {
	gw.ratePool.Update(rps, burst)
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

	// Authenticate the request — identify the principal
	principal := metadata.PrincipalAnonymous
	hasAuth := gw.creds.HasCredentials()
	if hasAuth {
		accessKey, err := gw.creds.VerifySignatureV4(r)
		if err != nil {
			WriteXMLError(w, http.StatusForbidden, ErrCodeAccessDenied,
				err.Error(), r.URL.Path, requestID)
			return
		}
		if accessKey != "anonymous" && accessKey != "" {
			principal = metadata.Principal(accessKey)
		}

		// RBAC authorization check
		if bucket == "" {
			// Service-level: ListBuckets requires PermList
			if r.Method == http.MethodGet {
				if err := gw.acl.CheckServiceAccess(principal, metadata.PermList); err != nil {
					WriteXMLError(w, http.StatusForbidden, ErrCodeAccessDenied,
						"Access Denied", r.URL.Path, requestID)
					return
				}
			}
		} else {
			perm := gw.requiredPermission(r.Method, key)
			if err := gw.acl.CheckAccess(bucket, principal, perm); err != nil {
				// No policy = authenticated users allowed by default
				if gw.acl.GetPolicy(bucket) == nil {
					if principal == metadata.PrincipalAnonymous {
						WriteXMLError(w, http.StatusForbidden, ErrCodeAccessDenied,
							"Access Denied", r.URL.Path, requestID)
						return
					}
				} else {
					WriteXMLError(w, http.StatusForbidden, ErrCodeAccessDenied,
						"Access Denied", r.URL.Path, requestID)
					return
				}
			}
		}
	}

	// Route to handler (same logic regardless of auth mode)
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
			if _, ok := r.URL.Query()["uploads"]; ok {
				gw.handleListMultipartUploads(w, r, bucket, requestID)
			} else {
				gw.handleListObjects(w, r, bucket, requestID)
			}
		case http.MethodPost:
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
// requiredPermission maps an HTTP method and key presence to a Permission.
func (gw *Gateway) requiredPermission(method, key string) metadata.Permission {
	switch method {
	case http.MethodGet, http.MethodHead:
		return metadata.PermRead
	case http.MethodPut:
		if key == "" {
			return metadata.PermAdmin // CreateBucket
		}
		return metadata.PermWrite
	case http.MethodDelete:
		if key == "" {
			return metadata.PermAdmin // DeleteBucket
		}
		return metadata.PermWrite
	case http.MethodPost:
		return metadata.PermWrite // multipart uploads
	default:
		return metadata.PermRead
	}
}

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
