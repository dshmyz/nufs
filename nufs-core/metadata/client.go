package metadata

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/example/dfs/internal/tlsutil"
)

// HTTPClient implements MetadataService over HTTP to a remote metad server.
// It supports automatic retries with exponential backoff and leader redirect
// following for Raft-based deployments.
type HTTPClient struct {
	baseURL    string
	httpClient *http.Client
	timeout    time.Duration
	authToken  string

	// Retry configuration
	maxRetries    int           // Maximum number of retries (default: 3)
	retryBaseWait time.Duration // Base wait between retries (default: 100ms)
}

// NewHTTPClient creates a metadata HTTP client connecting to a metad server.
// baseURL should be like "http://localhost:8091".
func NewHTTPClient(baseURL string, timeout time.Duration) *HTTPClient {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	return &HTTPClient{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				MaxIdleConns:        256,
				MaxIdleConnsPerHost: 128,
				MaxConnsPerHost:     256,
				IdleConnTimeout:     90 * time.Second,
			},
			// Don't auto-follow redirects — we handle them ourselves
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		timeout:       timeout,
		maxRetries:    3,
		retryBaseWait: 100 * time.Millisecond,
	}
}

// SetAuthToken configures a bearer token sent with every metadata request.
func (c *HTTPClient) SetAuthToken(token string) {
	c.authToken = token
}

func (c *HTTPClient) SetRetryConfig(maxRetries int, baseWait time.Duration) {
	if maxRetries >= 0 {
		c.maxRetries = maxRetries
	}
	if baseWait > 0 {
		c.retryBaseWait = baseWait
	}
}

// EnableTLS configures the underlying HTTP transport to use TLS based on
// the provided config. It must be called before any requests are made.
func (c *HTTPClient) EnableTLS(cfg tlsutil.Config) error {
	tlsCfg, err := tlsutil.ClientConfig(cfg)
	if err != nil {
		return fmt.Errorf("metadata client: tls config: %w", err)
	}
	transport, ok := c.httpClient.Transport.(*http.Transport)
	if !ok {
		transport = &http.Transport{
			MaxIdleConns:        256,
			MaxIdleConnsPerHost: 128,
			MaxConnsPerHost:     256,
			IdleConnTimeout:     90 * time.Second,
		}
	}
	transport.TLSClientConfig = tlsCfg
	c.httpClient.Transport = transport
	return nil
}

// doRequestWithRetry executes an HTTP request with exponential backoff retry
// and automatic leader redirect following.
func (c *HTTPClient) doRequestWithRetry(ctx context.Context, method, path string, body interface{}) (*http.Response, error) {
	var lastErr error

	for attempt := 0; attempt <= c.maxRetries; attempt++ {
		if attempt > 0 {
			// Exponential backoff: 100ms, 200ms, 400ms, ...
			wait := c.retryBaseWait * time.Duration(1<<uint(attempt-1))
			select {
			case <-time.After(wait):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}

		resp, err := c.doRequest(ctx, method, path, body)
		if err != nil {
			lastErr = err
			continue // Retry on network errors
		}

		// Handle leader redirect (HTTP 301/307 from follower nodes)
		if resp.StatusCode == http.StatusMovedPermanently || resp.StatusCode == http.StatusTemporaryRedirect {
			location := resp.Header.Get("Location")
			resp.Body.Close()
			if location != "" {
				redirectURL, err := url.Parse(location)
				if err == nil && redirectURL.Host != "" {
					// Follow redirect: construct a temporary URL instead of mutating c.baseURL
					redirectBase := redirectURL.Scheme + "://" + redirectURL.Host
					redirectReqURL := redirectBase + path
					var reqBody io.Reader
					if body != nil {
						data, err := json.Marshal(body)
						if err != nil {
							lastErr = fmt.Errorf("marshal redirect body: %w", err)
							continue
						}
						reqBody = bytes.NewReader(data)
					}
					req, reqErr := http.NewRequestWithContext(ctx, method, redirectReqURL, reqBody)
					if reqErr != nil {
						lastErr = reqErr
						continue
					}
					req.Header.Set("Content-Type", "application/json")
					c.addHeaders(req)
					resp, err = c.httpClient.Do(req)
					if err != nil {
						lastErr = err
						continue
					}
					return resp, nil
				}
			}
			lastErr = fmt.Errorf("metad: redirect without location")
			continue
		}

		// Retry on server errors (5xx) and connection-level issues
		if resp.StatusCode >= 500 {
			resp.Body.Close()
			lastErr = fmt.Errorf("metad: server error (status=%d)", resp.StatusCode)
			continue
		}

		return resp, nil
	}

	return nil, fmt.Errorf("metad: %w (after %d retries)", lastErr, c.maxRetries)
}

func (c *HTTPClient) doRequest(ctx context.Context, method, path string, body interface{}) (*http.Response, error) {
	var reqBody io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("marshal request: %w", err)
		}
		reqBody = bytes.NewReader(data)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reqBody)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	c.addHeaders(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http request: %w", err)
	}
	return resp, nil
}

func (c *HTTPClient) addHeaders(req *http.Request) {
	if c.authToken != "" {
		req.Header.Set("Authorization", "Bearer "+c.authToken)
	}
}

func (c *HTTPClient) readResponse(resp *http.Response, v interface{}) error {
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}
	if resp.StatusCode >= 400 {
		if resp.StatusCode == http.StatusConflict {
			var errResp struct {
				Error string `json:"error"`
				Code  string `json:"code"`
			}
			if json.Unmarshal(data, &errResp) == nil && errResp.Code == "node_already_registered" {
				return ErrNodeAlreadyExists
			}
		}
		return fmt.Errorf("metad: %s (status=%d)", string(data), resp.StatusCode)
	}
	if v != nil {
		if err := json.Unmarshal(data, v); err != nil {
			return fmt.Errorf("unmarshal response: %w", err)
		}
	}
	return nil
}

// Bucket operations

func (c *HTTPClient) CreateBucket(ctx context.Context, name string, policy PlacementPolicy) error {
	req := map[string]interface{}{
		"name":   name,
		"policy": policy,
	}
	resp, err := c.doRequestWithRetry(ctx, http.MethodPost, "/api/v1/buckets", req)
	if err != nil {
		return err
	}
	return c.readResponse(resp, nil)
}

func (c *HTTPClient) DeleteBucket(ctx context.Context, name string) error {
	resp, err := c.doRequestWithRetry(ctx, http.MethodDelete, "/api/v1/buckets/"+name, nil)
	if err != nil {
		return err
	}
	return c.readResponse(resp, nil)
}

func (c *HTTPClient) ListBuckets(ctx context.Context) ([]BucketInfo, error) {
	resp, err := c.doRequestWithRetry(ctx, http.MethodGet, "/api/v1/buckets", nil)
	if err != nil {
		return nil, err
	}
	var buckets []BucketInfo
	if err := c.readResponse(resp, &buckets); err != nil {
		return nil, err
	}
	return buckets, nil
}

func (c *HTTPClient) GetBucket(ctx context.Context, name string) (*BucketInfo, error) {
	resp, err := c.doRequestWithRetry(ctx, http.MethodGet, "/api/v1/buckets/"+name, nil)
	if err != nil {
		return nil, err
	}
	var bucket BucketInfo
	if err := c.readResponse(resp, &bucket); err != nil {
		return nil, err
	}
	return &bucket, nil
}

// Namespace operations — these are typically handled by the gateway layer
// but we provide them for completeness.

func (c *HTTPClient) MkDir(ctx context.Context, parent InodeID, name string, mode uint32) (*InodeMeta, error) {
	req := map[string]interface{}{"parent": parent, "name": name, "mode": mode}
	resp, err := c.doRequestWithRetry(ctx, http.MethodPost, "/api/v1/namespace/mkdir", req)
	if err != nil {
		return nil, err
	}
	var meta InodeMeta
	if err := c.readResponse(resp, &meta); err != nil {
		return nil, err
	}
	return &meta, nil
}

func (c *HTTPClient) RmDir(ctx context.Context, parent InodeID, name string) error {
	req := map[string]interface{}{"parent": parent, "name": name}
	resp, err := c.doRequestWithRetry(ctx, http.MethodPost, "/api/v1/namespace/rmdir", req)
	if err != nil {
		return err
	}
	return c.readResponse(resp, nil)
}

func (c *HTTPClient) ReadDir(ctx context.Context, parent InodeID, offset int, limit int) ([]DirEntry, error) {
	path := fmt.Sprintf("/api/v1/namespace/readdir?parent=%d&offset=%d&limit=%d", parent, offset, limit)
	resp, err := c.doRequestWithRetry(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}
	var entries []DirEntry
	if err := c.readResponse(resp, &entries); err != nil {
		return nil, err
	}
	return entries, nil
}

func (c *HTTPClient) CreateFile(ctx context.Context, parent InodeID, name string, mode uint32) (*InodeMeta, error) {
	req := map[string]interface{}{"parent": parent, "name": name, "mode": mode}
	resp, err := c.doRequestWithRetry(ctx, http.MethodPost, "/api/v1/namespace/createfile", req)
	if err != nil {
		return nil, err
	}
	var meta InodeMeta
	if err := c.readResponse(resp, &meta); err != nil {
		return nil, err
	}
	return &meta, nil
}

func (c *HTTPClient) Unlink(ctx context.Context, parent InodeID, name string) error {
	req := map[string]interface{}{"parent": parent, "name": name}
	resp, err := c.doRequestWithRetry(ctx, http.MethodPost, "/api/v1/namespace/unlink", req)
	if err != nil {
		return err
	}
	return c.readResponse(resp, nil)
}

func (c *HTTPClient) Lookup(ctx context.Context, parent InodeID, name string) (*InodeMeta, error) {
	path := fmt.Sprintf("/api/v1/namespace/lookup?parent=%d&name=%s", parent, url.QueryEscape(name))
	resp, err := c.doRequestWithRetry(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}
	var meta InodeMeta
	if err := c.readResponse(resp, &meta); err != nil {
		return nil, err
	}
	return &meta, nil
}

func (c *HTTPClient) Rename(ctx context.Context, oldParent InodeID, oldName string, newParent InodeID, newName string) error {
	req := map[string]interface{}{"old_parent": oldParent, "old_name": oldName, "new_parent": newParent, "new_name": newName}
	resp, err := c.doRequestWithRetry(ctx, http.MethodPost, "/api/v1/namespace/rename", req)
	if err != nil {
		return err
	}
	return c.readResponse(resp, nil)
}

func (c *HTTPClient) Symlink(ctx context.Context, parent InodeID, name string, target string) (*InodeMeta, error) {
	req := map[string]interface{}{"parent": parent, "name": name, "target": target}
	resp, err := c.doRequestWithRetry(ctx, http.MethodPost, "/api/v1/namespace/symlink", req)
	if err != nil {
		return nil, err
	}
	var meta InodeMeta
	if err := c.readResponse(resp, &meta); err != nil {
		return nil, err
	}
	return &meta, nil
}

func (c *HTTPClient) Readlink(ctx context.Context, id InodeID) (string, error) {
	path := fmt.Sprintf("/api/v1/namespace/readlink?id=%d", id)
	resp, err := c.doRequestWithRetry(ctx, http.MethodGet, path, nil)
	if err != nil {
		return "", err
	}
	var result struct {
		Target string `json:"target"`
	}
	if err := c.readResponse(resp, &result); err != nil {
		return "", err
	}
	return result.Target, nil
}

func (c *HTTPClient) Link(ctx context.Context, parent InodeID, name string, target InodeID) (*InodeMeta, error) {
	req := map[string]interface{}{"parent": parent, "name": name, "target": target}
	resp, err := c.doRequestWithRetry(ctx, http.MethodPost, "/api/v1/namespace/link", req)
	if err != nil {
		return nil, err
	}
	var meta InodeMeta
	if err := c.readResponse(resp, &meta); err != nil {
		return nil, err
	}
	return &meta, nil
}

// Inode operations

func (c *HTTPClient) GetInode(ctx context.Context, id InodeID) (*InodeMeta, error) {
	path := fmt.Sprintf("/api/v1/inodes/%d", id)
	resp, err := c.doRequestWithRetry(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}
	var meta InodeMeta
	if err := c.readResponse(resp, &meta); err != nil {
		return nil, err
	}
	return &meta, nil
}

func (c *HTTPClient) UpdateInode(ctx context.Context, meta *InodeMeta) error {
	resp, err := c.doRequestWithRetry(ctx, http.MethodPut, fmt.Sprintf("/api/v1/inodes/%d", meta.ID), meta)
	if err != nil {
		return err
	}
	return c.readResponse(resp, nil)
}

// Chunk operations

func (c *HTTPClient) AllocateChunk(ctx context.Context, inodeID InodeID, offset int64, policy PlacementPolicy) (*ChunkMeta, error) {
	req := map[string]interface{}{"inode_id": inodeID, "offset": offset, "policy": policy}
	resp, err := c.doRequestWithRetry(ctx, http.MethodPost, "/api/v1/chunks", req)
	if err != nil {
		return nil, err
	}
	var chunk ChunkMeta
	if err := c.readResponse(resp, &chunk); err != nil {
		return nil, err
	}
	return &chunk, nil
}

func (c *HTTPClient) CommitChunk(ctx context.Context, chunkID ChunkID, checksum uint32) error {
	req := map[string]interface{}{"checksum": checksum}
	resp, err := c.doRequestWithRetry(ctx, http.MethodPost, fmt.Sprintf("/api/v1/chunks/%d/commit", chunkID), req)
	if err != nil {
		return err
	}
	return c.readResponse(resp, nil)
}

func (c *HTTPClient) GetChunk(ctx context.Context, chunkID ChunkID) (*ChunkMeta, error) {
	path := fmt.Sprintf("/api/v1/chunks/%d", chunkID)
	resp, err := c.doRequestWithRetry(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}
	var chunk ChunkMeta
	if err := c.readResponse(resp, &chunk); err != nil {
		return nil, err
	}
	return &chunk, nil
}

func (c *HTTPClient) UpdateChunk(ctx context.Context, chunk *ChunkMeta) error {
	resp, err := c.doRequestWithRetry(ctx, http.MethodPut, fmt.Sprintf("/api/v1/chunks/%d", chunk.ID), chunk)
	if err != nil {
		return err
	}
	return c.readResponse(resp, nil)
}

func (c *HTTPClient) SealChunk(ctx context.Context, chunkID ChunkID) error {
	resp, err := c.doRequestWithRetry(ctx, http.MethodPost, fmt.Sprintf("/api/v1/chunks/%d/seal", chunkID), nil)
	if err != nil {
		return err
	}
	return c.readResponse(resp, nil)
}

func (c *HTTPClient) ListChunks(ctx context.Context, inodeID InodeID) ([]ChunkRef, error) {
	path := fmt.Sprintf("/api/v1/chunks?inode_id=%d", inodeID)
	resp, err := c.doRequestWithRetry(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}
	var refs []ChunkRef
	if err := c.readResponse(resp, &refs); err != nil {
		return nil, err
	}
	return refs, nil
}

func (c *HTTPClient) DeleteChunk(ctx context.Context, chunkID ChunkID) error {
	resp, err := c.doRequestWithRetry(ctx, http.MethodDelete, fmt.Sprintf("/api/v1/chunks/%d", chunkID), nil)
	if err != nil {
		return err
	}
	return c.readResponse(resp, nil)
}

func (c *HTTPClient) ReportChunkState(ctx context.Context, nodeID NodeID, states map[ChunkID]ReplicaState) error {
	req := map[string]interface{}{"node_id": nodeID, "states": states}
	resp, err := c.doRequestWithRetry(ctx, http.MethodPost, "/api/v1/chunks/report-state", req)
	if err != nil {
		return err
	}
	return c.readResponse(resp, nil)
}

// Cluster operations

func (c *HTTPClient) RegisterNode(ctx context.Context, info *NodeInfo) error {
	resp, err := c.doRequestWithRetry(ctx, http.MethodPost, "/api/v1/nodes", info)
	if err != nil {
		return err
	}
	return c.readResponse(resp, nil)
}

func (c *HTTPClient) Heartbeat(ctx context.Context, nodeID NodeID, report *NodeReport) error {
	path := fmt.Sprintf("/api/v1/nodes/%d/heartbeat", nodeID)
	resp, err := c.doRequestWithRetry(ctx, http.MethodPost, path, report)
	if err != nil {
		return err
	}
	return c.readResponse(resp, nil)
}

func (c *HTTPClient) DecommissionNode(ctx context.Context, nodeID NodeID) error {
	resp, err := c.doRequestWithRetry(ctx, http.MethodPost, fmt.Sprintf("/api/v1/nodes/%d/decommission", nodeID), nil)
	if err != nil {
		return err
	}
	return c.readResponse(resp, nil)
}

func (c *HTTPClient) EnterMaintenance(ctx context.Context, nodeID NodeID) error {
	resp, err := c.doRequestWithRetry(ctx, http.MethodPost, fmt.Sprintf("/api/v1/nodes/%d/maintenance", nodeID), nil)
	if err != nil {
		return err
	}
	return c.readResponse(resp, nil)
}

func (c *HTTPClient) ExitMaintenance(ctx context.Context, nodeID NodeID) error {
	resp, err := c.doRequestWithRetry(ctx, http.MethodDelete, fmt.Sprintf("/api/v1/nodes/%d/maintenance", nodeID), nil)
	if err != nil {
		return err
	}
	return c.readResponse(resp, nil)
}

func (c *HTTPClient) RollingUpgradePlan(ctx context.Context) ([]NodeID, error) {
	resp, err := c.doRequestWithRetry(ctx, http.MethodGet, "/api/v1/nodes/upgrade-plan", nil)
	if err != nil {
		return nil, err
	}
	var plan []NodeID
	if err := c.readResponse(resp, &plan); err != nil {
		return nil, err
	}
	return plan, nil
}

func (c *HTTPClient) ListNodes(ctx context.Context) ([]NodeInfo, error) {
	resp, err := c.doRequestWithRetry(ctx, http.MethodGet, "/api/v1/nodes", nil)
	if err != nil {
		return nil, err
	}
	var nodes []NodeInfo
	if err := c.readResponse(resp, &nodes); err != nil {
		return nil, err
	}
	return nodes, nil
}

func (c *HTTPClient) GetNode(ctx context.Context, nodeID NodeID) (*NodeInfo, error) {
	path := fmt.Sprintf("/api/v1/nodes/%d", nodeID)
	resp, err := c.doRequestWithRetry(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}
	var node NodeInfo
	if err := c.readResponse(resp, &node); err != nil {
		return nil, err
	}
	return &node, nil
}

// Repair operations

func (c *HTTPClient) GetRepairQueue(ctx context.Context) ([]RepairTask, error) {
	resp, err := c.doRequestWithRetry(ctx, http.MethodGet, "/api/v1/repair/queue", nil)
	if err != nil {
		return nil, err
	}
	var tasks []RepairTask
	if err := c.readResponse(resp, &tasks); err != nil {
		return nil, err
	}
	return tasks, nil
}

func (c *HTTPClient) TriggerRepair(ctx context.Context, chunkID ChunkID) error {
	req := map[string]interface{}{"chunk_id": chunkID}
	resp, err := c.doRequestWithRetry(ctx, http.MethodPost, "/api/v1/repair/trigger", req)
	if err != nil {
		return err
	}
	return c.readResponse(resp, nil)
}

func (c *HTTPClient) RemoveRepairTask(ctx context.Context, chunkID ChunkID) error {
	resp, err := c.doRequestWithRetry(ctx, http.MethodDelete, fmt.Sprintf("/api/v1/repair/%d", chunkID), nil)
	if err != nil {
		return err
	}
	return c.readResponse(resp, nil)
}

// Scaling operations

func (c *HTTPClient) ChunksByNode(ctx context.Context, nodeID NodeID) ([]ChunkMeta, error) {
	path := fmt.Sprintf("/api/v1/nodes/%d/chunks", nodeID)
	resp, err := c.doRequestWithRetry(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}
	var chunks []ChunkMeta
	if err := c.readResponse(resp, &chunks); err != nil {
		return nil, err
	}
	return chunks, nil
}

func (c *HTTPClient) MigrateChunkReplica(ctx context.Context, chunkID ChunkID, fromNode, toNode NodeID) error {
	req := map[string]interface{}{
		"chunk_id":  chunkID,
		"from_node": fromNode,
		"to_node":   toNode,
	}
	resp, err := c.doRequestWithRetry(ctx, http.MethodPost, "/api/v1/chunks/migrate-replica", req)
	if err != nil {
		return err
	}
	return c.readResponse(resp, nil)
}

// Rebalance operations

func (c *HTTPClient) TriggerRebalance(ctx context.Context) error {
	resp, err := c.doRequestWithRetry(ctx, http.MethodPost, "/api/v1/rebalance/trigger", nil)
	if err != nil {
		return err
	}
	return c.readResponse(resp, nil)
}

func (c *HTTPClient) Close() error {
	return nil
}

// Advisory lock operations — proxied to the metad HTTP API. The
// remote server is the single source of truth for the in-memory
// lock table, so all clients connected to the same metad see a
// consistent view of who holds what.

func (c *HTTPClient) AdvisoryLock(ctx context.Context, inode InodeID, owner string) error {
	return c.advisoryLockCall(ctx, "/api/v1/locks/acquire", inode, owner, "exclusive")
}

func (c *HTTPClient) AdvisoryLockShared(ctx context.Context, inode InodeID, owner string) error {
	return c.advisoryLockCall(ctx, "/api/v1/locks/acquire", inode, owner, "shared")
}

func (c *HTTPClient) AdvisoryUnlock(ctx context.Context, inode InodeID, owner string) error {
	req := map[string]interface{}{"inode": inode, "owner": owner}
	resp, err := c.doRequestWithRetry(ctx, http.MethodPost, "/api/v1/locks/release", req)
	if err != nil {
		return err
	}
	return c.readResponse(resp, nil)
}

func (c *HTTPClient) AdvisoryListLocks(ctx context.Context, inode InodeID) ([]LockInfo, error) {
	path := fmt.Sprintf("/api/v1/locks?inode=%d", inode)
	resp, err := c.doRequestWithRetry(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}
	var locks []LockInfo
	if err := c.readResponse(resp, &locks); err != nil {
		return nil, err
	}
	return locks, nil
}

func (c *HTTPClient) advisoryLockCall(ctx context.Context, path string, inode InodeID, owner, mode string) error {
	req := map[string]interface{}{"inode": inode, "owner": owner, "mode": mode}
	resp, err := c.doRequestWithRetry(ctx, http.MethodPost, path, req)
	if err != nil {
		return err
	}
	// Map HTTP 409 (Conflict) to ErrLockBusy so the caller's
	// errors.Is checks see a typed error rather than a generic
	// "metad: ... (status=409)" string. Other 4xx/5xx responses
	// fall through to readResponse and surface as before.
	if resp.StatusCode == http.StatusConflict {
		resp.Body.Close()
		return ErrLockBusy
	}
	return c.readResponse(resp, nil)
}

// ========== Extended attributes (stubs — xattr not yet exposed via HTTP) ==========

func (c *HTTPClient) ComputeAllBucketUsage(ctx context.Context) ([]BucketUsage, error) {
	path := "/api/v1/admin/bucket-usage"
	resp, err := c.doRequestWithRetry(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var usage []BucketUsage
	if err := c.readResponse(resp, &usage); err != nil {
		return nil, err
	}
	return usage, nil
}

func (c *HTTPClient) GetXAttr(_ context.Context, _ InodeID, _ string) ([]byte, error) {
	return nil, ErrXAttrNotFound
}
func (c *HTTPClient) SetXAttr(_ context.Context, _ InodeID, _ string, _ []byte) error {
	return fmt.Errorf("xattr not yet supported over HTTP")
}
func (c *HTTPClient) ListXAttr(_ context.Context, _ InodeID) (map[string][]byte, error) {
	return nil, nil
}
func (c *HTTPClient) RemoveXAttr(_ context.Context, _ InodeID, _ string) error {
	return fmt.Errorf("xattr not yet supported over HTTP")
}

// ========== AccessControlService Implementation ==========

func (c *HTTPClient) SetBucketPolicy(ctx context.Context, bucket string, policy BucketPolicy) error {
	req := map[string]interface{}{
		"bucket": bucket,
		"policy": policy,
	}
	resp, err := c.doRequestWithRetry(ctx, http.MethodPut, "/api/v1/acl/"+bucket, req)
	if err != nil {
		return err
	}
	return c.readResponse(resp, nil)
}

func (c *HTTPClient) GetBucketPolicy(ctx context.Context, bucket string) (*BucketPolicy, error) {
	resp, err := c.doRequestWithRetry(ctx, http.MethodGet, "/api/v1/acl/"+bucket, nil)
	if err != nil {
		return nil, err
	}
	var policy BucketPolicy
	if err := c.readResponse(resp, &policy); err != nil {
		return nil, err
	}
	return &policy, nil
}

func (c *HTTPClient) DeleteBucketPolicy(ctx context.Context, bucket string) error {
	resp, err := c.doRequestWithRetry(ctx, http.MethodDelete, "/api/v1/acl/"+bucket, nil)
	if err != nil {
		return err
	}
	return c.readResponse(resp, nil)
}
