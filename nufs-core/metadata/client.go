package metadata

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/dshmyz/nufs/nufs-core/internal/tlsutil"
)

// HTTPClient implements MetadataService over HTTP to a remote metad server.
// It supports automatic retries with exponential backoff and leader redirect
// following for Raft-based deployments.
type HTTPClient struct {
	baseURL    string
	httpClient *http.Client
	timeout    time.Duration
	// authToken is set concurrently by the fuse token-refresh goroutine
	// (SetAuthToken) and read on the data path (addHeaders), so it is an
	// atomic pointer rather than a plain string field to avoid a data race.
	authToken atomic.Pointer[string]

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
// It may be called from a background refresh goroutine while the data path is
// issuing requests, so the token is stored atomically.
func (c *HTTPClient) SetAuthToken(token string) {
	c.authToken.Store(&token)
}

// ExchangeCredentialResult is the response from POST /api/v1/auth/token.
type ExchangeCredentialResult struct {
	Token      string `json:"token"`
	Principal  string `json:"principal"`
	Bucket     string `json:"bucket,omitempty"`
	ExpiresAt  int64  `json:"expires_at"`
	TTLSeconds int64  `json:"ttl_seconds"`
}

// ExchangeCredential presents an accessKey/secretKey to metad and receives a
// short-lived, principal-bound signed bearer token plus the verified
// principal. This is how a fuse mount establishes its identity: metad is the
// only party that ever verifies the secret, and the returned principal is the
// one bound to the credential — never a client-supplied claim. The returned
// token should be passed to SetAuthToken for subsequent data-plane requests.
func (c *HTTPClient) ExchangeCredential(ctx context.Context, accessKey, secretKey, bucket string) (*ExchangeCredentialResult, error) {
	req := map[string]string{
		"access_key": accessKey,
		"secret_key": secretKey,
		"bucket":     bucket,
	}
	resp, err := c.doRequestWithRetry(ctx, http.MethodPost, "/api/v1/auth/token", req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read token response: %w", err)
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		var errResp struct {
			Error string `json:"error"`
			Code  string `json:"code"`
		}
		_ = json.Unmarshal(data, &errResp)
		if resp.StatusCode == http.StatusUnauthorized || errResp.Code == "access_denied" {
			return nil, fmt.Errorf("%w: invalid credentials", ErrAccessDenied)
		}
		if errResp.Code == "token_signing_disabled" {
			return nil, fmt.Errorf("metad has no --token-signing-key configured")
		}
		return nil, fmt.Errorf("metad: %s (status=%d)", string(data), resp.StatusCode)
	}
	var out ExchangeCredentialResult
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, fmt.Errorf("unmarshal token response: %w", err)
	}
	if out.Token == "" || out.Principal == "" {
		return nil, fmt.Errorf("metad returned incomplete token response")
	}
	return &out, nil
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

// leaderRedirectMaxHops bounds how many 307/301 leader redirects we follow
// before giving up. A follower redirects to the leader; in a multi-node Raft
// cluster a freshly-elected leader may in turn have an outdated view and
// redirect once more, so we chase a few hops (mirroring the admin cluster
// client) instead of stopping at the first redirect target.
const leaderRedirectMaxHops = 5

// leaderTransitionBudget bounds how long a single metadata request keeps
// retrying through a leader reconfiguration. A hashicorp/raft election can take
// several seconds (the V2.1 failover drill measures ~8s RTO), far longer than
// the default maxRetries backoff window (~700ms), so we keep retrying transient
// 5xx / "no leader" responses for up to this budget before giving up. It is
// comfortably under the per-attempt HTTP timeout (30s) and bounded so a truly
// wedged node does not block callers indefinitely.
const leaderTransitionBudget = 10 * time.Second

// doRequestWithRetry executes an HTTP request with exponential backoff retry
// and automatic leader redirect following. It is resilient to a Raft leader
// transition: it follows multiple redirect hops toward the leader and keeps
// retrying transient 5xx responses (e.g. "no leader available" on a follower)
// for up to leaderTransitionBudget so an in-flight election does not surface a
// spurious 503 to the caller.
func (c *HTTPClient) doRequestWithRetry(ctx context.Context, method, path string, body interface{}) (*http.Response, error) {
	var lastErr error

	// started tracks wall-clock elapsed since the request began so we can bound
	// total reconfiguration retries beyond the fixed maxRetries count. While the
	// Raft cluster is reconfiguring, a follower may keep answering 503 ("no
	// leader available") for several seconds; we stay in the loop until the
	// election settles or the budget is exhausted.
	started := time.Now()

	// waitRetry<== sleeps the exponential backoff for the given attempt index,
	// returning false if the context was cancelled.
	waitBackoff := func(attempt int) bool {
		wait := c.retryBaseWait * time.Duration(1<<uint(attempt))
		if time.Since(started)+wait > leaderTransitionBudget {
			wait = 0
		}
		if wait > 0 {
			select {
			case <-time.After(wait):
			case <-ctx.Done():
				return false
			}
		}
		return true
	}

	// attempt counts the distinct "first + configured retries" rounds so the
	// terminal error keeps the familiar "(after N retries)" shape for the
	// ordinary (non-reconfiguration) failure modes.
	attempt := 0
	for {
		resp, err := c.doFollowRedirects(ctx, method, path, body)
		if err != nil {
			lastErr = err
			if attempt >= c.maxRetries && time.Since(started) >= leaderTransitionBudget {
				break
			}
			if !waitBackoff(attempt) {
				return nil, ctx.Err()
			}
			attempt++
			continue // Retry on network errors
		}

		// 429 Too Many Requests — honor Retry-After. We block until the
		// server says we can try again (short) or give up on this attempt
		// but keep looping.
		if resp.StatusCode == http.StatusTooManyRequests {
			resp.Body.Close()
			wait := parseRetryAfter(resp.Header.Get("Retry-After"), 2*time.Second)
			select {
			case <-time.After(wait):
				lastErr = fmt.Errorf("metadata server returned 429 (throttled)")
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			if attempt >= c.maxRetries && time.Since(started) >= leaderTransitionBudget {
				break
			}
			attempt++
			continue
		}

		// Retry on server errors (5xx) and connection-level issues. During a
		// leader transition a follower briefly returns 503 ("no leader
		// available"); keep retrying until the election settles (bounded by
		// leaderTransitionBudget) so the write is served rather than surfacing
		// a spurious 503.
		if resp.StatusCode >= 500 {
			resp.Body.Close()
			lastErr = fmt.Errorf("metad: server error (status=%d)", resp.StatusCode)
			if time.Since(started) >= leaderTransitionBudget {
				return nil, fmt.Errorf("metad: %w (after %d retries)", lastErr, c.maxRetries)
			}
			if !waitBackoff(attempt) {
				return nil, ctx.Err()
			}
			attempt++
			continue
		}

		return resp, nil
	}

	return nil, fmt.Errorf("metad: %w (after %d retries)", lastErr, c.maxRetries)
}

// doFollowRedirects issues the request and follows up to leaderRedirectMaxHops
// 307/301 leader redirects. The caller owns the returned response's Body on
// success; on error (network failure or redirect exhaustion) the Body is closed
// and an error is returned.
func (c *HTTPClient) doFollowRedirects(ctx context.Context, method, path string, body interface{}) (*http.Response, error) {
	currentBase := c.baseURL
	for hop := 0; hop < leaderRedirectMaxHops; hop++ {
		resp, err := c.doRequestAt(ctx, method, path, body, currentBase)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode != http.StatusMovedPermanently && resp.StatusCode != http.StatusTemporaryRedirect {
			return resp, nil
		}
		location := resp.Header.Get("Location")
		resp.Body.Close()
		if location == "" {
			return nil, fmt.Errorf("metad: redirect without location")
		}
		redirectURL, err := url.Parse(location)
		if err != nil || redirectURL.Host == "" {
			return nil, fmt.Errorf("metad: invalid redirect location")
		}
		currentBase = redirectURL.Scheme + "://" + redirectURL.Host
	}
	return nil, fmt.Errorf("metad: exceeded %d redirect hops", leaderRedirectMaxHops)
}

func (c *HTTPClient) doAllocationRequest(ctx context.Context, path string, body interface{}) (*http.Response, error) {
	resp, err := c.doRequest(ctx, http.MethodPost, path, body)
	if err != nil {
		return nil, fmt.Errorf("%w: allocation outcome unknown: %v", ErrRaftConditionalOutcomeUnknown, err)
	}
	if resp.StatusCode == http.StatusMovedPermanently || resp.StatusCode == http.StatusTemporaryRedirect {
		location := resp.Header.Get("Location")
		resp.Body.Close()
		if location == "" {
			return nil, fmt.Errorf("metad: redirect without location")
		}
		redirectURL, err := url.Parse(location)
		if err != nil || redirectURL.Host == "" {
			return nil, fmt.Errorf("metad: invalid redirect location")
		}
		redirectBase := redirectURL.Scheme + "://" + redirectURL.Host
		var reqBody io.Reader
		if body != nil {
			data, err := json.Marshal(body)
			if err != nil {
				return nil, fmt.Errorf("marshal redirect body: %w", err)
			}
			reqBody = bytes.NewReader(data)
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, redirectBase+path, reqBody)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		c.addHeaders(req)
		resp, err = c.httpClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("%w: allocation outcome unknown after redirect: %v", ErrRaftConditionalOutcomeUnknown, err)
		}
	}
	return resp, nil
}

func (c *HTTPClient) doRequest(ctx context.Context, method, path string, body interface{}) (*http.Response, error) {
	return c.doRequestAt(ctx, method, path, body, c.baseURL)
}

func (c *HTTPClient) doRequestAt(ctx context.Context, method, path string, body interface{}, baseURL string) (*http.Response, error) {
	var reqBody io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("marshal request: %w", err)
		}
		reqBody = bytes.NewReader(data)
	}

	req, err := http.NewRequestWithContext(ctx, method, baseURL+path, reqBody)
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
	if tok := c.authToken.Load(); tok != nil && *tok != "" {
		req.Header.Set("Authorization", "Bearer "+*tok)
	}
}

// parseRetryAfter parses the HTTP Retry-After header (RFC 7231) which may
// be an integer number of seconds or an HTTP-date. Returns def on parse
// failure or when header is empty.
func parseRetryAfter(h string, def time.Duration) time.Duration {
	h = strings.TrimSpace(h)
	if h == "" {
		return def
	}
	// Try seconds first (most common).
	if n, err := strconv.Atoi(h); err == nil && n > 0 {
		return time.Duration(n) * time.Second
	}
	// Try HTTP-date (e.g. "Wed, 21 Oct 2015 07:28:00 GMT").
	if t, err := http.ParseTime(h); err == nil {
		d := time.Until(t)
		if d < 0 {
			return def
		}
		return d
	}
	return def
}

func (c *HTTPClient) readResponse(resp *http.Response, v interface{}) error {
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		var errResp struct {
			Error string `json:"error"`
			Code  string `json:"code"`
		}
		_ = json.Unmarshal(data, &errResp)
		if errResp.Code == "quota_exceeded" {
			if errResp.Error == "" {
				return ErrQuotaExceeded
			}
			return fmt.Errorf("%w: %s", ErrQuotaExceeded, errResp.Error)
		}
		if resp.StatusCode == http.StatusConflict {
			if errResp.Code == "node_already_registered" {
				return ErrNodeAlreadyExists
			}
			if errResp.Code == "allocation_outcome_unknown" {
				if errResp.Error == "" {
					return ErrRaftConditionalOutcomeUnknown
				}
				return fmt.Errorf("%w: allocation_outcome_unknown: %s", ErrRaftConditionalOutcomeUnknown, errResp.Error)
			}
			if errResp.Code == "entry_exists" {
				return ErrEntryExists
			}
		}
		if resp.StatusCode == http.StatusNotFound {
			switch errResp.Code {
			case "xattr_not_found":
				return ErrXAttrNotFound
			case "entry_not_found":
				return ErrEntryNotFound
			case "extent_not_found":
				return ErrExtentNotFound
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

func (c *HTTPClient) readAllocationResponse(resp *http.Response, v interface{}) error {
	if resp.StatusCode >= http.StatusInternalServerError {
		defer resp.Body.Close()
		_, _ = io.Copy(io.Discard, resp.Body)
		return fmt.Errorf("%w: allocation outcome unknown: server error status=%d", ErrRaftConditionalOutcomeUnknown, resp.StatusCode)
	}
	return c.readResponse(resp, v)
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
	resp, err := c.doRequestWithRetry(ctx, http.MethodDelete, "/api/v1/buckets/"+escapeBucketPathSegment(name), nil)
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

func (c *HTTPClient) GetBucketByRoot(ctx context.Context, rootInode InodeID) (*BucketInfo, error) {
	resp, err := c.doRequestWithRetry(ctx, http.MethodGet, fmt.Sprintf("/api/v1/buckets/by-root/%d", rootInode), nil)
	if err != nil {
		return nil, err
	}
	var bucket BucketInfo
	if err := c.readResponse(resp, &bucket); err != nil {
		return nil, err
	}
	return &bucket, nil
}

// BucketQuotaStatus reports a bucket's configured quota and current usage.
type BucketQuotaStatus struct {
	Bucket string       `json:"bucket"`
	Quota  *BucketQuota `json:"quota"`
	Usage  BucketUsage  `json:"usage"`
	Ratios struct {
		Bytes   float64 `json:"bytes"`
		Objects float64 `json:"objects"`
	} `json:"ratios"`
}

func (c *HTTPClient) GetBucketQuotaStatus(ctx context.Context, bucket string) (*BucketQuotaStatus, error) {
	resp, err := c.doRequestWithRetry(ctx, http.MethodGet, "/api/v1/buckets/"+escapeBucketPathSegment(bucket)+"/quota", nil)
	if err != nil {
		return nil, err
	}
	var status BucketQuotaStatus
	if err := c.readResponse(resp, &status); err != nil {
		return nil, err
	}
	return &status, nil
}

func (c *HTTPClient) GetBucketQuota(ctx context.Context, bucket string) (*BucketQuota, error) {
	status, err := c.GetBucketQuotaStatus(ctx, bucket)
	if err != nil {
		return nil, err
	}
	return status.Quota, nil
}

func (c *HTTPClient) GetBucketUsage(ctx context.Context, bucket string) (*BucketUsage, error) {
	status, err := c.GetBucketQuotaStatus(ctx, bucket)
	if err != nil {
		return nil, err
	}
	return &status.Usage, nil
}

func (c *HTTPClient) SetBucketQuota(ctx context.Context, bucket string, quota *BucketQuota) error {
	resp, err := c.doRequestWithRetry(ctx, http.MethodPut, "/api/v1/buckets/"+escapeBucketPathSegment(bucket)+"/quota", quota)
	if err != nil {
		return err
	}
	return c.readResponse(resp, nil)
}

func (c *HTTPClient) DeleteBucketQuota(ctx context.Context, bucket string) error {
	resp, err := c.doRequestWithRetry(ctx, http.MethodDelete, "/api/v1/buckets/"+escapeBucketPathSegment(bucket)+"/quota", nil)
	if err != nil {
		return err
	}
	return c.readResponse(resp, nil)
}

func (c *HTTPClient) CheckBucketQuota(ctx context.Context, bucket string, additionalBytes int64, additionalObjects int64) error {
	req := struct {
		AdditionalBytes   int64 `json:"additional_bytes"`
		AdditionalObjects int64 `json:"additional_objects"`
	}{
		AdditionalBytes:   additionalBytes,
		AdditionalObjects: additionalObjects,
	}
	resp, err := c.doRequestWithRetry(ctx, http.MethodPost, "/api/v1/buckets/"+escapeBucketPathSegment(bucket)+"/quota/check", req)
	if err != nil {
		return err
	}
	return c.readResponse(resp, nil)
}

func escapeBucketPathSegment(bucket string) string {
	switch bucket {
	case ".":
		return "%2E"
	case "..":
		return "%2E%2E"
	default:
		return url.PathEscape(bucket)
	}
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

func (c *HTTPClient) ReadDirFrom(ctx context.Context, parent InodeID, afterName string, limit int) ([]DirEntry, error) {
	path := fmt.Sprintf("/api/v1/namespace/readdir-from?parent=%d&after=%s&limit=%d", parent, url.QueryEscape(afterName), limit)
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

// CreateNode creates a special (non-regular) namespace entry (FIFO, char or
// block device, socket). rdev is the device number used by the device types.
func (c *HTTPClient) CreateNode(ctx context.Context, parent InodeID, name string, ftype FileType, mode uint32, rdev uint32) (*InodeMeta, error) {
	req := map[string]interface{}{"parent": parent, "name": name, "type": ftype, "mode": mode, "rdev": rdev}
	resp, err := c.doRequestWithRetry(ctx, http.MethodPost, "/api/v1/namespace/create-node", req)
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

// --- ExtentInodeService (V2.1 extent-layout inode surface) ---

// Compile-time check: HTTPClient satisfies the V2 extent-inode surface.
var _ ExtentInodeService = (*HTTPClient)(nil)

func (c *HTTPClient) ResolveExtents(ctx context.Context, id InodeID) ([]ExtentRef, error) {
	resp, err := c.doRequestWithRetry(ctx, http.MethodGet, fmt.Sprintf("/api/v1/inodes/%d/extents", id), nil)
	if err != nil {
		return nil, err
	}
	var out struct {
		Extents []ExtentRef `json:"extents"`
	}
	if err := c.readResponse(resp, &out); err != nil {
		return nil, err
	}
	return out.Extents, nil
}

func (c *HTTPClient) GetExtentMeta(ctx context.Context, extentID ExtentIDV2) (*ExtentMetaV2, error) {
	resp, err := c.doRequestWithRetry(ctx, http.MethodGet, fmt.Sprintf("/api/v1/extents/%d", extentID), nil)
	if err != nil {
		return nil, err
	}
	var m ExtentMetaV2
	if err := c.readResponse(resp, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

func (c *HTTPClient) SetInlineExtent(ctx context.Context, id InodeID, extent *ExtentMetaV2, size int64) error {
	req := map[string]interface{}{"extent": extent, "size": size}
	resp, err := c.doRequestWithRetry(ctx, http.MethodPut, fmt.Sprintf("/api/v1/inodes/%d/inline", id), req)
	if err != nil {
		return err
	}
	return c.readResponse(resp, nil)
}

func (c *HTTPClient) PromoteToPages(ctx context.Context, id InodeID) error {
	resp, err := c.doRequestWithRetry(ctx, http.MethodPut, fmt.Sprintf("/api/v1/inodes/%d/promote", id), nil)
	if err != nil {
		return err
	}
	return c.readResponse(resp, nil)
}

func (c *HTTPClient) AppendExtent(ctx context.Context, id InodeID, extent *ExtentMetaV2, offset int64) (uint64, error) {
	req := map[string]interface{}{"extent": extent, "offset": offset}
	resp, err := c.doRequestWithRetry(ctx, http.MethodPut, fmt.Sprintf("/api/v1/inodes/%d/append-extent", id), req)
	if err != nil {
		return 0, err
	}
	var out struct {
		ExtentRoot uint64 `json:"extent_root"`
	}
	if err := c.readResponse(resp, &out); err != nil {
		return 0, err
	}
	return out.ExtentRoot, nil
}

func (c *HTTPClient) ReplaceExtents(ctx context.Context, id InodeID, writes []ExtentWrite, size int64) error {
	req := map[string]interface{}{"writes": writes, "size": size}
	resp, err := c.doRequestWithRetry(ctx, http.MethodPut, fmt.Sprintf("/api/v1/inodes/%d/replace-extents", id), req)
	if err != nil {
		return err
	}
	return c.readResponse(resp, nil)
}

// Chunk operations

func (c *HTTPClient) AllocateChunk(ctx context.Context, inodeID InodeID, offset int64, policy PlacementPolicy) (*ChunkMeta, error) {
	req := map[string]interface{}{"inode_id": inodeID, "offset": offset, "policy": policy}
	resp, err := c.doAllocationRequest(ctx, "/api/v1/chunks", req)
	if err != nil {
		return nil, err
	}
	var chunk ChunkMeta
	if err := c.readAllocationResponse(resp, &chunk); err != nil {
		return nil, err
	}
	return &chunk, nil
}

func (c *HTTPClient) AllocateChunksBatch(ctx context.Context, inodeID InodeID, offsets []int64, policy PlacementPolicy) ([]*ChunkMeta, error) {
	if len(offsets) > MaxChunkAllocationBatch {
		return nil, fmt.Errorf("max chunk allocation batch is %d", MaxChunkAllocationBatch)
	}
	req := map[string]interface{}{"inode_id": inodeID, "offsets": offsets, "policy": policy}
	resp, err := c.doAllocationRequest(ctx, "/api/v1/chunks/batch", req)
	if err != nil {
		return nil, err
	}
	var chunks []*ChunkMeta
	if err := c.readAllocationResponse(resp, &chunks); err != nil {
		return nil, err
	}
	return chunks, nil
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

// AckChangeEvents reports the change-journal sequence this node believes it
// has delivered and returns the metadata authority's persisted reconciled
// watermark — the highest sequence metadata has in fact consumed (§12). The
// caller advances its local journal Ack only once this returns a watermark
// that covers its pending events, so no un-reconciled event is dropped.
func (c *HTTPClient) AckChangeEvents(ctx context.Context, nodeID NodeID, seq uint64) (uint64, error) {
	path := fmt.Sprintf("/api/v1/nodes/%d/change-ack", nodeID)
	req := map[string]uint64{"seq": seq}
	resp, err := c.doRequestWithRetry(ctx, http.MethodPost, path, req)
	if err != nil {
		return 0, err
	}
	var out struct {
		ChangeAck uint64 `json:"change_ack"`
	}
	if err := c.readResponse(resp, &out); err != nil {
		return 0, err
	}
	return out.ChangeAck, nil
}

func (c *HTTPClient) DecommissionNode(ctx context.Context, nodeID NodeID) error {
	resp, err := c.doRequestWithRetry(ctx, http.MethodPost, fmt.Sprintf("/api/v1/nodes/%d/decommission", nodeID), nil)
	if err != nil {
		return err
	}
	return c.readResponse(resp, nil)
}

// RestoreNode brings a decommissioned (draining), maintenance, offline, or
// failed node back to online via the control plane — the explicit inverse of
// DecommissionNode. See PebbleStore.RestoreNode.
func (c *HTTPClient) RestoreNode(ctx context.Context, nodeID NodeID) error {
	resp, err := c.doRequestWithRetry(ctx, http.MethodPost, fmt.Sprintf("/api/v1/nodes/%d/restore", nodeID), nil)
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

// Write attempt operations

func (c *HTTPClient) PutWriteAttempt(ctx context.Context, attempt *ObjectWriteAttempt) error {
	resp, err := c.doRequestWithRetry(ctx, http.MethodPut, "/api/v1/write-attempts/"+url.PathEscape(attempt.ID), attempt)
	if err != nil {
		return err
	}
	return c.readResponse(resp, nil)
}

func (c *HTTPClient) GetWriteAttempt(ctx context.Context, id string) (*ObjectWriteAttempt, error) {
	resp, err := c.doRequestWithRetry(ctx, http.MethodGet, "/api/v1/write-attempts/"+url.PathEscape(id), nil)
	if err != nil {
		return nil, err
	}
	var attempt ObjectWriteAttempt
	if err := c.readResponse(resp, &attempt); err != nil {
		return nil, err
	}
	return &attempt, nil
}

func (c *HTTPClient) ListWriteAttemptsByState(ctx context.Context, state WriteAttemptState, limit int) ([]ObjectWriteAttempt, error) {
	path := fmt.Sprintf("/api/v1/write-attempts?state=%s&limit=%d", url.QueryEscape(string(state)), limit)
	resp, err := c.doRequestWithRetry(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}
	var attempts []ObjectWriteAttempt
	if err := c.readResponse(resp, &attempts); err != nil {
		return nil, err
	}
	return attempts, nil
}

func (c *HTTPClient) DeleteWriteAttempt(ctx context.Context, id string) error {
	resp, err := c.doRequestWithRetry(ctx, http.MethodDelete, "/api/v1/write-attempts/"+url.PathEscape(id), nil)
	if err != nil {
		return err
	}
	return c.readResponse(resp, nil)
}

// Background task operations

func (c *HTTPClient) PutBackgroundTask(ctx context.Context, task *BackgroundTask) error {
	resp, err := c.doRequestWithRetry(ctx, http.MethodPut, "/api/v1/background-tasks/"+url.PathEscape(task.ID), task)
	if err != nil {
		return err
	}
	return c.readResponse(resp, nil)
}

func (c *HTTPClient) GetBackgroundTask(ctx context.Context, id string) (*BackgroundTask, error) {
	resp, err := c.doRequestWithRetry(ctx, http.MethodGet, "/api/v1/background-tasks/"+url.PathEscape(id), nil)
	if err != nil {
		return nil, err
	}
	var task BackgroundTask
	if err := c.readResponse(resp, &task); err != nil {
		return nil, err
	}
	return &task, nil
}

func (c *HTTPClient) LeaseBackgroundTask(ctx context.Context, taskType BackgroundTaskType, owner string, lease time.Duration) (*BackgroundTask, error) {
	req := map[string]interface{}{
		"type":        taskType,
		"owner":       owner,
		"lease_nanos": int64(lease),
	}
	resp, err := c.doRequestWithRetry(ctx, http.MethodPost, "/api/v1/background-tasks/lease", req)
	if err != nil {
		return nil, err
	}
	var task BackgroundTask
	if err := c.readResponse(resp, &task); err != nil {
		return nil, err
	}
	return &task, nil
}

// LeaseBackgroundTaskForNode leases a task of taskType restricted to those
// whose OwnerNodes include nodeID (the datanode ConversionWorker's
// owner-routed lease). The server ignores node_id when the task has no
// OwnerNodes (empty = any node may lease).
func (c *HTTPClient) LeaseBackgroundTaskForNode(ctx context.Context, taskType BackgroundTaskType, nodeID uint64, owner string, lease time.Duration) (*BackgroundTask, error) {
	req := map[string]interface{}{
		"type":        taskType,
		"owner":       owner,
		"lease_nanos": int64(lease),
		"node_id":     nodeID,
	}
	resp, err := c.doRequestWithRetry(ctx, http.MethodPost, "/api/v1/background-tasks/lease", req)
	if err != nil {
		return nil, err
	}
	var task BackgroundTask
	if err := c.readResponse(resp, &task); err != nil {
		return nil, err
	}
	return &task, nil
}

func (c *HTTPClient) CompleteBackgroundTask(ctx context.Context, id string) error {
	resp, err := c.doRequestWithRetry(ctx, http.MethodPost, "/api/v1/background-tasks/"+url.PathEscape(id)+"/complete", nil)
	if err != nil {
		return err
	}
	return c.readResponse(resp, nil)
}

func (c *HTTPClient) FailBackgroundTask(ctx context.Context, id string, lastErr string, maxAttempts int) error {
	req := map[string]interface{}{"last_error": lastErr, "max_attempts": maxAttempts}
	resp, err := c.doRequestWithRetry(ctx, http.MethodPost, "/api/v1/background-tasks/"+url.PathEscape(id)+"/fail", req)
	if err != nil {
		return err
	}
	return c.readResponse(resp, nil)
}

func (c *HTTPClient) GetWriteOpsStatus(ctx context.Context) (*WriteOpsStatus, error) {
	resp, err := c.doRequestWithRetry(ctx, http.MethodGet, "/api/v1/write-ops/status", nil)
	if err != nil {
		return nil, err
	}
	var status WriteOpsStatus
	if err := c.readResponse(resp, &status); err != nil {
		return nil, err
	}
	return &status, nil
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

// ========== Extended attributes ==========

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

func (c *HTTPClient) GetXAttr(ctx context.Context, id InodeID, name string) ([]byte, error) {
	path := fmt.Sprintf("/api/v1/inodes/%d/xattrs/%s", id, url.PathEscape(name))
	resp, err := c.doRequestWithRetry(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}
	var result struct {
		Value []byte `json:"value"`
	}
	if err := c.readResponse(resp, &result); err != nil {
		return nil, err
	}
	return result.Value, nil
}

func (c *HTTPClient) SetXAttr(ctx context.Context, id InodeID, name string, value []byte) error {
	req := map[string][]byte{"value": value}
	resp, err := c.doRequestWithRetry(ctx, http.MethodPut, fmt.Sprintf("/api/v1/inodes/%d/xattrs/%s", id, url.PathEscape(name)), req)
	if err != nil {
		return err
	}
	return c.readResponse(resp, nil)
}

func (c *HTTPClient) ListXAttr(ctx context.Context, id InodeID) (map[string][]byte, error) {
	resp, err := c.doRequestWithRetry(ctx, http.MethodGet, fmt.Sprintf("/api/v1/inodes/%d/xattrs", id), nil)
	if err != nil {
		return nil, err
	}
	var attrs map[string][]byte
	if err := c.readResponse(resp, &attrs); err != nil {
		return nil, err
	}
	return attrs, nil
}

func (c *HTTPClient) RemoveXAttr(ctx context.Context, id InodeID, name string) error {
	resp, err := c.doRequestWithRetry(ctx, http.MethodDelete, fmt.Sprintf("/api/v1/inodes/%d/xattrs/%s", id, url.PathEscape(name)), nil)
	if err != nil {
		return err
	}
	return c.readResponse(resp, nil)
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

// WatchEvent represents a single metadata change event delivered over
// the watch stream.
type WatchEvent struct {
	Type  string    `json:"type"` // "set" or "delete"
	Key   string    `json:"key"`  // "inode:42", "chunk:123", "bucket:photos"
	Time  time.Time `json:"time"`
	Value []byte    `json:"value,omitempty"` // optional serialized payload
}

// WatchEvents opens a long-lived streaming connection to the metad server
// and delivers events as they are published. It blocks until ctx is
// cancelled, the server closes the connection, or an error occurs.
//
// prefix filters events by key prefix (e.g. "inode:" or "chunk:"); leave
// empty for all events. The function returns an error on initial connection
// failure; afterwards any I/O error just ends the stream cleanly (nil error)
// so clients can reconnect.
func (c *HTTPClient) WatchEvents(ctx context.Context, prefix string) ([]WatchEvent, error) {
	// Build the request URL with query params.
	u := c.baseURL + "/api/v1/watch"
	if prefix != "" {
		u += "?prefix=" + url.QueryEscape(prefix)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	c.addHeaders(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("watch: status %d", resp.StatusCode)
	}
	defer resp.Body.Close()

	// Decode the stream: read ndjson lines, each line is one event.
	// The channel is unbuffered because consumer runs in this goroutine.
	var events []WatchEvent
	dec := json.NewDecoder(resp.Body)
	for {
		var e WatchEvent
		if err := dec.Decode(&e); err != nil {
			// EOF or connection reset by the server (watchdog timeout)
			// is a clean shutdown — return events we have, not an error.
			if err.Error() == "EOF" {
				return events, nil
			}
			return events, nil
		}
		events = append(events, e)
	}
}

// WatchEventsStream streams events via a channel pattern. It spawns a
// goroutine that pushes events onto the returned channel; the channel is
// closed when ctx is cancelled or the stream ends.
//
// This is the pattern preferred by gateways that want to continuously
// invalidate caches as events arrive, not just after the stream closes.
func (c *HTTPClient) WatchEventsStream(ctx context.Context, prefix string) <-chan WatchEvent {
	ch := make(chan WatchEvent, 64)
	go func() {
		defer close(ch)

		u := c.baseURL + "/api/v1/watch"
		if prefix != "" {
			u += "?prefix=" + url.QueryEscape(prefix)
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			return
		}
		c.addHeaders(req)

		resp, err := c.httpClient.Do(req)
		if err != nil {
			return
		}
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			return
		}
		defer resp.Body.Close()

		dec := json.NewDecoder(resp.Body)
		for {
			var e WatchEvent
			if err := dec.Decode(&e); err != nil {
				// End of stream — client will reconnect.
				return
			}
			// Ignore keep-alive empty events.
			if e.Key == "" {
				continue
			}
			select {
			case ch <- e:
			case <-ctx.Done():
				return
			}
		}
	}()
	return ch
}

// --- EC conversion lifecycle authority (Program A / S2) ---
//
// These methods implement the metadata.ECAuthority seam (datanode/ec_service.go)
// over HTTP to the metad service. On the V2.1 serving path the datanode drives
// a replication→6+3 conversion through the remote authority (production
// topology) instead of an in-process local ECStore (S1). Go interfaces are
// structural, so *HTTPClient satisfies datanode.ECAuthority exactly as long as
// these signatures match. Each mutation loads the authoritative stripe on the
// server and returns the full updated stripe; the caller copies it back into
// its local *ECStripe so the transaction view stays authoritative.

// BeginConversion starts an EC conversion transaction on the remote authority.
func (c *HTTPClient) BeginConversion(stripeID string, extentID uint64, gen uint64, checksum uint32) (*ECStripe, error) {
	req := map[string]interface{}{"stripe_id": stripeID, "extent_id": extentID, "generation": gen, "checksum": checksum}
	resp, err := c.doRequestWithRetry(context.Background(), http.MethodPost, "/api/v1/ec/convert/begin", req)
	if err != nil {
		return nil, err
	}
	var st ECStripe
	if err := c.readResponse(resp, &st); err != nil {
		return nil, err
	}
	return &st, nil
}

// PlanShards fills the stripe's §14-diverse placement on the remote authority.
func (c *HTTPClient) PlanShards(st *ECStripe, disks []ECDisk) error {
	req := map[string]interface{}{"stripe_id": st.StripeID, "disks": disks}
	resp, err := c.doRequestWithRetry(context.Background(), http.MethodPost, "/api/v1/ec/convert/plan", req)
	if err != nil {
		return err
	}
	var updated ECStripe
	if err := c.readResponse(resp, &updated); err != nil {
		return err
	}
	*st = updated
	return nil
}

// MarkSyncing advances the transaction to Syncing on the remote authority.
func (c *HTTPClient) MarkSyncing(st *ECStripe) error {
	req := map[string]interface{}{"stripe_id": st.StripeID}
	resp, err := c.doRequestWithRetry(context.Background(), http.MethodPost, "/api/v1/ec/convert/mark-syncing", req)
	if err != nil {
		return err
	}
	var updated ECStripe
	if err := c.readResponse(resp, &updated); err != nil {
		return err
	}
	*st = updated
	return nil
}

// CompleteConversion finalizes the stripe as durable on the remote authority.
func (c *HTTPClient) CompleteConversion(st *ECStripe, at time.Time) error {
	req := map[string]interface{}{"stripe_id": st.StripeID, "at": at.UnixNano()}
	resp, err := c.doRequestWithRetry(context.Background(), http.MethodPost, "/api/v1/ec/convert/complete", req)
	if err != nil {
		return err
	}
	var updated ECStripe
	if err := c.readResponse(resp, &updated); err != nil {
		return err
	}
	*st = updated
	return nil
}

// PublishConversion performs the atomic §14 layout switch on the remote
// authority: it tells metad to lift a completed conversion stripe's EC layout
// into the chunk's authoritative metadata (Replicas → nine shard placements,
// ECGroup set). It is the remote half of the datanode ECService publish hook.
func (c *HTTPClient) PublishConversion(st *ECStripe) error {
	req := map[string]interface{}{"stripe_id": st.StripeID}
	resp, err := c.doRequestWithRetry(context.Background(), http.MethodPost, "/api/v1/ec/convert/publish", req)
	if err != nil {
		return err
	}
	var updated ECStripe
	if err := c.readResponse(resp, &updated); err != nil {
		return err
	}
	*st = updated
	return nil
}

// ResolveStripeLanding resolves a chunk's authoritative per-shard landing (§14)
// over HTTP: the server loads the chunk by ID, resolves its durable
// ECStripe.Shards, and returns them. It structurally satisfies the datanode
// ECLandingResolver seam so a V2.1 node's ECSelfHealer repair-landing runs
// against the *remote* metadata authority (Program 7). A chunk with no stripe
// (not yet converted to EC) yields an empty landing.
func (c *HTTPClient) ResolveStripeLanding(chunk *ChunkMeta) ([]ECShard, error) {
	req := map[string]interface{}{"chunk_id": uint64(chunk.ID)}
	resp, err := c.doRequestWithRetry(context.Background(), http.MethodPost, "/api/v1/ec/convert/resolve-landing", req)
	if err != nil {
		return nil, err
	}
	var out struct {
		Shards []ECShard `json:"shards"`
	}
	if err := c.readResponse(resp, &out); err != nil {
		return nil, err
	}
	return out.Shards, nil
}

// IsChunkShardsOrphaned answers whether a chunk's shards on this node are
// reclaimable orphans over HTTP: the server runs the authoritative
// IsChunkShardsOrphaned decision (Complete → live; rolled-back-and-aged →
// orphaned; in-flight/young → not) against the remote metadata authority
// (Program 7). olderThan is the age gate below which a rolled-back stripe's
// shards are not yet reclaimable. It structurally satisfies the datanode
// ECorphanResolver seam so orphan GC works against the remote authority.
func (c *HTTPClient) IsChunkShardsOrphaned(ctx context.Context, chunkID ChunkID, olderThan time.Duration) (bool, error) {
	req := map[string]interface{}{"chunk_id": uint64(chunkID), "older_than_ns": int64(olderThan)}
	resp, err := c.doRequestWithRetry(ctx, http.MethodPost, "/api/v1/ec/convert/is-orphan", req)
	if err != nil {
		return false, err
	}
	var out struct {
		Orphaned bool `json:"orphaned"`
	}
	if err := c.readResponse(resp, &out); err != nil {
		return false, err
	}
	return out.Orphaned, nil
}

// RollbackConversion aborts a non-durable transaction on the remote authority.
func (c *HTTPClient) RollbackConversion(st *ECStripe, reason string) error {
	req := map[string]interface{}{"stripe_id": st.StripeID, "reason": reason}
	resp, err := c.doRequestWithRetry(context.Background(), http.MethodPost, "/api/v1/ec/convert/rollback", req)
	if err != nil {
		return err
	}
	var updated ECStripe
	if err := c.readResponse(resp, &updated); err != nil {
		return err
	}
	*st = updated
	return nil
}

// PlanECWrite queries the remote authority for where each shard of a write-path
// direct EC write should land (§14, Program 10). The gateway encodes K+M shards
// up front and pushes each shard straight to the owning node's shard store; the
// authority (not the gateway) decides the per-shard (NodeID, DiskID) placement
// for fault-domain diversity. It structurally satisfies the chunkstore
// ECWriteAuthority seam so *HTTPClient can be injected as the direct-write
// authority. shards' NodeID is aligned with the allocated Replicas' node per
// shard index (the server resolves it from the chunk), so the caller can push
// shard i to Replicas[i].Addr with a disk of shards[i].DiskID % 1000.
func (c *HTTPClient) PlanECWrite(ctx context.Context, chunkID ChunkID, dataShards, parityShards int) ([]ECShard, error) {
	req := map[string]interface{}{"chunk_id": uint64(chunkID), "data_shards": dataShards, "parity_shards": parityShards}
	resp, err := c.doRequestWithRetry(ctx, http.MethodPost, "/api/v1/ec/plan-write", req)
	if err != nil {
		return nil, err
	}
	var out struct {
		Shards []ECShard `json:"shards"`
	}
	if err := c.readResponse(resp, &out); err != nil {
		return nil, err
	}
	return out.Shards, nil
}

// RecordDirectEC records a directly-written EC chunk on the remote authority
// (§14, Program 10): after the gateway has pushed every shard to its owning
// node's shard store it reports the plan + original checksum, and the authority
// durably lifts the chunk into EC (Complete stripe + ChunkMeta.ECStripeID) — the
// same served state a converted chunk reaches through publish. It structurally
// satisfies the chunkstore ECWriteAuthority seam.
func (c *HTTPClient) RecordDirectEC(ctx context.Context, chunkID ChunkID, dataShards, parityShards int, shards []ECShard, checksum uint32) error {
	req := map[string]interface{}{
		"chunk_id": uint64(chunkID), "shards": shards,
		"data_shards": dataShards, "parity_shards": parityShards,
		"original_checksum": checksum,
	}
	resp, err := c.doRequestWithRetry(ctx, http.MethodPost, "/api/v1/ec/record-direct", req)
	if err != nil {
		return err
	}
	return c.readResponse(resp, nil)
}
