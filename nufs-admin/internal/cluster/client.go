// Package cluster provides cluster registry and client for proxying requests.
package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"
)

// Client wraps HTTP client for a single NUFS cluster.
type Client struct {
	name            string
	baseURL         string
	region          string
	description     string
	token           string
	datanodeOpsPort int
	http            *http.Client
	lastHealthCheck time.Time
	maxRedirectHops int
	maxRetries      int
	baseRetryDelay  time.Duration
}

const maxUpstreamErrorBody = 64 << 10

// ClientOption configures a Client.
type ClientOption func(*Client)

// WithMaxRetries sets the maximum number of retries for 5xx/429 errors.
func WithMaxRetries(n int) ClientOption {
	return func(c *Client) { c.maxRetries = n }
}

// WithRetryBaseDelay sets the base delay for exponential backoff.
func WithRetryBaseDelay(d time.Duration) ClientOption {
	return func(c *Client) { c.baseRetryDelay = d }
}

// WithMaxRedirectHops sets the maximum number of 307 redirects to follow.
func WithMaxRedirectHops(n int) ClientOption {
	return func(c *Client) { c.maxRedirectHops = n }
}

// WithHTTPTimeout sets the HTTP client timeout.
func WithHTTPTimeout(d time.Duration) ClientOption {
	return func(c *Client) { c.http.Timeout = d }
}

// WithMetadToken sets the Bearer token presented to the metad ops API.
// Empty (default) leaves requests unauthenticated (dev-mode metad).
func WithMetadToken(t string) ClientOption {
	return func(c *Client) { c.token = t }
}

// WithDatanodeOpsPort sets the port used to reach each datanode's ops HTTP
// API for disk/GC lifecycle actions proxied from the console. 0 = default 18096.
func WithDatanodeOpsPort(p int) ClientOption {
	return func(c *Client) {
		if p == 0 {
			p = 18096
		}
		c.datanodeOpsPort = p
	}
}

// DoDatanodeOps proxies an ops call to a single datanode: nodeAddr is the
// node's chunk host:port as reported by metad; the ops port is the cluster's
// configured datanode_ops_port. Used by the console to drive disk lifecycle,
// GC and node decommission from the management plane (server-side, so it works
// even when control-plane ports are not reachable from the operator browser).
func (c *Client) DoDatanodeOps(ctx context.Context, nodeAddr, opPath, method string, body io.Reader, result interface{}) error {
	host, _, err := net.SplitHostPort(nodeAddr)
	if err != nil || host == "" {
		host = nodeAddr
	}
	port := c.datanodeOpsPort
	if port == 0 {
		port = 18096
	}
	url := fmt.Sprintf("http://%s:%d/api/v1/%s", host, port, strings.TrimPrefix(opPath, "/"))
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return fmt.Errorf("datanode ops request: %w", err)
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("datanode ops %s: %w", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		return &UpstreamHTTPError{StatusCode: resp.StatusCode, Body: b, ContentType: resp.Header.Get("Content-Type")}
	}
	if result == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(result)
}

// UpstreamHTTPError preserves a metad HTTP failure for API-layer translation.
type UpstreamHTTPError struct {
	StatusCode  int
	Body        []byte
	ContentType string
}

func (e *UpstreamHTTPError) Error() string {
	return fmt.Sprintf("upstream returned HTTP %d", e.StatusCode)
}

// NewClient creates a cluster client with sensible defaults.
// Override defaults with ClientOption funcs (WithMaxRetries, etc.).
func NewClient(name, baseURL string, opts ...ClientOption) *Client {
	c := &Client{
		name:    name,
		baseURL: baseURL,
		http: &http.Client{
			Timeout: 3 * time.Second,
			// Disable automatic redirect following — we handle 307 manually
			// for leader following with retry logic.
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		maxRedirectHops: 5,
		maxRetries:      3,
		baseRetryDelay:  200 * time.Millisecond,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// SetMetadata sets region and description for the client.
func (c *Client) SetMetadata(region, description string) {
	c.region = region
	c.description = description
}

// Get performs GET request and unmarshals JSON response.
func (c *Client) Get(ctx context.Context, path string, result interface{}) error {
	return c.doJSON(ctx, http.MethodGet, path, nil, result)
}

// Post performs POST request with JSON body.
func (c *Client) Post(ctx context.Context, path string, body io.Reader, result interface{}) error {
	return c.doJSON(ctx, http.MethodPost, path, body, result)
}

// Put performs PUT request with JSON body.
func (c *Client) Put(ctx context.Context, path string, body io.Reader, result interface{}) error {
	return c.doJSON(ctx, http.MethodPut, path, body, result)
}

// Delete performs DELETE request.
func (c *Client) Delete(ctx context.Context, path string) error {
	return c.doJSON(ctx, http.MethodDelete, path, nil, nil)
}

func (c *Client) doJSON(ctx context.Context, method, path string, body io.Reader, result interface{}) error {
	// Buffer body so we can replay on retry/redirect.
	var bodyBytes []byte
	if body != nil {
		var err error
		bodyBytes, err = io.ReadAll(body)
		if err != nil {
			return fmt.Errorf("read request body: %w", err)
		}
	}

	for attempt := 0; attempt <= c.maxRetries; attempt++ {
		if attempt > 0 {
			delay := c.baseRetryDelay * (1 << (attempt - 1))
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
		}

		resp, err := c.doWithRedirects(ctx, method, c.baseURL+path, bodyBytes, result)
		if err != nil {
			if attempt == c.maxRetries {
				return fmt.Errorf("cluster %s: %w (after %d retries)", c.name, err, c.maxRetries)
			}
			slog.Warn("cluster client request failed, retrying",
				"cluster", c.name, "attempt", attempt+1, "error", err)
			continue
		}

		// Retry on 5xx and 429.
		if resp.StatusCode >= 500 || resp.StatusCode == http.StatusTooManyRequests {
			responseBody, _ := io.ReadAll(io.LimitReader(resp.Body, maxUpstreamErrorBody))
			resp.Body.Close()
			if attempt < c.maxRetries {
				slog.Warn("upstream error, retrying",
					"cluster", c.name, "status", resp.StatusCode, "attempt", attempt+1)
				continue
			}
			if len(responseBody) > maxUpstreamErrorBody {
				responseBody = responseBody[:maxUpstreamErrorBody]
			}
			return &UpstreamHTTPError{
				StatusCode:  resp.StatusCode,
				Body:        bytes.Clone(responseBody),
				ContentType: resp.Header.Get("Content-Type"),
			}
		}

		// Non-retryable error (4xx client errors).
		if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
			responseBody, readErr := io.ReadAll(io.LimitReader(resp.Body, maxUpstreamErrorBody+1))
			if readErr != nil {
				resp.Body.Close()
				return fmt.Errorf("cluster %s read error response: %w", c.name, readErr)
			}
			resp.Body.Close()
			if len(responseBody) > maxUpstreamErrorBody {
				responseBody = responseBody[:maxUpstreamErrorBody]
			}
			return &UpstreamHTTPError{
				StatusCode:  resp.StatusCode,
				Body:        bytes.Clone(responseBody),
				ContentType: resp.Header.Get("Content-Type"),
			}
		}

		// Success — decode.
		if result != nil {
			if err := json.NewDecoder(resp.Body).Decode(result); err != nil && err != io.EOF {
				resp.Body.Close()
				return fmt.Errorf("cluster %s decode error: %w", c.name, err)
			}
		}
		resp.Body.Close()
		return nil
	}

	return fmt.Errorf("cluster %s: exhausted %d retries", c.name, c.maxRetries)
}

// doWithRedirects issues a single request and follows 307 redirects up to c.maxRedirectHops.
// Returns the final response (caller owns and must close Body) or an error.
func (c *Client) doWithRedirects(ctx context.Context, method, url string, bodyBytes []byte, result interface{}) (*http.Response, error) {
	currentURL := url

	for hop := 0; hop <= c.maxRedirectHops; hop++ {
		var reqBody io.Reader
		if bodyBytes != nil {
			reqBody = bytes.NewReader(bodyBytes)
		}

		req, err := http.NewRequestWithContext(ctx, method, currentURL, reqBody)
		if err != nil {
			return nil, err
		}
		if bodyBytes != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		if c.token != "" {
			req.Header.Set("Authorization", "Bearer "+c.token)
		}

		resp, err := c.http.Do(req)
		if err != nil {
			return nil, fmt.Errorf("unreachable: %w", err)
		}

		if resp.StatusCode != http.StatusTemporaryRedirect {
			return resp, nil
		}

		// Follow 307 redirect (leader following).
		location := resp.Header.Get("Location")
		resp.Body.Close()
		if location == "" {
			return nil, fmt.Errorf("307 without Location header")
		}
		currentURL = location
		slog.Info("following 307 redirect", "cluster", c.name, "to", location)
	}

	return nil, fmt.Errorf("exceeded %d redirect hops", c.maxRedirectHops)
}

// CheckHealth probes /health endpoint.
// A 307 redirect is treated as healthy: the node is alive and knows the leader.
func (c *Client) CheckHealth(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	url := c.baseURL + "/health"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// 307 means the node is alive and redirecting to the leader — healthy.
	if resp.StatusCode == http.StatusTemporaryRedirect {
		c.lastHealthCheck = time.Now()
		return nil
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("health check returned %d", resp.StatusCode)
	}

	c.lastHealthCheck = time.Now()
	return nil
}
