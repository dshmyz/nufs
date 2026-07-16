// Package cluster provides cluster registry and client for proxying requests.
package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Client wraps HTTP client for a single NUFS cluster.
type Client struct {
	name            string
	baseURL         string
	region          string
	description     string
	http            *http.Client
	lastHealthCheck time.Time
}

// NewClient creates a cluster client.
func NewClient(name, baseURL string) *Client {
	return &Client{
		name:    name,
		baseURL: baseURL,
		http: &http.Client{
			Timeout: 3 * time.Second,
		},
	}
}

// SetMetadata sets region and description for the client.
func (c *Client) SetMetadata(region, description string) {
	c.region = region
	c.description = description
}

// Get performs GET request and unmarshals JSON response.
func (c *Client) Get(ctx context.Context, path string, result interface{}) error {
	url := c.baseURL + path
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("cluster %s unreachable: %w", c.name, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("cluster %s returned %d: %s", c.name, resp.StatusCode, body)
	}

	if result != nil {
		if err := json.NewDecoder(resp.Body).Decode(result); err != nil {
			return fmt.Errorf("cluster %s decode error: %w", c.name, err)
		}
	}

	return nil
}

// Post performs POST request with JSON body.
func (c *Client) Post(ctx context.Context, path string, body io.Reader, result interface{}) error {
	url := c.baseURL + path
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("cluster %s unreachable: %w", c.name, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("cluster %s returned %d: %s", c.name, resp.StatusCode, respBody)
	}

	if result != nil {
		if err := json.NewDecoder(resp.Body).Decode(result); err != nil {
			return fmt.Errorf("cluster %s decode error: %w", c.name, err)
		}
	}

	return nil
}

// Delete performs DELETE request.
func (c *Client) Delete(ctx context.Context, path string) error {
	url := c.baseURL + path
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)
	if err != nil {
		return err
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("cluster %s unreachable: %w", c.name, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("cluster %s returned %d: %s", c.name, resp.StatusCode, body)
	}

	return nil
}

// CheckHealth probes /health endpoint.
func (c *Client) CheckHealth(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	url := c.baseURL + "/health"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("health check returned %d", resp.StatusCode)
	}

	c.lastHealthCheck = time.Now()
	return nil
}