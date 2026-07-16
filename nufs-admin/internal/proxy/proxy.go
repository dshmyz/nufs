// Package proxy provides request proxying to NUFS clusters.
package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/your-org/nufs-admin/internal/cache"
	"github.com/your-org/nufs-admin/internal/cluster"
)

// Proxy handles request proxying to a single cluster.
type Proxy struct {
	Registry  *cluster.Registry
	cache     *cache.Cache
}

// NewProxy creates a proxy with registry and cache.
func NewProxy(registry *cluster.Registry, cache *cache.Cache) *Proxy {
	return &Proxy{
		Registry: registry,
		cache:    cache,
	}
}

// Get proxies GET request to a cluster with caching.
func (p *Proxy) Get(ctx context.Context, clusterName, path string, result interface{}) error {
	// Check cache first
	cacheKey := fmt.Sprintf("%s:%s", clusterName, path)
	if cached, ok := p.cache.Get(cacheKey); ok {
		if result != nil {
			if err := json.Unmarshal(cached, result); err != nil {
				return fmt.Errorf("cache unmarshal error: %w", err)
			}
		}
		return nil
	}

	// Get cluster client
	client, ok := p.Registry.GetClient(clusterName)
	if !ok {
		return fmt.Errorf("cluster %s not found", clusterName)
	}

	// Proxy request
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	var raw json.RawMessage
	if err := client.Get(ctx, path, &raw); err != nil {
		return err
	}

	// Cache response
	p.cache.Set(cacheKey, raw)

	// Unmarshal to result
	if result != nil {
		if err := json.Unmarshal(raw, result); err != nil {
			return fmt.Errorf("unmarshal error: %w", err)
		}
	}

	return nil
}

// Post proxies POST request without caching.
func (p *Proxy) Post(ctx context.Context, clusterName, path string, body io.Reader, result interface{}) error {
	client, ok := p.Registry.GetClient(clusterName)
	if !ok {
		return fmt.Errorf("cluster %s not found", clusterName)
	}

	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	return client.Post(ctx, path, body, result)
}

// Delete proxies DELETE request without caching.
func (p *Proxy) Delete(ctx context.Context, clusterName, path string) error {
	client, ok := p.Registry.GetClient(clusterName)
	if !ok {
		return fmt.Errorf("cluster %s not found", clusterName)
	}

	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	return client.Delete(ctx, path)
}

// RawGet returns raw JSON bytes for GET request (used by aggregator).
func (p *Proxy) RawGet(ctx context.Context, clusterName, path string) (json.RawMessage, error) {
	cacheKey := fmt.Sprintf("%s:%s", clusterName, path)
	if cached, ok := p.cache.Get(cacheKey); ok {
		return json.RawMessage(cached), nil
	}

	client, ok := p.Registry.GetClient(clusterName)
	if !ok {
		return nil, fmt.Errorf("cluster %s not found", clusterName)
	}

	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	var raw json.RawMessage
	if err := client.Get(ctx, path, &raw); err != nil {
		return nil, err
	}

	p.cache.Set(cacheKey, raw)
	return raw, nil
}