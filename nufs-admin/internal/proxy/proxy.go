// Package proxy provides request proxying to NUFS clusters.
package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/dshmyz/nufs/nufs-admin/internal/cache"
	"github.com/dshmyz/nufs/nufs-admin/internal/cluster"
)

// Proxy handles request proxying to a single cluster.
type Proxy struct {
	Registry *cluster.Registry
	cache    *cache.Cache
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
	cacheKey := p.cacheKey(clusterName, path)
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

// GetUncached proxies a GET request without reading or populating the cache.
func (p *Proxy) GetUncached(ctx context.Context, clusterName, path string, result interface{}) error {
	client, ok := p.Registry.GetClient(clusterName)
	if !ok {
		return fmt.Errorf("%w: %s", cluster.ErrClusterNotFound, clusterName)
	}

	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	return client.Get(ctx, path, result)
}

// Post proxies POST request without caching.
func (p *Proxy) Post(ctx context.Context, clusterName, path string, body io.Reader, result interface{}) error {
	client, ok := p.Registry.GetClient(clusterName)
	if !ok {
		return fmt.Errorf("%w: %s", cluster.ErrClusterNotFound, clusterName)
	}

	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	return client.Post(ctx, path, body, result)
}

// Put proxies PUT request and invalidates a cached representation on success.
func (p *Proxy) Put(ctx context.Context, clusterName, path string, body io.Reader, result interface{}) error {
	client, ok := p.Registry.GetClient(clusterName)
	if !ok {
		return fmt.Errorf("%w: %s", cluster.ErrClusterNotFound, clusterName)
	}

	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	if err := client.Put(ctx, path, body, result); err != nil {
		return err
	}
	p.invalidate(clusterName, path)
	return nil
}

// Delete proxies DELETE request and invalidates a cached representation on success.
func (p *Proxy) Delete(ctx context.Context, clusterName, path string) error {
	client, ok := p.Registry.GetClient(clusterName)
	if !ok {
		return fmt.Errorf("%w: %s", cluster.ErrClusterNotFound, clusterName)
	}

	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	if err := client.Delete(ctx, path); err != nil {
		return err
	}
	p.invalidate(clusterName, path)
	return nil
}

// RawGet returns raw JSON bytes for GET request (used by aggregator).
func (p *Proxy) RawGet(ctx context.Context, clusterName, path string) (json.RawMessage, error) {
	cacheKey := p.cacheKey(clusterName, path)
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

func (p *Proxy) cacheKey(clusterName, path string) string {
	return fmt.Sprintf("%s:%s", clusterName, path)
}

func (p *Proxy) invalidate(clusterName, path string) {
	p.cache.Delete(p.cacheKey(clusterName, path))
}
