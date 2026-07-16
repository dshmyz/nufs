// Package proxy provides request proxying to NUFS clusters.
package proxy

import (
	"context"
	"encoding/json"
	"sync"

	"golang.org/x/sync/errgroup"
)

// AggregatedResult contains results from multiple clusters with partial failures.
type AggregatedResult struct {
	Results  map[string]json.RawMessage `json:"results"`
	Failures map[string]string          `json:"failures"`
}

// Aggregator fetches data from all clusters concurrently.
type Aggregator struct {
	proxy *Proxy
}

// NewAggregator creates an aggregator.
func NewAggregator(proxy *Proxy) *Aggregator {
	return &Aggregator{proxy: proxy}
}

// FetchAll concurrently fetches from all clusters with partial failure tolerance.
func (a *Aggregator) FetchAll(ctx context.Context, path string) *AggregatedResult {
	clusters := a.proxy.Registry.List()

	result := &AggregatedResult{
		Results:  make(map[string]json.RawMessage),
		Failures: make(map[string]string),
	}

	var mu sync.Mutex
	g, ctx := errgroup.WithContext(ctx)

	for _, ci := range clusters {
		name := ci.Name
		g.Go(func() error {
			raw, err := a.proxy.RawGet(ctx, name, path)
			if err != nil {
				mu.Lock()
				result.Failures[name] = err.Error()
				mu.Unlock()
				return nil // Don't fail the group, record as partial failure
			}

			mu.Lock()
			result.Results[name] = raw
			mu.Unlock()
			return nil
		})
	}

	g.Wait() // Ignore error, we handle partial failures in result
	return result
}