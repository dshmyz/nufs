// Package cluster provides cluster registry and client for proxying requests.
package cluster

import (
	"context"
	"log"
	"time"
)

// runHealthChecker probes all clusters every 30 seconds.
func (r *Registry) runHealthChecker() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-r.ctx.Done():
			return
		case <-ticker.C:
			r.checkAll()
		}
	}
}

// runDBSync periodically reloads from DB to pick up dynamic changes
// made by other admin-server instances.
func (r *Registry) runDBSync() {
	if r.store == nil {
		return // No store, nothing to sync
	}

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-r.ctx.Done():
			return
		case <-ticker.C:
			if err := r.reload(); err != nil {
				log.Printf("WARN: DB sync failed: %v", err)
			}
		}
	}
}

// checkAll probes health for all registered clusters.
func (r *Registry) checkAll() {
	r.mu.RLock()
	entries := make(map[string]*entry)
	for name, e := range r.entries {
		entries[name] = e
	}
	r.mu.RUnlock()

	for name, e := range entries {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		err := e.client.CheckHealth(ctx)
		cancel()

		if err != nil {
			r.SetHealth(name, HealthUnhealthy)
		} else {
			r.SetHealth(name, HealthHealthy)
		}
	}
}