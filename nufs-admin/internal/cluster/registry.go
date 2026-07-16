// Package cluster provides cluster registry and client for proxying requests.
package cluster

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/your-org/nufs-admin/internal/config"
	"github.com/your-org/nufs-admin/internal/store"
)

// HealthStatus represents cluster health state.
type HealthStatus string

const (
	HealthHealthy   HealthStatus = "healthy"
	HealthUnhealthy HealthStatus = "unhealthy"
	HealthUnknown   HealthStatus = "unknown"
)

// ClusterInfo combines configuration with runtime health status.
type ClusterInfo struct {
	Name        string       `json:"name"`
	Region      string       `json:"region"`
	Description string       `json:"description"`
	Health      HealthStatus `json:"health"`
	LastCheck   time.Time    `json:"lastCheck"`
	Source      string       `json:"source"` // "static" or "dynamic"
}

// entry combines a client with metadata and health.
type entry struct {
	client      *Client
	region      string
	description string
	source      string
	health      HealthStatus
	lastCheck   time.Time
}

// Registry maintains cluster name → client mapping with health status.
// Supports Hybrid mode: static (YAML) + dynamic (MySQL).
type Registry struct {
	mu      sync.RWMutex
	entries map[string]*entry
	ctx     context.Context
	cancel  context.CancelFunc

	store     *store.Store
	configMgr *config.Manager
}

// NewRegistry creates a registry from configuration and optional store.
func NewRegistry(cfgMgr *config.Manager, st *store.Store) (*Registry, error) {
	ctx, cancel := context.WithCancel(context.Background())

	r := &Registry{
		entries:   make(map[string]*entry),
		ctx:       ctx,
		cancel:    cancel,
		store:     st,
		configMgr: cfgMgr,
	}

	// Sync static clusters from YAML to DB, then load all from DB
	if err := r.reload(); err != nil {
		cancel()
		return nil, err
	}

	// Start background goroutines
	go r.runHealthChecker()
	go r.runDBSync() // Periodically reload from DB to pick up dynamic changes

	return r, nil
}

// reload syncs static clusters to DB and reloads all clusters.
func (r *Registry) reload() error {
	cfg := r.configMgr.Get()

	// If store exists, sync static clusters and load all from DB
	if r.store != nil {
		// Convert config clusters to static records
		var static []store.ClusterRecord
		for _, cc := range cfg.Clusters {
			static = append(static, store.ClusterRecord{
				ID:          cc.Name,
				Region:      cc.Region,
				MetadOpsURL: cc.MetadOpsURL,
				Description: cc.Description,
				Source:      store.SourceStatic,
			})
		}

		// Sync static to DB
		if err := r.store.SyncStatic(r.ctx, static); err != nil {
			log.Printf("WARN: sync static clusters to DB failed: %v", err)
			// Continue anyway, DB might be temporarily unavailable
		}

		// Load all clusters from DB
		records, err := r.store.ListAll(r.ctx)
		if err != nil {
			return err
		}

		// Build new entries, preserving health from old entries
		newEntries := make(map[string]*entry)
		for _, rec := range records {
			// Preserve health status from existing entry
			var health HealthStatus = HealthUnknown
			var lastCheck time.Time
			if old, ok := r.entries[rec.ID]; ok {
				health = old.health
				lastCheck = old.lastCheck
			}

			newEntries[rec.ID] = &entry{
				client:      NewClient(rec.ID, rec.MetadOpsURL),
				region:      rec.Region,
				description: rec.Description,
				source:      string(rec.Source),
				health:      health,
				lastCheck:   lastCheck,
			}
		}

		r.mu.Lock()
		r.entries = newEntries
		r.mu.Unlock()
		return nil
	}

	// No store, use config only
	r.mu.Lock()
	defer r.mu.Unlock()
	r.entries = make(map[string]*entry)
	for _, cc := range cfg.Clusters {
		r.entries[cc.Name] = &entry{
			client:      NewClient(cc.Name, cc.MetadOpsURL),
			region:      cc.Region,
			description: cc.Description,
			source:      string(store.SourceStatic),
			health:      HealthUnknown,
		}
	}
	return nil
}

// Reload triggers a manual reload (called on SIGHUP).
func (r *Registry) Reload() error {
	return r.reload()
}

// AddDynamic adds a new dynamic cluster at runtime.
func (r *Registry) AddDynamic(ctx context.Context, rec store.ClusterRecord, operator string) error {
	if r.store == nil {
		return ErrStoreNotConfigured
	}

	if err := r.store.Add(ctx, rec); err != nil {
		return err
	}

	// Log to audit
	_ = r.store.AddAuditLog(ctx, store.AuditLogEntry{
		ClusterID: rec.ID,
		Action:    store.AuditAdd,
		Operator:  operator,
		Detail:    "Added via admin UI",
	})

	// Reload to pick up the change
	return r.reload()
}

// RemoveDynamic removes a dynamic cluster at runtime.
func (r *Registry) RemoveDynamic(ctx context.Context, id, operator string) error {
	if r.store == nil {
		return ErrStoreNotConfigured
	}

	if err := r.store.Remove(ctx, id); err != nil {
		return err
	}

	// Log to audit
	_ = r.store.AddAuditLog(ctx, store.AuditLogEntry{
		ClusterID: id,
		Action:    store.AuditRemove,
		Operator:  operator,
		Detail:    "Removed via admin UI",
	})

	// Reload to pick up the change
	return r.reload()
}

// UpdateDynamic updates a dynamic cluster at runtime.
func (r *Registry) UpdateDynamic(ctx context.Context, rec store.ClusterRecord, operator string) error {
	if r.store == nil {
		return ErrStoreNotConfigured
	}

	if err := r.store.Update(ctx, rec); err != nil {
		return err
	}

	// Log to audit
	_ = r.store.AddAuditLog(ctx, store.AuditLogEntry{
		ClusterID: rec.ID,
		Action:    store.AuditUpdate,
		Operator:  operator,
		Detail:    "Updated via admin UI",
	})

	// Reload to pick up the change
	return r.reload()
}

// ListAuditLogs returns recent cluster change logs.
func (r *Registry) ListAuditLogs(ctx context.Context, limit, offset int) ([]store.AuditLogEntry, error) {
	if r.store == nil {
		return nil, ErrStoreNotConfigured
	}
	return r.store.ListAuditLogs(ctx, limit, offset)
}

// GetClient returns client for a specific cluster.
func (r *Registry) GetClient(name string) (*Client, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	e, ok := r.entries[name]
	if !ok {
		return nil, false
	}
	return e.client, true
}

// List returns all clusters with their health status.
func (r *Registry) List() []ClusterInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var list []ClusterInfo
	for name, e := range r.entries {
		list = append(list, ClusterInfo{
			Name:        name,
			Region:      e.region,
			Description: e.description,
			Health:      e.health,
			LastCheck:   e.lastCheck,
			Source:      e.source,
		})
	}
	return list
}

// SetHealth updates health status for a cluster.
func (r *Registry) SetHealth(name string, status HealthStatus) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e, ok := r.entries[name]; ok {
		e.health = status
	}
}

// Close stops background goroutines and cancels all in-flight requests.
func (r *Registry) Close() {
	r.cancel()
}