package datanode

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// FailoverConfig holds configuration for the FailoverManager.
type FailoverConfig struct {
	CheckInterval time.Duration // How often to check remote zone health
	FailureThreshold int        // Consecutive failures before triggering failover
	RecoveryTimeout time.Duration // Time to wait before recovering from standby
}

// DefaultFailoverConfig returns a default failover configuration.
func DefaultFailoverConfig() FailoverConfig {
	return FailoverConfig{
		CheckInterval:   30 * time.Second,
		FailureThreshold: 3,
		RecoveryTimeout: 5 * time.Minute,
	}
}

// ZoneHealth represents the health state of a remote zone.
type ZoneHealth int

const (
	ZoneHealthy ZoneHealth = iota
	ZoneDegraded
	ZoneFailed
)

// FailoverManager monitors remote zone health and triggers failover when needed.
// It is designed to work with external DNS switching systems.
type FailoverManager struct {
	localZone   string
	remoteZone  string
	meta        FailoverMeta
	cfg         FailoverConfig

	mu          sync.RWMutex
	health      ZoneHealth
	failCount   int
	lastFailure time.Time
	running     bool
	stopCh      chan struct{}
}

// FailoverMeta defines the interface for querying remote zone health.
type FailoverMeta interface {
	// CheckZoneHealth returns the health status of a remote zone.
	CheckZoneHealth(ctx context.Context, zone string) (ZoneHealth, error)
	// TriggerFailover notifies the system to fail over to the standby zone.
	TriggerFailover(ctx context.Context, fromZone, toZone string) error
}

// NewFailoverManager creates a new FailoverManager.
func NewFailoverManager(localZone, remoteZone string, meta FailoverMeta, cfg FailoverConfig) *FailoverManager {
	if cfg.CheckInterval == 0 {
		cfg = DefaultFailoverConfig()
	}
	return &FailoverManager{
		localZone:  localZone,
		remoteZone: remoteZone,
		meta:       meta,
		cfg:        cfg,
		health:     ZoneHealthy,
		stopCh:     make(chan struct{}),
	}
}

// Start begins monitoring remote zone health.
func (fm *FailoverManager) Start() {
	fm.mu.Lock()
	if fm.running {
		fm.mu.Unlock()
		return
	}
	fm.running = true
	fm.mu.Unlock()

	go fm.monitorLoop()
	slog.Info("failover: manager started",
		"localZone", fm.localZone,
		"remoteZone", fm.remoteZone,
		"checkInterval", fm.cfg.CheckInterval)
}

// Stop halts the failover manager.
func (fm *FailoverManager) Stop() {
	fm.mu.Lock()
	if !fm.running {
		fm.mu.Unlock()
		return
	}
	fm.running = false
	close(fm.stopCh)
	fm.mu.Unlock()

	slog.Info("failover: manager stopped")
}

// monitorLoop periodically checks remote zone health.
func (fm *FailoverManager) monitorLoop() {
	ticker := time.NewTicker(fm.cfg.CheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-fm.stopCh:
			return
		case <-ticker.C:
			fm.checkHealth()
		}
	}
}

// checkHealth performs a health check on the remote zone.
func (fm *FailoverManager) checkHealth() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	health, err := fm.meta.CheckZoneHealth(ctx, fm.remoteZone)
	if err != nil {
		slog.Warn("failover: health check failed",
			"zone", fm.remoteZone,
			"error", err)
		fm.onHealthCheckFailed()
		return
	}

	fm.mu.Lock()
	wasHealthy := fm.health == ZoneHealthy
	fm.health = health
	fm.failCount = 0 // Reset failure count on successful check
	fm.mu.Unlock()

	if wasHealthy && health != ZoneHealthy {
		slog.Warn("failover: remote zone became unhealthy",
			"zone", fm.remoteZone,
			"health", health)
	}

	if health == ZoneHealthy {
		fm.checkRecovery()
	}
}

// onHealthCheckFailed increments the failure counter and triggers failover if threshold reached.
func (fm *FailoverManager) onHealthCheckFailed() {
	fm.mu.Lock()
	fm.failCount++
	fc := fm.failCount
	fm.mu.Unlock()

	if fc >= fm.cfg.FailureThreshold {
		slog.Error("failover: failure threshold reached, triggering failover",
			"zone", fm.remoteZone,
			"failures", fc)
		fm.triggerFailover()
	}
}

// triggerFailover initiates failover to the standby zone.
func (fm *FailoverManager) triggerFailover() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := fm.meta.TriggerFailover(ctx, fm.localZone, fm.remoteZone); err != nil {
		slog.Error("failover: trigger failed",
			"from", fm.localZone,
			"to", fm.remoteZone,
			"error", err)
		return
	}

	fm.mu.Lock()
	fm.lastFailure = time.Now()
	fm.mu.Unlock()

	slog.Warn("failover: failover triggered successfully",
		"from", fm.localZone,
		"to", fm.remoteZone)
}

// checkRecovery checks if the remote zone has recovered and triggers failback.
func (fm *FailoverManager) checkRecovery() {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	if fm.lastFailure.IsZero() {
		return
	}

	if time.Since(fm.lastFailure) < fm.cfg.RecoveryTimeout {
		return
	}

	// Recovery timeout elapsed, trigger failback
	slog.Info("failover: recovery timeout elapsed, triggering failback",
		"from", fm.remoteZone,
		"to", fm.localZone)
}

// ZoneHealth returns the current health status of the remote zone.
func (fm *FailoverManager) ZoneHealth() ZoneHealth {
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	return fm.health
}
