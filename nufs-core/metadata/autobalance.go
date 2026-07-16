package metadata

import (
	"context"
	"log/slog"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

// ============================================================
// Auto Rebalance Scheduler
// ============================================================
// Monitors cluster imbalance and automatically triggers rebalance
// when the imbalance ratio exceeds a configurable threshold.

// AutoBalancerConfig holds configuration for the auto rebalance scheduler.
type AutoBalancerConfig struct {
	// Threshold is the usage imbalance ratio that triggers rebalance.
	// E.g., 0.15 means rebalance when the most-loaded node is >15%
	// above the cluster average. Default: 0.15.
	Threshold float64

	// Interval is how often the balancer checks the cluster state.
	// Default: 5 minutes.
	Interval time.Duration

	// MaxConcurrentMigrations limits how many chunks can be in-flight
	// during a rebalance. Default: 10.
	MaxConcurrentMigrations int
}

// ApplyDefaults fills in zero-valued config fields with safe defaults.
func (c *AutoBalancerConfig) ApplyDefaults() {
	if c.Threshold <= 0 {
		c.Threshold = 0.15
	}
	if c.Interval <= 0 {
		c.Interval = 5 * time.Minute
	}
	if c.MaxConcurrentMigrations <= 0 {
		c.MaxConcurrentMigrations = 10
	}
}

// AutoBalancer periodically checks cluster balance and triggers
// chunk migration when nodes are unevenly loaded.
type AutoBalancer struct {
	cfg     AutoBalancerConfig
	planner RebalancePlanner
	exec    AutoBalanceExecutor

	mu      sync.Mutex
	running atomic.Bool
	stopCh  chan struct{}
	wg      sync.WaitGroup

	// Last analysis result (for monitoring)
	lastResult *RebalanceResult

	// Store interface for periodic analysis
	store AutoBalancerStore
}

// AutoBalancerStore is the interface the balancer needs to query cluster state.
type AutoBalancerStore interface {
	ListNodes(ctx context.Context) ([]NodeInfo, error)
}

type autoBalancerChunkStore interface {
	ChunksByNode(ctx context.Context, nodeID NodeID) ([]ChunkMeta, error)
}

// AutoBalanceExecutor executes concrete migration plans selected by the balancer.
type AutoBalanceExecutor interface {
	ExecutePlans(ctx context.Context, plans []MigrationPlan) error
}

// NewAutoBalancer creates an auto rebalance scheduler.
func NewAutoBalancer(cfg AutoBalancerConfig) *AutoBalancer {
	cfg.ApplyDefaults()
	return &AutoBalancer{
		cfg:     cfg,
		planner: RebalancePlanner{},
		stopCh:  make(chan struct{}),
	}
}

// SetStore configures the metadata store for periodic analysis.
func (ab *AutoBalancer) SetStore(store AutoBalancerStore) {
	ab.store = store
}

// SetExecutor configures the migration executor used when imbalance is detected.
func (ab *AutoBalancer) SetExecutor(exec AutoBalanceExecutor) {
	ab.exec = exec
}

// Analyze checks the current cluster state and returns a rebalance result.
// This is the core logic that can be called manually or by the scheduler.
func (ab *AutoBalancer) Analyze(nodes []NodeInfo) *RebalanceResult {
	return ab.planner.PlanRebalance(nodes, ab.cfg.Threshold)
}

// ShouldTrigger returns true if the cluster is imbalanced enough
// to warrant automatic rebalance.
func (ab *AutoBalancer) ShouldTrigger(nodes []NodeInfo) bool {
	result := ab.Analyze(nodes)
	return !result.Balanced
}

// Start begins the periodic rebalance check loop.
func (ab *AutoBalancer) Start() {
	if ab.running.Swap(true) {
		return
	}
	ab.wg.Add(1)
	go ab.loop()
	slog.Info("auto-balancer: started", "interval", ab.cfg.Interval, "threshold", ab.cfg.Threshold)
}

// Stop halts the auto balancer.
func (ab *AutoBalancer) Stop() {
	if !ab.running.Swap(false) {
		return
	}
	close(ab.stopCh)
	ab.wg.Wait()
	slog.Info("auto-balancer: stopped")
}

// LastResult returns the most recent analysis result.
func (ab *AutoBalancer) LastResult() *RebalanceResult {
	ab.mu.Lock()
	defer ab.mu.Unlock()
	return ab.lastResult
}

func (ab *AutoBalancer) loop() {
	defer ab.wg.Done()
	ticker := time.NewTicker(ab.cfg.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			ab.check()
		case <-ab.stopCh:
			return
		}
	}
}

func (ab *AutoBalancer) check() {
	if ab.store == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	nodes, err := ab.store.ListNodes(ctx)
	if err != nil {
		slog.Error("auto-balancer: list nodes failed", "error", err)
		return
	}

	result := ab.Analyze(nodes)

	ab.mu.Lock()
	ab.lastResult = result
	ab.mu.Unlock()

	if result.Balanced {
		slog.Debug("auto-balancer: cluster is balanced", "imbalance", result.Imbalance)
		return
	}

	slog.Info("auto-balancer: imbalance detected",
		"imbalance", result.Imbalance,
		"plans", len(result.Plans),
	)

	if ab.exec == nil {
		return
	}
	chunkStore, ok := ab.store.(autoBalancerChunkStore)
	if !ok {
		slog.Warn("auto-balancer: store cannot list chunks by node; skipping execution")
		return
	}
	concrete := ab.planConcreteMigrations(ctx, nodes, result, chunkStore)
	if len(concrete) == 0 {
		slog.Warn("auto-balancer: no concrete migration plans available")
		return
	}
	if len(concrete) > ab.cfg.MaxConcurrentMigrations {
		concrete = concrete[:ab.cfg.MaxConcurrentMigrations]
	}
	if err := ab.exec.ExecutePlans(ctx, concrete); err != nil {
		slog.Error("auto-balancer: execute plans failed", "error", err)
	}
}

func (ab *AutoBalancer) planConcreteMigrations(ctx context.Context, nodes []NodeInfo, result *RebalanceResult, store autoBalancerChunkStore) []MigrationPlan {
	nodeChunks := make(map[NodeID][]ChunkID)
	for _, plan := range result.Plans {
		if _, ok := nodeChunks[plan.SourceNode]; ok {
			continue
		}
		chunks, err := store.ChunksByNode(ctx, plan.SourceNode)
		if err != nil {
			slog.Error("auto-balancer: list chunks by node failed", "node_id", plan.SourceNode, "error", err)
			nodeChunks[plan.SourceNode] = nil
			continue
		}
		ids := make([]ChunkID, 0, len(chunks))
		for _, chunk := range chunks {
			ids = append(ids, chunk.ID)
		}
		nodeChunks[plan.SourceNode] = ids
	}
	concrete := ab.planner.PlanRebalanceWithChunks(nodes, nodeChunks, ab.cfg.Threshold)
	if concrete == nil || concrete.Balanced {
		return nil
	}
	plans := concrete.Plans[:0]
	for _, plan := range concrete.Plans {
		if plan.ChunkID != 0 {
			plans = append(plans, plan)
		}
	}
	return plans
}

// computeImbalance calculates the coefficient of variation of node usage.
// Returns 0 for a perfectly balanced cluster.
func computeImbalance(loads []NodeLoad) float64 {
	if len(loads) < 2 {
		return 0
	}

	var sum float64
	for _, l := range loads {
		sum += l.UsageRatio
	}
	mean := sum / float64(len(loads))

	if mean == 0 {
		return 0
	}

	var variance float64
	for _, l := range loads {
		diff := l.UsageRatio - mean
		variance += diff * diff
	}
	variance /= float64(len(loads))

	// Coefficient of variation (standard deviation / mean)
	return math.Sqrt(variance) / mean
}
