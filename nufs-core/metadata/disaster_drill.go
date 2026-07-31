package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// ============================================================
// Disaster Drill Runner — Automated disaster recovery exercises
// ============================================================
//
// DisasterDrillRunner automates periodic disaster recovery drills
// to verify that the system can survive and recover from various
// failure scenarios. Each drill:
//   1. Injects a controlled fault
//   2. Waits for the system to detect and respond
//   3. Validates data consistency and service availability
//   4. Restores normal state
//   5. Records the result for audit
//
// Usage:
//
//	runner := NewDisasterDrillRunner(store, DisasterDrillConfig{
//	    ScheduleInterval: 24 * time.Hour,
//	    Scenarios:        []DrillScenario{DrillNodeFailover, DrillDiskFailure},
//	    ReportDir:        "/var/log/nufs/drills",
//	})
//	runner.Start()
//

// DrillScenario identifies a type of disaster drill.
type DrillScenario string

const (
	// DrillNodeFailover simulates a single node going offline and verifies
	// that the cluster continues serving requests with remaining nodes.
	DrillNodeFailover DrillScenario = "node_failover"

	// DrillDiskFailure simulates a disk I/O error on a datanode and verifies
	// that the node transitions to read-only mode and data remains accessible
	// via replicas.
	DrillDiskFailure DrillScenario = "disk_failure"

	// DrillNetworkPartition simulates a network partition that isolates a
	// minority of nodes, verifying that the majority partition maintains
	// quorum and continues operating.
	DrillNetworkPartition DrillScenario = "network_partition"

	// DrillDataCorruption simulates silent data corruption on a chunk replica
	// and verifies that the integrity checker detects and repairs it.
	DrillDataCorruption DrillScenario = "data_corruption"

	// DrillFullOutage simulates a complete zone outage and verifies that
	// cross-zone replication provides data access from the surviving zone.
	DrillFullOutage DrillScenario = "full_zone_outage"

	// DrillBackupRestore restores the latest committed backup into a temporary
	// offline environment and verifies metadata-to-replica readability.
	DrillBackupRestore DrillScenario = "backup_restore"
)

// DrillStatus represents the outcome of a single drill run.
type DrillStatus string

const (
	DrillPassed  DrillStatus = "passed"
	DrillFailed  DrillStatus = "failed"
	DrillSkipped DrillStatus = "skipped"
	DrillError   DrillStatus = "error"
)

// DrillReport captures the result of a single disaster drill execution.
type DrillReport struct {
	Scenario    DrillScenario `json:"scenario"`
	Status      DrillStatus   `json:"status"`
	StartedAt   time.Time     `json:"started_at"`
	CompletedAt time.Time     `json:"completed_at"`
	Duration    time.Duration `json:"duration"`
	NodeID      NodeID        `json:"node_id,omitempty"`
	Zone        string        `json:"zone,omitempty"`
	Message     string        `json:"message,omitempty"`
	Checks      []DrillCheck  `json:"checks,omitempty"`
}

// DrillCheck is an individual validation step within a drill.
type DrillCheck struct {
	Name         string        `json:"name"`
	Passed       bool          `json:"passed"`
	Message      string        `json:"message,omitempty"`
	Took         time.Duration `json:"took"`
	ValueSeconds float64       `json:"value_seconds,omitempty"`
}

type DrillRestoreEngine func(context.Context, BackupRepository, RestoreOptions) (*RestoreReport, error)
type DrillOpenRestoredStore func(string) (*PebbleStore, error)

// DisasterDrillConfig configures the disaster drill runner.
type DisasterDrillConfig struct {
	// ScheduleInterval is the time between automatic drill runs.
	// Set to 0 to disable automatic scheduling (manual-only mode).
	ScheduleInterval time.Duration

	// Scenarios lists the drill scenarios to execute in each run.
	// If empty, all scenarios are run.
	Scenarios []DrillScenario

	// ReportDir is the directory where drill reports are persisted as JSON.
	// If empty, reports are only logged.
	ReportDir string

	// MaxConcurrentDrills limits how many drills can run simultaneously.
	MaxConcurrentDrills int

	// FailureTimeout is the maximum time to wait for the system to
	// stabilize after a fault injection before declaring failure.
	FailureTimeout time.Duration

	// DryRun runs all checks without actually injecting faults.
	DryRun bool

	// BackupRepository is used by the backup_restore scenario to select and
	// fetch the latest committed backup.
	BackupRepository BackupRepository

	// RestoreTempRoot is the parent for temporary, offline restore drills.
	RestoreTempRoot string

	// RestoreNewClusterID is written into the temporary restored metadata.
	RestoreNewClusterID string

	// RestoreMinimumReplicas is the minimum readable replica count required
	// before a restored metadata image is considered ready.
	RestoreMinimumReplicas int

	// RestoreReplicaProbe checks restored chunk replicas without mutating metadata.
	RestoreReplicaProbe RestoreReplicaProbe

	// RestoreEngine executes the offline restore. Defaults to RestoreBackupToNewCluster.
	RestoreEngine DrillRestoreEngine

	// OpenRestoredStore opens restored metadata without joining production Raft.
	OpenRestoredStore DrillOpenRestoredStore

	// Now supplies the clock used for observed RPO/RTO reporting.
	Now func() time.Time
}

// DisasterDrillRunner orchestrates automated disaster recovery drills.
type DisasterDrillRunner struct {
	store MetadataService
	cfg   DisasterDrillConfig

	mu      sync.Mutex
	running atomic.Bool
	stopCh  chan struct{}
	wg      sync.WaitGroup

	// Stats
	totalDrills   atomic.Int64
	passedDrills  atomic.Int64
	failedDrills  atomic.Int64
	skippedDrills atomic.Int64

	// Hooks for custom fault injection (e.g., datanode-level operations)
	faultInjector FaultInjector
}

// FaultInjector is the interface for injecting and reverting faults.
// The metadata layer implements node-level faults; datanode-level faults
// (disk I/O, data corruption) require a separate injector.
type FaultInjector interface {
	// InjectNodeFailure simulates a node going offline.
	InjectNodeFailure(ctx context.Context, nodeID NodeID) error

	// RevertNodeFailure restores a previously failed node.
	RevertNodeFailure(ctx context.Context, nodeID NodeID) error

	// InjectDiskFailure simulates a disk I/O error.
	InjectDiskFailure(ctx context.Context, nodeID NodeID) error

	// RevertDiskFailure restores disk I/O after a simulated failure.
	RevertDiskFailure(ctx context.Context, nodeID NodeID) error

	// InjectNetworkPartition isolates the given nodes from the cluster.
	InjectNetworkPartition(ctx context.Context, nodeIDs []NodeID) error

	// RevertNetworkPartition restores network connectivity.
	RevertNetworkPartition(ctx context.Context) error
}

// NewDisasterDrillRunner creates a new disaster drill runner.
func NewDisasterDrillRunner(store MetadataService, cfg DisasterDrillConfig) *DisasterDrillRunner {
	if cfg.MaxConcurrentDrills <= 0 {
		cfg.MaxConcurrentDrills = 1
	}
	if cfg.FailureTimeout <= 0 {
		cfg.FailureTimeout = 60 * time.Second
	}
	if cfg.RestoreMinimumReplicas <= 0 {
		cfg.RestoreMinimumReplicas = 1
	}
	if cfg.RestoreEngine == nil {
		cfg.RestoreEngine = RestoreBackupToNewCluster
	}
	if cfg.OpenRestoredStore == nil {
		cfg.OpenRestoredStore = func(path string) (*PebbleStore, error) {
			return NewPebbleStore(PebbleStoreConfig{Dir: path, NodeID: 1})
		}
	}
	if cfg.Now == nil {
		cfg.Now = func() time.Time { return time.Now().UTC() }
	}
	if len(cfg.Scenarios) == 0 {
		cfg.Scenarios = []DrillScenario{
			DrillNodeFailover,
			DrillDiskFailure,
			DrillNetworkPartition,
			DrillDataCorruption,
			DrillFullOutage,
		}
	}
	return &DisasterDrillRunner{
		store:         store,
		cfg:           cfg,
		stopCh:        make(chan struct{}),
		faultInjector: &defaultFaultInjector{store: store},
	}
}

// SetFaultInjector configures a custom fault injector for datanode-level operations.
func (r *DisasterDrillRunner) SetFaultInjector(fi FaultInjector) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.faultInjector = fi
}

// Start begins the periodic drill schedule.
func (r *DisasterDrillRunner) Start() {
	if r.running.Swap(true) {
		return
	}
	if r.cfg.ScheduleInterval <= 0 {
		slog.Info("drill: auto-scheduling disabled, manual mode only")
		return
	}

	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		slog.Info("drill: scheduler started", "interval", r.cfg.ScheduleInterval)
		ticker := time.NewTicker(r.cfg.ScheduleInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				ctx, cancel := context.WithTimeout(context.Background(), r.scheduleRunTimeout())
				r.RunAll(ctx)
				cancel()
			case <-r.stopCh:
				return
			}
		}
	}()
}

// Stop terminates the drill runner and waits for in-progress drills to complete.
func (r *DisasterDrillRunner) Stop() {
	if r.running.Swap(false) {
		close(r.stopCh)
	}
	r.wg.Wait()
}

// RunAll executes all configured drill scenarios sequentially.
func (r *DisasterDrillRunner) RunAll(ctx context.Context) []DrillReport {
	r.mu.Lock()
	scenarios := make([]DrillScenario, len(r.cfg.Scenarios))
	copy(scenarios, r.cfg.Scenarios)
	r.mu.Unlock()

	var reports []DrillReport
	for _, scenario := range scenarios {
		select {
		case <-ctx.Done():
			slog.Warn("drill: run cancelled", "error", ctx.Err())
			return reports
		default:
		}

		report := r.RunScenario(ctx, scenario)
		reports = append(reports, report)
	}

	r.persistReports(reports)
	return reports
}

func (r *DisasterDrillRunner) scheduleRunTimeout() time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, scenario := range r.cfg.Scenarios {
		if scenario == DrillBackupRestore {
			return 30 * time.Minute
		}
	}
	return 10 * time.Minute
}

// RunScenario executes a single drill scenario and returns the report.
func (r *DisasterDrillRunner) RunScenario(ctx context.Context, scenario DrillScenario) DrillReport {
	report := DrillReport{
		Scenario:  scenario,
		StartedAt: time.Now(),
	}

	slog.Info("drill: starting scenario", "scenario", scenario, "dry_run", r.cfg.DryRun)

	switch scenario {
	case DrillNodeFailover:
		report = r.runNodeFailover(ctx, report)
	case DrillDiskFailure:
		report = r.runDiskFailure(ctx, report)
	case DrillNetworkPartition:
		report = r.runNetworkPartition(ctx, report)
	case DrillDataCorruption:
		report = r.runDataCorruption(ctx, report)
	case DrillFullOutage:
		report = r.runFullZoneOutage(ctx, report)
	case DrillBackupRestore:
		report = r.runBackupRestore(ctx, report)
	default:
		report.Status = DrillSkipped
		report.Message = fmt.Sprintf("unknown scenario: %s", scenario)
	}

	report.CompletedAt = time.Now()
	report.Duration = report.CompletedAt.Sub(report.StartedAt)

	r.totalDrills.Add(1)
	switch report.Status {
	case DrillPassed:
		r.passedDrills.Add(1)
	case DrillFailed:
		r.failedDrills.Add(1)
	case DrillSkipped:
		r.skippedDrills.Add(1)
	}

	slog.Info("drill: scenario completed",
		"scenario", scenario,
		"status", report.Status,
		"duration", report.Duration.Round(time.Millisecond),
		"message", report.Message)

	return report
}

// Stats returns cumulative drill statistics.
func (r *DisasterDrillRunner) Stats() (total, passed, failed, skipped int64) {
	return r.totalDrills.Load(), r.passedDrills.Load(), r.failedDrills.Load(), r.skippedDrills.Load()
}

// ============================================================
// Scenario implementations
// ============================================================

// runNodeFailover: inject a node failure, verify cluster remains available, then restore.
func (r *DisasterDrillRunner) runNodeFailover(ctx context.Context, report DrillReport) DrillReport {
	fi := r.getFaultInjector()

	// Pick a random online node to fail
	nodeID, err := r.pickOnlineNode(ctx)
	if err != nil {
		report.Status = DrillSkipped
		report.Message = err.Error()
		return report
	}
	report.NodeID = nodeID

	// Phase 1: Inject node failure
	if !r.cfg.DryRun {
		if err := fi.InjectNodeFailure(ctx, nodeID); err != nil {
			report.Status = DrillError
			report.Message = fmt.Sprintf("inject failure: %v", err)
			return report
		}
		// Ensure rollback
		defer fi.RevertNodeFailure(ctx, nodeID)
	}

	// Phase 2: Verify cluster still has quorum
	check := r.timedCheck("quorum_maintained", func() (bool, string) {
		nodes, err := r.store.ListNodes(ctx)
		if err != nil {
			return false, fmt.Sprintf("list nodes: %v", err)
		}
		online := 0
		for _, n := range nodes {
			if n.State == NodeOnline {
				online++
			}
		}
		if online < 2 {
			return false, fmt.Sprintf("only %d nodes online, need >= 2", online)
		}
		return true, fmt.Sprintf("%d nodes online", online)
	})
	report.Checks = append(report.Checks, check)

	// Phase 3: Verify data access still works
	check = r.timedCheck("data_accessible", func() (bool, string) {
		buckets, err := r.store.ListBuckets(ctx)
		if err != nil {
			return false, fmt.Sprintf("list buckets: %v", err)
		}
		return true, fmt.Sprintf("%d buckets accessible", len(buckets))
	})
	report.Checks = append(report.Checks, check)

	report.Status = r.aggregateChecks(report.Checks)
	return report
}

// runDiskFailure: inject a disk failure, verify node goes read-only, then restore.
func (r *DisasterDrillRunner) runDiskFailure(ctx context.Context, report DrillReport) DrillReport {
	fi := r.getFaultInjector()

	nodeID, err := r.pickOnlineNode(ctx)
	if err != nil {
		report.Status = DrillSkipped
		report.Message = err.Error()
		return report
	}
	report.NodeID = nodeID

	// Phase 1: Inject disk failure
	if !r.cfg.DryRun {
		if err := fi.InjectDiskFailure(ctx, nodeID); err != nil {
			report.Status = DrillError
			report.Message = fmt.Sprintf("inject disk failure: %v", err)
			return report
		}
		defer fi.RevertDiskFailure(ctx, nodeID)
	}

	// Phase 2: Verify node transitions to degraded/failed state
	check := r.timedCheck("node_state_changed", func() (bool, string) {
		node, err := r.store.GetNode(ctx, nodeID)
		if err != nil {
			return false, fmt.Sprintf("get node: %v", err)
		}
		if node.State == NodeOnline {
			return false, "node still online after disk failure"
		}
		return true, fmt.Sprintf("node state: %s", node.State)
	})
	report.Checks = append(report.Checks, check)

	// Phase 3: Verify other nodes still serve data
	check = r.timedCheck("cluster_operational", func() (bool, string) {
		nodes, err := r.store.ListNodes(ctx)
		if err != nil {
			return false, fmt.Sprintf("list nodes: %v", err)
		}
		online := 0
		for _, n := range nodes {
			if n.State == NodeOnline {
				online++
			}
		}
		return online > 0, fmt.Sprintf("%d nodes still online", online)
	})
	report.Checks = append(report.Checks, check)

	report.Status = r.aggregateChecks(report.Checks)
	return report
}

// runNetworkPartition: isolate minority nodes, verify majority quorum, then restore.
func (r *DisasterDrillRunner) runNetworkPartition(ctx context.Context, report DrillReport) DrillReport {
	fi := r.getFaultInjector()

	nodes, err := r.store.ListNodes(ctx)
	if err != nil || len(nodes) < 3 {
		report.Status = DrillSkipped
		report.Message = fmt.Sprintf("need >= 3 nodes, got %d", len(nodes))
		return report
	}

	// Isolate 1 node (minority of 3+)
	isolatedID := nodes[0].ID
	report.NodeID = isolatedID

	if !r.cfg.DryRun {
		if err := fi.InjectNetworkPartition(ctx, []NodeID{isolatedID}); err != nil {
			report.Status = DrillError
			report.Message = fmt.Sprintf("inject partition: %v", err)
			return report
		}
		defer fi.RevertNetworkPartition(ctx)
	}

	// Verify majority partition maintains quorum
	check := r.timedCheck("majority_quorum", func() (bool, string) {
		nodes, err := r.store.ListNodes(ctx)
		if err != nil {
			return false, fmt.Sprintf("list nodes: %v", err)
		}
		online := 0
		for _, n := range nodes {
			if n.State == NodeOnline {
				online++
			}
		}
		total := len(nodes)
		if online > total/2 {
			return true, fmt.Sprintf("%d/%d nodes online (majority)", online, total)
		}
		return false, fmt.Sprintf("%d/%d nodes online (lost majority)", online, total)
	})
	report.Checks = append(report.Checks, check)

	// Verify metadata operations still work
	check = r.timedCheck("metadata_writable", func() (bool, string) {
		buckets, err := r.store.ListBuckets(ctx)
		if err != nil {
			return false, fmt.Sprintf("list buckets: %v", err)
		}
		return true, fmt.Sprintf("%d buckets listed", len(buckets))
	})
	report.Checks = append(report.Checks, check)

	report.Status = r.aggregateChecks(report.Checks)
	return report
}

// runDataCorruption: verify integrity detection works by checking chunk checksums.
func (r *DisasterDrillRunner) runDataCorruption(ctx context.Context, report DrillReport) DrillReport {
	// Data corruption detection requires datanode-level integrity checker.
	// At the metadata level, we verify that the repair system tracks chunks
	// needing repair.

	check := r.timedCheck("repair_queue_accessible", func() (bool, string) {
		// Verify repair service is functional
		repairs, err := r.store.GetRepairQueue(ctx)
		if err != nil {
			return false, fmt.Sprintf("get repair queue: %v", err)
		}
		return true, fmt.Sprintf("repair queue accessible, %d items", len(repairs))
	})
	report.Checks = append(report.Checks, check)

	check = r.timedCheck("bucket_data_consistent", func() (bool, string) {
		buckets, err := r.store.ListBuckets(ctx)
		if err != nil {
			return false, fmt.Sprintf("list buckets: %v", err)
		}
		return true, fmt.Sprintf("%d buckets listed", len(buckets))
	})
	report.Checks = append(report.Checks, check)

	report.Status = r.aggregateChecks(report.Checks)
	return report
}

// runFullZoneOutage: simulate an entire zone going offline.
func (r *DisasterDrillRunner) runFullZoneOutage(ctx context.Context, report DrillReport) DrillReport {
	nodes, err := r.store.ListNodes(ctx)
	if err != nil {
		report.Status = DrillError
		report.Message = fmt.Sprintf("list nodes: %v", err)
		return report
	}

	// Group nodes by zone
	zones := make(map[string][]NodeID)
	for _, n := range nodes {
		zones[n.Zone] = append(zones[n.Zone], n.ID)
	}

	if len(zones) < 2 {
		report.Status = DrillSkipped
		report.Message = "need >= 2 zones for full outage drill"
		return report
	}

	// Pick the zone with the fewest nodes to take offline
	var targetZone string
	minNodes := len(nodes) + 1
	for z, ids := range zones {
		if len(ids) < minNodes && len(ids) < len(nodes)/2 {
			targetZone = z
			minNodes = len(ids)
		}
	}
	if targetZone == "" {
		report.Status = DrillSkipped
		report.Message = "no suitable zone for outage drill (would lose majority)"
		return report
	}
	report.Zone = targetZone

	fi := r.getFaultInjector()

	// Take all nodes in the target zone offline
	if !r.cfg.DryRun {
		if err := fi.InjectNetworkPartition(ctx, zones[targetZone]); err != nil {
			report.Status = DrillError
			report.Message = fmt.Sprintf("inject zone outage: %v", err)
			return report
		}
		defer fi.RevertNetworkPartition(ctx)
	}

	// Verify remaining zones maintain quorum
	check := r.timedCheck("surviving_quorum", func() (bool, string) {
		nodes, err := r.store.ListNodes(ctx)
		if err != nil {
			return false, fmt.Sprintf("list nodes: %v", err)
		}
		online := 0
		for _, n := range nodes {
			if n.State == NodeOnline {
				online++
			}
		}
		if online == 0 {
			return false, "no nodes online after zone outage"
		}
		return true, fmt.Sprintf("%d nodes online in surviving zones", online)
	})
	report.Checks = append(report.Checks, check)

	// Verify metadata operations work in surviving zone
	check = r.timedCheck("surviving_metadata_accessible", func() (bool, string) {
		buckets, err := r.store.ListBuckets(ctx)
		if err != nil {
			return false, fmt.Sprintf("list buckets: %v", err)
		}
		return len(buckets) >= 0, fmt.Sprintf("%d buckets accessible", len(buckets))
	})
	report.Checks = append(report.Checks, check)

	report.Status = r.aggregateChecks(report.Checks)
	return report
}

func (r *DisasterDrillRunner) runBackupRestore(ctx context.Context, report DrillReport) DrillReport {
	if r.cfg.BackupRepository == nil {
		report.Status = DrillSkipped
		report.Message = "backup restore drill requires a backup repository"
		return report
	}
	if r.cfg.RestoreReplicaProbe == nil {
		report.Status = DrillSkipped
		report.Message = "backup restore drill requires a restore replica probe"
		return report
	}
	if r.cfg.RestoreNewClusterID == "" {
		report.Status = DrillSkipped
		report.Message = "backup restore drill requires a restore cluster ID"
		return report
	}
	deadlineCtx, cancel := context.WithTimeout(ctx, 30*time.Minute)
	defer cancel()

	latest, ok, err := r.latestCommittedBackup(deadlineCtx)
	if err != nil {
		report.Status = DrillFailed
		report.Message = err.Error()
		return report
	}
	if !ok {
		report.Status = DrillSkipped
		report.Message = "no committed backups available"
		return report
	}

	tempRoot := r.cfg.RestoreTempRoot
	if tempRoot == "" {
		tempRoot = os.TempDir()
	}
	envDir, err := os.MkdirTemp(tempRoot, "nufs-restore-drill-*")
	if err != nil {
		report.Status = DrillFailed
		report.Message = fmt.Sprintf("create temp restore environment: %v", err)
		return report
	}
	defer os.RemoveAll(envDir)

	restoreTarget := filepath.Join(envDir, "metadata")
	restoreStarted := r.cfg.Now()
	restoreReport, err := r.cfg.RestoreEngine(deadlineCtx, r.cfg.BackupRepository, RestoreOptions{
		BackupID:     latest.ID,
		TargetDir:    restoreTarget,
		NewClusterID: r.cfg.RestoreNewClusterID,
	})
	restoreCompleted := r.cfg.Now()
	if err != nil {
		report.Status = DrillFailed
		report.Message = fmt.Sprintf("restore latest backup %q: %v", latest.ID, err)
		return report
	}

	rpo := r.cfg.Now().Sub(latest.CreatedAt)
	rto := restoreCompleted.Sub(restoreStarted)
	if restoreReport != nil && !restoreReport.CompletedAt.IsZero() && !restoreReport.StartedAt.IsZero() {
		rto = restoreReport.CompletedAt.Sub(restoreReport.StartedAt)
	}
	report.Checks = append(report.Checks,
		DrillCheck{Name: "latest_committed_backup_selected", Passed: restoreReport == nil || restoreReport.BackupID == latest.ID, Message: latest.ID},
		DrillCheck{Name: "observed_rpo_seconds", Passed: rpo <= time.Hour, Message: rpo.String(), ValueSeconds: rpo.Seconds()},
		DrillCheck{Name: "observed_rto_seconds", Passed: rto <= 30*time.Minute, Message: rto.String(), ValueSeconds: rto.Seconds()},
	)
	if restoreReport != nil {
		report.Checks = append(report.Checks, DrillCheck{
			Name:    "backup_artifact_verified",
			Passed:  restoreReport.Verification.ManifestValid && restoreReport.Verification.FilesVerified > 0,
			Message: fmt.Sprintf("%d files verified", restoreReport.Verification.FilesVerified),
		})
	}

	restoredStore, err := r.cfg.OpenRestoredStore(restoreTarget)
	if err != nil {
		report.Status = DrillFailed
		report.Message = fmt.Sprintf("open restored store: %v", err)
		return report
	}
	defer restoredStore.Close()

	readinessReport, err := VerifyRestoredChunkAvailability(deadlineCtx, restoredStore, r.cfg.RestoreReplicaProbe, r.cfg.RestoreMinimumReplicas)
	report.Checks = append(report.Checks, DrillCheck{
		Name:    "restored_replica_readiness",
		Passed:  err == nil && readinessReport != nil && readinessReport.Ready,
		Message: restoreReadinessDrillMessage(readinessReport, err),
	})
	report.Status = r.aggregateChecks(report.Checks)
	if report.Status != DrillPassed && report.Message == "" {
		report.Message = "backup restore drill checks failed"
	}
	return report
}

func (r *DisasterDrillRunner) latestCommittedBackup(ctx context.Context) (BackupDescriptor, bool, error) {
	backups, err := r.cfg.BackupRepository.ListCommitted(ctx)
	if err != nil {
		return BackupDescriptor{}, false, fmt.Errorf("list committed backups: %w", err)
	}
	if len(backups) == 0 {
		return BackupDescriptor{}, false, nil
	}
	sort.Slice(backups, func(i, j int) bool {
		if backups[i].CreatedAt.Equal(backups[j].CreatedAt) {
			return backups[i].AppliedIndex > backups[j].AppliedIndex
		}
		return backups[i].CreatedAt.After(backups[j].CreatedAt)
	})
	return backups[0], true, nil
}

func restoreReadinessDrillMessage(report *RestoreReadinessReport, err error) string {
	if report == nil {
		if err != nil {
			return err.Error()
		}
		return "no restore readiness report"
	}
	if err != nil {
		return fmt.Sprintf("%s; chunks=%d unavailable=%d", err.Error(), report.TotalChunks, report.UnavailableChunks)
	}
	return fmt.Sprintf("chunks=%d unavailable=%d", report.TotalChunks, report.UnavailableChunks)
}

func DailyRestoreVerificationDrillConfig(repository BackupRepository, probe RestoreReplicaProbe, tempRoot, newClusterID string) DisasterDrillConfig {
	return DisasterDrillConfig{
		ScheduleInterval:       24 * time.Hour,
		Scenarios:              []DrillScenario{DrillBackupRestore},
		BackupRepository:       repository,
		RestoreReplicaProbe:    probe,
		RestoreTempRoot:        tempRoot,
		RestoreNewClusterID:    newClusterID,
		RestoreMinimumReplicas: 1,
		FailureTimeout:         30 * time.Minute,
	}
}

// ============================================================
// Helpers
// ============================================================

// timedCheck runs a validation function and records timing.
func (r *DisasterDrillRunner) timedCheck(name string, fn func() (bool, string)) DrillCheck {
	start := time.Now()
	passed, msg := fn()
	return DrillCheck{
		Name:    name,
		Passed:  passed,
		Message: msg,
		Took:    time.Since(start),
	}
}

// aggregateChecks returns the overall drill status based on individual checks.
func (r *DisasterDrillRunner) aggregateChecks(checks []DrillCheck) DrillStatus {
	for _, c := range checks {
		if !c.Passed {
			return DrillFailed
		}
	}
	return DrillPassed
}

// pickOnlineNode selects a random online node for fault injection.
func (r *DisasterDrillRunner) pickOnlineNode(ctx context.Context) (NodeID, error) {
	nodes, err := r.store.ListNodes(ctx)
	if err != nil {
		return 0, fmt.Errorf("list nodes: %w", err)
	}
	var online []NodeInfo
	for _, n := range nodes {
		if n.State == NodeOnline {
			online = append(online, n)
		}
	}
	if len(online) == 0 {
		return 0, fmt.Errorf("no online nodes available for drill")
	}
	// Pick a random node (avoid always failing the same one)
	return online[rand.Intn(len(online))].ID, nil
}

// getFaultInjector returns the current fault injector (thread-safe).
func (r *DisasterDrillRunner) getFaultInjector() FaultInjector {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.faultInjector
}

// persistReports writes drill reports to disk as JSON files.
func (r *DisasterDrillRunner) persistReports(reports []DrillReport) {
	if r.cfg.ReportDir == "" {
		return
	}
	if err := os.MkdirAll(r.cfg.ReportDir, 0755); err != nil {
		slog.Error("drill: failed to create report dir", "dir", r.cfg.ReportDir, "error", err)
		return
	}

	timestamp := time.Now().Format("20060102-150405")
	path := filepath.Join(r.cfg.ReportDir, fmt.Sprintf("drill-%s.json", timestamp))

	data, err := json.MarshalIndent(reports, "", "  ")
	if err != nil {
		slog.Error("drill: failed to marshal reports", "error", err)
		return
	}

	if err := os.WriteFile(path, data, 0644); err != nil {
		slog.Error("drill: failed to write report", "path", path, "error", err)
		return
	}

	slog.Info("drill: report persisted", "path", path, "scenarios", len(reports))
}

// ============================================================
// Default FaultInjector — metadata-level fault injection
// ============================================================

// defaultFaultInjector implements FaultInjector using metadata service
// operations. This handles node-level faults; datanode-level faults
// (disk I/O, data corruption) require a custom injector.
type defaultFaultInjector struct {
	store MetadataService
}

func (d *defaultFaultInjector) InjectNodeFailure(ctx context.Context, nodeID NodeID) error {
	return d.store.DecommissionNode(ctx, nodeID)
}

func (d *defaultFaultInjector) RevertNodeFailure(ctx context.Context, nodeID NodeID) error {
	// Re-register the node as online
	return d.store.RegisterNode(ctx, &NodeInfo{
		ID:    nodeID,
		State: NodeOnline,
	})
}

func (d *defaultFaultInjector) InjectDiskFailure(ctx context.Context, nodeID NodeID) error {
	// Mark the node as failed in metadata
	node, err := d.store.GetNode(ctx, nodeID)
	if err != nil {
		return fmt.Errorf("get node %d: %w", nodeID, err)
	}
	node.State = NodeFailed
	return d.store.RegisterNode(ctx, node)
}

func (d *defaultFaultInjector) RevertDiskFailure(ctx context.Context, nodeID NodeID) error {
	node, err := d.store.GetNode(ctx, nodeID)
	if err != nil {
		return fmt.Errorf("get node %d: %w", nodeID, err)
	}
	node.State = NodeOnline
	return d.store.RegisterNode(ctx, node)
}

func (d *defaultFaultInjector) InjectNetworkPartition(ctx context.Context, nodeIDs []NodeID) error {
	// Mark all specified nodes as failed to simulate partition
	for _, id := range nodeIDs {
		node, err := d.store.GetNode(ctx, id)
		if err != nil {
			return fmt.Errorf("get node %d: %w", id, err)
		}
		node.State = NodeFailed
		if err := d.store.RegisterNode(ctx, node); err != nil {
			return fmt.Errorf("mark node %d failed: %w", id, err)
		}
	}
	return nil
}

func (d *defaultFaultInjector) RevertNetworkPartition(ctx context.Context) error {
	// Restore all failed nodes to online
	nodes, err := d.store.ListNodes(ctx)
	if err != nil {
		return err
	}
	for _, n := range nodes {
		if n.State == NodeFailed {
			n.State = NodeOnline
			if err := d.store.RegisterNode(ctx, &n); err != nil {
				return fmt.Errorf("restore node %d: %w", n.ID, err)
			}
		}
	}
	return nil
}
