package slo

// ============================================================
// SLO / SLI Definitions and Alerting Rule Templates for NUFS
// ============================================================
//
// This file defines the Service Level Objectives (SLOs), Service
// Level Indicators (SLIs), and alerting rules for the NUFS storage
// system. These definitions serve as the source of truth for
// production monitoring and can be translated into Prometheus
// alerting rules, Grafana dashboards, or PagerDuty configurations.
//
// Architecture:
//   - SLI: A quantitative measure of service behavior
//   - SLO: A target value or range for an SLI
//   - Alert: A rule that fires when an SLO is at risk
//
// Categories:
//   1. Availability — Can users access their data?
//   2. Durability — Is data correctly stored and retrievable?
//   3. Latency — How fast are operations?
//   4. Correctness — Are operations producing correct results?
//   5. Operational — Is the cluster healthy?

// ============================================================
// SLI Definitions
// ============================================================

// SLI represents a Service Level Indicator — a quantitative measure
// of some aspect of the service.
type SLI struct {
	// Name is the unique identifier for this SLI.
	Name string `json:"name"`

	// Category groups related SLIs (availability, durability, latency, etc).
	Category string `json:"category"`

	// Description explains what this indicator measures.
	Description string `json:"description"`

	// Metric is the Prometheus metric expression or internal metric name.
	Metric string `json:"metric"`

	// Unit is the measurement unit (ratio, ms, count, bytes, etc).
	Unit string `json:"unit"`

	// Source identifies which component emits this metric.
	Source string `json:"source"` // "metad", "datanode", "gateway"
}

// ============================================================
// SLO Definitions
// ============================================================

// SLO represents a Service Level Objective — a target value for an SLI.
type SLO struct {
	// Name is the unique identifier for this SLO.
	Name string `json:"name"`

	// SLI references the indicator this objective targets.
	SLIName string `json:"sli_name"`

	// Target is the objective value (e.g., 99.9 for 99.9% availability).
	Target float64 `json:"target"`

	// Window is the measurement period (e.g., "30d" for 30-day rolling window).
	Window string `json:"window"`

	// Severity classifies the impact of missing this SLO.
	// "critical" = data loss or service outage
	// "high" = degraded user experience
	// "medium" = operational concern
	// "low" = informational
	Severity string `json:"severity"`

	// Description explains the business impact of this SLO.
	Description string `json:"description"`
}

// ============================================================
// Alert Rule Definitions
// ============================================================

// AlertRule defines a monitoring alert condition.
type AlertRule struct {
	// Name is the unique alert name.
	Name string `json:"name"`

	// SLOName references the SLO this alert protects (optional).
	SLOName string `json:"slo_name,omitempty"`

	// Expr is the PromQL expression that triggers the alert.
	Expr string `json:"expr"`

	// For is the duration the condition must hold before firing.
	For string `json:"for"`

	// Severity is the alert severity (critical, warning, info).
	Severity string `json:"severity"`

	// Summary is a short description shown in alert notifications.
	Summary string `json:"summary"`

	// RunbookURL links to the operational runbook for this alert.
	RunbookURL string `json:"runbook_url,omitempty"`

	// Labels are additional labels for routing and grouping.
	Labels map[string]string `json:"labels,omitempty"`
}

// ============================================================
// NUFS SLO/SLI Registry
// ============================================================

// SLIDefinitions contains all SLIs for the NUFS system.
var SLIDefinitions = []SLI{
	// ---- Availability SLIs ----
	{
		Name:        "metad_availability",
		Category:    "availability",
		Description: "Ratio of successful metadata operations to total operations (success = 1 - error rate over ops)",
		Metric:      `1 - (sum(rate(nufs_errors_total[5m])) / clamp_min(sum(rate(nufs_ops_total[5m])),1))`,
		Unit:        "ratio",
		Source:      "metad",
	},
	{
		Name:        "datanode_availability",
		Category:    "availability",
		Description: "Fraction of registered data nodes currently online (the data-plane fleet availability)",
		Metric:      `nufs_nodes_online / nufs_nodes_total`,
		Unit:        "ratio",
		Source:      "metad",
	},
	{
		Name:        "chunk_read_availability",
		Category:    "availability",
		Description: "Fraction of anti-entropy chunk scans that found no mismatch (data-plane read-path correctness)",
		Metric:      `1 - (sum(rate(nufs_datanode_antientropy_mismatches_total[5m])) / clamp_min(sum(rate(nufs_datanode_antientropy_scanned_total[5m])),1))`,
		Unit:        "ratio",
		Source:      "datanode",
	},
	{
		Name:        "metad_leader_failover_rto",
		Category:    "availability",
		Description: "Time from raft leader loss (SIGKILL/failure) until a new leader successfully serves a metadata write. Budget is a hard RTO gate for a 5-9 tier; measured by the failover drill (scripts/soak/run-v21-leader-failover.sh) and surfaced here as a machine-checked SLO rather than a run-rate ratio.",
		Metric:      `metad_leader_failover_rto_seconds`,
		Unit:        "seconds",
		Source:      "metad",
	},

	// ---- Durability SLIs ----
	{
		Name:        "chunk_durability",
		Category:    "durability",
		Description: "Fraction of chunks that are not under-replicated (meet their target replica count)",
		Metric:      `1 - (sum(nufs_cluster_chunks_under_replicated) / clamp_min(sum(nufs_chunks_total),1))`,
		Unit:        "ratio",
		Source:      "metad",
	},
	{
		Name:        "integrity_check_pass_rate",
		Category:    "durability",
		Description: "Fraction of anti-entropy chunk scans that passed integrity (no checksum mismatch)",
		Metric:      `1 - (sum(rate(nufs_datanode_antientropy_mismatches_total[1h])) / clamp_min(sum(rate(nufs_datanode_antientropy_scanned_total[1h])),1))`,
		Unit:        "ratio",
		Source:      "datanode",
	},

	// ---- Latency SLIs ----
	{
		Name:        "metad_read_latency_p99",
		Category:    "latency",
		Description: "99th percentile latency of metadata read operations (microsecond summary converted to ms)",
		Metric:      `nufs_read_latency_us{quantile="0.99"} / 1000`,
		Unit:        "ms",
		Source:      "metad",
	},
	{
		Name:        "metad_write_latency_p99",
		Category:    "latency",
		Description: "99th percentile latency of metadata write operations (microsecond summary converted to ms)",
		Metric:      `nufs_write_latency_us{quantile="0.99"} / 1000`,
		Unit:        "ms",
		Source:      "metad",
	},
	{
		Name:        "chunk_write_latency_p99",
		Category:    "latency",
		Description: "Mean chunk write-path wait (write semaphore) in ms over a 1h rate window — the datanode exposes cumulative latency counters, not a p99 histogram, so this is the metric-derived mean",
		Metric:      `(sum(rate(nufs_datanode_write_semaphore_wait_seconds_total[1h])) / clamp_min(sum(rate(nufs_datanode_replication_writes_total[1h])),1)) * 1000`,
		Unit:        "ms",
		Source:      "datanode",
	},
	{
		Name:        "chunk_read_efficiency",
		Category:    "latency",
		Description: "Read-path efficiency over a 1h rate window = fraction of requested chunk bytes served without amplification (1 - amplified/requested)",
		Metric:      `1 - clamp_max(sum(rate(nufs_datanode_read_amplified_bytes_total[1h]))/clamp_min(sum(rate(nufs_datanode_read_requested_bytes_total[1h])),1), 1)`,
		Unit:        "ratio",
		Source:      "datanode",
	},
	{
		Name:        "wal_fsync_latency_p99",
		Category:    "latency",
		Description: "Mean chunk-file fsync latency in ms over a 1h rate window (fsync seconds / fsync count) — write durabilty cost; no p99 histogram exposed",
		Metric:      `(sum(rate(nufs_datanode_fsync_seconds_total[1h])) / clamp_min(sum(rate(nufs_datanode_fsync_total[1h])),1)) * 1000`,
		Unit:        "ms",
		Source:      "datanode",
	},

	// ---- Correctness SLIs ----
	{
		Name:        "raft_leader_stability",
		Category:    "correctness",
		Description: "Raft leader stability (1 = stable, 0 = not stable). A persistent 0 with frequent transitions indicates leader flapping/instability",
		Metric:      `nufs_cluster_leader_stable`,
		Unit:        "bool-gauge",
		Source:      "metad",
	},
	{
		Name:        "error_rate",
		Category:    "correctness",
		Description: "Ratio of error responses to total responses",
		Metric:      `sum(rate(nufs_errors_total[5m])) / sum(rate(nufs_ops_total[5m]))`,
		Unit:        "ratio",
		Source:      "all",
	},

	// ---- Operational SLIs ----
	{
		Name:        "nodes_online_ratio",
		Category:    "operational",
		Description: "Ratio of online nodes to total registered nodes",
		Metric:      `nufs_nodes_online / nufs_nodes_total`,
		Unit:        "ratio",
		Source:      "metad",
	},
	{
		Name:        "disk_usage_ratio",
		Category:    "operational",
		Description: "Disk usage as a fraction of total capacity",
		Metric:      `nufs_disk_used_bytes / nufs_disk_capacity_bytes`,
		Unit:        "ratio",
		Source:      "datanode",
	},
	{
		Name:        "repair_queue_depth",
		Category:    "operational",
		Description: "Number of chunks queued for repair (under-replication backlog)",
		Metric:      `nufs_repair_tasks_queued`,
		Unit:        "count",
		Source:      "metad",
	},
	{
		Name:        "repair_lag_seconds",
		Category:    "operational",
		Description: "Age of the oldest pending repair task (0 when the queue is empty — the exporter emits oldest_timestamp=0 for an empty queue)",
		Metric:      `(nufs_repair_oldest_timestamp > bool 0) * (time() - nufs_repair_oldest_timestamp)`,
		Unit:        "seconds",
		Source:      "metad",
	},
	{
		Name:        "backup_freshness",
		Category:    "operational",
		Description: "Recency of the last successful metadata backup relative to a 15min window (1 = backed up within 15m, decays to 0; 0 when no backup has ever succeeded). Success/failure status is not split by the exporter, so freshness is the real, queryable health signal",
		Metric:      `clamp_min(1 - ((time() - nufs_backup_last_success_timestamp_seconds) / 900), 0) * (nufs_backup_last_success_timestamp_seconds > bool 0)`,
		Unit:        "ratio",
		Source:      "metad",
	},
}

// SLODefinitions contains all SLOs for the NUFS system.
var SLODefinitions = []SLO{
	// ---- Availability SLOs ----
	{
		Name:        "metad_availability_99.9%",
		SLIName:     "metad_availability",
		Target:      99.9,
		Window:      "30d",
		Severity:    "critical",
		Description: "Metadata service must be available 99.9% of the time (max ~43min downtime/month)",
	},
	{
		Name:        "datanode_availability_99.9%",
		SLIName:     "datanode_availability",
		Target:      99.9,
		Window:      "30d",
		Severity:    "critical",
		Description: "Data node operations must succeed 99.9% of the time",
	},
	{
		Name:        "chunk_read_availability_99.99%",
		SLIName:     "chunk_read_availability",
		Target:      99.99,
		Window:      "30d",
		Severity:    "critical",
		Description: "Chunk reads must succeed 99.99% of the time (replication provides redundancy)",
	},
	{
		Name:        "metad_leader_failover_rto_15s",
		SLIName:     "metad_leader_failover_rto",
		Target:      15,
		Window:      "1h",
		Severity:    "critical",
		Description: "A replaced raft leader must be serving writes within 15s of failure. Drilled automatically; exceeding the budget means the 5-9 availability tier's recovery time is not met.",
	},

	// ---- Durability SLOs ----
	{
		Name:        "chunk_durability_99.9999%",
		SLIName:     "chunk_durability",
		Target:      99.9999,
		Window:      "365d",
		Severity:    "critical",
		Description: "No more than 1 in 1M chunks may lose data per year (6 nines durability)",
	},
	{
		Name:        "integrity_pass_rate_99.99%",
		SLIName:     "integrity_check_pass_rate",
		Target:      99.99,
		Window:      "30d",
		Severity:    "high",
		Description: "99.99% of integrity checks must pass; failures indicate data corruption",
	},

	// ---- Latency SLOs ----
	{
		Name:        "metad_read_p99_under_10ms",
		SLIName:     "metad_read_latency_p99",
		Target:      10,
		Window:      "7d",
		Severity:    "high",
		Description: "Metadata reads at p99 must complete within 10ms",
	},
	{
		Name:        "metad_write_p99_under_50ms",
		SLIName:     "metad_write_latency_p99",
		Target:      50,
		Window:      "7d",
		Severity:    "high",
		Description: "Metadata writes at p99 must complete within 50ms (includes Raft consensus)",
	},
	{
		Name:        "chunk_write_p99_under_100ms",
		SLIName:     "chunk_write_latency_p99",
		Target:      100,
		Window:      "7d",
		Severity:    "high",
		Description: "Chunk writes at p99 must complete within 100ms",
	},
	{
		Name:        "chunk_read_efficiency_high",
		SLIName:     "chunk_read_efficiency",
		Target:      0.95,
		Window:      "7d",
		Severity:    "high",
		Description: "Chunk reads should stay low-amplification: at least 95% of requested bytes served without read amplification",
	},
	{
		Name:        "wal_fsync_p99_under_10ms",
		SLIName:     "wal_fsync_latency_p99",
		Target:      10,
		Window:      "7d",
		Severity:    "high",
		Description: "Mean chunk-file fsync latency must stay under 10ms (critical for write durability cost)",
	},

	// ---- Operational SLOs ----
	{
		Name:        "nodes_online_66%",
		SLIName:     "nodes_online_ratio",
		Target:      66,
		Window:      "1d",
		Severity:    "high",
		Description: "At least 2/3 of nodes must be online to maintain Raft quorum",
	},
	{
		Name:        "disk_usage_under_85%",
		SLIName:     "disk_usage_ratio",
		Target:      85,
		Window:      "1d",
		Severity:    "medium",
		Description: "Disk usage must stay below 85% to ensure rebalance capacity",
	},
	{
		Name:        "repair_lag_under_1h",
		SLIName:     "repair_lag_seconds",
		Target:      3600,
		Window:      "1d",
		Severity:    "high",
		Description: "Repair tasks must be processed within 1 hour of detection",
	},
	{
		Name:        "backup_fresh_15m",
		SLIName:     "backup_freshness",
		Target:      0.9,
		Window:      "30d",
		Severity:    "medium",
		Description: "Metadata backup should be fresh: freshness (recency within a 15m window) must average 90% or better",
	},
}

// AlertRules contains all alerting rules for the NUFS system.
// These can be directly translated to Prometheus alertmanager rules.
var AlertRules = []AlertRule{
	// ---- Critical Alerts ----
	{
		Name:       "NUFSClusterQuorumAtRisk",
		SLOName:    "nodes_online_66%",
		Expr:       `nufs_nodes_online / nufs_nodes_total < 0.66`,
		For:        "2m",
		Severity:   "critical",
		Summary:    "Cluster quorum is at risk: less than 2/3 nodes are online",
		RunbookURL: "https://docs.nufs.io/runbook/quorum-at-risk",
		Labels:     map[string]string{"team": "storage", "component": "metad"},
	},
	{
		Name:       "NUFSDataDurabilityAtRisk",
		SLOName:    "chunk_durability_99.9999%",
		Expr:       `1 - (sum(nufs_cluster_chunks_under_replicated)/clamp_min(sum(nufs_chunks_total),1)) < 0.999`,
		For:        "5m",
		Severity:   "critical",
		Summary:    "Chunk durability is below 99.9% — data loss risk",
		RunbookURL: "https://docs.nufs.io/runbook/durability-risk",
		Labels:     map[string]string{"team": "storage", "component": "datanode"},
	},
	{
		Name:       "NUFSMetadAvailabilityDrop",
		SLOName:    "metad_availability_99.9%",
		Expr:       `1 - (sum(rate(nufs_errors_total[5m])) / clamp_min(sum(rate(nufs_ops_total[5m])),1)) < 0.99`,
		For:        "5m",
		Severity:   "critical",
		Summary:    "Metadata service availability dropped below 99%",
		RunbookURL: "https://docs.nufs.io/runbook/metad-availability",
		Labels:     map[string]string{"team": "storage", "component": "metad"},
	},
	{
		Name:       "NUFSRaftLeaderFlapping",
		Expr:       `nufs_cluster_leader_stable == 0`,
		For:        "2m",
		Severity:   "critical",
		Summary:    "Raft leader is not stable — cluster instability, writes may be failing",
		RunbookURL: "https://docs.nufs.io/runbook/raft-flapping",
		Labels:     map[string]string{"team": "storage", "component": "metad"},
	},
	{
		Name:       "NUFSLeaderFailoverRTOExceeded",
		SLOName:    "metad_leader_failover_rto_15s",
		Expr:       `metad_leader_failover_rto_seconds > 15`,
		For:        "1m",
		Severity:   "critical",
		Summary:    "Leader failover RTO exceeded 15s budget — 5-9 availability tier recovery time not met",
		RunbookURL: "https://docs.nufs.io/runbook/leader-failover-rto",
		Labels:     map[string]string{"team": "storage", "component": "metad"},
	},

	// ---- Warning Alerts ----
	{
		Name:       "NUFSDiskUsageHigh",
		SLOName:    "disk_usage_under_85%",
		Expr:       `nufs_disk_used_bytes / nufs_disk_capacity_bytes > 0.85`,
		For:        "30m",
		Severity:   "warning",
		Summary:    "Disk usage exceeds 85% — rebalance capacity limited",
		RunbookURL: "https://docs.nufs.io/runbook/disk-usage-high",
		Labels:     map[string]string{"team": "storage", "component": "datanode"},
	},
	{
		Name:       "NUFSDiskUsageCritical",
		SLOName:    "disk_usage_under_85%",
		Expr:       `nufs_disk_used_bytes / nufs_disk_capacity_bytes > 0.95`,
		For:        "5m",
		Severity:   "critical",
		Summary:    "Disk usage exceeds 95% — writes may fail soon",
		RunbookURL: "https://docs.nufs.io/runbook/disk-usage-critical",
		Labels:     map[string]string{"team": "storage", "component": "datanode"},
	},
	{
		Name:       "NUFSRepairQueueGrowing",
		SLOName:    "repair_lag_under_1h",
		Expr:       `nufs_repair_tasks_queued > 1000 and deriv(nufs_repair_tasks_queued[1h]) > 0`,
		For:        "15m",
		Severity:   "warning",
		Summary:    "Repair queue is growing — repair may not be keeping up",
		RunbookURL: "https://docs.nufs.io/runbook/repair-queue-growing",
		Labels:     map[string]string{"team": "storage", "component": "metad"},
	},
	{
		Name:       "NUFSRepairLagHigh",
		SLOName:    "repair_lag_under_1h",
		Expr:       `nufs_repair_oldest_timestamp > 0 and (time() - nufs_repair_oldest_timestamp > 3600)`,
		For:        "10m",
		Severity:   "warning",
		Summary:    "Oldest repair task is over 1 hour old",
		RunbookURL: "https://docs.nufs.io/runbook/repair-lag-high",
		Labels:     map[string]string{"team": "storage", "component": "metad"},
	},
	{
		Name:       "NUFSMetadReadLatencyHigh",
		SLOName:    "metad_read_p99_under_10ms",
		Expr:       `nufs_read_latency_us{quantile="0.99"} / 1000 > 10`,
		For:        "10m",
		Severity:   "warning",
		Summary:    "Metadata read p99 latency exceeds 10ms SLO",
		RunbookURL: "https://docs.nufs.io/runbook/metad-read-latency",
		Labels:     map[string]string{"team": "storage", "component": "metad"},
	},
	{
		Name:       "NUFSMetadWriteLatencyHigh",
		SLOName:    "metad_write_p99_under_50ms",
		Expr:       `nufs_write_latency_us{quantile="0.99"} / 1000 > 50`,
		For:        "10m",
		Severity:   "warning",
		Summary:    "Metadata write p99 latency exceeds 50ms SLO",
		RunbookURL: "https://docs.nufs.io/runbook/metad-write-latency",
		Labels:     map[string]string{"team": "storage", "component": "metad"},
	},
	{
		Name:       "NUFSChunkWriteLatencyHigh",
		SLOName:    "chunk_write_p99_under_100ms",
		Expr:       `(sum(rate(nufs_datanode_write_semaphore_wait_seconds_total[1h])) / clamp_min(sum(rate(nufs_datanode_replication_writes_total[1h])),1)) * 1000 > 100`,
		For:        "10m",
		Severity:   "warning",
		Summary:    "Mean chunk write-path wait (1h window) exceeds 100ms SLO",
		RunbookURL: "https://docs.nufs.io/runbook/chunk-write-latency",
		Labels:     map[string]string{"team": "storage", "component": "datanode"},
	},
	{
		Name:       "NUFSChunkReadLatencyHigh",
		SLOName:    "chunk_read_efficiency_high",
		Expr:       `1 - clamp_max(sum(rate(nufs_datanode_read_amplified_bytes_total[1h]))/clamp_min(sum(rate(nufs_datanode_read_requested_bytes_total[1h])),1), 1) < 0.95`,
		For:        "10m",
		Severity:   "warning",
		Summary:    "Chunk read efficiency (low amplification) dropped below 95%",
		RunbookURL: "https://docs.nufs.io/runbook/chunk-read-latency",
		Labels:     map[string]string{"team": "storage", "component": "datanode"},
	},
	{
		Name:       "NUFSWALFsyncLatencyHigh",
		SLOName:    "wal_fsync_p99_under_10ms",
		Expr:       `(sum(rate(nufs_datanode_fsync_seconds_total[1h])) / clamp_min(sum(rate(nufs_datanode_fsync_total[1h])),1)) * 1000 > 10`,
		For:        "5m",
		Severity:   "warning",
		Summary:    "Mean chunk-file fsync latency (1h window) exceeds 10ms — write performance at risk",
		RunbookURL: "https://docs.nufs.io/runbook/wal-fsync-latency",
		Labels:     map[string]string{"team": "storage", "component": "datanode"},
	},
	{
		Name:       "NUFSNodeOffline",
		SLOName:    "nodes_online_66%",
		Expr:       `nufs_nodes_online < nufs_nodes_total * 0.8`,
		For:        "5m",
		Severity:   "warning",
		Summary:    "More than 20% of nodes are offline",
		RunbookURL: "https://docs.nufs.io/runbook/node-offline",
		Labels:     map[string]string{"team": "storage", "component": "metad"},
	},
	{
		Name:       "NUFSBackupFailing",
		SLOName:    "backup_fresh_15m",
		Expr:       `nufs_backup_last_success_timestamp_seconds > 0 and (time() - nufs_backup_last_success_timestamp_seconds > 4500)`,
		For:        "1h",
		Severity:   "warning",
		Summary:    "Metadata backup has not completed recently (last success is stale)",
		RunbookURL: "https://docs.nufs.io/runbook/backup-failing",
		Labels:     map[string]string{"team": "storage", "component": "metad"},
	},
	{
		Name:       "NUFSIntegrityCheckFailures",
		SLOName:    "integrity_pass_rate_99.99%",
		Expr:       `sum(rate(nufs_datanode_antientropy_mismatches_total[1h])) / clamp_min(sum(rate(nufs_datanode_antientropy_scanned_total[1h])),1) > 0.0001`,
		For:        "30m",
		Severity:   "warning",
		Summary:    "Integrity check failure rate exceeds 0.01% — possible data corruption",
		RunbookURL: "https://docs.nufs.io/runbook/integrity-failures",
		Labels:     map[string]string{"team": "storage", "component": "datanode"},
	},

	// ---- Info Alerts ----
	{
		Name:       "NUFSReplicationFactorUnderReplicated",
		SLOName:    "chunk_durability_99.9999%",
		Expr:       `sum(nufs_cluster_chunks_under_replicated) / clamp_min(sum(nufs_chunks_total),1) > 0.001`,
		For:        "30m",
		Severity:   "info",
		Summary:    "Some chunks are under-replicated — repair should catch up",
		RunbookURL: "https://docs.nufs.io/runbook/under-replicated",
		Labels:     map[string]string{"team": "storage", "component": "metad"},
	},
	{
		Name:       "NUFSDiskDegraded",
		Expr:       `nufs_disk_state{state="degraded"} > 0`,
		For:        "5m",
		Severity:   "info",
		Summary:    "A disk is in degraded state — monitoring for recovery",
		RunbookURL: "https://docs.nufs.io/runbook/disk-degraded",
		Labels:     map[string]string{"team": "storage", "component": "datanode"},
	},
}

// ============================================================
// Error Budget Calculations
// ============================================================

// ErrorBudget represents the allowed budget for an SLO.
type ErrorBudget struct {
	SLOName       string  `json:"slo_name"`
	Target        float64 `json:"target"`
	Window        string  `json:"window"`
	BudgetPercent float64 `json:"budget_percent"` // 100 - target
	BudgetMinutes float64 `json:"budget_minutes"` // allowed downtime in minutes per window
}

// ErrorBudgets pre-calculates error budgets for all SLOs.
var ErrorBudgets = []ErrorBudget{
	{
		SLOName:       "metad_availability_99.9%",
		Target:        99.9,
		Window:        "30d",
		BudgetPercent: 0.1,
		BudgetMinutes: 43.2, // 0.1% of 30 * 24 * 60
	},
	{
		SLOName:       "datanode_availability_99.9%",
		Target:        99.9,
		Window:        "30d",
		BudgetPercent: 0.1,
		BudgetMinutes: 43.2,
	},
	{
		SLOName:       "chunk_read_availability_99.99%",
		Target:        99.99,
		Window:        "30d",
		BudgetPercent: 0.01,
		BudgetMinutes: 4.32,
	},
	{
		SLOName:       "chunk_durability_99.9999%",
		Target:        99.9999,
		Window:        "365d",
		BudgetPercent: 0.0001,
		BudgetMinutes: 0.53, // ~31.5 seconds per year
	},
	{
		SLOName:       "metad_read_p99_under_10ms",
		Target:        99, // 99% of reads under 10ms
		Window:        "7d",
		BudgetPercent: 1,
		BudgetMinutes: 100.8, // 1% of 7 * 24 * 60
	},
	{
		SLOName:       "chunk_write_p99_under_100ms",
		Target:        99,
		Window:        "7d",
		BudgetPercent: 1,
		BudgetMinutes: 100.8,
	},
	{
		SLOName:       "metad_leader_failover_rto_15s",
		Target:        15,
		Window:        "1h",
		BudgetPercent: 100,
		BudgetMinutes: 0.25, // RTO is a hard one-shot budget, not a windowed ratio
	},
}
