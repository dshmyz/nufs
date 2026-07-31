package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/example/dfs/metadata"
)

var backupVerificationFailuresTotal atomic.Int64
var restoreVerificationFailuresTotal atomic.Int64
var restoreVerificationDurationMillis atomic.Int64

func prometheusMetricsHandler(store *metadata.PebbleStore, metrics *metadata.Metrics, backupDeps ...backupOpsDependency) http.Handler {
	base := metadata.PrometheusHandler(metrics)
	backupSource := pebbleBackupMetricsSource{store: store}
	if len(backupDeps) > 0 {
		backupSource.coordinator = backupDeps[0].coordinator
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rr := httptestResponseRecorder()
		base.ServeHTTP(rr, r)
		for key, values := range rr.headers {
			for _, value := range values {
				w.Header().Add(key, value)
			}
		}
		if rr.code != http.StatusOK {
			w.WriteHeader(rr.code)
			_, _ = w.Write(rr.body.Bytes())
			return
		}

		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(rr.body.Bytes())
		h := &opsHandlers{store: store}
		writePrometheusWriteOps(w, h.writeOpsStatus(r.Context()))
		writePrometheusBucketQuota(r.Context(), w, newPebbleBucketQuotaMetricsSource(store))
		writePrometheusBackup(r.Context(), w, backupSource, time.Now().UTC())
		writePrometheusNodeMetrics(w, store)
		writePrometheusClusterReadiness(w, store)
	})
}

type bucketQuotaMetricsSource interface {
	ListBuckets(context.Context) ([]metadata.BucketInfo, error)
	GetBucketQuota(context.Context, string) (*metadata.BucketQuota, error)
	GetBucketUsage(context.Context, string) (*metadata.BucketUsage, error)
}

type pebbleBucketQuotaMetricsSource struct {
	store       *metadata.PebbleStore
	usageOnce   sync.Once
	usageByName map[string]metadata.BucketUsage
	usageErr    error
}

func newPebbleBucketQuotaMetricsSource(store *metadata.PebbleStore) *pebbleBucketQuotaMetricsSource {
	return &pebbleBucketQuotaMetricsSource{store: store}
}

func (s *pebbleBucketQuotaMetricsSource) ListBuckets(ctx context.Context) ([]metadata.BucketInfo, error) {
	return s.store.ListBuckets(ctx)
}

func (s *pebbleBucketQuotaMetricsSource) GetBucketQuota(ctx context.Context, bucket string) (*metadata.BucketQuota, error) {
	return s.store.GetBucketQuota(ctx, bucket)
}

func (s *pebbleBucketQuotaMetricsSource) GetBucketUsage(ctx context.Context, bucket string) (*metadata.BucketUsage, error) {
	s.usageOnce.Do(func() {
		var usage []metadata.BucketUsage
		usage, s.usageErr = s.store.ComputeAllBucketUsage(ctx)
		if s.usageErr != nil {
			return
		}
		s.usageByName = make(map[string]metadata.BucketUsage, len(usage))
		for _, item := range usage {
			s.usageByName[item.Name] = item
		}
	})
	if s.usageErr != nil {
		return nil, s.usageErr
	}
	usage, ok := s.usageByName[bucket]
	if !ok {
		return &metadata.BucketUsage{Name: bucket}, nil
	}
	return &usage, nil
}

func writePrometheusBucketQuota(ctx context.Context, w io.Writer, source bucketQuotaMetricsSource) {
	fmt.Fprintln(w)
	fmt.Fprintln(w, "# HELP nufs_bucket_quota_limit Configured bucket quota limit by resource")
	fmt.Fprintln(w, "# TYPE nufs_bucket_quota_limit gauge")
	fmt.Fprintln(w, "# HELP nufs_bucket_quota_usage Current bucket quota usage by resource")
	fmt.Fprintln(w, "# TYPE nufs_bucket_quota_usage gauge")
	fmt.Fprintln(w, "# HELP nufs_bucket_quota_used_ratio Current bucket quota usage divided by its configured limit")
	fmt.Fprintln(w, "# TYPE nufs_bucket_quota_used_ratio gauge")

	buckets, err := source.ListBuckets(ctx)
	if err != nil {
		return
	}
	for _, bucket := range buckets {
		quota, err := source.GetBucketQuota(ctx, bucket.Name)
		if err != nil || quota == nil {
			continue
		}
		if quota.MaxSizeBytes == 0 && quota.MaxObjects == 0 {
			continue
		}
		usage, err := source.GetBucketUsage(ctx, bucket.Name)
		if err != nil || usage == nil {
			continue
		}

		label := prometheusLabelValue(bucket.Name)
		if quota.MaxSizeBytes > 0 {
			writePrometheusBucketQuotaResource(w, label, "bytes", quota.MaxSizeBytes, usage.UsedBytes)
		}
		if quota.MaxObjects > 0 {
			writePrometheusBucketQuotaResource(w, label, "objects", quota.MaxObjects, int64(usage.Objects))
		}
	}
}

func writePrometheusBucketQuotaResource(w io.Writer, bucket, resource string, limit, usage int64) {
	fmt.Fprintf(w, "nufs_bucket_quota_limit{bucket=\"%s\",resource=\"%s\"} %d\n", bucket, resource, limit)
	fmt.Fprintf(w, "nufs_bucket_quota_usage{bucket=\"%s\",resource=\"%s\"} %d\n", bucket, resource, usage)
	fmt.Fprintf(w, "nufs_bucket_quota_used_ratio{bucket=\"%s\",resource=\"%s\"} %g\n", bucket, resource, float64(usage)/float64(limit))
}

type responseRecorder struct {
	headers http.Header
	body    bytes.Buffer
	code    int
}

func httptestResponseRecorder() *responseRecorder {
	return &responseRecorder{headers: make(http.Header), code: http.StatusOK}
}

func (r *responseRecorder) Header() http.Header {
	return r.headers
}

func (r *responseRecorder) WriteHeader(code int) {
	r.code = code
}

func (r *responseRecorder) Write(p []byte) (int, error) {
	return r.body.Write(p)
}

func writePrometheusWriteOps(w io.Writer, status metadata.WriteOpsStatus) {
	fmt.Fprintln(w)
	fmt.Fprintln(w, "# HELP nufs_object_write_attempts Object write attempts by state")
	fmt.Fprintln(w, "# TYPE nufs_object_write_attempts gauge")
	for state, count := range status.Attempts {
		fmt.Fprintf(w, "nufs_object_write_attempts{state=\"%s\"} %d\n", prometheusLabelValue(state), count)
	}

	fmt.Fprintln(w)
	fmt.Fprintln(w, "# HELP nufs_object_write_background_task_state Object write background task current state")
	fmt.Fprintln(w, "# TYPE nufs_object_write_background_task_state gauge")
	writePrometheusBackgroundTaskState(w, "recovery", status.RecoveryTask)
	writePrometheusBackgroundTaskState(w, "gc", status.GCTask)

	fmt.Fprintln(w)
	fmt.Fprintln(w, "# HELP nufs_object_write_background_task_attempts Object write background task attempt count")
	fmt.Fprintln(w, "# TYPE nufs_object_write_background_task_attempts gauge")
	fmt.Fprintf(w, "nufs_object_write_background_task_attempts{task=\"recovery\"} %d\n", status.RecoveryTask.AttemptCount)
	fmt.Fprintf(w, "nufs_object_write_background_task_attempts{task=\"gc\"} %d\n", status.GCTask.AttemptCount)

	fmt.Fprintln(w)
	fmt.Fprintln(w, "# HELP nufs_object_write_background_task_last_updated_seconds Object write background task last update time")
	fmt.Fprintln(w, "# TYPE nufs_object_write_background_task_last_updated_seconds gauge")
	fmt.Fprintf(w, "nufs_object_write_background_task_last_updated_seconds{task=\"recovery\"} %.3f\n", float64(status.RecoveryTask.UpdatedAt)/1e9)
	fmt.Fprintf(w, "nufs_object_write_background_task_last_updated_seconds{task=\"gc\"} %.3f\n", float64(status.GCTask.UpdatedAt)/1e9)
}

func writePrometheusBackgroundTaskState(w io.Writer, task string, status metadata.BackgroundTaskStatus) {
	state := string(status.State)
	if state == "" {
		state = "missing"
	}
	fmt.Fprintf(w, "nufs_object_write_background_task_state{task=\"%s\",state=\"%s\"} 1\n", prometheusLabelValue(task), prometheusLabelValue(state))
}

func prometheusLabelValue(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, "\n", `\n`)
	value = strings.ReplaceAll(value, `"`, `\"`)
	return value
}

type backupMetricsSource interface {
	BackupStatus(context.Context) (metadata.BackupCoordinatorStatus, bool)
	ListBackupTasks(context.Context, int) ([]metadata.BackupTask, error)
	BackupCatalog(context.Context) (*metadata.BackupCatalogState, error)
	ListChunkTombstones(context.Context, int) ([]metadata.ChunkTombstone, error)
}

type pebbleBackupMetricsSource struct {
	store       *metadata.PebbleStore
	coordinator backupOpsCoordinator
}

func (s pebbleBackupMetricsSource) BackupStatus(ctx context.Context) (metadata.BackupCoordinatorStatus, bool) {
	if s.coordinator == nil {
		return metadata.BackupCoordinatorStatus{}, false
	}
	return s.coordinator.Status(ctx), true
}

func (s pebbleBackupMetricsSource) ListBackupTasks(ctx context.Context, limit int) ([]metadata.BackupTask, error) {
	if s.store == nil {
		return nil, nil
	}
	return s.store.ListBackupTasks(ctx, limit)
}

func (s pebbleBackupMetricsSource) BackupCatalog(ctx context.Context) (*metadata.BackupCatalogState, error) {
	if s.store == nil {
		return nil, nil
	}
	return s.store.GetBackupCatalogState(ctx)
}

func (s pebbleBackupMetricsSource) ListChunkTombstones(ctx context.Context, limit int) ([]metadata.ChunkTombstone, error) {
	if s.store == nil {
		return nil, nil
	}
	return s.store.ListChunkTombstones(ctx, limit)
}

func writePrometheusBackup(ctx context.Context, w io.Writer, source backupMetricsSource, now time.Time) {
	fmt.Fprintln(w)
	fmt.Fprintln(w, "# HELP nufs_backup_enabled Metadata backup coordinator configured")
	fmt.Fprintln(w, "# TYPE nufs_backup_enabled gauge")
	fmt.Fprintln(w, "# HELP nufs_backup_active Metadata backup run currently active")
	fmt.Fprintln(w, "# TYPE nufs_backup_active gauge")
	fmt.Fprintln(w, "# HELP nufs_backup_last_success_timestamp_seconds Unix timestamp of the latest committed metadata backup")
	fmt.Fprintln(w, "# TYPE nufs_backup_last_success_timestamp_seconds gauge")
	fmt.Fprintln(w, "# HELP nufs_backup_last_success_applied_index Applied index of the latest committed metadata backup")
	fmt.Fprintln(w, "# TYPE nufs_backup_last_success_applied_index gauge")
	fmt.Fprintln(w, "# HELP nufs_backup_last_failure_timestamp_seconds Unix timestamp of the latest failed metadata backup")
	fmt.Fprintln(w, "# TYPE nufs_backup_last_failure_timestamp_seconds gauge")
	fmt.Fprintln(w, "# HELP nufs_backup_duration_seconds Duration in seconds of the latest metadata backup run by terminal state")
	fmt.Fprintln(w, "# TYPE nufs_backup_duration_seconds gauge")
	fmt.Fprintln(w, "# HELP nufs_backup_artifact_bytes Total bytes in the latest committed metadata backup artifact")
	fmt.Fprintln(w, "# TYPE nufs_backup_artifact_bytes gauge")
	fmt.Fprintln(w, "# HELP nufs_backup_upload_failures_total Metadata backup failures after upload progress was recorded")
	fmt.Fprintln(w, "# TYPE nufs_backup_upload_failures_total counter")
	fmt.Fprintln(w, "# HELP nufs_backup_staging_artifacts Metadata backup tasks that may still have staging artifacts")
	fmt.Fprintln(w, "# TYPE nufs_backup_staging_artifacts gauge")
	fmt.Fprintln(w, "# HELP nufs_backup_runs_total Metadata backup tasks by terminal state")
	fmt.Fprintln(w, "# TYPE nufs_backup_runs_total counter")
	fmt.Fprintln(w, "# HELP nufs_backup_verification_failures_total Metadata backup verification failures observed by ops API")
	fmt.Fprintln(w, "# TYPE nufs_backup_verification_failures_total counter")
	fmt.Fprintln(w, "# HELP nufs_restore_verification_duration_seconds Duration in seconds of the latest restore verification check")
	fmt.Fprintln(w, "# TYPE nufs_restore_verification_duration_seconds gauge")
	fmt.Fprintln(w, "# HELP nufs_restore_verification_failures_total Restore verification failures observed by metad")
	fmt.Fprintln(w, "# TYPE nufs_restore_verification_failures_total counter")

	status, enabled := source.BackupStatus(ctx)
	writePrometheusBoolGauge(w, "nufs_backup_enabled", enabled)
	writePrometheusBoolGauge(w, "nufs_backup_active", enabled && status.Active)

	var lastSuccess, lastFailure time.Time
	var lastSuccessAppliedIndex uint64
	var artifactBytes int64
	var committed, failed, uploadFailures, stagingArtifacts int64
	latestDurations := map[metadata.BackupTaskState]struct {
		completed time.Time
		seconds   int64
	}{
		metadata.BackupTaskCommitted: {},
		metadata.BackupTaskFailed:    {},
	}
	tasks, err := source.ListBackupTasks(ctx, 1000)
	if err == nil {
		for _, task := range tasks {
			switch task.State {
			case metadata.BackupTaskCommitted:
				committed++
				if task.CompletedAt.After(lastSuccess) {
					lastSuccess = task.CompletedAt
					lastSuccessAppliedIndex = task.AppliedIndex
					if task.BytesUploaded > 0 {
						artifactBytes = task.BytesUploaded
					}
				}
				recordBackupDuration(latestDurations, task)
			case metadata.BackupTaskFailed:
				failed++
				if task.BytesUploaded > 0 || task.FilesUploaded > 0 {
					uploadFailures++
				}
				when := task.CompletedAt
				if when.IsZero() {
					when = task.UpdatedAt
				}
				if when.After(lastFailure) {
					lastFailure = when
				}
				recordBackupDuration(latestDurations, task)
			case metadata.BackupTaskCreating, metadata.BackupTaskUploading, metadata.BackupTaskVerifying:
				stagingArtifacts++
			}
		}
	}
	if catalog, err := source.BackupCatalog(ctx); err == nil && catalog != nil {
		for _, backup := range catalog.Backups {
			if backup.CreatedAt.After(lastSuccess) {
				lastSuccess = backup.CreatedAt
				lastSuccessAppliedIndex = backup.AppliedIndex
				artifactBytes = backup.TotalBytes
			}
		}
	}
	fmt.Fprintf(w, "nufs_backup_last_success_timestamp_seconds %d\n", unixSeconds(lastSuccess))
	fmt.Fprintf(w, "nufs_backup_last_success_applied_index %d\n", lastSuccessAppliedIndex)
	fmt.Fprintf(w, "nufs_backup_last_failure_timestamp_seconds %d\n", unixSeconds(lastFailure))
	fmt.Fprintf(w, "nufs_backup_duration_seconds{state=\"committed\"} %d\n", latestDurations[metadata.BackupTaskCommitted].seconds)
	fmt.Fprintf(w, "nufs_backup_duration_seconds{state=\"failed\"} %d\n", latestDurations[metadata.BackupTaskFailed].seconds)
	fmt.Fprintf(w, "nufs_backup_artifact_bytes %d\n", artifactBytes)
	fmt.Fprintf(w, "nufs_backup_upload_failures_total %d\n", uploadFailures)
	fmt.Fprintf(w, "nufs_backup_staging_artifacts %d\n", stagingArtifacts)
	fmt.Fprintf(w, "nufs_backup_runs_total{state=\"committed\"} %d\n", committed)
	fmt.Fprintf(w, "nufs_backup_runs_total{state=\"failed\"} %d\n", failed)
	fmt.Fprintf(w, "nufs_backup_verification_failures_total %d\n", backupVerificationFailuresTotal.Load())
	fmt.Fprintf(w, "nufs_restore_verification_duration_seconds %g\n", float64(restoreVerificationDurationMillis.Load())/1000)
	fmt.Fprintf(w, "nufs_restore_verification_failures_total %d\n", restoreVerificationFailuresTotal.Load())

	fmt.Fprintln(w)
	fmt.Fprintln(w, "# HELP nufs_chunk_tombstones Retained chunk tombstones awaiting safe physical purge")
	fmt.Fprintln(w, "# TYPE nufs_chunk_tombstones gauge")
	fmt.Fprintln(w, "# HELP nufs_chunk_tombstone_bytes Total bytes retained by chunk tombstones")
	fmt.Fprintln(w, "# TYPE nufs_chunk_tombstone_bytes gauge")
	fmt.Fprintln(w, "# HELP nufs_chunk_tombstone_backlog Retained chunk tombstones awaiting safe physical purge")
	fmt.Fprintln(w, "# TYPE nufs_chunk_tombstone_backlog gauge")
	fmt.Fprintln(w, "# HELP nufs_chunk_tombstone_oldest_age_seconds Age in seconds of the oldest retained chunk tombstone")
	fmt.Fprintln(w, "# TYPE nufs_chunk_tombstone_oldest_age_seconds gauge")
	tombstones, err := source.ListChunkTombstones(ctx, 0)
	var oldestAge int64
	var tombstoneBytes int64
	if err == nil {
		for _, tombstone := range tombstones {
			if tombstone.Size > 0 {
				tombstoneBytes += tombstone.Size
			}
			if tombstone.DeletedAt.IsZero() {
				continue
			}
			age := int64(now.Sub(tombstone.DeletedAt).Seconds())
			if age > oldestAge {
				oldestAge = age
			}
		}
		fmt.Fprintf(w, "nufs_chunk_tombstones %d\n", len(tombstones))
		fmt.Fprintf(w, "nufs_chunk_tombstone_bytes %d\n", tombstoneBytes)
		fmt.Fprintf(w, "nufs_chunk_tombstone_backlog %d\n", len(tombstones))
	} else {
		fmt.Fprintln(w, "nufs_chunk_tombstones 0")
		fmt.Fprintln(w, "nufs_chunk_tombstone_bytes 0")
		fmt.Fprintln(w, "nufs_chunk_tombstone_backlog 0")
	}
	fmt.Fprintf(w, "nufs_chunk_tombstone_oldest_age_seconds %d\n", oldestAge)
}

func recordBackupDuration(
	durations map[metadata.BackupTaskState]struct {
		completed time.Time
		seconds   int64
	},
	task metadata.BackupTask,
) {
	if task.StartedAt.IsZero() || task.CompletedAt.IsZero() || task.CompletedAt.Before(task.StartedAt) {
		return
	}
	current := durations[task.State]
	if task.CompletedAt.After(current.completed) {
		durations[task.State] = struct {
			completed time.Time
			seconds   int64
		}{
			completed: task.CompletedAt,
			seconds:   int64(task.CompletedAt.Sub(task.StartedAt).Seconds()),
		}
	}
}

func writePrometheusBoolGauge(w io.Writer, name string, value bool) {
	if value {
		fmt.Fprintf(w, "%s 1\n", name)
		return
	}
	fmt.Fprintf(w, "%s 0\n", name)
}

func unixSeconds(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.Unix()
}

func writePrometheusNodeMetrics(w io.Writer, store *metadata.PebbleStore) {
	fmt.Fprintln(w)
	fmt.Fprintln(w, "# HELP nufs_node_write_error_rate Per-node write error rate (0.0-1.0) from last heartbeat window")
	fmt.Fprintln(w, "# TYPE nufs_node_write_error_rate gauge")
	fmt.Fprintln(w, "# HELP nufs_node_disk_io Per-node disk I/O utilization (0.0-1.0)")
	fmt.Fprintln(w, "# TYPE nufs_node_disk_io gauge")
	fmt.Fprintln(w, "# HELP nufs_node_capacity_bytes Per-node total capacity in bytes")
	fmt.Fprintln(w, "# TYPE nufs_node_capacity_bytes gauge")
	fmt.Fprintln(w, "# HELP nufs_node_used_bytes Per-node used storage in bytes")
	fmt.Fprintln(w, "# TYPE nufs_node_used_bytes gauge")

	for _, nm := range store.NodeMetrics() {
		nodeID := fmt.Sprintf("%d", nm.NodeID)
		fmt.Fprintf(w, "nufs_node_write_error_rate{node_id=\"%s\"} %g\n", nodeID, nm.ErrorRate)
		fmt.Fprintf(w, "nufs_node_disk_io{node_id=\"%s\"} %g\n", nodeID, nm.LoadIndex)
		fmt.Fprintf(w, "nufs_node_capacity_bytes{node_id=\"%s\"} %d\n", nodeID, nm.CapacityGB*1024*1024*1024)
		fmt.Fprintf(w, "nufs_node_used_bytes{node_id=\"%s\"} %d\n", nodeID, nm.UsedGB*1024*1024*1024)
	}
}

func writePrometheusClusterReadiness(w io.Writer, store *metadata.PebbleStore) {
	hc := store.GetHealthChecker()
	if hc == nil {
		return
	}
	r := hc.ComputeClusterReadiness()

	fmt.Fprintln(w)
	fmt.Fprintln(w, "# HELP nufs_cluster_readiness Cluster readiness status (0=not_ready, 1=degraded, 2=ready)")
	fmt.Fprintln(w, "# TYPE nufs_cluster_readiness gauge")
	fmt.Fprintln(w, "# HELP nufs_cluster_can_write_rf Maximum replication factor the cluster can sustain")
	fmt.Fprintln(w, "# TYPE nufs_cluster_can_write_rf gauge")
	fmt.Fprintln(w, "# HELP nufs_cluster_chunks_under_replicated Number of under-replicated chunks")
	fmt.Fprintln(w, "# TYPE nufs_cluster_chunks_under_replicated gauge")
	fmt.Fprintln(w, "# HELP nufs_cluster_leader_stable Whether the Raft leader is stable (0/1)")
	fmt.Fprintln(w, "# TYPE nufs_cluster_leader_stable gauge")

	var readinessVal int
	switch r.Status {
	case "ready":
		readinessVal = 2
	case "degraded":
		readinessVal = 1
	default:
		readinessVal = 0
	}
	fmt.Fprintf(w, "nufs_cluster_readiness %d\n", readinessVal)
	fmt.Fprintf(w, "nufs_cluster_can_write_rf %d\n", r.CanWriteRF)
	fmt.Fprintf(w, "nufs_cluster_chunks_under_replicated %d\n", r.ChunksUnderReplicated)
	if r.LeaderStable {
		fmt.Fprintf(w, "nufs_cluster_leader_stable 1\n")
	} else {
		fmt.Fprintf(w, "nufs_cluster_leader_stable 0\n")
	}
}
