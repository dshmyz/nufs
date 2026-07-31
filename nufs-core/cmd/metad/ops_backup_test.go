package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/example/dfs/metadata"
	"gopkg.in/yaml.v3"
)

func TestOpsBackupDisabledReturnsStructuredServiceUnavailable(t *testing.T) {
	store, bundle := newOpsTestStore(t)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/backups/status", nil)
	rr := httptest.NewRecorder()

	(&opsHandlers{store: store, bundle: bundle}).handleBackupStatus(rr, req)

	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503, body=%s", rr.Code, rr.Body.String())
	}
	var body map[string]string
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body["code"] != "backup_disabled" {
		t.Fatalf("code = %q, want backup_disabled", body["code"])
	}
}

func TestOpsBackupCreateReturnsTriggeredTask(t *testing.T) {
	store, bundle := newOpsTestStore(t)
	started := time.Date(2026, 7, 30, 1, 2, 3, 0, time.UTC)
	coordinator := &fakeOpsBackupCoordinator{
		triggerResult: &metadata.BackupRunResult{Task: metadata.BackupTask{
			ID:              "backup-20260730T010203Z-0011223344556677",
			SourceClusterID: "cluster-a",
			State:           metadata.BackupTaskCommitted,
			StartedAt:       started,
			CompletedAt:     started.Add(2 * time.Second),
		}},
	}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/backups", bytes.NewReader(nil))
	rr := httptest.NewRecorder()

	(&opsHandlers{
		store:             store,
		bundle:            bundle,
		backupCoordinator: coordinator,
		backupRepository:  fakeOpsBackupRepository{},
	}).handleBackups(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200, body=%s", rr.Code, rr.Body.String())
	}
	if coordinator.triggerCalls != 1 {
		t.Fatalf("trigger calls = %d, want 1", coordinator.triggerCalls)
	}
	var body struct {
		Task metadata.BackupTask `json:"task"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.Task.ID != coordinator.triggerResult.Task.ID || body.Task.State != metadata.BackupTaskCommitted {
		t.Fatalf("task = %+v", body.Task)
	}
}

func TestOpsBackupCreateRejectsActiveBackupWithoutJoiningTrigger(t *testing.T) {
	ctx := context.Background()
	store, bundle := newOpsTestStore(t)
	started := time.Date(2026, 7, 30, 2, 0, 0, 0, time.UTC)
	active := metadata.BackupTask{
		ID:              "backup-20260730T020000Z-0011223344556677",
		SourceClusterID: "cluster-a",
		OwnerNodeID:     "meta-1",
		LeadershipTerm:  1,
		AppliedIndex:    10,
		State:           metadata.BackupTaskCreating,
		StartedAt:       started,
		UpdatedAt:       started.Add(time.Second),
	}
	if err := store.PutBackupTask(ctx, &active); err != nil {
		t.Fatalf("PutBackupTask: %v", err)
	}
	coordinator := &fakeOpsBackupCoordinator{
		status: metadata.BackupCoordinatorStatus{Active: true},
		triggerResult: &metadata.BackupRunResult{Task: metadata.BackupTask{
			ID:    "backup-should-not-start",
			State: metadata.BackupTaskCommitted,
		}},
	}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/backups", nil)
	rr := httptest.NewRecorder()

	(&opsHandlers{
		store:             store,
		bundle:            bundle,
		backupCoordinator: coordinator,
		backupRepository:  fakeOpsBackupRepository{},
	}).handleBackups(rr, req)

	if rr.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409, body=%s", rr.Code, rr.Body.String())
	}
	if coordinator.triggerCalls != 0 {
		t.Fatalf("trigger calls = %d, want 0", coordinator.triggerCalls)
	}
	var body struct {
		Code string              `json:"code"`
		Task metadata.BackupTask `json:"task"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.Code != "backup_in_progress" {
		t.Fatalf("code = %q, want backup_in_progress", body.Code)
	}
	if body.Task.ID != active.ID || body.Task.State != metadata.BackupTaskCreating {
		t.Fatalf("task = %+v, want active %+v", body.Task, active)
	}
}

func TestOpsBackupVerifyReportsStructuredFailureAndIncrementsMetric(t *testing.T) {
	store, bundle := newOpsTestStore(t)
	backupVerificationFailuresTotal.Store(0)
	repo := fakeOpsBackupRepository{fetchErr: errors.New("checksum mismatch")}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/backups/backup-20260730T010203Z-0011223344556677/verify", nil)
	rr := httptest.NewRecorder()

	(&opsHandlers{
		store:             store,
		bundle:            bundle,
		backupCoordinator: &fakeOpsBackupCoordinator{},
		backupRepository:  repo,
	}).handleBackupByID(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500, body=%s", rr.Code, rr.Body.String())
	}
	var body map[string]string
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body["code"] != "backup_failed" {
		t.Fatalf("code = %q, want backup_failed", body["code"])
	}
	if got := backupVerificationFailuresTotal.Load(); got != 1 {
		t.Fatalf("verification failures = %d, want 1", got)
	}
}

func TestOpsBackupVerifyRejectsConcurrentBackup(t *testing.T) {
	store, bundle := newOpsTestStore(t)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/backups/backup-20260730T010203Z-0011223344556677/verify", nil)
	rr := httptest.NewRecorder()

	(&opsHandlers{
		store:             store,
		bundle:            bundle,
		backupCoordinator: &fakeOpsBackupCoordinator{status: metadata.BackupCoordinatorStatus{Active: true}},
		backupRepository:  fakeOpsBackupRepository{},
	}).handleBackupByID(rr, req)

	if rr.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409, body=%s", rr.Code, rr.Body.String())
	}
	var body map[string]string
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body["code"] != "backup_in_progress" {
		t.Fatalf("code = %q, want backup_in_progress", body["code"])
	}
}

func TestPrometheusBackupMetricsExcludeBackupIDLabelsAndIncludeTombstoneAge(t *testing.T) {
	backupVerificationFailuresTotal.Store(2)
	now := time.Date(2026, 7, 30, 3, 0, 0, 0, time.UTC)
	source := fakeBackupMetricsSource{
		status: metadata.BackupCoordinatorStatus{Started: true},
		tasks: []metadata.BackupTask{
			{
				ID:            "backup-20260730T010203Z-0011223344556677",
				State:         metadata.BackupTaskCommitted,
				AppliedIndex:  41,
				StartedAt:     now.Add(-32 * time.Minute),
				CompletedAt:   now.Add(-30 * time.Minute),
				BytesUploaded: 4096,
			},
			{
				ID:            "backup-20260730T020203Z-0011223344556677",
				State:         metadata.BackupTaskFailed,
				StartedAt:     now.Add(-20 * time.Minute),
				CompletedAt:   now.Add(-18 * time.Minute),
				BytesUploaded: 128,
			},
		},
		tombstones: []metadata.ChunkTombstone{
			{ChunkID: 7, Size: 512, DeletedAt: now.Add(-93700 * time.Second)},
			{ChunkID: 8, Size: 256, DeletedAt: now.Add(-30 * time.Second)},
		},
		catalog: &metadata.BackupCatalogState{
			Backups: []metadata.CommittedBackup{{
				ID:           "backup-20260730T010203Z-0011223344556677",
				CreatedAt:    now.Add(-30 * time.Minute),
				AppliedIndex: 41,
				TotalBytes:   4096,
			}},
			ReconciledAt: now.Add(-29 * time.Minute),
		},
	}
	var output bytes.Buffer

	writePrometheusBackup(context.Background(), &output, source, now)
	body := output.String()

	for _, want := range []string{
		`nufs_backup_enabled 1`,
		`nufs_backup_active 0`,
		`nufs_backup_last_success_timestamp_seconds 1785378600`,
		`nufs_backup_last_success_applied_index 41`,
		`nufs_backup_duration_seconds{state="committed"} 120`,
		`nufs_backup_duration_seconds{state="failed"} 120`,
		`nufs_backup_artifact_bytes 4096`,
		`nufs_backup_upload_failures_total 1`,
		`nufs_backup_verification_failures_total 2`,
		`nufs_backup_staging_artifacts 0`,
		`nufs_restore_verification_duration_seconds 0`,
		`nufs_restore_verification_failures_total 0`,
		`nufs_chunk_tombstones 2`,
		`nufs_chunk_tombstone_bytes 768`,
		`nufs_chunk_tombstone_backlog 2`,
		`nufs_chunk_tombstone_oldest_age_seconds 93700`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("prometheus metrics missing %q:\n%s", want, body)
		}
	}
	if strings.Contains(body, "backup_id=") || strings.Contains(body, source.tasks[0].ID) {
		t.Fatalf("prometheus metrics must not expose backup IDs as labels or values:\n%s", body)
	}
}

func TestPrometheusBackupAlertsConfigured(t *testing.T) {
	raw, err := os.ReadFile("../../deploy/monitoring/alerting-rules.yaml")
	if err != nil {
		t.Fatalf("read alerts: %v", err)
	}
	var doc struct {
		Groups []struct {
			Rules []struct {
				Alert string `yaml:"alert"`
				Expr  string `yaml:"expr"`
				For   string `yaml:"for"`
			} `yaml:"rules"`
		} `yaml:"groups"`
	}
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("parse alerts: %v", err)
	}
	want := map[string]struct {
		expr string
		for_ string
	}{
		"NUFSBackupStale": {
			expr: "time() - nufs_backup_last_success_timestamp_seconds > 4500",
			for_: "5m",
		},
		"NUFSBackupVerificationFailed": {
			expr: "increase(nufs_backup_verification_failures_total[15m]) > 0",
			for_: "1m",
		},
		"NUFSChunkTombstoneBacklog": {
			expr: "nufs_chunk_tombstone_oldest_age_seconds > 93600",
			for_: "30m",
		},
	}
	for _, group := range doc.Groups {
		for _, rule := range group.Rules {
			expected, ok := want[rule.Alert]
			if !ok {
				continue
			}
			if rule.Expr != expected.expr || rule.For != expected.for_ {
				t.Fatalf("%s = expr %q for %q, want expr %q for %q", rule.Alert, rule.Expr, rule.For, expected.expr, expected.for_)
			}
			delete(want, rule.Alert)
		}
	}
	if len(want) != 0 {
		t.Fatalf("missing backup alerts: %+v", want)
	}
}

func TestOpsBackupPruneDryRunReturnsCandidates(t *testing.T) {
	ctx := context.Background()
	store, bundle := newOpsTestStore(t)
	now := time.Date(2026, 7, 30, 4, 0, 0, 0, time.UTC)
	backups := []metadata.CommittedBackup{
		{ID: "backup-new", SourceClusterID: "cluster-a", CreatedAt: now, RaftTerm: 1, AppliedIndex: 30, TotalBytes: 300},
		{ID: "backup-mid", SourceClusterID: "cluster-a", CreatedAt: now.Add(-time.Hour), RaftTerm: 1, AppliedIndex: 20, TotalBytes: 200},
		{ID: "backup-old", SourceClusterID: "cluster-a", CreatedAt: now.Add(-2 * time.Hour), RaftTerm: 1, AppliedIndex: 10, TotalBytes: 100},
	}
	if err := store.ReplaceCommittedBackupCatalog(ctx, backups, now); err != nil {
		t.Fatalf("ReplaceCommittedBackupCatalog: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/backups/prune?dry_run=true", nil)
	rr := httptest.NewRecorder()

	(&opsHandlers{
		store:             store,
		bundle:            bundle,
		backupCoordinator: &fakeOpsBackupCoordinator{status: metadata.BackupCoordinatorStatus{Retention: 2}},
		backupRepository:  fakeOpsBackupRepository{},
	}).handleBackupPrune(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200, body=%s", rr.Code, rr.Body.String())
	}
	var body struct {
		DryRun     bool                       `json:"dry_run"`
		Retention  int                        `json:"retention"`
		Candidates []metadata.CommittedBackup `json:"deletion_candidates"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !body.DryRun || body.Retention != 2 {
		t.Fatalf("dry run response = %+v", body)
	}
	if len(body.Candidates) != 1 || body.Candidates[0].ID != "backup-old" {
		t.Fatalf("candidates = %+v, want backup-old", body.Candidates)
	}
}

type fakeOpsBackupCoordinator struct {
	status        metadata.BackupCoordinatorStatus
	triggerCalls  int
	triggerResult *metadata.BackupRunResult
	triggerErr    error
}

func (c *fakeOpsBackupCoordinator) Trigger(context.Context) (*metadata.BackupRunResult, error) {
	c.triggerCalls++
	return c.triggerResult, c.triggerErr
}

func (c *fakeOpsBackupCoordinator) Status(context.Context) metadata.BackupCoordinatorStatus {
	return c.status
}

type fakeOpsBackupRepository struct {
	fetchManifest *metadata.BackupManifest
	fetchErr      error
}

func (r fakeOpsBackupRepository) Fetch(context.Context, string, string) (*metadata.BackupManifest, error) {
	return r.fetchManifest, r.fetchErr
}

type fakeBackupMetricsSource struct {
	status     metadata.BackupCoordinatorStatus
	tasks      []metadata.BackupTask
	tombstones []metadata.ChunkTombstone
	catalog    *metadata.BackupCatalogState
}

func (s fakeBackupMetricsSource) BackupStatus(context.Context) (metadata.BackupCoordinatorStatus, bool) {
	return s.status, true
}

func (s fakeBackupMetricsSource) ListBackupTasks(context.Context, int) ([]metadata.BackupTask, error) {
	return s.tasks, nil
}

func (s fakeBackupMetricsSource) BackupCatalog(context.Context) (*metadata.BackupCatalogState, error) {
	return s.catalog, nil
}

func (s fakeBackupMetricsSource) ListChunkTombstones(context.Context, int) ([]metadata.ChunkTombstone, error) {
	return s.tombstones, nil
}
