package metadata

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
)

func TestDisasterDrillBackupRestoreSelectsLatestAndRecordsRecoveryMetrics(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 7, 30, 12, 0, 0, 0, time.UTC)
	tempRoot := t.TempDir()
	repo := &drillBackupRepository{
		committed: []BackupDescriptor{
			{ID: "backup-old", CreatedAt: now.Add(-2 * time.Hour), AppliedIndex: 10},
			{ID: "backup-latest", CreatedAt: now.Add(-12 * time.Minute), AppliedIndex: 20},
		},
	}
	production := newTestPebbleStore(t)
	restoreProbe := drillReplicaProbe{reachable: map[ChunkID]int{42: 1}}
	var restoredPath string
	var restoredStoreOpened bool

	runner := NewDisasterDrillRunner(production, DisasterDrillConfig{
		Scenarios:              []DrillScenario{DrillBackupRestore},
		BackupRepository:       repo,
		RestoreTempRoot:        tempRoot,
		RestoreNewClusterID:    "cluster-restore-drill",
		RestoreMinimumReplicas: 1,
		RestoreReplicaProbe:    restoreProbe,
		Now:                    func() time.Time { return now },
		RestoreEngine: func(ctx context.Context, repository BackupRepository, opts RestoreOptions) (*RestoreReport, error) {
			if opts.BackupID != "backup-latest" {
				t.Fatalf("restore used backup %q, want latest committed backup", opts.BackupID)
			}
			if opts.NewClusterID != "cluster-restore-drill" {
				t.Fatalf("restore new cluster ID = %q", opts.NewClusterID)
			}
			if err := os.MkdirAll(opts.TargetDir, 0o700); err != nil {
				t.Fatal(err)
			}
			seedRestoreReadinessStore(t, opts.TargetDir, 42)
			return &RestoreReport{
				BackupID:     opts.BackupID,
				StartedAt:    now.Add(-90 * time.Second),
				CompletedAt:  now.Add(-30 * time.Second),
				AppliedIndex: 20,
				Verification: BackupVerificationReport{ManifestValid: true, FilesVerified: 1},
			}, nil
		},
		OpenRestoredStore: func(path string) (*PebbleStore, error) {
			restoredStoreOpened = true
			restoredPath = path
			return NewPebbleStore(PebbleStoreConfig{Dir: path, NodeID: 99})
		},
	})

	report := runner.RunScenario(ctx, DrillBackupRestore)
	if report.Status != DrillPassed {
		t.Fatalf("status = %s message=%s checks=%+v", report.Status, report.Message, report.Checks)
	}
	if !restoredStoreOpened {
		t.Fatal("restore drill did not open the restored store")
	}
	if restoredPath == "" || !filepath.IsAbs(restoredPath) {
		t.Fatalf("restored path = %q, want absolute temp path", restoredPath)
	}
	if entries, err := os.ReadDir(tempRoot); err != nil {
		t.Fatal(err)
	} else if len(entries) != 0 {
		t.Fatalf("restore drill left temporary entries behind: %+v", entries)
	}
	assertDrillCheckValue(t, report, "observed_rpo_seconds", 720)
	assertDrillCheckValue(t, report, "observed_rto_seconds", 60)
	assertDrillCheckPassed(t, report, "backup_artifact_verified")
	assertDrillCheckPassed(t, report, "restored_replica_readiness")
}

func TestDisasterDrillBackupRestoreReportsFailureWithoutMutatingProductionMetadata(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 7, 30, 12, 0, 0, 0, time.UTC)
	production := newTestPebbleStore(t)
	if err := production.CreateBucket(ctx, "survivor", PlacementPolicy{}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	repo := &drillBackupRepository{
		committed: []BackupDescriptor{{ID: "backup-latest", CreatedAt: now.Add(-time.Minute), AppliedIndex: 20}},
	}
	runner := NewDisasterDrillRunner(production, DisasterDrillConfig{
		Scenarios:              []DrillScenario{DrillBackupRestore},
		BackupRepository:       repo,
		RestoreTempRoot:        t.TempDir(),
		RestoreNewClusterID:    "cluster-restore-drill",
		RestoreMinimumReplicas: 1,
		RestoreReplicaProbe:    drillReplicaProbe{},
		Now:                    func() time.Time { return now },
		RestoreEngine: func(context.Context, BackupRepository, RestoreOptions) (*RestoreReport, error) {
			return nil, errors.New("fetch failed")
		},
	})

	report := runner.RunScenario(ctx, DrillBackupRestore)
	if report.Status != DrillFailed {
		t.Fatalf("status = %s, want failed: %+v", report.Status, report)
	}
	if _, err := production.GetBucket(ctx, "survivor"); err != nil {
		t.Fatalf("production metadata changed after failed drill: %v", err)
	}
}

func TestDailyRestoreVerificationDrillConfigSchedulesBackupRestoreOnly(t *testing.T) {
	cfg := DailyRestoreVerificationDrillConfig(nil, nil, "/tmp/nufs-drill", "cluster-restore")
	if cfg.ScheduleInterval != 24*time.Hour {
		t.Fatalf("ScheduleInterval = %s, want 24h", cfg.ScheduleInterval)
	}
	if len(cfg.Scenarios) != 1 || cfg.Scenarios[0] != DrillBackupRestore {
		t.Fatalf("Scenarios = %+v, want only backup_restore", cfg.Scenarios)
	}
}

type drillBackupRepository struct {
	committed []BackupDescriptor
}

func (r *drillBackupRepository) Publish(context.Context, string, *BackupManifest) error {
	return nil
}

func (r *drillBackupRepository) ListCommitted(context.Context) ([]BackupDescriptor, error) {
	return append([]BackupDescriptor(nil), r.committed...), nil
}

func (r *drillBackupRepository) Fetch(context.Context, string, string) (*BackupManifest, error) {
	return nil, errors.New("unexpected fetch from fake drill repository")
}

func (r *drillBackupRepository) Delete(context.Context, string) error {
	return nil
}

func (r *drillBackupRepository) DeleteStagingOlderThan(context.Context, time.Time) error {
	return nil
}

type drillReplicaProbe struct {
	reachable map[ChunkID]int
}

func (p drillReplicaProbe) ReachableReplicas(_ context.Context, chunk *ChunkMeta) (int, error) {
	return p.reachable[chunk.ID], nil
}

func assertDrillCheckPassed(t *testing.T, report DrillReport, name string) {
	t.Helper()
	for _, check := range report.Checks {
		if check.Name == name {
			if !check.Passed {
				t.Fatalf("check %q did not pass: %+v", name, check)
			}
			return
		}
	}
	t.Fatalf("missing check %q in %+v", name, report.Checks)
}

func assertDrillCheckValue(t *testing.T, report DrillReport, name string, want float64) {
	t.Helper()
	for _, check := range report.Checks {
		if check.Name == name {
			if check.ValueSeconds != want {
				t.Fatalf("check %q value = %v, want %v", name, check.ValueSeconds, want)
			}
			return
		}
	}
	t.Fatalf("missing check %q in %+v", name, report.Checks)
}

func seedRestoreReadinessStore(t *testing.T, dir string, chunkID ChunkID) {
	t.Helper()
	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	writeFixtureValue(t, db, prefixInode+"1", &InodeMeta{ID: RootInodeID, Type: FileDirectory})
	writeFixtureValue(t, db, prefixChunk+"42", &ChunkMeta{
		ID:    chunkID,
		State: ChunkReady,
		Replicas: []ReplicaInfo{
			{NodeID: 1, Addr: "node-1:9100", State: ReplicaReady},
		},
	})
}
