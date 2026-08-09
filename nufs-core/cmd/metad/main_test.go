package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dshmyz/nufs/nufs-core/metadata"
)

func validBackupRuntimeConfig(dir string) backupRuntimeConfig {
	return backupRuntimeConfig{
		Enabled:       true,
		RaftEnabled:   true,
		ClusterID:     "cluster-prod-1",
		LocalDir:      dir,
		Interval:      time.Hour,
		Retention:     24,
		S3Bucket:      "metadata-backups",
		S3Prefix:      "nufs/metadata/",
		S3Region:      "us-east-1",
		S3Endpoint:    "https://s3.example.com",
		UploadTimeout: 10 * time.Minute,
		StagingMaxAge: 24 * time.Hour,
	}
}

func TestBackupConfigDisabledHasNoFilesystemSideEffects(t *testing.T) {
	target := filepath.Join(t.TempDir(), "must-not-exist")
	cfg := validBackupRuntimeConfig(target)
	cfg.Enabled = false
	normalized, err := validateBackupRuntimeConfig(cfg)
	if err != nil {
		t.Fatalf("validate disabled config: %v", err)
	}
	if normalized.Enabled {
		t.Fatal("disabled config became enabled")
	}
	if _, err := os.Stat(target); !os.IsNotExist(err) {
		t.Fatalf("disabled validation touched local directory: %v", err)
	}
}

func TestBackupConfigValidatesAndNormalizes(t *testing.T) {
	target := filepath.Join(t.TempDir(), "backup-temp")
	normalized, err := validateBackupRuntimeConfig(validBackupRuntimeConfig(target))
	if err != nil {
		t.Fatalf("validate config: %v", err)
	}
	if normalized.S3Prefix != "nufs/metadata" {
		t.Fatalf("prefix = %q", normalized.S3Prefix)
	}
	if info, err := os.Stat(target); err != nil || !info.IsDir() {
		t.Fatalf("temp directory was not created: info=%v err=%v", info, err)
	}
	entries, err := os.ReadDir(target)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("writability probe leaked files: %v", entries)
	}
}

func TestBackupConfigRejectsInvalidValues(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*backupRuntimeConfig)
		want   string
	}{
		{"cluster", func(c *backupRuntimeConfig) { c.ClusterID = "../bad" }, "cluster"},
		{"bucket", func(c *backupRuntimeConfig) { c.S3Bucket = "" }, "bucket"},
		{"interval", func(c *backupRuntimeConfig) { c.Interval = 0 }, "interval"},
		{"retention", func(c *backupRuntimeConfig) { c.Retention = 0 }, "retention"},
		{"timeout", func(c *backupRuntimeConfig) { c.UploadTimeout = 0 }, "timeout"},
		{"staging age", func(c *backupRuntimeConfig) { c.StagingMaxAge = 0 }, "staging"},
		{"local dir", func(c *backupRuntimeConfig) { c.LocalDir = "" }, "local"},
		{"prefix", func(c *backupRuntimeConfig) { c.S3Prefix = "../bad" }, "prefix"},
		{"endpoint", func(c *backupRuntimeConfig) { c.S3Endpoint = "://bad" }, "endpoint"},
		{"raft disabled", func(c *backupRuntimeConfig) { c.RaftEnabled = false }, "raft"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := validBackupRuntimeConfig(filepath.Join(t.TempDir(), "tmp"))
			test.mutate(&cfg)
			if _, err := validateBackupRuntimeConfig(cfg); err == nil ||
				!strings.Contains(strings.ToLower(err.Error()), test.want) {
				t.Fatalf("error = %v, want substring %q", err, test.want)
			}
		})
	}
}

func TestBackupConfigRejectsUnwritableLocalPath(t *testing.T) {
	file := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := validBackupRuntimeConfig(file)
	if _, err := validateBackupRuntimeConfig(cfg); err == nil {
		t.Fatal("expected non-directory local path to fail")
	}
}

type fakeBackupLifecycle struct {
	started bool
	stopped bool
}

func (f *fakeBackupLifecycle) Start() { f.started = true }
func (f *fakeBackupLifecycle) Stop()  { f.stopped = true }

type inertBackupRepository struct{}

func (inertBackupRepository) Publish(context.Context, string, *metadata.BackupManifest) error {
	return nil
}
func (inertBackupRepository) ListCommitted(context.Context) ([]metadata.BackupDescriptor, error) {
	return nil, nil
}
func (inertBackupRepository) Fetch(context.Context, string, string) (*metadata.BackupManifest, error) {
	return nil, nil
}
func (inertBackupRepository) Delete(context.Context, string) error { return nil }
func (inertBackupRepository) DeleteStagingOlderThan(context.Context, time.Time) error {
	return nil
}

func TestBackupConfigDisabledDoesNotCreateRuntimeDependencies(t *testing.T) {
	repositoryCalls := 0
	coordinatorCalls := 0
	lifecycle, err := createBackupCoordinatorRuntime(
		backupRuntimeConfig{},
		nil,
		func(metadata.S3Config) (metadata.BackupRepository, error) {
			repositoryCalls++
			return inertBackupRepository{}, nil
		},
		func(metadata.BackupCoordinatorConfig, *metadata.PebbleStore, metadata.BackupRepository) backupCoordinatorLifecycle {
			coordinatorCalls++
			return &fakeBackupLifecycle{}
		},
	)
	if err != nil {
		t.Fatalf("create disabled runtime: %v", err)
	}
	if lifecycle != nil || repositoryCalls != 0 || coordinatorCalls != 0 {
		t.Fatalf("disabled runtime created dependencies: lifecycle=%v repository=%d coordinator=%d", lifecycle, repositoryCalls, coordinatorCalls)
	}
}

func TestBackupConfigBuildsCoordinatorWithoutStartingIt(t *testing.T) {
	cfg := validBackupRuntimeConfig(t.TempDir())
	lifecycle := &fakeBackupLifecycle{}
	var gotS3 metadata.S3Config
	var gotCoordinator metadata.BackupCoordinatorConfig
	runtime, err := createBackupCoordinatorRuntime(
		cfg,
		nil,
		func(s3cfg metadata.S3Config) (metadata.BackupRepository, error) {
			gotS3 = s3cfg
			return inertBackupRepository{}, nil
		},
		func(coordinatorCfg metadata.BackupCoordinatorConfig, _ *metadata.PebbleStore, _ metadata.BackupRepository) backupCoordinatorLifecycle {
			gotCoordinator = coordinatorCfg
			return lifecycle
		},
	)
	if err != nil {
		t.Fatalf("create runtime: %v", err)
	}
	if runtime != lifecycle || lifecycle.started {
		t.Fatal("runtime creation must not start the coordinator")
	}
	if gotS3.Bucket != cfg.S3Bucket || gotS3.Prefix != cfg.S3Prefix ||
		gotCoordinator.ClusterID != cfg.ClusterID || gotCoordinator.Retention != cfg.Retention {
		t.Fatalf("wiring mismatch: s3=%+v coordinator=%+v", gotS3, gotCoordinator)
	}
	runtime.Start()
	runtime.Stop()
	if !lifecycle.started || !lifecycle.stopped {
		t.Fatal("lifecycle did not receive Start/Stop")
	}
}
