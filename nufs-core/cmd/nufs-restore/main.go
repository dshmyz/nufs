package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/example/dfs/metadata"
)

type repositoryConfig struct {
	Type     string `json:"type"`
	Root     string `json:"root"`
	Bucket   string `json:"bucket"`
	Prefix   string `json:"prefix"`
	Region   string `json:"region"`
	Endpoint string `json:"endpoint"`
}

func main() {
	os.Exit(runRestoreCommand(context.Background(), os.Args[1:], os.Stdout, os.Stderr))
}

func runRestoreCommand(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	jsonOut, args := consumeJSONFlag(args)
	if len(args) == 0 {
		fmt.Fprintln(stderr, "usage: nufs-restore [--json] <inspect|restore>")
		return 2
	}
	switch args[0] {
	case "inspect":
		return runRestoreInspect(ctx, args[1:], jsonOut, stdout, stderr)
	case "restore":
		return runRestoreRestore(ctx, args[1:], jsonOut, stdout, stderr)
	default:
		fmt.Fprintf(stderr, "unknown command: %s\n", args[0])
		return 2
	}
}

func runRestoreInspect(ctx context.Context, args []string, jsonOut bool, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "usage: nufs-restore inspect <backup-id> --repository-config")
		return 2
	}
	backupID := args[0]
	fs := newFlagSet("inspect", stderr)
	configPath := fs.String("repository-config", "", "repository JSON config")
	if fs.Parse(args[1:]) != nil || *configPath == "" {
		fmt.Fprintln(stderr, "usage: nufs-restore inspect <backup-id> --repository-config")
		return 2
	}
	repo, err := openRepositoryConfig(*configPath)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	tempDir, err := os.MkdirTemp("", "nufs-restore-inspect-*")
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	defer os.RemoveAll(tempDir)
	manifest, err := repo.Fetch(ctx, backupID, tempDir)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	if jsonOut {
		return writeJSON(stdout, manifest)
	}
	fmt.Fprintf(stdout, "backup %s: source_cluster=%s applied_index=%d bytes=%d\n", manifest.BackupID, manifest.SourceClusterID, manifest.AppliedIndex, manifest.TotalBytes)
	return 0
}

func runRestoreRestore(ctx context.Context, args []string, jsonOut bool, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "usage: nufs-restore restore <backup-id> --repository-config --target-dir --new-cluster-id")
		return 2
	}
	backupID := args[0]
	fs := newFlagSet("restore", stderr)
	configPath := fs.String("repository-config", "", "repository JSON config")
	targetDir := fs.String("target-dir", "", "restored Pebble target directory")
	newClusterID := fs.String("new-cluster-id", "", "new cluster ID")
	if fs.Parse(args[1:]) != nil || *configPath == "" || *targetDir == "" || *newClusterID == "" {
		fmt.Fprintln(stderr, "usage: nufs-restore restore <backup-id> --repository-config --target-dir --new-cluster-id")
		return 2
	}
	repo, err := openRepositoryConfig(*configPath)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	report, err := metadata.RestoreBackupToNewCluster(ctx, repo, metadata.RestoreOptions{
		BackupID:     backupID,
		TargetDir:    *targetDir,
		NewClusterID: *newClusterID,
	})
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	if jsonOut {
		return writeJSON(stdout, report)
	}
	fmt.Fprintf(stdout, "restored backup %s to %s as cluster %s\n", report.BackupID, *targetDir, report.NewClusterID)
	return 0
}

func openRepositoryConfig(path string) (metadata.BackupRepository, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("repository config: read: %w", err)
	}
	var cfg repositoryConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("repository config: decode: %w", err)
	}
	if cfg.Type == "" && cfg.Root != "" {
		cfg.Type = "filesystem"
	}
	switch cfg.Type {
	case "filesystem":
		return metadata.NewFilesystemBackupRepository(cfg.Root)
	case "s3":
		return metadata.NewS3BackupRepository(metadata.S3Config{
			Bucket: cfg.Bucket, Prefix: cfg.Prefix, Region: cfg.Region, Endpoint: cfg.Endpoint,
		})
	default:
		return nil, fmt.Errorf("repository config: unsupported type %q", cfg.Type)
	}
}

func consumeJSONFlag(args []string) (bool, []string) {
	out := args[:0]
	jsonOut := false
	for _, arg := range args {
		if arg == "--json" {
			jsonOut = true
			continue
		}
		out = append(out, arg)
	}
	return jsonOut, out
}

func newFlagSet(name string, stderr io.Writer) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(stderr)
	return fs
}

func writeJSON(stdout io.Writer, value interface{}) int {
	enc := json.NewEncoder(stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(value); err != nil {
		return 1
	}
	return 0
}
