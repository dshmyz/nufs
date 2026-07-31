package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"text/tabwriter"
	"time"

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
	os.Exit(runBackupCommand(context.Background(), os.Args[1:], os.Stdout, os.Stderr))
}

func runBackupCommand(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	jsonOut, args := consumeJSONFlag(args)
	if len(args) == 0 {
		backupUsage(stderr)
		return 2
	}
	switch args[0] {
	case "create":
		return runBackupCreate(ctx, args[1:], jsonOut, stdout, stderr)
	case "list":
		return runBackupList(ctx, args[1:], jsonOut, stdout, stderr)
	case "verify":
		return runBackupVerify(ctx, args[1:], jsonOut, stdout, stderr)
	case "prune":
		return runBackupPrune(ctx, args[1:], jsonOut, stdout, stderr)
	default:
		fmt.Fprintf(stderr, "unknown command: %s\n", args[0])
		return 2
	}
}

func runBackupCreate(ctx context.Context, args []string, jsonOut bool, stdout, stderr io.Writer) int {
	fs := newFlagSet("create", stderr)
	opsURL := fs.String("ops-url", "", "metad ops API URL")
	authToken := fs.String("auth-token", "", "ops bearer token")
	if fs.Parse(args) != nil || *opsURL == "" || *authToken == "" {
		fmt.Fprintln(stderr, "usage: nufs-backup create --ops-url --auth-token")
		return 2
	}
	body, err := postOps(ctx, *opsURL, "/api/v1/backups", "", *authToken, nil)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return writeCommandBody(stdout, body, jsonOut)
}

func runBackupList(ctx context.Context, args []string, jsonOut bool, stdout, stderr io.Writer) int {
	fs := newFlagSet("list", stderr)
	configPath := fs.String("repository-config", "", "repository JSON config")
	if fs.Parse(args) != nil || *configPath == "" {
		fmt.Fprintln(stderr, "usage: nufs-backup list --repository-config")
		return 2
	}
	repo, err := openRepositoryConfig(*configPath)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	backups, err := repo.ListCommitted(ctx)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	if jsonOut {
		return writeJSON(stdout, backupDescriptorJSONRows(backups))
	}
	w := tabwriter.NewWriter(stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tCREATED\tAPPLIED_INDEX\tTOTAL_BYTES")
	for _, backup := range backups {
		fmt.Fprintf(w, "%s\t%s\t%d\t%d\n", backup.ID, backup.CreatedAt.Format(time.RFC3339), backup.AppliedIndex, backup.TotalBytes)
	}
	w.Flush()
	return 0
}

type backupDescriptorJSON struct {
	ID           string    `json:"id"`
	CreatedAt    time.Time `json:"created_at"`
	AppliedIndex uint64    `json:"applied_index"`
	TotalBytes   int64     `json:"total_bytes"`
}

func backupDescriptorJSONRows(backups []metadata.BackupDescriptor) []backupDescriptorJSON {
	rows := make([]backupDescriptorJSON, len(backups))
	for i, backup := range backups {
		rows[i] = backupDescriptorJSON{
			ID:           backup.ID,
			CreatedAt:    backup.CreatedAt,
			AppliedIndex: backup.AppliedIndex,
			TotalBytes:   backup.TotalBytes,
		}
	}
	return rows
}

func runBackupVerify(ctx context.Context, args []string, jsonOut bool, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "usage: nufs-backup verify <backup-id> --repository-config")
		return 2
	}
	backupID := args[0]
	fs := newFlagSet("verify", stderr)
	configPath := fs.String("repository-config", "", "repository JSON config")
	if fs.Parse(args[1:]) != nil || *configPath == "" {
		fmt.Fprintln(stderr, "usage: nufs-backup verify <backup-id> --repository-config")
		return 2
	}
	repo, err := openRepositoryConfig(*configPath)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	tempDir, err := os.MkdirTemp("", "nufs-backup-verify-*")
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
	report, err := metadata.VerifyBackupArtifact(ctx, tempDir, manifest)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	if jsonOut {
		return writeJSON(stdout, report)
	}
	fmt.Fprintf(stdout, "backup %s verified: %d files, %d bytes\n", backupID, report.FilesVerified, report.BytesVerified)
	return 0
}

func runBackupPrune(ctx context.Context, args []string, jsonOut bool, stdout, stderr io.Writer) int {
	fs := newFlagSet("prune", stderr)
	opsURL := fs.String("ops-url", "", "metad ops API URL")
	dryRun := fs.Bool("dry-run", false, "preview prune without deleting")
	if fs.Parse(args) != nil || *opsURL == "" || !*dryRun {
		fmt.Fprintln(stderr, "usage: nufs-backup prune --ops-url --dry-run")
		return 2
	}
	body, err := postOps(ctx, *opsURL, "/api/v1/backups/prune", "dry_run=true", "", nil)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return writeCommandBody(stdout, body, jsonOut)
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

func postOps(ctx context.Context, baseURL, path, rawQuery, token string, body io.Reader) ([]byte, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("ops-url: %w", err)
	}
	u.Path = path
	u.RawQuery = rawQuery
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), body)
	if err != nil {
		return nil, err
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return nil, readErr
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return data, fmt.Errorf("ops API returned %s: %s", resp.Status, string(data))
	}
	return data, nil
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

func writeCommandBody(stdout io.Writer, body []byte, jsonOut bool) int {
	if jsonOut {
		if len(body) == 0 {
			body = []byte("{}")
		}
		stdout.Write(body)
		if len(body) == 0 || body[len(body)-1] != '\n' {
			fmt.Fprintln(stdout)
		}
		return 0
	}
	if len(body) > 0 {
		stdout.Write(body)
		if body[len(body)-1] != '\n' {
			fmt.Fprintln(stdout)
		}
	}
	return 0
}

func backupUsage(stderr io.Writer) {
	fmt.Fprintln(stderr, "usage: nufs-backup [--json] <create|list|verify|prune>")
}
