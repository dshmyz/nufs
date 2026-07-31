package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	defaultOpsURL  = "http://127.0.0.1:8091"
	defaultTimeout = 5 * time.Second

	backupStaleAfterSeconds        = 4500
	tombstoneOldestWarningSeconds  = 93600
	defaultBackupAction            = "nufs-backup create --ops-url <leader-ops-url> --auth-token <token>"
	defaultRestoreVerificationHint = "run nufs-restore inspect/restore on the latest known-good backup and check metad logs"
)

var doctorNow = func() time.Time { return time.Now().UTC() }

type doctorStatus string

const (
	statusOK       doctorStatus = "ok"
	statusWarning  doctorStatus = "warning"
	statusCritical doctorStatus = "critical"
)

type doctorReport struct {
	Status   doctorStatus  `json:"status"`
	OpsURL   string        `json:"ops_url"`
	Checked  time.Time     `json:"checked_at"`
	Checks   []doctorCheck `json:"checks"`
	ExitCode int           `json:"exit_code"`
}

type doctorCheck struct {
	Name    string       `json:"name"`
	Status  doctorStatus `json:"status"`
	Summary string       `json:"summary"`
	Action  string       `json:"action,omitempty"`
}

type backupStatusPayload struct {
	Status struct {
		Active    bool `json:"active"`
		Retention int  `json:"retention"`
	} `json:"status"`
}

type clusterStatusPayload struct {
	IsLeader  bool   `json:"is_leader"`
	Version   string `json:"version"`
	LeaderURI string `json:"leader_uri"`
}

type opsClient struct {
	base   string
	client *http.Client
}

func main() {
	os.Exit(runDoctor(context.Background(), os.Args[1:], os.Stdout, os.Stderr))
}

func runDoctor(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("nufs-doctor", flag.ContinueOnError)
	fs.SetOutput(stderr)
	opsURL := fs.String("ops-url", defaultOpsURL, "metad ops API URL")
	jsonOut := fs.Bool("json", false, "write machine-readable JSON")
	timeout := fs.Duration("timeout", defaultTimeout, "per-command timeout")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() != 0 {
		fmt.Fprintf(stderr, "unexpected arguments: %s\n", strings.Join(fs.Args(), " "))
		return 2
	}
	if *timeout <= 0 {
		fmt.Fprintln(stderr, "--timeout must be positive")
		return 2
	}
	if err := validateOpsURL(*opsURL); err != nil {
		fmt.Fprintf(stderr, "--ops-url: %v\n", err)
		return 2
	}

	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	report := runDoctorChecks(ctx, opsClient{
		base:   strings.TrimRight(*opsURL, "/"),
		client: &http.Client{},
	}, *opsURL, doctorNow())
	if *jsonOut {
		return writeDoctorJSON(stdout, report)
	}
	writeDoctorText(stdout, report)
	return report.ExitCode
}

func runDoctorChecks(ctx context.Context, client opsClient, displayURL string, now time.Time) doctorReport {
	report := doctorReport{
		Status:  statusOK,
		OpsURL:  displayURL,
		Checked: now,
	}

	ready, readyErr := client.getStatus(ctx, "/ready")
	if readyErr != nil {
		report.add(doctorCheck{
			Name:    "ops_api",
			Status:  statusCritical,
			Summary: fmt.Sprintf("cannot reach metad ops API: %v", readyErr),
			Action:  "check metad process, --ops-url, firewall, and listen address",
		})
		report.finish()
		return report
	}
	if ready == http.StatusOK {
		report.add(doctorCheck{Name: "metad_ready", Status: statusOK, Summary: "metad reports ready"})
	} else {
		report.add(doctorCheck{
			Name:    "metad_ready",
			Status:  statusCritical,
			Summary: fmt.Sprintf("/ready returned HTTP %d", ready),
			Action:  "inspect metad startup, Raft status, and storage logs before serving traffic",
		})
	}

	var cluster clusterStatusPayload
	status, err := client.getJSON(ctx, "/api/v1/cluster/status", &cluster)
	if err != nil {
		report.add(doctorCheck{Name: "cluster_status", Status: statusWarning, Summary: fmt.Sprintf("cluster status unavailable: %v", err)})
	} else if status < 200 || status >= 300 {
		report.add(doctorCheck{Name: "cluster_status", Status: statusWarning, Summary: fmt.Sprintf("cluster status returned HTTP %d", status)})
	} else {
		role := "follower"
		if cluster.IsLeader {
			role = "leader"
		}
		summary := fmt.Sprintf("node is %s, version=%s", role, valueOrUnknown(cluster.Version))
		if cluster.LeaderURI != "" {
			summary += ", leader=" + cluster.LeaderURI
		}
		report.add(doctorCheck{Name: "cluster_status", Status: statusOK, Summary: summary})
	}

	backupEnabledFromAPI := true
	var backup backupStatusPayload
	status, err = client.getJSON(ctx, "/api/v1/backups/status", &backup)
	if err != nil {
		report.add(doctorCheck{Name: "backup_status", Status: statusWarning, Summary: fmt.Sprintf("backup status unavailable: %v", err)})
	} else if status == http.StatusServiceUnavailable {
		backupEnabledFromAPI = false
		report.add(doctorCheck{
			Name:    "backup_status",
			Status:  statusWarning,
			Summary: "backup coordinator is not configured",
			Action:  "configure metadata backups before relying on this cluster for production recovery",
		})
	} else if status < 200 || status >= 300 {
		report.add(doctorCheck{Name: "backup_status", Status: statusWarning, Summary: fmt.Sprintf("backup status returned HTTP %d", status)})
	} else if backup.Status.Active {
		report.add(doctorCheck{Name: "backup_status", Status: statusOK, Summary: fmt.Sprintf("backup is active, retention=%d", backup.Status.Retention)})
	} else {
		report.add(doctorCheck{Name: "backup_status", Status: statusOK, Summary: fmt.Sprintf("backup is idle, retention=%d", backup.Status.Retention)})
	}

	metricsText, status, err := client.getText(ctx, "/metrics")
	if err != nil {
		report.add(doctorCheck{Name: "metrics", Status: statusWarning, Summary: fmt.Sprintf("prometheus metrics unavailable: %v", err)})
		report.finish()
		return report
	}
	if status < 200 || status >= 300 {
		report.add(doctorCheck{Name: "metrics", Status: statusWarning, Summary: fmt.Sprintf("/metrics returned HTTP %d", status)})
		report.finish()
		return report
	}
	metrics := parsePrometheusScalars(metricsText)
	report.add(doctorCheck{Name: "metrics", Status: statusOK, Summary: fmt.Sprintf("read %d prometheus scalar samples", len(metrics))})
	report.add(backupFreshnessCheck(metrics, backupEnabledFromAPI, now))
	report.add(tombstoneBacklogCheck(metrics))
	report.add(restoreVerificationCheck(metrics))
	report.finish()
	return report
}

func (r *doctorReport) add(check doctorCheck) {
	r.Checks = append(r.Checks, check)
	if check.Status == statusCritical {
		r.Status = statusCritical
		return
	}
	if check.Status == statusWarning && r.Status == statusOK {
		r.Status = statusWarning
	}
}

func (r *doctorReport) finish() {
	switch r.Status {
	case statusCritical:
		r.ExitCode = 2
	case statusWarning:
		r.ExitCode = 1
	default:
		r.ExitCode = 0
	}
}

func backupFreshnessCheck(metrics map[string]float64, enabledFromAPI bool, now time.Time) doctorCheck {
	enabled, hasEnabled := metrics["nufs_backup_enabled"]
	if !enabledFromAPI || (hasEnabled && enabled == 0) {
		return doctorCheck{
			Name:    "backup_freshness",
			Status:  statusWarning,
			Summary: "metadata backup is disabled",
			Action:  "configure metadata backup repository and coordinator",
		}
	}
	active := metrics["nufs_backup_active"] > 0
	lastSuccess := int64(metrics["nufs_backup_last_success_timestamp_seconds"])
	if lastSuccess <= 0 {
		return doctorCheck{
			Name:    "backup_freshness",
			Status:  statusWarning,
			Summary: "no successful metadata backup has been recorded",
			Action:  defaultBackupAction,
		}
	}
	age := now.Sub(time.Unix(lastSuccess, 0).UTC())
	if age.Seconds() > backupStaleAfterSeconds {
		summary := fmt.Sprintf("latest successful backup is %s old", formatDuration(age))
		if active {
			summary += " and a backup is currently active"
		}
		return doctorCheck{
			Name:    "backup_freshness",
			Status:  statusWarning,
			Summary: summary,
			Action:  defaultBackupAction,
		}
	}
	summary := fmt.Sprintf("latest successful backup is %s old", formatDuration(age))
	if active {
		summary += "; backup currently active"
	}
	return doctorCheck{Name: "backup_freshness", Status: statusOK, Summary: summary}
}

func tombstoneBacklogCheck(metrics map[string]float64) doctorCheck {
	oldest := metrics["nufs_chunk_tombstone_oldest_age_seconds"]
	backlog := metrics["nufs_chunk_tombstone_backlog"]
	if oldest > tombstoneOldestWarningSeconds {
		return doctorCheck{
			Name:    "chunk_tombstones",
			Status:  statusWarning,
			Summary: fmt.Sprintf("oldest tombstone is %s old, backlog=%d", formatDuration(time.Duration(oldest)*time.Second), int64(backlog)),
			Action:  "check GC background task health and datanode deletion progress",
		}
	}
	return doctorCheck{Name: "chunk_tombstones", Status: statusOK, Summary: fmt.Sprintf("oldest tombstone age=%s, backlog=%d", formatDuration(time.Duration(oldest)*time.Second), int64(backlog))}
}

func restoreVerificationCheck(metrics map[string]float64) doctorCheck {
	failures := metrics["nufs_restore_verification_failures_total"]
	if failures > 0 {
		return doctorCheck{
			Name:    "restore_verification",
			Status:  statusWarning,
			Summary: fmt.Sprintf("restore verification failures recorded: %d", int64(failures)),
			Action:  defaultRestoreVerificationHint,
		}
	}
	backupFailures := metrics["nufs_backup_verification_failures_total"]
	if backupFailures > 0 {
		return doctorCheck{
			Name:    "restore_verification",
			Status:  statusWarning,
			Summary: fmt.Sprintf("backup verification failures recorded: %d", int64(backupFailures)),
			Action:  "verify the latest committed backup artifact and inspect backup repository access",
		}
	}
	return doctorCheck{Name: "restore_verification", Status: statusOK, Summary: "no restore or backup verification failures recorded"}
}

func (c opsClient) getStatus(ctx context.Context, path string) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+path, nil)
	if err != nil {
		return 0, err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode, nil
}

func (c opsClient) getJSON(ctx context.Context, path string, out interface{}) (int, error) {
	text, status, err := c.getText(ctx, path)
	if err != nil {
		return 0, err
	}
	if status >= 200 && status < 300 {
		if err := json.Unmarshal([]byte(text), out); err != nil {
			return status, err
		}
	}
	return status, nil
}

func (c opsClient) getText(ctx context.Context, path string) (string, int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+path, nil)
	if err != nil {
		return "", 0, err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return "", 0, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", resp.StatusCode, err
	}
	return string(data), resp.StatusCode, nil
}

func parsePrometheusScalars(text string) map[string]float64 {
	out := make(map[string]float64)
	scanner := bufio.NewScanner(strings.NewReader(text))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		name := fields[0]
		if strings.Contains(name, "{") {
			continue
		}
		value, err := strconv.ParseFloat(fields[1], 64)
		if err != nil || math.IsNaN(value) || math.IsInf(value, 0) {
			continue
		}
		out[name] = value
	}
	return out
}

func validateOpsURL(raw string) error {
	parsed, err := url.ParseRequestURI(raw)
	if err != nil {
		return err
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return fmt.Errorf("scheme must be http or https")
	}
	if parsed.Host == "" {
		return fmt.Errorf("host is required")
	}
	return nil
}

func writeDoctorJSON(stdout io.Writer, report doctorReport) int {
	enc := json.NewEncoder(stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(report); err != nil {
		return 1
	}
	return report.ExitCode
}

func writeDoctorText(stdout io.Writer, report doctorReport) {
	fmt.Fprintf(stdout, "NUFS Doctor: %s\n", strings.ToUpper(string(report.Status)))
	fmt.Fprintf(stdout, "Ops URL: %s\n", report.OpsURL)
	fmt.Fprintf(stdout, "Checked: %s\n", report.Checked.Format(time.RFC3339))
	for _, check := range report.Checks {
		fmt.Fprintf(stdout, "- %s: %s - %s\n", check.Name, strings.ToUpper(string(check.Status)), check.Summary)
		if check.Action != "" {
			fmt.Fprintf(stdout, "  action: %s\n", check.Action)
		}
	}
}

func valueOrUnknown(value string) string {
	if value == "" {
		return "unknown"
	}
	return value
}

func formatDuration(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	d = d.Round(time.Second)
	if d < time.Minute {
		return d.String()
	}
	if d < time.Hour {
		return fmt.Sprintf("%dm", int64(d.Minutes()))
	}
	return fmt.Sprintf("%dh%dm", int64(d.Hours()), int64(d.Minutes())%60)
}
