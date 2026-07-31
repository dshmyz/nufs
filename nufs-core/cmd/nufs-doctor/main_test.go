package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestDoctorHealthyClusterExitsOK(t *testing.T) {
	now := time.Unix(1800000000, 0).UTC()
	withDoctorNow(t, now)
	server := doctorTestServer(t, doctorTestState{
		Ready:         true,
		BackupEnabled: true,
		LastSuccess:   now.Add(-30 * time.Minute).Unix(),
	})
	var stdout, stderr bytes.Buffer

	code := runDoctor(context.Background(), []string{
		"--ops-url", server.URL,
	}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit = %d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	if !strings.Contains(stdout.String(), "NUFS Doctor: OK") {
		t.Fatalf("stdout = %s, want OK summary", stdout.String())
	}
}

func TestDoctorStaleBackupWarns(t *testing.T) {
	now := time.Unix(1800000000, 0).UTC()
	withDoctorNow(t, now)
	server := doctorTestServer(t, doctorTestState{
		Ready:         true,
		BackupEnabled: true,
		LastSuccess:   now.Add(-2 * time.Hour).Unix(),
	})
	var stdout, stderr bytes.Buffer

	code := runDoctor(context.Background(), []string{
		"--ops-url", server.URL,
	}, &stdout, &stderr)
	if code != 1 {
		t.Fatalf("exit = %d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	out := stdout.String()
	if !strings.Contains(out, "NUFS Doctor: WARNING") || !strings.Contains(out, "backup_freshness") || !strings.Contains(out, "nufs-backup create") {
		t.Fatalf("stdout = %s, want stale backup warning and action", out)
	}
}

func TestDoctorNotReadyIsCritical(t *testing.T) {
	now := time.Unix(1800000000, 0).UTC()
	withDoctorNow(t, now)
	server := doctorTestServer(t, doctorTestState{
		Ready:         false,
		BackupEnabled: true,
		LastSuccess:   now.Add(-30 * time.Minute).Unix(),
	})
	var stdout, stderr bytes.Buffer

	code := runDoctor(context.Background(), []string{
		"--ops-url", server.URL,
	}, &stdout, &stderr)
	if code != 2 {
		t.Fatalf("exit = %d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	if !strings.Contains(stdout.String(), "metad_ready") || !strings.Contains(stdout.String(), "CRITICAL") {
		t.Fatalf("stdout = %s, want readiness critical", stdout.String())
	}
}

func TestDoctorJSONOutput(t *testing.T) {
	now := time.Unix(1800000000, 0).UTC()
	withDoctorNow(t, now)
	server := doctorTestServer(t, doctorTestState{
		Ready:          true,
		BackupEnabled:  true,
		LastSuccess:    now.Add(-30 * time.Minute).Unix(),
		RestoreFailure: 1,
	})
	var stdout, stderr bytes.Buffer

	code := runDoctor(context.Background(), []string{
		"--json",
		"--ops-url", server.URL,
	}, &stdout, &stderr)
	if code != 1 {
		t.Fatalf("exit = %d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	var report doctorReport
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatalf("stdout is not JSON: %v\n%s", err, stdout.String())
	}
	if report.Status != statusWarning || report.ExitCode != 1 || report.OpsURL != server.URL {
		t.Fatalf("report = %+v", report)
	}
	if len(report.Checks) == 0 || !doctorReportHasCheck(report, "restore_verification", statusWarning) {
		t.Fatalf("report checks = %+v, want restore verification warning", report.Checks)
	}
}

func TestDoctorUnreachableOpsURLIsCritical(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := runDoctor(context.Background(), []string{
		"--ops-url", "http://127.0.0.1:1",
		"--timeout", "50ms",
	}, &stdout, &stderr)
	if code != 2 {
		t.Fatalf("exit = %d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	if !strings.Contains(stdout.String(), "ops_api") || !strings.Contains(stdout.String(), "CRITICAL") {
		t.Fatalf("stdout = %s, want ops API critical", stdout.String())
	}
}

func TestParsePrometheusScalarsIgnoresLabelledSeries(t *testing.T) {
	metrics := parsePrometheusScalars(`
nufs_backup_runs_total{state="failed"} 9
nufs_backup_enabled 1
nufs_backup_active 0
`)
	if _, ok := metrics["nufs_backup_runs_total"]; ok {
		t.Fatalf("labelled metric was included: %+v", metrics)
	}
	if metrics["nufs_backup_enabled"] != 1 || metrics["nufs_backup_active"] != 0 {
		t.Fatalf("metrics = %+v", metrics)
	}
}

type doctorTestState struct {
	Ready          bool
	BackupEnabled  bool
	LastSuccess    int64
	BackupActive   bool
	TombstoneAge   int64
	RestoreFailure int64
}

func doctorTestServer(t *testing.T, state doctorTestState) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ready":
			if state.Ready {
				w.WriteHeader(http.StatusOK)
				return
			}
			w.WriteHeader(http.StatusServiceUnavailable)
		case "/api/v1/cluster/status":
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"is_leader":true,"version":"test","leader_uri":"127.0.0.1:10300"}`)
		case "/api/v1/backups/status":
			w.Header().Set("Content-Type", "application/json")
			if !state.BackupEnabled {
				w.WriteHeader(http.StatusServiceUnavailable)
				fmt.Fprint(w, `{"code":"backup_disabled","error":"backup coordinator is not configured"}`)
				return
			}
			active := "false"
			if state.BackupActive {
				active = "true"
			}
			fmt.Fprintf(w, `{"status":{"active":%s,"retention":3},"catalog":{"backups":[]}}`, active)
		case "/metrics":
			w.Header().Set("Content-Type", "text/plain; version=0.0.4")
			enabled := 0
			if state.BackupEnabled {
				enabled = 1
			}
			active := 0
			if state.BackupActive {
				active = 1
			}
			fmt.Fprintf(w, "nufs_backup_enabled %d\n", enabled)
			fmt.Fprintf(w, "nufs_backup_active %d\n", active)
			fmt.Fprintf(w, "nufs_backup_last_success_timestamp_seconds %d\n", state.LastSuccess)
			fmt.Fprintln(w, "nufs_backup_verification_failures_total 0")
			fmt.Fprintf(w, "nufs_restore_verification_failures_total %d\n", state.RestoreFailure)
			fmt.Fprintf(w, "nufs_chunk_tombstone_backlog %d\n", boolToInt(state.TombstoneAge > 0))
			fmt.Fprintf(w, "nufs_chunk_tombstone_oldest_age_seconds %d\n", state.TombstoneAge)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	return server
}

func doctorReportHasCheck(report doctorReport, name string, status doctorStatus) bool {
	for _, check := range report.Checks {
		if check.Name == name && check.Status == status {
			return true
		}
	}
	return false
}

func boolToInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func withDoctorNow(t *testing.T, now time.Time) {
	t.Helper()
	old := doctorNow
	doctorNow = func() time.Time { return now }
	t.Cleanup(func() {
		doctorNow = old
	})
}
