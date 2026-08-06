package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/example/dfs/metadata"
)

func TestOpsHandlersWriteAttemptRoundTrip(t *testing.T) {
	ctx := context.Background()
	store, bundle := newOpsTestStore(t)

	mux := http.NewServeMux()
	registerOpsHandlers(mux, store, store, bundle, "")
	server := httptest.NewServer(mux)
	defer server.Close()

	client := metadata.NewHTTPClient(server.URL, 0)
	attempt := &metadata.ObjectWriteAttempt{
		ID:     "attempt-http-1",
		Bucket: "bucket",
		Key:    "object.txt",
		State:  metadata.WriteAttemptFailed,
	}
	if err := client.PutWriteAttempt(ctx, attempt); err != nil {
		t.Fatalf("PutWriteAttempt: %v", err)
	}
	got, err := client.GetWriteAttempt(ctx, attempt.ID)
	if err != nil {
		t.Fatalf("GetWriteAttempt: %v", err)
	}
	if got.ID != attempt.ID || got.State != metadata.WriteAttemptFailed {
		t.Fatalf("attempt = %+v", got)
	}
	listed, err := client.ListWriteAttemptsByState(ctx, metadata.WriteAttemptFailed, 10)
	if err != nil {
		t.Fatalf("ListWriteAttemptsByState: %v", err)
	}
	if len(listed) != 1 || listed[0].ID != attempt.ID {
		t.Fatalf("listed = %+v", listed)
	}
	if err := client.DeleteWriteAttempt(ctx, attempt.ID); err != nil {
		t.Fatalf("DeleteWriteAttempt: %v", err)
	}
	if _, err := client.GetWriteAttempt(ctx, attempt.ID); err != metadata.ErrEntryNotFound {
		t.Fatalf("GetWriteAttempt after delete = %v, want %v", err, metadata.ErrEntryNotFound)
	}
}

func TestOpsHandlersBackgroundTaskLeaseLifecycle(t *testing.T) {
	ctx := context.Background()
	store, bundle := newOpsTestStore(t)

	mux := http.NewServeMux()
	registerOpsHandlers(mux, store, store, bundle, "")
	server := httptest.NewServer(mux)
	defer server.Close()

	client := metadata.NewHTTPClient(server.URL, 0)
	task := &metadata.BackgroundTask{
		ID:     "task-http-1",
		Type:   metadata.TaskWriteGC,
		State:  metadata.TaskQueued,
		Target: "object-write-gc",
	}
	if err := client.PutBackgroundTask(ctx, task); err != nil {
		t.Fatalf("PutBackgroundTask: %v", err)
	}
	leased, err := client.LeaseBackgroundTask(ctx, metadata.TaskWriteGC, "worker-1", time.Minute)
	if err != nil {
		t.Fatalf("LeaseBackgroundTask: %v", err)
	}
	if leased.ID != task.ID || leased.State != metadata.TaskLeased {
		t.Fatalf("leased = %+v", leased)
	}
	if err := client.CompleteBackgroundTask(ctx, task.ID); err != nil {
		t.Fatalf("CompleteBackgroundTask: %v", err)
	}
	got, err := client.GetBackgroundTask(ctx, task.ID)
	if err != nil {
		t.Fatalf("GetBackgroundTask: %v", err)
	}
	if got.State != metadata.TaskSucceeded {
		t.Fatalf("task state = %s, want %s", got.State, metadata.TaskSucceeded)
	}
}

func TestOpsHandlersWriteOpsStatus(t *testing.T) {
	ctx := context.Background()
	store, bundle := newOpsTestStore(t)

	if err := store.PutWriteAttempt(ctx, &metadata.ObjectWriteAttempt{ID: "failed-1", State: metadata.WriteAttemptFailed}); err != nil {
		t.Fatalf("PutWriteAttempt failed: %v", err)
	}
	if err := store.PutWriteAttempt(ctx, &metadata.ObjectWriteAttempt{ID: "recover-1", State: metadata.WriteAttemptRecoveryNeeded}); err != nil {
		t.Fatalf("PutWriteAttempt recovery: %v", err)
	}
	if err := store.PutBackgroundTask(ctx, &metadata.BackgroundTask{
		ID:        "object-write-recovery-periodic",
		Type:      metadata.TaskWriteRecovery,
		State:     metadata.TaskSucceeded,
		Target:    "object-write-recovery",
		LastError: "",
	}); err != nil {
		t.Fatalf("PutBackgroundTask recovery: %v", err)
	}
	if err := store.PutBackgroundTask(ctx, &metadata.BackgroundTask{
		ID:        "object-write-gc-periodic",
		Type:      metadata.TaskWriteGC,
		State:     metadata.TaskDeadLetter,
		Target:    "object-write-gc",
		LastError: "delete failed",
	}); err != nil {
		t.Fatalf("PutBackgroundTask gc: %v", err)
	}

	mux := http.NewServeMux()
	registerOpsHandlers(mux, store, store, bundle, "")
	server := httptest.NewServer(mux)
	defer server.Close()
	client := metadata.NewHTTPClient(server.URL, 0)
	got, err := client.GetWriteOpsStatus(ctx)
	if err != nil {
		t.Fatalf("GetWriteOpsStatus: %v", err)
	}
	if got.Attempts[string(metadata.WriteAttemptFailed)] != 1 {
		t.Fatalf("failed attempts = %d, want 1", got.Attempts[string(metadata.WriteAttemptFailed)])
	}
	if got.Attempts[string(metadata.WriteAttemptRecoveryNeeded)] != 1 {
		t.Fatalf("recovery_needed attempts = %d, want 1", got.Attempts[string(metadata.WriteAttemptRecoveryNeeded)])
	}
	if got.RecoveryTask.State != metadata.TaskSucceeded {
		t.Fatalf("recovery task = %+v", got.RecoveryTask)
	}
	if got.GCTask.State != metadata.TaskDeadLetter || got.GCTask.LastError != "delete failed" {
		t.Fatalf("gc task = %+v", got.GCTask)
	}
}

func TestOpsHandlersMetricsIncludesWriteOpsStatus(t *testing.T) {
	ctx := context.Background()
	store, bundle := newOpsTestStore(t)
	if err := store.PutWriteAttempt(ctx, &metadata.ObjectWriteAttempt{ID: "failed-1", State: metadata.WriteAttemptFailed}); err != nil {
		t.Fatalf("PutWriteAttempt: %v", err)
	}

	mux := http.NewServeMux()
	registerOpsHandlers(mux, store, store, bundle, "")
	req := httptest.NewRequest(http.MethodGet, "/api/v1/metrics", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status code = %d body=%s", rr.Code, rr.Body.String())
	}
	var got map[string]json.RawMessage
	if err := json.NewDecoder(rr.Body).Decode(&got); err != nil {
		t.Fatalf("decode metrics: %v", err)
	}
	raw, ok := got["object_write_ops"]
	if !ok {
		t.Fatalf("metrics missing object_write_ops: %s", rr.Body.String())
	}
	var status metadata.WriteOpsStatus
	if err := json.Unmarshal(raw, &status); err != nil {
		t.Fatalf("decode write_ops: %v", err)
	}
	if status.Attempts[string(metadata.WriteAttemptFailed)] != 1 {
		t.Fatalf("failed attempts = %d, want 1", status.Attempts[string(metadata.WriteAttemptFailed)])
	}
}

func TestPrometheusMetricsIncludesObjectWriteOps(t *testing.T) {
	ctx := context.Background()
	store, bundle := newOpsTestStore(t)
	if err := store.PutWriteAttempt(ctx, &metadata.ObjectWriteAttempt{ID: "failed-1", State: metadata.WriteAttemptFailed}); err != nil {
		t.Fatalf("PutWriteAttempt failed: %v", err)
	}
	if err := store.PutWriteAttempt(ctx, &metadata.ObjectWriteAttempt{ID: "recover-1", State: metadata.WriteAttemptRecoveryNeeded}); err != nil {
		t.Fatalf("PutWriteAttempt recovery: %v", err)
	}
	if err := store.PutBackgroundTask(ctx, &metadata.BackgroundTask{
		ID:           "object-write-gc-periodic",
		Type:         metadata.TaskWriteGC,
		State:        metadata.TaskDeadLetter,
		Target:       "object-write-gc",
		AttemptCount: 3,
	}); err != nil {
		t.Fatalf("PutBackgroundTask gc: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rr := httptest.NewRecorder()
	prometheusMetricsHandler(store, bundle.Metrics).ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status code = %d body=%s", rr.Code, rr.Body.String())
	}
	body := rr.Body.String()
	for _, want := range []string{
		`# HELP nufs_object_write_attempts Object write attempts by state`,
		`nufs_object_write_attempts{state="failed"} 1`,
		`nufs_object_write_attempts{state="recovery_needed"} 1`,
		`nufs_object_write_background_task_state{task="gc",state="dead_letter"} 1`,
		`nufs_object_write_background_task_attempts{task="gc"} 3`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("prometheus metrics missing %q:\n%s", want, body)
		}
	}
}

func newOpsTestStore(t *testing.T) (*metadata.PebbleStore, *metadata.ServiceBundle) {
	t.Helper()

	store, err := metadata.NewPebbleStore(metadata.PebbleStoreConfig{
		Dir:         t.TempDir(),
		UseInMemory: true,
		NodeID:      1,
	})
	if err != nil {
		t.Fatalf("NewPebbleStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	bundle, err := metadata.NewPebbleServiceBundle(
		store,
		metadata.WithLeaseTTL(0),
		metadata.WithGCInterval(0),
		metadata.WithScrubInterval(0),
	)
	if err != nil {
		t.Fatalf("NewPebbleServiceBundle: %v", err)
	}
	t.Cleanup(func() { _ = bundle.Close() })
	return store, bundle
}
