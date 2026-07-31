package metadata

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/hashicorp/raft"
)

func TestBackupTaskStateMachineAndTerminalIdempotency(t *testing.T) {
	store := newBackupCatalogTestStore(t, t.TempDir())
	defer store.Close()

	ctx := context.Background()
	base := time.Date(2026, 7, 29, 1, 0, 0, 0, time.FixedZone("CST", 8*60*60))
	task := validBackupTask("backup-001", base)

	skipped := task
	skipped.State = BackupTaskUploading
	if err := store.PutBackupTask(ctx, &skipped); err == nil {
		t.Fatal("missing task accepted a non-creating state")
	}

	if err := store.PutBackupTask(ctx, &task); err != nil {
		t.Fatalf("put creating task: %v", err)
	}
	if !task.UpdatedAt.IsZero() {
		t.Fatal("PutBackupTask mutated the caller's UpdatedAt")
	}

	progress := task
	progress.BytesUploaded = 10
	progress.FilesUploaded = 1
	progress.UpdatedAt = base.Add(time.Minute)
	if err := store.PutBackupTask(ctx, &progress); err != nil {
		t.Fatalf("update creating progress: %v", err)
	}

	decreasing := progress
	decreasing.BytesUploaded--
	decreasing.UpdatedAt = base.Add(2 * time.Minute)
	if err := store.PutBackupTask(ctx, &decreasing); err == nil {
		t.Fatal("decreasing progress was accepted")
	}

	skipped = progress
	skipped.State = BackupTaskVerifying
	skipped.UpdatedAt = base.Add(2 * time.Minute)
	if err := store.PutBackupTask(ctx, &skipped); err == nil {
		t.Fatal("skipped transition was accepted")
	}

	uploading := progress
	uploading.State = BackupTaskUploading
	uploading.BytesUploaded = 20
	uploading.FilesUploaded = 2
	uploading.UpdatedAt = base.Add(2 * time.Minute)
	if err := store.PutBackupTask(ctx, &uploading); err != nil {
		t.Fatalf("transition to uploading: %v", err)
	}

	backward := uploading
	backward.State = BackupTaskCreating
	backward.UpdatedAt = base.Add(3 * time.Minute)
	if err := store.PutBackupTask(ctx, &backward); err == nil {
		t.Fatal("backward transition was accepted")
	}

	changedIdentity := uploading
	changedIdentity.OwnerNodeID = "node-2"
	changedIdentity.UpdatedAt = base.Add(3 * time.Minute)
	if err := store.PutBackupTask(ctx, &changedIdentity); err == nil {
		t.Fatal("identity mutation was accepted")
	}

	verifying := uploading
	verifying.State = BackupTaskVerifying
	verifying.UpdatedAt = base.Add(3 * time.Minute)
	if err := store.PutBackupTask(ctx, &verifying); err != nil {
		t.Fatalf("transition to verifying: %v", err)
	}

	committed := verifying
	committed.State = BackupTaskCommitted
	committed.CompletedAt = base.Add(4 * time.Minute)
	committed.UpdatedAt = base.Add(4 * time.Minute)
	if err := store.PutBackupTask(ctx, &committed); err != nil {
		t.Fatalf("transition to committed: %v", err)
	}

	retry := committed
	retry.StartedAt = retry.StartedAt.In(time.UTC)
	retry.CompletedAt = retry.CompletedAt.In(time.UTC)
	retry.UpdatedAt = time.Time{}
	if err := store.PutBackupTask(ctx, &retry); err != nil {
		t.Fatalf("identical terminal retry: %v", err)
	}

	mutatedTerminal := committed
	mutatedTerminal.BytesUploaded++
	mutatedTerminal.UpdatedAt = base.Add(5 * time.Minute)
	if err := store.PutBackupTask(ctx, &mutatedTerminal); err == nil {
		t.Fatal("terminal task mutation was accepted")
	}

	failed := validBackupTask("backup-002", base.Add(time.Hour))
	if err := store.PutBackupTask(ctx, &failed); err != nil {
		t.Fatalf("put second creating task: %v", err)
	}
	failed.State = BackupTaskFailed
	failed.LastError = "upload failed"
	failed.CompletedAt = failed.StartedAt.Add(time.Minute)
	failed.UpdatedAt = failed.CompletedAt
	if err := store.PutBackupTask(ctx, &failed); err != nil {
		t.Fatalf("active to failed: %v", err)
	}

	failedRetry := failed
	failedRetry.UpdatedAt = time.Time{}
	if err := store.PutBackupTask(ctx, &failedRetry); err != nil {
		t.Fatalf("failed terminal retry: %v", err)
	}
}

func TestBackupMetadataAtTermRejectsTaskAndCatalogInNewerLogTerm(t *testing.T) {
	store := newTestPebbleStore(t)
	defer func() {
		store.raft = nil
		store.Close()
	}()
	fsm := &PebbleFSM{store: store}
	var index uint64
	store.raft = &RaftNode{
		conditionalLeaderHook: func() bool { return true },
		conditionalApplyHook: func(data []byte, _ time.Duration) raft.ApplyFuture {
			index++
			future := newControlledConditionalFuture()
			future.Resolve(nil, fsm.Apply(&raft.Log{Index: index, Term: 8, Data: data}))
			return future
		},
	}

	task := validBackupTask("backup-term-fenced-task", testBackupTime())
	if err := store.putBackupTaskAtTerm(context.Background(), &task, 7); !errors.Is(err, ErrBackupMetadataConflict) {
		t.Fatalf("put task at stale term error = %v, want conflict", err)
	}
	assertRawMissing(t, store, prefixBackupTask+task.ID)

	entry := validCommittedBackup("backup-term-fenced-catalog", testBackupTime())
	if err := store.replaceCommittedBackupCatalogAtTerm(
		context.Background(),
		[]CommittedBackup{entry},
		testBackupTime(),
		7,
	); !errors.Is(err, ErrBackupMetadataConflict) {
		t.Fatalf("replace catalog at stale term error = %v, want conflict", err)
	}
	assertRawMissing(t, store, keyBackupCatalog)
	assertRawMissing(t, store, prefixBackupCatalog+entry.ID)
}

func TestScanActiveBackupTasksScansPastNewest1000(t *testing.T) {
	store := newBackupCatalogTestStore(t, t.TempDir())
	defer store.Close()

	base := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	orphan := validBackupTask("orphan-old", base)
	orphan.State = BackupTaskUploading
	orphan.UpdatedAt = base
	writeTask := func(task BackupTask) {
		t.Helper()
		data, err := marshalValue(&task, codecMsgpack)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.db.Set([]byte(prefixBackupTask+task.ID), data, pebble.NoSync); err != nil {
			t.Fatal(err)
		}
	}
	writeTask(orphan)
	for i := 0; i < 1001; i++ {
		at := base.Add(time.Duration(i+1) * time.Minute)
		task := validBackupTask(fmt.Sprintf("terminal-%04d", i), at)
		task.State = BackupTaskFailed
		task.CompletedAt = at
		task.UpdatedAt = at
		task.LastError = "terminal"
		writeTask(task)
	}

	var active []BackupTask
	err := store.ScanActiveBackupTasks(context.Background(), func(task BackupTask) error {
		active = append(active, task)
		return nil
	})
	if err != nil {
		t.Fatalf("ScanActiveBackupTasks: %v", err)
	}
	if len(active) != 1 || active[0].ID != orphan.ID {
		t.Fatalf("active tasks = %+v", active)
	}
}

func TestBackupTaskConcurrentTerminalTransitionsSerialize(t *testing.T) {
	store := newBackupCatalogTestStore(t, t.TempDir())
	defer store.Close()

	ctx := context.Background()
	base := time.Date(2026, 7, 29, 2, 0, 0, 0, time.UTC)
	task := validBackupTask("backup-race", base)
	if err := store.PutBackupTask(ctx, &task); err != nil {
		t.Fatalf("put creating task: %v", err)
	}
	task.State = BackupTaskUploading
	task.UpdatedAt = base.Add(time.Minute)
	if err := store.PutBackupTask(ctx, &task); err != nil {
		t.Fatalf("put uploading task: %v", err)
	}
	task.State = BackupTaskVerifying
	task.UpdatedAt = base.Add(2 * time.Minute)
	if err := store.PutBackupTask(ctx, &task); err != nil {
		t.Fatalf("put verifying task: %v", err)
	}

	committed := task
	committed.State = BackupTaskCommitted
	committed.CompletedAt = base.Add(3 * time.Minute)
	committed.UpdatedAt = committed.CompletedAt

	failed := task
	failed.State = BackupTaskFailed
	failed.LastError = "verification failed"
	failed.CompletedAt = base.Add(3 * time.Minute)
	failed.UpdatedAt = failed.CompletedAt

	start := make(chan struct{})
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for _, candidate := range []*BackupTask{&committed, &failed} {
		candidate := candidate
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			errs <- store.PutBackupTask(ctx, candidate)
		}()
	}
	close(start)
	wg.Wait()
	close(errs)

	var successes int
	for err := range errs {
		if err == nil {
			successes++
		}
	}
	if successes != 1 {
		t.Fatalf("successful terminal transitions = %d, want 1", successes)
	}
}

func TestBackupTaskListOrderingLimitAndDetachedResults(t *testing.T) {
	store := newBackupCatalogTestStore(t, t.TempDir())
	defer store.Close()

	ctx := context.Background()
	base := time.Date(2026, 7, 29, 3, 0, 0, 0, time.UTC)
	for _, task := range []BackupTask{
		validBackupTask("backup-b", base.Add(time.Hour)),
		validBackupTask("backup-a", base.Add(time.Hour)),
		validBackupTask("backup-c", base),
	} {
		task := task
		if err := store.PutBackupTask(ctx, &task); err != nil {
			t.Fatalf("put task %q: %v", task.ID, err)
		}
	}

	got, err := store.ListBackupTasks(ctx, 2)
	if err != nil {
		t.Fatalf("list tasks: %v", err)
	}
	if ids := backupTaskIDs(got); !reflect.DeepEqual(ids, []string{"backup-a", "backup-b"}) {
		t.Fatalf("task order = %v", ids)
	}
	if got[0].StartedAt.Location() != time.UTC || got[0].UpdatedAt.Location() != time.UTC {
		t.Fatal("returned task timestamps are not UTC")
	}
	got[0].ID = "mutated"

	again, err := store.ListBackupTasks(ctx, 3)
	if err != nil {
		t.Fatalf("list tasks again: %v", err)
	}
	if again[0].ID != "backup-a" {
		t.Fatal("returned task slice aliases durable state")
	}

	for _, limit := range []int{-1, 0, maxBackupTaskListLimit + 1} {
		if _, err := store.ListBackupTasks(ctx, limit); err == nil {
			t.Fatalf("limit %d was accepted", limit)
		}
	}
}

func TestBackupTaskTopKStaysBoundedAndOrdersNewestFirst(t *testing.T) {
	base := time.Date(2026, 7, 29, 3, 0, 0, 0, time.UTC)
	top := newBackupTaskTopK(3)
	for i := 0; i < 20; i++ {
		task := validBackupTask(fmt.Sprintf("backup-top-%02d", i), base.Add(time.Duration(i%7)*time.Minute))
		top.Add(task)
		if top.Len() > 3 {
			t.Fatalf("top-k retained %d tasks, want at most 3", top.Len())
		}
	}
	got := top.Sorted()
	if len(got) != 3 {
		t.Fatalf("top-k returned %d tasks", len(got))
	}
	for i := 1; i < len(got); i++ {
		if backupTaskNewer(got[i], got[i-1]) {
			t.Fatalf("top-k order = %v", backupTaskIDs(got))
		}
	}
}

func TestBackupTaskListChecksContextDuringScan(t *testing.T) {
	store := newBackupCatalogTestStore(t, t.TempDir())
	defer store.Close()
	base := time.Date(2026, 7, 29, 3, 0, 0, 0, time.UTC)
	for i := 0; i < 20; i++ {
		task := validBackupTask(fmt.Sprintf("backup-context-%02d", i), base.Add(time.Duration(i)*time.Minute))
		if err := store.PutBackupTask(context.Background(), &task); err != nil {
			t.Fatalf("seed task %d: %v", i, err)
		}
	}

	ctx := &cancelAfterChecksContext{Context: context.Background(), remaining: 6}
	if _, err := store.ListBackupTasks(ctx, 3); !errors.Is(err, context.Canceled) {
		t.Fatalf("ListBackupTasks error = %v, want context cancellation during scan", err)
	}
}

func TestBackupTaskValidation(t *testing.T) {
	base := time.Date(2026, 7, 29, 4, 0, 0, 0, time.UTC)
	tests := map[string]func(*BackupTask){
		"invalid backup ID":      func(v *BackupTask) { v.ID = "../escape" },
		"missing source cluster": func(v *BackupTask) { v.SourceClusterID = "" },
		"missing owner":          func(v *BackupTask) { v.OwnerNodeID = "" },
		"zero term":              func(v *BackupTask) { v.LeadershipTerm = 0 },
		"zero index":             func(v *BackupTask) { v.AppliedIndex = 0 },
		"missing start":          func(v *BackupTask) { v.StartedAt = time.Time{} },
		"negative bytes":         func(v *BackupTask) { v.BytesUploaded = -1 },
		"negative files":         func(v *BackupTask) { v.FilesUploaded = -1 },
		"active completed":       func(v *BackupTask) { v.CompletedAt = base.Add(time.Minute) },
		"active error":           func(v *BackupTask) { v.LastError = "unexpected" },
		"failed without error":   func(v *BackupTask) { v.State = BackupTaskFailed; v.CompletedAt = base.Add(time.Minute) },
		"oversized error": func(v *BackupTask) {
			v.State = BackupTaskFailed
			v.CompletedAt = base.Add(time.Minute)
			v.LastError = strings.Repeat("x", maxBackupTaskErrorBytes+1)
		},
		"committed with error": func(v *BackupTask) {
			v.State = BackupTaskCommitted
			v.CompletedAt = base.Add(time.Minute)
			v.LastError = "unexpected"
		},
		"terminal without finish": func(v *BackupTask) { v.State = BackupTaskCommitted },
		"finish before start": func(v *BackupTask) {
			v.State = BackupTaskFailed
			v.LastError = "failed"
			v.CompletedAt = base.Add(-time.Second)
		},
		"updated before started":   func(v *BackupTask) { v.UpdatedAt = base.Add(-time.Second) },
		"unknown state":            func(v *BackupTask) { v.State = BackupTaskState("unknown") },
		"unsafe source cluster ID": func(v *BackupTask) { v.SourceClusterID = "cluster/escape" },
	}

	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			store := newBackupCatalogTestStore(t, t.TempDir())
			defer store.Close()
			task := validBackupTask("backup-invalid", base)
			mutate(&task)
			if err := store.PutBackupTask(context.Background(), &task); err == nil {
				t.Fatal("invalid task was accepted")
			}
		})
	}
}

func TestBackupCatalogReplacementOrderingDeletionAndDetachedResults(t *testing.T) {
	store := newBackupCatalogTestStore(t, t.TempDir())
	defer store.Close()

	ctx := context.Background()
	base := time.Date(2026, 7, 29, 5, 0, 0, 0, time.FixedZone("west", -7*60*60))
	first := []CommittedBackup{
		validCommittedBackup("backup-b", base.Add(time.Hour)),
		validCommittedBackup("backup-a", base.Add(time.Hour)),
		validCommittedBackup("backup-old", base),
	}
	reconciled := base.Add(2 * time.Hour)
	if err := store.ReplaceCommittedBackupCatalog(ctx, first, reconciled); err != nil {
		t.Fatalf("replace catalog: %v", err)
	}
	if first[0].ID != "backup-b" {
		t.Fatal("catalog replacement mutated caller ordering")
	}

	state, err := store.GetBackupCatalogState(ctx)
	if err != nil {
		t.Fatalf("get catalog: %v", err)
	}
	if ids := committedBackupIDs(state.Backups); !reflect.DeepEqual(ids, []string{"backup-a", "backup-b", "backup-old"}) {
		t.Fatalf("catalog order = %v", ids)
	}
	if state.ReconciledAt.Location() != time.UTC {
		t.Fatal("catalog timestamp was not normalized to UTC")
	}
	state.Backups[0].ID = "mutated"

	again, err := store.GetBackupCatalogState(ctx)
	if err != nil {
		t.Fatalf("get catalog again: %v", err)
	}
	if again.Backups[0].ID != "backup-a" {
		t.Fatal("catalog result aliases durable state")
	}

	next := []CommittedBackup{validCommittedBackup("backup-a", base.Add(3*time.Hour))}
	if err := store.ReplaceCommittedBackupCatalog(ctx, next, base.Add(4*time.Hour)); err != nil {
		t.Fatalf("replace catalog with pruned set: %v", err)
	}
	for _, staleID := range []string{"backup-b", "backup-old"} {
		var stale CommittedBackup
		exists, err := store.getValue(prefixBackupCatalog+staleID, &stale)
		if err != nil {
			t.Fatalf("read stale entry %q: %v", staleID, err)
		}
		if exists {
			t.Fatalf("stale catalog entry %q still exists", staleID)
		}
	}

	before, err := store.GetBackupCatalogState(ctx)
	if err != nil {
		t.Fatalf("get catalog before invalid replacement: %v", err)
	}
	duplicate := []CommittedBackup{next[0], next[0]}
	if err := store.ReplaceCommittedBackupCatalog(ctx, duplicate, base.Add(5*time.Hour)); err == nil {
		t.Fatal("duplicate catalog IDs were accepted")
	}
	after, err := store.GetBackupCatalogState(ctx)
	if err != nil {
		t.Fatalf("get catalog after invalid replacement: %v", err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatal("invalid replacement partially changed catalog state")
	}
}

func TestBackupCatalogReplacementDoesNotScanStaleLeaderState(t *testing.T) {
	store := newBackupCatalogTestStore(t, t.TempDir())
	defer store.Close()
	if err := store.db.Set([]byte(prefixBackupCatalog), []byte("malformed-stale-entry"), pebble.Sync); err != nil {
		t.Fatalf("seed malformed stale entry: %v", err)
	}
	base := time.Date(2026, 7, 29, 8, 0, 0, 0, time.UTC)
	entry := validCommittedBackup("backup-prefix-replace", base)
	if err := store.ReplaceCommittedBackupCatalog(context.Background(), []CommittedBackup{entry}, base); err != nil {
		t.Fatalf("ReplaceCommittedBackupCatalog: %v", err)
	}
	if _, closer, err := store.db.Get([]byte(prefixBackupCatalog)); !errors.Is(err, pebble.ErrNotFound) {
		if closer != nil {
			closer.Close()
		}
		t.Fatalf("stale prefix key error = %v, want not found", err)
	}
}

func TestBackupCatalogValidation(t *testing.T) {
	base := time.Date(2026, 7, 29, 6, 0, 0, 0, time.UTC)
	tests := map[string]func(*CommittedBackup){
		"invalid ID":      func(v *CommittedBackup) { v.ID = "../escape" },
		"missing cluster": func(v *CommittedBackup) { v.SourceClusterID = "" },
		"missing created": func(v *CommittedBackup) { v.CreatedAt = time.Time{} },
		"zero term":       func(v *CommittedBackup) { v.RaftTerm = 0 },
		"zero index":      func(v *CommittedBackup) { v.AppliedIndex = 0 },
		"negative bytes":  func(v *CommittedBackup) { v.TotalBytes = -1 },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			store := newBackupCatalogTestStore(t, t.TempDir())
			defer store.Close()
			entry := validCommittedBackup("backup-invalid", base)
			mutate(&entry)
			if err := store.ReplaceCommittedBackupCatalog(context.Background(), []CommittedBackup{entry}, base.Add(time.Hour)); err == nil {
				t.Fatal("invalid catalog entry was accepted")
			}
		})
	}

	store := newBackupCatalogTestStore(t, t.TempDir())
	defer store.Close()
	if err := store.ReplaceCommittedBackupCatalog(context.Background(), nil, time.Time{}); err == nil {
		t.Fatal("zero reconciliation time was accepted")
	}
}

func TestClusterIDCreateOnceGeneratedAndMalformedExisting(t *testing.T) {
	ctx := context.Background()
	store := newBackupCatalogTestStore(t, t.TempDir())
	defer store.Close()

	got, err := store.EnsureClusterID(ctx, "cluster-primary")
	if err != nil {
		t.Fatalf("create cluster ID: %v", err)
	}
	if got != "cluster-primary" {
		t.Fatalf("cluster ID = %q", got)
	}
	for _, requested := range []string{"", "cluster-primary"} {
		got, err = store.EnsureClusterID(ctx, requested)
		if err != nil || got != "cluster-primary" {
			t.Fatalf("ensure existing cluster ID with %q: got %q, err %v", requested, got, err)
		}
	}
	if _, err := store.EnsureClusterID(ctx, "cluster-other"); err == nil {
		t.Fatal("conflicting cluster ID was accepted")
	}
	if _, err := store.EnsureClusterID(ctx, "../unsafe"); err == nil {
		t.Fatal("unsafe requested cluster ID was accepted")
	}

	generatedStore := newBackupCatalogTestStore(t, t.TempDir())
	defer generatedStore.Close()
	generated, err := generatedStore.EnsureClusterID(ctx, "")
	if err != nil {
		t.Fatalf("generate cluster ID: %v", err)
	}
	if !isUUIDShaped(generated) {
		t.Fatalf("generated cluster ID is not UUID-shaped: %q", generated)
	}

	malformedStore := newBackupCatalogTestStore(t, t.TempDir())
	defer malformedStore.Close()
	data, err := marshalValue("", codecMsgpack)
	if err != nil {
		t.Fatalf("marshal malformed cluster ID: %v", err)
	}
	if err := malformedStore.db.Set([]byte(keyClusterID), data, pebble.Sync); err != nil {
		t.Fatalf("write malformed cluster ID: %v", err)
	}
	if _, err := malformedStore.EnsureClusterID(ctx, "cluster-replacement"); err == nil {
		t.Fatal("malformed durable cluster ID was silently replaced")
	}
}

func TestClusterIDConcurrentDifferentCreatesReturnStableConflict(t *testing.T) {
	store := newBackupCatalogTestStore(t, t.TempDir())
	defer store.Close()

	start := make(chan struct{})
	type result struct {
		id  string
		err error
	}
	results := make(chan result, 2)
	for _, requested := range []string{"cluster-race-a", "cluster-race-b"} {
		requested := requested
		go func() {
			<-start
			id, err := store.EnsureClusterID(context.Background(), requested)
			results <- result{id: id, err: err}
		}()
	}
	close(start)

	var successes, conflicts int
	for i := 0; i < 2; i++ {
		result := <-results
		switch {
		case result.err == nil:
			successes++
		case errors.Is(result.err, ErrBackupMetadataConflict):
			conflicts++
		default:
			t.Fatalf("unexpected EnsureClusterID error: %v", result.err)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("successes=%d conflicts=%d, want 1/1", successes, conflicts)
	}
}

func TestRestorePendingMarkerLifecycleValidationAndDetachedResult(t *testing.T) {
	store := newBackupCatalogTestStore(t, t.TempDir())
	defer store.Close()
	ctx := context.Background()

	got, err := store.GetRestorePendingMarker(ctx)
	if err != nil || got != nil {
		t.Fatalf("missing marker: got %#v, err %v", got, err)
	}

	base := time.Date(2026, 7, 29, 7, 0, 0, 0, time.FixedZone("east", 9*60*60))
	marker := RestorePendingMarker{
		BackupID:        "backup-restore",
		SourceClusterID: "cluster-source",
		AppliedIndex:    42,
		RestoredAt:      base,
	}
	if err := store.PutRestorePendingMarker(ctx, &marker); err != nil {
		t.Fatalf("put marker: %v", err)
	}
	if marker.RestoredAt.Location() == time.UTC {
		t.Fatal("PutRestorePendingMarker mutated caller timestamp")
	}
	if err := store.PutRestorePendingMarker(ctx, &marker); err != nil {
		t.Fatalf("idempotent marker put: %v", err)
	}

	got, err = store.GetRestorePendingMarker(ctx)
	if err != nil {
		t.Fatalf("get marker: %v", err)
	}
	if got == nil || got.BackupID != marker.BackupID || got.RestoredAt.Location() != time.UTC {
		t.Fatalf("unexpected marker: %#v", got)
	}
	got.BackupID = "mutated"
	again, err := store.GetRestorePendingMarker(ctx)
	if err != nil || again == nil || again.BackupID != marker.BackupID {
		t.Fatalf("marker result aliases durable state: %#v, %v", again, err)
	}

	invalid := marker
	invalid.AppliedIndex = 0
	if err := store.PutRestorePendingMarker(ctx, &invalid); err == nil {
		t.Fatal("zero marker index was accepted")
	}
	invalid = marker
	invalid.SourceClusterID = "cluster/escape"
	if err := store.PutRestorePendingMarker(ctx, &invalid); err == nil {
		t.Fatal("unsafe marker cluster ID was accepted")
	}
	invalid = marker
	invalid.RestoredAt = time.Time{}
	if err := store.PutRestorePendingMarker(ctx, &invalid); err == nil {
		t.Fatal("zero restore time was accepted")
	}

	if err := store.ClearRestorePendingMarker(ctx); err != nil {
		t.Fatalf("clear marker: %v", err)
	}
	if err := store.ClearRestorePendingMarker(ctx); err != nil {
		t.Fatalf("idempotent clear marker: %v", err)
	}
	got, err = store.GetRestorePendingMarker(ctx)
	if err != nil || got != nil {
		t.Fatalf("marker after clear: got %#v, err %v", got, err)
	}
}

func TestRestorePendingMarkerIsImmutableUntilExplicitClear(t *testing.T) {
	store := newBackupCatalogTestStore(t, t.TempDir())
	defer store.Close()
	base := time.Date(2026, 7, 29, 13, 0, 0, 0, time.UTC)
	first := RestorePendingMarker{
		BackupID:        "backup-restore-first",
		SourceClusterID: "cluster-source",
		AppliedIndex:    101,
		RestoredAt:      base,
	}
	second := RestorePendingMarker{
		BackupID:        "backup-restore-second",
		SourceClusterID: "cluster-source",
		AppliedIndex:    202,
		RestoredAt:      base.Add(time.Hour),
	}

	if err := store.PutRestorePendingMarker(context.Background(), &first); err != nil {
		t.Fatalf("put first marker: %v", err)
	}
	if err := store.PutRestorePendingMarker(context.Background(), &second); !errors.Is(err, ErrBackupMetadataConflict) {
		t.Fatalf("different marker error = %v, want conflict", err)
	}
	durable, err := store.GetRestorePendingMarker(context.Background())
	if err != nil {
		t.Fatalf("get first marker: %v", err)
	}
	if durable == nil || !restorePendingMarkersEqual(*durable, first) {
		t.Fatalf("durable marker = %#v, want first %#v", durable, first)
	}

	if err := store.ClearRestorePendingMarker(context.Background()); err != nil {
		t.Fatalf("clear first marker: %v", err)
	}
	if err := store.PutRestorePendingMarker(context.Background(), &second); err != nil {
		t.Fatalf("put second after clear: %v", err)
	}
	durable, err = store.GetRestorePendingMarker(context.Background())
	if err != nil {
		t.Fatalf("get second marker: %v", err)
	}
	if durable == nil || !restorePendingMarkersEqual(*durable, second) {
		t.Fatalf("durable marker = %#v, want second %#v", durable, second)
	}
}

func TestRestorePendingMarkerConcurrentDifferentPutsKeepWinner(t *testing.T) {
	store := newBackupCatalogTestStore(t, t.TempDir())
	defer store.Close()
	base := time.Date(2026, 7, 29, 14, 0, 0, 0, time.UTC)
	markers := []RestorePendingMarker{
		{
			BackupID:        "backup-restore-race-a",
			SourceClusterID: "cluster-source",
			AppliedIndex:    301,
			RestoredAt:      base,
		},
		{
			BackupID:        "backup-restore-race-b",
			SourceClusterID: "cluster-source",
			AppliedIndex:    302,
			RestoredAt:      base.Add(time.Minute),
		},
	}
	start := make(chan struct{})
	results := make(chan error, len(markers))
	for i := range markers {
		marker := markers[i]
		go func() {
			<-start
			results <- store.PutRestorePendingMarker(context.Background(), &marker)
		}()
	}
	close(start)

	var successes, conflicts int
	for range markers {
		err := <-results
		switch {
		case err == nil:
			successes++
		case errors.Is(err, ErrBackupMetadataConflict):
			conflicts++
		default:
			t.Fatalf("unexpected marker put error: %v", err)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("successes=%d conflicts=%d, want 1/1", successes, conflicts)
	}
	durable, err := store.GetRestorePendingMarker(context.Background())
	if err != nil {
		t.Fatalf("get winning marker: %v", err)
	}
	if durable == nil ||
		(!restorePendingMarkersEqual(*durable, markers[0]) &&
			!restorePendingMarkersEqual(*durable, markers[1])) {
		t.Fatalf("durable marker = %#v, want one submitted marker", durable)
	}
}

func TestRestorePendingMarkerStaleAbsenceCannotOverwriteNewerMarker(t *testing.T) {
	store := newBackupCatalogTestStore(t, t.TempDir())
	defer store.Close()
	base := time.Date(2026, 7, 29, 15, 0, 0, 0, time.UTC)
	newer := RestorePendingMarker{
		BackupID:        "backup-restore-newer",
		SourceClusterID: "cluster-source",
		AppliedIndex:    402,
		RestoredAt:      base.Add(time.Minute),
	}
	stale := RestorePendingMarker{
		BackupID:        "backup-restore-stale",
		SourceClusterID: "cluster-source",
		AppliedIndex:    401,
		RestoredAt:      base,
	}
	if err := store.PutRestorePendingMarker(context.Background(), &newer); err != nil {
		t.Fatalf("put newer marker: %v", err)
	}
	staleBytes, err := marshalValue(&stale, codecMsgpack)
	if err != nil {
		t.Fatalf("marshal stale marker: %v", err)
	}
	err = store.applyBackupMetadataConditional(context.Background(), &ConditionalBatch{
		Version: conditionalBatchVersion,
		Preconditions: []ConditionalPrecondition{{
			Key:          []byte(keyRestorePending),
			ExpectAbsent: true,
		}},
		Mutations: []BatchOp{{Key: []byte(keyRestorePending), Value: staleBytes}},
	})
	if !errors.Is(err, ErrBackupMetadataConflict) {
		t.Fatalf("stale marker error = %v, want conflict", err)
	}
	durable, err := store.GetRestorePendingMarker(context.Background())
	if err != nil {
		t.Fatalf("get newer marker: %v", err)
	}
	if durable == nil || !restorePendingMarkersEqual(*durable, newer) {
		t.Fatalf("durable marker = %#v, want newer %#v", durable, newer)
	}
}

func TestRestorePendingClearRejectsMalformedDurableMarker(t *testing.T) {
	store := newBackupCatalogTestStore(t, t.TempDir())
	defer store.Close()

	malformed := RestorePendingMarker{
		BackupID:        "backup-malformed",
		SourceClusterID: "cluster-source",
		AppliedIndex:    0,
		RestoredAt:      time.Date(2026, 7, 29, 7, 30, 0, 0, time.UTC),
	}
	data, err := marshalValue(&malformed, codecMsgpack)
	if err != nil {
		t.Fatalf("marshal malformed marker: %v", err)
	}
	if err := store.db.Set([]byte(keyRestorePending), data, pebble.Sync); err != nil {
		t.Fatalf("write malformed marker: %v", err)
	}

	if err := store.ClearRestorePendingMarker(context.Background()); err == nil {
		t.Fatal("malformed durable marker was cleared")
	}
	var stillPresent RestorePendingMarker
	found, err := store.getValue(keyRestorePending, &stillPresent)
	if err != nil || !found {
		t.Fatalf("malformed marker was not retained: found=%v err=%v", found, err)
	}
}

func TestBackupCatalogRecordsSurviveReopen(t *testing.T) {
	dir := t.TempDir()
	store := newBackupCatalogTestStore(t, dir)
	ctx := context.Background()
	base := time.Date(2026, 7, 29, 8, 0, 0, 0, time.UTC)

	task := validBackupTask("backup-persist", base)
	if err := store.PutBackupTask(ctx, &task); err != nil {
		t.Fatalf("put task: %v", err)
	}
	backup := validCommittedBackup("backup-persist", base)
	if err := store.ReplaceCommittedBackupCatalog(ctx, []CommittedBackup{backup}, base.Add(time.Hour)); err != nil {
		t.Fatalf("replace catalog: %v", err)
	}
	if _, err := store.EnsureClusterID(ctx, "cluster-persist"); err != nil {
		t.Fatalf("ensure cluster ID: %v", err)
	}
	marker := RestorePendingMarker{
		BackupID:        backup.ID,
		SourceClusterID: backup.SourceClusterID,
		AppliedIndex:    backup.AppliedIndex,
		RestoredAt:      base.Add(2 * time.Hour),
	}
	if err := store.PutRestorePendingMarker(ctx, &marker); err != nil {
		t.Fatalf("put marker: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	store = newBackupCatalogTestStore(t, dir)
	defer store.Close()
	tasks, err := store.ListBackupTasks(ctx, 10)
	if err != nil || len(tasks) != 1 || tasks[0].ID != task.ID {
		t.Fatalf("tasks after reopen: %#v, %v", tasks, err)
	}
	state, err := store.GetBackupCatalogState(ctx)
	if err != nil || state == nil || len(state.Backups) != 1 || state.Backups[0].ID != backup.ID {
		t.Fatalf("catalog after reopen: %#v, %v", state, err)
	}
	clusterID, err := store.EnsureClusterID(ctx, "")
	if err != nil || clusterID != "cluster-persist" {
		t.Fatalf("cluster ID after reopen: %q, %v", clusterID, err)
	}
	gotMarker, err := store.GetRestorePendingMarker(ctx)
	if err != nil || gotMarker == nil || gotMarker.BackupID != marker.BackupID {
		t.Fatalf("marker after reopen: %#v, %v", gotMarker, err)
	}
}

func TestBackupCatalogContextAndClosedStoreBehavior(t *testing.T) {
	store := newBackupCatalogTestStore(t, t.TempDir())
	base := time.Date(2026, 7, 29, 9, 0, 0, 0, time.UTC)
	task := validBackupTask("backup-context", base)
	entry := validCommittedBackup("backup-context", base)
	marker := RestorePendingMarker{
		BackupID:        entry.ID,
		SourceClusterID: entry.SourceClusterID,
		AppliedIndex:    entry.AppliedIndex,
		RestoredAt:      base,
	}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	cancelledCalls := []func() error{
		func() error { return store.PutBackupTask(cancelled, &task) },
		func() error { _, err := store.ListBackupTasks(cancelled, 1); return err },
		func() error { return store.ReplaceCommittedBackupCatalog(cancelled, []CommittedBackup{entry}, base) },
		func() error { _, err := store.GetBackupCatalogState(cancelled); return err },
		func() error { _, err := store.EnsureClusterID(cancelled, "cluster-context"); return err },
		func() error { return store.PutRestorePendingMarker(cancelled, &marker) },
		func() error { _, err := store.GetRestorePendingMarker(cancelled); return err },
		func() error { return store.ClearRestorePendingMarker(cancelled) },
	}
	for i, call := range cancelledCalls {
		if err := call(); !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled call %d error = %v", i, err)
		}
	}

	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	closedCalls := []func() error{
		func() error { return store.PutBackupTask(context.Background(), &task) },
		func() error { _, err := store.ListBackupTasks(context.Background(), 1); return err },
		func() error {
			return store.ReplaceCommittedBackupCatalog(context.Background(), []CommittedBackup{entry}, base)
		},
		func() error { _, err := store.GetBackupCatalogState(context.Background()); return err },
		func() error { _, err := store.EnsureClusterID(context.Background(), "cluster-context"); return err },
		func() error { return store.PutRestorePendingMarker(context.Background(), &marker) },
		func() error { _, err := store.GetRestorePendingMarker(context.Background()); return err },
		func() error { return store.ClearRestorePendingMarker(context.Background()) },
	}
	for i, call := range closedCalls {
		if err := call(); !errors.Is(err, ErrServiceClosed) {
			t.Fatalf("closed call %d error = %v", i, err)
		}
	}
}

func TestBackupCatalogFollowerMutationsRejectedAndReplacementUsesOneRaftApply(t *testing.T) {
	if testing.Short() {
		t.Skip("real raft integration test")
	}
	cluster := startRealRaftTestCluster(t, 3)
	defer cluster.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	leader := cluster.WaitForLeader(t, ctx)
	var follower *realRaftTestNode
	for _, node := range cluster.Nodes {
		if node.ID != leader.ID {
			follower = node
			break
		}
	}
	if follower == nil {
		t.Fatal("follower not found")
	}

	base := time.Date(2026, 7, 29, 10, 0, 0, 0, time.UTC)
	task := validBackupTask("backup-follower", base)
	entry := validCommittedBackup("backup-follower", base)
	marker := RestorePendingMarker{
		BackupID:        entry.ID,
		SourceClusterID: entry.SourceClusterID,
		AppliedIndex:    entry.AppliedIndex,
		RestoredAt:      base,
	}
	followerCalls := []func() error{
		func() error { return follower.Store.PutBackupTask(ctx, &task) },
		func() error { return follower.Store.ReplaceCommittedBackupCatalog(ctx, []CommittedBackup{entry}, base) },
		func() error { _, err := follower.Store.EnsureClusterID(ctx, "cluster-follower"); return err },
		func() error { return follower.Store.PutRestorePendingMarker(ctx, &marker) },
		func() error { return follower.Store.ClearRestorePendingMarker(ctx) },
	}
	for i, call := range followerCalls {
		if err := call(); err == nil || !strings.Contains(err.Error(), "not leader") {
			t.Fatalf("follower mutation %d error = %v", i, err)
		}
	}

	metrics := NewMetrics()
	leader.Store.SetMetrics(metrics)
	beforeWrites := metrics.Snapshot().WriteOps
	beforeIndex := raftFSMConsumedIndex(leader.Store.raft)
	if err := leader.Store.ReplaceCommittedBackupCatalog(ctx, []CommittedBackup{entry}, base.Add(time.Hour)); err != nil {
		t.Fatalf("leader replace catalog: %v", err)
	}
	afterWrites := metrics.Snapshot().WriteOps
	if afterWrites != beforeWrites+1 {
		t.Fatalf("catalog replacement recorded %d writes, want one batch", afterWrites-beforeWrites)
	}
	if afterIndex := raftFSMConsumedIndex(leader.Store.raft); afterIndex <= beforeIndex {
		t.Fatalf("catalog replacement did not advance the Raft FSM: before=%d after=%d", beforeIndex, afterIndex)
	}
}

func TestBackupMetadataRaftWaitDoesNotHoldStoreMutexAndContextIsExplicit(t *testing.T) {
	if testing.Short() {
		t.Skip("real raft integration test")
	}
	cluster := startRealRaftTestCluster(t, 1)
	defer cluster.Stop()
	setupCtx, cancelSetup := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancelSetup()
	leader := cluster.WaitForLeader(t, setupCtx)

	leader.Store.raft.fsm.snapshotMu.Lock()
	locked := true
	defer func() {
		if locked {
			leader.Store.raft.fsm.snapshotMu.Unlock()
		}
	}()

	before, err := strconv.ParseUint(leader.Store.raft.Stats()["last_log_index"], 10, 64)
	if err != nil {
		t.Fatalf("parse last log index: %v", err)
	}
	callCtx, cancelCall := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancelCall()
	result := make(chan error, 1)
	task := validBackupTask("backup-context-outcome", testBackupTime())
	go func() {
		result <- leader.Store.PutBackupTask(callCtx, &task)
	}()

	waitDeadline := time.Now().Add(5 * time.Second)
	for {
		current, parseErr := strconv.ParseUint(leader.Store.raft.Stats()["last_log_index"], 10, 64)
		if parseErr != nil {
			t.Fatalf("parse current last log index: %v", parseErr)
		}
		if current > before {
			break
		}
		if time.Now().After(waitDeadline) {
			t.Fatal("conditional proposal was not appended")
		}
		time.Sleep(10 * time.Millisecond)
	}

	if !leader.Store.mu.TryLock() {
		t.Fatal("PebbleStore.mu is held while waiting for Raft/FSM application")
	}
	leader.Store.mu.Unlock()

	callErr := <-result
	if !errors.Is(callErr, context.DeadlineExceeded) ||
		!errors.Is(callErr, ErrRaftConditionalOutcomeUnknown) {
		t.Fatalf("PutBackupTask error = %v, want deadline and unknown-outcome sentinels", callErr)
	}

	leader.Store.raft.fsm.snapshotMu.Unlock()
	locked = false
	durableDeadline := time.Now().Add(5 * time.Second)
	for {
		var durable BackupTask
		found, readErr := leader.Store.getValue(prefixBackupTask+task.ID, &durable)
		if readErr == nil && found {
			break
		}
		if time.Now().After(durableDeadline) {
			t.Fatalf("proposal did not finish after releasing FSM: found=%v err=%v", found, readErr)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func newBackupCatalogTestStore(t *testing.T, dir string) *PebbleStore {
	t.Helper()
	store, err := NewPebbleStore(PebbleStoreConfig{Dir: dir, NodeID: 1})
	if err != nil {
		t.Fatalf("create Pebble store: %v", err)
	}
	return store
}

func validBackupTask(id string, startedAt time.Time) BackupTask {
	return BackupTask{
		ID:              id,
		SourceClusterID: "cluster-source",
		OwnerNodeID:     "node-1",
		LeadershipTerm:  7,
		AppliedIndex:    99,
		State:           BackupTaskCreating,
		StartedAt:       startedAt,
	}
}

func validCommittedBackup(id string, createdAt time.Time) CommittedBackup {
	return CommittedBackup{
		ID:              id,
		SourceClusterID: "cluster-source",
		CreatedAt:       createdAt,
		RaftTerm:        7,
		AppliedIndex:    99,
		TotalBytes:      1024,
	}
}

func backupTaskIDs(tasks []BackupTask) []string {
	ids := make([]string, len(tasks))
	for i := range tasks {
		ids[i] = tasks[i].ID
	}
	return ids
}

func committedBackupIDs(backups []CommittedBackup) []string {
	ids := make([]string, len(backups))
	for i := range backups {
		ids[i] = backups[i].ID
	}
	return ids
}

func isUUIDShaped(id string) bool {
	if len(id) != 36 {
		return false
	}
	for _, pos := range []int{8, 13, 18, 23} {
		if id[pos] != '-' {
			return false
		}
	}
	for i, r := range id {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			continue
		}
		if !strings.ContainsRune("0123456789abcdef", r) {
			return false
		}
	}
	return true
}

func raftFSMConsumedIndex(node *RaftNode) uint64 {
	node.fsm.snapshotMu.RLock()
	defer node.fsm.snapshotMu.RUnlock()
	return node.fsm.lastAppliedIndex
}

type cancelAfterChecksContext struct {
	context.Context
	mu        sync.Mutex
	remaining int
}

func (c *cancelAfterChecksContext) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.remaining--
	if c.remaining <= 0 {
		return context.Canceled
	}
	return nil
}

func ExampleBackupTaskState() {
	fmt.Println(BackupTaskCreating, BackupTaskUploading, BackupTaskVerifying, BackupTaskCommitted, BackupTaskFailed)
	// Output: creating uploading verifying committed failed
}
