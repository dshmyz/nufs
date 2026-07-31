package metadata

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeCoordinatorMetadata struct {
	mu                  sync.Mutex
	tasks               map[string]BackupTask
	catalog             *BackupCatalogState
	events              *[]string
	putErr              error
	putBeforeErr        map[BackupTaskState]error
	putAfterErr         map[BackupTaskState]error
	catalogErr          error
	putAttempts         int
	currentRaftTerm     func() uint64
	beforeTaskAtTerm    func(BackupTask, uint64)
	beforeCatalogAtTerm func(uint64)
}

type fakeCoordinatorLeadership struct {
	leader atomic.Bool
	term   atomic.Uint64
}

func (l *fakeCoordinatorLeadership) Store(leader bool) {
	l.leader.Store(leader)
}

func (l *fakeCoordinatorLeadership) SetTerm(term uint64) {
	l.term.Store(term)
}

func (l *fakeCoordinatorLeadership) Current() (bool, uint64, error) {
	return l.leader.Load(), l.term.Load(), nil
}

func newFakeCoordinatorMetadata(events *[]string) *fakeCoordinatorMetadata {
	return &fakeCoordinatorMetadata{tasks: make(map[string]BackupTask), events: events}
}

func (m *fakeCoordinatorMetadata) PutBackupTask(ctx context.Context, task *BackupTask) error {
	return m.putBackupTask(ctx, task, 0)
}

func (m *fakeCoordinatorMetadata) putBackupTaskAtTerm(
	ctx context.Context,
	task *BackupTask,
	expectedRaftTerm uint64,
) error {
	if m.beforeTaskAtTerm != nil {
		m.beforeTaskAtTerm(*task, expectedRaftTerm)
	}
	return m.putBackupTask(ctx, task, expectedRaftTerm)
}

func (m *fakeCoordinatorMetadata) putBackupTask(
	ctx context.Context,
	task *BackupTask,
	expectedRaftTerm uint64,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if expectedRaftTerm != 0 &&
		m.currentRaftTerm != nil &&
		m.currentRaftTerm() != expectedRaftTerm {
		return ErrBackupMetadataConflict
	}
	m.putAttempts++
	if m.putErr != nil {
		err := m.putErr
		m.putErr = nil
		return err
	}
	if err := m.putBeforeErr[task.State]; err != nil {
		delete(m.putBeforeErr, task.State)
		return err
	}
	m.tasks[task.ID] = *task
	if m.events != nil {
		*m.events = append(*m.events, "task:"+string(task.State))
	}
	if err := m.putAfterErr[task.State]; err != nil {
		delete(m.putAfterErr, task.State)
		return err
	}
	return nil
}

func (m *fakeCoordinatorMetadata) ListBackupTasks(_ context.Context, limit int) ([]BackupTask, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	top := newBackupTaskTopK(limit)
	for _, task := range m.tasks {
		top.Add(task)
	}
	return top.Sorted(), nil
}

func (m *fakeCoordinatorMetadata) GetBackupTask(_ context.Context, id string) (*BackupTask, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	task, ok := m.tasks[id]
	if !ok {
		return nil, nil
	}
	return &task, nil
}

func (m *fakeCoordinatorMetadata) ScanActiveBackupTasks(ctx context.Context, visit func(BackupTask) error) error {
	m.mu.Lock()
	tasks := make([]BackupTask, 0, len(m.tasks))
	for _, task := range m.tasks {
		if !isTerminalBackupTaskState(task.State) {
			tasks = append(tasks, task)
		}
	}
	m.mu.Unlock()
	for _, task := range tasks {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := visit(task); err != nil {
			return err
		}
	}
	return nil
}

func (m *fakeCoordinatorMetadata) ReplaceCommittedBackupCatalog(_ context.Context, backups []CommittedBackup, at time.Time) error {
	return m.replaceCommittedBackupCatalog(backups, at, 0)
}

func (m *fakeCoordinatorMetadata) replaceCommittedBackupCatalogAtTerm(
	_ context.Context,
	backups []CommittedBackup,
	at time.Time,
	expectedRaftTerm uint64,
) error {
	if m.beforeCatalogAtTerm != nil {
		m.beforeCatalogAtTerm(expectedRaftTerm)
	}
	return m.replaceCommittedBackupCatalog(backups, at, expectedRaftTerm)
}

func (m *fakeCoordinatorMetadata) replaceCommittedBackupCatalog(
	backups []CommittedBackup,
	at time.Time,
	expectedRaftTerm uint64,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if expectedRaftTerm != 0 &&
		m.currentRaftTerm != nil &&
		m.currentRaftTerm() != expectedRaftTerm {
		return ErrBackupMetadataConflict
	}
	if m.catalogErr != nil {
		err := m.catalogErr
		m.catalogErr = nil
		return err
	}
	m.catalog = &BackupCatalogState{Backups: append([]CommittedBackup(nil), backups...), ReconciledAt: at}
	if m.events != nil {
		*m.events = append(*m.events, "catalog")
	}
	return nil
}

func (m *fakeCoordinatorMetadata) GetBackupCatalogState(context.Context) (*BackupCatalogState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.catalog == nil {
		return nil, nil
	}
	out := *m.catalog
	out.Backups = append([]CommittedBackup(nil), m.catalog.Backups...)
	return &out, nil
}

func (m *fakeCoordinatorMetadata) EnsureClusterID(_ context.Context, requested string) (string, error) {
	return requested, nil
}

type fakeCoordinatorRepository struct {
	mu                        sync.Mutex
	events                    *[]string
	manifests                 map[string]*BackupManifest
	publishErr                error
	publishGate               chan struct{}
	publishSeen               chan struct{}
	publishOnce               sync.Once
	deleteGate                chan struct{}
	deleteSeen                chan struct{}
	deleteOnce                sync.Once
	deleteIgnoresCancellation bool
	deleteErrAt               string
	deleted                   []string
	publishes                 int
	janitorErr                error
}

func newFakeCoordinatorRepository(events *[]string) *fakeCoordinatorRepository {
	return &fakeCoordinatorRepository{events: events, manifests: make(map[string]*BackupManifest)}
}

func (r *fakeCoordinatorRepository) Publish(ctx context.Context, _ string, manifest *BackupManifest) error {
	if r.publishSeen != nil {
		r.publishOnce.Do(func() { close(r.publishSeen) })
	}
	if r.publishGate != nil {
		select {
		case <-r.publishGate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.events != nil {
		*r.events = append(*r.events, "publish")
	}
	r.publishes++
	if r.publishErr != nil {
		err := r.publishErr
		r.publishErr = nil
		return err
	}
	if existing := r.manifests[manifest.BackupID]; existing != nil {
		if !backupManifestsEqual(existing, manifest) {
			return fmt.Errorf("immutable backup %q already has different contents", manifest.BackupID)
		}
		return nil
	}
	r.manifests[manifest.BackupID] = cloneBackupManifest(manifest)
	return nil
}

func (r *fakeCoordinatorRepository) ListCommitted(context.Context) ([]BackupDescriptor, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.events != nil {
		*r.events = append(*r.events, "list")
	}
	out := make([]BackupDescriptor, 0, len(r.manifests))
	for _, manifest := range r.manifests {
		out = append(out, descriptorFromManifest(manifest))
	}
	sortBackupDescriptors(out)
	return out, nil
}

func (r *fakeCoordinatorRepository) Fetch(_ context.Context, id, target string) (*BackupManifest, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.events != nil {
		*r.events = append(*r.events, "fetch")
	}
	manifest := r.manifests[id]
	if manifest == nil {
		return nil, fmt.Errorf("missing backup %s", id)
	}
	if err := os.MkdirAll(target, 0o700); err != nil {
		return nil, err
	}
	return cloneBackupManifest(manifest), nil
}

func (r *fakeCoordinatorRepository) Delete(ctx context.Context, id string) error {
	if r.deleteSeen != nil {
		r.deleteOnce.Do(func() { close(r.deleteSeen) })
	}
	if r.deleteGate != nil {
		if r.deleteIgnoresCancellation {
			<-r.deleteGate
		} else {
			select {
			case <-r.deleteGate:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if id == r.deleteErrAt {
		return errors.New("delete failed")
	}
	delete(r.manifests, id)
	r.deleted = append(r.deleted, id)
	return nil
}

func (r *fakeCoordinatorRepository) DeleteStagingOlderThan(context.Context, time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.events != nil {
		*r.events = append(*r.events, "janitor")
	}
	return r.janitorErr
}

func newCoordinatorHarness(t *testing.T) (*BackupCoordinator, *fakeCoordinatorMetadata, *fakeCoordinatorRepository, *fakeCoordinatorLeadership, *[]string) {
	t.Helper()
	events := &[]string{}
	meta := newFakeCoordinatorMetadata(events)
	repo := newFakeCoordinatorRepository(events)
	leader := &fakeCoordinatorLeadership{}
	leader.Store(true)
	leader.SetTerm(7)
	base := time.Date(2026, 7, 30, 1, 2, 3, 0, time.UTC)
	var tick atomic.Int64
	cfg := BackupCoordinatorConfig{
		ClusterID:     "cluster-a",
		Interval:      time.Hour,
		Retention:     24,
		LocalTempDir:  t.TempDir(),
		StagingMaxAge: time.Hour,
		UploadTimeout: time.Minute,
	}
	c := NewBackupCoordinator(cfg, nil, repo)
	c.metadata = meta
	c.currentLeadership = leader.Current
	meta.currentRaftTerm = leader.term.Load
	c.ownerNodeID = func() string { return "meta-1" }
	c.now = func() time.Time { return base.Add(time.Duration(tick.Add(1)) * time.Millisecond) }
	c.newBackupID = func(time.Time) (string, error) { return "backup-20260730T010203Z-0011223344556677", nil }
	c.createCheckpoint = func(context.Context, string) (*PortableCheckpoint, error) {
		*events = append(*events, "checkpoint")
		dir := filepath.Join(cfg.LocalTempDir, "checkpoint")
		if err := os.Mkdir(dir, 0o700); err != nil {
			return nil, err
		}
		return &PortableCheckpoint{Dir: dir, Term: 7, AppliedIndex: 42}, nil
	}
	c.buildManifest = func(_ context.Context, _ string, meta BackupSnapshotMetadata) (*BackupManifest, error) {
		*events = append(*events, "build")
		return &BackupManifest{
			FormatVersion:      BackupFormatVersion,
			BackupID:           meta.BackupID,
			SourceClusterID:    meta.SourceClusterID,
			CreatedAt:          meta.CreatedAt,
			RaftTerm:           meta.RaftTerm,
			AppliedIndex:       meta.AppliedIndex,
			CheckpointFormat:   "pebble",
			MinimumNUFSVersion: "test",
		}, nil
	}
	c.verifyArtifact = func(_ context.Context, dir string, _ *BackupManifest) (*BackupVerificationReport, error) {
		if filepath.Base(dir) == "checkpoint" {
			*events = append(*events, "verify:local")
		} else {
			*events = append(*events, "verify:remote")
		}
		return &BackupVerificationReport{ManifestValid: true}, nil
	}
	c.waitLeadershipPoll = func(ctx context.Context) bool {
		<-ctx.Done()
		return false
	}
	return c, meta, repo, leader, events
}

func TestBackupCoordinatorPublishesVerifiedBackup(t *testing.T) {
	c, meta, _, _, events := newCoordinatorHarness(t)
	result, err := c.Trigger(context.Background())
	if err != nil {
		t.Fatalf("Trigger: %v", err)
	}
	if result.Task.State != BackupTaskCommitted || result.Manifest == nil {
		t.Fatalf("unexpected result: %+v", result)
	}
	meta.mu.Lock()
	durable := meta.tasks[result.Task.ID]
	meta.mu.Unlock()
	if durable.State != BackupTaskCommitted {
		t.Fatalf("durable task state = %q", durable.State)
	}
	want := []string{
		"list", "catalog",
		"checkpoint", "task:creating", "build", "verify:local",
		"task:uploading", "publish", "task:verifying", "fetch",
		"verify:remote", "task:committed", "list", "catalog",
		"list", "catalog", "janitor",
	}
	if !reflect.DeepEqual(*events, want) {
		t.Fatalf("pipeline events:\n got %v\nwant %v", *events, want)
	}
}

func TestBackupCoordinatorFollowerDoesNotRun(t *testing.T) {
	c, meta, repo, leader, _ := newCoordinatorHarness(t)
	leader.Store(false)
	_, err := c.Trigger(context.Background())
	if !errors.Is(err, ErrBackupCoordinatorNotLeader) {
		t.Fatalf("expected not leader, got %v", err)
	}
	if meta.putAttempts != 0 || len(repo.manifests) != 0 {
		t.Fatal("follower produced durable or repository side effects")
	}
}

func TestBackupCoordinatorCoalescesConcurrentTriggers(t *testing.T) {
	c, _, repo, _, _ := newCoordinatorHarness(t)
	repo.publishGate = make(chan struct{})
	repo.publishSeen = make(chan struct{})
	joined := make(chan struct{}, 1)
	c.onRunJoin = func() { joined <- struct{}{} }
	var generated atomic.Int64
	c.newBackupID = func(time.Time) (string, error) {
		return fmt.Sprintf("backup-run-%d", generated.Add(1)), nil
	}
	type response struct {
		result *BackupRunResult
		err    error
	}
	responses := make(chan response, 2)
	go func() {
		result, err := c.Trigger(context.Background())
		responses <- response{result: result, err: err}
	}()
	<-repo.publishSeen
	go func() {
		result, err := c.Trigger(context.Background())
		responses <- response{result: result, err: err}
	}()
	<-joined
	close(repo.publishGate)
	first, second := <-responses, <-responses
	if first.err != nil || second.err != nil {
		t.Fatalf("trigger errors: %v, %v", first.err, second.err)
	}
	if first.result.Task.ID != second.result.Task.ID ||
		generated.Load() != 1 || repo.publishes != 1 || len(repo.manifests) != 1 {
		t.Fatalf("triggers did not coalesce: %+v %+v", first.result, second.result)
	}
}

func TestBackupCoordinatorCallerCancellationDoesNotCancelSharedRun(t *testing.T) {
	c, _, repo, _, _ := newCoordinatorHarness(t)
	repo.publishGate = make(chan struct{})
	repo.publishSeen = make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	firstDone := make(chan error, 1)
	go func() {
		_, err := c.Trigger(ctx)
		firstDone <- err
	}()
	<-repo.publishSeen
	cancel()
	if err := <-firstDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("caller error = %v", err)
	}
	secondDone := make(chan error, 1)
	go func() {
		_, err := c.Trigger(context.Background())
		secondDone <- err
	}()
	close(repo.publishGate)
	if err := <-secondDone; err != nil {
		t.Fatalf("shared run was canceled: %v", err)
	}
}

func TestBackupCoordinatorCancelsOnLeadershipLoss(t *testing.T) {
	c, meta, repo, leader, _ := newCoordinatorHarness(t)
	repo.publishGate = make(chan struct{})
	repo.publishSeen = make(chan struct{})
	polls := make(chan struct{}, 1)
	c.waitLeadershipPoll = func(ctx context.Context) bool {
		select {
		case <-polls:
			return true
		case <-ctx.Done():
			return false
		}
	}
	done := make(chan error, 1)
	go func() {
		_, err := c.Trigger(context.Background())
		done <- err
	}()
	<-repo.publishSeen
	leader.Store(false)
	polls <- struct{}{}
	err := <-done
	if !errors.Is(err, ErrBackupCoordinatorLeadershipLost) {
		t.Fatalf("expected leadership loss, got %v", err)
	}
	meta.mu.Lock()
	defer meta.mu.Unlock()
	for _, task := range meta.tasks {
		if task.State == BackupTaskFailed {
			t.Fatal("lost leader must leave active task for recovery")
		}
	}
	if len(repo.deleted) != 0 {
		t.Fatal("lost leader pruned a backup")
	}
}

func TestBackupCoordinatorAllowsImmutablePublishThenTerm8Reconciles(t *testing.T) {
	c, meta, repo, leadership, _ := newCoordinatorHarness(t)
	repo.publishGate = make(chan struct{})
	repo.publishSeen = make(chan struct{})
	var ids atomic.Int32
	c.newBackupID = func(time.Time) (string, error) {
		return fmt.Sprintf("backup-term-%d", ids.Add(1)), nil
	}
	var checkpoints atomic.Int32
	c.createCheckpoint = func(context.Context, string) (*PortableCheckpoint, error) {
		call := checkpoints.Add(1)
		term := leadership.term.Load()
		dir := filepath.Join(c.cfg.LocalTempDir, fmt.Sprintf("generation-checkpoint-%d", call))
		if err := os.Mkdir(dir, 0o700); err != nil {
			return nil, err
		}
		return &PortableCheckpoint{Dir: dir, Term: term, AppliedIndex: uint64(40 + call)}, nil
	}
	done := make(chan error, 1)
	go func() {
		_, err := c.Trigger(context.Background())
		done <- err
	}()
	<-repo.publishSeen
	leadership.Store(false)
	leadership.SetTerm(8)
	leadership.Store(true)
	close(repo.publishGate)
	if err := <-done; !errors.Is(err, ErrBackupCoordinatorLeadershipLost) {
		t.Fatalf("expected generation loss, got %v", err)
	}

	repo.mu.Lock()
	published := cloneBackupManifest(repo.manifests["backup-term-1"])
	repo.mu.Unlock()
	if published == nil || published.RaftTerm != 7 {
		t.Fatalf("stale immutable Publish side effect = %+v, want committed term-7 artifact", published)
	}
	meta.mu.Lock()
	oldTask := meta.tasks["backup-term-1"]
	oldCatalogAdvanced := catalogContainsBackup(meta.catalog, "backup-term-1")
	meta.mu.Unlock()
	if oldTask.State != BackupTaskUploading {
		t.Fatalf("old generation advanced task after Publish: %+v", oldTask)
	}
	if oldCatalogAdvanced {
		t.Fatal("old generation advanced catalog after Publish")
	}

	repo.publishGate = nil
	if _, err := c.Trigger(context.Background()); err != nil {
		t.Fatalf("term-8 run did not reconcile stale immutable Publish: %v", err)
	}
	meta.mu.Lock()
	defer meta.mu.Unlock()
	if meta.tasks["backup-term-1"].State != BackupTaskFailed {
		t.Fatalf("term-8 recovery did not close old active task: %+v", meta.tasks["backup-term-1"])
	}
	if !catalogContainsBackup(meta.catalog, "backup-term-1") ||
		!catalogContainsBackup(meta.catalog, "backup-term-2") {
		t.Fatalf("term-8 catalog did not converge to repository: %+v", meta.catalog)
	}
}

func TestBackupCoordinatorFencesTaskWhenTermChangesBetweenCheckAndApply(t *testing.T) {
	c, meta, _, leadership, _ := newCoordinatorHarness(t)
	var once sync.Once
	meta.beforeTaskAtTerm = func(task BackupTask, expectedTerm uint64) {
		if task.State == BackupTaskCreating {
			once.Do(func() { leadership.SetTerm(expectedTerm + 1) })
		}
	}

	if _, err := c.Trigger(context.Background()); err == nil {
		t.Fatal("expected stale task mutation to fail")
	}
	meta.mu.Lock()
	defer meta.mu.Unlock()
	if len(meta.tasks) != 0 {
		t.Fatalf("stale generation wrote task records: %+v", meta.tasks)
	}
}

func TestBackupCoordinatorFencesCatalogWhenTermChangesBetweenCheckAndApply(t *testing.T) {
	c, meta, _, leadership, _ := newCoordinatorHarness(t)
	var once sync.Once
	meta.beforeCatalogAtTerm = func(expectedTerm uint64) {
		once.Do(func() { leadership.SetTerm(expectedTerm + 1) })
	}

	if _, err := c.Trigger(context.Background()); err == nil {
		t.Fatal("expected stale catalog mutation to fail")
	}
	meta.mu.Lock()
	defer meta.mu.Unlock()
	if meta.catalog != nil {
		t.Fatalf("stale generation replaced catalog: %+v", meta.catalog)
	}
	if len(meta.tasks) != 0 {
		t.Fatalf("new attempt started before fenced preflight completed: %+v", meta.tasks)
	}
}

func TestBackupCoordinatorFencesFailedTaskDeferUnderNewTerm(t *testing.T) {
	c, meta, _, leadership, _ := newCoordinatorHarness(t)
	c.buildManifest = func(context.Context, string, BackupSnapshotMetadata) (*BackupManifest, error) {
		return nil, errors.New("build failed")
	}
	meta.beforeTaskAtTerm = func(task BackupTask, expectedTerm uint64) {
		if task.State == BackupTaskFailed {
			leadership.SetTerm(expectedTerm + 1)
		}
	}

	if _, err := c.Trigger(context.Background()); err == nil {
		t.Fatal("expected build failure")
	}
	meta.mu.Lock()
	defer meta.mu.Unlock()
	if len(meta.tasks) != 1 {
		t.Fatalf("task count = %d, want creating task only", len(meta.tasks))
	}
	for _, task := range meta.tasks {
		if task.State != BackupTaskCreating {
			t.Fatalf("failed defer crossed generation fence: %+v", task)
		}
	}
}

func TestBackupCoordinatorStaleDeleteOnlyRemovesObsoleteAndTerm8Converges(t *testing.T) {
	c, meta, repo, leadership, _ := newCoordinatorHarness(t)
	c.cfg.Retention = 1
	for i := 0; i < 2; i++ {
		manifest := coordinatorTestManifest(
			fmt.Sprintf("backup-old-%02d", i),
			time.Date(2026, 7, 1, 0, 0, i, 0, time.UTC),
			uint64(i+1),
		)
		repo.manifests[manifest.BackupID] = manifest
	}
	repo.deleteGate = make(chan struct{})
	repo.deleteSeen = make(chan struct{})
	repo.deleteIgnoresCancellation = true
	polls := make(chan struct{}, 1)
	c.waitLeadershipPoll = func(ctx context.Context) bool {
		select {
		case <-polls:
			return true
		case <-ctx.Done():
			return false
		}
	}
	done := make(chan error, 1)
	go func() {
		_, err := c.Trigger(context.Background())
		done <- err
	}()
	<-repo.deleteSeen
	leadership.Store(false)
	leadership.SetTerm(8)
	leadership.Store(true)
	polls <- struct{}{}
	close(repo.deleteGate)
	if err := <-done; !errors.Is(err, ErrBackupCoordinatorLeadershipLost) {
		t.Fatalf("expected generation loss during delete, got %v", err)
	}
	repo.mu.Lock()
	deleted := append([]string(nil), repo.deleted...)
	_, currentStillPresent := repo.manifests["backup-20260730T010203Z-0011223344556677"]
	remainingRepositoryIDs := make(map[string]struct{}, len(repo.manifests))
	for id := range repo.manifests {
		remainingRepositoryIDs[id] = struct{}{}
	}
	repo.mu.Unlock()
	if len(deleted) != 1 || deleted[0] == "backup-20260730T010203Z-0011223344556677" {
		t.Fatalf("stale Delete side effect was not limited to one obsolete candidate: %v", deleted)
	}
	if !currentStillPresent {
		t.Fatal("stale Delete removed the fresh repository top-1/current backup")
	}

	c.createCheckpoint = func(context.Context, string) (*PortableCheckpoint, error) {
		return nil, errors.New("stop after term-8 preflight")
	}
	if _, err := c.Trigger(context.Background()); err == nil {
		t.Fatal("expected term-8 attempt to stop after preflight")
	}
	meta.mu.Lock()
	defer meta.mu.Unlock()
	if meta.catalog == nil || len(meta.catalog.Backups) != len(remainingRepositoryIDs) {
		t.Fatalf("term-8 preflight did not converge catalog: %+v", meta.catalog)
	}
	for _, backup := range meta.catalog.Backups {
		if _, ok := remainingRepositoryIDs[backup.ID]; !ok {
			t.Fatalf("catalog retained deleted backup %q: %+v", backup.ID, meta.catalog)
		}
	}
}

func TestBackupCoordinatorStopWaitsAndIsIdempotent(t *testing.T) {
	c, _, repo, _, _ := newCoordinatorHarness(t)
	repo.publishGate = make(chan struct{})
	runDone := make(chan error, 1)
	go func() {
		_, err := c.Trigger(context.Background())
		runDone <- err
	}()
	stopDone := make(chan struct{})
	go func() {
		c.Stop()
		c.Stop()
		close(stopDone)
	}()
	select {
	case <-stopDone:
	case <-time.After(time.Second):
		t.Fatal("Stop did not wait for canceled run cleanup")
	}
	if err := <-runDone; !errors.Is(err, ErrBackupCoordinatorStopped) {
		t.Fatalf("run error = %v", err)
	}
}

func TestBackupCoordinatorStopPersistsActiveTaskFailure(t *testing.T) {
	c, meta, _, _, _ := newCoordinatorHarness(t)
	buildSeen := make(chan struct{})
	c.buildManifest = func(ctx context.Context, _ string, _ BackupSnapshotMetadata) (*BackupManifest, error) {
		close(buildSeen)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	runDone := make(chan error, 1)
	go func() {
		_, err := c.Trigger(context.Background())
		runDone <- err
	}()
	<-buildSeen
	c.Stop()
	<-runDone
	meta.mu.Lock()
	defer meta.mu.Unlock()
	for _, task := range meta.tasks {
		if task.State != BackupTaskFailed {
			t.Fatalf("task after Stop = %q, want failed", task.State)
		}
		return
	}
	t.Fatal("missing durable task")
}

func TestBackupCoordinatorDoesNotPruneAfterFailure(t *testing.T) {
	c, _, repo, _, _ := newCoordinatorHarness(t)
	old := coordinatorTestManifest("old", time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC), 1)
	repo.manifests[old.BackupID] = old
	repo.publishErr = errors.New("publish failed")
	c.cfg.Retention = 1
	if _, err := c.Trigger(context.Background()); err == nil {
		t.Fatal("expected publish failure")
	}
	if len(repo.deleted) != 0 {
		t.Fatalf("pruned after failure: %v", repo.deleted)
	}
}

func TestBackupCoordinatorJanitorFailureIsBestEffort(t *testing.T) {
	c, _, repo, _, _ := newCoordinatorHarness(t)
	repo.janitorErr = errors.New("janitor unavailable")
	result, err := c.Trigger(context.Background())
	if err != nil {
		t.Fatalf("Trigger failed after committed backup: %v", err)
	}
	status := c.Status(context.Background())
	if result == nil || result.Task.State != BackupTaskCommitted {
		t.Fatalf("missing committed result: %+v", result)
	}
	if status.LastBackupID != result.Task.ID || !strings.Contains(status.LastError, "janitor") {
		t.Fatalf("janitor status was not reported separately: %+v", status)
	}
}

func TestBackupCoordinatorPreflightReconcilesBeforeFailedAttempt(t *testing.T) {
	c, meta, repo, _, events := newCoordinatorHarness(t)
	existing := coordinatorTestManifest(
		"backup-existing",
		time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC),
		1,
	)
	repo.manifests[existing.BackupID] = existing
	c.createCheckpoint = func(context.Context, string) (*PortableCheckpoint, error) {
		*events = append(*events, "checkpoint:failed")
		return nil, errors.New("checkpoint failed")
	}
	if _, err := c.Trigger(context.Background()); err == nil {
		t.Fatal("expected checkpoint failure")
	}
	meta.mu.Lock()
	defer meta.mu.Unlock()
	if meta.catalog == nil || len(meta.catalog.Backups) != 1 ||
		meta.catalog.Backups[0].ID != existing.BackupID {
		t.Fatalf("preflight catalog was not reconciled: %+v", meta.catalog)
	}
	wantPrefix := []string{"list", "fetch", "verify:remote", "catalog", "checkpoint:failed"}
	if !reflect.DeepEqual(*events, wantPrefix) {
		t.Fatalf("preflight order:\n got %v\nwant %v", *events, wantPrefix)
	}
}

func TestBackupCoordinatorRunsPreflightWhenFollowerBecomesLeader(t *testing.T) {
	c, meta, repo, leadership, _ := newCoordinatorHarness(t)
	leadership.Store(false)
	repo.publishGate = make(chan struct{})
	repo.publishSeen = make(chan struct{})
	polls := make(chan struct{}, 1)
	c.waitLeadershipPoll = func(ctx context.Context) bool {
		select {
		case <-polls:
			return true
		case <-ctx.Done():
			return false
		}
	}
	c.waitSchedulerTick = func(ctx context.Context) bool {
		<-ctx.Done()
		return false
	}
	c.createCheckpoint = func(context.Context, string) (*PortableCheckpoint, error) {
		dir := filepath.Join(c.cfg.LocalTempDir, "promoted-checkpoint")
		if err := os.Mkdir(dir, 0o700); err != nil {
			return nil, err
		}
		return &PortableCheckpoint{Dir: dir, Term: 8, AppliedIndex: 43}, nil
	}
	c.Start()
	leadership.SetTerm(8)
	leadership.Store(true)
	polls <- struct{}{}
	select {
	case <-repo.publishSeen:
	case <-time.After(time.Second):
		t.Fatal("leader promotion did not trigger an immediate run")
	}
	meta.mu.Lock()
	if meta.catalog == nil {
		meta.mu.Unlock()
		t.Fatal("leader promotion did not reconcile catalog before running")
	}
	meta.mu.Unlock()
	close(repo.publishGate)
	c.Stop()
}

func TestBackupCoordinatorPromotesPendingGenerationAfterOldRunCleanup(t *testing.T) {
	c, _, _, leadership, _ := newCoordinatorHarness(t)
	c.waitSchedulerTick = func(ctx context.Context) bool {
		<-ctx.Done()
		return false
	}
	polls := make(chan struct{}, 2)
	pollWaiters := make(chan struct{}, 4)
	c.waitLeadershipPoll = func(ctx context.Context) bool {
		select {
		case pollWaiters <- struct{}{}:
		case <-ctx.Done():
			return false
		}
		select {
		case <-polls:
			return true
		case <-ctx.Done():
			return false
		}
	}

	events := make(chan string, 8)
	c.metadata.(*fakeCoordinatorMetadata).beforeCatalogAtTerm = func(term uint64) {
		events <- fmt.Sprintf("catalog:%d", term)
	}
	var checkpointCalls atomic.Int32
	c.createCheckpoint = func(context.Context, string) (*PortableCheckpoint, error) {
		call := checkpointCalls.Add(1)
		term := leadership.term.Load()
		events <- fmt.Sprintf("checkpoint:%d", term)
		dir := filepath.Join(c.cfg.LocalTempDir, fmt.Sprintf("pending-checkpoint-%d", call))
		if err := os.Mkdir(dir, 0o700); err != nil {
			return nil, err
		}
		return &PortableCheckpoint{Dir: dir, Term: term, AppliedIndex: uint64(40 + call)}, nil
	}
	buildStarted := make(chan struct{})
	buildCanceled := make(chan struct{})
	releaseOldCleanup := make(chan struct{})
	originalBuild := c.buildManifest
	c.buildManifest = func(ctx context.Context, dir string, metadata BackupSnapshotMetadata) (*BackupManifest, error) {
		if checkpointCalls.Load() == 1 {
			close(buildStarted)
			<-ctx.Done()
			close(buildCanceled)
			<-releaseOldCleanup
			return nil, ctx.Err()
		}
		return originalBuild(ctx, dir, metadata)
	}
	defer c.Stop()
	c.Start()

	for _, want := range []string{"catalog:7", "checkpoint:7"} {
		if got := <-events; got != want {
			t.Fatalf("initial run event = %q, want %q", got, want)
		}
	}
	<-buildStarted
	<-pollWaiters
	<-pollWaiters

	leadership.Store(false)
	leadership.SetTerm(8)
	leadership.Store(true)
	polls <- struct{}{}
	polls <- struct{}{}
	<-buildCanceled
	<-pollWaiters
	close(releaseOldCleanup)

	for _, want := range []string{"catalog:8", "checkpoint:8"} {
		select {
		case got := <-events:
			if got != want {
				t.Fatalf("promoted run event = %q, want %q", got, want)
			}
		case <-time.After(time.Second):
			t.Fatalf("pending generation did not produce %q promptly", want)
		}
	}
	c.Stop()
	if got := checkpointCalls.Load(); got != 2 {
		t.Fatalf("checkpoint calls = %d, want exactly term-7 and one promoted term-8 run", got)
	}
}

func TestBackupCoordinatorCleansCheckpointAndFailsTaskOnTransitionError(t *testing.T) {
	c, meta, _, _, _ := newCoordinatorHarness(t)
	meta.putBeforeErr = map[BackupTaskState]error{
		BackupTaskUploading: errors.New("transition failed"),
	}
	if _, err := c.Trigger(context.Background()); err == nil {
		t.Fatal("expected transition failure")
	}
	if _, err := os.Stat(filepath.Join(c.cfg.LocalTempDir, "checkpoint")); !os.IsNotExist(err) {
		t.Fatalf("checkpoint was not cleaned: %v", err)
	}
	meta.mu.Lock()
	defer meta.mu.Unlock()
	for _, task := range meta.tasks {
		if task.State != BackupTaskFailed {
			t.Fatalf("task state = %q, want failed", task.State)
		}
		return
	}
	t.Fatal("missing task")
}

func TestBackupCoordinatorKeepsLatest24CommittedBackups(t *testing.T) {
	c, _, repo, _, _ := newCoordinatorHarness(t)
	c.cfg.Retention = 24
	for i := 0; i < 25; i++ {
		manifest := coordinatorTestManifest(
			fmt.Sprintf("backup-old-%02d", i),
			time.Date(2026, 7, 1, 0, 0, i, 0, time.UTC),
			uint64(i+1),
		)
		repo.manifests[manifest.BackupID] = manifest
	}
	if _, err := c.Trigger(context.Background()); err != nil {
		t.Fatalf("Trigger: %v", err)
	}
	if got := len(repo.manifests); got != 24 {
		t.Fatalf("committed backups = %d, want 24", got)
	}
}

func TestBackupCoordinatorReconcilesAfterPartialPruneFailure(t *testing.T) {
	c, meta, repo, _, _ := newCoordinatorHarness(t)
	c.cfg.Retention = 1
	for i := 0; i < 3; i++ {
		manifest := coordinatorTestManifest(
			fmt.Sprintf("backup-old-%02d", i),
			time.Date(2026, 7, 1, 0, 0, i, 0, time.UTC),
			uint64(i+1),
		)
		repo.manifests[manifest.BackupID] = manifest
	}
	repo.deleteErrAt = "backup-old-01"
	if _, err := c.Trigger(context.Background()); err == nil {
		t.Fatal("expected partial prune failure")
	}
	meta.mu.Lock()
	defer meta.mu.Unlock()
	if meta.catalog == nil || len(meta.catalog.Backups) != len(repo.manifests) {
		t.Fatalf("catalog was not reconciled to actual repository: %+v", meta.catalog)
	}
}

func TestBackupCoordinatorRecoversInterruptedTasks(t *testing.T) {
	c, meta, _, _, _ := newCoordinatorHarness(t)
	started := time.Date(2026, 7, 29, 0, 0, 0, 0, time.UTC)
	meta.tasks["orphan"] = BackupTask{
		ID:              "orphan",
		SourceClusterID: "cluster-a",
		OwnerNodeID:     "meta-old",
		LeadershipTerm:  3,
		AppliedIndex:    9,
		State:           BackupTaskUploading,
		StartedAt:       started,
		UpdatedAt:       started,
	}
	if _, err := c.Trigger(context.Background()); err != nil {
		t.Fatalf("Trigger: %v", err)
	}
	meta.mu.Lock()
	orphan := meta.tasks["orphan"]
	meta.mu.Unlock()
	if orphan.State != BackupTaskFailed || orphan.LastError == "" {
		t.Fatalf("orphan was not failed: %+v", orphan)
	}
}

func TestBackupCoordinatorRecoversActiveTaskOlderThanNewest1000(t *testing.T) {
	c, meta, _, _, _ := newCoordinatorHarness(t)
	base := time.Date(2026, 7, 29, 0, 0, 0, 0, time.UTC)
	orphan := BackupTask{
		ID:              "orphan-old",
		SourceClusterID: "cluster-a",
		OwnerNodeID:     "meta-old",
		LeadershipTerm:  3,
		AppliedIndex:    9,
		State:           BackupTaskUploading,
		StartedAt:       base,
		UpdatedAt:       base,
	}
	meta.tasks[orphan.ID] = orphan
	for i := 0; i < 1001; i++ {
		at := base.Add(time.Duration(i+1) * time.Minute)
		task := BackupTask{
			ID:              fmt.Sprintf("terminal-%04d", i),
			SourceClusterID: "cluster-a",
			OwnerNodeID:     "meta-old",
			LeadershipTerm:  3,
			AppliedIndex:    uint64(i + 10),
			State:           BackupTaskFailed,
			StartedAt:       at,
			CompletedAt:     at,
			UpdatedAt:       at,
			LastError:       "done",
		}
		meta.tasks[task.ID] = task
	}
	if _, err := c.Trigger(context.Background()); err != nil {
		t.Fatalf("Trigger: %v", err)
	}
	meta.mu.Lock()
	recovered := meta.tasks[orphan.ID]
	meta.mu.Unlock()
	if recovered.State != BackupTaskFailed {
		t.Fatalf("older active orphan was not recovered: %+v", recovered)
	}
}

func TestBackupCoordinatorReconcilesIndeterminatePublish(t *testing.T) {
	c, _, repo, _, _ := newCoordinatorHarness(t)
	repo.publishErr = errors.New("indeterminate publish")
	originalPublish := c.repository
	c.repository = backupRepositoryFuncs{
		publish: func(ctx context.Context, dir string, manifest *BackupManifest) error {
			repo.mu.Lock()
			repo.manifests[manifest.BackupID] = cloneBackupManifest(manifest)
			repo.mu.Unlock()
			return originalPublish.Publish(ctx, dir, manifest)
		},
		list: repo.ListCommitted, fetch: repo.Fetch, delete: repo.Delete, janitor: repo.DeleteStagingOlderThan,
	}
	result, err := c.Trigger(context.Background())
	if err != nil {
		t.Fatalf("Trigger: %v", err)
	}
	if result.Task.State != BackupTaskCommitted {
		t.Fatalf("task state = %q", result.Task.State)
	}
}

func TestBackupCoordinatorReconcilesUnknownTaskTransition(t *testing.T) {
	c, meta, _, _, _ := newCoordinatorHarness(t)
	meta.putAfterErr = map[BackupTaskState]error{
		BackupTaskUploading: fmt.Errorf("%w: caller timed out", ErrRaftConditionalOutcomeUnknown),
	}
	result, err := c.Trigger(context.Background())
	if err != nil {
		t.Fatalf("Trigger: %v", err)
	}
	if result.Task.State != BackupTaskCommitted {
		t.Fatalf("task state = %q", result.Task.State)
	}
}

func TestBackupCoordinatorRejectsForeignCatalogBeforePrune(t *testing.T) {
	c, _, repo, _, _ := newCoordinatorHarness(t)
	c.cfg.Retention = 1
	foreign := coordinatorTestManifest("backup-foreign", time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC), 1)
	foreign.SourceClusterID = "other-cluster"
	repo.manifests[foreign.BackupID] = foreign
	if _, err := c.Trigger(context.Background()); err == nil {
		t.Fatal("expected foreign cluster rejection")
	}
	if len(repo.deleted) != 0 {
		t.Fatalf("pruned before catalog validation: %v", repo.deleted)
	}
}

func TestBackupCoordinatorRejectsCurrentDescriptorMismatchBeforePrune(t *testing.T) {
	c, _, repo, _, _ := newCoordinatorHarness(t)
	c.cfg.Retention = 1
	base := c.repository
	c.repository = backupRepositoryFuncs{
		publish: base.Publish,
		list: func(ctx context.Context) ([]BackupDescriptor, error) {
			descriptors, err := base.ListCommitted(ctx)
			if len(descriptors) > 0 {
				descriptors[0].TotalBytes++
			}
			return descriptors, err
		},
		fetch: base.Fetch, delete: base.Delete, janitor: base.DeleteStagingOlderThan,
	}
	if _, err := c.Trigger(context.Background()); err == nil {
		t.Fatal("expected current descriptor mismatch")
	}
	if len(repo.deleted) != 0 {
		t.Fatalf("pruned before descriptor validation: %v", repo.deleted)
	}
}

func TestBackupCoordinatorTickCoalescesWithActiveRun(t *testing.T) {
	c, _, repo, _, _ := newCoordinatorHarness(t)
	repo.publishGate = make(chan struct{})
	repo.publishSeen = make(chan struct{})
	ticks := make(chan struct{}, 2)
	c.waitSchedulerTick = func(ctx context.Context) bool {
		select {
		case <-ticks:
			return true
		case <-ctx.Done():
			return false
		}
	}
	c.Start()
	<-repo.publishSeen
	ticks <- struct{}{}
	ticks <- struct{}{}
	close(repo.publishGate)
	c.Stop()
	repo.mu.Lock()
	publishes := repo.publishes
	repo.mu.Unlock()
	if publishes != 1 {
		t.Fatalf("publish count = %d, want one coalesced run", publishes)
	}
}

func TestBackupCoordinatorStartStopTriggerRaces(t *testing.T) {
	c, _, repo, _, _ := newCoordinatorHarness(t)
	repo.publishGate = make(chan struct{})
	repo.publishSeen = make(chan struct{})
	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		_, _ = c.Trigger(context.Background())
	}()
	<-repo.publishSeen

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			c.Start()
		}()
		go func() {
			defer wg.Done()
			_, _ = c.Trigger(context.Background())
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.Stop()
	}()
	wg.Wait()
	<-firstDone
	c.Stop()
}

func coordinatorTestManifest(id string, createdAt time.Time, index uint64) *BackupManifest {
	return &BackupManifest{
		FormatVersion:      BackupFormatVersion,
		BackupID:           id,
		SourceClusterID:    "cluster-a",
		CreatedAt:          createdAt,
		RaftTerm:           1,
		AppliedIndex:       index,
		CheckpointFormat:   "pebble",
		MinimumNUFSVersion: "test",
	}
}

func catalogContainsBackup(catalog *BackupCatalogState, id string) bool {
	if catalog == nil {
		return false
	}
	for _, backup := range catalog.Backups {
		if backup.ID == id {
			return true
		}
	}
	return false
}

type backupRepositoryFuncs struct {
	publish func(context.Context, string, *BackupManifest) error
	list    func(context.Context) ([]BackupDescriptor, error)
	fetch   func(context.Context, string, string) (*BackupManifest, error)
	delete  func(context.Context, string) error
	janitor func(context.Context, time.Time) error
}

func (r backupRepositoryFuncs) Publish(ctx context.Context, dir string, manifest *BackupManifest) error {
	return r.publish(ctx, dir, manifest)
}
func (r backupRepositoryFuncs) ListCommitted(ctx context.Context) ([]BackupDescriptor, error) {
	return r.list(ctx)
}
func (r backupRepositoryFuncs) Fetch(ctx context.Context, id, dir string) (*BackupManifest, error) {
	return r.fetch(ctx, id, dir)
}
func (r backupRepositoryFuncs) Delete(ctx context.Context, id string) error {
	return r.delete(ctx, id)
}
func (r backupRepositoryFuncs) DeleteStagingOlderThan(ctx context.Context, cutoff time.Time) error {
	return r.janitor(ctx, cutoff)
}
