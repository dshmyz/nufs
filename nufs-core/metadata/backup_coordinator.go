package metadata

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"
)

var (
	ErrBackupCoordinatorNotLeader      = errors.New("backup coordinator: not leader")
	ErrBackupCoordinatorLeadershipLost = errors.New("backup coordinator: leadership lost")
	ErrBackupCoordinatorStopped        = errors.New("backup coordinator: stopped")
)

type BackupCoordinatorConfig struct {
	ClusterID     string
	Interval      time.Duration
	Retention     int
	LocalTempDir  string
	StagingMaxAge time.Duration
	UploadTimeout time.Duration
}

type BackupRunResult struct {
	Task     BackupTask
	Manifest *BackupManifest
}

type BackupCoordinatorStatus struct {
	Started       bool
	Active        bool
	Leader        bool
	Retention     int
	LastBackupID  string
	LastError     string
	LastStartedAt time.Time
	LastEndedAt   time.Time
	NextRunAt     time.Time
}

type backupCoordinatorMetadata interface {
	putBackupTaskAtTerm(context.Context, *BackupTask, uint64) error
	GetBackupTask(context.Context, string) (*BackupTask, error)
	ListBackupTasks(context.Context, int) ([]BackupTask, error)
	ScanActiveBackupTasks(context.Context, func(BackupTask) error) error
	replaceCommittedBackupCatalogAtTerm(context.Context, []CommittedBackup, time.Time, uint64) error
	GetBackupCatalogState(context.Context) (*BackupCatalogState, error)
	EnsureClusterID(context.Context, string) (string, error)
}

type backupSharedRun struct {
	done       chan struct{}
	cancel     context.CancelFunc
	generation uint64
	result     *BackupRunResult
	err        error
}

type BackupCoordinator struct {
	cfg        BackupCoordinatorConfig
	store      *PebbleStore
	metadata   backupCoordinatorMetadata
	repository BackupRepository

	createCheckpoint   func(context.Context, string) (*PortableCheckpoint, error)
	currentLeadership  func() (bool, uint64, error)
	ownerNodeID        func() string
	now                func() time.Time
	newBackupID        func(time.Time) (string, error)
	buildManifest      func(context.Context, string, BackupSnapshotMetadata) (*BackupManifest, error)
	verifyArtifact     func(context.Context, string, *BackupManifest) (*BackupVerificationReport, error)
	waitLeadershipPoll func(context.Context) bool
	waitSchedulerTick  func(context.Context) bool
	onRunJoin          func()

	mu                sync.Mutex
	rootCtx           context.Context
	rootCancel        context.CancelFunc
	started           bool
	stopped           bool
	active            *backupSharedRun
	pendingGeneration uint64
	status            BackupCoordinatorStatus
	wg                sync.WaitGroup
}

func NewBackupCoordinator(
	cfg BackupCoordinatorConfig,
	store *PebbleStore,
	repository BackupRepository,
) *BackupCoordinator {
	rootCtx, rootCancel := context.WithCancel(context.Background())
	c := &BackupCoordinator{
		cfg:            cfg,
		store:          store,
		metadata:       store,
		repository:     repository,
		rootCtx:        rootCtx,
		rootCancel:     rootCancel,
		now:            func() time.Time { return time.Now().UTC() },
		newBackupID:    newCoordinatorBackupID,
		buildManifest:  BuildBackupManifest,
		verifyArtifact: VerifyBackupArtifact,
	}
	c.status.Retention = cfg.Retention
	c.currentLeadership = func() (bool, uint64, error) {
		if store == nil {
			return false, 0, fmt.Errorf("metadata store is required")
		}
		if store.raft == nil {
			return store.IsLeader(), 0, nil
		}
		stats := store.raft.Stats()
		leader := stats["state"] == "Leader"
		term, err := parseCheckpointTerm(stats)
		if err != nil {
			return leader, 0, err
		}
		return leader, term, nil
	}
	c.ownerNodeID = func() string {
		if store == nil {
			return "unknown"
		}
		if store.raft != nil {
			return store.raft.NodeID()
		}
		return fmt.Sprintf("meta-%d", store.cfg.NodeID)
	}
	c.createCheckpoint = func(ctx context.Context, parent string) (*PortableCheckpoint, error) {
		if store == nil {
			return nil, fmt.Errorf("backup coordinator: metadata store is required")
		}
		if store.raft == nil {
			return nil, fmt.Errorf("backup coordinator: Raft is required for positioned backups")
		}
		return store.raft.CreateBackupCheckpoint(ctx, parent)
	}
	c.waitLeadershipPoll = func(ctx context.Context) bool {
		timer := time.NewTimer(100 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-timer.C:
			return true
		case <-ctx.Done():
			return false
		}
	}
	c.waitSchedulerTick = func(ctx context.Context) bool {
		if cfg.Interval <= 0 {
			<-ctx.Done()
			return false
		}
		timer := time.NewTimer(cfg.Interval)
		defer timer.Stop()
		select {
		case <-timer.C:
			return true
		case <-ctx.Done():
			return false
		}
	}
	return c
}

func (c *BackupCoordinator) Start() {
	c.mu.Lock()
	if c.started || c.stopped {
		c.mu.Unlock()
		return
	}
	c.started = true
	c.status.Started = true
	c.status.NextRunAt = c.now()
	c.wg.Add(2)
	c.mu.Unlock()

	go c.schedulerLoop()
	go c.leadershipLoop()
}

func (c *BackupCoordinator) Stop() {
	c.mu.Lock()
	if !c.stopped {
		c.stopped = true
		c.status.Started = false
		c.status.NextRunAt = time.Time{}
		c.rootCancel()
		if c.active != nil {
			c.active.cancel()
		}
	}
	c.mu.Unlock()
	c.wg.Wait()
}

func (c *BackupCoordinator) Trigger(ctx context.Context) (*BackupRunResult, error) {
	if ctx == nil {
		return nil, fmt.Errorf("backup coordinator: nil trigger context")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	run, err := c.beginRun()
	if err != nil {
		return nil, err
	}
	select {
	case <-run.done:
		return cloneBackupRunResult(run.result), run.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *BackupCoordinator) Status(context.Context) BackupCoordinatorStatus {
	c.mu.Lock()
	defer c.mu.Unlock()
	status := c.status
	leader, _, err := c.readLeadership()
	status.Leader = err == nil && leader
	return status
}

func (c *BackupCoordinator) schedulerLoop() {
	defer c.wg.Done()
	_, _ = c.beginRun()
	for c.waitSchedulerTick(c.rootCtx) {
		c.mu.Lock()
		if !c.stopped {
			c.status.NextRunAt = c.now().Add(c.cfg.Interval)
		}
		c.mu.Unlock()
		_, _ = c.beginRun()
	}
}

func (c *BackupCoordinator) leadershipLoop() {
	defer c.wg.Done()
	var lastLeader bool
	var lastGeneration uint64
	observed := false
	for {
		leader, generation, err := c.readLeadership()
		if err == nil && leader &&
			(!observed || !lastLeader || generation != lastGeneration) {
			c.requestLeadershipRun(generation)
		}
		if err == nil {
			lastLeader = leader
			lastGeneration = generation
			observed = true
		}
		if !c.waitLeadershipPoll(c.rootCtx) {
			return
		}
	}
}

func (c *BackupCoordinator) beginRun() (*backupSharedRun, error) {
	return c.beginRunForGeneration(0)
}

func (c *BackupCoordinator) requestLeadershipRun(generation uint64) {
	_, _ = c.beginRunForGeneration(generation)
}

func (c *BackupCoordinator) beginRunForGeneration(requestedGeneration uint64) (*backupSharedRun, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopped {
		return nil, ErrBackupCoordinatorStopped
	}
	if c.active != nil {
		if requestedGeneration > c.active.generation &&
			requestedGeneration > c.pendingGeneration {
			c.pendingGeneration = requestedGeneration
		}
		if c.onRunJoin != nil {
			c.onRunJoin()
		}
		return c.active, nil
	}
	leader, generation, err := c.readLeadership()
	if err != nil {
		return nil, fmt.Errorf("backup coordinator: read leadership generation: %w", err)
	}
	if !leader {
		return nil, ErrBackupCoordinatorNotLeader
	}
	if generation == 0 {
		return nil, fmt.Errorf("backup coordinator: leadership generation is unavailable")
	}
	if requestedGeneration != 0 && generation < requestedGeneration {
		return nil, fmt.Errorf(
			"%w: current generation %d is older than requested generation %d",
			ErrBackupCoordinatorLeadershipLost,
			generation,
			requestedGeneration,
		)
	}
	runCtx, cancel := context.WithCancel(c.rootCtx)
	run := &backupSharedRun{done: make(chan struct{}), cancel: cancel, generation: generation}
	c.active = run
	c.status.Active = true
	c.status.LastStartedAt = c.now()
	c.status.LastError = ""
	c.wg.Add(1)
	go c.executeSharedRun(runCtx, run)
	return run, nil
}

func (c *BackupCoordinator) executeSharedRun(parent context.Context, run *backupSharedRun) {
	defer c.wg.Done()
	ctx := parent
	cancel := func() {}
	if c.cfg.UploadTimeout > 0 {
		ctx, cancel = context.WithTimeout(parent, c.cfg.UploadTimeout)
	}
	defer cancel()

	var leadershipLost atomic.Bool
	monitorCtx, stopMonitor := context.WithCancel(ctx)
	monitorDone := make(chan struct{})
	go func() {
		defer close(monitorDone)
		for c.waitLeadershipPoll(monitorCtx) {
			if err := c.requireGeneration(monitorCtx, run.generation); err != nil {
				leadershipLost.Store(true)
				cancel()
				run.cancel()
				return
			}
		}
	}()

	result, err := c.run(ctx, run.generation)
	stopMonitor()
	<-monitorDone
	if leadershipLost.Load() {
		err = errors.Join(ErrBackupCoordinatorLeadershipLost, err)
	} else if errors.Is(parent.Err(), context.Canceled) {
		err = errors.Join(ErrBackupCoordinatorStopped, err)
	}
	run.cancel()

	c.mu.Lock()
	run.result = cloneBackupRunResult(result)
	run.err = err
	c.active = nil
	pendingGeneration := c.pendingGeneration
	c.pendingGeneration = 0
	c.status.Active = false
	c.status.LastEndedAt = c.now()
	if result != nil {
		c.status.LastBackupID = result.Task.ID
	}
	if err != nil {
		c.status.LastError = truncateBackupError(err.Error())
	}
	if c.started && !c.stopped {
		c.status.NextRunAt = c.now().Add(c.cfg.Interval)
	}
	close(run.done)
	c.mu.Unlock()

	if pendingGeneration > run.generation {
		_, _ = c.beginRunForGeneration(pendingGeneration)
	}
}

func (c *BackupCoordinator) run(ctx context.Context, generation uint64) (_ *BackupRunResult, retErr error) {
	if err := c.requireGeneration(ctx, generation); err != nil {
		return nil, err
	}
	if err := c.validateConfig(); err != nil {
		return nil, err
	}
	if err := c.requireGeneration(ctx, generation); err != nil {
		return nil, err
	}
	clusterID, err := c.metadata.EnsureClusterID(ctx, c.cfg.ClusterID)
	generationErr := c.requireGeneration(context.WithoutCancel(ctx), generation)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("ensure cluster identity: %w", err), generationErr)
	}
	if generationErr != nil {
		return nil, generationErr
	}
	if clusterID != c.cfg.ClusterID {
		return nil, fmt.Errorf("durable cluster identity %q differs from configured identity %q", clusterID, c.cfg.ClusterID)
	}
	if err := c.recoverInterruptedTasks(ctx, generation); err != nil {
		return nil, fmt.Errorf("recover interrupted backup tasks: %w", err)
	}
	if _, err := c.reconcileRepository(ctx, generation, nil); err != nil {
		return nil, fmt.Errorf("preflight repository reconciliation: %w", err)
	}

	startedAt := c.now()
	backupID, err := c.newBackupID(startedAt)
	if err != nil {
		return nil, fmt.Errorf("generate backup ID: %w", err)
	}
	if err := c.requireGeneration(ctx, generation); err != nil {
		return nil, err
	}
	checkpoint, err := c.createCheckpoint(ctx, c.cfg.LocalTempDir)
	generationErr = c.requireGeneration(context.WithoutCancel(ctx), generation)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("create backup checkpoint: %w", err), generationErr)
	}
	defer func() {
		retErr = errors.Join(retErr, checkpoint.Release())
	}()
	if generationErr != nil {
		return nil, generationErr
	}
	if checkpoint.Term != generation {
		return nil, fmt.Errorf(
			"%w: checkpoint term %d differs from run generation %d",
			ErrBackupCoordinatorLeadershipLost,
			checkpoint.Term,
			generation,
		)
	}

	task := BackupTask{
		ID:              backupID,
		SourceClusterID: c.cfg.ClusterID,
		OwnerNodeID:     c.ownerNodeID(),
		LeadershipTerm:  checkpoint.Term,
		AppliedIndex:    checkpoint.AppliedIndex,
		State:           BackupTaskCreating,
		StartedAt:       startedAt,
		UpdatedAt:       c.now(),
	}
	taskCreated := false
	taskCommitted := false
	defer func() {
		if retErr == nil || !taskCreated || taskCommitted {
			return
		}
		failed := task
		failed.State = BackupTaskFailed
		failed.LastError = truncateBackupError(retErr.Error())
		failed.CompletedAt = c.now()
		failed.UpdatedAt = failed.CompletedAt
		failureTimeout := 5 * time.Second
		if c.cfg.UploadTimeout > 0 && c.cfg.UploadTimeout < failureTimeout {
			failureTimeout = c.cfg.UploadTimeout
		}
		failureCtx, cancelFailure := context.WithTimeout(context.Background(), failureTimeout)
		defer cancelFailure()
		if err := c.requireGeneration(failureCtx, generation); err != nil {
			return
		}
		if err := c.persistTask(failureCtx, generation, &failed); err != nil {
			retErr = errors.Join(retErr, fmt.Errorf("persist failed backup task: %w", err))
		}
	}()

	if err := c.persistTask(ctx, generation, &task); err != nil {
		return nil, err
	}
	taskCreated = true

	manifest, err := c.buildManifest(ctx, checkpoint.Dir, BackupSnapshotMetadata{
		BackupID:           backupID,
		SourceClusterID:    c.cfg.ClusterID,
		CreatedAt:          startedAt,
		RaftTerm:           checkpoint.Term,
		AppliedIndex:       checkpoint.AppliedIndex,
		MinimumNUFSVersion: "nufs",
	})
	if err != nil {
		return nil, fmt.Errorf("build backup manifest: %w", err)
	}
	if manifest == nil ||
		manifest.BackupID != backupID ||
		manifest.SourceClusterID != c.cfg.ClusterID ||
		!manifest.CreatedAt.Equal(startedAt) ||
		manifest.RaftTerm != checkpoint.Term ||
		manifest.AppliedIndex != checkpoint.AppliedIndex {
		return nil, fmt.Errorf("backup manifest identity or position differs from checkpoint request")
	}
	if _, err := c.verifyArtifact(ctx, checkpoint.Dir, manifest); err != nil {
		return nil, fmt.Errorf("verify local backup artifact: %w", err)
	}

	task.State = BackupTaskUploading
	task.BytesUploaded = manifest.TotalBytes
	task.FilesUploaded = len(manifest.Files)
	task.UpdatedAt = c.now()
	if err := c.persistTask(ctx, generation, &task); err != nil {
		return nil, err
	}
	manifest.DurationMillis = maxInt64(0, c.now().Sub(startedAt).Milliseconds())
	if err := c.requireGeneration(ctx, generation); err != nil {
		return nil, err
	}

	// Publish is backup-ID/manifest immutable. A remote call may finish after
	// cancellation, but the term-fenced task/catalog writes below cannot be
	// applied by an old generation; the next leader preflight discovers it.
	publishErr := c.repository.Publish(ctx, checkpoint.Dir, manifest)
	if err := c.requireGeneration(ctx, generation); err != nil {
		return nil, err
	}
	remoteVerified := false
	if publishErr != nil {
		verified, verifyErr := c.fetchAndVerify(ctx, generation, manifest.BackupID, manifest)
		if verifyErr != nil {
			return nil, errors.Join(
				fmt.Errorf("publish backup: %w", publishErr),
				fmt.Errorf("reconcile publication: %w", verifyErr),
			)
		}
		remoteVerified = verified
	}

	task.State = BackupTaskVerifying
	task.UpdatedAt = c.now()
	if err := c.persistTask(ctx, generation, &task); err != nil {
		return nil, err
	}
	if !remoteVerified {
		if _, err := c.fetchAndVerify(ctx, generation, manifest.BackupID, manifest); err != nil {
			return nil, fmt.Errorf("verify published backup: %w", err)
		}
	}

	task.State = BackupTaskCommitted
	task.CompletedAt = c.now()
	task.UpdatedAt = task.CompletedAt
	if err := c.persistTask(ctx, generation, &task); err != nil {
		return nil, err
	}
	taskCommitted = true

	if err := c.prune(ctx, generation, manifest); err != nil {
		return nil, err
	}
	if _, err := c.reconcileRepository(ctx, generation, manifest); err != nil {
		return nil, fmt.Errorf("reconcile catalog after pruning: %w", err)
	}
	// The repository janitor only removes unclaimed staging attempts older than
	// the cutoff. It cannot remove committed artifacts or the current claimed
	// attempt, so it is term-independent, best-effort maintenance.
	janitorErr := c.repository.DeleteStagingOlderThan(ctx, c.now().Add(-c.cfg.StagingMaxAge))
	if janitorErr != nil {
		message := fmt.Sprintf("backup staging janitor: %v", janitorErr)
		slog.Warn("backup coordinator staging janitor failed", "backup_id", backupID, "error", janitorErr)
		c.recordMaintenanceError(message)
	}
	return &BackupRunResult{Task: task, Manifest: cloneBackupManifest(manifest)}, nil
}

func (c *BackupCoordinator) recoverInterruptedTasks(ctx context.Context, generation uint64) error {
	return c.metadata.ScanActiveBackupTasks(ctx, func(task BackupTask) error {
		if err := c.requireGeneration(ctx, generation); err != nil {
			return err
		}
		task.State = BackupTaskFailed
		task.LastError = "backup interrupted before completion; recovered by a later leader"
		task.CompletedAt = c.now()
		task.UpdatedAt = task.CompletedAt
		if err := c.persistTask(ctx, generation, &task); err != nil {
			return err
		}
		return nil
	})
}

func (c *BackupCoordinator) persistTask(ctx context.Context, generation uint64, desired *BackupTask) error {
	if err := c.requireGeneration(ctx, generation); err != nil {
		return err
	}
	err := c.metadata.putBackupTaskAtTerm(ctx, desired, generation)
	generationErr := c.requireGeneration(context.WithoutCancel(ctx), generation)
	if err == nil {
		return generationErr
	}
	if !errors.Is(err, ErrRaftConditionalOutcomeUnknown) &&
		!errors.Is(err, ErrBackupMetadataConflict) {
		return errors.Join(err, generationErr)
	}
	actual, readErr := c.metadata.GetBackupTask(context.WithoutCancel(ctx), desired.ID)
	if readErr != nil {
		return errors.Join(err, fmt.Errorf("re-read backup task: %w", readErr))
	}
	if actual != nil && backupTasksReconciled(*actual, *desired) {
		if generationErr != nil {
			return generationErr
		}
		return nil
	}
	return errors.Join(err, generationErr)
}

func (c *BackupCoordinator) reconcileRepository(
	ctx context.Context,
	generation uint64,
	current *BackupManifest,
) ([]CommittedBackup, error) {
	if err := c.requireGeneration(ctx, generation); err != nil {
		return nil, err
	}
	durable, err := c.metadata.GetBackupCatalogState(ctx)
	if err != nil {
		return nil, err
	}
	if err := c.requireGeneration(ctx, generation); err != nil {
		return nil, err
	}
	known := make(map[string]CommittedBackup)
	if durable != nil {
		for _, entry := range durable.Backups {
			known[entry.ID] = entry
		}
	}
	descriptors, err := c.repository.ListCommitted(ctx)
	if err != nil {
		return nil, errors.Join(err, c.requireGeneration(context.WithoutCancel(ctx), generation))
	}
	if err := c.requireGeneration(ctx, generation); err != nil {
		return nil, err
	}
	seen := make(map[string]struct{}, len(descriptors))
	backups := make([]CommittedBackup, 0, len(descriptors))
	for _, descriptor := range descriptors {
		if _, duplicate := seen[descriptor.ID]; duplicate {
			return nil, fmt.Errorf("duplicate repository backup ID %q", descriptor.ID)
		}
		seen[descriptor.ID] = struct{}{}

		var entry CommittedBackup
		switch {
		case current != nil && descriptor.ID == current.BackupID:
			if !descriptorMatchesManifest(descriptor, current) {
				return nil, fmt.Errorf("current repository descriptor differs from verified manifest for %q", descriptor.ID)
			}
			entry = committedBackupFromManifest(current)
		case descriptorMatchesCommitted(descriptor, known[descriptor.ID]):
			entry = known[descriptor.ID]
		default:
			manifest, verifyErr := c.fetchManifest(ctx, generation, descriptor.ID)
			if verifyErr != nil {
				return nil, verifyErr
			}
			if !descriptorMatchesManifest(descriptor, manifest) {
				return nil, fmt.Errorf("repository descriptor differs from manifest for %q", descriptor.ID)
			}
			entry = committedBackupFromManifest(manifest)
		}
		if entry.SourceClusterID != c.cfg.ClusterID {
			return nil, fmt.Errorf("foreign cluster backup %q belongs to %q", entry.ID, entry.SourceClusterID)
		}
		backups = append(backups, entry)
	}
	sort.Slice(backups, func(i, j int) bool {
		if backups[i].CreatedAt.Equal(backups[j].CreatedAt) {
			return backups[i].ID < backups[j].ID
		}
		return backups[i].CreatedAt.After(backups[j].CreatedAt)
	})
	if err := validateCoordinatorCatalog(backups); err != nil {
		return nil, err
	}
	if err := c.requireGeneration(ctx, generation); err != nil {
		return nil, err
	}
	reconciledAt := c.now()
	err = c.metadata.replaceCommittedBackupCatalogAtTerm(ctx, backups, reconciledAt, generation)
	generationErr := c.requireGeneration(context.WithoutCancel(ctx), generation)
	if err == nil {
		if generationErr != nil {
			return nil, generationErr
		}
		return backups, nil
	}
	if !errors.Is(err, ErrRaftConditionalOutcomeUnknown) &&
		!errors.Is(err, ErrBackupMetadataConflict) {
		return nil, errors.Join(err, generationErr)
	}
	latest, readErr := c.metadata.GetBackupCatalogState(context.WithoutCancel(ctx))
	if readErr == nil && latest != nil && committedBackupSlicesEqual(latest.Backups, backups) {
		if generationErr != nil {
			return nil, generationErr
		}
		return backups, nil
	}
	return nil, errors.Join(err, readErr, generationErr)
}

func (c *BackupCoordinator) prune(
	ctx context.Context,
	generation uint64,
	current *BackupManifest,
) error {
	if c.cfg.Retention < 1 {
		return fmt.Errorf("backup retention must be at least 1")
	}
	// Selection starts from a fresh, fully verified repository-authoritative
	// list whose exact contents have already been durably reconciled.
	backups, err := c.reconcileRepository(ctx, generation, current)
	if err != nil {
		return fmt.Errorf("reconcile backup catalog before pruning: %w", err)
	}
	currentFound := false
	for _, backup := range backups {
		if backup.ID == current.BackupID {
			currentFound = true
			break
		}
	}
	if !currentFound {
		return fmt.Errorf("current committed backup %q is absent from repository catalog", current.BackupID)
	}
	if len(backups) <= c.cfg.Retention {
		return nil
	}
	// Committed artifacts are immutable and sorted newest first. A candidate
	// already outside top-N can only become more obsolete as newer backups are
	// added. Repository Delete is idempotent, so a stale completion is limited
	// to an already-obsolete candidate and never targets current/top-N.
	for _, candidate := range backups[c.cfg.Retention:] {
		if candidate.ID == current.BackupID {
			return fmt.Errorf("current backup %q selected as obsolete", candidate.ID)
		}
		if err := c.requireGeneration(ctx, generation); err != nil {
			return err
		}
		if err := c.repository.Delete(ctx, candidate.ID); err != nil {
			_, reconcileErr := c.reconcileRepository(ctx, generation, current)
			return errors.Join(
				fmt.Errorf("delete obsolete backup %q: %w", candidate.ID, err),
				reconcileErr,
			)
		}
		if err := c.requireGeneration(ctx, generation); err != nil {
			return err
		}
	}
	return nil
}

func (c *BackupCoordinator) fetchAndVerify(
	ctx context.Context,
	generation uint64,
	backupID string,
	expected *BackupManifest,
) (bool, error) {
	manifest, err := c.fetchManifest(ctx, generation, backupID)
	if err != nil {
		return false, err
	}
	if !backupManifestsEqual(manifest, expected) {
		return false, fmt.Errorf("fetched manifest for %q differs from published manifest", backupID)
	}
	return true, nil
}

func (c *BackupCoordinator) fetchManifest(
	ctx context.Context,
	generation uint64,
	backupID string,
) (_ *BackupManifest, retErr error) {
	if err := c.requireGeneration(ctx, generation); err != nil {
		return nil, err
	}
	parent, err := os.MkdirTemp(c.cfg.LocalTempDir, "backup-verify-*")
	if err != nil {
		return nil, fmt.Errorf("create verification directory: %w", err)
	}
	defer func() {
		retErr = errors.Join(retErr, os.RemoveAll(parent))
	}()
	target := filepath.Join(parent, "artifact")
	manifest, err := c.repository.Fetch(ctx, backupID, target)
	if err != nil {
		return nil, errors.Join(err, c.requireGeneration(context.WithoutCancel(ctx), generation))
	}
	if err := c.requireGeneration(ctx, generation); err != nil {
		return nil, err
	}
	if manifest == nil || manifest.BackupID != backupID {
		return nil, fmt.Errorf("repository returned mismatched manifest for %q", backupID)
	}
	if _, err := c.verifyArtifact(ctx, target, manifest); err != nil {
		return nil, err
	}
	if err := c.requireGeneration(ctx, generation); err != nil {
		return nil, err
	}
	return cloneBackupManifest(manifest), nil
}

func (c *BackupCoordinator) requireGeneration(ctx context.Context, expected uint64) error {
	leader, current, err := c.readLeadership()
	if err != nil {
		return fmt.Errorf("%w: read current term: %v", ErrBackupCoordinatorLeadershipLost, err)
	}
	if !leader || current != expected {
		return fmt.Errorf(
			"%w: expected term %d, leader=%t current term=%d",
			ErrBackupCoordinatorLeadershipLost,
			expected,
			leader,
			current,
		)
	}
	return ctx.Err()
}

func (c *BackupCoordinator) readLeadership() (bool, uint64, error) {
	if c.currentLeadership == nil {
		return false, 0, fmt.Errorf("leadership provider is required")
	}
	return c.currentLeadership()
}

func (c *BackupCoordinator) recordMaintenanceError(message string) {
	c.mu.Lock()
	c.status.LastError = truncateBackupError(message)
	c.mu.Unlock()
}

func (c *BackupCoordinator) validateConfig() error {
	if c.metadata == nil {
		return fmt.Errorf("backup coordinator: metadata service is required")
	}
	if c.repository == nil {
		return fmt.Errorf("backup coordinator: repository is required")
	}
	if strings.TrimSpace(c.cfg.ClusterID) == "" {
		return fmt.Errorf("backup coordinator: cluster ID is required")
	}
	if c.cfg.Interval <= 0 {
		return fmt.Errorf("backup coordinator: interval must be positive")
	}
	if c.cfg.Retention < 1 {
		return fmt.Errorf("backup coordinator: retention must be at least 1")
	}
	if strings.TrimSpace(c.cfg.LocalTempDir) == "" {
		return fmt.Errorf("backup coordinator: local temp directory is required")
	}
	if c.cfg.StagingMaxAge <= 0 {
		return fmt.Errorf("backup coordinator: staging max age must be positive")
	}
	if c.cfg.UploadTimeout <= 0 {
		return fmt.Errorf("backup coordinator: upload timeout must be positive")
	}
	return nil
}

func newCoordinatorBackupID(now time.Time) (string, error) {
	var suffix [8]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		return "", err
	}
	return fmt.Sprintf(
		"backup-%s-%s",
		now.UTC().Format("20060102T150405.000000000Z"),
		hex.EncodeToString(suffix[:]),
	), nil
}

func committedBackupFromManifest(manifest *BackupManifest) CommittedBackup {
	return CommittedBackup{
		ID:              manifest.BackupID,
		SourceClusterID: manifest.SourceClusterID,
		CreatedAt:       manifest.CreatedAt,
		RaftTerm:        manifest.RaftTerm,
		AppliedIndex:    manifest.AppliedIndex,
		TotalBytes:      manifest.TotalBytes,
	}
}

func descriptorMatchesManifest(descriptor BackupDescriptor, manifest *BackupManifest) bool {
	return manifest != nil &&
		descriptor.ID == manifest.BackupID &&
		descriptor.CreatedAt.Equal(manifest.CreatedAt) &&
		descriptor.AppliedIndex == manifest.AppliedIndex &&
		descriptor.TotalBytes == manifest.TotalBytes
}

func descriptorMatchesCommitted(descriptor BackupDescriptor, backup CommittedBackup) bool {
	return backup.ID != "" &&
		descriptor.ID == backup.ID &&
		descriptor.CreatedAt.Equal(backup.CreatedAt) &&
		descriptor.AppliedIndex == backup.AppliedIndex &&
		descriptor.TotalBytes == backup.TotalBytes
}

func validateCoordinatorCatalog(backups []CommittedBackup) error {
	for i := len(backups) - 1; i > 0; i-- {
		older := backups[i]
		newer := backups[i-1]
		if newer.AppliedIndex < older.AppliedIndex {
			return fmt.Errorf("non-monotonic applied index between %q and %q", older.ID, newer.ID)
		}
		if newer.RaftTerm < older.RaftTerm {
			return fmt.Errorf("non-monotonic Raft term between %q and %q", older.ID, newer.ID)
		}
	}
	return nil
}

func backupTasksReconciled(actual, desired BackupTask) bool {
	return actual.ID == desired.ID &&
		actual.SourceClusterID == desired.SourceClusterID &&
		actual.OwnerNodeID == desired.OwnerNodeID &&
		actual.LeadershipTerm == desired.LeadershipTerm &&
		actual.AppliedIndex == desired.AppliedIndex &&
		actual.State == desired.State &&
		actual.StartedAt.Equal(desired.StartedAt) &&
		actual.CompletedAt.Equal(desired.CompletedAt) &&
		actual.BytesUploaded == desired.BytesUploaded &&
		actual.FilesUploaded == desired.FilesUploaded &&
		actual.LastError == desired.LastError
}

func backupManifestsEqual(a, b *BackupManifest) bool {
	if a == nil || b == nil {
		return a == b
	}
	if a.FormatVersion != b.FormatVersion ||
		a.BackupID != b.BackupID ||
		a.SourceClusterID != b.SourceClusterID ||
		!a.CreatedAt.Equal(b.CreatedAt) ||
		a.RaftTerm != b.RaftTerm ||
		a.AppliedIndex != b.AppliedIndex ||
		a.CheckpointFormat != b.CheckpointFormat ||
		a.MinimumNUFSVersion != b.MinimumNUFSVersion ||
		a.RecordCounts != b.RecordCounts ||
		a.TotalBytes != b.TotalBytes ||
		a.DurationMillis != b.DurationMillis ||
		len(a.Files) != len(b.Files) {
		return false
	}
	for i := range a.Files {
		if a.Files[i] != b.Files[i] {
			return false
		}
	}
	return true
}

func cloneBackupManifest(manifest *BackupManifest) *BackupManifest {
	if manifest == nil {
		return nil
	}
	out := *manifest
	out.Files = append([]BackupFile(nil), manifest.Files...)
	return &out
}

func cloneBackupRunResult(result *BackupRunResult) *BackupRunResult {
	if result == nil {
		return nil
	}
	out := *result
	out.Manifest = cloneBackupManifest(result.Manifest)
	return &out
}

func truncateBackupError(message string) string {
	message = strings.TrimSpace(message)
	if len(message) <= maxBackupTaskErrorBytes {
		return message
	}
	message = message[:maxBackupTaskErrorBytes]
	for !utf8.ValidString(message) {
		message = message[:len(message)-1]
	}
	return message
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
