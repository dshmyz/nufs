package metadata

import (
	"bytes"
	"container/heap"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/pebble"
)

const (
	maxBackupTaskListLimit  = 1000
	maxBackupTaskErrorBytes = 4096
	maxBackupOpaqueIDBytes  = 255
)

// ErrBackupMetadataConflict is returned when a durable conditional write lost
// a race with an earlier Raft/FSM mutation.
var ErrBackupMetadataConflict = errors.New("backup metadata conflict")

// BackupTaskState identifies one durable stage in the backup workflow.
type BackupTaskState string

const (
	BackupTaskCreating  BackupTaskState = "creating"
	BackupTaskUploading BackupTaskState = "uploading"
	BackupTaskVerifying BackupTaskState = "verifying"
	BackupTaskCommitted BackupTaskState = "committed"
	BackupTaskFailed    BackupTaskState = "failed"
)

// BackupTask is the durable execution record for one immutable backup run.
type BackupTask struct {
	ID              string          `json:"id" msgpack:"id"`
	SourceClusterID string          `json:"source_cluster_id" msgpack:"source_cluster_id"`
	OwnerNodeID     string          `json:"owner_node_id" msgpack:"owner_node_id"`
	LeadershipTerm  uint64          `json:"leadership_term" msgpack:"leadership_term"`
	AppliedIndex    uint64          `json:"applied_index" msgpack:"applied_index"`
	State           BackupTaskState `json:"state" msgpack:"state"`
	StartedAt       time.Time       `json:"started_at" msgpack:"started_at"`
	CompletedAt     time.Time       `json:"completed_at,omitempty" msgpack:"completed_at"`
	BytesUploaded   int64           `json:"bytes_uploaded" msgpack:"bytes_uploaded"`
	FilesUploaded   int             `json:"files_uploaded" msgpack:"files_uploaded"`
	LastError       string          `json:"last_error,omitempty" msgpack:"last_error"`
	UpdatedAt       time.Time       `json:"updated_at" msgpack:"updated_at"`
}

// CommittedBackup is the catalog's durable summary of a verified artifact.
type CommittedBackup struct {
	ID              string    `json:"id" msgpack:"id"`
	SourceClusterID string    `json:"source_cluster_id" msgpack:"source_cluster_id"`
	CreatedAt       time.Time `json:"created_at" msgpack:"created_at"`
	RaftTerm        uint64    `json:"raft_term" msgpack:"raft_term"`
	AppliedIndex    uint64    `json:"applied_index" msgpack:"applied_index"`
	TotalBytes      int64     `json:"total_bytes" msgpack:"total_bytes"`
}

// BackupCatalogState is the last repository-reconciled committed backup set.
type BackupCatalogState struct {
	Backups      []CommittedBackup `json:"backups" msgpack:"backups"`
	ReconciledAt time.Time         `json:"reconciled_at" msgpack:"reconciled_at"`
}

// RestorePendingMarker keeps a restored cluster unready until replica
// availability has been verified.
type RestorePendingMarker struct {
	BackupID        string    `json:"backup_id" msgpack:"backup_id"`
	SourceClusterID string    `json:"source_cluster_id" msgpack:"source_cluster_id"`
	AppliedIndex    uint64    `json:"applied_index" msgpack:"applied_index"`
	RestoredAt      time.Time `json:"restored_at" msgpack:"restored_at"`
}

type backupTaskHeap []BackupTask

func (h backupTaskHeap) Len() int { return len(h) }

func (h backupTaskHeap) Less(i, j int) bool {
	return backupTaskNewer(h[j], h[i])
}

func (h backupTaskHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *backupTaskHeap) Push(value interface{}) {
	*h = append(*h, value.(BackupTask))
}

func (h *backupTaskHeap) Pop() interface{} {
	old := *h
	last := old[len(old)-1]
	*h = old[:len(old)-1]
	return last
}

type backupTaskTopK struct {
	limit int
	tasks backupTaskHeap
}

func newBackupTaskTopK(limit int) *backupTaskTopK {
	return &backupTaskTopK{limit: limit, tasks: make(backupTaskHeap, 0, limit)}
}

func (top *backupTaskTopK) Add(task BackupTask) {
	if top.limit <= 0 {
		return
	}
	if len(top.tasks) < top.limit {
		heap.Push(&top.tasks, task)
		return
	}
	if backupTaskNewer(task, top.tasks[0]) {
		heap.Pop(&top.tasks)
		heap.Push(&top.tasks, task)
	}
}

func (top *backupTaskTopK) Len() int {
	return len(top.tasks)
}

func (top *backupTaskTopK) Sorted() []BackupTask {
	out := append([]BackupTask(nil), top.tasks...)
	sort.Slice(out, func(i, j int) bool {
		return backupTaskNewer(out[i], out[j])
	})
	return out
}

func backupTaskNewer(a, b BackupTask) bool {
	if a.StartedAt.Equal(b.StartedAt) {
		return a.ID < b.ID
	}
	return a.StartedAt.After(b.StartedAt)
}

var _ BackupMetadataService = (*PebbleStore)(nil)

// PutBackupTask creates or advances one task through the exact backup state
// machine. The raw value read here is only an expected-value precondition; the
// FSM evaluates it after all earlier Raft logs.
func (s *PebbleStore) PutBackupTask(ctx context.Context, task *BackupTask) error {
	return s.putBackupTask(ctx, task, 0)
}

// putBackupTaskAtTerm is the coordinator-only mutation path. The expected term
// is carried in the Raft command and checked by the FSM against raft.Log.Term.
func (s *PebbleStore) putBackupTaskAtTerm(
	ctx context.Context,
	task *BackupTask,
	expectedRaftTerm uint64,
) error {
	if expectedRaftTerm == 0 {
		return fmt.Errorf("backup task: expected Raft term is required")
	}
	return s.putBackupTask(ctx, task, expectedRaftTerm)
}

func (s *PebbleStore) putBackupTask(
	ctx context.Context,
	task *BackupTask,
	expectedRaftTerm uint64,
) error {
	if err := s.checkBackupMetadataCall(ctx); err != nil {
		return err
	}
	if err := s.checkBackupMetadataMutation(ctx); err != nil {
		return err
	}

	candidate, err := normalizeBackupTask(task)
	if err != nil {
		return err
	}

	key := prefixBackupTask + candidate.ID
	raw, found, err := s.readBackupMetadataRaw(key)
	if err != nil {
		return fmt.Errorf("backup task %q: read existing: %w", candidate.ID, err)
	}
	var existing BackupTask
	if !found {
		if candidate.State != BackupTaskCreating {
			return fmt.Errorf("backup task %q: initial state must be %q", candidate.ID, BackupTaskCreating)
		}
	} else {
		if err := unmarshalValue(raw, &existing); err != nil {
			return fmt.Errorf("backup task %q: decode existing: %w", candidate.ID, err)
		}
		if err := validateStoredBackupTask(&existing); err != nil {
			return fmt.Errorf("backup task %q: invalid durable record: %w", candidate.ID, err)
		}
		if existing.ID != candidate.ID {
			return fmt.Errorf("backup task %q: durable key identity mismatch", candidate.ID)
		}
		if isTerminalBackupTaskState(existing.State) {
			if terminalBackupTasksEqual(existing, candidate) {
				return nil
			}
			return fmt.Errorf("backup task %q: terminal record is immutable", candidate.ID)
		}
		if err := validateBackupTaskUpdate(existing, candidate); err != nil {
			return fmt.Errorf("backup task %q: %w", candidate.ID, err)
		}
	}

	encoded, err := marshalValue(&candidate, codecMsgpack)
	if err != nil {
		return fmt.Errorf("backup task %q: encode: %w", candidate.ID, err)
	}
	precondition := ConditionalPrecondition{Key: []byte(key)}
	if found {
		precondition.ExpectedValue = raw
	} else {
		precondition.ExpectAbsent = true
	}
	conditional := &ConditionalBatch{
		Version:          backupConditionalBatchVersion(expectedRaftTerm),
		ExpectedRaftTerm: expectedRaftTerm,
		Preconditions:    []ConditionalPrecondition{precondition},
		Mutations:        []BatchOp{{Key: []byte(key), Value: encoded}},
	}
	if err := s.applyBackupMetadataConditional(ctx, conditional); err != nil {
		if !errors.Is(err, ErrBackupMetadataConflict) {
			return fmt.Errorf("backup task %q: persist: %w", candidate.ID, err)
		}
		latestRaw, latestFound, readErr := s.readBackupMetadataRaw(key)
		if readErr != nil {
			return fmt.Errorf("backup task %q: reconcile conflict: %w", candidate.ID, readErr)
		}
		if latestFound {
			var latest BackupTask
			if decodeErr := unmarshalValue(latestRaw, &latest); decodeErr != nil {
				return fmt.Errorf("backup task %q: reconcile decode: %w", candidate.ID, decodeErr)
			}
			if validateErr := validateStoredBackupTask(&latest); validateErr != nil {
				return fmt.Errorf("backup task %q: reconcile invalid record: %w", candidate.ID, validateErr)
			}
			if isTerminalBackupTaskState(latest.State) && terminalBackupTasksEqual(latest, candidate) {
				return nil
			}
		}
		return fmt.Errorf("backup task %q: %w", candidate.ID, ErrBackupMetadataConflict)
	}
	return nil
}

// GetBackupTask returns one detached durable task, or nil when it is absent.
func (s *PebbleStore) GetBackupTask(ctx context.Context, id string) (*BackupTask, error) {
	if err := s.checkBackupMetadataCall(ctx); err != nil {
		return nil, err
	}
	if err := validateCatalogBackupID(id); err != nil {
		return nil, err
	}
	raw, found, err := s.readBackupMetadataRaw(prefixBackupTask + id)
	if err != nil {
		return nil, fmt.Errorf("backup task %q: read: %w", id, err)
	}
	if !found {
		return nil, nil
	}
	var task BackupTask
	if err := unmarshalValue(raw, &task); err != nil {
		return nil, fmt.Errorf("backup task %q: decode: %w", id, err)
	}
	if err := validateStoredBackupTask(&task); err != nil {
		return nil, fmt.Errorf("backup task %q: invalid durable record: %w", id, err)
	}
	if task.ID != id {
		return nil, fmt.Errorf("backup task %q: durable identity mismatch %q", id, task.ID)
	}
	return &task, nil
}

// ListBackupTasks returns a detached, deterministic newest-first task list.
func (s *PebbleStore) ListBackupTasks(ctx context.Context, limit int) ([]BackupTask, error) {
	if err := s.checkBackupMetadataCall(ctx); err != nil {
		return nil, err
	}
	if limit <= 0 || limit > maxBackupTaskListLimit {
		return nil, fmt.Errorf("backup tasks: limit must be between 1 and %d", maxBackupTaskListLimit)
	}

	top := newBackupTaskTopK(limit)
	err := s.scanPrefix(prefixBackupTask, func(key, value []byte) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		var task BackupTask
		if err := unmarshalValue(value, &task); err != nil {
			return fmt.Errorf("decode %q: %w", string(key), err)
		}
		if err := validateStoredBackupTask(&task); err != nil {
			return fmt.Errorf("invalid durable task %q: %w", string(key), err)
		}
		if string(key) != prefixBackupTask+task.ID {
			return fmt.Errorf("durable task %q has mismatched ID %q", string(key), task.ID)
		}
		top.Add(task)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("backup tasks: scan: %w", err)
	}
	return top.Sorted(), nil
}

// ScanActiveBackupTasks visits every nonterminal task while retaining only one
// decoded record at a time. Terminal task volume does not hide older orphaned
// work and does not increase caller memory usage.
func (s *PebbleStore) ScanActiveBackupTasks(ctx context.Context, visit func(BackupTask) error) error {
	if err := s.checkBackupMetadataCall(ctx); err != nil {
		return err
	}
	if visit == nil {
		return fmt.Errorf("backup tasks: active-task visitor is required")
	}
	err := s.scanPrefix(prefixBackupTask, func(key, value []byte) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		var task BackupTask
		if err := unmarshalValue(value, &task); err != nil {
			return fmt.Errorf("decode %q: %w", string(key), err)
		}
		if err := validateStoredBackupTask(&task); err != nil {
			return fmt.Errorf("invalid durable task %q: %w", string(key), err)
		}
		if string(key) != prefixBackupTask+task.ID {
			return fmt.Errorf("durable task %q has mismatched ID %q", string(key), task.ID)
		}
		if isTerminalBackupTaskState(task.State) {
			return nil
		}
		return visit(task)
	})
	if err != nil {
		return fmt.Errorf("backup tasks: scan active: %w", err)
	}
	return nil
}

// ReplaceCommittedBackupCatalog atomically replaces the durable catalog state,
// all per-backup entries, and all stale-entry deletions in one Raft/Pebble batch.
func (s *PebbleStore) ReplaceCommittedBackupCatalog(
	ctx context.Context,
	backups []CommittedBackup,
	reconciledAt time.Time,
) error {
	return s.replaceCommittedBackupCatalog(ctx, backups, reconciledAt, 0)
}

// replaceCommittedBackupCatalogAtTerm fences the coordinator's authoritative
// catalog replacement in the FSM, while the public Task 4 API remains v1.
func (s *PebbleStore) replaceCommittedBackupCatalogAtTerm(
	ctx context.Context,
	backups []CommittedBackup,
	reconciledAt time.Time,
	expectedRaftTerm uint64,
) error {
	if expectedRaftTerm == 0 {
		return fmt.Errorf("backup catalog: expected Raft term is required")
	}
	return s.replaceCommittedBackupCatalog(ctx, backups, reconciledAt, expectedRaftTerm)
}

func (s *PebbleStore) replaceCommittedBackupCatalog(
	ctx context.Context,
	backups []CommittedBackup,
	reconciledAt time.Time,
	expectedRaftTerm uint64,
) error {
	if err := s.checkBackupMetadataCall(ctx); err != nil {
		return err
	}

	if err := s.checkBackupMetadataMutation(ctx); err != nil {
		return err
	}

	state, err := normalizeBackupCatalog(backups, reconciledAt)
	if err != nil {
		return err
	}
	stateBytes, err := marshalValue(&state, codecMsgpack)
	if err != nil {
		return fmt.Errorf("backup catalog: encode state: %w", err)
	}
	replacement := ConditionalPrefixReplacement{
		Prefix: []byte(prefixBackupCatalog),
		Sets:   make([]BatchOp, 0, len(state.Backups)),
	}
	for i := range state.Backups {
		entry := state.Backups[i]
		encoded, encodeErr := marshalValue(&entry, codecMsgpack)
		if encodeErr != nil {
			return fmt.Errorf("backup catalog: encode entry %q: %w", entry.ID, encodeErr)
		}
		replacement.Sets = append(replacement.Sets, BatchOp{
			Key:   []byte(prefixBackupCatalog + entry.ID),
			Value: encoded,
		})
	}
	return s.applyBackupMetadataConditional(ctx, &ConditionalBatch{
		Version:            backupConditionalBatchVersion(expectedRaftTerm),
		ExpectedRaftTerm:   expectedRaftTerm,
		Mutations:          []BatchOp{{Key: []byte(keyBackupCatalog), Value: stateBytes}},
		PrefixReplacements: []ConditionalPrefixReplacement{replacement},
	})
}

// GetBackupCatalogState returns a detached catalog after verifying that its
// state record and per-backup index agree.
func (s *PebbleStore) GetBackupCatalogState(ctx context.Context) (*BackupCatalogState, error) {
	if err := s.checkBackupMetadataCall(ctx); err != nil {
		return nil, err
	}

	var state BackupCatalogState
	found, err := s.getValue(keyBackupCatalog, &state)
	if err != nil {
		return nil, fmt.Errorf("backup catalog: read state: %w", err)
	}
	if !found {
		return nil, nil
	}
	normalized, err := normalizeBackupCatalog(state.Backups, state.ReconciledAt)
	if err != nil {
		return nil, fmt.Errorf("backup catalog: invalid durable state: %w", err)
	}
	if !committedBackupSlicesEqual(state.Backups, normalized.Backups) ||
		!state.ReconciledAt.Equal(normalized.ReconciledAt) {
		return nil, fmt.Errorf("backup catalog: durable state is not canonical")
	}

	indexed := make(map[string]CommittedBackup, len(normalized.Backups))
	err = s.scanPrefix(prefixBackupCatalog, func(key, value []byte) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		var entry CommittedBackup
		if err := unmarshalValue(value, &entry); err != nil {
			return fmt.Errorf("decode %q: %w", string(key), err)
		}
		canonical, err := normalizeCommittedBackup(entry)
		if err != nil {
			return fmt.Errorf("invalid entry %q: %w", string(key), err)
		}
		if string(key) != prefixBackupCatalog+canonical.ID {
			return fmt.Errorf("entry %q has mismatched ID %q", string(key), canonical.ID)
		}
		if _, duplicate := indexed[canonical.ID]; duplicate {
			return fmt.Errorf("duplicate durable entry %q", canonical.ID)
		}
		indexed[canonical.ID] = canonical
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("backup catalog: scan entries: %w", err)
	}
	if len(indexed) != len(normalized.Backups) {
		return nil, fmt.Errorf("backup catalog: state/index entry count mismatch")
	}
	for _, expected := range normalized.Backups {
		actual, ok := indexed[expected.ID]
		if !ok || !committedBackupsEqual(actual, expected) {
			return nil, fmt.Errorf("backup catalog: state/index mismatch for %q", expected.ID)
		}
	}
	out := normalized
	out.Backups = append([]CommittedBackup(nil), normalized.Backups...)
	return &out, nil
}

// EnsureClusterID creates the durable cluster identity once, or verifies the
// caller's requested identity against the existing record.
func (s *PebbleStore) EnsureClusterID(ctx context.Context, requested string) (string, error) {
	if err := s.checkBackupMetadataCall(ctx); err != nil {
		return "", err
	}
	if err := s.checkBackupMetadataMutation(ctx); err != nil {
		return "", err
	}

	raw, found, err := s.readBackupMetadataRaw(keyClusterID)
	if err != nil {
		return "", fmt.Errorf("cluster ID: read durable identity: %w", err)
	}
	if found {
		return reconcileClusterID(raw, requested)
	}

	id := requested
	if id == "" {
		id, err = generateClusterID()
		if err != nil {
			return "", err
		}
	}
	if err := validateOpaqueID("cluster ID", id); err != nil {
		return "", err
	}
	encoded, err := marshalValue(id, codecMsgpack)
	if err != nil {
		return "", fmt.Errorf("cluster ID: encode: %w", err)
	}
	err = s.applyBackupMetadataConditional(ctx, &ConditionalBatch{
		Version: conditionalBatchVersion,
		Preconditions: []ConditionalPrecondition{{
			Key:          []byte(keyClusterID),
			ExpectAbsent: true,
		}},
		Mutations: []BatchOp{{Key: []byte(keyClusterID), Value: encoded}},
	})
	if errors.Is(err, ErrBackupMetadataConflict) {
		latest, latestFound, readErr := s.readBackupMetadataRaw(keyClusterID)
		if readErr != nil {
			return "", fmt.Errorf("cluster ID: reconcile conflict: %w", readErr)
		}
		if !latestFound {
			return "", fmt.Errorf("cluster ID: %w", ErrBackupMetadataConflict)
		}
		return reconcileClusterID(latest, requested)
	}
	if err != nil {
		return "", fmt.Errorf("cluster ID: persist: %w", err)
	}
	return id, nil
}

// PutRestorePendingMarker stores a validated, UTC-normalized readiness marker.
func (s *PebbleStore) PutRestorePendingMarker(ctx context.Context, marker *RestorePendingMarker) error {
	if err := s.checkBackupMetadataCall(ctx); err != nil {
		return err
	}
	if err := s.checkBackupMetadataMutation(ctx); err != nil {
		return err
	}

	candidate, err := normalizeRestorePendingMarker(marker)
	if err != nil {
		return err
	}
	raw, found, err := s.readBackupMetadataRaw(keyRestorePending)
	if err != nil {
		return fmt.Errorf("restore pending marker: read existing: %w", err)
	}
	if found {
		var existing RestorePendingMarker
		if err := unmarshalValue(raw, &existing); err != nil {
			return fmt.Errorf("restore pending marker: decode existing: %w", err)
		}
		canonical, err := normalizeRestorePendingMarker(&existing)
		if err != nil {
			return fmt.Errorf("restore pending marker: invalid durable record: %w", err)
		}
		if restorePendingMarkersEqual(canonical, candidate) {
			return nil
		}
		return fmt.Errorf(
			"restore pending marker differs from durable marker; clear it before replacement: %w",
			ErrBackupMetadataConflict,
		)
	}
	encoded, err := marshalValue(&candidate, codecMsgpack)
	if err != nil {
		return fmt.Errorf("restore pending marker: encode: %w", err)
	}
	precondition := ConditionalPrecondition{Key: []byte(keyRestorePending)}
	if found {
		precondition.ExpectedValue = raw
	} else {
		precondition.ExpectAbsent = true
	}
	err = s.applyBackupMetadataConditional(ctx, &ConditionalBatch{
		Version:       conditionalBatchVersion,
		Preconditions: []ConditionalPrecondition{precondition},
		Mutations:     []BatchOp{{Key: []byte(keyRestorePending), Value: encoded}},
	})
	if !errors.Is(err, ErrBackupMetadataConflict) {
		return err
	}
	latestRaw, latestFound, readErr := s.readBackupMetadataRaw(keyRestorePending)
	if readErr != nil {
		return fmt.Errorf("restore pending marker: reconcile conflict: %w", readErr)
	}
	if latestFound {
		var latest RestorePendingMarker
		if decodeErr := unmarshalValue(latestRaw, &latest); decodeErr != nil {
			return fmt.Errorf("restore pending marker: reconcile decode: %w", decodeErr)
		}
		canonical, normalizeErr := normalizeRestorePendingMarker(&latest)
		if normalizeErr != nil {
			return fmt.Errorf("restore pending marker: reconcile invalid record: %w", normalizeErr)
		}
		if restorePendingMarkersEqual(canonical, candidate) {
			return nil
		}
	}
	return fmt.Errorf("restore pending marker: %w", ErrBackupMetadataConflict)
}

// GetRestorePendingMarker returns nil when no restore readiness gate exists.
func (s *PebbleStore) GetRestorePendingMarker(ctx context.Context) (*RestorePendingMarker, error) {
	if err := s.checkBackupMetadataCall(ctx); err != nil {
		return nil, err
	}
	var marker RestorePendingMarker
	found, err := s.getValue(keyRestorePending, &marker)
	if err != nil {
		return nil, fmt.Errorf("restore pending marker: read: %w", err)
	}
	if !found {
		return nil, nil
	}
	normalized, err := normalizeRestorePendingMarker(&marker)
	if err != nil {
		return nil, fmt.Errorf("restore pending marker: invalid durable record: %w", err)
	}
	return &normalized, nil
}

// ClearRestorePendingMarker removes the readiness gate idempotently.
func (s *PebbleStore) ClearRestorePendingMarker(ctx context.Context) error {
	if err := s.checkBackupMetadataCall(ctx); err != nil {
		return err
	}
	if err := s.checkBackupMetadataMutation(ctx); err != nil {
		return err
	}
	raw, found, err := s.readBackupMetadataRaw(keyRestorePending)
	if err != nil {
		return fmt.Errorf("restore pending marker: read before clear: %w", err)
	}
	if !found {
		return nil
	}
	var marker RestorePendingMarker
	if err := unmarshalValue(raw, &marker); err != nil {
		return fmt.Errorf("restore pending marker: decode before clear: %w", err)
	}
	if _, err := normalizeRestorePendingMarker(&marker); err != nil {
		return fmt.Errorf("restore pending marker: invalid durable record: %w", err)
	}
	err = s.applyBackupMetadataConditional(ctx, &ConditionalBatch{
		Version: conditionalBatchVersion,
		Preconditions: []ConditionalPrecondition{{
			Key:           []byte(keyRestorePending),
			ExpectedValue: raw,
		}},
		Mutations: []BatchOp{{Delete: true, Key: []byte(keyRestorePending)}},
	})
	if !errors.Is(err, ErrBackupMetadataConflict) {
		return err
	}
	_, latestFound, readErr := s.readBackupMetadataRaw(keyRestorePending)
	if readErr != nil {
		return fmt.Errorf("restore pending marker: reconcile clear conflict: %w", readErr)
	}
	if !latestFound {
		return nil
	}
	return fmt.Errorf("restore pending marker: %w", ErrBackupMetadataConflict)
}

func (s *PebbleStore) readBackupMetadataRaw(key string) ([]byte, bool, error) {
	found, value, err := s.getRaw(key)
	return value, found, err
}

func (s *PebbleStore) applyBackupMetadataConditional(ctx context.Context, conditional *ConditionalBatch) error {
	if err := s.checkBackupMetadataCall(ctx); err != nil {
		return err
	}
	if s.degradation.IsReadOnly() {
		return ErrServiceClosed
	}
	startedAt := time.Now()
	var err error
	if s.raft != nil {
		if !s.raft.conditionalIsLeader() {
			return fmt.Errorf("backup metadata: not leader")
		}
		err = s.raft.applyConditional(ctx, &RaftLogEntry{
			Op:          OpConditionalBatch,
			Conditional: conditional,
		})
	} else {
		if conditional.ExpectedRaftTerm != 0 {
			return fmt.Errorf("backup metadata: term-fenced mutation requires Raft")
		}
		s.mu.Lock()
		err = ctx.Err()
		if err == nil {
			err = applyConditionalBatch(s.db, conditional, pebble.Sync)
		}
		s.mu.Unlock()
	}
	if errors.Is(err, ErrRaftConditionalConflict) {
		err = ErrBackupMetadataConflict
	}
	if err == nil && s.metrics != nil {
		s.metrics.RecordWrite(time.Since(startedAt))
	}
	return err
}

func backupConditionalBatchVersion(expectedRaftTerm uint64) uint8 {
	if expectedRaftTerm != 0 {
		return conditionalBatchTermFencedVersion
	}
	return conditionalBatchVersion
}

func validateTermFencedBackupConditionalBatch(conditional *ConditionalBatch) error {
	switch len(conditional.PrefixReplacements) {
	case 0:
		return validateTermFencedBackupTaskBatch(conditional)
	case 1:
		return validateTermFencedBackupCatalogBatch(conditional)
	default:
		return fmt.Errorf("term-fenced conditional batch has an unsupported shape")
	}
}

func validateTermFencedBackupTaskBatch(conditional *ConditionalBatch) error {
	if len(conditional.Preconditions) != 1 ||
		len(conditional.Mutations) != 1 ||
		len(conditional.PrefixReplacements) != 0 {
		return fmt.Errorf("term-fenced backup task batch must contain one precondition and one mutation")
	}
	precondition := conditional.Preconditions[0]
	mutation := conditional.Mutations[0]
	if mutation.Delete || !bytes.Equal(precondition.Key, mutation.Key) {
		return fmt.Errorf("term-fenced backup task batch must set its precondition key")
	}
	key := string(mutation.Key)
	if !strings.HasPrefix(key, prefixBackupTask) {
		return fmt.Errorf("term-fenced backup task key is outside the task namespace")
	}
	id := strings.TrimPrefix(key, prefixBackupTask)
	if err := validateCatalogBackupID(id); err != nil {
		return fmt.Errorf("term-fenced backup task key: %w", err)
	}

	var desired BackupTask
	if err := unmarshalValue(mutation.Value, &desired); err != nil {
		return fmt.Errorf("term-fenced backup task value: %w", err)
	}
	if err := validateStoredBackupTask(&desired); err != nil {
		return fmt.Errorf("term-fenced backup task value: %w", err)
	}
	if desired.ID != id {
		return fmt.Errorf("term-fenced backup task key and value identities differ")
	}
	canonicalDesired, err := marshalValue(&desired, codecMsgpack)
	if err != nil {
		return fmt.Errorf("term-fenced backup task value: %w", err)
	}
	if !bytes.Equal(canonicalDesired, mutation.Value) {
		return fmt.Errorf("term-fenced backup task value is not canonical")
	}

	if precondition.ExpectAbsent {
		if desired.State != BackupTaskCreating {
			return fmt.Errorf("term-fenced backup task initial state must be %q", BackupTaskCreating)
		}
		return nil
	}
	var existing BackupTask
	if err := unmarshalValue(precondition.ExpectedValue, &existing); err != nil {
		return fmt.Errorf("term-fenced backup task precondition: %w", err)
	}
	if err := validateStoredBackupTask(&existing); err != nil {
		return fmt.Errorf("term-fenced backup task precondition: %w", err)
	}
	if existing.ID != id {
		return fmt.Errorf("term-fenced backup task precondition identity differs from its key")
	}
	if isTerminalBackupTaskState(existing.State) {
		return fmt.Errorf("term-fenced backup task cannot mutate a terminal record")
	}
	if err := validateBackupTaskUpdate(existing, desired); err != nil {
		return fmt.Errorf("term-fenced backup task transition: %w", err)
	}
	return nil
}

func validateTermFencedBackupCatalogBatch(conditional *ConditionalBatch) error {
	if len(conditional.Preconditions) != 0 ||
		len(conditional.Mutations) != 1 ||
		len(conditional.PrefixReplacements) != 1 {
		return fmt.Errorf("term-fenced backup catalog batch has an invalid shape")
	}
	mutation := conditional.Mutations[0]
	replacement := conditional.PrefixReplacements[0]
	if mutation.Delete ||
		!bytes.Equal(mutation.Key, []byte(keyBackupCatalog)) ||
		!bytes.Equal(replacement.Prefix, []byte(prefixBackupCatalog)) {
		return fmt.Errorf("term-fenced backup catalog batch targets an invalid namespace")
	}

	var state BackupCatalogState
	if err := unmarshalValue(mutation.Value, &state); err != nil {
		return fmt.Errorf("term-fenced backup catalog state: %w", err)
	}
	normalized, err := normalizeBackupCatalog(state.Backups, state.ReconciledAt)
	if err != nil {
		return fmt.Errorf("term-fenced backup catalog state: %w", err)
	}
	canonicalState, err := marshalValue(&normalized, codecMsgpack)
	if err != nil {
		return fmt.Errorf("term-fenced backup catalog state: %w", err)
	}
	if !bytes.Equal(canonicalState, mutation.Value) {
		return fmt.Errorf("term-fenced backup catalog state is not canonical")
	}
	if len(replacement.Sets) != len(normalized.Backups) {
		return fmt.Errorf("term-fenced backup catalog entries differ from catalog state")
	}

	expected := make(map[string][]byte, len(normalized.Backups))
	for i := range normalized.Backups {
		entry := normalized.Backups[i]
		encoded, encodeErr := marshalValue(&entry, codecMsgpack)
		if encodeErr != nil {
			return fmt.Errorf("term-fenced backup catalog entry %q: %w", entry.ID, encodeErr)
		}
		expected[prefixBackupCatalog+entry.ID] = encoded
	}
	for _, set := range replacement.Sets {
		encoded, ok := expected[string(set.Key)]
		if !ok || set.Delete || !bytes.Equal(encoded, set.Value) {
			return fmt.Errorf("term-fenced backup catalog contains an invalid entry")
		}
	}
	return nil
}

func reconcileClusterID(raw []byte, requested string) (string, error) {
	var existing string
	if err := unmarshalValue(raw, &existing); err != nil {
		return "", fmt.Errorf("cluster ID: decode durable identity: %w", err)
	}
	if err := validateOpaqueID("cluster ID", existing); err != nil {
		return "", fmt.Errorf("cluster ID: malformed durable identity: %w", err)
	}
	if requested == "" || requested == existing {
		return existing, nil
	}
	if err := validateOpaqueID("requested cluster ID", requested); err != nil {
		return "", err
	}
	return "", fmt.Errorf(
		"cluster ID: requested %q conflicts with durable identity %q: %w",
		requested,
		existing,
		ErrBackupMetadataConflict,
	)
}

func (s *PebbleStore) checkBackupMetadataCall(ctx context.Context) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}
	if ctx == nil {
		return fmt.Errorf("backup metadata: nil context")
	}
	return ctx.Err()
}

func (s *PebbleStore) checkBackupMetadataMutation(ctx context.Context) error {
	if err := s.checkBackupMetadataCall(ctx); err != nil {
		return err
	}
	if s.raft != nil && !s.raft.conditionalIsLeader() {
		return fmt.Errorf("backup metadata: not leader")
	}
	return nil
}

func normalizeBackupTask(task *BackupTask) (BackupTask, error) {
	if task == nil {
		return BackupTask{}, fmt.Errorf("backup task: record is required")
	}
	out := *task
	if err := validateCatalogBackupID(out.ID); err != nil {
		return BackupTask{}, err
	}
	if err := validateOpaqueID("backup task source cluster ID", out.SourceClusterID); err != nil {
		return BackupTask{}, err
	}
	if err := validateOpaqueID("backup task owner node ID", out.OwnerNodeID); err != nil {
		return BackupTask{}, err
	}
	out.StartedAt = normalizeUTCTime(out.StartedAt)
	out.CompletedAt = normalizeUTCTime(out.CompletedAt)
	out.UpdatedAt = normalizeUTCTime(out.UpdatedAt)
	if out.UpdatedAt.IsZero() {
		out.UpdatedAt = out.StartedAt
		if out.CompletedAt.After(out.UpdatedAt) {
			out.UpdatedAt = out.CompletedAt
		}
	}
	if err := validateBackupTaskFields(out); err != nil {
		return BackupTask{}, err
	}
	return out, nil
}

func validateStoredBackupTask(task *BackupTask) error {
	if task == nil || task.UpdatedAt.IsZero() {
		return fmt.Errorf("updated time is required")
	}
	normalized, err := normalizeBackupTask(task)
	if err != nil {
		return err
	}
	*task = normalized
	return nil
}

func validateBackupTaskFields(task BackupTask) error {
	if task.LeadershipTerm == 0 {
		return fmt.Errorf("backup task: leadership term must be non-zero")
	}
	if task.AppliedIndex == 0 {
		return fmt.Errorf("backup task: applied index must be non-zero")
	}
	if task.StartedAt.IsZero() {
		return fmt.Errorf("backup task: start time is required")
	}
	if task.BytesUploaded < 0 || task.FilesUploaded < 0 {
		return fmt.Errorf("backup task: progress counters cannot be negative")
	}
	if len([]byte(task.LastError)) > maxBackupTaskErrorBytes {
		return fmt.Errorf("backup task: last error exceeds %d bytes", maxBackupTaskErrorBytes)
	}
	if task.UpdatedAt.Before(task.StartedAt) {
		return fmt.Errorf("backup task: updated time precedes start time")
	}
	switch task.State {
	case BackupTaskCreating, BackupTaskUploading, BackupTaskVerifying:
		if !task.CompletedAt.IsZero() {
			return fmt.Errorf("backup task: active task cannot have a completion time")
		}
		if task.LastError != "" {
			return fmt.Errorf("backup task: active task cannot have a last error")
		}
	case BackupTaskCommitted:
		if task.CompletedAt.IsZero() {
			return fmt.Errorf("backup task: committed task requires a completion time")
		}
		if task.LastError != "" {
			return fmt.Errorf("backup task: committed task cannot have a last error")
		}
	case BackupTaskFailed:
		if task.CompletedAt.IsZero() {
			return fmt.Errorf("backup task: failed task requires a completion time")
		}
		if strings.TrimSpace(task.LastError) == "" {
			return fmt.Errorf("backup task: failed task requires a last error")
		}
	default:
		return fmt.Errorf("backup task: unknown state %q", task.State)
	}
	if !task.CompletedAt.IsZero() {
		if task.CompletedAt.Before(task.StartedAt) {
			return fmt.Errorf("backup task: completion time precedes start time")
		}
		if task.UpdatedAt.Before(task.CompletedAt) {
			return fmt.Errorf("backup task: updated time precedes completion time")
		}
	}
	return nil
}

func validateBackupTaskUpdate(existing, candidate BackupTask) error {
	if existing.SourceClusterID != candidate.SourceClusterID ||
		existing.OwnerNodeID != candidate.OwnerNodeID ||
		existing.LeadershipTerm != candidate.LeadershipTerm ||
		existing.AppliedIndex != candidate.AppliedIndex ||
		!existing.StartedAt.Equal(candidate.StartedAt) {
		return fmt.Errorf("immutable identity fields changed")
	}
	if candidate.BytesUploaded < existing.BytesUploaded ||
		candidate.FilesUploaded < existing.FilesUploaded {
		return fmt.Errorf("progress counters cannot decrease")
	}
	if candidate.UpdatedAt.Before(existing.UpdatedAt) {
		return fmt.Errorf("updated time cannot move backward")
	}
	if candidate.State == existing.State {
		return nil
	}
	if candidate.State == BackupTaskFailed {
		return nil
	}
	next := map[BackupTaskState]BackupTaskState{
		BackupTaskCreating:  BackupTaskUploading,
		BackupTaskUploading: BackupTaskVerifying,
		BackupTaskVerifying: BackupTaskCommitted,
	}
	if next[existing.State] != candidate.State {
		return fmt.Errorf("invalid state transition %q -> %q", existing.State, candidate.State)
	}
	return nil
}

func terminalBackupTasksEqual(a, b BackupTask) bool {
	return a.ID == b.ID &&
		a.SourceClusterID == b.SourceClusterID &&
		a.OwnerNodeID == b.OwnerNodeID &&
		a.LeadershipTerm == b.LeadershipTerm &&
		a.AppliedIndex == b.AppliedIndex &&
		a.State == b.State &&
		a.StartedAt.Equal(b.StartedAt) &&
		a.CompletedAt.Equal(b.CompletedAt) &&
		a.BytesUploaded == b.BytesUploaded &&
		a.FilesUploaded == b.FilesUploaded &&
		a.LastError == b.LastError
}

func isTerminalBackupTaskState(state BackupTaskState) bool {
	return state == BackupTaskCommitted || state == BackupTaskFailed
}

func normalizeBackupCatalog(backups []CommittedBackup, reconciledAt time.Time) (BackupCatalogState, error) {
	if reconciledAt.IsZero() {
		return BackupCatalogState{}, fmt.Errorf("backup catalog: reconciliation time is required")
	}
	out := BackupCatalogState{
		Backups:      make([]CommittedBackup, len(backups)),
		ReconciledAt: normalizeUTCTime(reconciledAt),
	}
	seen := make(map[string]struct{}, len(backups))
	for i := range backups {
		entry, err := normalizeCommittedBackup(backups[i])
		if err != nil {
			return BackupCatalogState{}, fmt.Errorf("backup catalog entry %d: %w", i, err)
		}
		if _, duplicate := seen[entry.ID]; duplicate {
			return BackupCatalogState{}, fmt.Errorf("backup catalog: duplicate backup ID %q", entry.ID)
		}
		seen[entry.ID] = struct{}{}
		out.Backups[i] = entry
	}
	sort.Slice(out.Backups, func(i, j int) bool {
		if out.Backups[i].CreatedAt.Equal(out.Backups[j].CreatedAt) {
			return out.Backups[i].ID < out.Backups[j].ID
		}
		return out.Backups[i].CreatedAt.After(out.Backups[j].CreatedAt)
	})
	return out, nil
}

func normalizeCommittedBackup(entry CommittedBackup) (CommittedBackup, error) {
	if err := validateCatalogBackupID(entry.ID); err != nil {
		return CommittedBackup{}, err
	}
	if err := validateOpaqueID("committed backup source cluster ID", entry.SourceClusterID); err != nil {
		return CommittedBackup{}, err
	}
	entry.CreatedAt = normalizeUTCTime(entry.CreatedAt)
	if entry.CreatedAt.IsZero() {
		return CommittedBackup{}, fmt.Errorf("committed backup: creation time is required")
	}
	if entry.RaftTerm == 0 {
		return CommittedBackup{}, fmt.Errorf("committed backup: Raft term must be non-zero")
	}
	if entry.AppliedIndex == 0 {
		return CommittedBackup{}, fmt.Errorf("committed backup: applied index must be non-zero")
	}
	if entry.TotalBytes < 0 {
		return CommittedBackup{}, fmt.Errorf("committed backup: total bytes cannot be negative")
	}
	return entry, nil
}

func normalizeRestorePendingMarker(marker *RestorePendingMarker) (RestorePendingMarker, error) {
	if marker == nil {
		return RestorePendingMarker{}, fmt.Errorf("restore pending marker: record is required")
	}
	out := *marker
	if err := validateCatalogBackupID(out.BackupID); err != nil {
		return RestorePendingMarker{}, err
	}
	if err := validateOpaqueID("restore source cluster ID", out.SourceClusterID); err != nil {
		return RestorePendingMarker{}, err
	}
	if out.AppliedIndex == 0 {
		return RestorePendingMarker{}, fmt.Errorf("restore pending marker: applied index must be non-zero")
	}
	out.RestoredAt = normalizeUTCTime(out.RestoredAt)
	if out.RestoredAt.IsZero() {
		return RestorePendingMarker{}, fmt.Errorf("restore pending marker: restore time is required")
	}
	return out, nil
}

func restorePendingMarkersEqual(a, b RestorePendingMarker) bool {
	return a.BackupID == b.BackupID &&
		a.SourceClusterID == b.SourceClusterID &&
		a.AppliedIndex == b.AppliedIndex &&
		a.RestoredAt.Equal(b.RestoredAt)
}

func committedBackupsEqual(a, b CommittedBackup) bool {
	return a.ID == b.ID &&
		a.SourceClusterID == b.SourceClusterID &&
		a.CreatedAt.Equal(b.CreatedAt) &&
		a.RaftTerm == b.RaftTerm &&
		a.AppliedIndex == b.AppliedIndex &&
		a.TotalBytes == b.TotalBytes
}

func committedBackupSlicesEqual(a, b []CommittedBackup) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !committedBackupsEqual(a[i], b[i]) {
			return false
		}
	}
	return true
}

func validateOpaqueID(field, id string) error {
	if id == "" {
		return fmt.Errorf("%s is required", field)
	}
	if len(id) > maxBackupOpaqueIDBytes {
		return fmt.Errorf("%s exceeds %d bytes", field, maxBackupOpaqueIDBytes)
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		isAlphaNumeric := c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9'
		if i == 0 && !isAlphaNumeric {
			return fmt.Errorf("%s must start with an ASCII letter or digit", field)
		}
		if !isAlphaNumeric && c != '-' && c != '_' && c != '.' && c != ':' {
			return fmt.Errorf("%s contains unsafe byte 0x%02x", field, c)
		}
	}
	return nil
}

func validateCatalogBackupID(id string) error {
	if err := validateBackupID(id); err != nil {
		return err
	}
	if err := validateOpaqueID("backup ID", id); err != nil {
		return err
	}
	return nil
}

func generateClusterID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("cluster ID: generate random identity: %w", err)
	}
	raw[6] = raw[6]&0x0f | 0x40
	raw[8] = raw[8]&0x3f | 0x80
	var encoded [32]byte
	hex.Encode(encoded[:], raw[:])
	return fmt.Sprintf(
		"%s-%s-%s-%s-%s",
		encoded[0:8],
		encoded[8:12],
		encoded[12:16],
		encoded[16:20],
		encoded[20:32],
	), nil
}

func normalizeUTCTime(value time.Time) time.Time {
	if value.IsZero() {
		return time.Time{}
	}
	return value.Round(0).UTC()
}
