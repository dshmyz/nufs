package metadata

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/hashicorp/raft"
)

type blockingCheckpointFuture struct {
	started chan struct{}
	unblock <-chan struct{}
}

func (f *blockingCheckpointFuture) Error() error {
	close(f.started)
	<-f.unblock
	return nil
}

type completedCheckpointFuture struct {
	err error
}

func (f completedCheckpointFuture) Error() error {
	return f.err
}

type blockingSnapshotSink struct {
	buf          bytes.Buffer
	writeStarted chan struct{}
	unblockWrite <-chan struct{}
	writeOnce    sync.Once
	canceled     atomic.Bool
}

func (s *blockingSnapshotSink) Write(p []byte) (int, error) {
	s.writeOnce.Do(func() {
		if s.writeStarted != nil {
			close(s.writeStarted)
		}
		if s.unblockWrite != nil {
			<-s.unblockWrite
		}
	})
	return s.buf.Write(p)
}

func (s *blockingSnapshotSink) Close() error {
	return nil
}

func (s *blockingSnapshotSink) ID() string {
	return "blocking-test"
}

func (s *blockingSnapshotSink) Cancel() error {
	s.canceled.Store(true)
	return nil
}

func openCheckpointDB(t *testing.T, dir string) *pebble.DB {
	t.Helper()
	db, err := pebble.Open(dir, &pebble.Options{ReadOnly: true})
	if err != nil {
		t.Fatalf("open checkpoint %s: %v", dir, err)
	}
	return db
}

func assertDBKey(t *testing.T, db *pebble.DB, key, want string) {
	t.Helper()
	value, closer, err := db.Get([]byte(key))
	if err != nil {
		t.Fatalf("get %q: %v", key, err)
	}
	defer closer.Close()
	if got := string(value); got != want {
		t.Fatalf("get %q = %q, want %q", key, got, want)
	}
}

func assertDBKeyMissing(t *testing.T, db *pebble.DB, key string) {
	t.Helper()
	_, closer, err := db.Get([]byte(key))
	if err == nil {
		closer.Close()
		t.Fatalf("key %q unexpectedly exists", key)
	}
	if !errors.Is(err, pebble.ErrNotFound) {
		t.Fatalf("get missing key %q: %v", key, err)
	}
}

func unusedRaftAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve raft address: %v", err)
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("release raft address: %v", err)
	}
	return addr
}

func newCheckpointRaftNode(t *testing.T, bootstrap bool) (*PebbleStore, *RaftNode) {
	t.Helper()
	store := newCheckpointStore(t)
	node, err := NewRaftNode(store, RaftNodeConfig{
		NodeID:             "checkpoint-test",
		BindAddr:           unusedRaftAddress(t),
		RaftDir:            t.TempDir(),
		Bootstrap:          bootstrap,
		HeartbeatTimeout:   100 * time.Millisecond,
		ElectionTimeout:    100 * time.Millisecond,
		LeaderLeaseTimeout: 50 * time.Millisecond,
		SnapshotInterval:   time.Hour,
	})
	if err != nil {
		t.Fatalf("NewRaftNode: %v", err)
	}
	return store, node
}

func TestCreateStandaloneCheckpointIsImmutable(t *testing.T) {
	store := newCheckpointStore(t)
	putTestKey(t, store, "before", "one")
	parentDir := t.TempDir()

	checkpoint, err := store.CreateStandaloneCheckpoint(context.Background(), parentDir)
	if err != nil {
		t.Fatal(err)
	}
	if checkpoint.Term != 0 || checkpoint.AppliedIndex != 0 {
		t.Fatalf("standalone position = term %d index %d, want zeroes", checkpoint.Term, checkpoint.AppliedIndex)
	}
	if filepath.Dir(checkpoint.Dir) != parentDir {
		t.Fatalf("checkpoint dir %q is not under %q", checkpoint.Dir, parentDir)
	}

	putTestKey(t, store, "after", "two")
	checkpointDB := openCheckpointDB(t, checkpoint.Dir)
	assertDBKey(t, checkpointDB, "before", "one")
	assertDBKeyMissing(t, checkpointDB, "after")
	if err := checkpointDB.Close(); err != nil {
		t.Fatal(err)
	}

	if err := checkpoint.Release(); err != nil {
		t.Fatalf("release checkpoint: %v", err)
	}
	if _, err := os.Stat(checkpoint.Dir); !os.IsNotExist(err) {
		t.Fatalf("checkpoint still exists after release: %v", err)
	}
	if err := checkpoint.Release(); err != nil {
		t.Fatalf("second release: %v", err)
	}
}

func TestCreateStandaloneCheckpointHonorsCanceledContext(t *testing.T) {
	store := newCheckpointStore(t)
	parentDir := filepath.Join(t.TempDir(), "not-created")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := store.CreateStandaloneCheckpoint(ctx, parentDir)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("CreateStandaloneCheckpoint error = %v, want context.Canceled", err)
	}
	if _, statErr := os.Stat(parentDir); !os.IsNotExist(statErr) {
		t.Fatalf("checkpoint parent was created: %v", statErr)
	}
}

func TestPebbleFSMTracksEveryConsumedLogPosition(t *testing.T) {
	store := newTestPebbleStore(t)
	fsm := &PebbleFSM{store: store}

	if result := fsm.Apply(&raft.Log{Index: 11, Term: 3}); result != nil {
		t.Fatalf("empty-data Apply result = %v", result)
	}
	if fsm.lastAppliedIndex != 11 || fsm.lastAppliedTerm != 3 {
		t.Fatalf("empty-data position = %d/%d, want 11/3", fsm.lastAppliedIndex, fsm.lastAppliedTerm)
	}

	if result := fsm.Apply(&raft.Log{Index: 12, Term: 4, Data: []byte{0xff}}); result == nil {
		t.Fatal("malformed Apply returned nil")
	}
	if fsm.lastAppliedIndex != 11 || fsm.lastAppliedTerm != 3 {
		t.Fatalf("failed-apply position = %d/%d, want retained 11/3", fsm.lastAppliedIndex, fsm.lastAppliedTerm)
	}

	invalidCAS := (&RaftLogEntry{Op: OpCAS, Key: []byte("cas"), Value: []byte{1}}).Encode()
	if result := fsm.Apply(&raft.Log{Index: 13, Term: 4, Data: invalidCAS}); result == nil {
		t.Fatal("invalid CAS Apply returned nil")
	}
	if fsm.lastAppliedIndex != 11 || fsm.lastAppliedTerm != 3 {
		t.Fatalf("application-error position = %d/%d, want retained 11/3", fsm.lastAppliedIndex, fsm.lastAppliedTerm)
	}

	marker := (&RaftLogEntry{Op: OpBatch}).Encode()
	decoded, err := DecodeRaftLogEntry(marker)
	if err != nil {
		t.Fatalf("decode empty batch marker: %v", err)
	}
	if decoded.Op != OpBatch || len(decoded.Batch) != 0 {
		t.Fatalf("decoded marker = %+v, want empty OpBatch", decoded)
	}
	if result := fsm.Apply(&raft.Log{Index: 14, Term: 5, Data: marker}); result != nil {
		t.Fatalf("empty batch marker Apply result = %v", result)
	}
	if fsm.lastAppliedIndex != 14 || fsm.lastAppliedTerm != 5 {
		t.Fatalf("marker position = %d/%d, want 14/5", fsm.lastAppliedIndex, fsm.lastAppliedTerm)
	}
}

func TestPebbleSnapshotPBL1DoesNotIncludeWritesAfterSnapshot(t *testing.T) {
	store := newTestPebbleStore(t)
	putTestKey(t, store, "before-pbl1", "one")
	snapshot, err := (&PebbleFSM{store: store}).Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	putTestKey(t, store, "after-pbl1", "two")

	data := persistSnapshotToBytes(t, snapshot)
	if got := string(data[:4]); got != "PBL1" {
		t.Fatalf("snapshot magic = %q, want PBL1", got)
	}
	restored := restoreSnapshotBytes(t, data)
	assertTestKey(t, restored, "before-pbl1", "one")
	assertTestKeyMissing(t, restored, "after-pbl1")
}

func TestPebblePBL1RestoreClearsPrimedInodeCache(t *testing.T) {
	store := newTestPebbleStore(t)
	defer store.Close()
	fsm := &PebbleFSM{store: store}
	snapshot, err := fsm.Snapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	data := persistSnapshotToBytes(t, snapshot)

	root, err := store.GetInode(context.Background(), RootInodeID)
	if err != nil {
		t.Fatalf("get root: %v", err)
	}
	root.Mode = 0600
	changed, err := marshalValue(root, codecMsgpack)
	if err != nil {
		t.Fatalf("encode changed root: %v", err)
	}
	if err := store.db.Set([]byte(fmt.Sprintf("%s%d", prefixInode, RootInodeID)), changed, pebble.Sync); err != nil {
		t.Fatalf("write changed root: %v", err)
	}
	store.inCache.clear()
	primed, err := store.GetInode(context.Background(), RootInodeID)
	if err != nil || primed.Mode != 0600 {
		t.Fatalf("prime changed root = (%#o, %v), want 0600", primed.Mode, err)
	}
	if err := fsm.Restore(io.NopCloser(bytes.NewReader(data))); err != nil {
		t.Fatalf("restore pbl1: %v", err)
	}
	restored, err := store.GetInode(context.Background(), RootInodeID)
	if err != nil || restored.Mode != 0755 {
		t.Fatalf("restored root = (%#o, %v), want 0755", restored.Mode, err)
	}
}

func TestPebbleSnapshotReleaseWaitsForConcurrentPersist(t *testing.T) {
	store := newCheckpointStore(t)
	putTestKey(t, store, "before", "one")
	snapshot, err := (&PebbleFSM{store: store}).Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	pebbleSnapshot := snapshot.(*PebbleSnapshot)

	writeStarted := make(chan struct{})
	unblockWrite := make(chan struct{})
	sink := &blockingSnapshotSink{writeStarted: writeStarted, unblockWrite: unblockWrite}
	persistDone := make(chan error, 1)
	go func() {
		persistDone <- snapshot.Persist(sink)
	}()
	select {
	case <-writeStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("Persist did not begin writing")
	}

	releaseDone := make(chan struct{})
	go func() {
		snapshot.Release()
		close(releaseDone)
	}()
	select {
	case <-releaseDone:
		t.Fatal("Release completed while Persist was blocked")
	case <-time.After(100 * time.Millisecond):
	}

	close(unblockWrite)
	if err := <-persistDone; err != nil {
		t.Fatalf("Persist: %v", err)
	}
	select {
	case <-releaseDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Release did not complete after Persist")
	}
	if _, err := os.Stat(pebbleSnapshot.checkpointDir); !os.IsNotExist(err) {
		t.Fatalf("checkpoint still exists after Release: %v", err)
	}
}

func TestPebbleSnapshotReleaseBeforePersistCancelsSink(t *testing.T) {
	store := newCheckpointStore(t)
	snapshot, err := (&PebbleFSM{store: store}).Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	pebbleSnapshot := snapshot.(*PebbleSnapshot)
	snapshot.Release()
	if _, err := os.Stat(pebbleSnapshot.checkpointDir); !os.IsNotExist(err) {
		t.Fatalf("checkpoint still exists after Release: %v", err)
	}

	sink := &blockingSnapshotSink{}
	err = snapshot.Persist(sink)
	if err == nil || !strings.Contains(err.Error(), "released") {
		t.Fatalf("Persist after Release error = %v, want released", err)
	}
	if !sink.canceled.Load() {
		t.Fatal("Persist after Release did not cancel sink")
	}
	if sink.buf.Len() != 0 {
		t.Fatalf("Persist after Release wrote %d bytes", sink.buf.Len())
	}
}

func TestPebbleSnapshotReleaseLogsCleanupFailure(t *testing.T) {
	parentDir := t.TempDir()
	checkpointDir := filepath.Join(parentDir, "checkpoint")
	if err := os.Mkdir(checkpointDir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(checkpointDir, "state"), []byte("data"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(checkpointDir, 0000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = os.Chmod(checkpointDir, 0700)
		_ = os.RemoveAll(checkpointDir)
	})

	var logs bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	defer slog.SetDefault(previousLogger)

	(&PebbleSnapshot{checkpointDir: checkpointDir}).Release()
	if _, err := os.Stat(checkpointDir); err != nil {
		t.Skipf("platform removed non-searchable directory; cannot exercise cleanup failure: %v", err)
	}
	if !strings.Contains(logs.String(), "failed to remove snapshot checkpoint") {
		t.Fatalf("Release log = %q, want cleanup warning", logs.String())
	}
}

func TestCreateBackupCheckpointMarkerEstablishesFSMPosition(t *testing.T) {
	_, node := newCheckpointRaftNode(t, true)
	if leader := waitForLeader([]*RaftNode{node}, 5*time.Second); leader == nil {
		t.Fatal("node did not become leader")
	}
	if node.fsm.lastAppliedIndex != 0 || node.fsm.lastAppliedTerm != 0 {
		t.Fatalf("initial FSM position = %d/%d, want zeroes", node.fsm.lastAppliedIndex, node.fsm.lastAppliedTerm)
	}

	checkpoint, err := node.CreateBackupCheckpoint(context.Background(), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer checkpoint.Release()
	if checkpoint.AppliedIndex == 0 || checkpoint.Term == 0 {
		t.Fatalf("marker position = %d/%d, want non-zeroes", checkpoint.AppliedIndex, checkpoint.Term)
	}
	if checkpoint.AppliedIndex != node.fsm.lastAppliedIndex || checkpoint.Term != node.fsm.lastAppliedTerm {
		t.Fatalf(
			"checkpoint position = %d/%d, FSM consumed = %d/%d",
			checkpoint.AppliedIndex,
			checkpoint.Term,
			node.fsm.lastAppliedIndex,
			node.fsm.lastAppliedTerm,
		)
	}
}

func TestCreateBackupCheckpointCapturesExactFSMBoundary(t *testing.T) {
	store, node := newCheckpointRaftNode(t, true)
	if leader := waitForLeader([]*RaftNode{node}, 5*time.Second); leader == nil {
		t.Fatal("node did not become leader")
	}
	if err := node.ApplySet([]byte("before"), []byte("one"), 2*time.Second); err != nil {
		t.Fatalf("apply before checkpoint: %v", err)
	}

	gateLocked := make(chan struct{})
	releaseGate := make(chan struct{})
	var wantIndex, wantTerm uint64
	node.backupCheckpointLockedHook = func() {
		wantIndex = node.fsm.lastAppliedIndex
		wantTerm = node.fsm.lastAppliedTerm
		close(gateLocked)
		<-releaseGate
	}

	type checkpointResult struct {
		checkpoint *PortableCheckpoint
		err        error
	}
	checkpointDone := make(chan checkpointResult, 1)
	go func() {
		checkpoint, err := node.CreateBackupCheckpoint(context.Background(), t.TempDir())
		checkpointDone <- checkpointResult{checkpoint: checkpoint, err: err}
	}()

	select {
	case <-gateLocked:
	case <-time.After(5 * time.Second):
		t.Fatal("checkpoint did not acquire FSM gate")
	}

	applyDone := make(chan error, 1)
	go func() {
		applyDone <- node.ApplySet([]byte("after"), []byte("two"), 2*time.Second)
	}()
	select {
	case err := <-applyDone:
		t.Fatalf("Apply completed while checkpoint held FSM gate: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	close(releaseGate)
	result := <-checkpointDone
	if result.err != nil {
		t.Fatal(result.err)
	}
	checkpoint := result.checkpoint
	defer checkpoint.Release()
	if err := <-applyDone; err != nil {
		t.Fatalf("apply after checkpoint: %v", err)
	}
	if checkpoint.AppliedIndex != wantIndex || checkpoint.Term != wantTerm {
		t.Fatalf(
			"checkpoint position = %d/%d, locked FSM boundary = %d/%d",
			checkpoint.AppliedIndex,
			checkpoint.Term,
			wantIndex,
			wantTerm,
		)
	}

	checkpointDB := openCheckpointDB(t, checkpoint.Dir)
	assertDBKey(t, checkpointDB, "before", "one")
	assertDBKeyMissing(t, checkpointDB, "after")
	if err := checkpointDB.Close(); err != nil {
		t.Fatal(err)
	}
	assertTestKey(t, store, "after", "two")
}

func TestCreateBackupCheckpointRejectsNonLeader(t *testing.T) {
	_, node := newCheckpointRaftNode(t, false)
	_, err := node.CreateBackupCheckpoint(context.Background(), t.TempDir())
	if err == nil || !strings.Contains(err.Error(), "not leader") {
		t.Fatalf("CreateBackupCheckpoint error = %v, want not leader", err)
	}
}

func TestCreateBackupCheckpointHonorsGateDeadline(t *testing.T) {
	_, node := newCheckpointRaftNode(t, true)
	if leader := waitForLeader([]*RaftNode{node}, 5*time.Second); leader == nil {
		t.Fatal("node did not become leader")
	}

	node.fsm.snapshotMu.Lock()
	defer node.fsm.snapshotMu.Unlock()
	parentDir := filepath.Join(t.TempDir(), "not-created")
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := node.CreateBackupCheckpoint(ctx, parentDir)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("CreateBackupCheckpoint error = %v, want context deadline", err)
	}
	if _, statErr := os.Stat(parentDir); !os.IsNotExist(statErr) {
		t.Fatalf("checkpoint parent was created: %v", statErr)
	}
}

func TestBackupFutureWaiterIsSingleFlightAfterCancellation(t *testing.T) {
	node := &RaftNode{}
	blocked := make(chan struct{})
	started := make(chan struct{})
	var futureStarts atomic.Int32
	factory := func() raft.Future {
		futureStarts.Add(1)
		return &blockingCheckpointFuture{started: started, unblock: blocked}
	}

	firstCtx, cancelFirst := context.WithCancel(context.Background())
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- node.waitBackupFuture(firstCtx, factory)
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first future waiter did not start")
	}
	cancelFirst()
	if err := <-firstDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("first wait error = %v, want context.Canceled", err)
	}

	for i := 0; i < 5; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		err := node.waitBackupFuture(ctx, factory)
		cancel()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("wait %d error = %v, want deadline", i, err)
		}
	}
	if got := futureStarts.Load(); got != 1 {
		t.Fatalf("future factory started %d waiters while slot occupied, want 1", got)
	}

	close(blocked)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := node.waitBackupFuture(ctx, func() raft.Future {
		futureStarts.Add(1)
		return completedCheckpointFuture{}
	}); err != nil {
		t.Fatalf("wait after releasing slot: %v", err)
	}
	if got := futureStarts.Load(); got != 2 {
		t.Fatalf("future factory starts = %d, want 2", got)
	}
}

func TestBackupFutureWaiterCancellationWhileCreatingFuture(t *testing.T) {
	node := &RaftNode{}
	factoryStarted := make(chan struct{})
	unblockFactory := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	waitDone := make(chan error, 1)
	go func() {
		waitDone <- node.waitBackupFuture(ctx, func() raft.Future {
			close(factoryStarted)
			<-unblockFactory
			return completedCheckpointFuture{}
		})
	}()
	select {
	case <-factoryStarted:
	case <-time.After(time.Second):
		t.Fatal("future factory did not start")
	}
	cancel()
	select {
	case err := <-waitDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("wait error = %v, want context.Canceled", err)
		}
		close(unblockFactory)
	case <-time.After(100 * time.Millisecond):
		close(unblockFactory)
		<-waitDone
		t.Fatal("cancellation waited for blocked future factory")
	}
}

func TestCreateBackupCheckpointCleansArtifactWhenLeadershipChanges(t *testing.T) {
	tests := []struct {
		name      string
		final     func(startTerm uint64) (bool, uint64, error)
		wantError string
	}{
		{
			name: "higher term after leadership regain",
			final: func(startTerm uint64) (bool, uint64, error) {
				return true, startTerm + 1, nil
			},
			wantError: "leadership changed",
		},
		{
			name: "final term unavailable",
			final: func(uint64) (bool, uint64, error) {
				return true, 0, errors.New("invalid final term")
			},
			wantError: "read final raft term",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, node := newCheckpointRaftNode(t, true)
			if leader := waitForLeader([]*RaftNode{node}, 5*time.Second); leader == nil {
				t.Fatal("node did not become leader")
			}
			startTerm, err := parseCheckpointTerm(node.raft.Stats())
			if err != nil {
				t.Fatal(err)
			}
			node.backupCheckpointFinalStateHook = func() (bool, uint64, error) {
				return test.final(startTerm)
			}

			parentDir := t.TempDir()
			checkpoint, err := node.CreateBackupCheckpoint(context.Background(), parentDir)
			if checkpoint != nil {
				t.Fatalf("checkpoint = %+v, want nil", checkpoint)
			}
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("CreateBackupCheckpoint error = %v, want %q", err, test.wantError)
			}
			entries, readErr := os.ReadDir(parentDir)
			if readErr != nil {
				t.Fatal(readErr)
			}
			if len(entries) != 0 {
				t.Fatalf("checkpoint artifacts remain after failure: %v", entries)
			}
		})
	}
}

func TestCheckpointTermParsingRejectsInvalidValues(t *testing.T) {
	for _, stats := range []map[string]string{
		{},
		{"term": ""},
		{"term": "not-a-number"},
	} {
		if _, err := parseCheckpointTerm(stats); err == nil {
			t.Fatalf("parseCheckpointTerm(%v) returned nil error", stats)
		}
	}
}
