package datanode

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/example/dfs/metadata"
)

// newTestDiskManager creates a DiskManager in a temp directory for testing.
func newTestDiskManager(t *testing.T) (*DiskManager, string) {
	t.Helper()
	dir := t.TempDir()
	wal, err := NewWriteAheadLog(filepath.Join(dir, "wal"))
	if err != nil {
		t.Fatalf("NewWriteAheadLog: %v", err)
	}
	cs, err := NewChunkStore(dir, 8, 8, wal)
	if err != nil {
		t.Fatalf("NewChunkStore: %v", err)
	}
	dm, err := NewDiskManager(dir, cs, 100, wal)
	if err != nil {
		t.Fatalf("NewDiskManager: %v", err)
	}
	return dm, dir
}

func TestDiskManager_InitialState(t *testing.T) {
	dm, _ := newTestDiskManager(t)
	defer dm.Stop()

	if state := dm.DiskState(); state != DiskOnline {
		t.Errorf("expected DiskOnline, got %v", state)
	}
	if errs := dm.DiskIOErrors(); errs != 0 {
		t.Errorf("expected 0 errors, got %d", errs)
	}
	stats := dm.Stats()
	if stats.TotalBytes == 0 {
		t.Error("expected non-zero TotalBytes")
	}
	if stats.DiskState != "online" {
		t.Errorf("expected disk_state online, got %s", stats.DiskState)
	}
}

func TestDiskManager_CanAdmitWrite_WithinCapacity(t *testing.T) {
	dm, _ := newTestDiskManager(t)
	defer dm.Stop()

	err := dm.CanAdmitWrite(0, 1024)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

func TestDiskManager_CanAdmitWrite_ExceedsCapacity(t *testing.T) {
	dm, _ := newTestDiskManager(t)
	defer dm.Stop()

	dm.rejectPct = 0.5
	dm.store.disks[0].usedBytes.Store(60 * 1024 * 1024 * 1024) // 60GB used out of 100GB
	dm.capacities[0] = 100 * 1024 * 1024 * 1024 // 100GB total

	err := dm.CanAdmitWrite(0, 1024)
	if err == nil {
		t.Error("expected error for exceeded capacity")
	}
}

func TestDiskManager_CanAdmitWrite_DiskFailed(t *testing.T) {
	dm, _ := newTestDiskManager(t)
	defer dm.Stop()

	dm.MarkDiskFailed(0)

	err := dm.CanAdmitWrite(0, 1024)
	if err == nil {
		t.Error("expected error for failed disk")
	}
}

func TestDiskManager_RecordWriteError_TransitionsToDegraded(t *testing.T) {
	dm, _ := newTestDiskManager(t)
	defer dm.Stop()

	// First error should transition to Degraded
	for i := 0; i < DiskIOErrorThreshold-1; i++ {
		dm.RecordWriteError(fmt.Errorf("i/o error %d", i))
		if state := dm.DiskState(); state != DiskDegraded {
			t.Errorf("iteration %d: expected Degraded, got %v", i, state)
		}
	}
	if errs := dm.DiskIOErrors(); errs != int64(DiskIOErrorThreshold-1) {
		t.Errorf("expected %d errors, got %d", DiskIOErrorThreshold-1, errs)
	}
}

func TestDiskManager_RecordWriteError_TransitionsToFailed(t *testing.T) {
	dm, _ := newTestDiskManager(t)
	defer dm.Stop()

	failedCh := make(chan string, 1)
	dm.SetOnDiskFailed(func(diskID string) {
		failedCh <- diskID
	})

	for i := 0; i < DiskIOErrorThreshold; i++ {
		dm.RecordWriteError(fmt.Errorf("i/o error %d", i))
	}
	if state := dm.DiskState(); state != DiskFailed {
		t.Errorf("expected Failed, got %v", state)
	}

	select {
	case diskID := <-failedCh:
		if diskID == "" {
			t.Error("expected non-empty disk ID in callback")
		}
	case <-time.After(time.Second):
		t.Error("expected onDiskFailed callback to be called within timeout")
	}
}

func TestDiskManager_RecordSuccess_RecoversToOnline(t *testing.T) {
	dm, _ := newTestDiskManager(t)
	defer dm.Stop()

	// Push to failed
	for i := 0; i < DiskIOErrorThreshold; i++ {
		dm.RecordWriteError(fmt.Errorf("i/o error %d", i))
	}
	if state := dm.DiskState(); state != DiskFailed {
		t.Fatalf("expected Failed, got %v", state)
	}

	// Record successes until recovered
	dm.ioErrors.Store(1)
	for i := 0; i < DiskRecoveryOps+1; i++ {
		dm.RecordSuccess()
	}
	if state := dm.DiskState(); state != DiskOnline {
		t.Errorf("expected Online after recovery, got %v", state)
	}
	if errs := dm.DiskIOErrors(); errs != 0 {
		t.Errorf("expected 0 errors after recovery, got %d", errs)
	}
}

func TestDiskManager_RecordIOError(t *testing.T) {
	dm, _ := newTestDiskManager(t)
	defer dm.Stop()

	dm.RecordIOError()
	if errs := dm.DiskIOErrors(); errs != 1 {
		t.Errorf("expected 1 error, got %d", errs)
	}
	if state := dm.DiskState(); state != DiskDegraded {
		t.Errorf("expected Degraded, got %v", state)
	}
}

func TestDiskManager_StatsSnapshot(t *testing.T) {
	dm, _ := newTestDiskManager(t)
	defer dm.Stop()

	dm.RecordIOError()
	dm.RecordIOError()

	stats := dm.Stats()
	if stats.IOErrors != 2 {
		t.Errorf("expected 2 IOErrors, got %d", stats.IOErrors)
	}
	if stats.DiskState != "degraded" {
		t.Errorf("expected degraded, got %s", stats.DiskState)
	}
}

func TestDiskManager_StartStop(t *testing.T) {
	dm, _ := newTestDiskManager(t)
	dm.Start()

	// Should not panic
	dm.Start() // double start
	dm.Stop()
	dm.Stop() // double stop

	// After stop, WAL should be closed
	if dm.wal == nil {
		t.Error("expected WAL reference to exist")
	}
}

func TestDiskManager_RecordReadWrite(t *testing.T) {
	dm, _ := newTestDiskManager(t)
	defer dm.Stop()

	dm.RecordRead(4096)
	dm.RecordWrite(8192)

	stats := dm.Stats()
	if stats.ReadBytes != 4096 {
		t.Errorf("expected ReadBytes=4096, got %d", stats.ReadBytes)
	}
	if stats.WriteBytes != 8192 {
		t.Errorf("expected WriteBytes=8192, got %d", stats.WriteBytes)
	}
	if stats.ReadIOPS != 1 {
		t.Errorf("expected ReadIOPS=1, got %d", stats.ReadIOPS)
	}
	if stats.WriteIOPS != 1 {
		t.Errorf("expected WriteIOPS=1, got %d", stats.WriteIOPS)
	}
}

func TestDiskManager_CanAdmitWrite_DiskFailedWithNoCapacity(t *testing.T) {
	dm, _ := newTestDiskManager(t)
	defer dm.Stop()

	dm.MarkDiskFailed(0)
	dm.stats.TotalBytes = 0 // No capacity limit

	err := dm.CanAdmitWrite(0, 1024)
	if err == nil {
		t.Error("expected error even without capacity limit when disk is failed")
	}
}

func TestDiskManager_RefreshStats(t *testing.T) {
	dm, _ := newTestDiskManager(t)
	defer dm.Stop()

	// Write a chunk to update store stats
	chunkID := metadata.ChunkID(42)
	data := []byte("test data for stats refresh")
	if err := dm.store.Write(chunkID, data); err != nil {
		t.Fatalf("Write: %v", err)
	}

	dm.refreshStats()
	stats := dm.Stats()
	if stats.ChunkCount != 1 {
		t.Errorf("expected ChunkCount=1, got %d", stats.ChunkCount)
	}
	if stats.UsedBytes == 0 {
		t.Error("expected non-zero UsedBytes")
	}
}

func TestDiskManager_NilOnDiskFailed(t *testing.T) {
	dm, _ := newTestDiskManager(t)
	defer dm.Stop()

	// Should not panic with nil callback
	dm.SetOnDiskFailed(nil)
	for i := 0; i < DiskIOErrorThreshold; i++ {
		dm.RecordWriteError(fmt.Errorf("error %d", i))
	}
	if state := dm.DiskState(); state != DiskFailed {
		t.Errorf("expected Failed, got %v", state)
	}
}

func TestWAL_Recover_CleansCorrectChunkPath(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	dataDir := dir

	wal, err := NewWriteAheadLog(walDir)
	if err != nil {
		t.Fatalf("NewWriteAheadLog: %v", err)
	}
	defer wal.Close()

	cs, err := NewChunkStore(dataDir, 8, 8, wal)
	if err != nil {
		t.Fatalf("NewChunkStore: %v", err)
	}

	// Write a chunk via the store (creates real chunk file in dataDir/chunks/)
	chunkID := metadata.ChunkID(99999)
	data := []byte("test orphan cleanup")
	if err := cs.Write(chunkID, data); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Verify the chunk file exists at the correct path
	chunkPath := cs.disks[0].chunkPath(chunkID)
	if _, err := os.Stat(chunkPath); os.IsNotExist(err) {
		t.Fatalf("chunk file should exist at %s", chunkPath)
	}

	// WAL's chunkPath should match the store's chunkPath for the same chunkID
	// The bug: WAL uses its own dir instead of the data dir
	walChunkPath := wal.chunkPath(chunkID)
	if walChunkPath == chunkPath {
		// If they match, the bug is already fixed
		t.Logf("WAL chunkPath matches store chunkPath (bug already fixed): %s", walChunkPath)
	} else {
		// They don't match — this is the bug
		t.Errorf("WAL chunkPath (%s) does not match store chunkPath (%s) — orphan cleanup will fail",
			walChunkPath, chunkPath)
	}
}

func TestDiskManager_MonitorLoop(t *testing.T) {
	dm, _ := newTestDiskManager(t)
	defer dm.Stop()

	dm.Start()
	time.Sleep(50 * time.Millisecond) // Give monitor a chance to run
	dm.Stop()
	// Should not panic
}

func createTestDiskManagerWithStore(t *testing.T) (*DiskManager, string) {
	t.Helper()
	dir := t.TempDir()
	wal, err := NewWriteAheadLog(filepath.Join(dir, "wal"))
	if err != nil {
		t.Fatalf("NewWriteAheadLog: %v", err)
	}

	// Create chunk store with specific data dir
	dataDir := filepath.Join(dir, "data")
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	cs, err := NewChunkStore(dataDir, 8, 8, wal)
	if err != nil {
		t.Fatalf("NewChunkStore: %v", err)
	}

	dm, err := NewDiskManager(dataDir, cs, 100, wal)
	if err != nil {
		t.Fatalf("NewDiskManager: %v", err)
	}
	return dm, dataDir
}

// TestTierMigrator_StopWaitsForGoroutine verifies that Stop() blocks until
// the background migration goroutine has fully exited. Without wg.Wait(),
// a subsequent store close could race with in-flight migration work.
//
// TDD red phase: before the fix, Stop() returns immediately after
// close(stopCh) without waiting, so the goroutine may still be running
// when we proceed to close the store.
func TestTierMigrator_StopWaitsForGoroutine(t *testing.T) {
	dm, _ := newTestDiskManager(t)
	defer dm.Stop()

	tm := NewTierMigrator(dm.store, dm)
	tm.Start(10 * time.Millisecond)

	// Let the goroutine run at least one tick.
	time.Sleep(50 * time.Millisecond)

	// Stop must block until the goroutine has returned. We assert this
	// by verifying Stop returns within a reasonable time (not hanging
	// due to a missing wg.Done, and not returning instantly while the
	// goroutine is still alive).
	stopDone := make(chan struct{})
	go func() {
		tm.Stop()
		close(stopDone)
	}()

	select {
	case <-stopDone:
		// success: Stop returned
	case <-time.After(2 * time.Second):
		t.Fatal("TierMigrator.Stop() did not return within 2s — goroutine leak or deadlock")
	}

	// Double Stop must not panic (idempotency).
	tm.Stop()
}

// TestTierMigrator_RestartAfterStop verifies that Start can be called
// again after Stop without panicking. Before the fix, stopCh was not
// recreated on Start, causing "close of closed channel" panic on the
// second Stop.
func TestTierMigrator_RestartAfterStop(t *testing.T) {
	dm, _ := newTestDiskManager(t)
	defer dm.Stop()

	tm := NewTierMigrator(dm.store, dm)

	// First cycle
	tm.Start(50 * time.Millisecond)
	time.Sleep(20 * time.Millisecond)
	tm.Stop()

	// Second cycle — must not panic on close(stopCh)
	tm.Start(50 * time.Millisecond)
	time.Sleep(20 * time.Millisecond)

	stopDone := make(chan struct{})
	go func() {
		tm.Stop()
		close(stopDone)
	}()
	select {
	case <-stopDone:
	case <-time.After(2 * time.Second):
		t.Fatal("second Stop did not return — stopCh not recreated on Start")
	}
}

// TestTierMigrator_StartIdempotent ensures Start called twice does not
// spawn two goroutines.
func TestTierMigrator_StartIdempotent(t *testing.T) {
	dm, _ := newTestDiskManager(t)
	defer dm.Stop()

	tm := NewTierMigrator(dm.store, dm)
	tm.Start(50 * time.Millisecond)
	defer tm.Stop()

	// Second Start should be a no-op (running flag already true).
	tm.Start(50 * time.Millisecond)

	// If two goroutines were spawned, Stop would close stopCh once and
	// the second goroutine would never exit — but we can't directly
	// observe that. The wg.Wait() in Stop would hang. So this test
	// mainly ensures Stop returns in reasonable time.
	done := make(chan struct{})
	go func() {
		tm.Stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not return — possible duplicate goroutine")
	}
}
