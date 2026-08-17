package datanode

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// newTestConversionWorker wires the S1 serving path: ECService over a V2Store
// with attached shard stores, and the in-process Pebble store as both the
// conversion authority AND the worker's task meta. No publish hook — the
// worker-loop tests focus on task lifecycle + data movement; the chunk
// metadata flip (SwitchChunkToEC) is exercised end-to-end in the smoke test,
// where the chunk row genuinely exists from the S3 write path.
func newTestConversionWorker(t *testing.T, nodeID uint64, shardStoreCount int) (*V2Store, *ConversionWorker, *metadata.PebbleStore) {
	t.Helper()
	v, svc, ms := newTestECService(t, shardStoreCount)
	return v, NewConversionWorker(ms, svc, nodeID, 0), ms
}

func TestConversionWorker_SuccessPath(t *testing.T) {
	const nodeID = uint64(7)
	v, w, ms := newTestConversionWorker(t, nodeID, 3)
	ctx := context.Background()

	cid := metadata.ChunkID(22001)
	payload := []byte("ec-convert-worker-success-payload")
	for i := 0; i < 40; i++ {
		payload = append(payload, 0x42)
	}
	if err := v.Write(cid, payload); err != nil {
		t.Fatalf("write replicated chunk: %v", err)
	}

	taskID := fmt.Sprintf("ec-convert-%d", uint64(cid))
	task := &metadata.BackgroundTask{
		ID: taskID, Type: metadata.TaskECConvert, State: metadata.TaskQueued,
		Target: taskID, OwnerNodes: []uint64{nodeID},
	}
	if err := ms.PutBackgroundTask(ctx, task); err != nil {
		t.Fatalf("enqueue task: %v", err)
	}

	w.processConversionQueue(ctx)

	got, err := ms.GetBackgroundTask(ctx, taskID)
	if err != nil {
		t.Fatalf("get task: %v", err)
	}
	if got.State != metadata.TaskSucceeded {
		t.Fatalf("task state = %s, want %s (last error %q)", got.State, metadata.TaskSucceeded, got.LastError)
	}
	if c, f := w.Stats(); c != 1 || f != 0 {
		t.Fatalf("stats converted=%d failed=%d, want 1/0", c, f)
	}

	// The store now serves the converted chunk from the 6+3 shards.
	data, sum, err := v.ReadChunkEC(cid, len(payload))
	if err != nil {
		t.Fatalf("ReadChunkEC: %v", err)
	}
	if string(data) != string(payload) || sum == 0 {
		t.Fatalf("serving read mismatch after conversion (got %d bytes)", len(data))
	}
}

func TestConversionWorker_OwnershipFiltered(t *testing.T) {
	const nodeID = uint64(7)
	v, w, ms := newTestConversionWorker(t, nodeID, 3)
	ctx := context.Background()

	cid := metadata.ChunkID(22002)
	payload := []byte("owner-filtered-chunk")
	for i := 0; i < 20; i++ {
		payload = append(payload, 0x43)
	}
	if err := v.Write(cid, payload); err != nil {
		t.Fatalf("write replicated chunk: %v", err)
	}

	// Task owned by a DIFFERENT node: this worker must not touch it.
	taskID := fmt.Sprintf("ec-convert-%d", uint64(cid))
	task := &metadata.BackgroundTask{
		ID: taskID, Type: metadata.TaskECConvert, State: metadata.TaskQueued,
		Target: taskID, OwnerNodes: []uint64{nodeID + 1},
	}
	if err := ms.PutBackgroundTask(ctx, task); err != nil {
		t.Fatalf("enqueue task: %v", err)
	}

	w.processConversionQueue(ctx)

	got, err := ms.GetBackgroundTask(ctx, taskID)
	if err != nil {
		t.Fatalf("get task: %v", err)
	}
	if got.State != metadata.TaskQueued {
		t.Fatalf("task state = %s, want %s (must stay queued for the owner)", got.State, metadata.TaskQueued)
	}
	if c, f := w.Stats(); c != 0 || f != 0 {
		t.Fatalf("stats converted=%d failed=%d, want 0/0", c, f)
	}
}

func TestConversionWorker_FailureRetriesThenDeadLetters(t *testing.T) {
	const nodeID = uint64(7)
	v, w, ms := newTestConversionWorker(t, nodeID, 3)
	ctx := context.Background()

	// No local data for this chunk: the source read fails before Begin, so
	// the transaction never starts and the task is failed with retry.
	cid := metadata.ChunkID(22003)
	taskID := fmt.Sprintf("ec-convert-%d", uint64(cid))
	task := &metadata.BackgroundTask{
		ID: taskID, Type: metadata.TaskECConvert, State: metadata.TaskQueued,
		Target: taskID, OwnerNodes: []uint64{nodeID},
	}
	if err := ms.PutBackgroundTask(ctx, task); err != nil {
		t.Fatalf("enqueue task: %v", err)
	}

	runCycle := func(expectAttempt int) metadata.BackgroundTaskState {
		t.Helper()
		w.processConversionQueue(ctx)
		got, err := ms.GetBackgroundTask(ctx, taskID)
		if err != nil {
			t.Fatalf("get task: %v", err)
		}
		if got.AttemptCount != expectAttempt {
			t.Fatalf("attempt = %d, want %d", got.AttemptCount, expectAttempt)
		}
		// Make the retry backoff due so the next cycle re-leases it.
		got.NextRunAt = time.Now().Add(-time.Second).UnixNano()
		if err := ms.PutBackgroundTask(ctx, got); err != nil {
			t.Fatalf("rewind retry: %v", err)
		}
		return got.State
	}

	// Attempts 1 and 2 land in TaskRetrying; attempt 3 (== maxAttempts)
	// dead-letters the task.
	if st := runCycle(1); st != metadata.TaskRetrying {
		t.Fatalf("after 1st fail state = %s, want %s", st, metadata.TaskRetrying)
	}
	if st := runCycle(2); st != metadata.TaskRetrying {
		t.Fatalf("after 2nd fail state = %s, want %s", st, metadata.TaskRetrying)
	}
	if st := runCycle(3); st != metadata.TaskDeadLetter {
		t.Fatalf("after 3rd fail state = %s, want %s", st, metadata.TaskDeadLetter)
	}
	if c, f := w.Stats(); c != 0 || f != 3 {
		t.Fatalf("stats converted=%d failed=%d, want 0/3", c, f)
	}

	// The source chunk never existed locally — nothing should have been
	// written for it (no orphan shards).
	if _, _, err := v.ReadChunkEC(cid, 1); err == nil {
		t.Fatal("unexpected shard data after failed conversion")
	}
}

func TestConversionExtentFromTask(t *testing.T) {
	ok, err := conversionExtentFromTask(&metadata.BackgroundTask{Target: "ec-convert-123"})
	if err != nil || ok != 123 {
		t.Fatalf("parse ec-convert-123 = %d, %v", ok, err)
	}
	ok, err = conversionExtentFromTask(&metadata.BackgroundTask{Target: "ec-convert-18446744073709551615"})
	if err != nil || ok != ^uint64(0) {
		t.Fatalf("parse max uint64 = %d, %v", ok, err)
	}
	if _, err := conversionExtentFromTask(&metadata.BackgroundTask{Target: "chunk:9"}); err == nil {
		t.Fatal("expected error for non-ec-convert target")
	}
	if _, err := conversionExtentFromTask(&metadata.BackgroundTask{Target: "ec-convert-abc"}); err == nil {
		t.Fatal("expected error for non-numeric extent")
	}
	if _, err := conversionExtentFromTask(nil); err == nil {
		t.Fatal("expected error for nil task")
	}
}
