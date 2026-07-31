package s3

import (
	"context"
	"net"
	"os"
	"testing"
	"time"

	"github.com/example/dfs/chunkstore"
	"github.com/example/dfs/metadata"
)

func TestRunStartsObjectWriteBackgroundWorkers(t *testing.T) {
	ctx := context.Background()
	meta := newMockMetaService()
	if err := meta.PutBackgroundTask(ctx, &metadata.BackgroundTask{
		ID:     "write-recovery-task",
		Type:   metadata.TaskWriteRecovery,
		State:  metadata.TaskQueued,
		Target: "object-write-recovery",
	}); err != nil {
		t.Fatalf("put recovery task: %v", err)
	}
	if err := meta.PutBackgroundTask(ctx, &metadata.BackgroundTask{
		ID:     "write-gc-task",
		Type:   metadata.TaskWriteGC,
		State:  metadata.TaskQueued,
		Target: "object-write-gc",
	}); err != nil {
		t.Fatalf("put gc task: %v", err)
	}

	gw := NewGateway(GatewayConfig{
		MetaService: meta,
		Creds:       NewCredentialStore(),
		ChunkStore:  chunkstore.NewMemoryChunkStore(),
		BackgroundWorkers: ObjectWriteBackgroundWorkerConfig{
			Enabled:        true,
			Interval:       10 * time.Millisecond,
			Lease:          time.Second,
			RecoveryLimit:  10,
			GCLimit:        10,
			GCAbandonAge:   time.Hour,
			GCInitialDelay: time.Millisecond,
			RecoveryOwner:  "test-recovery",
			GCWorkerOwner:  "test-gc",
		},
	})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()

	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runErr := make(chan error, 1)
	go func() {
		runErr <- gw.Run(runCtx, ServerConfig{
			Addr:            addr,
			GracefulTimeout: time.Second,
			Trap:            func(c chan<- os.Signal) {},
		})
	}()

	waitForBackgroundTaskState(t, meta, "write-recovery-task", metadata.TaskSucceeded)
	waitForBackgroundTaskState(t, meta, "write-gc-task", metadata.TaskSucceeded)

	cancel()
	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("run returned error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("run did not stop")
	}
}

func TestRunCreatesPeriodicObjectWriteBackgroundTasks(t *testing.T) {
	meta := newMockMetaService()
	gw := NewGateway(GatewayConfig{
		MetaService: meta,
		Creds:       NewCredentialStore(),
		ChunkStore:  chunkstore.NewMemoryChunkStore(),
		BackgroundWorkers: ObjectWriteBackgroundWorkerConfig{
			Enabled:       true,
			Interval:      10 * time.Millisecond,
			Lease:         time.Second,
			RecoveryLimit: 10,
			GCLimit:       10,
			GCAbandonAge:  time.Hour,
		},
	})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()

	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runErr := make(chan error, 1)
	go func() {
		runErr <- gw.Run(runCtx, ServerConfig{
			Addr:            addr,
			GracefulTimeout: time.Second,
			Trap:            func(c chan<- os.Signal) {},
		})
	}()

	waitForBackgroundTaskState(t, meta, ObjectWriteRecoveryPeriodicTaskID, metadata.TaskSucceeded)
	waitForBackgroundTaskState(t, meta, ObjectWriteGCPeriodicTaskID, metadata.TaskSucceeded)

	cancel()
	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("run returned error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("run did not stop")
	}
}

func waitForBackgroundTaskState(t *testing.T, meta *mockMetaService, id string, state metadata.BackgroundTaskState) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		task, err := meta.GetBackgroundTask(context.Background(), id)
		if err == nil && task.State == state {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	task, err := meta.GetBackgroundTask(context.Background(), id)
	t.Fatalf("task %s state = %+v, err=%v, want %s", id, task, err, state)
}
