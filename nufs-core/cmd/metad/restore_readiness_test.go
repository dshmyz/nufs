package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/example/dfs/metadata"
)

func TestRestoredClusterStaysNotReadyUntilReplicaVerificationPasses(t *testing.T) {
	store, bundle := newOpsTestStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	chunkID := createRestoreGateChunk(t, store)
	if err := store.PutRestorePendingMarker(ctx, restoreGateMarker()); err != nil {
		t.Fatalf("PutRestorePendingMarker: %v", err)
	}
	probe := &gateProbe{reachable: map[metadata.ChunkID]int{chunkID: 0}}

	gate, err := startRestoreReadinessGate(ctx, store, bundle, restoreReadinessConfig{
		MinimumReadableReplicas: 1,
		Probe:                   probe,
		PollInterval:            time.Millisecond,
	})
	if err != nil {
		t.Fatalf("startRestoreReadinessGate: %v", err)
	}
	t.Cleanup(gate.Stop)

	if bundle.IsReady() {
		t.Fatal("restored cluster was ready before replica verification passed")
	}
	probe.setReachable(chunkID, 1)
	waitForRestoreReady(t, bundle)
	if marker, err := store.GetRestorePendingMarker(ctx); err != nil {
		t.Fatalf("GetRestorePendingMarker: %v", err)
	} else if marker != nil {
		t.Fatalf("restore marker still present after successful verification: %+v", marker)
	}
}

func TestRestoredClusterRemainsNotReadyWhenAllReplicasAreMissing(t *testing.T) {
	store, bundle := newOpsTestStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	chunkID := createRestoreGateChunk(t, store)
	if err := store.PutRestorePendingMarker(ctx, restoreGateMarker()); err != nil {
		t.Fatalf("PutRestorePendingMarker: %v", err)
	}
	probe := &gateProbe{reachable: map[metadata.ChunkID]int{chunkID: 0}}

	gate, err := startRestoreReadinessGate(ctx, store, bundle, restoreReadinessConfig{
		MinimumReadableReplicas: 1,
		Probe:                   probe,
		PollInterval:            time.Millisecond,
	})
	if err != nil {
		t.Fatalf("startRestoreReadinessGate: %v", err)
	}
	t.Cleanup(gate.Stop)

	waitForRestoreIssue(t, bundle, metadata.RestoreChunkMissing)
	if bundle.IsReady() {
		t.Fatal("restored cluster became ready with all replicas missing")
	}
	if marker, err := store.GetRestorePendingMarker(ctx); err != nil {
		t.Fatalf("GetRestorePendingMarker: %v", err)
	} else if marker == nil {
		t.Fatal("restore marker was cleared despite missing replicas")
	}
}

func TestNormalClusterDoesNotWaitForRestoreVerification(t *testing.T) {
	store, bundle := newOpsTestStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	probe := &gateProbe{}

	gate, err := startRestoreReadinessGate(ctx, store, bundle, restoreReadinessConfig{
		MinimumReadableReplicas: 1,
		Probe:                   probe,
		PollInterval:            time.Millisecond,
	})
	if err != nil {
		t.Fatalf("startRestoreReadinessGate: %v", err)
	}
	t.Cleanup(gate.Stop)
	if !bundle.IsReady() {
		t.Fatal("normal cluster waited for restore verification")
	}
	if got := probe.calls.Load(); got != 0 {
		t.Fatalf("probe calls = %d, want 0 for normal cluster", got)
	}
}

func TestRestoreReadinessRejectsMinimumBelowOne(t *testing.T) {
	store, bundle := newOpsTestStore(t)
	_, err := startRestoreReadinessGate(context.Background(), store, bundle, restoreReadinessConfig{
		MinimumReadableReplicas: 0,
		Probe:                   &gateProbe{},
	})
	if err == nil {
		t.Fatal("startRestoreReadinessGate accepted minimum replicas below one")
	}
}

func TestDatanodeRestoreReplicaProbeCountsOnlyReadableReplicas(t *testing.T) {
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		methods = append(methods, r.Method+" "+r.URL.Path)
		if r.URL.Path != "/api/v1/chunks/77/verify" {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"chunk_id": 77, "valid": true})
	}))
	t.Cleanup(server.Close)
	bad := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(bad.Close)

	probe := datanodeRestoreReplicaProbe{Client: server.Client()}
	reachable, err := probe.ReachableReplicas(context.Background(), &metadata.ChunkMeta{
		ID: 77,
		Replicas: []metadata.ReplicaInfo{
			{Addr: server.URL, State: metadata.ReplicaReady},
			{Addr: bad.URL, State: metadata.ReplicaReady},
			{Addr: server.URL, State: metadata.ReplicaStale},
		},
	})
	if err != nil {
		t.Fatalf("ReachableReplicas: %v", err)
	}
	if reachable != 1 {
		t.Fatalf("reachable = %d, want 1", reachable)
	}
	if len(methods) != 1 || methods[0] != "GET /api/v1/chunks/77/verify" {
		t.Fatalf("probe methods = %+v", methods)
	}
}

type gateProbe struct {
	mu        sync.Mutex
	reachable map[metadata.ChunkID]int
	calls     atomic.Int64
}

func (p *gateProbe) ReachableReplicas(_ context.Context, chunk *metadata.ChunkMeta) (int, error) {
	p.calls.Add(1)
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.reachable[chunk.ID], nil
}

func (p *gateProbe) setReachable(chunkID metadata.ChunkID, reachable int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.reachable == nil {
		p.reachable = make(map[metadata.ChunkID]int)
	}
	p.reachable[chunkID] = reachable
}

func createRestoreGateChunk(t *testing.T, store *metadata.PebbleStore) metadata.ChunkID {
	t.Helper()
	ctx := context.Background()
	if err := store.RegisterNode(ctx, &metadata.NodeInfo{ID: 1, Addr: "node-1:9100", State: metadata.NodeOnline}); err != nil {
		t.Fatalf("RegisterNode: %v", err)
	}
	if err := store.CreateBucket(ctx, "restore", metadata.PlacementPolicy{ReplicationFactor: 1, TopologySpread: metadata.SpreadNode}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	bucket, err := store.GetBucket(ctx, "restore")
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}
	file, err := store.CreateFile(ctx, bucket.RootInode, "file.bin", 0o644)
	if err != nil {
		t.Fatalf("CreateFile: %v", err)
	}
	chunk, err := store.AllocateChunk(ctx, file.ID, 0, metadata.PlacementPolicy{ReplicationFactor: 1, TopologySpread: metadata.SpreadNode})
	if err != nil {
		t.Fatalf("AllocateChunk: %v", err)
	}
	if err := store.ReportChunkState(ctx, 1, map[metadata.ChunkID]metadata.ReplicaState{chunk.ID: metadata.ReplicaReady}); err != nil {
		t.Fatalf("ReportChunkState: %v", err)
	}
	return chunk.ID
}

func restoreGateMarker() *metadata.RestorePendingMarker {
	return &metadata.RestorePendingMarker{
		BackupID:        "backup-restore-gate",
		SourceClusterID: "cluster-source",
		AppliedIndex:    42,
		RestoredAt:      time.Date(2026, 7, 30, 11, 0, 0, 0, time.UTC),
	}
}

func waitForRestoreReady(t *testing.T, bundle *metadata.ServiceBundle) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if bundle.IsReady() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("bundle did not become ready")
}

func waitForRestoreIssue(t *testing.T, bundle *metadata.ServiceBundle, want metadata.RestoreChunkAvailabilityStatus) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		report := bundle.RestoreReadinessReport()
		if report != nil {
			for _, issue := range report.Issues {
				if issue.Status == want {
					return
				}
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("restore readiness report did not contain issue %q", want)
}
