package metadata

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestComputeClusterReadiness_AllHealthy(t *testing.T) {
	store := newTestPebbleStore(t)
	ctx := context.Background()

	// Register 3 online nodes.
	for i := NodeID(1); i <= 3; i++ {
		if err := store.RegisterNode(ctx, &NodeInfo{
			ID:    i,
			Addr:  "127.0.0.1:900" + string(rune('0'+i)),
			State: NodeOnline,
		}); err != nil {
			t.Fatalf("RegisterNode %d: %v", i, err)
		}
	}

	// Create a healthy chunk with 3 ready replicas.
	chunk := &ChunkMeta{
		ID:       100,
		Size:     64 * 1024 * 1024,
		State:    ChunkReady,
		Replicas: []ReplicaInfo{{NodeID: 1, State: ReplicaReady}, {NodeID: 2, State: ReplicaReady}, {NodeID: 3, State: ReplicaReady}},
	}
	if err := store.putJSON(prefixChunk+"100", chunk); err != nil {
		t.Fatalf("put chunk: %v", err)
	}

	metrics := NewMetrics()
	hc := NewHealthChecker(store, nil, metrics, "test")

	r := hc.ComputeClusterReadiness()

	if r.Status != "ready" {
		t.Errorf("status = %q, want %q; checks: %v", r.Status, "ready", r.Checks)
	}
	if r.NodesOnline != 3 {
		t.Errorf("nodes_online = %d, want 3", r.NodesOnline)
	}
	if r.NodesTotal != 3 {
		t.Errorf("nodes_total = %d, want 3", r.NodesTotal)
	}
	if r.CanWriteRF != 3 {
		t.Errorf("can_write_rf = %d, want 3", r.CanWriteRF)
	}
	if r.LeaderStable != true {
		t.Errorf("leader_stable = false, want true (standalone mode)")
	}
	if r.ChunksUnderReplicated != 0 {
		t.Errorf("chunks_under_replicated = %d, want 0", r.ChunksUnderReplicated)
	}
	if r.Checks["quorum"] != "ok" {
		t.Errorf("quorum check = %q, want ok", r.Checks["quorum"])
	}
	if r.Checks["replication"] != "ok" {
		t.Errorf("replication check = %q, want ok", r.Checks["replication"])
	}

	// ComputeClusterReadiness must populate the atomics that back the
	// nufs_nodes_online / nufs_nodes_total / nufs_chunks_total gauges exposed by
	// PrometheusHandler. These feed the datanode_availability and chunk_durability
	// SLOs; if they stay 0 the alerts can never fire (SLO = 0/0 NaN). See
	// metadata/prometheus.go PrometheusHandler.
	if got := metrics.NodesOnline.Load(); got != 3 {
		t.Errorf("metrics.NodesOnline = %d, want 3", got)
	}
	if got := metrics.NodesTotal.Load(); got != 3 {
		t.Errorf("metrics.NodesTotal = %d, want 3", got)
	}
	if got := metrics.ChunksTotal.Load(); got != 1 {
		t.Errorf("metrics.ChunksTotal = %d, want 1", got)
	}
}

func TestComputeClusterReadiness_NoQuorum(t *testing.T) {
	store := newTestPebbleStore(t)
	ctx := context.Background()

	// Register 5 nodes — 1 online, 4 forced offline via direct write.
	// RegisterNode always sets NodeOnline, so we use putMsgpack for the
	// offline ones.
	for i := NodeID(1); i <= 5; i++ {
		if i == 1 {
			if err := store.RegisterNode(ctx, &NodeInfo{
				ID:    i,
				Addr:  "127.0.0.1:9001",
				State: NodeOnline,
			}); err != nil {
				t.Fatalf("RegisterNode: %v", err)
			}
			continue
		}
		// Write directly to force offline state.
		info := &NodeInfo{
			ID:       i,
			Addr:     "127.0.0.1:900" + string(rune('0'+i)),
			State:    NodeOffline,
			LastSeen: time.Now().Add(-10 * time.Minute).UnixNano(),
		}
		key := prefixNode + fmt.Sprintf("%d", i)
		if err := store.putMsgpack(key, info); err != nil {
			t.Fatalf("putMsgpack node %d: %v", i, err)
		}
	}

	metrics := NewMetrics()
	hc := NewHealthChecker(store, nil, metrics, "test")

	r := hc.ComputeClusterReadiness()

	if r.Status != "not_ready" {
		t.Errorf("status = %q, want not_ready; checks: %v", r.Status, r.Checks)
	}
	if r.NodesOnline != 1 {
		t.Errorf("nodes_online = %d, want 1", r.NodesOnline)
	}
	if r.CanWriteRF != 1 {
		t.Errorf("can_write_rf = %d, want 1", r.CanWriteRF)
	}
}

func TestComputeClusterReadiness_UnderReplicated(t *testing.T) {
	store := newTestPebbleStore(t)
	ctx := context.Background()

	// Register 3 online nodes — enough for quorum.
	for i := NodeID(1); i <= 3; i++ {
		if err := store.RegisterNode(ctx, &NodeInfo{
			ID:    i,
			Addr:  "127.0.0.1:900" + string(rune('0'+i)),
			State: NodeOnline,
		}); err != nil {
			t.Fatalf("RegisterNode: %v", err)
		}
	}

	// Chunk with only 1 of 3 replicas ready → under-replicated.
	chunk := &ChunkMeta{
		ID:    200,
		Size:  64 * 1024 * 1024,
		State: ChunkReady,
		Replicas: []ReplicaInfo{
			{NodeID: 1, State: ReplicaReady},
			{NodeID: 2, State: ReplicaFailed},
			{NodeID: 3, State: ReplicaStale},
		},
	}
	if err := store.putJSON(prefixChunk+"200", chunk); err != nil {
		t.Fatalf("put chunk: %v", err)
	}

	metrics := NewMetrics()
	hc := NewHealthChecker(store, nil, metrics, "test")

	r := hc.ComputeClusterReadiness()

	if r.Status != "degraded" {
		t.Errorf("status = %q, want degraded; checks: %v", r.Status, r.Checks)
	}
	if r.ChunksUnderReplicated != 1 {
		t.Errorf("chunks_under_replicated = %d, want 1", r.ChunksUnderReplicated)
	}
	if r.ChunksTotal != 1 {
		t.Errorf("chunks_total = %d, want 1", r.ChunksTotal)
	}

	// The atomics backing the Prometheus gauges must reflect the real scan.
	if got := metrics.ChunksTotal.Load(); got != 1 {
		t.Errorf("metrics.ChunksTotal = %d, want 1", got)
	}
}

func TestComputeClusterReadiness_DegradedStore(t *testing.T) {
	store := newTestPebbleStore(t)
	ctx := context.Background()

	if err := store.RegisterNode(ctx, &NodeInfo{
		ID: 1, Addr: "127.0.0.1:9001", State: NodeOnline,
	}); err != nil {
		t.Fatalf("RegisterNode: %v", err)
	}

	// Force the degradation manager into ReadOnly state.
	dm := store.GetDegradationManager()
	for i := 0; i < 15; i++ {
		dm.RecordWriteError()
	}

	metrics := NewMetrics()
	hc := NewHealthChecker(store, nil, metrics, "test")

	r := hc.ComputeClusterReadiness()

	if r.Status != "degraded" {
		t.Errorf("status = %q, want degraded; checks: %v", r.Status, r.Checks)
	}
	if r.DegradationState != "ReadOnly" {
		t.Errorf("degradation_state = %q, want ReadOnly", r.DegradationState)
	}
}

func TestComputeClusterReadiness_RepairQueueBacklog(t *testing.T) {
	store := newTestPebbleStore(t)
	ctx := context.Background()

	if err := store.RegisterNode(ctx, &NodeInfo{
		ID: 1, Addr: "127.0.0.1:9001", State: NodeOnline,
	}); err != nil {
		t.Fatalf("RegisterNode: %v", err)
	}

	metrics := NewMetrics()
	metrics.RepairTasksQueued.Store(5000) // large backlog

	hc := NewHealthChecker(store, nil, metrics, "test")

	r := hc.ComputeClusterReadiness()

	if r.Status != "degraded" {
		t.Errorf("status = %q, want degraded; checks: %v", r.Status, r.Checks)
	}
	if r.RepairQueueDepth != 5000 {
		t.Errorf("repair_queue_depth = %d, want 5000", r.RepairQueueDepth)
	}
}

func TestComputeClusterReadiness_TimestampPopulated(t *testing.T) {
	store := newTestPebbleStore(t)
	hc := NewHealthChecker(store, nil, NewMetrics(), "test")

	before := time.Now()
	r := hc.ComputeClusterReadiness()
	after := time.Now()

	if r.Timestamp.Before(before) || r.Timestamp.After(after.Add(time.Second)) {
		t.Errorf("timestamp %v not in range [%v, %v]", r.Timestamp, before, after)
	}
}
