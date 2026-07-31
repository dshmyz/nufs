package metadata

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestIntegrationHeartbeatErrorRatePlacement validates the full pipeline:
// register nodes → heartbeat with write error rates → PlaceChunk → verify
// high-error-rate node is excluded from placement.
func TestIntegrationHeartbeatErrorRatePlacement(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store := newTestPebbleStore(t)

	// Register 4 nodes across different racks
	for i := NodeID(1); i <= 4; i++ {
		err := store.RegisterNode(ctx, &NodeInfo{
			ID:         i,
			Rack:       "rack-" + string(rune('A'+i-1)),
			Tier:       TierHot,
			CapacityGB: 1000,
			UsedGB:     100,
		})
		if err != nil {
			t.Fatalf("register node %d: %v", i, err)
		}
	}

	// Heartbeat: node 4 has high write error rate (0.9), others healthy
	for i := NodeID(1); i <= 3; i++ {
		err := store.Heartbeat(ctx, i, &NodeReport{
			UsedGB:         100,
			ChunkCount:     50,
			DiskIO:         0.1,
			WriteErrorRate: 0.01,
		})
		if err != nil {
			t.Fatalf("heartbeat node %d: %v", i, err)
		}
	}
	err := store.Heartbeat(ctx, 4, &NodeReport{
		UsedGB:         100,
		ChunkCount:     50,
		DiskIO:         0.1,
		WriteErrorRate: 0.9, // above default filter threshold (0.8)
	})
	if err != nil {
		t.Fatalf("heartbeat node 4: %v", err)
	}

	// PlaceChunk with RF=3 — node 4 should be excluded (error rate > 0.8)
	placed, err := store.placement.PlaceChunk(PlacementPolicy{
		ID:                "default",
		ReplicationFactor: 3,
		StorageTier:       TierHot,
	}, nil)
	if err != nil {
		t.Fatalf("place chunk: %v", err)
	}

	for _, id := range placed {
		if id == 4 {
			t.Fatalf("node 4 (error rate 0.9) should be excluded from placement, but got %v", placed)
		}
	}
	if len(placed) != 3 {
		t.Fatalf("expected 3 replicas, got %d: %v", len(placed), placed)
	}
	t.Logf("placed chunk on nodes %v (correctly excluded node 4)", placed)
}

// TestIntegrationErrorRateRecovery verifies that a node's placement eligibility
// recovers after its error rate drops below the threshold on subsequent heartbeats.
func TestIntegrationErrorRateRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store := newTestPebbleStore(t)

	// Register 3 nodes
	for i := NodeID(1); i <= 3; i++ {
		if err := store.RegisterNode(ctx, &NodeInfo{
			ID: i, Rack: "rack-" + string(rune('A'+i-1)),
			Tier: TierHot, CapacityGB: 1000, UsedGB: 100,
		}); err != nil {
			t.Fatalf("register node %d: %v", i, err)
		}
	}

	// Initial heartbeat: node 3 has high error rate
	for i := NodeID(1); i <= 2; i++ {
		store.Heartbeat(ctx, i, &NodeReport{UsedGB: 100, WriteErrorRate: 0.01})
	}
	store.Heartbeat(ctx, 3, &NodeReport{UsedGB: 100, WriteErrorRate: 0.95})

	// With RF=3, should fail — node 3 filtered, only 2 eligible
	_, err := store.placement.PlaceChunk(PlacementPolicy{
		ID: "default", ReplicationFactor: 3, StorageTier: TierHot,
	}, nil)
	if err == nil {
		t.Fatal("expected ErrInsufficientNodes with node 3 error rate 0.95")
	}

	// Node 3 recovers — error rate drops to 0.05
	store.Heartbeat(ctx, 3, &NodeReport{UsedGB: 100, WriteErrorRate: 0.05})

	// Now RF=3 should succeed
	placed, err := store.placement.PlaceChunk(PlacementPolicy{
		ID: "default", ReplicationFactor: 3, StorageTier: TierHot,
	}, nil)
	if err != nil {
		t.Fatalf("place chunk after recovery: %v", err)
	}
	if len(placed) != 3 {
		t.Fatalf("expected 3 replicas after recovery, got %d", len(placed))
	}
	t.Logf("node 3 recovered; placed on %v", placed)
}

// TestIntegrationReadinessHTTP tests the /api/v1/cluster/readiness endpoint
// end-to-end through an httptest server.
func TestIntegrationReadinessHTTP(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store := newTestPebbleStore(t)
	bundle, err := NewPebbleServiceBundle(store)
	if err != nil {
		t.Fatalf("create bundle: %v", err)
	}
	defer bundle.Close()

	// Wire readiness endpoint
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/cluster/readiness", func(w http.ResponseWriter, _ *http.Request) {
		if bundle.Health == nil {
			http.Error(w, "no health checker", http.StatusServiceUnavailable)
			return
		}
		r := bundle.Health.ComputeClusterReadiness()
		w.Header().Set("Content-Type", "application/json")
		if r.Status == "not_ready" {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		json.NewEncoder(w).Encode(r)
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	// Case 1: No nodes → not_ready (0 < quorum threshold of 1)
	t.Run("NoNodes", func(t *testing.T) {
		resp, err := http.Get(server.URL + "/api/v1/cluster/readiness")
		if err != nil {
			t.Fatalf("request: %v", err)
		}
		defer resp.Body.Close()

		var r ClusterReadiness
		json.NewDecoder(resp.Body).Decode(&r)

		// 0 nodes → quorum check: 0 < (0/2)+1 = 1 → not_ready
		t.Logf("status=%s nodes=%d/%d checks=%v", r.Status, r.NodesOnline, r.NodesTotal, r.Checks)
		if r.Status != "not_ready" {
			t.Fatalf("expected not_ready with no nodes, got %s", r.Status)
		}
		if resp.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("expected 503 for not_ready, got %d", resp.StatusCode)
		}
	})

	// Case 2: 3 nodes all online → ready
	t.Run("AllOnline", func(t *testing.T) {
		for i := NodeID(1); i <= 3; i++ {
			store.RegisterNode(ctx, &NodeInfo{
				ID: i, Rack: "rack-1", Tier: TierHot,
				CapacityGB: 1000, UsedGB: 100,
			})
		}

		resp, err := http.Get(server.URL + "/api/v1/cluster/readiness")
		if err != nil {
			t.Fatalf("request: %v", err)
		}
		defer resp.Body.Close()

		var r ClusterReadiness
		json.NewDecoder(resp.Body).Decode(&r)

		t.Logf("status=%s nodes=%d/%d checks=%v", r.Status, r.NodesOnline, r.NodesTotal, r.Checks)
		if r.Status != "ready" {
			t.Fatalf("expected ready with 3 online nodes, got %s", r.Status)
		}
		if r.NodesOnline != 3 {
			t.Fatalf("expected 3 online, got %d", r.NodesOnline)
		}
	})

	// Case 3: Decommission 2 of 3 nodes → not_ready (quorum lost)
	t.Run("QuorumLost", func(t *testing.T) {
		store.DecommissionNode(ctx, 2)
		store.DecommissionNode(ctx, 3)

		resp, err := http.Get(server.URL + "/api/v1/cluster/readiness")
		if err != nil {
			t.Fatalf("request: %v", err)
		}
		defer resp.Body.Close()

		var r ClusterReadiness
		json.NewDecoder(resp.Body).Decode(&r)

		t.Logf("status=%s nodes=%d/%d checks=%v", r.Status, r.NodesOnline, r.NodesTotal, r.Checks)
		if r.Status != "not_ready" {
			t.Fatalf("expected not_ready with 1/3 online, got %s", r.Status)
		}
		if resp.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("expected 503 for not_ready, got %d", resp.StatusCode)
		}
	})
}

// TestIntegrationPlacementWithDynamicConfig verifies that DynamicConfig
// threshold changes take effect immediately without restart.
func TestIntegrationPlacementWithDynamicConfig(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store := newTestPebbleStore(t)

	// Register 3 nodes
	for i := NodeID(1); i <= 3; i++ {
		store.RegisterNode(ctx, &NodeInfo{
			ID: i, Rack: "rack-" + string(rune('A'+i-1)),
			Tier: TierHot, CapacityGB: 1000, UsedGB: 100,
		})
	}

	// Node 3 has error rate 0.5 — below default filter (0.8), should be included
	for i := NodeID(1); i <= 2; i++ {
		store.Heartbeat(ctx, i, &NodeReport{UsedGB: 100, WriteErrorRate: 0.01})
	}
	store.Heartbeat(ctx, 3, &NodeReport{UsedGB: 100, WriteErrorRate: 0.5})

	// With default threshold (0.8), node 3 is included
	placed, err := store.placement.PlaceChunk(PlacementPolicy{
		ID: "default", ReplicationFactor: 3, StorageTier: TierHot,
	}, nil)
	if err != nil {
		t.Fatalf("place chunk default config: %v", err)
	}
	hasNode3 := false
	for _, id := range placed {
		if id == 3 {
			hasNode3 = true
		}
	}
	if !hasNode3 {
		t.Fatalf("node 3 (error rate 0.5) should be included with default threshold 0.8, got %v", placed)
	}

	// Lower the threshold to 0.3 — now node 3 (0.5) should be filtered
	cfg := store.GetDynamicConfig()
	cfg.PlacementErrorRateFilter = 0.3
	store.SetDynamicConfig(cfg)

	_, err = store.placement.PlaceChunk(PlacementPolicy{
		ID: "default", ReplicationFactor: 3, StorageTier: TierHot,
	}, nil)
	if err == nil {
		t.Fatal("expected ErrInsufficientNodes after lowering error rate filter to 0.3")
	}
	t.Log("correctly failed after dynamic config lowered error rate filter")

	// Restore threshold — node 3 should be included again
	cfg.PlacementErrorRateFilter = 0.8
	store.SetDynamicConfig(cfg)

	placed, err = store.placement.PlaceChunk(PlacementPolicy{
		ID: "default", ReplicationFactor: 3, StorageTier: TierHot,
	}, nil)
	if err != nil {
		t.Fatalf("place chunk after restoring config: %v", err)
	}
	if len(placed) != 3 {
		t.Fatalf("expected 3 replicas after restoring config, got %d", len(placed))
	}
	t.Logf("placement restored: %v", placed)
}
