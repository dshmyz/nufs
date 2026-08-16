package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// TestHandleScrub_ReportsExtentCounts verifies /api/v1/scrub reports the V2
// extent summary (roadmap §1.4) alongside the chunk summary: Lifecycle
// counts plus dangling/unhealthy backing-chunk health.
func TestHandleScrub_ReportsExtentCounts(t *testing.T) {
	ctx := context.Background()
	store, err := metadata.NewPebbleStore(metadata.PebbleStoreConfig{
		Dir:         t.TempDir(),
		UseInMemory: true,
		NodeID:      1,
	})
	if err != nil {
		t.Fatalf("NewPebbleStore: %v", err)
	}
	defer store.Close()

	policy := metadata.PlacementPolicy{
		ID: "default", ReplicationFactor: 1, TopologySpread: metadata.SpreadRack,
	}
	if err := store.CreateBucket(ctx, "scrub", policy); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	bucket, err := store.GetBucket(ctx, "scrub")
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}
	for i := metadata.NodeID(1); i <= 2; i++ {
		if err := store.RegisterNode(ctx, &metadata.NodeInfo{
			ID: i, Addr: fmt.Sprintf("n%d:9100", i),
			Rack: fmt.Sprintf("r%d", i), Zone: "z1", Tier: metadata.TierHot,
		}); err != nil {
			t.Fatalf("RegisterNode: %v", err)
		}
	}
	f, err := store.CreateFile(ctx, bucket.RootInode, "d.bin", 0644)
	if err != nil {
		t.Fatalf("CreateFile: %v", err)
	}
	chunk, err := store.AllocateChunk(ctx, f.ID, 0, policy)
	if err != nil {
		t.Fatalf("AllocateChunk: %v", err)
	}
	if err := store.CommitChunk(ctx, chunk.ID, 0xABCDEF); err != nil {
		t.Fatalf("CommitChunk: %v", err)
	}
	if err := store.SealChunk(ctx, chunk.ID); err != nil {
		t.Fatalf("SealChunk: %v", err)
	}
	ext := &metadata.ExtentMetaV2{ID: metadata.ExtentIDV2(chunk.ID), Generation: 1, LogicalLen: 4096}
	if err := store.SetInlineExtent(ctx, f.ID, ext, 4096); err != nil {
		t.Fatalf("SetInlineExtent: %v", err)
	}

	// Degrade the only replica: chunk Degraded, extent ReadyDegraded, zero
	// healthy replicas → reported as extent unhealthy.
	ready, err := store.GetChunk(ctx, chunk.ID)
	if err != nil {
		t.Fatalf("GetChunk: %v", err)
	}
	if err := store.ReportChunkState(ctx, ready.Replicas[0].NodeID, map[metadata.ChunkID]metadata.ReplicaState{
		chunk.ID: metadata.ReplicaFailed,
	}); err != nil {
		t.Fatalf("ReportChunkState: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/scrub", nil)
	rr := httptest.NewRecorder()
	(&opsHandlers{store: store, dataStore: store}).handleScrub(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200, body=%s", rr.Code, rr.Body.String())
	}
	var body struct {
		Scanned          int `json:"scanned"`
		Healthy          int `json:"healthy"`
		Unhealthy        int `json:"unhealthy"`
		ExtentsScanned   int `json:"extents_scanned"`
		ExtentsReady     int `json:"extents_ready"`
		ExtentsDegraded  int `json:"extents_degraded"`
		ExtentsDangling  int `json:"extents_dangling"`
		ExtentsUnhealthy int `json:"extents_unhealthy"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if body.Scanned != 1 || body.Healthy != 0 || body.Unhealthy != 1 {
		t.Fatalf("chunk summary = scanned %d / healthy %d / unhealthy %d, want 1/0/1",
			body.Scanned, body.Healthy, body.Unhealthy)
	}
	if body.ExtentsScanned != 1 || body.ExtentsReady != 0 || body.ExtentsDegraded != 1 {
		t.Fatalf("extent lifecycle = scanned %d / ready %d / degraded %d, want 1/0/1",
			body.ExtentsScanned, body.ExtentsReady, body.ExtentsDegraded)
	}
	if body.ExtentsDangling != 0 || body.ExtentsUnhealthy != 1 {
		t.Fatalf("extent health = dangling %d / unhealthy %d, want 0/1",
			body.ExtentsDangling, body.ExtentsUnhealthy)
	}
}
