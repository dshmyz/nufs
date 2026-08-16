package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// newRepairExtentFixture seeds a single V2 extent-backed chunk (inline
// layout, Ready lifecycle) and returns the store plus its ID, mirroring the
// ops_scrub_test fixture.
func newRepairExtentFixture(t *testing.T) (*metadata.PebbleStore, metadata.ChunkID) {
	t.Helper()
	ctx := context.Background()
	store, err := metadata.NewPebbleStore(metadata.PebbleStoreConfig{
		Dir:         t.TempDir(),
		UseInMemory: true,
		NodeID:      1,
	})
	if err != nil {
		t.Fatalf("NewPebbleStore: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	policy := metadata.PlacementPolicy{
		ID: "default", ReplicationFactor: 1, TopologySpread: metadata.SpreadRack,
	}
	if err := store.CreateBucket(ctx, "repair", policy); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	bucket, err := store.GetBucket(ctx, "repair")
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
	return store, chunk.ID
}

// TestHandleTriggerRepair_AcceptsExtentID verifies /api/v1/repair/trigger
// accepts an extent_id (roadmap §1.4 extent semantics): a valid extent queues
// the backing chunk's repair, a nonexistent extent 404s (fail fast, no repair
// for a GC'd chunk), and a body with neither ID 400s.
func TestHandleTriggerRepair_AcceptsExtentID(t *testing.T) {
	store, chunkID := newRepairExtentFixture(t)

	t.Run("extent_id triggers and appears in queue", func(t *testing.T) {
		body, _ := json.Marshal(map[string]uint64{"extent_id": uint64(chunkID)})
		req := httptest.NewRequest(http.MethodPost, "/api/v1/repair/trigger", bytes.NewReader(body))
		rr := httptest.NewRecorder()
		(&opsHandlers{store: store, dataStore: store}).handleTriggerRepair(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200, body=%s", rr.Code, rr.Body.String())
		}

		qreq := httptest.NewRequest(http.MethodGet, "/api/v1/repair/queue", nil)
		qrr := httptest.NewRecorder()
		(&opsHandlers{store: store, dataStore: store}).handleRepairQueue(qrr, qreq)
		if qrr.Code != http.StatusOK {
			t.Fatalf("queue status = %d, want 200", qrr.Code)
		}
		var tasks []repairQueueEntry
		if err := json.Unmarshal(qrr.Body.Bytes(), &tasks); err != nil {
			t.Fatalf("decode queue: %v", err)
		}
		if len(tasks) != 1 {
			t.Fatalf("queue size = %d, want 1", len(tasks))
		}
		if tasks[0].ChunkID != chunkID {
			t.Fatalf("queued chunk = %d, want %d", tasks[0].ChunkID, chunkID)
		}
		if tasks[0].Reason != "extent_unhealthy" {
			t.Fatalf("reason = %q, want %q", tasks[0].Reason, "extent_unhealthy")
		}
		if !tasks[0].IsExtent {
			t.Fatal("expected is_extent=true for extent-backed repair")
		}
	})

	t.Run("unknown extent_id 404s", func(t *testing.T) {
		body, _ := json.Marshal(map[string]uint64{"extent_id": 999999})
		req := httptest.NewRequest(http.MethodPost, "/api/v1/repair/trigger", bytes.NewReader(body))
		rr := httptest.NewRecorder()
		(&opsHandlers{store: store, dataStore: store}).handleTriggerRepair(rr, req)
		if rr.Code != http.StatusNotFound {
			t.Fatalf("status = %d, want 404, body=%s", rr.Code, rr.Body.String())
		}
	})

	t.Run("neither id 400s", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/repair/trigger", bytes.NewReader([]byte(`{}`)))
		rr := httptest.NewRecorder()
		(&opsHandlers{store: store, dataStore: store}).handleTriggerRepair(rr, req)
		if rr.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400, body=%s", rr.Code, rr.Body.String())
		}
	})
}

// TestHandleRepairQueue_AnnotatesExtents verifies /api/v1/repair/queue marks
// extent-backed repairs (is_extent + extent_lifecycle) while leaving legacy
// chunk repairs unannotated, so an operator can tell them apart at a glance.
func TestHandleRepairQueue_AnnotatesExtents(t *testing.T) {
	store, chunkID := newRepairExtentFixture(t)
	ctx := context.Background()

	// Extent-backed repair: extent row exists (Ready lifecycle).
	if err := store.TriggerExtentRepair(ctx, metadata.ExtentIDV2(chunkID)); err != nil {
		t.Fatalf("TriggerExtentRepair: %v", err)
	}
	// Legacy chunk repair: no /extent-meta row for this chunk.
	if err := store.TriggerRepair(ctx, metadata.ChunkID(777)); err != nil {
		t.Fatalf("TriggerRepair: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/repair/queue", nil)
	rr := httptest.NewRecorder()
	(&opsHandlers{store: store, dataStore: store}).handleRepairQueue(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200, body=%s", rr.Code, rr.Body.String())
	}

	var tasks []repairQueueEntry
	if err := json.Unmarshal(rr.Body.Bytes(), &tasks); err != nil {
		t.Fatalf("decode queue: %v", err)
	}
	if len(tasks) != 2 {
		t.Fatalf("queue size = %d, want 2", len(tasks))
	}
	byChunk := map[metadata.ChunkID]repairQueueEntry{}
	for _, e := range tasks {
		byChunk[e.ChunkID] = e
	}
	ext, ok := byChunk[chunkID]
	if !ok {
		t.Fatal("extent-backed task missing from queue")
	}
	if !ext.IsExtent {
		t.Fatal("expected is_extent=true for extent-backed repair")
	}
	if ext.ExtentLifecycle != "ready" {
		t.Fatalf("extent_lifecycle = %q, want %q", ext.ExtentLifecycle, "ready")
	}
	legacy, ok := byChunk[777]
	if !ok {
		t.Fatal("legacy task missing from queue")
	}
	if legacy.IsExtent {
		t.Fatal("expected is_extent=false (omitempty) for legacy chunk repair")
	}
	if legacy.ExtentLifecycle != "" {
		t.Fatalf("extent_lifecycle = %q, want empty for legacy chunk repair", legacy.ExtentLifecycle)
	}
}
