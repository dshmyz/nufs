package metadata

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// ========== Roadmap §1.4: Scrubber extent 版 ==========

// newScrubExtentFixture seeds a single V2 extent (inline layout) whose
// backing chunk is degraded via the heartbeat path, returning the store and
// the degraded chunk. Mirrors the heartbeat-degrade fixture then degrades.
func newScrubExtentFixture(t *testing.T) (*PebbleStore, *ChunkMeta) {
	t.Helper()
	store, _, chunk := newExtentDegradeFixture(t, 2, "inline")
	ctx := context.Background()

	ready, err := store.GetChunk(ctx, chunk.ID)
	if err != nil {
		t.Fatalf("GetChunk: %v", err)
	}
	if ready.State != ChunkReady {
		t.Fatalf("fixture: chunk state = %d, want ChunkReady", ready.State)
	}
	failing := ready.Replicas[0].NodeID
	if err := store.ReportChunkState(ctx, failing, map[ChunkID]ReplicaState{chunk.ID: ReplicaFailed}); err != nil {
		t.Fatalf("ReportChunkState: %v", err)
	}

	deg, err := store.GetChunk(ctx, chunk.ID)
	if err != nil {
		t.Fatalf("GetChunk after degrade: %v", err)
	}
	if deg.State != ChunkDegraded {
		t.Fatalf("fixture: chunk state = %d, want ChunkDegraded", deg.State)
	}
	return store, deg
}

// simulateRepairCompletion returns the chunk to the state a real repair
// leaves it in: every replica ReplicaReady but chunk.State still
// ChunkDegraded (repairByAddingReplica → ReportChunkState(target,
// ReplicaReady) never flips the chunk state).
func simulateRepairCompletion(t *testing.T, store *PebbleStore, chunk *ChunkMeta) {
	t.Helper()
	ctx := context.Background()
	chunk, err := store.GetChunk(ctx, chunk.ID)
	if err != nil {
		t.Fatalf("GetChunk: %v", err)
	}
	for i := range chunk.Replicas {
		chunk.Replicas[i].State = ReplicaReady
	}
	if err := store.UpdateChunk(ctx, chunk); err != nil {
		t.Fatalf("UpdateChunk (simulate repair): %v", err)
	}
}

// replicasFromStates builds a ReplicaInfo list with one replica per state,
// each on a distinct node, for seed fixtures.
func replicasFromStates(states []ReplicaState) []ReplicaInfo {
	reps := make([]ReplicaInfo, 0, len(states))
	for i, st := range states {
		reps = append(reps, ReplicaInfo{
			NodeID: NodeID(i + 1),
			Addr:   fmt.Sprintf("n%d:9100", i+1),
			State:  st,
		})
	}
	return reps
}

// TestExtentScrubber_CountsLifecycle verifies the scrub counts the Lifecycle
// distribution and does not count healthy extents as unhealthy or degraded.
// The ReadyDegraded extent carries one Failed replica so it is neither
// recovered nor unhealthy — a stable, count-only case.
func TestExtentScrubber_CountsLifecycle(t *testing.T) {
	store := newTestPebbleStore(t)
	ctx := context.Background()

	seed := []struct {
		id        uint64
		lifecycle ExtentLifecycle
		replicas  []ReplicaState
	}{
		{98001, LifecycleReady, []ReplicaState{ReplicaReady}},
		{98002, LifecycleReadyDegraded, []ReplicaState{ReplicaReady, ReplicaFailed}},
		{98003, LifecycleECConverting, []ReplicaState{ReplicaReady}},
	}
	for _, s := range seed {
		if err := store.putJSON(fmt.Sprintf("%s%d", prefixChunk, s.id), &ChunkMeta{
			ID: ChunkID(s.id), Size: 4096, State: ChunkReady,
			Replicas: replicasFromStates(s.replicas),
			Checksum: 0xABCDEF,
		}); err != nil {
			t.Fatalf("seed chunk %d: %v", s.id, err)
		}
		if err := store.putExtentMeta(&ExtentMetaV2{
			ID: ExtentIDV2(s.id), Generation: 1, LogicalLen: 4096, Lifecycle: s.lifecycle,
		}); err != nil {
			t.Fatalf("seed extent %d: %v", s.id, err)
		}
	}

	result, err := NewExtentScrubber(store).Scan(ctx)
	if err != nil {
		t.Fatalf("extent scrub: %v", err)
	}
	if result.ExtentsScanned != 3 {
		t.Fatalf("scanned = %d, want 3", result.ExtentsScanned)
	}
	if result.Ready != 1 || result.ReadyDegraded != 1 || result.Other != 1 {
		t.Fatalf("lifecycle counts = ready %d / degraded %d / other %d, want 1/1/1",
			result.Ready, result.ReadyDegraded, result.Other)
	}
	if result.Dangling != 0 || result.Unhealthy != 0 || result.Recovered != 0 || result.RepairTriggered != 0 {
		t.Fatalf("health counts = dangling %d / unhealthy %d / recovered %d / repair_triggered %d, want 0/0/0/0",
			result.Dangling, result.Unhealthy, result.Recovered, result.RepairTriggered)
	}
}

// TestExtentScrubber_RecoversFullyReplicatedDegraded is the core recovery
// test: a chunk that was degraded, then repaired (all replicas Ready but
// State still ChunkDegraded) must be restored to ChunkReady and its extent
// from ReadyDegraded back to Ready, together. Without the recovery block in
// ExtentScrubber.Scan the extent stays ReadyDegraded and this test fails.
func TestExtentScrubber_RecoversFullyReplicatedDegraded(t *testing.T) {
	store, chunk := newScrubExtentFixture(t)
	ctx := context.Background()
	extID := ExtentIDV2(chunk.ID)

	// Sanity: extent degraded from the heartbeat mirror.
	before, err := store.GetExtentMeta(ctx, extID)
	if err != nil {
		t.Fatalf("GetExtentMeta before: %v", err)
	}
	if before.Lifecycle != LifecycleReadyDegraded {
		t.Fatalf("fixture: extent lifecycle = %d, want ReadyDegraded", before.Lifecycle)
	}

	simulateRepairCompletion(t, store, chunk)

	result, err := NewExtentScrubber(store).Scan(ctx)
	if err != nil {
		t.Fatalf("extent scrub: %v", err)
	}
	if result.Recovered != 1 {
		t.Fatalf("recovered = %d, want 1", result.Recovered)
	}
	if result.Ready != 1 || result.ReadyDegraded != 0 {
		t.Fatalf("lifecycle counts after recovery = ready %d / degraded %d, want 1/0",
			result.Ready, result.ReadyDegraded)
	}

	got, err := store.GetChunk(ctx, chunk.ID)
	if err != nil {
		t.Fatalf("GetChunk after: %v", err)
	}
	if got.State != ChunkReady {
		t.Fatalf("chunk state = %d, want ChunkReady", got.State)
	}
	after, err := store.GetExtentMeta(ctx, extID)
	if err != nil {
		t.Fatalf("GetExtentMeta after: %v", err)
	}
	if after.Lifecycle != LifecycleReady {
		t.Fatalf("extent lifecycle = %d, want LifecycleReady", after.Lifecycle)
	}

	// A second pass is a no-op: nothing left to recover.
	idem, err := NewExtentScrubber(store).Scan(ctx)
	if err != nil {
		t.Fatalf("extent scrub idempotent: %v", err)
	}
	if idem.Recovered != 0 {
		t.Fatalf("recovered on repeat scan = %d, want 0", idem.Recovered)
	}
}

// TestExtentScrubber_NoRecoveryWhileReplicaFailed verifies recovery is keyed
// on full replica health: a degraded extent whose chunk still has a Failed
// replica is neither recovered nor counted unhealthy (it is readable).
func TestExtentScrubber_NoRecoveryWhileReplicaFailed(t *testing.T) {
	store, chunk := newScrubExtentFixture(t)
	ctx := context.Background()

	result, err := NewExtentScrubber(store).Scan(ctx)
	if err != nil {
		t.Fatalf("extent scrub: %v", err)
	}
	if result.Recovered != 0 {
		t.Fatalf("recovered = %d, want 0 (replica still failed)", result.Recovered)
	}
	if result.Unhealthy != 0 {
		t.Fatalf("unhealthy = %d, want 0 (one replica still ready)", result.Unhealthy)
	}

	got, err := store.GetChunk(ctx, chunk.ID)
	if err != nil {
		t.Fatalf("GetChunk: %v", err)
	}
	if got.State != ChunkDegraded {
		t.Fatalf("chunk state = %d, want ChunkDegraded", got.State)
	}
	ext, err := store.GetExtentMeta(ctx, ExtentIDV2(chunk.ID))
	if err != nil {
		t.Fatalf("GetExtentMeta: %v", err)
	}
	if ext.Lifecycle != LifecycleReadyDegraded {
		t.Fatalf("extent lifecycle = %d, want ReadyDegraded", ext.Lifecycle)
	}
}

// TestExtentScrubber_SkipsECRecovery verifies EC-backed extents are counted
// by Lifecycle but never flagged unhealthy or recovered — shard health is the
// EC healer's domain.
func TestExtentScrubber_SkipsECRecovery(t *testing.T) {
	store := newTestPebbleStore(t)
	ctx := context.Background()

	if err := store.putJSON(fmt.Sprintf("%s%d", prefixChunk, 98004), &ChunkMeta{
		ID: ChunkID(98004), Size: 4096, State: ChunkReady,
		ECGroup:  &ECGroupInfo{GroupID: "ec-98004", DataShards: 6, ParityShards: 3},
		Replicas: []ReplicaInfo{},
		Checksum: 0xABCDEF,
	}); err != nil {
		t.Fatalf("seed EC chunk: %v", err)
	}
	if err := store.putExtentMeta(&ExtentMetaV2{
		ID: ExtentIDV2(98004), Generation: 1, LogicalLen: 4096, Lifecycle: LifecycleReadyDegraded,
	}); err != nil {
		t.Fatalf("seed extent: %v", err)
	}

	result, err := NewExtentScrubber(store).Scan(ctx)
	if err != nil {
		t.Fatalf("extent scrub: %v", err)
	}
	if result.ReadyDegraded != 1 || result.Unhealthy != 0 || result.Recovered != 0 || result.RepairTriggered != 0 {
		t.Fatalf("EC extent = degraded %d / unhealthy %d / recovered %d / repair_triggered %d, want 1/0/0/0",
			result.ReadyDegraded, result.Unhealthy, result.Recovered, result.RepairTriggered)
	}
	ext, err := store.GetExtentMeta(ctx, 98004)
	if err != nil {
		t.Fatalf("GetExtentMeta: %v", err)
	}
	if ext.Lifecycle != LifecycleReadyDegraded {
		t.Fatalf("EC extent lifecycle = %d, want ReadyDegraded preserved", ext.Lifecycle)
	}
}

// TestExtentScrubber_FlagsDanglingAndUnhealthy verifies the two health
// failure signals: an orphan /extent-meta row (backing chunk gone) counts as
// dangling, a chunk with no healthy replica counts as unhealthy.
func TestExtentScrubber_FlagsDanglingAndUnhealthy(t *testing.T) {
	store := newTestPebbleStore(t)
	ctx := context.Background()

	// Dangling: /extent-meta row with no backing chunk row.
	if err := store.putExtentMeta(&ExtentMetaV2{
		ID: ExtentIDV2(99001), Generation: 1, LogicalLen: 4096, Lifecycle: LifecycleReady,
	}); err != nil {
		t.Fatalf("seed dangling extent: %v", err)
	}
	// Unhealthy: chunk present but no healthy replica.
	if err := store.putJSON(fmt.Sprintf("%s%d", prefixChunk, 99002), &ChunkMeta{
		ID: ChunkID(99002), Size: 4096, State: ChunkDegraded,
		Replicas: []ReplicaInfo{{NodeID: 1, State: ReplicaFailed}},
	}); err != nil {
		t.Fatalf("seed unhealthy chunk: %v", err)
	}
	if err := store.putExtentMeta(&ExtentMetaV2{
		ID: ExtentIDV2(99002), Generation: 1, LogicalLen: 4096, Lifecycle: LifecycleReadyDegraded,
	}); err != nil {
		t.Fatalf("seed unhealthy extent: %v", err)
	}

	result, err := NewExtentScrubber(store).Scan(ctx)
	if err != nil {
		t.Fatalf("extent scrub: %v", err)
	}
	if result.ExtentsScanned != 2 {
		t.Fatalf("scanned = %d, want 2", result.ExtentsScanned)
	}
	if result.Dangling != 1 {
		t.Fatalf("dangling = %d, want 1", result.Dangling)
	}
	if result.Unhealthy != 1 {
		t.Fatalf("unhealthy = %d, want 1", result.Unhealthy)
	}
	if result.RepairTriggered != 1 {
		t.Fatalf("repair_triggered = %d, want 1 (unhealthy triggers, dangling does not)", result.RepairTriggered)
	}
	if result.Recovered != 0 {
		t.Fatalf("recovered = %d, want 0", result.Recovered)
	}
}

// TestExtentScrubber_TriggersRepairForUnhealthy verifies the safety-net
// repair trigger: an extent whose backing chunk has no healthy replica is
// enqueued via TriggerExtentRepair (Reason "extent_unhealthy"), a healthy
// extent is never queued, and re-triggering across passes is idempotent
// (overwrites the same queue key, never accumulates).
func TestExtentScrubber_TriggersRepairForUnhealthy(t *testing.T) {
	store := newTestPebbleStore(t)
	ctx := context.Background()

	// Healthy extent: must never be queued.
	if err := store.putJSON(fmt.Sprintf("%s%d", prefixChunk, 98001), &ChunkMeta{
		ID: ChunkID(98001), Size: 4096, State: ChunkReady,
		Replicas: []ReplicaInfo{{NodeID: 1, State: ReplicaReady}},
		Checksum: 0xABCDEF,
	}); err != nil {
		t.Fatalf("seed healthy chunk: %v", err)
	}
	if err := store.putExtentMeta(&ExtentMetaV2{
		ID: ExtentIDV2(98001), Generation: 1, LogicalLen: 4096, Lifecycle: LifecycleReady,
	}); err != nil {
		t.Fatalf("seed healthy extent: %v", err)
	}
	// Unhealthy extent: every replica Failed, no healthy source.
	if err := store.putJSON(fmt.Sprintf("%s%d", prefixChunk, 99002), &ChunkMeta{
		ID: ChunkID(99002), Size: 4096, State: ChunkDegraded,
		Replicas: []ReplicaInfo{{NodeID: 1, State: ReplicaFailed}, {NodeID: 2, State: ReplicaFailed}},
	}); err != nil {
		t.Fatalf("seed unhealthy chunk: %v", err)
	}
	if err := store.putExtentMeta(&ExtentMetaV2{
		ID: ExtentIDV2(99002), Generation: 1, LogicalLen: 4096, Lifecycle: LifecycleReadyDegraded,
	}); err != nil {
		t.Fatalf("seed unhealthy extent: %v", err)
	}

	scrubber := NewExtentScrubber(store)
	result, err := scrubber.Scan(ctx)
	if err != nil {
		t.Fatalf("extent scrub: %v", err)
	}
	if result.Unhealthy != 1 {
		t.Fatalf("unhealthy = %d, want 1", result.Unhealthy)
	}
	if result.RepairTriggered != 1 {
		t.Fatalf("repair_triggered = %d, want 1", result.RepairTriggered)
	}

	tasks, err := store.GetRepairQueue(ctx)
	if err != nil {
		t.Fatalf("GetRepairQueue: %v", err)
	}
	if len(tasks) != 1 {
		t.Fatalf("repair queue size = %d, want 1 (healthy extent not queued)", len(tasks))
	}
	if tasks[0].ChunkID != ChunkID(99002) {
		t.Fatalf("queued chunk = %d, want 99002", tasks[0].ChunkID)
	}
	if tasks[0].Reason != "extent_unhealthy" {
		t.Fatalf("reason = %q, want %q", tasks[0].Reason, "extent_unhealthy")
	}
	if tasks[0].Priority != 1 {
		t.Fatalf("priority = %d, want 1", tasks[0].Priority)
	}

	// A second pass re-triggers the same queue key — idempotent, not additive.
	idem, err := scrubber.Scan(ctx)
	if err != nil {
		t.Fatalf("extent scrub repeat: %v", err)
	}
	if idem.RepairTriggered != 1 {
		t.Fatalf("repair_triggered on repeat scan = %d, want 1", idem.RepairTriggered)
	}
	if tasks, err = store.GetRepairQueue(ctx); err != nil {
		t.Fatalf("GetRepairQueue after repeat: %v", err)
	}
	if len(tasks) != 1 {
		t.Fatalf("repair queue size after repeat = %d, want 1", len(tasks))
	}
}

// TestExtentScrubber_StartStop verifies the periodic worker actually runs
// Scan: with a short interval it heals a recoverable extent, and Stop is
// idempotent and does not hang.
func TestExtentScrubber_StartStop(t *testing.T) {
	store, chunk := newScrubExtentFixture(t)
	simulateRepairCompletion(t, store, chunk)

	scrubber := NewExtentScrubber(store)
	scrubber.Start(5 * time.Millisecond)
	defer scrubber.Stop()

	ctx := context.Background()
	deadline := time.Now().Add(2 * time.Second)
	for {
		ext, err := store.GetExtentMeta(ctx, ExtentIDV2(chunk.ID))
		if err == nil && ext.Lifecycle == LifecycleReady {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("extent not recovered within 2s (lifecycle=%v err=%v)", ext, err)
		}
		time.Sleep(5 * time.Millisecond)
	}

	scrubber.Stop()
	if scrubber.running.Load() {
		t.Fatal("expected scrubber to stop")
	}
	scrubber.Stop() // idempotent — must not panic or hang
}

// TestServiceBundle_StartsExtentScrubberWhenConfigured verifies the
// ServiceBundle wires the extent scrubber under the same ScrubInterval gate
// as the V1 Scrubber: started when configured, absent when disabled, stopped
// on Close.
func TestServiceBundle_StartsExtentScrubberWhenConfigured(t *testing.T) {
	t.Run("configured", func(t *testing.T) {
		store := newTestPebbleStore(t)
		bundle, err := NewPebbleServiceBundle(
			store,
			WithLeaseTTL(0),
			WithGCInterval(0),
			WithScrubInterval(10*time.Millisecond),
		)
		if err != nil {
			t.Fatalf("NewPebbleServiceBundle: %v", err)
		}
		if bundle.ExtentScrub == nil {
			t.Fatal("expected ExtentScrub to be configured")
		}
		if !bundle.ExtentScrub.running.Load() {
			t.Fatal("expected ExtentScrub to be running")
		}
		if err := bundle.Close(); err != nil {
			t.Fatalf("bundle close: %v", err)
		}
		if bundle.ExtentScrub.running.Load() {
			t.Fatal("expected ExtentScrub to stop during bundle close")
		}
	})

	t.Run("disabled", func(t *testing.T) {
		store := newTestPebbleStore(t)
		bundle, err := NewPebbleServiceBundle(
			store,
			WithLeaseTTL(0),
			WithGCInterval(0),
			WithScrubInterval(0),
		)
		if err != nil {
			t.Fatalf("NewPebbleServiceBundle: %v", err)
		}
		defer bundle.Close()
		if bundle.ExtentScrub != nil {
			t.Fatal("expected ExtentScrub to be nil when scrub disabled")
		}
	})
}
