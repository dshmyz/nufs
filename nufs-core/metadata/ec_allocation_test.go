package metadata

import (
	"context"
	"fmt"
	"testing"
)

// newECAllocTestStore returns an in-memory PebbleStore with 12 registered
// online nodes — enough to spread any scheme up to 8+4 (12 shards). Mirrors
// the smoke EC test's node registration so buildAllocatedChunks can place the
// full K+M replica set.
func newECAllocTestStore(t *testing.T) *PebbleStore {
	t.Helper()
	store, err := NewPebbleStore(PebbleStoreConfig{Dir: t.TempDir(), UseInMemory: true})
	if err != nil {
		t.Fatalf("NewPebbleStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()
	for i := NodeID(1); i <= 12; i++ {
		if err := store.RegisterNode(ctx, &NodeInfo{
			ID: i, Addr: fmt.Sprintf("127.0.0.1:%d", 9000+i),
			State: NodeOnline, CapacityGB: 100,
			Tier: TierHot, Zone: "z", Rack: "r", MachineID: "m",
		}); err != nil {
			t.Fatalf("RegisterNode %d: %v", i, err)
		}
	}
	return store
}

// TestAllocateChunks_HonorsBucketECConfig guards the allocation-side honor of
// the bucket's ECConfig: buildAllocatedChunks must size each chunk's ECGroup
// (the codec K/M) from the configured scheme, not the canonical 6+3 default.
//
// Before the fix, a 4+2 bucket allocated 6 replicas (TotalShards) but the
// ECGroup materialized the 9-shard 6+3 profile — the write path then persisted
// only the 6 *data* shards and never any parity, so the object had zero fault
// tolerance: the initial read passed (6 present == K=6) but any single shard
// loss dropped available to 5 < K=6 and decode failed
// ("ec: insufficient shards (have 5, need 6)").
func TestAllocateChunks_HonorsBucketECConfig(t *testing.T) {
	store := newECAllocTestStore(t)
	ctx := context.Background()

	cases := []struct {
		name   string
		data   int
		parity int
		wantID string
	}{
		{"EC(6,3) canonical", 6, 3, "ec-6-3"},
		{"EC(4,2)", 4, 2, "ec-4-2"},
		{"EC(8,4)", 8, 4, "ec-8-4"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			chunks, err := store.buildAllocatedChunks(ctx, []int64{0}, PlacementPolicy{
				ReplicationFactor: 1, // ignored for EC; TotalShards drives the replica count
				ECConfig:          &ECConfig{DataShards: tc.data, ParityShards: tc.parity},
			})
			if err != nil {
				t.Fatalf("buildAllocatedChunks: %v", err)
			}
			if len(chunks) != 1 {
				t.Fatalf("allocated %d chunks, want 1", len(chunks))
			}
			chunk := chunks[0]
			ec := chunk.ECGroup
			if ec == nil {
				t.Fatal("ECGroup is nil for an ECConfig bucket")
			}
			if ec.DataShards != tc.data || ec.ParityShards != tc.parity {
				t.Errorf("ECGroup K/M = %d+%d, want %d+%d (codec must match the bucket's ECConfig)",
					ec.DataShards, ec.ParityShards, tc.data, tc.parity)
			}
			if ec.ProfileID != tc.wantID {
				t.Errorf("ECGroup.ProfileID = %q, want %q", ec.ProfileID, tc.wantID)
			}
			wantReplicas := tc.data + tc.parity
			if len(chunk.Replicas) != wantReplicas {
				t.Fatalf("len(Replicas) = %d, want %d", len(chunk.Replicas), wantReplicas)
			}
			for i, rep := range chunk.Replicas {
				if rep.ShardIndex != i {
					t.Errorf("Replicas[%d].ShardIndex = %d, want %d", i, rep.ShardIndex, i)
				}
			}
		})
	}
}
