package datanode

import (
	"bytes"
	"context"
	"testing"

	"github.com/example/dfs/datanode/storage"
	"github.com/example/dfs/metadata"
)

// This file is Program 6 Phase F2: the EC self-heal scan. When shards of a 6+3
// stripe are lost (disk/node degrades), ECSelfHealer discovers the degraded
// chunk on its periodic sweep, and — when the loss is within §14 tolerance and
// the stripe's original length resolves from metadata — drives RepairChunkEC to
// rebuild the missing shards from the survivors, restoring the full nine.

// stubChunkResolver serves chunk.Size (the authoritative original pre-encoding
// length, §14) for the self-healer without a live metadata HTTP server.
type stubChunkResolver struct {
	size int
}

func (s stubChunkResolver) GetChunk(ctx context.Context, cid metadata.ChunkID) (*metadata.ChunkMeta, error) {
	return &metadata.ChunkMeta{ID: cid, Size: int32(s.size)}, nil
}

// TestECSelfHeal_ResolvesDegradedStripe restores a stripe after three shards
// are lost: one Enumerate pass discovers the degraded chunk, resolves its
// original length from the stub metadata, and repairs all three missing shards
// from the six survivors — leaving a full byte-exact stripe.
func TestECSelfHeal_ResolvesDegradedStripe(t *testing.T) {
	v, _ := newTestShardMultiStore(t, 3)
	cid := metadata.ChunkID(20001)
	payload := bytes.Repeat([]byte("self-heal-6+3-"), 800)
	placement := []int{0, 1, 2, 0, 1, 2, 0, 1, 2}
	if err := v.WriteChunkEC(cid, payload, placement); err != nil {
		t.Fatalf("WriteChunkEC: %v", err)
	}

	lost := []int{1, 4, 7}
	for _, idx := range lost {
		if err := v.DeleteShard(cid, idx); err != nil {
			t.Fatalf("DeleteShard(%d): %v", idx, err)
		}
	}

	h := NewECSelfHealer(v, stubChunkResolver{size: len(payload)}, ECSelfHealConfig{})
	h.Enumerate(context.Background())

	// All nine shards back, byte-exact, checksum verified, idempotent re-scan.
	for idx := 0; idx < 9; idx++ {
		if _, _, err := v.ReadShard(cid, idx); err != nil {
			t.Fatalf("shard %d not restored: %v", idx, err)
		}
	}
	data, sum, err := v.ReadChunkEC(cid, len(payload))
	if err != nil {
		t.Fatalf("ReadChunkEC after self-heal: %v", err)
	}
	if !bytes.Equal(data, payload) {
		t.Fatal("post-heal mismatch")
	}
	if want := storage.CRC32C(payload); sum != want {
		t.Fatalf("post-heal checksum = %#x, want %#x", sum, want)
	}

	// Stats: one chunk scanned, three shards repaired.
	if scanned, repaired, skipped, failed := h.Stats(); scanned != 1 || repaired != 3 || skipped != 0 || failed != 0 {
		t.Fatalf("stats = (scanned=%d repaired=%d skipped=%d failed=%d), want (1,3,0,0)",
			scanned, repaired, skipped, failed)
	}
	// A full stripe is a no-op: re-scan leaves everything in place.
	h.Enumerate(context.Background())
	if scanned, repaired, _, _ := h.Stats(); scanned != 2 || repaired != 3 {
		t.Fatalf("after no-op re-scan stats = (scanned=%d repaired=%d), want (2,3)", scanned, repaired)
	}
}

// TestECSelfHeal_SkipsBeyondTolerance leaves four shards down (below the 6/9
// reconstruction quorum): the healer must not fabricate shards it cannot
// verify, so it skips and leaves the stripe degraded.
func TestECSelfHeal_SkipsBeyondTolerance(t *testing.T) {
	v, _ := newTestShardMultiStore(t, 3)
	cid := metadata.ChunkID(20002)
	payload := bytes.Repeat([]byte("self-heal-skip-6+3-"), 500)
	placement := []int{0, 1, 2, 0, 1, 2, 0, 1, 2}
	if err := v.WriteChunkEC(cid, payload, placement); err != nil {
		t.Fatalf("WriteChunkEC: %v", err)
	}
	for _, idx := range []int{0, 1, 2, 3} {
		if err := v.DeleteShard(cid, idx); err != nil {
			t.Fatalf("DeleteShard(%d): %v", idx, err)
		}
	}

	h := NewECSelfHealer(v, stubChunkResolver{size: len(payload)}, ECSelfHealConfig{})
	h.Enumerate(context.Background())

	scanned, repaired, skipped, failed := h.Stats()
	if scanned != 1 || repaired != 0 || skipped != 1 || failed != 0 {
		t.Fatalf("stats = (scanned=%d repaired=%d skipped=%d failed=%d), want (1,0,1,0)",
			scanned, repaired, skipped, failed)
	}
	// Still exactly five shards present — nothing fabricated.
	if _, _, missing, err := v.ReadChunkECDegraded(cid, len(payload)); err == nil && len(missing) != 4 {
		t.Fatalf("missing = %v, want the 4 lost shards", missing)
	}
}

// TestECSelfHeal_SkipsWithoutOriginalLen with no resolver (or an unresolvable
// chunk) cannot safely decode/reconstruct, so repair is skipped rather than
// writing garbage shards.
func TestECSelfHeal_SkipsWithoutOriginalLen(t *testing.T) {
	v, _ := newTestShardMultiStore(t, 3)
	cid := metadata.ChunkID(20003)
	payload := []byte("self-heal-no-resolver")
	placement := []int{0, 1, 2, 0, 1, 2, 0, 1, 2}
	if err := v.WriteChunkEC(cid, payload, placement); err != nil {
		t.Fatalf("WriteChunkEC: %v", err)
	}
	if err := v.DeleteShard(cid, 5); err != nil {
		t.Fatalf("DeleteShard: %v", err)
	}

	// No resolver → the original length is unknowable → skip repair.
	h := NewECSelfHealer(v, nil, ECSelfHealConfig{})
	h.Enumerate(context.Background())
	scanned, repaired, skipped, _ := h.Stats()
	if scanned != 1 || repaired != 0 || skipped != 1 {
		t.Fatalf("stats = (scanned=%d repaired=%d skipped=%d), want (1,0,1)", scanned, repaired, skipped)
	}
	if _, _, err := v.ReadShard(cid, 5); err == nil {
		t.Fatal("shard 5 should remain missing (repair skipped)")
	}
}
