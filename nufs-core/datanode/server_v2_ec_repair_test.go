package datanode

import (
	"bytes"
	"testing"

	"github.com/example/dfs/datanode/storage"
	"github.com/example/dfs/metadata"
)

// TestV2StoreEC_DegradedReadReconstructsFromSurvivors is the E5 degraded-read
// core (§14): after losing three shards (across distinct shard disks), a read
// reconstructs the original byte-exact from the six survivors, still verifies
// the original checksum, and reports exactly which shards are missing. Losing
// a fourth shard (leaving only five) makes the degraded read fail cleanly —
// the quorum for 6+3 reconstruction is six.
func TestV2StoreEC_DegradedReadReconstructsFromSurvivors(t *testing.T) {
	v, _ := newTestShardMultiStore(t, 3)
	cid := metadata.ChunkID(11011)
	payload := bytes.Repeat([]byte("degraded-6+3-"), 800) // 9200 bytes
	placement := []int{0, 1, 2, 0, 1, 2, 0, 1, 2}
	if err := v.WriteChunkEC(cid, payload, placement); err != nil {
		t.Fatalf("WriteChunkEC: %v", err)
	}

	// Kill three shards on three distinct disks (§14 ≤3 per disk → one loss
	// per disk is the most taxing placement).
	lost := []int{1, 4, 7}
	for _, idx := range lost {
		if err := v.DeleteShard(cid, idx); err != nil {
			t.Fatalf("DeleteShard(%d): %v", idx, err)
		}
	}

	// The strict full read now fails (it requires all nine).
	if _, _, err := v.ReadChunkEC(cid, len(payload)); err == nil {
		t.Fatal("strict ReadChunkEC should fail with shards missing")
	}

	// The degraded read reconstructs the original byte-exact.
	data, sum, missing, err := v.ReadChunkECDegraded(cid, len(payload))
	if err != nil {
		t.Fatalf("ReadChunkECDegraded: %v", err)
	}
	if !bytes.Equal(data, payload) {
		t.Fatalf("degraded read mismatch: got %d bytes, want %d", len(data), len(payload))
	}
	if want := storage.CRC32C(payload); sum != want {
		t.Fatalf("degraded checksum = %#x, want %#x", sum, want)
	}
	if len(missing) != 3 {
		t.Fatalf("missing = %v, want the 3 lost shards", missing)
	}

	// Losing a fourth shard drops below the six-shard quorum → clean error.
	if err := v.DeleteShard(cid, 2); err != nil {
		t.Fatalf("DeleteShard(%d): %v", 2, err)
	}
	if _, _, _, err := v.ReadChunkECDegraded(cid, len(payload)); err == nil {
		t.Fatal("degraded read should fail with only five shards present")
	}
}

// TestV2StoreEC_RepairRebuildsMissingShards verifies the storage-layer repair
// (§14): after losing three shards, RepairChunkEC reconstructs the missing
// shards from the six survivors and writes them back to their owning disks,
// restoring a full nine-shard stripe that is byte-exact and fully readable.
func TestV2StoreEC_RepairRebuildsMissingShards(t *testing.T) {
	v, _ := newTestShardMultiStore(t, 3)
	cid := metadata.ChunkID(12012)
	payload := bytes.Repeat([]byte("repair-6+3-"), 700) // 5600 bytes
	placement := []int{0, 1, 2, 0, 1, 2, 0, 1, 2}
	if err := v.WriteChunkEC(cid, payload, placement); err != nil {
		t.Fatalf("WriteChunkEC: %v", err)
	}

	lost := []int{0, 3, 8}
	for _, idx := range lost {
		if err := v.DeleteShard(cid, idx); err != nil {
			t.Fatalf("DeleteShard(%d): %v", idx, err)
		}
	}
	if _, _, _, err := v.ReadChunkECDegraded(cid, len(payload)); err != nil {
		t.Fatalf("pre-repair degraded read: %v", err)
	}

	rebuilt, err := v.RepairChunkEC(cid, len(payload))
	if err != nil {
		t.Fatalf("RepairChunkEC: %v", err)
	}
	if rebuilt != 3 {
		t.Fatalf("repaired %d shards, want 3", rebuilt)
	}

	// Full nine shards present and the strict read reconstructs byte-exact.
	for _, idx := range []int{0, 1, 2, 3, 4, 5, 6, 7, 8} {
		if _, _, err := v.ReadShard(cid, idx); err != nil {
			t.Fatalf("shard %d not restored: %v", idx, err)
		}
	}
	data, sum, err := v.ReadChunkEC(cid, len(payload))
	if err != nil {
		t.Fatalf("ReadChunkEC after repair: %v", err)
	}
	if !bytes.Equal(data, payload) {
		t.Fatalf("post-repair mismatch")
	}
	if want := storage.CRC32C(payload); sum != want {
		t.Fatalf("post-repair checksum = %#x, want %#x", sum, want)
	}

	// Idempotent: repairing a full stripe is a no-op.
	if n, err := v.RepairChunkEC(cid, len(payload)); err != nil || n != 0 {
		t.Fatalf("idempotent repair: n=%d err=%v, want 0/nil", n, err)
	}
}

// TestV2StoreEC_ReheatRebuildsOntoFreshDisk verifies §14 reheat: when a whole
// shard disk (a full-stripe donor) is lost, a clean replacement disk joins the
// stripe and ReheatChunkEC reconstructs the complete nine-shard set from
// whatever survivors remain, writing every shard onto the replacement, leaving
// a full byte-exact stripe on the new location.
func TestV2StoreEC_ReheatRebuildsOntoFreshDisk(t *testing.T) {
	// Four shard disks: {0,1,2} host the original stripe, disk 3 is a fresh
	// replacement node that joins after disk 2 is lost. Deleting disk 2's
	// shards tombstones their generations, so a clean store (disk 3) is what
	// reheat targets — a lost disk cannot be re-written in place (§14 gen
	// fencing).
	v, _ := newTestShardMultiStore(t, 4)
	cid := metadata.ChunkID(13013)
	payload := bytes.Repeat([]byte("reheat-6+3-"), 900) // 6300 bytes
	placement := []int{0, 1, 2, 0, 1, 2, 0, 1, 2}
	if err := v.WriteChunkEC(cid, payload, placement); err != nil {
		t.Fatalf("WriteChunkEC: %v", err)
	}

	// Lose shard disk 2 (a whole replacement node): every shard it held — the
	// stripe's {2,5,8} — is deleted from it (tombstoned), leaving it a dead
	// donor. The stripe is degraded but still readable from the six survivors.
	victim := 2
	for idx := 0; idx < 9; idx++ {
		if placement[idx] == victim {
			if err := v.DeleteShard(cid, idx); err != nil {
				t.Fatalf("DeleteShard(%d): %v", idx, err)
			}
		}
	}
	if _, _, _, err := v.ReadChunkECDegraded(cid, len(payload)); err != nil {
		t.Fatalf("pre-reheat degraded read: %v", err)
	}

	// A clean replacement disk (3) joins and reheats the full stripe onto it.
	replacement := 3
	written, err := v.ReheatChunkEC(cid, len(payload), replacement)
	if err != nil {
		t.Fatalf("ReheatChunkEC: %v", err)
	}
	if written != 9 {
		t.Fatalf("reheated %d shards, want 9", written)
	}
	// Every shard now reads from the replacement disk.
	for idx := 0; idx < 9; idx++ {
		if got := v.shardDisk(cid, idx); got != replacement {
			t.Fatalf("shard %d routed to disk %d, want reheat target %d", idx, got, replacement)
		}
	}

	data, sum, err := v.ReadChunkEC(cid, len(payload))
	if err != nil {
		t.Fatalf("ReadChunkEC after reheat: %v", err)
	}
	if !bytes.Equal(data, payload) {
		t.Fatalf("post-reheat mismatch")
	}
	if want := storage.CRC32C(payload); sum != want {
		t.Fatalf("post-reheat checksum = %#x, want %#x", sum, want)
	}
}
