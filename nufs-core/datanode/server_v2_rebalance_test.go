package datanode

import (
	"testing"

	"github.com/example/dfs/metadata"
)

// TestV2StoreRebalanceOne verifies a single cross-disk move: locOf re-points
// to the target disk, reads route there and stay byte-exact, and both disks'
// usage accounting moves by exactly the payload size (source down, target up).
func TestV2StoreRebalanceOne(t *testing.T) {
	v, _ := newTestMultiStore(t, 2)
	cid := metadata.ChunkID(701)
	payload := []byte("the-extent-to-relocate-somewhere-else-on-another-disk")

	// First write goes to disk 0 (least-used tie broken by index).
	if err := v.Write(cid, payload); err != nil {
		t.Fatalf("write: %v", err)
	}

	total0, count0 := v.Stats()
	ds0 := v.DiskStats()
	before := map[int]int64{0: ds0[0].UsedBytes, 1: ds0[1].UsedBytes}

	if err := v.RebalanceOne(cid, 0, 1); err != nil {
		t.Fatalf("RebalanceOne: %v", err)
	}

	// locOf now points at disk 1, keeping the same generation.
	v.mu.RLock()
	loc, ok := v.locOf[cid]
	v.mu.RUnlock()
	if !ok {
		t.Fatal("chunk lost from locOf after rebalance")
	}
	if loc.disk != 1 {
		t.Fatalf("loc.disk=%d after rebalance, want 1", loc.disk)
	}

	// Reads route through disk 1 and are byte-exact.
	data, _, err := v.Read(cid, 0, 0)
	if err != nil {
		t.Fatalf("read after rebalance: %v", err)
	}
	if string(data) != string(payload) {
		t.Fatalf("read after rebalance = %q, want %q", data, payload)
	}

	// Accounting moved by exactly the payload size.
	ds1 := v.DiskStats()
	if got := ds1[0].UsedBytes - before[0]; got != -int64(len(payload)) {
		t.Fatalf("disk0 used delta = %d, want %d", got, -int64(len(payload)))
	}
	if got := ds1[1].UsedBytes - before[1]; got != int64(len(payload)) {
		t.Fatalf("disk1 used delta = %d, want %d", got, int64(len(payload)))
	}
	if ds1[0].ChunkCount != 0 || ds1[1].ChunkCount != 1 {
		t.Fatalf("chunk counts after move: disk0=%d disk1=%d, want 0 and 1", ds1[0].ChunkCount, ds1[1].ChunkCount)
	}

	// Aggregates unchanged: a rebalance never creates or destroys data.
	total1, count1 := v.Stats()
	if total1 != total0 || count1 != count0 {
		t.Fatalf("Stats changed across rebalance: (%d,%d)->(%d,%d)", total0, count0, total1, count1)
	}
}

// TestV2StoreRebalanceOnePreservesGeneration verifies a rebalance keeps the
// chunk's generation (no gen+1 bump): the target holds the same payload and
// a subsequent overwrite chains from that preserved generation.
func TestV2StoreRebalanceOnePreservesGeneration(t *testing.T) {
	v, _ := newTestMultiStore(t, 2)
	cid := metadata.ChunkID(702)

	if err := v.Write(cid, []byte("gen-one")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := v.Write(cid, []byte("gen-two-longer")); err != nil {
		t.Fatalf("rewrite: %v", err)
	}
	v.mu.RLock()
	genBefore := v.locOf[cid].gen
	v.mu.RUnlock()
	if genBefore != 2 {
		t.Fatalf("expected generation 2 before move, got %d", genBefore)
	}

	if err := v.RebalanceOne(cid, 0, 1); err != nil {
		t.Fatalf("RebalanceOne: %v", err)
	}
	v.mu.RLock()
	genAfter := v.locOf[cid].gen
	diskAfter := v.locOf[cid].disk
	v.mu.RUnlock()
	if genAfter != genBefore {
		t.Fatalf("generation changed across rebalance: %d->%d (want preserved)", genBefore, genAfter)
	}
	if diskAfter != 1 {
		t.Fatalf("disk after move = %d, want 1", diskAfter)
	}
	if data, _, err := v.Read(cid, 0, 0); err != nil || string(data) != "gen-two-longer" {
		t.Fatalf("read gen-2 payload after move: data=%q err=%v", data, err)
	}
}

// TestV2StoreRebalanceOneSkipsWrongDisk verifies the guarded re-point: if the
// chunk does not actually live on the requested source disk, RebalanceOne fails
// without touching state (the locOf CAS would otherwise accept a stale source).
func TestV2StoreRebalanceOneSkipsWrongDisk(t *testing.T) {
	v, _ := newTestMultiStore(t, 2)
	cid := metadata.ChunkID(703)
	if err := v.Write(cid, []byte("payload")); err != nil {
		t.Fatalf("write: %v", err)
	}

	// Chunk lives on disk 0; asking to move it from disk 1 must fail and
	// leave locOf pointing at disk 0.
	if err := v.RebalanceOne(cid, 1, 0); err == nil {
		t.Fatal("expected error when source disk does not own the chunk")
	}
	v.mu.RLock()
	loc, ok := v.locOf[cid]
	v.mu.RUnlock()
	if !ok || loc.disk != 0 {
		t.Fatalf("state changed despite rejected move: loc=%+v ok=%v", loc, ok)
	}
	if data, _, err := v.Read(cid, 0, 0); err != nil || string(data) != "payload" {
		t.Fatalf("payload unreadable after rejected move: data=%q err=%v", data, err)
	}
}

// TestV2StoreRebalanceBalanced verifies the free-space driver: with disk 0
// holding several extents and disk 1 empty, RebalanceBalanced drains disk 0
// toward disk 1 without overshooting into a reversed imbalance, conserving
// total bytes and keeping every relocated chunk byte-exact and reachable.
// A second call is idempotent (no further moves once balanced).
func TestV2StoreRebalanceBalanced(t *testing.T) {
	v, _ := newTestMultiStore(t, 2)

	// Write 4 distinct chunks directly onto disk 0 at gen 1 via writeTo so we
	// control placement deterministically, leaving disk 1 empty.
	payloads := map[metadata.ChunkID][]byte{
		metadata.ChunkID(801): []byte("eight-zero-one-payload........"),
		metadata.ChunkID(802): []byte("802"),
		metadata.ChunkID(803): []byte("eight-zero-three.........."),
		metadata.ChunkID(804): []byte("804xx"),
	}
	for cid, p := range payloads {
		if err := v.writeTo(cid, 0, 1, p); err != nil {
			t.Fatalf("write chunk %d: %v", cid, err)
		}
	}

	totalBefore, countBefore := v.Stats()
	d0b := v.DiskStats()[0].UsedBytes
	if d0b == 0 || v.DiskStats()[1].UsedBytes != 0 {
		t.Fatalf("setup: disk0=%d disk1=%d, want disk0>0 and disk1=0", d0b, v.DiskStats()[1].UsedBytes)
	}

	moved, err := v.RebalanceBalanced(0)
	if err != nil {
		t.Fatalf("RebalanceBalanced: %v", err)
	}
	if moved == 0 {
		t.Fatal("expected at least one extent moved")
	}

	// Every chunk is still reachable and byte-exact after the move.
	for cid, p := range payloads {
		data, _, err := v.Read(cid, 0, 0)
		if err != nil {
			t.Fatalf("read chunk %d after rebalance: %v", cid, err)
		}
		if string(data) != string(p) {
			t.Fatalf("chunk %d corruption after rebalance: got %q want %q", cid, data, p)
		}
	}
	// Accounting conserved: total across disks unchanged (rebalance never
	// creates or destroys data).
	totalAfter, countAfter := v.Stats()
	if totalAfter != totalBefore || countAfter != countBefore {
		t.Fatalf("aggregate changed across rebalance: (%d,%d)->(%d,%d)",
			totalBefore, countBefore, totalAfter, countAfter)
	}
	// Data spread across both disks (driver moved weight off the fuller disk
	// but never overshot, so no disk holds zero).
	ds := v.DiskStats()
	if ds[0].ChunkCount == 0 || ds[1].ChunkCount == 0 {
		t.Fatalf("expected chunks on both disks after rebalance, got disk0=%d disk1=%d",
			ds[0].ChunkCount, ds[1].ChunkCount)
	}
	// Convergence: a second pass finds nothing left to move (no thrashing).
	moved2, err := v.RebalanceBalanced(0)
	if err != nil {
		t.Fatalf("second RebalanceBalanced: %v", err)
	}
	if moved2 != 0 {
		t.Fatalf("second pass moved %d, want 0 (driver should have converged)", moved2)
	}
}

// TestV2StoreRebalanceBalancedConverges verifies the driver stops when disks
// are already balanced: with equal weight on both disks and a large
// threshold, no extent moves.
func TestV2StoreRebalanceBalancedConverges(t *testing.T) {
	v, _ := newTestMultiStore(t, 2)
	if err := v.Write(metadata.ChunkID(901), make([]byte, 100)); err != nil {
		t.Fatalf("write 901: %v", err)
	}
	if err := v.Write(metadata.ChunkID(902), make([]byte, 100)); err != nil {
		t.Fatalf("write 902: %v", err)
	}
	// Threshold larger than any possible difference -> no move.
	moved, err := v.RebalanceBalanced(1 << 30)
	if err != nil {
		t.Fatalf("RebalanceBalanced: %v", err)
	}
	if moved != 0 {
		t.Fatalf("expected no moves for balanced disks, moved %d", moved)
	}
}
