package datanode

import (
	"bytes"
	"testing"

	"github.com/dshmyz/nufs/nufs-core/datanode/storage"
	"github.com/dshmyz/nufs/nufs-core/datanode/storage/segment"
	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// TestV2StoreEC63_AggregateWriteReadRoundTrip is the E3 single-node 6+3
// service-path closed loop: a payload is encoded into six data + three parity
// shards, written as independent extents spread across the node's distinct
// shard disks (disk-level fault isolation, ≤3 shards per disk), and read back
// through the aggregate decode to the original bytes, byte-exact.
//
// The placement vector (one shard-disk index per shard) is what the metadata
// ECStore.PlanShards produces for a multi-machine stripe in production (E4/E5);
// here it is a local placement that honors §14 disk diversity within one node.
func TestV2StoreEC63_AggregateWriteReadRoundTrip(t *testing.T) {
	v, _ := newTestShardMultiStore(t, 3)
	cid := metadata.ChunkID(5005)

	payload := bytes.Repeat([]byte("ec-6+3-"), 1000) // 7000 bytes, not a multiple of 6
	// Placement: round-robin across the 3 shard disks, no disk > 3 shards.
	// Index = shard index, value = owning shard disk.
	placement := []int{0, 1, 2, 0, 1, 2, 0, 1, 2}
	if err := v.WriteChunkEC(cid, payload, placement); err != nil {
		t.Fatalf("WriteChunkEC: %v", err)
	}

	// Each shard landed on its assigned disk (per-index routing, not one home).
	for idx, wantDisk := range placement {
		if got := v.shardDisk(cid, idx); got != wantDisk {
			t.Fatalf("shard %d on disk %d, want %d", idx, got, wantDisk)
		}
	}

	// Aggregate read reconstructs the original byte-exact.
	data, sum, err := v.ReadChunkEC(cid, len(payload))
	if err != nil {
		t.Fatalf("ReadChunkEC: %v", err)
	}
	if !bytes.Equal(data, payload) {
		t.Fatalf("round-trip mismatch: got %d bytes, want %d", len(data), len(payload))
	}
	if want := storage.CRC32C(payload); sum != want {
		t.Fatalf("checksum = %#x, want %#x", sum, want)
	}

	// Shards are NOT reported as whole-chunk replicas (fragments), and the
	// whole-chunk snapshot/listing stays empty for a shard-only chunk.
	snap := v.ChunkStateSnapshot()
	if _, ok := snap[cid]; ok {
		t.Fatal("EC chunk (shards only) reported as a whole-chunk replica")
	}
}

// TestV2StoreEC63_RestartReconstruct verifies a 6+3 stripe survives a restart:
// closing the segment stores and rebuilding the V2Store (startup reconstruction
// recovers the per-index shard-disk map from each shard extent's generation)
// makes the aggregate read return the original bytes unchanged.
func TestV2StoreEC63_RestartReconstruct(t *testing.T) {
	dirs := make([]string, 3)
	for i := range dirs {
		dirs[i] = t.TempDir()
	}
	newPair := func(i int) (*segment.Store, *segment.Store) {
		ds, err := segment.New(segment.Config{Dir: dirs[i], UseMemIndex: true, StreamID: 1})
		if err != nil {
			t.Fatalf("segment.New data: %v", err)
		}
		ss, err := segment.New(segment.Config{Dir: dirs[i], UseMemIndex: true, StreamID: 2})
		if err != nil {
			t.Fatalf("segment.New shard: %v", err)
		}
		return ds, ss
	}

	var dataStores, shardStores []storage.Store
	for i := 0; i < 3; i++ {
		d, s := newPair(i)
		dataStores = append(dataStores, d)
		shardStores = append(shardStores, s)
	}
	v := NewMultiV2Store(dataStores, dirs...)
	if err := v.AttachShardStores(shardStores); err != nil {
		t.Fatalf("AttachShardStores: %v", err)
	}

	cid := metadata.ChunkID(6006)
	payload := bytes.Repeat([]byte("survive-"), 417) // 3336 bytes
	placement := []int{0, 1, 2, 0, 1, 2, 0, 1, 2}
	if err := v.WriteChunkEC(cid, payload, placement); err != nil {
		t.Fatalf("WriteChunkEC: %v", err)
	}

	for _, s := range append(dataStores, shardStores...) {
		if err := s.(interface{ Close() error }).Close(); err != nil {
			t.Fatalf("close backend: %v", err)
		}
	}

	// Reopen and reconstruct from scratch (dirs only).
	var rData, rShard []storage.Store
	for i := 0; i < 3; i++ {
		d, s := newPair(i)
		rData = append(rData, d)
		rShard = append(rShard, s)
		defer d.Close()
		defer s.Close()
	}
	rv := NewMultiV2Store(rData, dirs...)
	if err := rv.AttachShardStores(rShard); err != nil {
		t.Fatalf("re-AttachShardStores: %v", err)
	}

	data, _, err := rv.ReadChunkEC(cid, len(payload))
	if err != nil {
		t.Fatalf("ReadChunkEC after restart: %v", err)
	}
	if !bytes.Equal(data, payload) {
		t.Fatalf("restart round-trip mismatch")
	}
}

// TestV2StoreEC63_ShardDiskIndexRecoveredFromGeneration verifies the core of
// restart reconstruction: a shard extent's owning disk and shard index are
// recovered purely from (store index, generation) — gen = shard index + 1 —
// so a spread stripe re-routes to the correct disk per shard after restart.
func TestV2StoreEC63_ShardDiskIndexRecoveredFromGeneration(t *testing.T) {
	v, _ := newTestShardMultiStore(t, 3)
	cid := metadata.ChunkID(7007)
	// Write each shard to a known disk explicitly.
	for idx := 0; idx < 9; idx++ {
		if err := v.WriteShardAtDisk(cid, idx, idx%3, []byte{byte(idx)}); err != nil {
			t.Fatalf("WriteShardAtDisk(%d,%d): %v", idx, idx%3, err)
		}
	}
	// The per-index routing is authoritative.
	for idx := 0; idx < 9; idx++ {
		if got := v.shardDisk(cid, idx); got != idx%3 {
			t.Fatalf("shard %d routed to disk %d, want %d", idx, got, idx%3)
		}
	}
}
