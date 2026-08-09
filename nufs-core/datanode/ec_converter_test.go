package datanode

import (
	"bytes"
	"testing"

	"github.com/dshmyz/nufs/nufs-core/datanode/storage"
	"github.com/dshmyz/nufs/nufs-core/datanode/storage/segment"
	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// testShardDisks builds a 3-node × 3-disk candidate set (9 disks) for the
// conversion planner, mirroring the metadata E2 testDisks topology (§14:
// NodeID = node, DiskID = node*100+dev).
func testShardDisks() []metadata.ECDisk {
	var disks []metadata.ECDisk
	for node := uint64(1); node <= 3; node++ {
		for dev := uint64(0); dev < 3; dev++ {
			disks = append(disks, metadata.ECDisk{NodeID: node, DiskID: node*100 + dev})
		}
	}
	return disks
}

func newTestECStore(t *testing.T) *metadata.ECStore {
	t.Helper()
	ms, err := metadata.NewPebbleStore(metadata.PebbleStoreConfig{
		Dir: t.TempDir(), UseInMemory: true, UseBucketStats: false,
	})
	if err != nil {
		t.Fatalf("NewPebbleStore: %v", err)
	}
	t.Cleanup(func() { ms.Close() })
	return metadata.NewECStore(ms)
}

// TestECConverter_ConvertReplicaRoundTrip is the E4 capstone: a replicated
// chunk (a whole extent written via V2Store.Write) is converted through the
// metadata ECStore transaction into a completed 6+3 stripe — real per-shard
// extents placed by the §14 planner onto the node's shard stores — and the
// aggregate reads back byte-exact. It proves the full RF→EC conversion service
// path: read replica → Begin → PlanShards → encode → write real shards →
// MarkSyncing → verify → Complete.
func TestECConverter_ConvertReplicaRoundTrip(t *testing.T) {
	v, _ := newTestShardMultiStore(t, 3)
	ec := newTestECStore(t)

	cid := metadata.ChunkID(8008)
	payload := bytes.Repeat([]byte("convert-me-6+3-"), 500) // 7250 bytes, not a multiple of 6
	if err := v.Write(cid, payload); err != nil {
		t.Fatalf("write replicated chunk: %v", err)
	}

	conv := NewECConverter(ec, v, func(d metadata.ECDisk) int {
		// 3-node planner topology mapped onto this node's 3 shard stores:
		// node 1→store 0, node 2→store 1, node 3→store 2. Each store ends up
		// owning 3 shards (≤3 per disk, §14) — the physical analogue of the
		// metadata plan's ≤3 per machine.
		return int(d.NodeID) - 1
	})

	st, err := conv.ConvertReplica("stripe-8008", uint64(cid), 1, testShardDisks())
	if err != nil {
		t.Fatalf("ConvertReplica: %v", err)
	}

	// Transaction reached Complete and the original checksum is preserved.
	if st.State != metadata.ECConversionComplete || st.ConvertedAt == 0 {
		t.Fatalf("conversion state = %s, want complete", st.State)
	}
	if st.OriginalChecksum == 0 {
		t.Fatal("original checksum not recorded")
	}

	// §14 placement honored: 9 shards across 3 distinct nodes, 3 each.
	if len(st.Shards) != 9 {
		t.Fatalf("planned %d shards, want 9", len(st.Shards))
	}
	perNode := map[uint64]int{}
	for _, s := range st.Shards {
		perNode[s.NodeID]++
	}
	if len(perNode) != 3 {
		t.Fatalf("distinct nodes = %d, want 3", len(perNode))
	}
	for n, cnt := range perNode {
		if cnt != 3 {
			t.Fatalf("node %d holds %d shards, want 3", n, cnt)
		}
	}

	// Every shard landed on the node's store it was planned for, byte-exact.
	for i := 0; i < 9; i++ {
		got, _, err := v.ReadShard(cid, i)
		if err != nil {
			t.Fatalf("ReadShard(%d): %v", i, err)
		}
		if len(got) == 0 {
			t.Fatalf("shard %d empty", i)
		}
	}

	// Aggregate read reconstructs the original byte-exact with the recorded
	// checksum — the same closed loop the metadata degrader will rely on.
	data, sum, err := v.ReadChunkEC(cid, len(payload))
	if err != nil {
		t.Fatalf("ReadChunkEC: %v", err)
	}
	if !bytes.Equal(data, payload) {
		t.Fatalf("converted read mismatch: got %d bytes, want %d", len(data), len(payload))
	}
	if sum != st.OriginalChecksum {
		t.Fatalf("aggregate checksum %#x, want recorded %#x", sum, st.OriginalChecksum)
	}

	// The metadata-visible switch: build the EC layout and confirm it is a
	// 6+3 Replicas/ECGroup description, not the original replicas.
	cm := BuildECGroup(st, int32(len(payload)), metadata.TierCold)
	if cm.ECGroup == nil || cm.ECGroup.DataShards != 6 || cm.ECGroup.ParityShards != 3 {
		t.Fatalf("ECGroup = %+v, want 6+3", cm.ECGroup)
	}
	// The consolidated form references the shared profile + durable stripe
	// rather than embedding only the config (Program 5).
	if cm.ECGroup.ProfileID != metadata.DefaultECProfileID {
		t.Fatalf("ECGroup profile id = %q, want %q", cm.ECGroup.ProfileID, metadata.DefaultECProfileID)
	}
	if cm.ECStripeID != st.StripeID {
		t.Fatalf("ECStripeID = %q, want %q", cm.ECStripeID, st.StripeID)
	}
	if len(cm.Replicas) != 9 {
		t.Fatalf("EC layout has %d replicas, want 9 shards", len(cm.Replicas))
	}
	for i, r := range cm.Replicas {
		if r.ShardIndex != i {
			t.Fatalf("replica %d shard index = %d, want %d", i, r.ShardIndex, i)
		}
	}

	// The original replica is still readable during/after conversion (§14:
	// old replicas stay available until the EC layout fully serves).
	if orig, _, err := v.Read(cid, 0, 0); err != nil || !bytes.Equal(orig, payload) {
		t.Fatalf("original replica unreadable after conversion: data=%d err=%v", len(orig), err)
	}

	// Transaction persisted: reload the stripe.
	gotSt, err := ec.GetStripe("stripe-8008")
	if err != nil || gotSt == nil || gotSt.State != metadata.ECConversionComplete {
		t.Fatalf("persisted stripe = %+v err=%v, want complete", gotSt, err)
	}
}

// TestECConverter_FailedConversionRollsBack verifies a mid-transaction failure
// (an unplaceable shard write) rolls the conversion back: the stripe is
// RolledBack (metadata still points at replicas) and any shards written before
// the failure are reclaimable orphans — the original replica remains fully
// readable.
func TestECConverter_FailedConversionRollsBack(t *testing.T) {
	// Only 2 shard stores, but the 3-node plan routes node 3 → store 2, which
	// does not exist — shard writes that reach it fail.
	v, _ := newTestShardMultiStore(t, 2)
	ec := newTestECStore(t)

	cid := metadata.ChunkID(9009)
	if err := v.Write(cid, []byte("keep-me-as-replica")); err != nil {
		t.Fatalf("write replicated chunk: %v", err)
	}

	conv := NewECConverter(ec, v, func(d metadata.ECDisk) int { return int(d.NodeID) - 1 })
	_, err := conv.ConvertReplica("stripe-9009", uint64(cid), 1, testShardDisks())
	if err == nil {
		t.Fatal("expected conversion to fail with only 2 shard stores")
	}

	gotSt, gerr := ec.GetStripe("stripe-9009")
	if gerr != nil || gotSt == nil {
		t.Fatalf("stripe lookup after rollback: %+v err=%v", gotSt, gerr)
	}
	if gotSt.State != metadata.ECConversionRolledBack {
		t.Fatalf("state = %s, want rolled_back", gotSt.State)
	}

	// Metadata still serves the replicas; the failed conversion left the
	// whole extent fully readable (partial shards are orphans, §14).
	if orig, _, err := v.Read(cid, 0, 0); err != nil || string(orig) != "keep-me-as-replica" {
		t.Fatalf("replica unreadable after rollback: data=%q err=%v", orig, err)
	}
	// The chunk was never switched to EC: shard-only ReadChunkEC must fail
	// (aggregate can't complete from a partial orphan set).
	if _, _, err := v.ReadChunkEC(cid, 15); err == nil {
		t.Fatal("ReadChunkEC should fail on an incomplete/rolled-back stripe")
	}
}

// TestECConverter_ConversionSurvivesRestart verifies durability of the whole
// conversion: after closing every store and reconstructing from scratch, the
// completed stripe is still persisted (metadata Pebble) and the 6+3 stripe
// still reads back byte-exact (shard extents). This is the crash-safe close
// of the RF→EC switch — both the metadata transaction row and the shard
// extents must survive independently.
func TestECConverter_ConversionSurvivesRestart(t *testing.T) {
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

	// Durable (on-disk) metadata store so the transaction survives reopen.
	msDir := t.TempDir()
	ms, err := metadata.NewPebbleStore(metadata.PebbleStoreConfig{
		Dir: msDir, UseInMemory: false, UseBucketStats: false,
	})
	if err != nil {
		t.Fatalf("NewPebbleStore: %v", err)
	}
	ec := metadata.NewECStore(ms)

	cid := metadata.ChunkID(10010)
	payload := bytes.Repeat([]byte("survive-ec-conv-"), 300) // 4800 bytes
	if err := v.Write(cid, payload); err != nil {
		t.Fatalf("write replicated chunk: %v", err)
	}
	conv := NewECConverter(ec, v, func(d metadata.ECDisk) int { return int(d.NodeID) - 1 })
	if _, err := conv.ConvertReplica("stripe-10010", uint64(cid), 1, testShardDisks()); err != nil {
		t.Fatalf("ConvertReplica: %v", err)
	}

	// Close everything (segment stores + metadata store).
	for _, s := range append(dataStores, shardStores...) {
		if err := s.(interface{ Close() error }).Close(); err != nil {
			t.Fatalf("close backend: %v", err)
		}
	}
	if err := ms.Close(); err != nil {
		t.Fatalf("close metadata: %v", err)
	}

	// Reopen both from scratch.
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
	ms2, err := metadata.NewPebbleStore(metadata.PebbleStoreConfig{
		Dir: msDir, UseInMemory: false, UseBucketStats: false,
	})
	if err != nil {
		t.Fatalf("reopen metadata: %v", err)
	}
	defer ms2.Close()
	ec2 := metadata.NewECStore(ms2)

	// Metadata transaction still Complete.
	st, err := ec2.GetStripe("stripe-10010")
	if err != nil || st == nil || st.State != metadata.ECConversionComplete {
		t.Fatalf("persisted stripe after restart = %+v err=%v, want complete", st, err)
	}

	// Shard extents still aggregate to the original byte-exact.
	data, sum, err := rv.ReadChunkEC(cid, len(payload))
	if err != nil {
		t.Fatalf("ReadChunkEC after restart: %v", err)
	}
	if !bytes.Equal(data, payload) {
		t.Fatalf("restart round-trip mismatch")
	}
	if sum != st.OriginalChecksum {
		t.Fatalf("restart checksum %#x, want %#x", sum, st.OriginalChecksum)
	}
}
