package datanode

import (
	"bytes"
	"context"
	"testing"

	"github.com/example/dfs/datanode/storage"
	"github.com/example/dfs/metadata"
)

// This file is Program 6 Phase F2/F3: the EC self-heal scan. When shards of a
// 6+3 stripe are lost (disk/node degrades), ECSelfHealer discovers the degraded
// chunk on its periodic sweep, and — when the loss is within §14 tolerance and
// the stripe's original length resolves from metadata — drives RepairChunkEC to
// rebuild the missing shards from the survivors, restoring the full nine. F3
// adds the authoritative-landing preference: with an ECLandingResolver wired,
// a lost shard is rebuilt back onto the disk it originally landed on (§14).

// stubChunkResolver serves chunk.Size (the authoritative original pre-encoding
// length, §14) and, via stripeID, the reference that lets a landing resolver
// find the durable ECStripe — without a live metadata HTTP server. When
// replicas is set it is materialized into the resolved ChunkMeta so a
// cross-node repair has the per-shard owner addrs to reach over the wire.
type stubChunkResolver struct {
	size     int
	stripe   string
	replicas []metadata.ReplicaInfo
}

func (s stubChunkResolver) GetChunk(ctx context.Context, cid metadata.ChunkID) (*metadata.ChunkMeta, error) {
	return &metadata.ChunkMeta{ID: cid, Size: int32(s.size), ECStripeID: s.stripe, Replicas: s.replicas}, nil
}

// ecLandingStub wraps a real *metadata.ECStore as the healer's authoritative
// landing resolver (F3, §14) and records how often it is consulted, so the
// test can assert the repair path prefers the authoritative landing.
type ecLandingStub struct {
	ec    *metadata.ECStore
	calls int
}

func (s *ecLandingStub) ResolveStripeLanding(chunk *metadata.ChunkMeta) ([]metadata.ECShard, error) {
	s.calls++
	return s.ec.ResolveStripeLanding(chunk)
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

// buildF3Authority persists a durable ECStripe whose per-shard landing matches
// placement: DiskID = 1000 + placement[i] (so DiskID%1000 resolves to the local
// shard-store index the healer's V2Store expects), node 1 hosting all nine.
func buildF3Authority(t *testing.T, placement []int) (*metadata.ECStore, *ecLandingStub) {
	t.Helper()
	pb, err := metadata.NewPebbleStore(metadata.PebbleStoreConfig{
		Dir: t.TempDir(), UseInMemory: true, UseBucketStats: false,
	})
	if err != nil {
		t.Fatalf("NewPebbleStore: %v", err)
	}
	t.Cleanup(func() { _ = pb.Close() })
	auth := metadata.NewECStore(pb)
	var shards []metadata.ECShard
	for i, d := range placement {
		shards = append(shards, metadata.ECShard{Index: i, NodeID: 1, DiskID: 1000 + uint64(d)})
	}
	if err := auth.PutStripe(&metadata.ECStripe{StripeID: "stripe-f3", Shards: shards}); err != nil {
		t.Fatalf("PutStripe: %v", err)
	}
	return auth, &ecLandingStub{ec: auth}
}

// TestECSelfHeal_RepairsOntoAuthoritativeLanding (F3, §14) wires the durable
// landing into the repair: after three shards are lost, self-heal rebuilds each
// one back onto the disk the stripe originally landed on (not a least-used
// replacement), consulting the landing resolver on the way, and leaves a full
// byte-exact stripe.
func TestECSelfHeal_RepairsOntoAuthoritativeLanding(t *testing.T) {
	v, _ := newTestShardMultiStore(t, 4)
	cid := metadata.ChunkID(30001)
	payload := bytes.Repeat([]byte("landing-6+3-"), 600)
	placement := []int{0, 1, 2, 3, 0, 1, 2, 3, 0}

	// Materialize only the six-shard quorum, leaving shards {1,4,7} absent on
	// this node while their authoritative landing disks remain accepting. This
	// mirrors a shard lost without the landing disk being tombstoned (e.g. it
	// was never written onto the current generation of an otherwise-healthy
	// disk), so the landing preference — not a fallback — is what restores them.
	shards, err := encodeEC63(payload)
	if err != nil {
		t.Fatalf("encodeEC63: %v", err)
	}
	lost := []int{1, 4, 7}
	for idx, d := range placement {
		if slicesContains(lost, idx) {
			continue
		}
		if err := v.WriteShardAtDisk(cid, idx, d, shards[idx]); err != nil {
			t.Fatalf("WriteShardAtDisk(%d -> %d): %v", idx, d, err)
		}
	}

	_, landing := buildF3Authority(t, placement)
	h := NewECSelfHealer(v, stubChunkResolver{size: len(payload), stripe: "stripe-f3"}, ECSelfHealConfig{})
	h.SetLandingResolver(landing)
	h.Enumerate(context.Background())

	// Each restored shard routes back onto its authoritative landing disk.
	for _, idx := range lost {
		if got := v.shardDisk(cid, idx); got != placement[idx] {
			t.Fatalf("shard %d routed to disk %d, want authoritative disk %d", idx, got, placement[idx])
		}
	}
	// The landing resolver was consulted on the repair path (the F3 seam).
	if landing.calls == 0 {
		t.Fatal("ResolveStripeLanding was not consulted on the repair path")
	}
	// Full stripe reads byte-exact with the original checksum.
	data, sum, err := v.ReadChunkEC(cid, len(payload))
	if err != nil {
		t.Fatalf("ReadChunkEC after authoritative landing repair: %v", err)
	}
	if !bytes.Equal(data, payload) {
		t.Fatal("post-heal mismatch")
	}
	if want := storage.CRC32C(payload); sum != want {
		t.Fatalf("post-heal checksum = %#x, want %#x", sum, want)
	}
}

// slicesContains reports whether xs contains x.
func slicesContains(xs []int, x int) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

// TestECSelfHeal_FallsBackWhenAuthoritativeDiskTombstoned (F3, §14) covers the
// precise fallback: when the authoritative landing disk can no longer accept a
// fresh write (the lost shard was deleted off it, tombstoning its generation),
// self-heal must NOT write back there — it falls back to a healthy least-used
// disk, still leaving a byte-exact stripe.
func TestECSelfHeal_FallsBackWhenAuthoritativeDiskTombstoned(t *testing.T) {
	v, _ := newTestShardMultiStore(t, 4)
	cid := metadata.ChunkID(30002)
	payload := bytes.Repeat([]byte("landing-fallback-6+3-"), 500)
	placement := []int{0, 1, 2, 3, 0, 1, 2, 3, 0}
	if err := v.WriteChunkEC(cid, payload, placement); err != nil {
		t.Fatalf("WriteChunkEC: %v", err)
	}
	_, landing := buildF3Authority(t, placement)

	h := NewECSelfHealer(v, stubChunkResolver{size: len(payload), stripe: "stripe-f3"}, ECSelfHealConfig{})
	h.SetLandingResolver(landing)

	// Lost shard 2 was deleted off its authoritative disk (placement[2] == 2),
	// tombstoning gen 3 there — so that disk can no longer accept a fresh write.
	lost := 2
	if err := v.DeleteShard(cid, lost); err != nil {
		t.Fatalf("DeleteShard(%d): %v", lost, err)
	}
	h.Enumerate(context.Background())

	// The rebuild falls back to a different healthy disk, not the tombstoned one.
	got := v.shardDisk(cid, lost)
	if got < 0 || got >= 4 {
		t.Fatalf("hard-fallback disk %d out of range", got)
	}
	if got == placement[lost] {
		t.Fatalf("repair landed back on the tombstoned authoritative disk %d", got)
	}
	// Stripe is restored and reads byte-exact.
	data, _, err := v.ReadChunkEC(cid, len(payload))
	if err != nil {
		t.Fatalf("ReadChunkEC after fallback repair: %v", err)
	}
	if !bytes.Equal(data, payload) {
		t.Fatal("post-fallback mismatch")
	}
}

// TestECSelfHeal_CrossNodeRebuildsRemoteShard (Task #202) proves the healer,
// when wired with a peer dialer, repairs a shard lost on a *different* node.
// The old node-local path could never see it: a healer on node 1 only scans its
// own ~3 shards — all present means loss 0 and a no-op — so a shard deleted on
// node 2 stayed lost forever even though the cluster still held six survivors
// (the soak defect). With the cluster-wide view the healer reads every shard
// from its owning node over the wire, reconstructs the missing one, and pushes
// it back to its authoritative landing node/disk.
func TestECSelfHeal_CrossNodeRebuildsRemoteShard(t *testing.T) {
	const (
		nNodes   = 3
		disksPer = 3
	)
	nodes, pb := buildClusterN(t, nNodes, disksPer)
	cid := metadata.ChunkID(40001)
	payload := bytes.Repeat([]byte("xnode-self-heal-6+3-"), 400)

	// §14 placement: shard i -> node i/3+1, landing disk i%3 (DiskID=node*1000+disk,
	// so DiskID%1000 is the node-local shard-store index). Mirror it as the
	// ChunkMeta.Replicas the healer uses to reach each owner over the wire, and as
	// the durable stripe whose landing the repair must prefer.
	placement := make([]metadata.ECShard, 9)
	replicas := make([]metadata.ReplicaInfo, 9)
	shards := mustEncodeEC63(t, payload)
	for i := 0; i < 9; i++ {
		node := nodes[i/3]
		disk := i % 3
		placement[i] = metadata.ECShard{Index: i, NodeID: uint64(node.id), DiskID: uint64(node.id)*1000 + uint64(disk)}
		replicas[i] = metadata.ReplicaInfo{NodeID: node.id, Addr: node.srv.Addr(), ShardIndex: i}
		if err := node.v2.WriteShardAtDisk(cid, i, disk, shards[i]); err != nil {
			t.Fatalf("write shard %d -> node %d disk %d: %v", i, node.id, disk, err)
		}
	}
	// Sanity: the nine shards are spread across all three nodes.
	// (WriteShardAtDisk above already placed 0-2 on node 1, 3-5 on node 2, 6-8
	// on node 3, so each node hosts a share of the stripe.)

	// Delete shard 4 on node 2 — a shard the node-1 healer never holds, so its
	// node-local view would see all of its own shards present and no-op.
	if err := nodes[1].v2.DeleteShard(cid, 4); err != nil {
		t.Fatalf("DeleteShard(4) on node 2: %v", err)
	}
	if _, _, err := nodes[1].v2.ReadShard(cid, 4); err == nil {
		t.Fatal("shard 4 should be missing before healing")
	}

	h := NewECSelfHealer(nodes[0].v2,
		stubChunkResolver{size: len(payload), stripe: "stripe-xnode", replicas: replicas}, ECSelfHealConfig{})
	h.SetPeerDialer(func(addr string) *Client { return NewClient(addr) })
	auth := metadata.NewECStore(pb)
	if err := auth.PutStripe(&metadata.ECStripe{StripeID: "stripe-xnode", Shards: placement}); err != nil {
		t.Fatalf("PutStripe: %v", err)
	}
	h.SetLandingResolver(&ecLandingStub{ec: auth})
	h.Enumerate(context.Background())

	// The remote shard is restored byte-exact on its owning node (node 2).
	got, _, err := nodes[1].v2.ReadShard(cid, 4)
	if err != nil {
		t.Fatalf("shard 4 not restored on node 2: %v", err)
	}
	if !bytes.Equal(got, shards[4]) {
		t.Fatal("restored shard 4 mismatch")
	}
	// The restore did not re-write onto the tombstoned landing disk: shard 4
	// originally landed on node 2's disk (placement[4].DiskID%1000 == 1), which
	// DeleteShard tombstoned at gen 5 (§14 fence) — so the cross-node push must
	// have fallen back to a healthy disk on node 2 rather than returning an
	// idempotent non-persist. Assert the shard now lives on a *different* disk.
	if disk := nodes[1].v2.shardDisk(cid, 4); disk == int(placement[4].DiskID%1000) {
		t.Fatalf("shard 4 re-landed on the tombstoned disk %d (wanted fallback)",
			placement[4].DiskID%1000)
	}
	// The landing resolver was consulted (the F3 seam on the cross-node path).
	// (landing.calls is asserted below via the shared ecLandingStub.)

	// The full stripe decodes byte-exact across the cluster.
	all := make([][]byte, 9)
	for i := 0; i < 9; i++ {
		nd := nodes[i/3]
		d, _, err := nd.v2.ReadShard(cid, i)
		if err != nil {
			t.Fatalf("read shard %d: %v", i, err)
		}
		all[i] = d
	}
	dec, err := decodeEC63(all, len(payload))
	if err != nil {
		t.Fatalf("decode cluster aggregate: %v", err)
	}
	if !bytes.Equal(dec, payload) {
		t.Fatal("cluster decode mismatch after cross-node heal")
	}

	// Healer stats: one chunk scanned, one (remote) shard repaired.
	scanned, repaired, skipped, failed := h.Stats()
	if scanned != 1 || repaired != 1 || skipped != 0 || failed != 0 {
		t.Fatalf("stats = (scanned=%d repaired=%d skipped=%d failed=%d), want (1,1,0,0)",
			scanned, repaired, skipped, failed)
	}
}

// TestECSelfHeal_RepairsDirectECWithCapSize reproduces the production soak
// defect that motivated the cross-node fix: a directly-written EC chunk's
// ChunkMeta.Size holds the allocation cap (metadata.MaxChunkSize), not the
// literal payload length — so the healer's reconstruction, keyed on chunk.Size,
// demanded 64 MiB from a small object and failed ("reconstructed data too
// short"). The repair now clamps originalLen to the shard-derived padded length,
// so a degraded direct-EC stripe self-heals byte-exact. The payload's literal
// length is deliberately NOT a multiple of 6 so paddedLen > literal length.
func TestECSelfHeal_RepairsDirectECWithCapSize(t *testing.T) {
	v, _ := newTestShardMultiStore(t, 3)
	cid := metadata.ChunkID(50001)
	// 50001 bytes: not divisible by 6, so the encoder pads to a larger length.
	payload := bytes.Repeat([]byte("direct-ec-cap-size-"), 2632)[:50001]
	placement := []int{0, 1, 2, 0, 1, 2, 0, 1, 2}
	if err := v.WriteChunkEC(cid, payload, placement); err != nil {
		t.Fatalf("WriteChunkEC: %v", err)
	}
	// A directly-written chunk reports Size = the 64 MiB allocation cap (§14
	// / AllocateChunk), NOT the literal length — mirroring production.
	if err := v.DeleteShard(cid, 3); err != nil {
		t.Fatalf("DeleteShard(3): %v", err)
	}

	h := NewECSelfHealer(v, stubChunkResolver{size: metadata.MaxChunkSize}, ECSelfHealConfig{})
	h.Enumerate(context.Background())

	scanned, repaired, skipped, failed := h.Stats()
	if scanned != 1 || repaired != 1 || skipped != 0 || failed != 0 {
		t.Fatalf("stats = (scanned=%d repaired=%d skipped=%d failed=%d), want (1,1,0,0)",
			scanned, repaired, skipped, failed)
	}
	// The clamped repair restores the stripe byte-exact with the original CRC.
	data, sum, err := v.ReadChunkEC(cid, len(payload))
	if err != nil {
		t.Fatalf("ReadChunkEC after cap-size repair: %v", err)
	}
	if !bytes.Equal(data, payload) {
		t.Fatal("post-repair mismatch")
	}
	if want := storage.CRC32C(payload); sum != want {
		t.Fatalf("post-repair checksum = %#x, want %#x", sum, want)
	}
}

