package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/example/dfs/datanode"
	"github.com/example/dfs/datanode/storage"
	"github.com/example/dfs/datanode/storage/segment"
	"github.com/example/dfs/metadata"
)

// buildOpsTestMux wires the real ops handlers onto a fresh mux over the given
// store, ready to serve through httptest. advertiseAddr is empty (no raft
// redirects to advertise).
func buildOpsTestMux(t *testing.T, store *metadata.PebbleStore, bundle *metadata.ServiceBundle) *http.ServeMux {
	t.Helper()
	mux := http.NewServeMux()
	registerOpsHandlers(mux, store, bundle, "")
	return mux
}

// TestECConvertHTTPContract drives the full replication→6+3 conversion
// transaction through the real ops mux + HTTPClient authority and asserts the
// metad service persisted a durable Complete stripe — the S2 metadata-authority
// contract. Each ECAuthority notch (begin → plan → mark-syncing → complete)
// round-trips over HTTP; the server owns the §14 placement and the lifecycle
// state.
func TestECConvertHTTPContract(t *testing.T) {
	store, bundle := newOpsTestStore(t)
	srv := httptest.NewServer(buildOpsTestMux(t, store, bundle))
	defer srv.Close()

	auth := metadata.NewHTTPClient(srv.URL, 10*time.Second)

	// Begin → Preparing, records the source extent + checksum.
	st, err := auth.BeginConversion("stripe-s2-contract", 42001, 3, 0xdeadbeef)
	if err != nil {
		t.Fatalf("BeginConversion: %v", err)
	}
	if st.State != metadata.ECConversionPreparing {
		t.Fatalf("state after begin = %s, want preparing", st.State)
	}
	if st.OriginalChecksum != 0xdeadbeef || st.ExtentID != 42001 || st.Generation != 3 {
		t.Fatalf("begin recorded %+v, want extent 42001 gen 3 csum deadbeef", st)
	}

	// Plan → Encoding with a §14-diverse 9-shard placement across 3 nodes.
	disks := []metadata.ECDisk{
		{NodeID: 1, DiskID: 1000}, {NodeID: 1, DiskID: 1001}, {NodeID: 1, DiskID: 1002},
		{NodeID: 2, DiskID: 2000}, {NodeID: 2, DiskID: 2001}, {NodeID: 2, DiskID: 2002},
		{NodeID: 3, DiskID: 3000}, {NodeID: 3, DiskID: 3001}, {NodeID: 3, DiskID: 3002},
	}
	if err := auth.PlanShards(st, disks); err != nil {
		t.Fatalf("PlanShards: %v", err)
	}
	if st.State != metadata.ECConversionEncoding {
		t.Fatalf("state after plan = %s, want encoding", st.State)
	}
	if len(st.Shards) != 9 {
		t.Fatalf("shards = %d, want 9", len(st.Shards))
	}
	for i, s := range st.Shards[:6] {
		if s.Index != i {
			t.Fatalf("shard %d index = %d", i, s.Index)
		}
	}
	perNode := map[uint64]int{}
	for _, s := range st.Shards {
		perNode[s.NodeID]++
	}
	if len(perNode) < 3 || len(perNode) != 3 {
		t.Fatalf("distinct nodes = %d, want 3 (plan must span ≥3 machines)", len(perNode))
	}
	for n, cnt := range perNode {
		if cnt > 3 {
			t.Fatalf("node %d holds %d shards, want ≤3 §14", n, cnt)
		}
	}

	// MarkSyncing → Syncing (shard payloads written).
	if err := auth.MarkSyncing(st); err != nil {
		t.Fatalf("MarkSyncing: %v", err)
	}
	if st.State != metadata.ECConversionSyncing {
		t.Fatalf("state after mark-syncing = %s, want syncing", st.State)
	}

	// Complete → durable Complete.
	if err := auth.CompleteConversion(st, time.Now()); err != nil {
		t.Fatalf("CompleteConversion: %v", err)
	}
	if st.State != metadata.ECConversionComplete {
		t.Fatalf("state after complete = %s, want complete", st.State)
	}
	if st.ConvertedAt == 0 {
		t.Fatal("converted_at not recorded")
	}

	// The authority persisted the Complete stripe: read it back out of the
	// same Pebble store the metad handlers write through.
	durable, err := metadata.NewECStore(store).GetStripe("stripe-s2-contract")
	if err != nil {
		t.Fatalf("GetStripe: %v", err)
	}
	if durable == nil {
		t.Fatal("stripe not persisted by metad authority")
	}
	if durable.State != metadata.ECConversionComplete {
		t.Fatalf("durable state = %s, want complete", durable.State)
	}
	if len(durable.Shards) != 9 {
		t.Fatalf("durable shards = %d, want 9", len(durable.Shards))
	}
	if durable.OriginalChecksum != 0xdeadbeef {
		t.Fatalf("durable checksum = %#x, want deadbeef", durable.OriginalChecksum)
	}
}

// TestECConvertHTTPContractRollback drives begin → plan → rollback and asserts
// the failed conversion is recorded RolledBack (metadata still points at
// replicas; partial shards become reclaimable orphans, §14).
func TestECConvertHTTPContractRollback(t *testing.T) {
	store, bundle := newOpsTestStore(t)
	srv := httptest.NewServer(buildOpsTestMux(t, store, bundle))
	defer srv.Close()

	auth := metadata.NewHTTPClient(srv.URL, 10*time.Second)
	st, err := auth.BeginConversion("stripe-s2-rollback", 42002, 9, 12345)
	if err != nil {
		t.Fatalf("BeginConversion: %v", err)
	}
	disks := []metadata.ECDisk{
		{NodeID: 1, DiskID: 1000}, {NodeID: 1, DiskID: 1001}, {NodeID: 1, DiskID: 1002},
		{NodeID: 2, DiskID: 2000}, {NodeID: 2, DiskID: 2001}, {NodeID: 2, DiskID: 2002},
		{NodeID: 3, DiskID: 3000}, {NodeID: 3, DiskID: 3001}, {NodeID: 3, DiskID: 3002},
	}
	if err := auth.PlanShards(st, disks); err != nil {
		t.Fatalf("PlanShards: %v", err)
	}
	if len(st.Shards) != 9 {
		t.Fatalf("shards = %d, want 9", len(st.Shards))
	}
	if err := auth.RollbackConversion(st, "test abort"); err != nil {
		t.Fatalf("RollbackConversion: %v", err)
	}
	if st.State != metadata.ECConversionRolledBack {
		t.Fatalf("state after rollback = %s, want rolled_back", st.State)
	}
	durable, err := metadata.NewECStore(store).GetStripe("stripe-s2-rollback")
	if err != nil {
		t.Fatalf("GetStripe: %v", err)
	}
	if durable == nil || durable.State != metadata.ECConversionRolledBack {
		t.Fatalf("durable stripe = %+v, want rolled_back", durable)
	}
}

// opsECDatanode builds a datanode V2Store over 3 disks, each hosting a data
// segment store (StreamID 1) and an EC-shard segment store (StreamID 2), with
// the shard stores attached — the same shape runDataNodeV21 wires for the S2
// serving path.
func opsECDatanode(t *testing.T, n int) (*datanode.V2Store, []storage.Store) {
	t.Helper()
	dirs := make([]string, n)
	var dataStores, shardStores []storage.Store
	for i := 0; i < n; i++ {
		d := t.TempDir()
		dirs[i] = d
		ds, err := segment.New(segment.Config{Dir: d, UseMemIndex: true, StreamID: 1})
		if err != nil {
			t.Fatalf("segment.New data %d: %v", i, err)
		}
		ss, err := segment.New(segment.Config{Dir: d, UseMemIndex: true, StreamID: 2})
		if err != nil {
			t.Fatalf("segment.New shard %d: %v", i, err)
		}
		dataStores = append(dataStores, ds)
		shardStores = append(shardStores, ss)
	}
	v := datanode.NewMultiV2Store(dataStores, dirs...)
	if err := v.AttachShardStores(shardStores); err != nil {
		t.Fatalf("AttachShardStores: %v", err)
	}
	t.Cleanup(func() {
		for _, s := range shardStores {
			if c, ok := s.(interface{ Close() error }); ok {
				_ = c.Close()
			}
		}
		for _, s := range dataStores {
			if c, ok := s.(interface{ Close() error }); ok {
				_ = c.Close()
			}
		}
	})
	all := append(append([]storage.Store{}, shardStores...), dataStores...)
	return v, all
}

// TestECConvertS2ServingEndToEnd is the S2 capstone: a datanode V2Store wired
// to the *remote* metad HTTP authority (the same HTTPClient surface the real
// serving path uses) converts a replicated chunk to a completed 6+3 stripe,
// serves it byte-exact, and survives losing three shards via the degraded
// read — with the durable Complete stripe recorded on the metad side.
func TestECConvertS2ServingEndToEnd(t *testing.T) {
	store, bundle := newOpsTestStore(t)
	srv := httptest.NewServer(buildOpsTestMux(t, store, bundle))
	defer srv.Close()

	v, _ := opsECDatanode(t, 3)
	cid := metadata.ChunkID(43001)
	payload := []byte("s2-http-authority-6+3-convert")
	for i := 0; i < 60; i++ {
		payload = append(payload, 0x5A)
	}
	if err := v.Write(cid, payload); err != nil {
		t.Fatalf("write replicated chunk: %v", err)
	}

	// The remote HTTPClient authority implements metadata.ECAuthority
	// structurally, so ECService accepts it directly off the wire.
	auth := metadata.NewHTTPClient(srv.URL, 10*time.Second)
	svc := datanode.NewECService(v, auth)

	st, err := svc.ConvertToEC(context.Background(), cid, 1)
	if err != nil {
		t.Fatalf("ConvertToEC: %v", err)
	}
	if st.State != metadata.ECConversionComplete {
		t.Fatalf("state = %s, want complete", st.State)
	}
	if len(st.Shards) != 9 {
		t.Fatalf("shards = %d, want 9", len(st.Shards))
	}

	// The metad authority durably recorded the completed stripe.
	durable, err := metadata.NewECStore(store).GetStripe(st.StripeID)
	if err != nil {
		t.Fatalf("GetStripe: %v", err)
	}
	if durable == nil || durable.State != metadata.ECConversionComplete {
		t.Fatalf("metad durable stripe = %+v, want complete", durable)
	}

	// Strict aggregate read serves the converted chunk byte- and checksum-exact.
	data, sum, err := v.ReadChunkEC(cid, len(payload))
	if err != nil {
		t.Fatalf("ReadChunkEC: %v", err)
	}
	if string(data) != string(payload) {
		t.Fatalf("serving read mismatch: got %d bytes, want %d", len(data), len(payload))
	}
	if sum == 0 {
		t.Fatal("serving read checksum is zero")
	}

	// Kill three shards → degraded read reconstructs the original byte-exact.
	for _, idx := range []int{0, 3, 6} {
		if err := v.DeleteShard(cid, idx); err != nil {
			t.Fatalf("DeleteShard(%d): %v", idx, err)
		}
	}
	deg, _, _, err := v.ReadChunkECDegraded(cid, len(payload))
	if err != nil {
		t.Fatalf("ReadChunkECDegraded: %v", err)
	}
	if string(deg) != string(payload) {
		t.Fatal("degraded read mismatch")
	}
}

// TestECConvertS2UnderProvisionedFailsCleanly proves an under-provisioned node
// (fewer than §14's nine candidate shard stores) fails the HTTP-driven
// conversion cleanly — no panic — and the chunk stays a readable replica.
func TestECConvertS2UnderProvisionedFailsCleanly(t *testing.T) {
	store, bundle := newOpsTestStore(t)
	srv := httptest.NewServer(buildOpsTestMux(t, store, bundle))
	defer srv.Close()

	v, _ := opsECDatanode(t, 2) // only 2 shard stores → only 6 candidate shards
	cid := metadata.ChunkID(43002)
	payload := []byte("s2-under-provisioned")
	if err := v.Write(cid, payload); err != nil {
		t.Fatalf("write: %v", err)
	}

	auth := metadata.NewHTTPClient(srv.URL, 10*time.Second)
	svc := datanode.NewECService(v, auth)
	if _, err := svc.ConvertToEC(context.Background(), cid, 1); err == nil {
		t.Fatal("ConvertToEC succeeded on an under-provisioned node, want error")
	}

	if got, _, err := v.Read(cid, 0, 0); err != nil || string(got) != string(payload) {
		t.Fatalf("replica unreadable after failed convert: data=%q err=%v", got, err)
	}
}
