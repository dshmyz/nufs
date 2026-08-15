package datanode

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// newTestECService builds an ECService wired exactly as runDataNodeV21 wires
// the S1 serving path: a V2Store with attached shard stores plus a local
// in-process Pebble-backed ECStore as the conversion authority (the dev/
// single-node stand-in for the production authority-seam).
func newTestECService(t *testing.T, shardStoreCount int) (*V2Store, *ECService, *metadata.PebbleStore) {
	t.Helper()
	v, _ := newTestShardMultiStore(t, shardStoreCount)
	ms, err := metadata.NewPebbleStore(metadata.PebbleStoreConfig{
		Dir: t.TempDir(), UseInMemory: true, UseBucketStats: false,
	})
	if err != nil {
		t.Fatalf("NewPebbleStore: %v", err)
	}
	return v, NewECService(v, metadata.NewECStore(ms)), ms
}

// TestECService_ServingPath_ConvertThenRead is the S1 serving-path core: a
// replicated chunk written through the store is converted to a completed 6+3
// stripe via the ops control-plane endpoint (the same HTTP surface
// runDataNodeV21 serves), the authority records it durable/complete, and the
// store then serves the chunk from the shards — strict aggregate read
// byte-exact and checksum-exact, and a degraded read after losing three shards
// still reconstructs the original byte-exact.
func TestECService_ServingPath_ConvertThenRead(t *testing.T) {
	v, svc, _ := newTestECService(t, 3)

	cid := metadata.ChunkID(21001)
	payload := []byte("serving-path-replicated-chunk-6+3-convert")
	for i := 0; i < 40; i++ {
		payload = append(payload, 0x41)
	}
	if err := v.Write(cid, payload); err != nil {
		t.Fatalf("write replicated chunk: %v", err)
	}

	// Wire the same ops surface runDataNodeV21 serves, with the EC driver
	// attached, and drive the conversion through the real endpoint.
	s := NewOpsServerWithRepair(Config{NodeID: 7}, v, newMockMetadataService(), nil)
	s.SetECService(svc)
	dispatch := func(method, path string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, nil)
		rec := httptest.NewRecorder()
		s.listener.Handler.ServeHTTP(rec, req)
		return rec
	}

	rec := dispatch(http.MethodPost, "/api/v1/ec/convert?chunk_id=21001")
	if rec.Code != http.StatusOK {
		t.Fatalf("convert code=%d body=%s, want 200", rec.Code, rec.Body.String())
	}
	var resp struct {
		StripeID        string `json:"stripe_id"`
		State           string `json:"state"`
		OriginalChecksum uint32 `json:"original_checksum"`
		Shards          int    `json:"shards"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal convert resp: %v", err)
	}
	if resp.State != "complete" {
		t.Fatalf("state=%q, want complete", resp.State)
	}
	if resp.Shards != 9 {
		t.Fatalf("shards=%d, want 9", resp.Shards)
	}
	if resp.OriginalChecksum == 0 {
		t.Fatal("original checksum not recorded")
	}

	// The store now serves the chunk from the 6+3 shards: strict aggregate
	// read reconstructs the original byte- and checksum-exact.
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

	// Kill three shards (the §14 worst tolerated loss) → the degraded read
	// reconstructs the original byte-exact from the six survivors.
	for _, idx := range []int{0, 3, 8} {
		if err := v.DeleteShard(cid, idx); err != nil {
			t.Fatalf("DeleteShard(%d): %v", idx, err)
		}
	}
	deg, dsum, missing, err := v.ReadChunkECDegraded(cid, len(payload))
	if err != nil {
		t.Fatalf("ReadChunkECDegraded: %v", err)
	}
	if string(deg) != string(payload) {
		t.Fatalf("degraded read mismatch")
	}
	if len(missing) != 3 {
		t.Fatalf("missing=%v, want 3", missing)
	}
	if dsum != resp.OriginalChecksum {
		t.Fatalf("degraded checksum %#x, want recorded %#x", dsum, resp.OriginalChecksum)
	}
}

// TestECService_ServingPath_UnderProvisionedFailsCleanly proves an
// under-provisioned node (fewer than §14's nine candidate shard stores) fails
// the conversion cleanly through the endpoint — no panic, chunk stays a
// readable replica.
func TestECService_ServingPath_UnderProvisionedFailsCleanly(t *testing.T) {
	v, svc, _ := newTestECService(t, 2) // only 2 shard stores → only 6 candidate shards
	cid := metadata.ChunkID(21002)
	payload := []byte("under-provisioned-ec")
	if err := v.Write(cid, payload); err != nil {
		t.Fatalf("write: %v", err)
	}

	s := NewOpsServerWithRepair(Config{NodeID: 7}, v, newMockMetadataService(), nil)
	s.SetECService(svc)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/ec/convert?chunk_id=21002", nil)
	rec := httptest.NewRecorder()
	s.listener.Handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("convert code=%d, want 500", rec.Code)
	}

	// The chunk was never switched away from replicas: it reads back intact.
	if got, _, err := v.Read(cid, 0, 0); err != nil || string(got) != string(payload) {
		t.Fatalf("replica unreadable after failed convert: data=%q err=%v", got, err)
	}
}

// TestECService_ConvertToEC_Direct verifies the driver method (not just the
// HTTP endpoint) converts and the authority records the completed stripe;
// the returned stripe can be lifted into a switchable EC layout via
// BuildECGroup (§14 atomic flip).
func TestECService_ConvertToEC_Direct(t *testing.T) {
	v, svc, pb := newTestECService(t, 3)
	defer pb.Close()

	cid := metadata.ChunkID(21003)
	payload := []byte("direct-convert-to-ec")
	for i := 0; i < 200; i++ {
		payload = append(payload, byte(i))
	}
	if err := v.Write(cid, payload); err != nil {
		t.Fatalf("write: %v", err)
	}

	st, err := svc.ConvertToEC(context.Background(), cid, 1)
	if err != nil {
		t.Fatalf("ConvertToEC: %v", err)
	}
	if st.State != metadata.ECConversionComplete {
		t.Fatalf("state=%s, want complete", st.State)
	}
	if len(st.Shards) != 9 {
		t.Fatalf("shards=%d, want 9", len(st.Shards))
	}

	// The completed stripe builds an atomically-switchable EC layout.
	cm := BuildECGroup(st, int32(len(payload)), metadata.TierCold)
	if cm.ECGroup == nil || cm.ECGroup.DataShards != 6 || cm.ECGroup.ParityShards != 3 {
		t.Fatalf("ECGroup=%+v, want 6+3", cm.ECGroup)
	}
	if len(cm.Replicas) != 9 {
		t.Fatalf("replicas=%d, want 9", len(cm.Replicas))
	}

	// Strict aggregate read serves the converted chunk byte-exact.
	data, _, err := v.ReadChunkEC(cid, len(payload))
	if err != nil {
		t.Fatalf("ReadChunkEC: %v", err)
	}
	if string(data) != string(payload) {
		t.Fatalf("serving read mismatch")
	}
}
