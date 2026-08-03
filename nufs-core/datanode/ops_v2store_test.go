package datanode

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/example/dfs/metadata"
)

// newV2OpsServer builds an OpsServer driven by a V2Store (the V2.1 engine)
// with all V1-only subsystems (disk manager, replicator, anti-entropy,
// repair) nil, the configuration runDataNodeV21 would use. It returns the
// server and a dispatch helper that routes a request through the registered
// mux without binding a live port.
func newV2OpsServer(t *testing.T) (*OpsServer, func(method, path string) *httptest.ResponseRecorder) {
	t.Helper()
	v, _ := newTestMultiStore(t, 2)
	// Write two chunks so disks/metrics/verify have real state to report.
	for i := 0; i < 2; i++ {
		if err := v.Write(metadata.ChunkID(100+i), []byte("abcd")); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	s := NewOpsServerWithRepair(Config{NodeID: 7}, v, newMockMetadataService(), nil, nil, nil, nil)
	dispatch := func(method, path string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, nil)
		rec := httptest.NewRecorder()
		s.listener.Handler.ServeHTTP(rec, req)
		return rec
	}
	return s, dispatch
}

// TestOpsServer_V2StoreHealthAndDisks proves the V2.1 engine drives the same
// health/disks operational surface V1 does.
func TestOpsServer_V2StoreHealthAndDisks(t *testing.T) {
	_, dispatch := newV2OpsServer(t)

	// /health reports healthy (no V1 disk manager → UsagePct 0).
	rec := dispatch(http.MethodGet, "/health")
	if rec.Code != http.StatusOK {
		t.Fatalf("/health code=%d, want 200", rec.Code)
	}
	var h HealthStatus
	if err := json.Unmarshal(rec.Body.Bytes(), &h); err != nil {
		t.Fatalf("unmarshal health: %v", err)
	}
	if h.Status != "healthy" {
		t.Fatalf("health status=%q, want healthy", h.Status)
	}

	// /api/v1/disks lists both V2.1 disks with their real payload bytes.
	rec = dispatch(http.MethodGet, "/api/v1/disks")
	if rec.Code != http.StatusOK {
		t.Fatalf("/api/v1/disks code=%d, want 200 (body: %s)", rec.Code, rec.Body.String())
	}
	var d struct {
		Disks []struct {
			Index  int   `json:"index"`
			Chunks int64 `json:"chunks"`
			Bytes  int64 `json:"bytes"`
		} `json:"disks"`
		TotalChunks int64 `json:"total_chunks"`
		TotalBytes  int64 `json:"total_bytes"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &d); err != nil {
		t.Fatalf("unmarshal disks: %v", err)
	}
	if len(d.Disks) != 2 {
		t.Fatalf("disks len=%d, want 2", len(d.Disks))
	}
	if d.TotalChunks != 2 {
		t.Fatalf("total_chunks=%d, want 2", d.TotalChunks)
	}
	if d.TotalBytes != 8 {
		t.Fatalf("total_bytes=%d, want 8", d.TotalBytes)
	}
}

// TestOpsServer_V2StoreMetrics proves handleMetrics no longer reads concrete
// ChunkStore fields (it uses OpsStore.Stats) and does not panic with nil V1
// subsystems.
func TestOpsServer_V2StoreMetrics(t *testing.T) {
	_, dispatch := newV2OpsServer(t)

	rec := dispatch(http.MethodGet, "/api/v1/metrics")
	if rec.Code != http.StatusOK {
		t.Fatalf("/api/v1/metrics code=%d, want 200 (body: %s)", rec.Code, rec.Body.String())
	}
	var m OpsMetrics
	if err := json.Unmarshal(rec.Body.Bytes(), &m); err != nil {
		t.Fatalf("unmarshal metrics: %v", err)
	}
	if m.Cache.ChunkCount != 2 {
		t.Fatalf("metrics chunk_count=%d, want 2", m.Cache.ChunkCount)
	}
	if m.Cache.UsedBytes != 8 {
		t.Fatalf("metrics used_bytes=%d, want 8", m.Cache.UsedBytes)
	}
}

// TestOpsServer_V2StoreVerifyChunk proves the verify endpoint works on the
// V2.1 engine.
func TestOpsServer_V2StoreVerifyChunk(t *testing.T) {
	// handleVerifyChunk cross-references metadata; seed it so chunk 100 is
	// known there too.
	s := NewOpsServerWithRepair(Config{NodeID: 7}, mustV2Store(t), &mockMetadataService{
		chunks: map[metadata.ChunkID]*metadata.ChunkMeta{
			metadata.ChunkID(100): {},
		},
	}, nil, nil, nil, nil)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/chunks/100/verify", nil)
	rec := httptest.NewRecorder()
	s.listener.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("verify code=%d, want 200 (body: %s)", rec.Code, rec.Body.String())
	}
	var v VerifyResult
	if err := json.Unmarshal(rec.Body.Bytes(), &v); err != nil {
		t.Fatalf("unmarshal verify: %v", err)
	}
	if !v.Valid {
		t.Fatalf("verify reported invalid for intact chunk")
	}
}

// mustV2Store builds a V2Store over one disk, writing one chunk (100).
func mustV2Store(t *testing.T) *V2Store {
	t.Helper()
	v, _ := newTestMultiStore(t, 1)
	if err := v.Write(metadata.ChunkID(100), []byte("abcd")); err != nil {
		t.Fatalf("write: %v", err)
	}
	return v
}

// TestOpsServer_V2StoreDiskLifecycleUnsupported proves the disk-lifecycle
// endpoints answer "unsupported" (503) because V2.1 has no disk lifecycle,
// rather than dereferencing a nil capability or panicking.
func TestOpsServer_V2StoreDiskLifecycleUnsupported(t *testing.T) {
	_, dispatch := newV2OpsServer(t)

	for _, tc := range []struct{ method, path string }{
		{http.MethodPost, "/api/v1/disks/adopt?dir=/tmp/x"},
		{http.MethodPost, "/api/v1/disks/retire?dir=/tmp/x"},
		{http.MethodPost, "/api/v1/disks/decommission?dir=/tmp/x"},
		{http.MethodPost, "/api/v1/disks/migrate?dir=/tmp/x"},
		{http.MethodPost, "/api/v1/disks/drain"},
	} {
		rec := dispatch(tc.method, tc.path)
		if rec.Code != http.StatusNotImplemented {
			t.Fatalf("%s code=%d, want 501 (body: %s)", tc.path, rec.Code, rec.Body.String())
		}
		if !strings.Contains(rec.Body.String(), "unsupported by this engine") {
			t.Fatalf("%s body=%q, want 'unsupported by this engine'", tc.path, rec.Body.String())
		}
	}
}
