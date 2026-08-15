package datanode

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/dshmyz/nufs/nufs-core/datanode/storage"
	"github.com/dshmyz/nufs/nufs-core/datanode/storage/segment"
	"github.com/dshmyz/nufs/nufs-core/metadata"
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
	s := NewOpsServerWithRepair(Config{NodeID: 7}, v, newMockMetadataService(), nil)
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
	}, nil)
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

// TestOpsServer_V2StoreDrain proves the V2.1 engine exposes the drain surface
// (DrainOps parity): POST /api/v1/disks/drain acquires the QuiesceWrites
// barrier and reports "drained" instead of the previous 501 degrade.
func TestOpsServer_V2StoreDrain(t *testing.T) {
	_, dispatch := newV2OpsServer(t)

	rec := dispatch(http.MethodPost, "/api/v1/disks/drain")
	if rec.Code != http.StatusOK {
		t.Fatalf("/api/v1/disks/drain code=%d, want 200 (body: %s)", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "drained") {
		t.Fatalf("/api/v1/disks/drain body=%q, want 'drained'", rec.Body.String())
	}

	// The unsupported method forms still 405.
	rec = dispatch(http.MethodGet, "/api/v1/disks/drain")
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET /api/v1/disks/drain code=%d, want 405", rec.Code)
	}
}

func TestOpsServer_V2StoreDiskLifecycleAdoptRetire(t *testing.T) {
	s, dispatch := newV2OpsServer(t)
	// The datanode main wires this factory; a configured store advertises and
	// serves the full disk-lifecycle surface (task #183).
	s.store.(*V2Store).SetDiskFactory(func(dir string) (storage.Store, storage.Store, storage.Store, error) {
		data, err := segment.New(segment.Config{Dir: dir, UseMemIndex: true, StreamID: 1})
		if err != nil {
			return nil, nil, nil, err
		}
		shard, err := segment.New(segment.Config{Dir: dir, UseMemIndex: true, StreamID: 2})
		if err != nil {
			_ = data.Close()
			return nil, nil, nil, err
		}
		small, err := segment.NewSmallStore(segment.Config{Dir: dir, UseMemIndex: true})
		if err != nil {
			_ = data.Close()
			_ = shard.Close()
			return nil, nil, nil, err
		}
		return data, shard, small, nil
	})

	dir := t.TempDir()
	rec := dispatch(http.MethodPost, "/api/v1/disks/adopt?dir="+dir)
	if rec.Code != http.StatusOK {
		t.Fatalf("adopt code=%d, want 200 (body: %s)", rec.Code, rec.Body.String())
	}
	var res struct {
		Dir   string `json:"dir"`
		Index int    `json:"index"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
		t.Fatalf("unmarshal adopt: %v", err)
	}
	if res.Index != 2 {
		t.Fatalf("adopt index=%d, want 2", res.Index)
	}

	// The adopted disk now appears in the disk list.
	rec = dispatch(http.MethodGet, "/api/v1/disks")
	var d struct {
		Disks []struct {
			Index int    `json:"index"`
			Dir   string `json:"dir"`
		} `json:"disks"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &d)
	found := false
	for _, disk := range d.Disks {
		if disk.Index == 2 && disk.Dir == dir {
			found = true
		}
	}
	if !found {
		t.Fatalf("adopted disk (index 2, dir %s) missing from /api/v1/disks: %+v", dir, d.Disks)
	}

	// Retire it back — reversible, proving lifecycle round-trips on V2.1.
	rec = dispatch(http.MethodPost, "/api/v1/disks/retire?dir="+dir)
	if rec.Code != http.StatusOK {
		t.Fatalf("retire code=%d, want 200 (body: %s)", rec.Code, rec.Body.String())
	}

	// Unknown dir → 404 for the dir-based lifecycle verbs.
	for _, path := range []string{
		"/api/v1/disks/retire?dir=/nope",
		"/api/v1/disks/decommission?dir=/nope",
		"/api/v1/disks/migrate?dir=/nope",
	} {
		if c := dispatch(http.MethodPost, path).Code; c != http.StatusNotFound {
			t.Fatalf("%s code=%d, want 404", path, c)
		}
	}
}

func TestOpsServer_V2StoreDiskLifecycleWithoutFactoryDegrades(t *testing.T) {
	_, dispatch := newV2OpsServer(t)
	// Without SetDiskFactory the V2Store cannot build an engine backend for an
	// arbitrary new dir, so adopt degrades to an unsupported error (TASK #183
	// keeps the degrade path; production main always wires the factory).
	rec := dispatch(http.MethodPost, "/api/v1/disks/adopt?dir=/tmp/x")
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("adopt-no-factory code=%d, want 500 (body: %s)", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "not configured") {
		t.Fatalf("adopt-no-factory body=%q, want 'not configured'", rec.Body.String())
	}
}

// TestDiskIndexByDirRegression is the pure-function regression for the
// retire + re-adopt same-dir bug: the ops/mgmt commands that address disks by
// dir must resolve to the HEALTHY re-adopted entry, not the preserved failed
// slot (which would report "already retired" / verify the closed backend).
func TestDiskIndexByDirRegression(t *testing.T) {
	// Two entries claim the same dir: preserved failed slot at lower index,
	// healthy re-adopted slot at higher index (the post-retire+re-adopt shape).
	infos := []DiskInfo{
		{Index: 2, Dir: "/data", Failed: true},
		{Index: 3, Dir: "/data", Failed: false},
		{Index: 0, Dir: "/other", Failed: false},
	}
	if got := DiskIndexByDir(infos, "/data"); got != 3 {
		t.Fatalf("DiskIndexByDir(same-dir) = %d, want 3 (healthy re-adopted, not failed slot 2)", got)
	}
	// Single healthy entry → itself.
	if got := DiskIndexByDir(infos, "/other"); got != 0 {
		t.Fatalf("DiskIndexByDir(healthy-only) = %d, want 0", got)
	}
	// Only failed entries for the dir → falls back to the failed one, preserving
	// the "genuinely-only-failed" behavior (use retire / unreadable).
	if got := DiskIndexByDir(infos, "/gone"); got != -1 {
		t.Fatalf("DiskIndexByDir(unknown) = %d, want -1", got)
	}
	onlyFailed := []DiskInfo{{Index: 5, Dir: "/dead", Failed: true}}
	if got := DiskIndexByDir(onlyFailed, "/dead"); got != 5 {
		t.Fatalf("DiskIndexByDir(only-failed) = %d, want 5", got)
	}
}

// TestOpsServer_SameDirReAdoptPrefersHealthy commands a retire + re-adopt of
// the same dir through the REAL ops HTTP handlers and asserts migrate /
// decommission target the healthy re-adopted disk — the end-to-end regression
// for the bug, exercising DiskIndexByDir inside the ops dispatcher rather than
// the raw store.
func TestOpsServer_SameDirReAdoptPrefersHealthy(t *testing.T) {
	s, dispatch := newV2OpsServer(t)
	s.store.(*V2Store).SetDiskFactory(func(dir string) (storage.Store, storage.Store, storage.Store, error) {
		data, err := segment.New(segment.Config{Dir: dir, UseMemIndex: true, StreamID: 1})
		if err != nil {
			return nil, nil, nil, err
		}
		shard, err := segment.New(segment.Config{Dir: dir, UseMemIndex: true, StreamID: 2})
		if err != nil {
			_ = data.Close()
			return nil, nil, nil, err
		}
		small, err := segment.NewSmallStore(segment.Config{Dir: dir, UseMemIndex: true})
		if err != nil {
			_ = data.Close()
			_ = shard.Close()
			return nil, nil, nil, err
		}
		return data, shard, small, nil
	})

	dir := t.TempDir()
	// Adopt → retire → re-adopt the SAME dir. This leaves two DiskInfo entries
	// for dir: the preserved failed slot (index 2) and the healthy re-adopted
	// one (index 3).
	adopt := func() int {
		rec := dispatch(http.MethodPost, "/api/v1/disks/adopt?dir="+dir)
		if rec.Code != http.StatusOK {
			t.Fatalf("adopt code=%d body=%s", rec.Code, rec.Body.String())
		}
		var r struct {
			Index int `json:"index"`
		}
		_ = json.Unmarshal(rec.Body.Bytes(), &r)
		return r.Index
	}
	a1 := adopt()
	if rec := dispatch(http.MethodPost, "/api/v1/disks/retire?dir="+dir); rec.Code != http.StatusOK {
		t.Fatalf("retire code=%d body=%s", rec.Code, rec.Body.String())
	}
	a2 := adopt()
	if a2 <= a1 {
		t.Fatalf("re-adopt must append a fresh slot (adopt1=%d adopt2=%d)", a1, a2)
	}

	// Sanity: exactly two entries claim dir, first is failed, second healthy.
	infos := s.store.DiskInfos()
	var dirInfos []DiskInfo
	for _, d := range infos {
		if d.Dir == dir {
			dirInfos = append(dirInfos, d)
		}
	}
	if len(dirInfos) != 2 || !dirInfos[0].Failed || dirInfos[1].Failed {
		t.Fatalf("expected [failed, healthy] same-dir entries, got %+v", dirInfos)
	}

	// migrate?dir= MUST act on the healthy re-adopted disk (200, ok), not the
	// failed preserved slot. Before the fix it was a coin-flip on first-match.
	if rec := dispatch(http.MethodPost, "/api/v1/disks/migrate?dir="+dir); rec.Code != http.StatusOK {
		t.Fatalf("migrate same-dir post-re-adopt code=%d body=%s (want 200 on healthy slot)", rec.Code, rec.Body.String())
	}

	// decommission?dir= must also succeed (not "already retired") and retire the
	// healthy re-adopted disk, leaving the same-dir case fully drained.
	if rec := dispatch(http.MethodPost, "/api/v1/disks/decommission?dir="+dir); rec.Code != http.StatusOK {
		t.Fatalf("decommission same-dir post-re-adopt code=%d body=%s (want 200, not 'already retired')", rec.Code, rec.Body.String())
	}
}
