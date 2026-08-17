package main

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/dshmyz/nufs/nufs-core/metadata"
)

func TestLeaderRedirectTargetPreservesEscapedPathAndQuery(t *testing.T) {
	requestURL := &url.URL{
		Path:     "/api/v1/buckets/./quota",
		RawPath:  "/api/v1/buckets/%2E/quota",
		RawQuery: "bucket_path=dot&source=admin",
	}

	got := leaderRedirectTarget("http://leader:8091", requestURL)
	want := "http://leader:8091/api/v1/buckets/%2E/quota?bucket_path=dot&source=admin"
	if got != want {
		t.Fatalf("leader redirect target = %q, want %q", got, want)
	}
}

// TestFollowerDataPlaneReadsGateOnLeader proves the follower-read
// linearizability fix: on a Raft follower, data-plane read endpoints must
// NOT serve from the possibly-lagging local FSM. They hit the leader gate
// instead — 503 when no leader is known (this test's construction), 307
// when one is. Before the fix these routes read the local DB directly and
// returned 200, violating read-after-write consistency after a failover.
// Control-plane reads (cluster/status, metrics) stay ungated by design.
func TestFollowerDataPlaneReadsGateOnLeader(t *testing.T) {
	store, bundle := newOpsTestStore(t)

	// Follower construction: a RaftNode whose IsLeader() is false and whose
	// internal raft handle is nil, so LeaderOpsAddr() reports "leader
	// unknown" (the empty-string branch of requireLeaderRedirect).
	node := &metadata.RaftNode{}
	setUnexportedField(t, node, "publicLeaderHook", func() bool { return false })
	store.SetRaftNode(node)
	t.Cleanup(func() { setUnexportedField(t, store, "raft", (*metadata.RaftNode)(nil)) })

	mux := http.NewServeMux()
	registerOpsHandlers(mux, store, store, bundle, "")

	gatedReads := []struct {
		name string
		path string
	}{
		{"lookup", "/api/v1/namespace/lookup?bucket=b&path=dir"},
		{"readdir", "/api/v1/namespace/readdir?bucket=b"},
		{"readlink", "/api/v1/namespace/readlink?bucket=b&path=ln"},
		{"inodes", "/api/v1/inodes/123"},
		{"extents", "/api/v1/extents/123"},
	}
	for _, tc := range gatedReads {
		req := httptest.NewRequest(http.MethodGet, tc.path, nil)
		rr := httptest.NewRecorder()
		mux.ServeHTTP(rr, req)
		if rr.Code != http.StatusServiceUnavailable {
			t.Fatalf("%s on follower = %d, want 503 (leader unknown) — local read would violate read-after-write; body=%s",
				tc.name, rr.Code, rr.Body.String())
		}
	}

	// Control-plane reads remain servable on followers by design.
	ungated := []struct {
		name string
		path string
	}{
		{"cluster/status", "/api/v1/cluster/status"},
		{"metrics", "/api/v1/metrics"},
	}
	for _, tc := range ungated {
		req := httptest.NewRequest(http.MethodGet, tc.path, nil)
		rr := httptest.NewRecorder()
		mux.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("%s on follower = %d, want 200 (control-plane read stays local)", tc.name, rr.Code)
		}
	}
}
