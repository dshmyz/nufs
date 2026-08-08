package metadata

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

var _ MetadataService = (*HTTPClient)(nil)

func TestHTTPClientSendsAuthToken(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer secret" {
			t.Fatalf("expected Authorization header, got %q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[]`))
	}))
	defer srv.Close()

	c := NewHTTPClient(srv.URL, time.Second)
	c.SetAuthToken("secret")
	if _, err := c.ListBuckets(context.Background()); err != nil {
		t.Fatalf("ListBuckets: %v", err)
	}
}

func TestHTTPClientSendsAuthTokenOnRedirect(t *testing.T) {
	leader := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer secret" {
			t.Fatalf("expected redirected Authorization header, got %q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[]`))
	}))
	defer leader.Close()

	follower := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, leader.URL+r.URL.Path, http.StatusTemporaryRedirect)
	}))
	defer follower.Close()

	c := NewHTTPClient(follower.URL, time.Second)
	c.SetAuthToken("secret")
	if _, err := c.ListBuckets(context.Background()); err != nil {
		t.Fatalf("ListBuckets via redirect: %v", err)
	}
}

// TestHTTPClientFollowsMultiHopRedirect verifies the client chases a leader
// through more than one 307 redirect (follower -> second follower -> leader),
// re-sending the original method, body and auth headers on each hop.
func TestHTTPClientFollowsMultiHopRedirect(t *testing.T) {
	leader := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer secret" {
			t.Fatalf("expected redirected Authorization header, got %q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"name":"b1"}]`))
	}))
	defer leader.Close()

	hop2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, leader.URL+r.URL.Path, http.StatusTemporaryRedirect)
	}))
	defer hop2.Close()

	follower := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// First hop points at another follower rather than the leader directly.
		http.Redirect(w, r, hop2.URL+r.URL.Path, http.StatusTemporaryRedirect)
	}))
	defer follower.Close()

	c := NewHTTPClient(follower.URL, time.Second)
	c.SetAuthToken("secret")
	buckets, err := c.ListBuckets(context.Background())
	if err != nil {
		t.Fatalf("ListBuckets via multi-hop redirect: %v", err)
	}
	if len(buckets) != 1 || buckets[0].Name != "b1" {
		t.Fatalf("unexpected buckets: %+v", buckets)
	}
}

// TestHTTPClientRetriesThroughLeaderTransition verifies the client keeps
// retrying transient 5xx responses (a follower answering "no leader available"
// while the Raft cluster re-elects) for a bounded transition budget instead of
// surfacing an immediate 503 to the caller.
func TestHTTPClientRetriesThroughLeaderTransition(t *testing.T) {
	var fails atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if fails.Add(1) <= 3 {
			http.Error(w, "no leader available", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"name":"b1"}]`))
	}))
	defer srv.Close()

	c := NewHTTPClient(srv.URL, 2*time.Second)
	c.SetRetryConfig(3, time.Millisecond)
	buckets, err := c.ListBuckets(context.Background())
	if err != nil {
		t.Fatalf("ListBuckets through 503 transition: %v", err)
	}
	if len(buckets) != 1 {
		t.Fatalf("unexpected buckets: %+v", buckets)
	}
}

func TestHTTPClientXAttrRoundTripOverJSON(t *testing.T) {
	var stored []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPut && r.URL.Path == "/api/v1/inodes/42/xattrs/user.mime":
			var req struct {
				Value []byte `json:"value"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode set xattr request: %v", err)
			}
			stored = append([]byte(nil), req.Value...)
			_, _ = w.Write([]byte(`{"status":"updated"}`))
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/inodes/42/xattrs/user.mime":
			_ = json.NewEncoder(w).Encode(map[string][]byte{"value": stored})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/inodes/42/xattrs":
			_ = json.NewEncoder(w).Encode(map[string][]byte{"user.mime": stored})
		case r.Method == http.MethodDelete && r.URL.Path == "/api/v1/inodes/42/xattrs/user.mime":
			stored = nil
			_, _ = w.Write([]byte(`{"status":"removed"}`))
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer srv.Close()

	c := NewHTTPClient(srv.URL, time.Second)
	ctx := context.Background()
	value := []byte{0, 1, 2, 255, 'n', 'u'}
	if err := c.SetXAttr(ctx, 42, "user.mime", value); err != nil {
		t.Fatalf("SetXAttr: %v", err)
	}
	got, err := c.GetXAttr(ctx, 42, "user.mime")
	if err != nil {
		t.Fatalf("GetXAttr: %v", err)
	}
	if string(got) != string(value) {
		t.Fatalf("xattr value mismatch: got %v want %v", got, value)
	}
	listed, err := c.ListXAttr(ctx, 42)
	if err != nil {
		t.Fatalf("ListXAttr: %v", err)
	}
	if string(listed["user.mime"]) != string(value) {
		t.Fatalf("listed xattr mismatch: got %v want %v", listed["user.mime"], value)
	}
	if err := c.RemoveXAttr(ctx, 42, "user.mime"); err != nil {
		t.Fatalf("RemoveXAttr: %v", err)
	}
}

func TestHTTPClientGetXAttrMapsNotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":"missing","code":"xattr_not_found"}`))
	}))
	defer srv.Close()

	c := NewHTTPClient(srv.URL, time.Second)
	_, err := c.GetXAttr(context.Background(), 42, "user.missing")
	if !errors.Is(err, ErrXAttrNotFound) {
		t.Fatalf("expected ErrXAttrNotFound, got %v", err)
	}
}

func TestHTTPClientBucketQuotaEscapesPathAndUsesPublicJSONFields(t *testing.T) {
	const bucket = "a/b c"
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPut && r.URL.EscapedPath() == "/api/v1/buckets/a%2Fb%20c/quota":
			var quota map[string]json.RawMessage
			if err := json.NewDecoder(r.Body).Decode(&quota); err != nil {
				t.Fatalf("decode quota request: %v", err)
			}
			if _, ok := quota["max_bytes"]; !ok {
				t.Fatalf("quota request missing max_bytes: %#v", quota)
			}
			if _, ok := quota["max_objects"]; !ok {
				t.Fatalf("quota request missing max_objects: %#v", quota)
			}
			if _, ok := quota["max_chunk_count"]; ok {
				t.Fatalf("quota request leaked max_chunk_count: %#v", quota)
			}
			_, _ = w.Write([]byte(`{"status":"updated"}`))
		case r.Method == http.MethodGet && r.URL.EscapedPath() == "/api/v1/buckets/a%2Fb%20c/quota":
			_, _ = w.Write([]byte(`{"bucket":"a/b c","quota":{"max_bytes":1000,"max_objects":10},"usage":{"name":"a/b c","used_bytes":7,"objects":2},"ratios":{"bytes":0.007,"objects":0.2}}`))
		case r.Method == http.MethodPost && r.URL.EscapedPath() == "/api/v1/buckets/a%2Fb%20c/quota/check":
			var req map[string]int64
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode quota check request: %v", err)
			}
			if req["additional_bytes"] != 11 || req["additional_objects"] != 1 {
				t.Fatalf("quota check request = %#v", req)
			}
			_, _ = w.Write([]byte(`{"status":"allowed"}`))
		case r.Method == http.MethodDelete && r.URL.EscapedPath() == "/api/v1/buckets/a%2Fb%20c/quota":
			_, _ = w.Write([]byte(`{"status":"deleted"}`))
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.EscapedPath())
		}
	}))
	defer srv.Close()

	c := NewHTTPClient(srv.URL, time.Second)
	ctx := context.Background()
	if err := c.SetBucketQuota(ctx, bucket, &BucketQuota{MaxSizeBytes: 1000, MaxObjects: 10, MaxChunkCount: 99}); err != nil {
		t.Fatalf("SetBucketQuota: %v", err)
	}
	status, err := c.GetBucketQuotaStatus(ctx, bucket)
	if err != nil {
		t.Fatalf("GetBucketQuotaStatus: %v", err)
	}
	if status.Quota == nil || status.Quota.MaxSizeBytes != 1000 {
		t.Fatalf("quota status = %+v", status)
	}
	quota, err := c.GetBucketQuota(ctx, bucket)
	if err != nil {
		t.Fatalf("GetBucketQuota: %v", err)
	}
	if quota == nil || quota.MaxObjects != 10 || quota.MaxChunkCount != 0 {
		t.Fatalf("quota = %+v", quota)
	}
	usage, err := c.GetBucketUsage(ctx, bucket)
	if err != nil {
		t.Fatalf("GetBucketUsage: %v", err)
	}
	if usage.Name != bucket || usage.UsedBytes != 7 || usage.Objects != 2 {
		t.Fatalf("usage = %+v", usage)
	}
	if err := c.CheckBucketQuota(ctx, bucket, 11, 1); err != nil {
		t.Fatalf("CheckBucketQuota: %v", err)
	}
	if err := c.DeleteBucketQuota(ctx, bucket); err != nil {
		t.Fatalf("DeleteBucketQuota: %v", err)
	}
	if calls != 6 {
		t.Fatalf("quota client calls = %d, want 6", calls)
	}
}

func TestHTTPClientBucketQuotaDistinguishesDotFromLiteralEncodedDot(t *testing.T) {
	var paths []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.EscapedPath())
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"bucket":"","quota":null,"usage":{},"ratios":{}}`))
	}))
	defer srv.Close()

	client := NewHTTPClient(srv.URL, time.Second)
	for _, bucket := range []string{".", "%2E"} {
		if _, err := client.GetBucketQuotaStatus(context.Background(), bucket); err != nil {
			t.Fatalf("GetBucketQuotaStatus(%q): %v", bucket, err)
		}
	}

	want := []string{
		"/api/v1/buckets/%2E/quota",
		"/api/v1/buckets/%252E/quota",
	}
	if fmt.Sprint(paths) != fmt.Sprint(want) {
		t.Fatalf("escaped paths = %v, want %v", paths, want)
	}
}

func TestHTTPClientCheckBucketQuotaMapsOnlyQuotaExceeded(t *testing.T) {
	for _, tc := range []struct {
		name       string
		statusCode int
		body       string
		wantQuota  bool
	}{
		{name: "exceeded", statusCode: http.StatusConflict, body: `{"error":"over limit","code":"quota_exceeded"}`, wantQuota: true},
		{name: "backend error", statusCode: http.StatusInternalServerError, body: `{"error":"pebble unavailable","code":"internal_error"}`},
		{name: "other conflict", statusCode: http.StatusConflict, body: `{"error":"conflict","code":"other_conflict"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.statusCode)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer srv.Close()

			client := NewHTTPClient(srv.URL, time.Second)
			err := client.CheckBucketQuota(context.Background(), "photos", 1, 1)
			if err == nil {
				t.Fatal("CheckBucketQuota returned nil")
			}
			if got := errors.Is(err, ErrQuotaExceeded); got != tc.wantQuota {
				t.Fatalf("errors.Is(ErrQuotaExceeded) = %v, want %v: %v", got, tc.wantQuota, err)
			}
		})
	}
}

func TestHTTPClientBucketQuotaMethodsRejectNon2xxResponses(t *testing.T) {
	for _, status := range []int{http.StatusBadRequest, http.StatusMultipleChoices} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_, _ = w.Write([]byte(`{"error":"invalid quota"}`))
		}))

		c := NewHTTPClient(srv.URL, time.Second)
		ctx := context.Background()
		for name, call := range map[string]func() error{
			"get status": func() error {
				_, err := c.GetBucketQuotaStatus(ctx, "photos")
				return err
			},
			"set": func() error {
				return c.SetBucketQuota(ctx, "photos", &BucketQuota{})
			},
			"delete": func() error {
				return c.DeleteBucketQuota(ctx, "photos")
			},
		} {
			if err := call(); err == nil {
				t.Fatalf("%s quota request accepted status %d", name, status)
			}
		}
		srv.Close()
	}
}

func TestHTTPClientQuotaBackendErrorRetainsServerDetails(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = fmt.Fprint(w, `{"error":"quota backend unavailable"}`)
	}))
	defer srv.Close()

	client := NewHTTPClient(srv.URL, time.Second)
	err := client.CheckBucketQuota(context.Background(), "photos", 1, 0)
	if err == nil || errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("backend error = %v", err)
	}
}

func TestHTTPClientWriteAttemptCleanupPlanRoundTrip(t *testing.T) {
	var stored ObjectWriteAttempt
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodPut:
			if err := json.NewDecoder(r.Body).Decode(&stored); err != nil {
				t.Fatalf("decode write attempt: %v", err)
			}
			_, _ = w.Write([]byte(`{"status":"updated"}`))
		case http.MethodGet:
			if err := json.NewEncoder(w).Encode(&stored); err != nil {
				t.Fatalf("encode write attempt: %v", err)
			}
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer srv.Close()

	client := NewHTTPClient(srv.URL, time.Second)
	ctx := context.Background()
	want := &ObjectWriteAttempt{
		ID:               "cleanup/a",
		Bucket:           "photos",
		Key:              "a.txt",
		InodeID:          42,
		InodeCTime:       12345,
		RecoveryIntent:   WriteAttemptRecoveryCleanup,
		CleanupParent:    2,
		CleanupNewObject: true,
		RollbackInode: &InodeMeta{
			ID:       42,
			CTime:    12345,
			Size:     3,
			ChunkMap: []ChunkRef{{ID: 9, Offset: 0, Length: 3, Version: 1}},
		},
		Chunks: []ChunkRef{{ID: 10, Offset: 0, Length: 7, Version: 1}},
		State:  WriteAttemptRecoveryNeeded,
	}
	if err := client.PutWriteAttempt(ctx, want); err != nil {
		t.Fatalf("PutWriteAttempt: %v", err)
	}
	got, err := client.GetWriteAttempt(ctx, want.ID)
	if err != nil {
		t.Fatalf("GetWriteAttempt: %v", err)
	}
	if got.RecoveryIntent != want.RecoveryIntent || got.InodeCTime != want.InodeCTime ||
		got.CleanupParent != want.CleanupParent || !got.CleanupNewObject {
		t.Fatalf("cleanup plan identity did not round trip: %+v", got)
	}
	if got.RollbackInode == nil || got.RollbackInode.ID != want.RollbackInode.ID ||
		got.RollbackInode.CTime != want.RollbackInode.CTime ||
		len(got.RollbackInode.ChunkMap) != 1 || got.RollbackInode.ChunkMap[0].ID != 9 {
		t.Fatalf("rollback inode did not round trip: %+v", got.RollbackInode)
	}
	if len(got.Chunks) != 1 || got.Chunks[0].ID != 10 {
		t.Fatalf("cleanup chunks did not round trip: %+v", got.Chunks)
	}
}

func TestHTTPClientDoesNotReplayAllocationAfterServerOrTransportAmbiguity(t *testing.T) {
	for _, tc := range []struct {
		name       string
		handler    http.HandlerFunc
		wantCalls  int32
		wantErrSub string
	}{
		{
			name: "allocation unknown response",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusConflict)
				_, _ = w.Write([]byte(`{"code":"allocation_outcome_unknown","error":"outcome unknown"}`))
			},
			wantCalls:  1,
			wantErrSub: "allocation_outcome_unknown",
		},
		{
			name: "server error after possible allocation submit",
			handler: func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, "ambiguous", http.StatusInternalServerError)
			},
			wantCalls:  1,
			wantErrSub: "allocation outcome unknown",
		},
		{
			name: "transport EOF after possible allocation submit",
			handler: func(w http.ResponseWriter, r *http.Request) {
				hj, ok := w.(http.Hijacker)
				if !ok {
					t.Fatalf("response writer is not hijackable")
				}
				conn, _, err := hj.Hijack()
				if err != nil {
					t.Fatalf("Hijack: %v", err)
				}
				_ = conn.Close()
			},
			wantCalls:  1,
			wantErrSub: "allocation outcome unknown",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.Method != http.MethodPost || (r.URL.Path != "/api/v1/chunks" && r.URL.Path != "/api/v1/chunks/batch") {
					t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
				}
				tc.handler(w, r)
			}))
			defer srv.Close()

			c := NewHTTPClient(srv.URL, time.Second)
			c.SetRetryConfig(3, time.Millisecond)
			_, err := c.AllocateChunk(context.Background(), 7, 0, PlacementPolicy{ReplicationFactor: 1})
			if err == nil || !strings.Contains(err.Error(), tc.wantErrSub) {
				t.Fatalf("AllocateChunk error = %v, want %q", err, tc.wantErrSub)
			}
			if got := calls.Load(); got != tc.wantCalls {
				t.Fatalf("server calls = %d, want %d", got, tc.wantCalls)
			}
		})
	}
}

// TestHTTPClientCreateNodeRoundTrip verifies HTTPClient.CreateNode POSTs the
// ftype/mode/rdev to /api/v1/namespace/create-node and decodes the returned
// InodeMeta (type + rdev preserved), mirroring the createfile client contract.
func TestHTTPClientCreateNodeRoundTrip(t *testing.T) {
	var gotPath string
	var gotBody struct {
		Parent InodeID `json:"parent"`
		Name   string  `json:"name"`
		Type   FileType `json:"type"`
		Mode   uint32  `json:"mode"`
		Rdev   uint32  `json:"rdev"`
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/v1/namespace/create-node" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		gotPath = r.URL.Path
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Fatalf("decode create-node request: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(InodeMeta{
			ID: 99, Type: FileCharDevice, Mode: 0666, Rdev: 0x0102, NLink: 1,
		})
	}))
	defer srv.Close()

	c := NewHTTPClient(srv.URL, time.Second)
	ctx := context.Background()
	meta, err := c.CreateNode(ctx, 7, "dev", FileCharDevice, 0666, 0x0102)
	if err != nil {
		t.Fatalf("CreateNode: %v", err)
	}

	if gotPath != "/api/v1/namespace/create-node" {
		t.Fatalf("path = %q", gotPath)
	}
	if gotBody.Parent != 7 || gotBody.Name != "dev" || gotBody.Type != FileCharDevice || gotBody.Mode != 0666 || gotBody.Rdev != 0x0102 {
		t.Fatalf("request body = %+v", gotBody)
	}
	if meta.Type != FileCharDevice || meta.Rdev != 0x0102 {
		t.Fatalf("decoded meta = %+v", meta)
	}
}
