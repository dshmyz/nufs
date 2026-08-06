package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/example/dfs/metadata"
)

// TestHandleLookup_EntryNotFoundCode verifies that a Lookup of a
// non-existent key returns a 404 with a machine-readable code
// ("entry_not_found"). The HTTP client's readResponse matches on this
// code to map it to ErrEntryNotFound; without it, callers (e.g. the S3
// PUT new-object path) treat "key absent" as an unexpected failure and
// wrap it into ErrObjectMetadataFailed.
func TestHandleLookup_EntryNotFoundCode(t *testing.T) {
	store, _ := newOpsTestStore(t)
	ctx := context.Background()
	if err := store.CreateBucket(ctx, "lookup-test", metadata.PlacementPolicy{ReplicationFactor: 1}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	bucket, err := store.GetBucket(ctx, "lookup-test")
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}

	// Lookup a key that does not exist.
	url := "/api/v1/namespace/lookup?parent=" + strconv.FormatUint(uint64(bucket.RootInode), 10) + "&name=nope"
	req := httptest.NewRequest(http.MethodGet, url, nil)
	rr := httptest.NewRecorder()
	(&opsHandlers{store: store, dataStore: store}).handleLookup(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404, body=%s", rr.Code, rr.Body.String())
	}
	var body map[string]string
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body["code"] != "entry_not_found" {
		t.Fatalf("code = %q, want entry_not_found", body["code"])
	}
}
