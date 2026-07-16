package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/example/dfs/metadata"
)

func TestOpsHandlersXAttrRoundTrip(t *testing.T) {
	ctx := context.Background()
	store, err := metadata.NewPebbleStore(metadata.PebbleStoreConfig{
		Dir:         t.TempDir(),
		UseInMemory: true,
		NodeID:      1,
	})
	if err != nil {
		t.Fatalf("NewPebbleStore: %v", err)
	}
	defer store.Close()

	if err := store.CreateBucket(ctx, "bucket", metadata.PlacementPolicy{}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	bucket, err := store.GetBucket(ctx, "bucket")
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}
	file, err := store.CreateFile(ctx, bucket.RootInode, "file", 0644)
	if err != nil {
		t.Fatalf("CreateFile: %v", err)
	}

	bundle, err := metadata.NewPebbleServiceBundle(
		store,
		metadata.WithLeaseTTL(0),
		metadata.WithGCInterval(0),
		metadata.WithScrubInterval(0),
	)
	if err != nil {
		t.Fatalf("NewPebbleServiceBundle: %v", err)
	}
	defer bundle.Close()

	mux := http.NewServeMux()
	registerOpsHandlers(mux, store, bundle, "")
	server := httptest.NewServer(mux)
	defer server.Close()

	client := metadata.NewHTTPClient(server.URL, 0)
	value := []byte{0, 1, 2, 255, 'x'}
	if err := client.SetXAttr(ctx, file.ID, "user.bin", value); err != nil {
		t.Fatalf("SetXAttr: %v", err)
	}
	got, err := client.GetXAttr(ctx, file.ID, "user.bin")
	if err != nil {
		t.Fatalf("GetXAttr: %v", err)
	}
	if !bytes.Equal(got, value) {
		t.Fatalf("xattr value mismatch: got %v want %v", got, value)
	}
	listed, err := client.ListXAttr(ctx, file.ID)
	if err != nil {
		t.Fatalf("ListXAttr: %v", err)
	}
	if !bytes.Equal(listed["user.bin"], value) {
		t.Fatalf("listed xattr mismatch: got %v want %v", listed["user.bin"], value)
	}
	if err := client.RemoveXAttr(ctx, file.ID, "user.bin"); err != nil {
		t.Fatalf("RemoveXAttr: %v", err)
	}
	if _, err := client.GetXAttr(ctx, file.ID, "user.bin"); err != metadata.ErrXAttrNotFound {
		t.Fatalf("expected ErrXAttrNotFound after remove, got %v", err)
	}
}

func TestOpsHandlersXAttrRejectsInvalidInodeID(t *testing.T) {
	store, err := metadata.NewPebbleStore(metadata.PebbleStoreConfig{
		Dir:         t.TempDir(),
		UseInMemory: true,
		NodeID:      1,
	})
	if err != nil {
		t.Fatalf("NewPebbleStore: %v", err)
	}
	defer store.Close()

	bundle, err := metadata.NewPebbleServiceBundle(
		store,
		metadata.WithLeaseTTL(0),
		metadata.WithGCInterval(0),
		metadata.WithScrubInterval(0),
	)
	if err != nil {
		t.Fatalf("NewPebbleServiceBundle: %v", err)
	}
	defer bundle.Close()

	mux := http.NewServeMux()
	registerOpsHandlers(mux, store, bundle, "")
	body, _ := json.Marshal(map[string][]byte{"value": []byte("x")})
	req := httptest.NewRequest(http.MethodPut, "/api/v1/inodes/not-a-number/xattrs/user.key", bytes.NewReader(body))
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d body=%s", rr.Code, rr.Body.String())
	}
}
