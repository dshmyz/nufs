package main

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// TestExtentInodeServiceHTTPContract drives the full V2.1 extent-inode
// lifecycle (empty → inline → pages) through the real ops mux + HTTPClient:
// SetInlineExtent → ResolveExtents → GetExtentMeta → PromoteToPages →
// AppendExtent, asserting the metad service persisted both the inode layout
// and the /extent-meta rows. This is the remote-mode (metad) serving contract
// for roadmap stage 1 §1.3a.
func TestExtentInodeServiceHTTPContract(t *testing.T) {
	store, bundle := newOpsTestStore(t)
	srv := httptest.NewServer(buildOpsTestMux(t, store, bundle))
	defer srv.Close()

	client := metadata.NewHTTPClient(srv.URL, 10*time.Second)
	ctx := context.Background()
	id := metadata.InodeID(90001)

	// Seed a V2 empty inode (1.3c owns the gateway-side create; the metad
	// surface starts at the layout transition).
	if _, err := metadata.NewInodeStoreV2(store).CreateEmpty(id, metadata.FileRegular, 1, 0, 0, 0644); err != nil {
		t.Fatal(err)
	}

	first := &metadata.ExtentMetaV2{
		ID:           metadata.ExtentIDV2(0x90000009001),
		Generation:   1,
		LogicalLen:   4096,
		Checksum:     0xcafebabe,
		PGID:         3,
		Lifecycle:    metadata.LifecycleReady,
		StorageClass: metadata.StorageClassHotReplica,
	}
	if err := client.SetInlineExtent(ctx, id, first, 4096); err != nil {
		t.Fatalf("SetInlineExtent: %v", err)
	}

	refs, err := client.ResolveExtents(ctx, id)
	if err != nil {
		t.Fatalf("ResolveExtents: %v", err)
	}
	if len(refs) != 1 || refs[0].ExtentID != first.ID {
		t.Fatalf("resolve inline refs = %+v, want [%d]", refs, first.ID)
	}

	got, err := client.GetExtentMeta(ctx, first.ID)
	if err != nil {
		t.Fatalf("GetExtentMeta: %v", err)
	}
	if got.PGID != 3 || got.LogicalLen != 4096 || got.Checksum != 0xcafebabe {
		t.Fatalf("extent meta = %+v", got)
	}

	if err := client.PromoteToPages(ctx, id); err != nil {
		t.Fatalf("PromoteToPages: %v", err)
	}
	second := &metadata.ExtentMetaV2{
		ID:           metadata.ExtentIDV2(0x90000009002),
		Generation:   1,
		LogicalLen:   8192,
		PGID:         4,
		Lifecycle:    metadata.LifecycleReady,
		StorageClass: metadata.StorageClassColdEC,
	}
	root, err := client.AppendExtent(ctx, id, second, 4096)
	if err != nil {
		t.Fatalf("AppendExtent: %v", err)
	}
	if root == 0 {
		t.Fatal("append returned root 0")
	}
	refs, err = client.ResolveExtents(ctx, id)
	if err != nil {
		t.Fatalf("ResolveExtents after append: %v", err)
	}
	if len(refs) != 2 || refs[0].ExtentID != first.ID || refs[1].ExtentID != second.ID {
		t.Fatalf("resolved refs = %+v, want [%d %d]", refs, first.ID, second.ID)
	}

	// Both extents' metadata rows are durable in the metad store.
	m2, err := store.GetExtentMeta(ctx, second.ID)
	if err != nil {
		t.Fatalf("durable GetExtentMeta(second): %v", err)
	}
	if m2.StorageClass != metadata.StorageClassColdEC {
		t.Fatalf("durable storage class = %d, want ColdEC", m2.StorageClass)
	}

	// Layout is pages in the authoritative store.
	in, err := metadata.NewInodeStoreV2(store).Get(id)
	if err != nil {
		t.Fatal(err)
	}
	if in.Layout != metadata.LayoutExtentPages {
		t.Fatalf("durable layout = %d, want ExtentPages", in.Layout)
	}
}

// TestExtentInodeServiceHTTPErrors checks the machine-readable error mapping
// across the HTTP hop: a missing extent id → 404 mapped back to
// ErrExtentNotFound on the client, and a V1 UpdateInode PUT on a V2-layout
// row → the collision guard refuses a data write (non-empty chunks) with a
// 500 whose body names the V2 layout, while a metadata-only update (nil
// chunks — GetInode on a V2 row returns none) merges into the row so
// chmod/chown/touch keep working on V2 files. The guard therefore holds even
// when the gateway talks to metad remotely. (The HTTPClient treats 5xx as
// retryable leader-transition noise and drops the body, so the refusal text
// is asserted at the handler, not the client.)
func TestExtentInodeServiceHTTPErrors(t *testing.T) {
	store, bundle := newOpsTestStore(t)
	mux := buildOpsTestMux(t, store, bundle)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	client := metadata.NewHTTPClient(srv.URL, 10*time.Second)
	ctx := context.Background()

	// 404 → ErrExtentNotFound via the machine-readable code.
	if _, err := client.GetExtentMeta(ctx, metadata.ExtentIDV2(424242)); !errors.Is(err, metadata.ErrExtentNotFound) {
		t.Fatalf("GetExtentMeta missing = %v, want ErrExtentNotFound", err)
	}

	// V1 UpdateInode on a V2-layout row across HTTP.
	id := metadata.InodeID(90002)
	if _, err := metadata.NewInodeStoreV2(store).CreateEmpty(id, metadata.FileRegular, 1, 0, 0, 0644); err != nil {
		t.Fatal(err)
	}
	ext := &metadata.ExtentMetaV2{ID: metadata.ExtentIDV2(0x90000009003), LogicalLen: 4096, PGID: 3}
	if err := store.SetInlineExtent(ctx, id, ext, 4096); err != nil {
		t.Fatalf("SetInlineExtent: %v", err)
	}

	// Metadata-only update merges: 200, layout preserved, size overlaid.
	metaBody := bytes.NewBufferString(`{"id":90002,"size":9999}`)
	req := httptest.NewRequest(http.MethodPut, "/api/v1/inodes/90002", metaBody)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("metadata-only V1 update status = %d body=%s, want 200", rr.Code, rr.Body.String())
	}
	in, err := metadata.NewInodeStoreV2(store).Get(id)
	if err != nil {
		t.Fatal(err)
	}
	if in.Layout != metadata.LayoutInlineExtent || in.Size != 9999 {
		t.Fatalf("merged metadata-only update = %+v, want inline layout size 9999", in)
	}

	// A V1 data write (non-empty chunks) refuses across HTTP.
	dataBody := bytes.NewBufferString(`{"id":90002,"size":9999,"chunks":[{"id":123,"offset":0,"length":9999,"version":1}]}`)
	req = httptest.NewRequest(http.MethodPut, "/api/v1/inodes/90002", dataBody)
	rr = httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("V1 data write on V2-layout row status = %d body=%s, want 500", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "V2 layout") {
		t.Fatalf("V1 data write refusal body = %s, want it to name the V2 layout", rr.Body.String())
	}

	// The refused write corrupted nothing: the row is still the inline layout.
	in, err = metadata.NewInodeStoreV2(store).Get(id)
	if err != nil {
		t.Fatal(err)
	}
	if in.Layout != metadata.LayoutInlineExtent || in.Size != 9999 {
		t.Fatalf("durable inode after refused V1 data write = %+v, want inline layout size 9999", in)
	}
}
