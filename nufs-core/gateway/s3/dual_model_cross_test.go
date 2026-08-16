package s3

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/dshmyz/nufs/nufs-core/chunkstore"
	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// The 1.3c write dual-model and its cross-gateway verification (roadmap
// §1.1 hard requirement: S3 写 → FUSE 读；FUSE 写 → S3 读). These tests run a
// shared PebbleStore + in-memory chunkstore under both the S3 committer and
// the FUSE BufferedFile to prove the two paths land the same V2 extent layout
// and each can read back what the other committed.

func putDualModelObject(t *testing.T, store *metadata.PebbleStore, chunkStore chunkstore.ChunkStore, bucket, key string, body []byte) *metadata.InodeMeta {
	t.Helper()
	root := quotaBucketRoot(t, store, bucket)
	if _, err := newMetadataObjectCommitter(store, chunkStore, false).Put(context.Background(), PutObjectRequest{
		Bucket:        bucket,
		Key:           key,
		Body:          bytes.NewReader(body),
		ContentLength: int64(len(body)),
	}); err != nil {
		t.Fatalf("Put %s/%s: %v", bucket, key, err)
	}
	inode, err := store.Lookup(context.Background(), root, key)
	if err != nil {
		t.Fatalf("Lookup %s/%s: %v", bucket, key, err)
	}
	return inode
}

func assertV2Layout(t *testing.T, store *metadata.PebbleStore, id metadata.InodeID, want metadata.InodeLayout) {
	t.Helper()
	v2, err := metadata.NewInodeStoreV2(store).Get(id)
	if err != nil {
		t.Fatalf("Get V2 inode %d: %v", id, err)
	}
	if v2 == nil {
		t.Fatalf("V2 inode %d absent", id)
	}
	if v2.Layout != want {
		t.Fatalf("inode %d layout = %v, want %v (inode=%+v)", id, v2.Layout, want, v2)
	}
}

func getObjectBody(t *testing.T, gw *Gateway, bucket, key string) []byte {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/"+bucket+"/"+key, nil)
	rr := httptest.NewRecorder()
	gw.handleGetObject(rr, req, bucket, key, "cross-get")
	if rr.Code != http.StatusOK {
		t.Fatalf("GET %s/%s status = %d, want 200; body=%s", bucket, key, rr.Code, rr.Body.String())
	}
	return rr.Body.Bytes()
}

// TestPutObjectDualModelCommit drives the S3 PUT commit decision across the
// layout boundary: a single small chunk lands as a V2 inline extent, anything
// larger (even a single chunk) lands as COW extent pages, and every body reads
// back byte-exact through the S3 GET path (which resolves V2 layouts).
func TestPutObjectDualModelCommit(t *testing.T) {
	cases := []struct {
		name     string
		body     []byte
		wantMeta metadata.InodeLayout
	}{
		{name: "small inline", body: []byte("1234"), wantMeta: metadata.LayoutInlineExtent},
		{name: "single chunk pages", body: bytes.Repeat([]byte("P"), metadata.MaxInlineExtentSize+1), wantMeta: metadata.LayoutExtentPages},
		{name: "multi chunk pages", body: bytes.Repeat([]byte("Q"), metadata.MaxChunkSize+1), wantMeta: metadata.LayoutExtentPages},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := newTestMetadataWithQuota(t)
			createQuotaTestBucket(t, store, "photos")
			chunkStore := chunkstore.NewMemoryChunkStore()
			gw := NewGateway(GatewayConfig{MetaService: store, ChunkStore: chunkStore})

			inode := putDualModelObject(t, store, chunkStore, "photos", "obj", tc.body)
			assertV2Layout(t, store, inode.ID, tc.wantMeta)
			if inode.Size != int64(len(tc.body)) {
				t.Fatalf("inode.Size = %d, want %d", inode.Size, len(tc.body))
			}
			if got := getObjectBody(t, gw, "photos", "obj"); !bytes.Equal(got, tc.body) {
				t.Fatalf("GET body len=%d, want %d (prefix=%q)", len(got), len(tc.body), got[:minInt(len(got), 8)])
			}
		})
	}
}

// TestPutObjectDualModelOverwrite re-commits existing objects across layout
// boundaries. The inode must follow the new size (small → inline, large →
// pages), the stale extent set must be replaced, and each overwrite reads back
// byte-exact.
func TestPutObjectDualModelOverwrite(t *testing.T) {
	cases := []struct {
		name   string
		first  []byte
		second []byte
		want   metadata.InodeLayout
	}{
		{name: "inline to pages", first: []byte("1234"), second: bytes.Repeat([]byte("A"), metadata.MaxInlineExtentSize+1), want: metadata.LayoutExtentPages},
		{name: "pages to inline", first: bytes.Repeat([]byte("B"), metadata.MaxInlineExtentSize+1), second: []byte("5678"), want: metadata.LayoutInlineExtent},
		{name: "inline to inline", first: []byte("abcd"), second: []byte("wxyz"), want: metadata.LayoutInlineExtent},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := newTestMetadataWithQuota(t)
			createQuotaTestBucket(t, store, "photos")
			chunkStore := chunkstore.NewMemoryChunkStore()
			gw := NewGateway(GatewayConfig{MetaService: store, ChunkStore: chunkStore})

			putDualModelObject(t, store, chunkStore, "photos", "obj", tc.first)
			inode := putDualModelObject(t, store, chunkStore, "photos", "obj", tc.second)
			assertV2Layout(t, store, inode.ID, tc.want)
			if inode.Size != int64(len(tc.second)) {
				t.Fatalf("inode.Size = %d, want %d", inode.Size, len(tc.second))
			}
			if got := getObjectBody(t, gw, "photos", "obj"); !bytes.Equal(got, tc.second) {
				t.Fatalf("overwrite GET body len=%d, want %d", len(got), len(tc.second))
			}
		})
	}
}

// TestCrossPath_S3WriteFuseRead is roadmap §1.1's S3 写 → FUSE 读: objects
// committed through the S3 committer (inline and pages) must be readable by a
// FUSE BufferedFile ReadView over the same metadata + chunkstore.
func TestCrossPath_S3WriteFuseRead(t *testing.T) {
	store := newTestMetadataWithQuota(t)
	createQuotaTestBucket(t, store, "photos")
	chunkStore := chunkstore.NewMemoryChunkStore()
	ctx := context.Background()

	cases := []struct {
		name string
		body []byte
	}{
		{name: "inline", body: []byte("s3 wrote this for fuse")},
		{name: "pages", body: bytes.Repeat([]byte("X"), metadata.MaxInlineExtentSize+1)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			inode := putDualModelObject(t, store, chunkStore, "photos", "obj", tc.body)
			b := chunkstore.NewBufferedFile(func() metadata.MetadataService { return store }, chunkStore, inode.ID, inode.Size)
			got, err := b.ReadView(ctx, 0, inode.Size)
			if err != nil {
				t.Fatalf("FUSE ReadView: %v", err)
			}
			if !bytes.Equal(got, tc.body) {
				t.Fatalf("FUSE ReadView len=%d, want %d (prefix=%q)", len(got), len(tc.body), got[:minInt(len(got), 8)])
			}
			// A windowed read lands on the same extent-backed chunks.
			if len(tc.body) > 16 {
				window := tc.body[4:12]
				got, err = b.ReadView(ctx, 4, 8)
				if err != nil {
					t.Fatalf("FUSE window ReadView: %v", err)
				}
				if !bytes.Equal(got, window) {
					t.Fatalf("FUSE window = %q, want %q", got, window)
				}
			}
		})
	}
}

// TestCrossPath_FuseWriteS3Read is roadmap §1.1's FUSE 写 → S3 读: data
// committed through the FUSE BufferedFile flush (inline and pages) must be
// readable by the S3 GET path. The inode is created through the namespace
// (CreateFile) exactly as FUSE's create does, then flushed, then read back
// through the S3 handler.
func TestCrossPath_FuseWriteS3Read(t *testing.T) {
	store := newTestMetadataWithQuota(t)
	createQuotaTestBucket(t, store, "photos")
	chunkStore := chunkstore.NewMemoryChunkStore()
	gw := NewGateway(GatewayConfig{MetaService: store, ChunkStore: chunkStore})
	ctx := context.Background()
	root := quotaBucketRoot(t, store, "photos")

	cases := []struct {
		name string
		key  string
		body []byte
	}{
		{name: "inline", key: "fuse-small", body: []byte("fuse flushed this for s3")},
		{name: "pages", key: "fuse-big", body: bytes.Repeat([]byte("Y"), metadata.MaxInlineExtentSize+1)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			inode, err := store.CreateFile(ctx, root, tc.key, 0o644)
			if err != nil {
				t.Fatalf("CreateFile: %v", err)
			}
			b := chunkstore.NewBufferedFile(func() metadata.MetadataService { return store }, chunkStore, inode.ID, inode.Size)
			if _, err := b.Write(ctx, tc.body, 0); err != nil {
				t.Fatalf("BufferedFile Write: %v", err)
			}
			res, err := b.Flush(ctx)
			if err != nil {
				t.Fatalf("BufferedFile Flush: %v", err)
			}
			if res.NewSize != int64(len(tc.body)) {
				t.Fatalf("flush NewSize = %d, want %d", res.NewSize, len(tc.body))
			}
			if got := getObjectBody(t, gw, "photos", tc.key); !bytes.Equal(got, tc.body) {
				t.Fatalf("S3 GET after FUSE flush len=%d, want %d (prefix=%q)", len(got), len(tc.body), got[:minInt(len(got), 8)])
			}
		})
	}
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
