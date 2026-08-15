package s3

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/dshmyz/nufs/nufs-core/chunkstore"
	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// Read dual-model tests (roadmap stage 1 §1.3b): a V2.1 extent-layout inode
// must stream its data through GET / COPY exactly like a V1 ChunkMap file.
// These use a real PebbleStore (which implements both MetadataService and
// ExtentInodeService) so the resolver probe runs for real, with an
// in-memory chunk store holding the payload under the chunk whose ID equals
// the extent ID (the extent==chunk-ID invariant).

// seedV2InlineFile creates a bucket file, allocates one real chunk, writes
// its payload, then promotes the inode to a V2 inline extent whose ID
// mirrors the chunk ID. The V1 ChunkMap is dropped by SetInlineExtent.
func seedV2InlineFile(t *testing.T, store *metadata.PebbleStore, mem *chunkstore.MemoryChunkStore, bucket, key string, payload []byte) {
	t.Helper()
	ctx := context.Background()
	root := quotaBucketRoot(t, store, bucket)
	inode, err := store.CreateFile(ctx, root, key, 0o644)
	if err != nil {
		t.Fatalf("CreateFile: %v", err)
	}
	binfo, err := store.GetBucket(ctx, bucket)
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}
	chunk, err := store.AllocateChunk(ctx, inode.ID, 0, binfo.Policy)
	if err != nil {
		t.Fatalf("AllocateChunk: %v", err)
	}
	if err := mem.WriteChunk(ctx, chunk, payload); err != nil {
		t.Fatalf("WriteChunk: %v", err)
	}
	ext := &metadata.ExtentMetaV2{
		ID:         metadata.ExtentIDV2(chunk.ID),
		Generation: 1,
		LogicalLen: int64(len(payload)),
		PGID:       1,
	}
	if err := store.SetInlineExtent(ctx, inode.ID, ext, int64(len(payload))); err != nil {
		t.Fatalf("SetInlineExtent: %v", err)
	}
}

func newV2Gateway(t *testing.T, store *metadata.PebbleStore, mem *chunkstore.MemoryChunkStore) *Gateway {
	t.Helper()
	return NewGateway(GatewayConfig{
		MetaService: store,
		ChunkStore:  mem,
	})
}

func TestGetObject_V2ExtentLayout(t *testing.T) {
	store := newTestMetadataWithQuota(t)
	mem := chunkstore.NewMemoryChunkStore()
	createQuotaTestBucket(t, store, "photos")

	payload := []byte("hello v2 extent file")
	seedV2InlineFile(t, store, mem, "photos", "v2.txt", payload)

	gw := newV2Gateway(t, store, mem)

	req := httptest.NewRequest(http.MethodGet, "/photos/v2.txt", nil)
	rec := httptest.NewRecorder()
	gw.handleGetObject(rec, req, "photos", "v2.txt", "get-request")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	if got := rec.Body.String(); got != string(payload) {
		t.Fatalf("GET body = %q, want %q", got, payload)
	}
}

func TestGetObject_V2ExtentLayout_Range(t *testing.T) {
	store := newTestMetadataWithQuota(t)
	mem := chunkstore.NewMemoryChunkStore()
	createQuotaTestBucket(t, store, "photos")

	payload := []byte("the quick brown fox jumps")
	seedV2InlineFile(t, store, mem, "photos", "v2.txt", payload)

	gw := newV2Gateway(t, store, mem)
	req := httptest.NewRequest(http.MethodGet, "/photos/v2.txt", nil)
	req.Header.Set("Range", "bytes=4-14")
	rec := httptest.NewRecorder()
	gw.handleGetObject(rec, req, "photos", "v2.txt", "get-request")

	if rec.Code != http.StatusPartialContent {
		t.Fatalf("status = %d, want 206; body=%s", rec.Code, rec.Body.String())
	}
	want := payload[4:15]
	if got := rec.Body.String(); got != string(want) {
		t.Fatalf("range body = %q, want %q", got, want)
	}
	if cr := rec.Header().Get("Content-Range"); !strings.HasPrefix(cr, "bytes 4-14/") {
		t.Fatalf("Content-Range = %q, want prefix bytes 4-14/", cr)
	}
}

// TestGetObject_V1ChunkMapRegression locks in the V1 passthrough: a file
// still stored as a ChunkMap (never promoted to a V2 extent) streams
// unchanged through the resolver.
func TestGetObject_V1ChunkMapRegression(t *testing.T) {
	store := newTestMetadataWithQuota(t)
	mem := chunkstore.NewMemoryChunkStore()
	createQuotaTestBucket(t, store, "photos")

	ctx := context.Background()
	root := quotaBucketRoot(t, store, "photos")
	inode, err := store.CreateFile(ctx, root, "v1.txt", 0o644)
	if err != nil {
		t.Fatal(err)
	}
	binfo, err := store.GetBucket(ctx, "photos")
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("v1 chunkmap file")
	chunk, err := store.AllocateChunk(ctx, inode.ID, 0, binfo.Policy)
	if err != nil {
		t.Fatalf("AllocateChunk: %v", err)
	}
	if err := mem.WriteChunk(ctx, chunk, payload); err != nil {
		t.Fatalf("WriteChunk: %v", err)
	}
	// A real write path fills the ChunkRef length and the file size via
	// UpdateInode (AllocateChunk only records the ChunkMap ref); mirror that
	// so GET computes chunk windows from the actual payload length.
	// Read-modify-write: UpdateInode replaces the whole V1 row, so carry the
	// allocated ChunkMap forward.
	in, err := store.GetInode(ctx, inode.ID)
	if err != nil {
		t.Fatal(err)
	}
	in.Size = int64(len(payload))
	in.ChunkMap[0].Length = int32(len(payload))
	if err := store.UpdateInode(ctx, in); err != nil {
		t.Fatalf("UpdateInode(size): %v", err)
	}

	gw := newV2Gateway(t, store, mem)
	req := httptest.NewRequest(http.MethodGet, "/photos/v1.txt", nil)
	rec := httptest.NewRecorder()
	gw.handleGetObject(rec, req, "photos", "v1.txt", "get-request")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	if got := rec.Body.String(); got != string(payload) {
		t.Fatalf("V1 GET body = %q, want %q", got, payload)
	}
}

// TestCopyObject_V2ExtentSource copies FROM a V2 inline source object
// through streamCopySource (the resolver's second gateway consumer), then
// reads the destination back.
func TestCopyObject_V2ExtentSource(t *testing.T) {
	store := newTestMetadataWithQuota(t)
	mem := chunkstore.NewMemoryChunkStore()
	createQuotaTestBucket(t, store, "photos")

	payload := []byte("copied v2 extent payload")
	seedV2InlineFile(t, store, mem, "photos", "src.txt", payload)

	gw := newV2Gateway(t, store, mem)
	req := httptest.NewRequest(http.MethodPut, "/photos/dst.txt", nil)
	req.Header.Set("X-Amz-Copy-Source", "/photos/src.txt")
	rec := httptest.NewRecorder()
	gw.handleCopyObject(rec, req, "photos", "dst.txt", "copy-request")

	if rec.Code != http.StatusOK {
		t.Fatalf("copy status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}

	// The destination is a fresh V1 ChunkMap object; read it back.
	req = httptest.NewRequest(http.MethodGet, "/photos/dst.txt", nil)
	rec = httptest.NewRecorder()
	gw.handleGetObject(rec, req, "photos", "dst.txt", "get-request")
	if rec.Code != http.StatusOK {
		t.Fatalf("dest GET status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	if got := rec.Body.String(); got != string(payload) {
		t.Fatalf("copied body = %q, want %q", got, payload)
	}
}
