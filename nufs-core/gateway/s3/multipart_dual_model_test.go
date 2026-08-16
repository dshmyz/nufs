package s3

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/dshmyz/nufs/nufs-core/chunkstore"
	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// Roadmap §1.4: multipart complete lands the merged object on the V2 extent
// model exactly like a single PUT. Complete reuses metadataObjectCommitter.Put,
// so the concatenated parts are committed via CommitChunkRefsModelAware:
// ≤16MiB total lands as an inline extent, anything larger as COW extent pages;
// reads resolve through ResolveFileChunks; an overwrite supersedes and
// tombstones the old object's chunks. Complete is the last gateway write path
// that previously produced a V1 ChunkMap.
//
// These tests drive the real HTTP multiplexer over a shared PebbleStore +
// in-memory chunkstore, mirroring dual_model_cross_test.go. The mock gateway is
// unsuitable here: it has no extent serving surface (a V2 commit would degrade
// to V1) and its replica-less chunks read back as empty bodies.

type multipartPartSpec struct {
	num  int
	etag string
}

func initiateMultipartUpload(t *testing.T, ts *httptest.Server, bucket, key string) string {
	t.Helper()
	resp, err := http.Post(ts.URL+"/"+bucket+"/"+key+"?uploads", "application/octet-stream", nil)
	if err != nil {
		t.Fatalf("initiate: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("initiate: status = %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	var out InitiateMultipartUploadResult
	if err := xml.Unmarshal(body, &out); err != nil {
		t.Fatalf("unmarshal initiate: %v", err)
	}
	if out.UploadID == "" {
		t.Fatal("initiate: empty upload ID")
	}
	return out.UploadID
}

func uploadMultipartPart(t *testing.T, ts *httptest.Server, bucket, key, uploadID string, num int, data []byte) string {
	t.Helper()
	url := fmt.Sprintf("%s/%s/%s?uploadId=%s&partNumber=%d", ts.URL, bucket, key, uploadID, num)
	req, _ := http.NewRequest(http.MethodPut, url, bytes.NewReader(data))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("upload part %d: %v", num, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("upload part %d: status = %d", num, resp.StatusCode)
	}
	etag := resp.Header.Get("ETag")
	if etag == "" {
		t.Fatalf("upload part %d: missing ETag", num)
	}
	return etag
}

// completeMultipartUpload posts a Complete listing the given parts in request
// order and returns the parsed success result plus the HTTP status.
func completeMultipartUpload(t *testing.T, ts *httptest.Server, bucket, key, uploadID string, parts []multipartPartSpec) (*CompleteMultipartUploadResult, int) {
	t.Helper()
	var sb strings.Builder
	sb.WriteString("<CompleteMultipartUpload>")
	for _, p := range parts {
		fmt.Fprintf(&sb, "<Part><PartNumber>%d</PartNumber><ETag>%s</ETag></Part>", p.num, p.etag)
	}
	sb.WriteString("</CompleteMultipartUpload>")
	req, _ := http.NewRequest(http.MethodPost,
		ts.URL+"/"+bucket+"/"+key+"?uploadId="+uploadID, strings.NewReader(sb.String()))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("complete: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, resp.StatusCode
	}
	var out CompleteMultipartUploadResult
	if err := xml.Unmarshal(body, &out); err != nil {
		t.Fatalf("unmarshal complete result: %v", err)
	}
	return &out, resp.StatusCode
}

// TestMultipartCompleteLandsV2Layout drives the complete commit decision across
// the layout boundary: a small merged object lands as a V2 inline extent, a
// larger one as COW extent pages, and every object reads back byte-exact as the
// in-order concatenation of the parts (the complete request order, not the part
// numbers, defines the byte order).
func TestMultipartCompleteLandsV2Layout(t *testing.T) {
	store := newTestMetadataWithQuota(t)
	createQuotaTestBucket(t, store, "photos")
	chunkStore := chunkstore.NewMemoryChunkStore()
	gw := NewGateway(GatewayConfig{MetaService: store, ChunkStore: chunkStore})
	ts := httptest.NewServer(gw.Handler())
	defer ts.Close()
	ctx := context.Background()

	cases := []struct {
		name     string
		part1    []byte
		part2    []byte
		wantMeta metadata.InodeLayout
	}{
		{
			name:     "small parts inline",
			part1:    []byte("hello part one "),
			part2:    []byte("world part two"),
			wantMeta: metadata.LayoutInlineExtent,
		},
		{
			// Total > MaxInlineExtentSize forces the pages layout even though
			// the two parts merge into a single chunk.
			name:     "large parts pages",
			part1:    bytes.Repeat([]byte("A"), metadata.MaxInlineExtentSize/2+1),
			part2:    bytes.Repeat([]byte("B"), metadata.MaxInlineExtentSize/2+1),
			wantMeta: metadata.LayoutExtentPages,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			key := "obj-" + strings.ReplaceAll(tc.name, " ", "-")
			uploadID := initiateMultipartUpload(t, ts, "photos", key)
			etag1 := uploadMultipartPart(t, ts, "photos", key, uploadID, 1, tc.part1)
			etag2 := uploadMultipartPart(t, ts, "photos", key, uploadID, 2, tc.part2)
			result, status := completeMultipartUpload(t, ts, "photos", key, uploadID, []multipartPartSpec{{1, etag1}, {2, etag2}})
			if status != http.StatusOK {
				t.Fatalf("complete: status = %d", status)
			}
			if result.ETag == "" {
				t.Fatal("complete: missing ETag")
			}

			root := quotaBucketRoot(t, store, "photos")
			inode, err := store.Lookup(ctx, root, key)
			if err != nil {
				t.Fatalf("Lookup %s: %v", key, err)
			}
			assertV2Layout(t, store, inode.ID, tc.wantMeta)
			if inode.Size != int64(len(tc.part1)+len(tc.part2)) {
				t.Fatalf("inode.Size = %d, want %d", inode.Size, len(tc.part1)+len(tc.part2))
			}
			want := append(append([]byte{}, tc.part1...), tc.part2...)
			if got := getObjectBody(t, gw, "photos", key); !bytes.Equal(got, want) {
				t.Fatalf("GET body len=%d, want %d (prefix=%q)", len(got), len(want), got[:minInt(len(got), 8)])
			}
			refs, err := metadata.ResolveFileChunks(ctx, store, inode)
			if err != nil {
				t.Fatalf("ResolveFileChunks: %v", err)
			}
			if len(refs) == 0 {
				t.Fatal("merged object resolved to no chunk refs")
			}
		})
	}
}

// TestMultipartCompleteOverwriteTombstonesOldChunks verifies that a complete
// overwriting an existing object supersedes it through the same path as a
// single-PUT overwrite: the object reads back the merged bytes, the inode
// follows the new size, and the old object's chunks are tombstoned (via
// DeleteChunk) rather than left dangling in live metadata.
func TestMultipartCompleteOverwriteTombstonesOldChunks(t *testing.T) {
	store := newTestMetadataWithQuota(t)
	createQuotaTestBucket(t, store, "photos")
	chunkStore := chunkstore.NewMemoryChunkStore()
	gw := NewGateway(GatewayConfig{MetaService: store, ChunkStore: chunkStore})
	ts := httptest.NewServer(gw.Handler())
	defer ts.Close()
	ctx := context.Background()

	oldBody := []byte("original single-put object that the multipart complete will replace")
	oldInode := putDualModelObject(t, store, chunkStore, "photos", "obj", oldBody)
	oldRefs, err := metadata.ResolveFileChunks(ctx, store, oldInode)
	if err != nil {
		t.Fatalf("ResolveFileChunks(seed): %v", err)
	}
	if len(oldRefs) == 0 {
		t.Fatal("seed object resolved to no refs")
	}

	uploadID := initiateMultipartUpload(t, ts, "photos", "obj")
	etag1 := uploadMultipartPart(t, ts, "photos", "obj", uploadID, 1, []byte("merged part "))
	etag2 := uploadMultipartPart(t, ts, "photos", "obj", uploadID, 2, []byte("two"))
	if _, status := completeMultipartUpload(t, ts, "photos", "obj", uploadID, []multipartPartSpec{{1, etag1}, {2, etag2}}); status != http.StatusOK {
		t.Fatalf("complete overwrite: status = %d", status)
	}

	want := []byte("merged part two")
	if got := getObjectBody(t, gw, "photos", "obj"); !bytes.Equal(got, want) {
		t.Fatalf("GET after overwrite = %q, want %q", got, want)
	}
	newInode, err := store.Lookup(ctx, quotaBucketRoot(t, store, "photos"), "obj")
	if err != nil {
		t.Fatalf("Lookup after overwrite: %v", err)
	}
	if newInode.Size != int64(len(want)) {
		t.Fatalf("inode.Size = %d, want %d", newInode.Size, len(want))
	}

	// The overwritten object's chunks must be tombstoned, not referenced any
	// longer. DeleteChunk retains the chunk row for the GC quarantine window
	// and records a durable tombstone; the tombstone presence is the durable
	// proof of supersede.
	tombstones, err := store.ListChunkTombstones(ctx, 0)
	if err != nil {
		t.Fatalf("ListChunkTombstones: %v", err)
	}
	tombstoned := make(map[metadata.ChunkID]bool, len(tombstones))
	for _, tsEntry := range tombstones {
		tombstoned[tsEntry.ChunkID] = true
	}
	for _, ref := range oldRefs {
		if !tombstoned[ref.ID] {
			t.Errorf("old chunk %d not tombstoned after multipart overwrite", ref.ID)
		}
	}
}
