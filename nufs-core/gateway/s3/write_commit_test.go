package s3

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/example/dfs/chunkstore"
	"github.com/example/dfs/metadata"
)

func TestPutObjectDoesNotExposeCommittedChunksWhenReplicaWriteFails(t *testing.T) {
	store := chunkstore.NewMemoryChunkStore()
	store.WriteHook = func(metadata.ChunkID, []byte) error {
		return errors.New("replica quorum failed")
	}
	_, ts, meta := newTestGatewayWithStore(t, store)
	defer ts.Close()

	if err := meta.CreateBucket(context.Background(), "bucket", metadata.PlacementPolicy{ReplicationFactor: 1}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}

	req, err := http.NewRequest(http.MethodPut, ts.URL+"/bucket/object.txt", strings.NewReader("payload"))
	if err != nil {
		t.Fatalf("new put request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("put object: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusServiceUnavailable)
	}

	bucket, err := meta.GetBucket(context.Background(), "bucket")
	if err != nil {
		t.Fatalf("get bucket: %v", err)
	}
	if _, err := meta.Lookup(context.Background(), bucket.RootInode, "object.txt"); !errors.Is(err, metadata.ErrEntryNotFound) {
		t.Fatalf("lookup object inode = %v, want %v", err, metadata.ErrEntryNotFound)
	}

	if len(meta.chunks) != 0 {
		t.Fatalf("allocated chunks remain after failed write: %+v", meta.chunks)
	}
}
