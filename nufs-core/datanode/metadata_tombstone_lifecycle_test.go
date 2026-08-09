package datanode

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/dshmyz/nufs/nufs-core/metadata"
	"github.com/vmihailenco/msgpack/v5"
)

func TestMetadataTombstoneRetainsRealDataNodePayloadUntilPurge(t *testing.T) {
	ctx := context.Background()
	meta, err := metadata.NewPebbleStore(metadata.PebbleStoreConfig{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("new metadata store: %v", err)
	}
	defer meta.Close()
	chunks, err := NewChunkStore(t.TempDir(), 8, 8, nil)
	if err != nil {
		t.Fatalf("new chunk store: %v", err)
	}

	chunkID := metadata.ChunkID(66001)
	payload := []byte("retained through metadata quarantine")
	chunkRaw, err := msgpack.Marshal(&metadata.ChunkMeta{ID: chunkID, Size: int32(len(payload)), State: metadata.ChunkReady})
	if err != nil {
		t.Fatalf("encode chunk metadata: %v", err)
	}
	if err := meta.DB().Set([]byte("/chunk/66001"), chunkRaw, pebble.Sync); err != nil {
		t.Fatalf("seed chunk metadata: %v", err)
	}
	if err := chunks.Write(chunkID, payload); err != nil {
		t.Fatalf("write data-node payload: %v", err)
	}

	if err := meta.DeleteChunk(ctx, chunkID); err != nil {
		t.Fatalf("logical delete: %v", err)
	}
	if _, err := meta.GetChunk(ctx, chunkID); err != nil {
		t.Fatalf("metadata disappeared during quarantine: %v", err)
	}
	got, _, err := chunks.Read(chunkID, 0, 0)
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("payload during quarantine = (%q, %v), want retained payload", got, err)
	}

	deletedAt := time.Now().UTC().Add(-25 * time.Hour).Round(0)
	tombstoneRaw, err := msgpack.Marshal(&metadata.ChunkTombstone{
		ChunkID:     chunkID,
		Size:        int64(len(payload)),
		Reason:      "test",
		DeletedAt:   deletedAt,
		DeleteAfter: deletedAt.Add(25 * time.Hour),
	})
	if err != nil {
		t.Fatalf("encode eligible tombstone: %v", err)
	}
	if err := meta.DB().Set([]byte("chunk-tombstone/66001"), tombstoneRaw, pebble.Sync); err != nil {
		t.Fatalf("age tombstone: %v", err)
	}
	if err := meta.ReplaceCommittedBackupCatalog(ctx, []metadata.CommittedBackup{{
		ID:              "20260728T120000Z-000000000001",
		SourceClusterID: "cluster-a",
		CreatedAt:       deletedAt.Add(time.Second),
		RaftTerm:        1,
		AppliedIndex:    1,
	}}, time.Now().UTC().Round(0)); err != nil {
		t.Fatalf("replace backup catalog: %v", err)
	}
	if err := meta.PurgeChunk(ctx, chunkID); err != nil {
		t.Fatalf("physical metadata purge: %v", err)
	}
	if _, err := meta.GetChunk(ctx, chunkID); !errors.Is(err, metadata.ErrChunkNotFound) {
		t.Fatalf("metadata after purge = %v, want ErrChunkNotFound", err)
	}
	gc := &OpsServer{store: chunks, meta: meta}
	deleted, err := gc.triggerGCScan(ctx)
	if err != nil || deleted != 1 {
		t.Fatalf("datanode orphan GC = (%d, %v), want (1, nil)", deleted, err)
	}
	if _, _, err := chunks.Read(chunkID, 0, 0); err == nil {
		t.Fatal("payload remained after metadata purge and datanode orphan GC")
	}
}
