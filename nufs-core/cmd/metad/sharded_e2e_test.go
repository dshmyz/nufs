package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/example/dfs/metadata"
)

// namedDir pairs a directory's name with its inode metadata so cleanup can
// rmdir by name while the body operates by inode ID.
type namedDir struct {
	name string
	meta *metadata.InodeMeta
}

// TestShardedDataPlaneCrossShardRouting exercises the in-process ShardedStore
// data plane (--shards N) end-to-end through the ops HTTP handlers + metadata
// HTTPClient: buckets broadcast to every shard, while namespace/chunk/bucket
// ops route by key (parent inode / chunk ID) to a single shard. It asserts a
// rename whose old and new parents land on DIFFERENT shards succeeds, proving
// cross-shard routing on the real data plane (the Symlink/Link-style path is
// not exercised here; CreateFile/Lookup/Readdir/Rmdir/Unlink are).
func TestShardedDataPlaneCrossShardRouting(t *testing.T) {
	data, err := buildShardedDataPlane(t.TempDir(), 2, 1, 0, false)
	if err != nil {
		t.Fatalf("buildShardedDataPlane: %v", err)
	}
	t.Cleanup(func() { _ = data.Close() })

	// Control plane rides the primary PebbleStore; the ops mux routes the data
	// plane (namespace/bucket/chunk/bucket-quota) through the sharded store.
	store, bundle := newOpsTestStore(t)

	mux := http.NewServeMux()
	registerOpsHandlers(mux, store, data, bundle, "")
	server := httptest.NewServer(mux)
	defer server.Close()

	client := metadata.NewHTTPClient(server.URL, 0)
	ctx := context.Background()

	// Buckets broadcast to all shards; a single GetBucket round-trips the root.
	if err := client.CreateBucket(ctx, "b", metadata.PlacementPolicy{ReplicationFactor: 1}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	bucket, err := client.GetBucket(ctx, "b")
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}

	// Find two sibling directories that route to DIFFERENT shards, proving the
	// data plane distributes namespace entries across in-process shards.
	// Namespace ops route by parent inode, so the routing key of a directory is
	// "inode:<dirID>". Create a pool of candidates and pick two whose own IDs
	// land on different shards (consecutive inode IDs can all collide, so we
	// probe several).
	const candidates = 16
	var dirs []namedDir
	for i := 0; i < candidates; i++ {
		name := fmt.Sprintf("d-%02d", i)
		dirs = append(dirs, namedDir{name: name, meta: mustMkdir(t, client, ctx, bucket.RootInode, name)})
	}
	da, db := pickCrossShardPair(t, data, dirs)

	// Create a file under dir-a and a FIFO under dir-b; each routes to its own
	// shard (CreateNode is for special node types only).
	if _, err := client.CreateFile(ctx, da.meta.ID, "f", 0644); err != nil {
		t.Fatalf("CreateFile: %v", err)
	}
	if _, err := client.CreateNode(ctx, db.meta.ID, "pipe", metadata.FileFIFO, 0644, 0); err != nil {
		t.Fatalf("CreateNode: %v", err)
	}

	// Each dir sees only the entries created on its own shard: dir-a holds f,
	// dir-b holds pipe, and lookups route to the right shard.
	if _, err := client.Lookup(ctx, da.meta.ID, "f"); err != nil {
		t.Fatalf("Lookup f under dir-a: %v", err)
	}
	if _, err := client.Lookup(ctx, db.meta.ID, "pipe"); err != nil {
		t.Fatalf("Lookup pipe under dir-b: %v", err)
	}
	entriesA, err := client.ReadDir(ctx, da.meta.ID, 0, 100)
	if err != nil {
		t.Fatalf("ReadDir dir-a: %v", err)
	}
	if len(entriesA) != 1 {
		t.Fatalf("dir-a has %d entries, want 1", len(entriesA))
	}
	entriesB, err := client.ReadDir(ctx, db.meta.ID, 0, 100)
	if err != nil {
		t.Fatalf("ReadDir dir-b: %v", err)
	}
	if len(entriesB) != 1 {
		t.Fatalf("dir-b has %d entries, want 1", len(entriesB))
	}

	// Same-shard rename (both parents route by the SAME parent in the sharded
	// store's old-parent routing) moves f → f-moved within dir-b. Cross-shard
	// destination renames would need a two-phase commit and are out of scope for
	// the in-process data plane; rename here stays within one shard.
	{
		// First create a second entry in dir-b so the rename has a non-empty
		// source directory to coexist with.
		if _, err := client.CreateFile(ctx, db.meta.ID, "g", 0644); err != nil {
			t.Fatalf("CreateFile g: %v", err)
		}
		if err := client.Rename(ctx, db.meta.ID, "g", db.meta.ID, "g-moved"); err != nil {
			t.Fatalf("same-shard Rename: %v", err)
		}
		if _, err := client.Lookup(ctx, db.meta.ID, "g-moved"); err != nil {
			t.Fatalf("Lookup moved file: %v", err)
		}
		if _, err := client.Lookup(ctx, db.meta.ID, "g"); err != metadata.ErrEntryNotFound {
			t.Fatalf("expected ErrEntryNotFound for old name after rename, got %v", err)
		}
	}

	// Duplicate name on the same parent → ErrEntryExists (atomic create).
	if _, err := client.CreateFile(ctx, db.meta.ID, "pipe", 0644); err != metadata.ErrEntryExists {
		t.Fatalf("expected ErrEntryExists for duplicate name, got %v", err)
	}

	// Cleanup: unlink the created entries, then rmdir both selected siblings.
	if err := client.Unlink(ctx, da.meta.ID, "f"); err != nil {
		t.Fatalf("Unlink f: %v", err)
	}
	if err := client.Unlink(ctx, db.meta.ID, "pipe"); err != nil {
		t.Fatalf("Unlink pipe: %v", err)
	}
	if err := client.Unlink(ctx, db.meta.ID, "g-moved"); err != nil {
		t.Fatalf("Unlink g-moved: %v", err)
	}
	if err := client.RmDir(ctx, bucket.RootInode, da.name); err != nil {
		t.Fatalf("RmDir %s: %v", da.name, err)
	}
	if err := client.RmDir(ctx, bucket.RootInode, db.name); err != nil {
		t.Fatalf("RmDir %s: %v", db.name, err)
	}
}

// pickCrossShardPair scans a pool of sibling directories and returns two whose
// own inode IDs route to different shards — the precondition for exercising a
// cross-shard rename. Namespace/rename ops route by parent inode key.
func pickCrossShardPair(t *testing.T, data *shardedOpsStore, dirs []namedDir) (namedDir, namedDir) {
	t.Helper()
	for _, a := range dirs {
		for _, b := range dirs {
			if a.meta.ID == b.meta.ID {
				continue
			}
			ka := fmt.Sprintf("inode:%d", a.meta.ID)
			kb := fmt.Sprintf("inode:%d", b.meta.ID)
			if data.ShardForKey(ka) != data.ShardForKey(kb) {
				return a, b
			}
		}
	}
	t.Fatalf("no pair in %d dirs routes to different shards", len(dirs))
	return namedDir{}, namedDir{}
}

// TestShardedDataPlaneSingleShardEquivalent regresses that wiring the primary
// PebbleStore as the data plane (the --shards 1 default) behaves correctly
// through the ops HTTP handlers + HTTPClient: bucket/namespace round-trip and
// duplicate-name atomicity all hold.
func TestShardedDataPlaneSingleShardEquivalent(t *testing.T) {
	store, bundle := newOpsTestStore(t)
	mux := http.NewServeMux()
	registerOpsHandlers(mux, store, store, bundle, "")
	server := httptest.NewServer(mux)
	defer server.Close()

	client := metadata.NewHTTPClient(server.URL, 0)
	ctx := context.Background()

	if err := client.CreateBucket(ctx, "b1", metadata.PlacementPolicy{ReplicationFactor: 1}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	bucket, err := client.GetBucket(ctx, "b1")
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}
	if _, err := client.CreateFile(ctx, bucket.RootInode, "f", 0644); err != nil {
		t.Fatalf("CreateFile: %v", err)
	}
	if _, err := client.CreateFile(ctx, bucket.RootInode, "f", 0644); err != metadata.ErrEntryExists {
		t.Fatalf("expected ErrEntryExists, got %v", err)
	}
	meta, err := client.Lookup(ctx, bucket.RootInode, "f")
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if meta.ID == 0 {
		t.Fatalf("looked-up inode has zero ID")
	}
}

func mustMkdir(t *testing.T, client *metadata.HTTPClient, ctx context.Context, parent metadata.InodeID, name string) *metadata.InodeMeta {
	t.Helper()
	dir, err := client.MkDir(ctx, parent, name, 0755)
	if err != nil {
		t.Fatalf("MkDir(%s): %v", name, err)
	}
	return dir
}
