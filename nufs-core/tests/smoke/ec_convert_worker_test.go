package smoke

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/dshmyz/nufs/nufs-core/chunkstore"
	"github.com/dshmyz/nufs/nufs-core/datanode"
	"github.com/dshmyz/nufs/nufs-core/gateway/s3"
	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// TestECConversionWorker_EndToEnd closes the §14 auto-EC-conversion loop:
// a metad-enqueued TaskECConvert background task is consumed by the datanode
// ConversionWorker (owner-routed lease → ECService.ConvertToEC → complete),
// the chunk's authoritative metadata flips to EC via the publish hook
// (SwitchChunkToEC, the in-process twin of HTTPClient.PublishConversion), and
// the object stays readable through the S3 gateway.
func TestECConversionWorker_EndToEnd(t *testing.T) {
	if os.Getenv("NUFS_RUN_SMOKE") != "1" {
		t.Skip("set NUFS_RUN_SMOKE=1 to run")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	metaDir := t.TempDir()
	metaStore, err := metadata.NewPebbleStore(metadata.PebbleStoreConfig{
		Dir: metaDir, NodeID: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer metaStore.Close()

	// One V2.1 datanode with three data+shard+small store trios: the default
	// candidate synthesis (3 fault-domain slots × 3 shard stores) yields the
	// ≥9 disks the §14 conversion needs on a single physical node. The slots
	// carry synthetic NodeIDs 1..3 whose shards all land on this one node, so
	// all three are registered at its real address — the publish step resolves
	// each shard owner's Addr from this registry, and the gateway serving read
	// dials it to fetch every shard from the same physical server.
	n := startV21Datanode(t, metadata.NodeID(1), t.TempDir(), t.TempDir(), t.TempDir())
	for _, id := range []metadata.NodeID{1, 2, 3} {
		if err := metaStore.RegisterNode(ctx, &metadata.NodeInfo{
			ID: id, Addr: n.Server.Addr(),
			State: metadata.NodeOnline, CapacityGB: 10000,
			Tier: metadata.TierHot, Zone: "z", Rack: "r", MachineID: "m",
			ShardDiskCount: 3,
		}); err != nil {
			t.Fatalf("RegisterNode(%d): %v", id, err)
		}
	}

	cs := chunkstore.NewDatanodeChunkStore()
	defer cs.Close()
	gw := s3.NewGateway(s3.GatewayConfig{
		MetaService: metaStore, ChunkStore: cs,
		RejectEmptyReplicas: true,
	})
	ts := httptest.NewServer(gw.Handler())
	defer ts.Close()

	if err := metaStore.CreateBucket(ctx, "ec-convert-test", metadata.PlacementPolicy{
		ReplicationFactor: 1,
		StorageTier:       metadata.TierHot,
	}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	// Small object → V2 inline extent: the EC conversion demographic.
	payload := []byte("ec-conversion-worker-e2e-payload-")
	for i := 0; i < 20; i++ {
		payload = append(payload, byte('A'+i%26))
	}
	doPut(t, ctx, ts.URL+"/ec-convert-test/data.txt", newReadCloser(payload), http.StatusOK)

	bucket, err := metaStore.GetBucket(ctx, "ec-convert-test")
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}
	obj, err := metaStore.Lookup(ctx, bucket.RootInode, "data.txt")
	if err != nil {
		t.Fatalf("Lookup data.txt: %v", err)
	}
	refs, err := metadata.ResolveFileChunks(ctx, metaStore, obj)
	if err != nil {
		t.Fatalf("ResolveFileChunks: %v", err)
	}
	if len(refs) != 1 {
		t.Fatalf("PUT landed %d refs, want 1 (small object → inline extent)", len(refs))
	}
	cid := metadata.ChunkID(refs[0].ID)
	chunk, err := metaStore.GetChunk(ctx, cid)
	if err != nil {
		t.Fatalf("GetChunk before: %v", err)
	}
	if chunk.ECGroup != nil || chunk.ECStripeID != "" {
		t.Fatalf("chunk already EC before conversion: %+v", chunk)
	}

	// Wire the conversion service over the node's V2Store with the in-process
	// Pebble authority (NewECStore implements ECAuthority, same as the serving
	// path /S1) and the metad publish hook (in-process twin of
	// HTTPClient.PublishConversion), then enqueue the task exactly as the metad
	// scheduler does (OwnerNodes = the chunk's replica holder, this node).
	ec := datanode.NewECService(n.Store, metadata.NewECStore(metaStore))
	ec.SetPublish(func(_ context.Context, st *metadata.ECStripe) error {
		_, err := metadata.NewECStore(metaStore).SwitchChunkToEC(ctx, st.StripeID)
		return err
	})
	taskID := fmt.Sprintf("ec-convert-%d", uint64(cid))
	if err := metaStore.PutBackgroundTask(ctx, &metadata.BackgroundTask{
		ID: taskID, Type: metadata.TaskECConvert, State: metadata.TaskQueued,
		Target: taskID, OwnerNodes: []uint64{1},
	}); err != nil {
		t.Fatalf("enqueue conversion task: %v", err)
	}

	worker := datanode.NewConversionWorker(metaStore, ec, 1, 300*time.Millisecond)
	worker.Start(ctx)
	defer worker.Stop()

	// Wait for the worker to lease → convert → complete the task.
	deadline := time.Now().Add(30 * time.Second)
	for {
		task, err := metaStore.GetBackgroundTask(ctx, taskID)
		if err != nil {
			t.Fatalf("GetBackgroundTask: %v", err)
		}
		if task.State == metadata.TaskSucceeded {
			break
		}
		if task.State == metadata.TaskDeadLetter || task.State == metadata.TaskRetrying {
			t.Fatalf("task %s after conversion attempt (last error %q)", task.State, task.LastError)
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for conversion; task state=%s", task.State)
		}
		time.Sleep(200 * time.Millisecond)
	}

	// The chunk's authoritative metadata flipped to EC and the object is
	// still byte-exact through the S3 gateway.
	chunk, err = metaStore.GetChunk(ctx, cid)
	if err != nil {
		t.Fatalf("GetChunk after: %v", err)
	}
	if chunk.ECGroup == nil || chunk.ECStripeID == "" {
		t.Fatalf("chunk not flipped to EC after conversion: %+v", chunk)
	}
	body := doGet(t, ctx, ts.URL+"/ec-convert-test/data.txt", http.StatusOK)
	if !bytes.Equal(body, payload) {
		t.Fatal("S3 GET mismatch after EC conversion")
	}
	t.Logf("EC conversion worker e2e: chunk %d converted to stripe %s, read back %d bytes OK",
		uint64(cid), chunk.ECStripeID, len(body))
}
