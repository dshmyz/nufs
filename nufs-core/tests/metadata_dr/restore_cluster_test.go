package metadata_dr

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/example/dfs/metadata"
)

func TestLocalRestoreRecoveryFixturePreservesMetadataAndGatesReadiness(t *testing.T) {
	ctx := context.Background()
	started := time.Now()
	root := t.TempDir()
	sourceDir := filepath.Join(root, "source")
	source := newFixtureStore(t, sourceDir, 1)
	if _, err := source.EnsureClusterID(ctx, "cluster-source"); err != nil {
		t.Fatalf("EnsureClusterID: %v", err)
	}
	chunkID := seedRecoveryFixture(t, source)
	if err := source.Close(); err != nil {
		t.Fatalf("close source store: %v", err)
	}

	manifest, err := metadata.BuildBackupManifest(ctx, sourceDir, metadata.BackupSnapshotMetadata{
		BackupID:        "backup-20260730T120000Z-localfixture",
		SourceClusterID: "cluster-source",
		CreatedAt:       time.Now().UTC(),
		RaftTerm:        1,
		AppliedIndex:    1,
	})
	if err != nil {
		t.Fatalf("BuildBackupManifest: %v", err)
	}
	repo, err := metadata.NewFilesystemBackupRepository(filepath.Join(root, "repository"))
	if err != nil {
		t.Fatalf("NewFilesystemBackupRepository: %v", err)
	}
	if err := repo.Publish(ctx, sourceDir, manifest); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	targetDir := filepath.Join(root, "restored")
	restoreReport, err := metadata.RestoreBackupToNewCluster(ctx, repo, metadata.RestoreOptions{
		BackupID:     manifest.BackupID,
		TargetDir:    targetDir,
		NewClusterID: "cluster-restored",
	})
	if err != nil {
		t.Fatalf("RestoreBackupToNewCluster: %v", err)
	}
	restored := newFixtureStore(t, targetDir, 101)
	defer restored.Close()
	assertDurableFixtureRecords(t, restored, chunkID)

	bundle, err := metadata.NewPebbleServiceBundle(
		restored,
		metadata.WithLeaseTTL(0),
		metadata.WithGCInterval(0),
		metadata.WithScrubInterval(0),
	)
	if err != nil {
		t.Fatalf("NewPebbleServiceBundle: %v", err)
	}
	defer bundle.Close()
	bundle.SetRestoreReadinessPending(&metadata.RestoreReadinessReport{Ready: false, MinimumReplicas: 1})
	readyServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if !bundle.IsReady() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer readyServer.Close()

	assertReadyStatus(t, readyServer.URL, http.StatusServiceUnavailable)
	readiness, err := metadata.VerifyRestoredChunkAvailability(ctx, restored, fixtureReplicaProbe{reachable: map[metadata.ChunkID]int{chunkID: 3}}, 1)
	if err != nil {
		t.Fatalf("VerifyRestoredChunkAvailability: %v", err)
	}
	if err := restored.ClearRestorePendingMarker(ctx); err != nil {
		t.Fatalf("ClearRestorePendingMarker: %v", err)
	}
	bundle.CompleteRestoreReadiness(readiness)
	assertReadyStatus(t, readyServer.URL, http.StatusOK)

	if rto := restoreReport.CompletedAt.Sub(restoreReport.StartedAt); rto >= 30*time.Minute {
		t.Fatalf("restore RTO = %s, want below 30m", rto)
	}
	if observed := time.Since(started); observed >= 30*time.Minute {
		t.Fatalf("fixture recovery time = %s, want below 30m", observed)
	}
}

func newFixtureStore(t *testing.T, dir string, nodeID uint64) *metadata.PebbleStore {
	t.Helper()
	store, err := metadata.NewPebbleStore(metadata.PebbleStoreConfig{Dir: dir, NodeID: nodeID})
	if err != nil {
		t.Fatalf("NewPebbleStore(%s): %v", dir, err)
	}
	return store
}

func seedRecoveryFixture(t *testing.T, store *metadata.PebbleStore) metadata.ChunkID {
	t.Helper()
	ctx := context.Background()
	for id := 1; id <= 3; id++ {
		if err := store.RegisterNode(ctx, &metadata.NodeInfo{
			ID:         metadata.NodeID(id),
			Addr:       "127.0.0.1:9100",
			Rack:       "rack-a",
			Zone:       "zone-a",
			CapacityGB: 10,
			State:      metadata.NodeOnline,
		}); err != nil {
			t.Fatalf("RegisterNode %d: %v", id, err)
		}
	}
	policy := metadata.PlacementPolicy{ReplicationFactor: 3, TopologySpread: metadata.SpreadNode}
	if err := store.CreateBucket(ctx, "photos", policy); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	if err := store.SetBucketQuota(ctx, "photos", &metadata.BucketQuota{MaxSizeBytes: 1 << 30, MaxObjects: 100}); err != nil {
		t.Fatalf("SetBucketQuota: %v", err)
	}
	bucket, err := store.GetBucket(ctx, "photos")
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}
	dir, err := store.MkDir(ctx, bucket.RootInode, "2026", 0o755)
	if err != nil {
		t.Fatalf("MkDir: %v", err)
	}
	file, err := store.CreateFile(ctx, dir.ID, "img.bin", 0o644)
	if err != nil {
		t.Fatalf("CreateFile: %v", err)
	}
	chunk, err := store.AllocateChunk(ctx, file.ID, 0, policy)
	if err != nil {
		t.Fatalf("AllocateChunk: %v", err)
	}
	states := map[metadata.ChunkID]metadata.ReplicaState{chunk.ID: metadata.ReplicaReady}
	for id := 1; id <= 3; id++ {
		if err := store.ReportChunkState(ctx, metadata.NodeID(id), states); err != nil {
			t.Fatalf("ReportChunkState %d: %v", id, err)
		}
	}
	if err := store.PutWriteAttempt(ctx, &metadata.ObjectWriteAttempt{
		ID:     "attempt-restore-fixture",
		Bucket: "photos",
		Key:    "2026/img.bin",
		State:  metadata.WriteAttemptRecoveryNeeded,
		Chunks: []metadata.ChunkRef{{ID: chunk.ID, Offset: 0, Length: 64}},
	}); err != nil {
		t.Fatalf("PutWriteAttempt: %v", err)
	}
	if err := store.PutBackgroundTask(ctx, &metadata.BackgroundTask{
		ID:    "task-restore-fixture",
		Type:  metadata.TaskWriteRecovery,
		State: metadata.TaskQueued,
	}); err != nil {
		t.Fatalf("PutBackgroundTask: %v", err)
	}
	return chunk.ID
}

func assertDurableFixtureRecords(t *testing.T, store *metadata.PebbleStore, chunkID metadata.ChunkID) {
	t.Helper()
	ctx := context.Background()
	bucket, err := store.GetBucket(ctx, "photos")
	if err != nil || bucket.RootInode == 0 {
		t.Fatalf("restored bucket = %+v err=%v", bucket, err)
	}
	if quota, err := store.GetBucketQuota(ctx, "photos"); err != nil || quota.MaxObjects != 100 {
		t.Fatalf("restored quota = %+v err=%v", quota, err)
	}
	if attempt, err := store.GetWriteAttempt(ctx, "attempt-restore-fixture"); err != nil || attempt.State != metadata.WriteAttemptRecoveryNeeded {
		t.Fatalf("restored write attempt = %+v err=%v", attempt, err)
	}
	if task, err := store.GetBackgroundTask(ctx, "task-restore-fixture"); err != nil || task.State != metadata.TaskQueued {
		t.Fatalf("restored background task = %+v err=%v", task, err)
	}
	if chunk, err := store.GetChunk(ctx, chunkID); err != nil || len(chunk.Replicas) != 3 {
		t.Fatalf("restored chunk = %+v err=%v", chunk, err)
	}
	if marker, err := store.GetRestorePendingMarker(ctx); err != nil || marker == nil {
		t.Fatalf("restore pending marker = %+v err=%v", marker, err)
	}
}

func assertReadyStatus(t *testing.T, baseURL string, want int) {
	t.Helper()
	resp, err := http.Get(baseURL)
	if err != nil {
		t.Fatalf("GET ready: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != want {
		t.Fatalf("ready status = %d, want %d", resp.StatusCode, want)
	}
}

type fixtureReplicaProbe struct {
	reachable map[metadata.ChunkID]int
}

func (p fixtureReplicaProbe) ReachableReplicas(_ context.Context, chunk *metadata.ChunkMeta) (int, error) {
	return p.reachable[chunk.ID], nil
}
