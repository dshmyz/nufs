package metadata

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// ========== Roadmap §1.4: 备份/恢复按 extent 清单 ==========
//
// The backup artifact itself is a whole-Pebble checkpoint archive, so V2
// rows are captured by construction. The gap this knife closes is the
// verify layer: inspectBackupCheckpoint decoded /inode/ rows as V1
// InodeMeta only, so a V2-layout inode (InlineExtent / ExtentPages)
// decoded to an empty ChunkMap and its backing chunk (data in the
// ID==ExtentID /chunk/ row) was invisible to the inode_chunk_references
// cross-check — a V2 file's backing chunk missing from the artifact
// verified PASS. The new model-aware scan collects inode-referenced
// extents and requires each to have both its /extent-meta/ and /chunk/
// rows present, and /extent-meta/ + /extent-page/ now get counted.

// backupExtentFixture builds a PebbleStore (real dir — in-memory stores
// cannot checkpoint) with one bucket and three files: a V1 ChunkMap file,
// a V2 inline file, and a V2 pages file. Every referenced chunk row is
// created so the fixture verifies cleanly; individual tests delete rows
// to inject the failure modes.
type backupExtentFixture struct {
	store     *PebbleStore
	root      InodeID
	v1ID      InodeID
	v1Chunk   ChunkID
	inlineID  InodeID
	inlineExt ExtentIDV2
	pagesID   InodeID
	pagesExts []ExtentIDV2
}

func newBackupExtentFixture(t *testing.T) *backupExtentFixture {
	t.Helper()
	ctx := context.Background()
	store, err := NewPebbleStore(PebbleStoreConfig{Dir: t.TempDir(), NodeID: 1, UseBucketStats: true})
	if err != nil {
		t.Fatalf("NewPebbleStore: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	if err := store.CreateBucket(ctx, "b", PlacementPolicy{ID: "default", ReplicationFactor: 1}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	b, err := store.GetBucket(ctx, "b")
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}
	fx := &backupExtentFixture{store: store, root: b.RootInode}

	// V1 file: one chunk ref + its backing chunk row.
	v1, err := store.CreateFile(ctx, fx.root, "v1.bin", 0644)
	if err != nil {
		t.Fatalf("CreateFile v1: %v", err)
	}
	fx.v1ID = v1.ID
	fx.v1Chunk = ChunkID(40001)
	// The chunk row must exist before UpdateInode: the reference-epoch
	// check requires every referenced chunk to already be attached.
	if err := store.putMsgpack(fmt.Sprintf("%s%d", prefixChunk, fx.v1Chunk), &ChunkMeta{ID: fx.v1Chunk}); err != nil {
		t.Fatalf("put chunk v1: %v", err)
	}
	v1.Size = 512
	v1.ChunkMap = []ChunkRef{{ID: fx.v1Chunk, Length: 512}}
	if err := store.UpdateInode(ctx, v1); err != nil {
		t.Fatalf("UpdateInode v1: %v", err)
	}

	// V2 inline file: extent metadata + backing chunk + inline layout.
	vi, err := store.CreateFile(ctx, fx.root, "v2i.bin", 0644)
	if err != nil {
		t.Fatalf("CreateFile v2i: %v", err)
	}
	fx.inlineID = vi.ID
	fx.inlineExt = ExtentIDV2(60001)
	inlineExt := &ExtentMetaV2{ID: fx.inlineExt, Generation: 1, LogicalLen: 4096}
	if err := store.putExtentMeta(inlineExt); err != nil {
		t.Fatalf("putExtentMeta inline: %v", err)
	}
	if err := store.putMsgpack(fmt.Sprintf("%s%d", prefixChunk, fx.inlineExt), &ChunkMeta{ID: ChunkID(fx.inlineExt)}); err != nil {
		t.Fatalf("put inline backing chunk: %v", err)
	}
	if err := NewInodeStoreV2(store).SetInlineExtent(vi.ID, inlineExt, 4096); err != nil {
		t.Fatalf("SetInlineExtent: %v", err)
	}

	// V2 pages file: two extents via ReplaceExtents (one page under root 1).
	vp, err := store.CreateFile(ctx, fx.root, "v2p.bin", 0644)
	if err != nil {
		t.Fatalf("CreateFile v2p: %v", err)
	}
	fx.pagesID = vp.ID
	var writes []ExtentWrite
	off := int64(0)
	for _, id := range []uint64{60002, 60003} {
		ext := &ExtentMetaV2{ID: ExtentIDV2(id), Generation: 1, LogicalLen: 1024}
		if err := store.putExtentMeta(ext); err != nil {
			t.Fatalf("putExtentMeta pages %d: %v", id, err)
		}
		if err := store.putMsgpack(fmt.Sprintf("%s%d", prefixChunk, id), &ChunkMeta{ID: ChunkID(id)}); err != nil {
			t.Fatalf("put pages backing chunk %d: %v", id, err)
		}
		writes = append(writes, ExtentWrite{Extent: ext, Offset: off})
		fx.pagesExts = append(fx.pagesExts, ExtentIDV2(id))
		off += 1024
	}
	if err := NewInodeStoreV2(store).ReplaceExtents(vp.ID, writes, 2048); err != nil {
		t.Fatalf("ReplaceExtents: %v", err)
	}
	return fx
}

// buildBackupArtifact checkpoints the fixture and builds a manifest for
// it, returning the manifest and the checkpoint (whose Release runs via
// t.Cleanup).
func buildBackupArtifact(t *testing.T, fx *backupExtentFixture, backupID string) (*BackupManifest, *PortableCheckpoint) {
	t.Helper()
	ctx := context.Background()
	cp, err := fx.store.CreateStandaloneCheckpoint(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("CreateStandaloneCheckpoint: %v", err)
	}
	t.Cleanup(func() { _ = cp.Release() })
	manifest, err := BuildBackupManifest(ctx, cp.Dir, BackupSnapshotMetadata{
		BackupID:        backupID,
		SourceClusterID: "source-cluster",
		CreatedAt:       time.Unix(10, 0).UTC(),
		RaftTerm:        3,
		AppliedIndex:    42,
	})
	if err != nil {
		t.Fatalf("BuildBackupManifest: %v", err)
	}
	return manifest, cp
}

// TestBackupExtentVerify_ReportsV2ExtentsPass is the happy path: the
// fixture's V2 inline + pages extents are counted, the backing chunks are
// all present, and the extent_references cross-check passes.
func TestBackupExtentVerify_ReportsV2ExtentsPass(t *testing.T) {
	ctx := context.Background()
	fx := newBackupExtentFixture(t)
	manifest, cp := buildBackupArtifact(t, fx, "bt-pass")
	report, err := VerifyBackupArtifact(ctx, cp.Dir, manifest)
	if err != nil {
		t.Fatalf("VerifyBackupArtifact: %v", err)
	}
	if report.RecordCounts.ExtentMeta != 3 {
		t.Fatalf("ExtentMeta = %d, want 3 (inline + 2 pages)", report.RecordCounts.ExtentMeta)
	}
	if report.RecordCounts.ExtentPages != 1 {
		t.Fatalf("ExtentPages = %d, want 1 (two refs, one page)", report.RecordCounts.ExtentPages)
	}
	if report.RecordCounts.Inodes != 5 {
		t.Fatalf("Inodes = %d, want 5 (root + bucket root + v1 + inline + pages)", report.RecordCounts.Inodes)
	}
	if report.RecordCounts.Chunks != 4 {
		t.Fatalf("Chunks = %d, want 4 (v1 + 3 backing)", report.RecordCounts.Chunks)
	}
	if !checkPassed(report, "extent_references") {
		t.Fatalf("extent_references check failed: %+v", report.Checks)
	}
	if checkPassed(report, "inode_chunk_references") == false {
		t.Fatalf("inode_chunk_references check failed: %+v", report.Checks)
	}
}

// TestBackupExtentVerify_RejectsMissingBackingChunk deletes the inline
// extent's /chunk/ row (data row) and asserts the build fails: a V2
// inode-referenced extent must have its backing chunk in the artifact.
// Before this knife the model-blind decode collected no extent references,
// so this corruption verified PASS.
func TestBackupExtentVerify_RejectsMissingBackingChunk(t *testing.T) {
	ctx := context.Background()
	fx := newBackupExtentFixture(t)
	if err := fx.store.db.Delete([]byte(fmt.Sprintf("%s%d", prefixChunk, fx.inlineExt)), nil); err != nil {
		t.Fatalf("Delete backing chunk: %v", err)
	}
	cp, err := fx.store.CreateStandaloneCheckpoint(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("CreateStandaloneCheckpoint: %v", err)
	}
	t.Cleanup(func() { _ = cp.Release() })
	_, err = BuildBackupManifest(ctx, cp.Dir, BackupSnapshotMetadata{BackupID: "bt-miss-chunk"})
	if err == nil || !strings.Contains(err.Error(), "lacks chunk metadata") {
		t.Fatalf("BuildBackupManifest err = %v, want lacking chunk metadata", err)
	}
}

// TestBackupExtentVerify_RejectsMissingExtentMeta deletes the inline
// extent's /extent-meta/ row and asserts the build fails: a V2
// inode-referenced extent must have its metadata row too.
func TestBackupExtentVerify_RejectsMissingExtentMeta(t *testing.T) {
	ctx := context.Background()
	fx := newBackupExtentFixture(t)
	if err := fx.store.db.Delete([]byte(fmt.Sprintf("%s%d", prefixExtentMeta, fx.inlineExt)), nil); err != nil {
		t.Fatalf("Delete extent metadata: %v", err)
	}
	cp, err := fx.store.CreateStandaloneCheckpoint(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("CreateStandaloneCheckpoint: %v", err)
	}
	t.Cleanup(func() { _ = cp.Release() })
	_, err = BuildBackupManifest(ctx, cp.Dir, BackupSnapshotMetadata{BackupID: "bt-miss-meta"})
	if err == nil || !strings.Contains(err.Error(), "lacks extent metadata") {
		t.Fatalf("BuildBackupManifest err = %v, want lacking extent metadata", err)
	}
}

// TestBackupRestore_V2ExtentsRoundTrip is the golden test: backup via
// checkpoint → Publish to a filesystem repository → RestoreBackupToNewCluster
// → reopen the restored dir and resolve both V2 layouts to their original
// extent IDs. It proves the on-disk V2 rows survive the backup+restore
// round trip, not just verify.
func TestBackupRestore_V2ExtentsRoundTrip(t *testing.T) {
	ctx := context.Background()
	fx := newBackupExtentFixture(t)
	manifest, cp := buildBackupArtifact(t, fx, "bt-roundtrip")

	repo, err := NewFilesystemBackupRepository(t.TempDir())
	if err != nil {
		t.Fatalf("NewFilesystemBackupRepository: %v", err)
	}
	if err := repo.Publish(ctx, cp.Dir, manifest); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	target := filepath.Join(t.TempDir(), "metadata")
	report, err := RestoreBackupToNewCluster(ctx, repo, RestoreOptions{
		BackupID:     manifest.BackupID,
		TargetDir:    target,
		NewClusterID: "restored-cluster",
	})
	if err != nil {
		t.Fatalf("RestoreBackupToNewCluster: %v", err)
	}
	if report.Verification.RecordCounts.ExtentMeta == 0 || report.Verification.RecordCounts.ExtentPages == 0 {
		t.Fatalf("restored record counts = %+v, want non-zero extent counts", report.Verification.RecordCounts)
	}

	restored, err := NewPebbleStore(PebbleStoreConfig{Dir: target})
	if err != nil {
		t.Fatalf("NewPebbleStore (restored): %v", err)
	}
	defer restored.Close()

	// Inline layout survives with its single extent resolvable.
	irefs, err := NewInodeStoreV2(restored).ResolveExtents(fx.inlineID)
	if err != nil {
		t.Fatalf("ResolveExtents inline: %v", err)
	}
	if len(irefs) != 1 || irefs[0].ExtentID != fx.inlineExt {
		t.Fatalf("restored inline extents = %+v, want [%d]", irefs, fx.inlineExt)
	}
	imeta, err := restored.GetExtentMeta(ctx, fx.inlineExt)
	if err != nil {
		t.Fatalf("GetExtentMeta inline: %v", err)
	}
	if imeta.LogicalLen != 4096 {
		t.Fatalf("restored inline LogicalLen = %d, want 4096", imeta.LogicalLen)
	}

	// Pages layout survives with both extents in order.
	prefs, err := NewInodeStoreV2(restored).ResolveExtents(fx.pagesID)
	if err != nil {
		t.Fatalf("ResolveExtents pages: %v", err)
	}
	if len(prefs) != 2 {
		t.Fatalf("restored pages extents = %+v, want 2", prefs)
	}
	for i, want := range fx.pagesExts {
		if prefs[i].ExtentID != want {
			t.Fatalf("restored pages extent[%d] = %d, want %d", i, prefs[i].ExtentID, want)
		}
	}
}
