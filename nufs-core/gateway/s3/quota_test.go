package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/dshmyz/nufs/nufs-core/chunkstore"
	"github.com/dshmyz/nufs/nufs-core/metadata"
)

func TestPutObjectRejectsByteQuotaExceeded(t *testing.T) {
	ctx := context.Background()
	store := newTestMetadataWithQuota(t)
	createQuotaTestBucket(t, store, "photos")
	if err := store.SetBucketQuota(ctx, "photos", &metadata.BucketQuota{MaxSizeBytes: 3}); err != nil {
		t.Fatalf("SetBucketQuota: %v", err)
	}

	tracked := &quotaTrackingMetadata{PebbleStore: store}
	gw := NewGateway(GatewayConfig{
		MetaService: tracked,
		ChunkStore:  chunkstore.NewMemoryChunkStore(),
	})
	req := httptest.NewRequest(http.MethodPut, "/photos/a.txt", strings.NewReader("1234"))
	req.ContentLength = 4
	rr := httptest.NewRecorder()

	gw.Handler().ServeHTTP(rr, req)

	if rr.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d; body=%s", rr.Code, http.StatusForbidden, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "<Code>QuotaExceeded</Code>") {
		t.Fatalf("body missing QuotaExceeded: %s", rr.Body.String())
	}
	if tracked.createFileCalls != 0 {
		t.Fatalf("CreateFile calls = %d, want 0", tracked.createFileCalls)
	}
	if tracked.allocateChunkCalls != 0 || tracked.allocateBatchCalls != 0 {
		t.Fatalf("chunk allocation calls = %d/%d, want 0/0", tracked.allocateChunkCalls, tracked.allocateBatchCalls)
	}
	if _, err := store.Lookup(ctx, quotaBucketRoot(t, store, "photos"), "a.txt"); !errors.Is(err, metadata.ErrEntryNotFound) {
		t.Fatalf("Lookup rejected object = %v, want %v", err, metadata.ErrEntryNotFound)
	}
	assertQuotaUsage(t, store, "photos", 0, 0)
}

func TestCopyObjectRejectsDestinationQuotaExceeded(t *testing.T) {
	ctx := context.Background()
	store := newTestMetadataWithQuota(t)
	createQuotaTestBucket(t, store, "source")
	createQuotaTestBucket(t, store, "destination")
	chunkStore := chunkstore.NewMemoryChunkStore()
	committer := newMetadataObjectCommitter(store, chunkStore, false)
	putQuotaTestObject(t, committer, "source", "object", "1234", 4)
	if err := store.SetBucketQuota(ctx, "destination", &metadata.BucketQuota{MaxSizeBytes: 3}); err != nil {
		t.Fatalf("SetBucketQuota: %v", err)
	}

	gw := NewGateway(GatewayConfig{MetaService: store, ChunkStore: chunkStore})
	req := httptest.NewRequest(http.MethodPut, "/destination/copy", nil)
	req.Header.Set("X-Amz-Copy-Source", "/source/object")
	rr := httptest.NewRecorder()
	gw.handleCopyObject(rr, req, "destination", "copy", "copy-quota")

	if rr.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d; body=%s", rr.Code, http.StatusForbidden, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "<Code>QuotaExceeded</Code>") {
		t.Fatalf("body missing QuotaExceeded: %s", rr.Body.String())
	}
	if _, err := store.Lookup(ctx, quotaBucketRoot(t, store, "destination"), "copy"); !errors.Is(err, metadata.ErrEntryNotFound) {
		t.Fatalf("Lookup rejected copy = %v, want %v", err, metadata.ErrEntryNotFound)
	}
	assertQuotaUsage(t, store, "destination", 0, 0)
}

func TestCopyObjectAccountsDestinationUsage(t *testing.T) {
	ctx := context.Background()
	store := newTestMetadataWithQuota(t)
	createQuotaTestBucket(t, store, "source")
	createQuotaTestBucket(t, store, "destination")
	chunkStore := chunkstore.NewMemoryChunkStore()
	committer := newMetadataObjectCommitter(store, chunkStore, false)
	putQuotaTestObject(t, committer, "source", "object", "1234", 4)
	if err := store.SetBucketQuota(ctx, "destination", &metadata.BucketQuota{MaxSizeBytes: 10, MaxObjects: 1}); err != nil {
		t.Fatalf("SetBucketQuota: %v", err)
	}

	gw := NewGateway(GatewayConfig{MetaService: store, ChunkStore: chunkStore})
	req := httptest.NewRequest(http.MethodPut, "/destination/copy", nil)
	req.Header.Set("X-Amz-Copy-Source", "/source/object")
	rr := httptest.NewRecorder()
	gw.handleCopyObject(rr, req, "destination", "copy", "copy-success")

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rr.Code, http.StatusOK, rr.Body.String())
	}
	assertQuotaUsage(t, store, "destination", 4, 1)

	source, err := store.Lookup(ctx, quotaBucketRoot(t, store, "source"), "object")
	if err != nil {
		t.Fatalf("Lookup source: %v", err)
	}
	destination, err := store.Lookup(ctx, quotaBucketRoot(t, store, "destination"), "copy")
	if err != nil {
		t.Fatalf("Lookup destination: %v", err)
	}
	if source.ID == destination.ID {
		t.Fatalf("copy reused source inode %d", source.ID)
	}
	if len(source.ChunkMap) == 0 || len(destination.ChunkMap) == 0 {
		t.Fatalf("copy chunk maps are empty: source=%+v destination=%+v", source.ChunkMap, destination.ChunkMap)
	}
	if source.ChunkMap[0].ID == destination.ChunkMap[0].ID {
		t.Fatalf("copy reused source chunk %d", source.ChunkMap[0].ID)
	}
}

func TestObjectCommitterAllowsBelowSmallByteQuota(t *testing.T) {
	ctx := context.Background()
	store := newTestMetadataWithQuota(t)
	createQuotaTestBucket(t, store, "photos")
	if err := store.SetBucketQuota(ctx, "photos", &metadata.BucketQuota{MaxSizeBytes: 10}); err != nil {
		t.Fatalf("SetBucketQuota: %v", err)
	}

	committer := newMetadataObjectCommitter(store, chunkstore.NewMemoryChunkStore(), false)
	if _, err := committer.Put(ctx, PutObjectRequest{
		Bucket:        "photos",
		Key:           "a.txt",
		Body:          strings.NewReader("1234"),
		ContentLength: 4,
	}); err != nil {
		t.Fatalf("Put below quota: %v", err)
	}

	assertQuotaUsage(t, store, "photos", 4, 1)
}

func TestObjectCommitterRejectsNewObjectAndAllowsOverwriteAtMaxObjects(t *testing.T) {
	ctx := context.Background()
	store := newTestMetadataWithQuota(t)
	createQuotaTestBucket(t, store, "photos")
	committer := newMetadataObjectCommitter(store, chunkstore.NewMemoryChunkStore(), false)

	putQuotaTestObject(t, committer, "photos", "a.txt", "old", 3)
	if err := store.SetBucketQuota(ctx, "photos", &metadata.BucketQuota{
		MaxObjects:   1,
		MaxSizeBytes: 10,
	}); err != nil {
		t.Fatalf("SetBucketQuota: %v", err)
	}

	putQuotaTestObject(t, committer, "photos", "a.txt", "new", 3)
	_, err := committer.Put(ctx, PutObjectRequest{
		Bucket:        "photos",
		Key:           "b.txt",
		Body:          strings.NewReader("x"),
		ContentLength: 1,
	})
	if !errors.Is(err, ErrObjectQuotaExceeded) {
		t.Fatalf("new object Put err = %v, want %v", err, ErrObjectQuotaExceeded)
	}
	if _, err := store.Lookup(ctx, quotaBucketRoot(t, store, "photos"), "b.txt"); !errors.Is(err, metadata.ErrEntryNotFound) {
		t.Fatalf("Lookup rejected object = %v, want %v", err, metadata.ErrEntryNotFound)
	}
	assertQuotaUsage(t, store, "photos", 3, 1)
}

func TestObjectCommitterAllowsOverwriteShrinkAboveCurrentByteQuota(t *testing.T) {
	ctx := context.Background()
	store := newTestMetadataWithQuota(t)
	createQuotaTestBucket(t, store, "photos")
	committer := newMetadataObjectCommitter(store, chunkstore.NewMemoryChunkStore(), false)

	putQuotaTestObject(t, committer, "photos", "a.txt", "12345678", 8)
	if err := store.SetBucketQuota(ctx, "photos", &metadata.BucketQuota{MaxSizeBytes: 4}); err != nil {
		t.Fatalf("SetBucketQuota: %v", err)
	}

	putQuotaTestObject(t, committer, "photos", "a.txt", "123", 3)
	assertQuotaUsage(t, store, "photos", 3, 1)
}

func TestObjectCommitterTreatsZeroContentLengthAsKnown(t *testing.T) {
	ctx := context.Background()
	store := newTestMetadataWithQuota(t)
	createQuotaTestBucket(t, store, "photos")
	if err := store.SetBucketQuota(ctx, "photos", &metadata.BucketQuota{MaxObjects: 1}); err != nil {
		t.Fatalf("SetBucketQuota: %v", err)
	}
	committer := newMetadataObjectCommitter(store, chunkstore.NewMemoryChunkStore(), false)

	putQuotaTestObject(t, committer, "photos", "empty", "", 0)
	_, err := committer.Put(ctx, PutObjectRequest{
		Bucket:        "photos",
		Key:           "second",
		Body:          strings.NewReader(""),
		ContentLength: 0,
	})
	if !errors.Is(err, ErrObjectQuotaExceeded) {
		t.Fatalf("second empty Put err = %v, want %v", err, ErrObjectQuotaExceeded)
	}
	assertQuotaUsage(t, store, "photos", 0, 1)
}

func TestObjectCommitterUnknownLengthFinalRejectCleansNewObject(t *testing.T) {
	ctx := context.Background()
	store := newTestMetadataWithQuota(t)
	createQuotaTestBucket(t, store, "photos")
	if err := store.SetBucketQuota(ctx, "photos", &metadata.BucketQuota{MaxSizeBytes: 3}); err != nil {
		t.Fatalf("SetBucketQuota: %v", err)
	}
	committer := newMetadataObjectCommitter(store, chunkstore.NewMemoryChunkStore(), false)

	_, err := committer.Put(ctx, PutObjectRequest{
		Bucket:        "photos",
		Key:           "a.txt",
		Body:          strings.NewReader("1234"),
		ContentLength: -1,
	})
	if !errors.Is(err, ErrObjectQuotaExceeded) {
		t.Fatalf("Put err = %v, want %v", err, ErrObjectQuotaExceeded)
	}
	if _, err := store.Lookup(ctx, quotaBucketRoot(t, store, "photos"), "a.txt"); !errors.Is(err, metadata.ErrEntryNotFound) {
		t.Fatalf("Lookup rejected object = %v, want %v", err, metadata.ErrEntryNotFound)
	}
	assertQuotaUsage(t, store, "photos", 0, 0)

	failed := quotaAttemptsInState(t, store, metadata.WriteAttemptFailed)
	if len(failed) != 1 {
		t.Fatalf("failed attempts = %d, want 1", len(failed))
	}
	if len(failed[0].Chunks) == 0 {
		t.Fatal("failed attempt did not retain chunk cleanup evidence")
	}
	for _, ref := range failed[0].Chunks {
		if _, err := store.GetChunk(ctx, ref.ID); err != nil {
			t.Fatalf("GetChunk(%d) after rejection = %v, want retained metadata", ref.ID, err)
		}
	}
	if got := quotaAttemptsInState(t, store, metadata.WriteAttemptRecoveryNeeded); len(got) != 0 {
		t.Fatalf("recovery-needed attempts = %d, want 0", len(got))
	}
}

func TestObjectCommitterUnknownLengthRejectPreservesOverwrite(t *testing.T) {
	ctx := context.Background()
	store := newTestMetadataWithQuota(t)
	createQuotaTestBucket(t, store, "photos")
	committer := newMetadataObjectCommitter(store, chunkstore.NewMemoryChunkStore(), false)
	putQuotaTestObject(t, committer, "photos", "a.txt", "old", 3)

	root := quotaBucketRoot(t, store, "photos")
	oldInode, err := store.Lookup(ctx, root, "a.txt")
	if err != nil {
		t.Fatalf("Lookup old object: %v", err)
	}
	oldChunks := append([]metadata.ChunkRef(nil), oldInode.ChunkMap...)
	if err := store.SetBucketQuota(ctx, "photos", &metadata.BucketQuota{MaxSizeBytes: 3}); err != nil {
		t.Fatalf("SetBucketQuota: %v", err)
	}

	_, err = committer.Put(ctx, PutObjectRequest{
		Bucket:        "photos",
		Key:           "a.txt",
		Body:          strings.NewReader("1234"),
		ContentLength: -1,
	})
	if !errors.Is(err, ErrObjectQuotaExceeded) {
		t.Fatalf("overwrite Put err = %v, want %v", err, ErrObjectQuotaExceeded)
	}

	got, err := store.Lookup(ctx, root, "a.txt")
	if err != nil {
		t.Fatalf("Lookup preserved object: %v", err)
	}
	if got.ID != oldInode.ID || got.Size != oldInode.Size {
		t.Fatalf("preserved inode = (id=%d size=%d), want (id=%d size=%d)", got.ID, got.Size, oldInode.ID, oldInode.Size)
	}
	if !equalChunkRefs(got.ChunkMap, oldChunks) {
		t.Fatalf("preserved chunks = %+v, want %+v", got.ChunkMap, oldChunks)
	}
	for _, ref := range oldChunks {
		if _, err := store.GetChunk(ctx, ref.ID); err != nil {
			t.Fatalf("old chunk %d was not preserved: %v", ref.ID, err)
		}
	}
	assertQuotaUsage(t, store, "photos", 3, 1)
}

func TestObjectCommitterRejectsPostLockInodeVersionChange(t *testing.T) {
	ctx := context.Background()
	store := newTestMetadataWithQuota(t)
	createQuotaTestBucket(t, store, "photos")
	chunkStore := chunkstore.NewMemoryChunkStore()
	committer := newMetadataObjectCommitter(store, chunkStore, false)
	putQuotaTestObject(t, committer, "photos", "a.txt", "old", 3)

	root := quotaBucketRoot(t, store, "photos")
	bucket, err := store.GetBucket(ctx, "photos")
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}
	var concurrentSnapshot *metadata.InodeMeta
	tracked := &quotaTrackingMetadata{PebbleStore: store}
	tracked.advisoryLockHook = func(ctx context.Context, inodeID metadata.InodeID) error {
		current, err := store.Lookup(ctx, root, "a.txt")
		if err != nil {
			return err
		}
		oldRefs := append([]metadata.ChunkRef(nil), current.ChunkMap...)
		chunk, err := store.AllocateChunk(ctx, inodeID, 0, bucket.Policy)
		if err != nil {
			return err
		}
		data := []byte("concurrent")
		if err := chunkStore.WriteChunk(ctx, chunk, data); err != nil {
			return err
		}
		if err := store.CommitChunk(ctx, chunk.ID, crc32Checksum(data)); err != nil {
			return err
		}
		if err := store.SealChunk(ctx, chunk.ID); err != nil {
			return err
		}
		current.Size = int64(len(data))
		current.ChunkMap = []metadata.ChunkRef{{
			ID: chunk.ID, Offset: 0, Length: int32(len(data)), Version: 1,
		}}
		if err := store.UpdateInode(ctx, current); err != nil {
			return err
		}
		for _, ref := range oldRefs {
			if err := store.DeleteChunk(ctx, ref.ID); err != nil {
				return err
			}
		}
		snapshot := *current
		snapshot.ChunkMap = append([]metadata.ChunkRef(nil), current.ChunkMap...)
		concurrentSnapshot = &snapshot
		return nil
	}
	_, err = newMetadataObjectCommitter(tracked, chunkStore, false).Put(ctx, PutObjectRequest{
		Bucket:        "photos",
		Key:           "a.txt",
		Body:          strings.NewReader("current"),
		ContentLength: 7,
	})
	if !errors.Is(err, ErrObjectLocked) {
		t.Fatalf("Put err = %v, want %v", err, ErrObjectLocked)
	}
	if concurrentSnapshot == nil {
		t.Fatal("advisory lock hook did not install concurrent inode version")
	}

	got, err := store.Lookup(ctx, root, "a.txt")
	if err != nil {
		t.Fatalf("Lookup after rejection: %v", err)
	}
	if got.ID != concurrentSnapshot.ID || got.Size != concurrentSnapshot.Size {
		t.Fatalf("inode after rejection = (id=%d size=%d), want post-lock version (id=%d size=%d)",
			got.ID, got.Size, concurrentSnapshot.ID, concurrentSnapshot.Size)
	}
	if !equalChunkRefs(got.ChunkMap, concurrentSnapshot.ChunkMap) {
		t.Fatalf("chunks after rejection = %+v, want post-lock version %+v", got.ChunkMap, concurrentSnapshot.ChunkMap)
	}
	for _, ref := range concurrentSnapshot.ChunkMap {
		if _, err := store.GetChunk(ctx, ref.ID); err != nil {
			t.Fatalf("post-lock chunk %d was not preserved: %v", ref.ID, err)
		}
	}
	if tracked.allocateChunkCalls != 0 || tracked.allocateBatchCalls != 0 {
		t.Fatalf("version-changed write allocated chunks: single=%d batch=%d",
			tracked.allocateChunkCalls, tracked.allocateBatchCalls)
	}
}

func TestObjectCommitterFailsIfKeyChangesInodeWhileAcquiringLock(t *testing.T) {
	ctx := context.Background()
	store := newTestMetadataWithQuota(t)
	createQuotaTestBucket(t, store, "photos")
	committer := newMetadataObjectCommitter(store, chunkstore.NewMemoryChunkStore(), false)
	putQuotaTestObject(t, committer, "photos", "a.txt", "old", 3)

	root := quotaBucketRoot(t, store, "photos")
	oldInode, err := store.Lookup(ctx, root, "a.txt")
	if err != nil {
		t.Fatalf("Lookup old object: %v", err)
	}
	var replacement *metadata.InodeMeta
	tracked := &quotaTrackingMetadata{PebbleStore: store}
	tracked.advisoryLockHook = func(ctx context.Context, _ metadata.InodeID) error {
		if err := store.Unlink(ctx, root, "a.txt"); err != nil {
			return err
		}
		var err error
		replacement, err = store.CreateFile(ctx, root, "a.txt", 0644)
		return err
	}

	_, err = newMetadataObjectCommitter(tracked, chunkstore.NewMemoryChunkStore(), false).Put(ctx, PutObjectRequest{
		Bucket:        "photos",
		Key:           "a.txt",
		Body:          strings.NewReader(""),
		ContentLength: 0,
	})
	if !errors.Is(err, ErrObjectLocked) {
		t.Fatalf("Put err = %v, want %v", err, ErrObjectLocked)
	}
	if replacement == nil || replacement.ID != oldInode.ID {
		t.Fatalf("replacement inode = %+v, want reused ID %d", replacement, oldInode.ID)
	}
	if replacement.CTime == oldInode.CTime {
		t.Fatalf("replacement CTime = %d, want different from %d", replacement.CTime, oldInode.CTime)
	}
	got, err := store.Lookup(ctx, root, "a.txt")
	if err != nil {
		t.Fatalf("Lookup replacement: %v", err)
	}
	if got.ID != replacement.ID || got.CTime != replacement.CTime || got.Size != replacement.Size || !equalChunkRefs(got.ChunkMap, replacement.ChunkMap) {
		t.Fatalf("replacement changed = %+v, want %+v", got, replacement)
	}
	if tracked.allocateChunkCalls != 0 || tracked.allocateBatchCalls != 0 {
		t.Fatalf("replacement write allocated chunks: single=%d batch=%d", tracked.allocateChunkCalls, tracked.allocateBatchCalls)
	}
}

func TestObjectCommitterNewObjectLockFailureDefersCleanupToRecovery(t *testing.T) {
	ctx := context.Background()
	store := newTestMetadataWithQuota(t)
	createQuotaTestBucket(t, store, "photos")
	root := quotaBucketRoot(t, store, "photos")
	tracked := &quotaTrackingMetadata{PebbleStore: store}
	tracked.advisoryLockHook = func(context.Context, metadata.InodeID) error {
		return metadata.ErrLockBusy
	}

	_, err := newMetadataObjectCommitter(tracked, chunkstore.NewMemoryChunkStore(), false).Put(ctx, PutObjectRequest{
		Bucket:        "photos",
		Key:           "ghost.txt",
		Body:          strings.NewReader(""),
		ContentLength: 0,
	})
	if !errors.Is(err, ErrObjectLocked) {
		t.Fatalf("Put err = %v, want %v", err, ErrObjectLocked)
	}
	ghost, err := store.Lookup(ctx, root, "ghost.txt")
	if err != nil {
		t.Fatalf("Lookup deferred-cleanup object: %v", err)
	}
	assertQuotaUsage(t, store, "photos", 0, 1)

	recovery := quotaAttemptsInState(t, store, metadata.WriteAttemptRecoveryNeeded)
	if len(recovery) != 1 {
		t.Fatalf("recovery attempts = %+v, want one", recovery)
	}
	plan := recovery[0]
	if plan.RecoveryIntent != metadata.WriteAttemptRecoveryCleanup ||
		plan.InodeID != ghost.ID ||
		plan.InodeCTime != ghost.CTime ||
		plan.CleanupParent != root ||
		!plan.CleanupNewObject ||
		len(plan.Chunks) != 0 {
		t.Fatalf("cleanup plan = %+v, want exact new-object identity and no allocations", plan)
	}

	result, err := NewObjectWriteRecoveryWorker(tracked).RecoverOnce(ctx, 10)
	if err != nil {
		t.Fatalf("RecoverOnce: %v", err)
	}
	if result.Cleaned != 1 || result.Committed != 0 || result.Failed != 0 {
		t.Fatalf("recovery result = %+v, want one cleanup only", result)
	}
	if _, err := store.Lookup(ctx, root, "ghost.txt"); !errors.Is(err, metadata.ErrEntryNotFound) {
		t.Fatalf("Lookup recovered ghost = %v, want %v", err, metadata.ErrEntryNotFound)
	}
	assertQuotaUsage(t, store, "photos", 0, 0)
}

func TestObjectCommitterNewObjectLockFailureJoinsCleanupPlanPersistenceError(t *testing.T) {
	ctx := context.Background()
	store := newTestMetadataWithQuota(t)
	createQuotaTestBucket(t, store, "photos")
	persistErr := errors.New("injected cleanup plan persistence failure")
	tracked := &quotaTrackingMetadata{PebbleStore: store}
	tracked.advisoryLockHook = func(context.Context, metadata.InodeID) error {
		return metadata.ErrLockBusy
	}
	tracked.putWriteAttemptHook = func(_ context.Context, attempt *metadata.ObjectWriteAttempt) error {
		if attempt.State == metadata.WriteAttemptRecoveryNeeded &&
			attempt.RecoveryIntent == metadata.WriteAttemptRecoveryCleanup {
			return persistErr
		}
		return nil
	}

	_, err := newMetadataObjectCommitter(tracked, chunkstore.NewMemoryChunkStore(), false).Put(ctx, PutObjectRequest{
		Bucket:        "photos",
		Key:           "ghost.txt",
		Body:          strings.NewReader(""),
		ContentLength: 0,
	})
	if !errors.Is(err, ErrObjectLocked) || !errors.Is(err, persistErr) {
		t.Fatalf("Put err = %v, want joined %v and %v", err, ErrObjectLocked, persistErr)
	}
}

func TestObjectCommitterNewObjectPostLockLookupFailureCleansGhost(t *testing.T) {
	ctx := context.Background()
	store := newTestMetadataWithQuota(t)
	createQuotaTestBucket(t, store, "photos")
	root := quotaBucketRoot(t, store, "photos")
	lookupErr := errors.New("injected post-lock lookup failure")
	lookupCalls := 0
	tracked := &quotaTrackingMetadata{PebbleStore: store}
	tracked.lookupHook = func(ctx context.Context, parent metadata.InodeID, name string) (*metadata.InodeMeta, error) {
		lookupCalls++
		if lookupCalls == 2 {
			return nil, lookupErr
		}
		return store.Lookup(ctx, parent, name)
	}

	_, err := newMetadataObjectCommitter(tracked, chunkstore.NewMemoryChunkStore(), false).Put(ctx, PutObjectRequest{
		Bucket:        "photos",
		Key:           "ghost.txt",
		Body:          strings.NewReader(""),
		ContentLength: 0,
	})
	if !errors.Is(err, ErrObjectMetadataFailed) {
		t.Fatalf("Put err = %v, want %v", err, ErrObjectMetadataFailed)
	}
	if _, err := store.Lookup(ctx, root, "ghost.txt"); !errors.Is(err, metadata.ErrEntryNotFound) {
		t.Fatalf("Lookup failed object = %v, want %v", err, metadata.ErrEntryNotFound)
	}
	assertQuotaUsage(t, store, "photos", 0, 0)

	failed := quotaAttemptsInState(t, store, metadata.WriteAttemptFailed)
	if len(failed) != 1 ||
		failed[0].RecoveryIntent != metadata.WriteAttemptRecoveryCleanup ||
		!failed[0].CleanupNewObject ||
		len(failed[0].Chunks) != 0 {
		t.Fatalf("failed attempts = %+v, want completed cleanup plan without allocations", failed)
	}
}

func TestObjectCommitterNewObjectPostLockIdentityChangePreservesReplacement(t *testing.T) {
	ctx := context.Background()
	store := newTestMetadataWithQuota(t)
	createQuotaTestBucket(t, store, "photos")
	root := quotaBucketRoot(t, store, "photos")
	tracked := &quotaTrackingMetadata{PebbleStore: store}
	var created *metadata.InodeMeta
	var replacement *metadata.InodeMeta
	tracked.advisoryLockHook = func(ctx context.Context, _ metadata.InodeID) error {
		created = cloneInodeMeta(tracked.createdInode)
		if err := store.Unlink(ctx, root, "raced.txt"); err != nil {
			return err
		}
		var err error
		replacement, err = store.CreateFile(ctx, root, "raced.txt", 0600)
		return err
	}

	_, err := newMetadataObjectCommitter(tracked, chunkstore.NewMemoryChunkStore(), false).Put(ctx, PutObjectRequest{
		Bucket:        "photos",
		Key:           "raced.txt",
		Body:          strings.NewReader(""),
		ContentLength: 0,
	})
	if !errors.Is(err, ErrObjectLocked) {
		t.Fatalf("Put err = %v, want %v", err, ErrObjectLocked)
	}
	if created == nil || replacement == nil ||
		replacement.ID != created.ID ||
		replacement.CTime == created.CTime {
		t.Fatalf("created=%+v replacement=%+v, want reused ID with a new CTime", created, replacement)
	}
	got, err := store.Lookup(ctx, root, "raced.txt")
	if err != nil {
		t.Fatalf("Lookup replacement: %v", err)
	}
	if got.ID != replacement.ID || got.CTime != replacement.CTime || got.Mode != replacement.Mode {
		t.Fatalf("replacement changed = %+v, want %+v", got, replacement)
	}
	assertQuotaUsage(t, store, "photos", 0, 1)

	failed := quotaAttemptsInState(t, store, metadata.WriteAttemptFailed)
	if len(failed) != 1 ||
		failed[0].RecoveryIntent != metadata.WriteAttemptRecoveryCleanup ||
		!failed[0].CleanupNewObject ||
		len(failed[0].Chunks) != 0 {
		t.Fatalf("failed attempts = %+v, want idempotent completed cleanup state", failed)
	}
}

func TestObjectCommitterQuotaCleanupIdentityMismatchLeavesReplacement(t *testing.T) {
	ctx := context.Background()
	store := newTestMetadataWithQuota(t)
	createQuotaTestBucket(t, store, "photos")
	root := quotaBucketRoot(t, store, "photos")
	tracked := &quotaTrackingMetadata{PebbleStore: store}
	lateQuotaErr := fmt.Errorf("%w: injected late quota rejection", metadata.ErrQuotaExceeded)
	var rejectedIdentity *metadata.InodeMeta
	var replacement *metadata.InodeMeta
	tracked.checkBucketQuota = func(ctx context.Context, bucket string, additionalBytes, additionalObjects int64) error {
		if tracked.commitChunkCalls == 0 {
			return store.CheckBucketQuota(ctx, bucket, additionalBytes, additionalObjects)
		}
		var err error
		rejectedIdentity, err = store.Lookup(ctx, root, "a.txt")
		if err != nil {
			return err
		}
		if err := store.Unlink(ctx, root, "a.txt"); err != nil {
			return err
		}
		replacement, err = store.CreateFile(ctx, root, "a.txt", 0600)
		if err != nil {
			return err
		}
		return lateQuotaErr
	}

	_, err := newMetadataObjectCommitter(tracked, chunkstore.NewMemoryChunkStore(), false).Put(ctx, PutObjectRequest{
		Bucket:        "photos",
		Key:           "a.txt",
		Body:          strings.NewReader("rejected"),
		ContentLength: 8,
	})
	if !errors.Is(err, ErrObjectQuotaExceeded) {
		t.Fatalf("Put err = %v, want %v", err, ErrObjectQuotaExceeded)
	}
	if rejectedIdentity == nil || replacement == nil {
		t.Fatalf("identities not captured: rejected=%+v replacement=%+v", rejectedIdentity, replacement)
	}
	if replacement.ID != rejectedIdentity.ID || replacement.CTime == rejectedIdentity.CTime {
		t.Fatalf("replacement identity = (%d, %d), want reused ID %d with CTime different from %d",
			replacement.ID, replacement.CTime, rejectedIdentity.ID, rejectedIdentity.CTime)
	}
	got, err := store.Lookup(ctx, root, "a.txt")
	if err != nil {
		t.Fatalf("Lookup replacement: %v", err)
	}
	if got.ID != replacement.ID || got.CTime != replacement.CTime || got.Mode != replacement.Mode ||
		got.Size != replacement.Size || !equalChunkRefs(got.ChunkMap, replacement.ChunkMap) {
		t.Fatalf("replacement changed = %+v, want %+v", got, replacement)
	}
	failed := quotaAttemptsInState(t, store, metadata.WriteAttemptFailed)
	if len(failed) != 1 {
		t.Fatalf("failed attempts = %d, want 1", len(failed))
	}
	for _, ref := range failed[0].Chunks {
		if _, err := store.GetChunk(ctx, ref.ID); err != nil {
			t.Fatalf("GetChunk(%d) after mismatch cleanup = %v, want retained metadata", ref.ID, err)
		}
	}
}

func TestObjectCommitterBatchFinalRejectCleansAllAllocatedChunks(t *testing.T) {
	ctx := context.Background()
	store := newTestMetadataWithQuota(t)
	createQuotaTestBucket(t, store, "photos")
	tracked := &quotaTrackingMetadata{PebbleStore: store}
	lateQuotaErr := fmt.Errorf("%w: injected late quota rejection", metadata.ErrQuotaExceeded)
	tracked.checkBucketQuota = func(ctx context.Context, bucket string, additionalBytes, additionalObjects int64) error {
		if tracked.commitChunkCalls > 0 {
			return lateQuotaErr
		}
		return store.CheckBucketQuota(ctx, bucket, additionalBytes, additionalObjects)
	}
	committer := newMetadataObjectCommitter(tracked, chunkstore.NewMemoryChunkStore(), false)

	_, err := committer.Put(ctx, PutObjectRequest{
		Bucket:        "photos",
		Key:           "a.txt",
		Body:          strings.NewReader("short"),
		ContentLength: metadata.MaxChunkSize + 1,
	})
	if !errors.Is(err, ErrObjectQuotaExceeded) {
		t.Fatalf("Put err = %v, want %v", err, ErrObjectQuotaExceeded)
	}
	if tracked.allocateBatchCalls != 1 {
		t.Fatalf("AllocateChunksBatch calls = %d, want 1", tracked.allocateBatchCalls)
	}
	failed := quotaAttemptsInState(t, store, metadata.WriteAttemptFailed)
	if len(failed) != 1 {
		t.Fatalf("failed attempts = %d, want 1", len(failed))
	}
	if len(failed[0].Chunks) != 2 {
		t.Fatalf("failed attempt chunks = %+v, want evidence for both batch allocations", failed[0].Chunks)
	}
	for _, ref := range failed[0].Chunks {
		if _, err := store.GetChunk(ctx, ref.ID); err != nil {
			t.Fatalf("GetChunk(%d) after rejection = %v, want retained metadata", ref.ID, err)
		}
	}
	if _, err := store.Lookup(ctx, quotaBucketRoot(t, store, "photos"), "a.txt"); !errors.Is(err, metadata.ErrEntryNotFound) {
		t.Fatalf("Lookup rejected object = %v, want %v", err, metadata.ErrEntryNotFound)
	}
}

func TestObjectCommitterBatchShortBodyDeletesUnusedChunksOnSuccess(t *testing.T) {
	ctx := context.Background()
	store := newTestMetadataWithQuota(t)
	createQuotaTestBucket(t, store, "photos")
	tracked := &quotaTrackingMetadata{PebbleStore: store}
	committer := newMetadataObjectCommitter(tracked, chunkstore.NewMemoryChunkStore(), false)

	result, err := committer.Put(ctx, PutObjectRequest{
		Bucket:        "photos",
		Key:           "a.txt",
		Body:          strings.NewReader("short"),
		ContentLength: metadata.MaxChunkSize + 1,
	})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if result.Size != 5 {
		t.Fatalf("Put size = %d, want 5", result.Size)
	}
	if len(tracked.allocatedBatchChunks) != 2 {
		t.Fatalf("batch allocations = %+v, want 2 chunks", tracked.allocatedBatchChunks)
	}
	inode, err := store.Lookup(ctx, quotaBucketRoot(t, store, "photos"), "a.txt")
	if err != nil {
		t.Fatalf("Lookup committed object: %v", err)
	}
	if len(inode.ChunkMap) != 1 || inode.ChunkMap[0].ID != tracked.allocatedBatchChunks[0] {
		t.Fatalf("committed chunks = %+v, want only first batch allocation", inode.ChunkMap)
	}
	if _, err := store.GetChunk(ctx, tracked.allocatedBatchChunks[0]); err != nil {
		t.Fatalf("GetChunk(consumed): %v", err)
	}
	if _, err := store.GetChunk(ctx, tracked.allocatedBatchChunks[1]); err != nil {
		t.Fatalf("GetChunk(unused) = %v, want retained metadata", err)
	}
}

func TestObjectCommitterBatchUnusedCleanupFailureNeedsRecovery(t *testing.T) {
	ctx := context.Background()
	store := newTestMetadataWithQuota(t)
	createQuotaTestBucket(t, store, "photos")
	tracked := &quotaTrackingMetadata{
		PebbleStore:    store,
		deleteChunkErr: errors.New("injected unused chunk cleanup failure"),
	}
	committer := newMetadataObjectCommitter(tracked, chunkstore.NewMemoryChunkStore(), false)

	_, err := committer.Put(ctx, PutObjectRequest{
		Bucket:        "photos",
		Key:           "a.txt",
		Body:          strings.NewReader("short"),
		ContentLength: metadata.MaxChunkSize + 1,
	})
	if !errors.Is(err, ErrObjectMetadataFailed) {
		t.Fatalf("Put err = %v, want %v", err, ErrObjectMetadataFailed)
	}
	recovery := quotaAttemptsInState(t, store, metadata.WriteAttemptRecoveryNeeded)
	if len(recovery) != 1 {
		t.Fatalf("recovery-needed attempts = %d, want 1", len(recovery))
	}
	if len(recovery[0].Chunks) != 2 {
		t.Fatalf("recovery chunks = %+v, want evidence for both batch allocations", recovery[0].Chunks)
	}
	if recovery[0].RecoveryIntent != metadata.WriteAttemptRecoveryCleanup ||
		recovery[0].CleanupParent != quotaBucketRoot(t, store, "photos") ||
		!recovery[0].CleanupNewObject ||
		recovery[0].RollbackInode != nil {
		t.Fatalf("recovery cleanup plan = %+v, want new-object cleanup plan", recovery[0])
	}
	if !strings.Contains(recovery[0].LastError, "unused") {
		t.Fatalf("recovery LastError = %q, want unused cleanup context", recovery[0].LastError)
	}
}

func TestObjectCommitterKnownLengthFinalRejectCleansDurableWrite(t *testing.T) {
	ctx := context.Background()
	store := newTestMetadataWithQuota(t)
	createQuotaTestBucket(t, store, "photos")
	tracked := &quotaTrackingMetadata{PebbleStore: store}
	lateQuotaErr := fmt.Errorf("%w: injected late quota rejection", metadata.ErrQuotaExceeded)
	tracked.checkBucketQuota = func(ctx context.Context, bucket string, additionalBytes, additionalObjects int64) error {
		if tracked.commitChunkCalls > 0 {
			return lateQuotaErr
		}
		return store.CheckBucketQuota(ctx, bucket, additionalBytes, additionalObjects)
	}
	committer := newMetadataObjectCommitter(tracked, chunkstore.NewMemoryChunkStore(), false)

	_, err := committer.Put(ctx, PutObjectRequest{
		Bucket:        "photos",
		Key:           "a.txt",
		Body:          strings.NewReader("known"),
		ContentLength: 5,
	})
	if !errors.Is(err, ErrObjectQuotaExceeded) {
		t.Fatalf("Put err = %v, want %v", err, ErrObjectQuotaExceeded)
	}
	if _, err := store.Lookup(ctx, quotaBucketRoot(t, store, "photos"), "a.txt"); !errors.Is(err, metadata.ErrEntryNotFound) {
		t.Fatalf("Lookup rejected object = %v, want %v", err, metadata.ErrEntryNotFound)
	}
	failed := quotaAttemptsInState(t, store, metadata.WriteAttemptFailed)
	if len(failed) != 1 || len(failed[0].Chunks) != 1 {
		t.Fatalf("failed attempts = %+v, want one attempt with one durable chunk", failed)
	}
	if _, err := store.GetChunk(ctx, failed[0].Chunks[0].ID); err != nil {
		t.Fatalf("GetChunk(%d) after rejection = %v, want retained metadata",
			failed[0].Chunks[0].ID, err)
	}
}

func TestObjectCommitterQuotaCleanupFailureNeedsRecovery(t *testing.T) {
	ctx := context.Background()
	store := newTestMetadataWithQuota(t)
	createQuotaTestBucket(t, store, "photos")
	if err := store.SetBucketQuota(ctx, "photos", &metadata.BucketQuota{MaxSizeBytes: 3}); err != nil {
		t.Fatalf("SetBucketQuota: %v", err)
	}
	tracked := &quotaTrackingMetadata{
		PebbleStore:    store,
		deleteChunkErr: errors.New("injected chunk cleanup failure"),
	}
	committer := newMetadataObjectCommitter(tracked, chunkstore.NewMemoryChunkStore(), false)

	_, err := committer.Put(ctx, PutObjectRequest{
		Bucket:        "photos",
		Key:           "a.txt",
		Body:          strings.NewReader("1234"),
		ContentLength: -1,
	})
	if !errors.Is(err, ErrObjectQuotaExceeded) {
		t.Fatalf("Put err = %v, want %v", err, ErrObjectQuotaExceeded)
	}
	recovery := quotaAttemptsInState(t, store, metadata.WriteAttemptRecoveryNeeded)
	if len(recovery) != 1 {
		t.Fatalf("recovery-needed attempts = %d, want 1", len(recovery))
	}
	attempt := recovery[0]
	if attempt.RecoveryIntent != metadata.WriteAttemptRecoveryCleanup {
		t.Fatalf("RecoveryIntent = %q, want %q", attempt.RecoveryIntent, metadata.WriteAttemptRecoveryCleanup)
	}
	if tracked.createdInode == nil {
		t.Fatal("CreateFile identity was not captured")
	}
	if attempt.InodeID != tracked.createdInode.ID || attempt.InodeCTime != tracked.createdInode.CTime {
		t.Fatalf("cleanup identity = (%d, %d), want (%d, %d)",
			attempt.InodeID, attempt.InodeCTime, tracked.createdInode.ID, tracked.createdInode.CTime)
	}
	if attempt.CleanupParent != quotaBucketRoot(t, store, "photos") {
		t.Fatalf("CleanupParent = %d, want bucket root", attempt.CleanupParent)
	}
	if !attempt.CleanupNewObject {
		t.Fatal("CleanupNewObject = false, want true")
	}
	if attempt.RollbackInode != nil {
		t.Fatalf("RollbackInode = %+v, want nil for new object", attempt.RollbackInode)
	}
	if len(attempt.Chunks) != tracked.allocateChunkCalls {
		t.Fatalf("cleanup chunks = %+v, want all %d allocations", attempt.Chunks, tracked.allocateChunkCalls)
	}
	if !strings.Contains(attempt.LastError, "cleanup") {
		t.Fatalf("recovery LastError = %q, want cleanup context", attempt.LastError)
	}
}

func TestObjectCommitterOverwriteCleanupFailureRecordsDeepRollbackPlan(t *testing.T) {
	ctx := context.Background()
	store := newTestMetadataWithQuota(t)
	createQuotaTestBucket(t, store, "photos")
	chunkStore := chunkstore.NewMemoryChunkStore()
	committer := newMetadataObjectCommitter(store, chunkStore, false)
	putQuotaTestObject(t, committer, "photos", "a.txt", "old", 3)

	root := quotaBucketRoot(t, store, "photos")
	oldInode, err := store.Lookup(ctx, root, "a.txt")
	if err != nil {
		t.Fatalf("Lookup old object: %v", err)
	}
	oldInode.XAttrs = map[string][]byte{"user.test": []byte("original")}
	if err := store.UpdateInode(ctx, oldInode); err != nil {
		t.Fatalf("UpdateInode xattrs: %v", err)
	}
	wantRollback := cloneInodeMeta(oldInode)

	tracked := &quotaTrackingMetadata{
		PebbleStore:    store,
		updateInodeErr: errors.New("injected inode rollback failure"),
	}
	lateQuotaErr := fmt.Errorf("%w: injected late quota rejection", metadata.ErrQuotaExceeded)
	tracked.checkBucketQuota = func(ctx context.Context, bucket string, additionalBytes, additionalObjects int64) error {
		if tracked.commitChunkCalls > 0 {
			return lateQuotaErr
		}
		return store.CheckBucketQuota(ctx, bucket, additionalBytes, additionalObjects)
	}

	_, err = newMetadataObjectCommitter(tracked, chunkStore, false).Put(ctx, PutObjectRequest{
		Bucket:        "photos",
		Key:           "a.txt",
		Body:          strings.NewReader("replacement"),
		ContentLength: 11,
	})
	if !errors.Is(err, ErrObjectQuotaExceeded) {
		t.Fatalf("Put err = %v, want %v", err, ErrObjectQuotaExceeded)
	}
	recovery := quotaAttemptsInState(t, store, metadata.WriteAttemptRecoveryNeeded)
	if len(recovery) != 1 {
		t.Fatalf("recovery-needed attempts = %d, want 1", len(recovery))
	}
	attempt := recovery[0]
	if attempt.RecoveryIntent != metadata.WriteAttemptRecoveryCleanup {
		t.Fatalf("RecoveryIntent = %q, want %q", attempt.RecoveryIntent, metadata.WriteAttemptRecoveryCleanup)
	}
	if attempt.InodeID != oldInode.ID || attempt.InodeCTime != oldInode.CTime {
		t.Fatalf("cleanup identity = (%d, %d), want (%d, %d)",
			attempt.InodeID, attempt.InodeCTime, oldInode.ID, oldInode.CTime)
	}
	if attempt.CleanupParent != root || attempt.CleanupNewObject {
		t.Fatalf("cleanup target = (parent=%d new=%v), want (parent=%d new=false)",
			attempt.CleanupParent, attempt.CleanupNewObject, root)
	}
	if attempt.RollbackInode == nil {
		t.Fatal("RollbackInode = nil, want overwrite snapshot")
	}
	if attempt.RollbackInode == oldInode ||
		&attempt.RollbackInode.ChunkMap[0] == &oldInode.ChunkMap[0] ||
		&attempt.RollbackInode.XAttrs["user.test"][0] == &oldInode.XAttrs["user.test"][0] {
		t.Fatal("RollbackInode aliases the source inode")
	}
	if attempt.RollbackInode.ID != wantRollback.ID || attempt.RollbackInode.CTime != wantRollback.CTime ||
		attempt.RollbackInode.Size != wantRollback.Size ||
		!equalChunkRefs(attempt.RollbackInode.ChunkMap, wantRollback.ChunkMap) ||
		string(attempt.RollbackInode.XAttrs["user.test"]) != "original" {
		t.Fatalf("RollbackInode = %+v, want deep snapshot %+v", attempt.RollbackInode, wantRollback)
	}
	if len(attempt.Chunks) != tracked.allocateChunkCalls {
		t.Fatalf("cleanup chunks = %+v, want all %d allocations", attempt.Chunks, tracked.allocateChunkCalls)
	}
}

func TestObjectCommitterQuotaCleanupFailureRecoversWithoutCommit(t *testing.T) {
	ctx := context.Background()
	store := newTestMetadataWithQuota(t)
	createQuotaTestBucket(t, store, "photos")
	root := quotaBucketRoot(t, store, "photos")
	tracked := &quotaTrackingMetadata{
		PebbleStore:    store,
		deleteChunkErr: errors.New("injected immediate cleanup failure"),
	}
	lateQuotaErr := fmt.Errorf("%w: injected late quota rejection", metadata.ErrQuotaExceeded)
	tracked.checkBucketQuota = func(ctx context.Context, bucket string, additionalBytes, additionalObjects int64) error {
		if tracked.commitChunkCalls > 0 {
			return lateQuotaErr
		}
		return store.CheckBucketQuota(ctx, bucket, additionalBytes, additionalObjects)
	}

	_, err := newMetadataObjectCommitter(tracked, chunkstore.NewMemoryChunkStore(), false).Put(ctx, PutObjectRequest{
		Bucket:        "photos",
		Key:           "a.txt",
		Body:          strings.NewReader("rejected"),
		ContentLength: 8,
	})
	if !errors.Is(err, ErrObjectQuotaExceeded) {
		t.Fatalf("Put err = %v, want %v", err, ErrObjectQuotaExceeded)
	}
	recovery := quotaAttemptsInState(t, store, metadata.WriteAttemptRecoveryNeeded)
	if len(recovery) != 1 {
		t.Fatalf("recovery-needed attempts = %d, want 1", len(recovery))
	}
	rejectedChunks := append([]metadata.ChunkRef(nil), recovery[0].Chunks...)
	tracked.deleteChunkErr = nil

	result, err := NewObjectWriteRecoveryWorker(tracked).RecoverOnce(ctx, 10)
	if err != nil {
		t.Fatalf("RecoverOnce: %v", err)
	}
	if result.Committed != 0 || result.Cleaned != 1 || result.Failed != 0 {
		t.Fatalf("recovery result = %+v, want one cleanup and no commit/failure", result)
	}
	if _, err := store.Lookup(ctx, root, "a.txt"); !errors.Is(err, metadata.ErrEntryNotFound) {
		t.Fatalf("Lookup rejected object = %v, want %v", err, metadata.ErrEntryNotFound)
	}
	for _, ref := range rejectedChunks {
		if _, err := store.GetChunk(ctx, ref.ID); err != nil {
			t.Fatalf("GetChunk(%d) after recovery = %v, want retained metadata", ref.ID, err)
		}
	}
	failed := quotaAttemptsInState(t, store, metadata.WriteAttemptFailed)
	if len(failed) != 1 || failed[0].ID != recovery[0].ID {
		t.Fatalf("failed attempts = %+v, want recovered attempt %q", failed, recovery[0].ID)
	}
	if len(failed[0].Chunks) == 0 {
		t.Fatal("recovered attempt lost cleanup evidence")
	}
}

func TestObjectCommitterQuotaRejectionRecordsFailedAttempt(t *testing.T) {
	ctx := context.Background()
	store := newTestMetadataWithQuota(t)
	createQuotaTestBucket(t, store, "photos")
	if err := store.SetBucketQuota(ctx, "photos", &metadata.BucketQuota{MaxObjects: 1}); err != nil {
		t.Fatalf("SetBucketQuota: %v", err)
	}
	committer := newMetadataObjectCommitter(store, chunkstore.NewMemoryChunkStore(), false)
	putQuotaTestObject(t, committer, "photos", "a.txt", "a", 1)

	_, err := committer.Put(ctx, PutObjectRequest{
		Bucket:        "photos",
		Key:           "b.txt",
		Body:          strings.NewReader("b"),
		ContentLength: 1,
	})
	if !errors.Is(err, ErrObjectQuotaExceeded) {
		t.Fatalf("Put err = %v, want %v", err, ErrObjectQuotaExceeded)
	}
	failed := quotaAttemptsInState(t, store, metadata.WriteAttemptFailed)
	if len(failed) != 1 {
		t.Fatalf("failed attempts = %d, want 1", len(failed))
	}
	if failed[0].InodeID != 0 || len(failed[0].Chunks) != 0 {
		t.Fatalf("rejected attempt created resources: inode=%d chunks=%+v", failed[0].InodeID, failed[0].Chunks)
	}
	if !strings.Contains(failed[0].LastError, "quota") {
		t.Fatalf("failed LastError = %q, want quota context", failed[0].LastError)
	}
	if got := quotaAttemptsInState(t, store, metadata.WriteAttemptRecoveryNeeded); len(got) != 0 {
		t.Fatalf("recovery-needed attempts = %d, want 0", len(got))
	}
}

type quotaTrackingMetadata struct {
	*metadata.PebbleStore
	createFileCalls      int
	createdInode         *metadata.InodeMeta
	allocateChunkCalls   int
	allocateBatchCalls   int
	commitChunkCalls     int
	allocatedBatchChunks []metadata.ChunkID
	emptyReplicas        bool
	commitChunkErr       error
	deleteChunkErr       error
	deleteChunkHook      func(metadata.ChunkID)
	updateInodeErr       error
	advisoryLockHook     func(context.Context, metadata.InodeID) error
	afterAdvisoryLock    func()
	afterSharedLock      func()
	afterUnlink          func()
	lookupHook           func(context.Context, metadata.InodeID, string) (*metadata.InodeMeta, error)
	checkBucketQuota     func(context.Context, string, int64, int64) error
	putWriteAttemptHook  func(context.Context, *metadata.ObjectWriteAttempt) error
}

func (m *quotaTrackingMetadata) CreateFile(ctx context.Context, parent metadata.InodeID, name string, mode uint32) (*metadata.InodeMeta, error) {
	m.createFileCalls++
	inode, err := m.PebbleStore.CreateFile(ctx, parent, name, mode)
	if err == nil {
		m.createdInode = cloneInodeMeta(inode)
	}
	return inode, err
}

func (m *quotaTrackingMetadata) Lookup(ctx context.Context, parent metadata.InodeID, name string) (*metadata.InodeMeta, error) {
	if m.lookupHook != nil {
		return m.lookupHook(ctx, parent, name)
	}
	return m.PebbleStore.Lookup(ctx, parent, name)
}

func (m *quotaTrackingMetadata) AllocateChunk(ctx context.Context, inodeID metadata.InodeID, offset int64, policy metadata.PlacementPolicy) (*metadata.ChunkMeta, error) {
	m.allocateChunkCalls++
	chunk, err := m.PebbleStore.AllocateChunk(ctx, inodeID, offset, policy)
	if err == nil && m.emptyReplicas {
		chunk.Replicas = nil
	}
	return chunk, err
}

func (m *quotaTrackingMetadata) AllocateChunksBatch(ctx context.Context, inodeID metadata.InodeID, offsets []int64, policy metadata.PlacementPolicy) ([]*metadata.ChunkMeta, error) {
	m.allocateBatchCalls++
	chunks, err := m.PebbleStore.AllocateChunksBatch(ctx, inodeID, offsets, policy)
	if err != nil {
		return nil, err
	}
	for _, chunk := range chunks {
		m.allocatedBatchChunks = append(m.allocatedBatchChunks, chunk.ID)
		if m.emptyReplicas {
			chunk.Replicas = nil
		}
	}
	return chunks, nil
}

func (m *quotaTrackingMetadata) CommitChunk(ctx context.Context, chunkID metadata.ChunkID, checksum uint32) error {
	if m.commitChunkErr != nil {
		return m.commitChunkErr
	}
	if err := m.PebbleStore.CommitChunk(ctx, chunkID, checksum); err != nil {
		return err
	}
	m.commitChunkCalls++
	return nil
}

func (m *quotaTrackingMetadata) DeleteChunk(ctx context.Context, chunkID metadata.ChunkID) error {
	if m.deleteChunkHook != nil {
		m.deleteChunkHook(chunkID)
	}
	if m.deleteChunkErr != nil {
		return m.deleteChunkErr
	}
	return m.PebbleStore.DeleteChunk(ctx, chunkID)
}

func (m *quotaTrackingMetadata) UpdateInode(ctx context.Context, inode *metadata.InodeMeta) error {
	if m.updateInodeErr != nil {
		return m.updateInodeErr
	}
	return m.PebbleStore.UpdateInode(ctx, inode)
}

func (m *quotaTrackingMetadata) AdvisoryLock(ctx context.Context, inodeID metadata.InodeID, owner string) error {
	if hook := m.advisoryLockHook; hook != nil {
		m.advisoryLockHook = nil
		if err := hook(ctx, inodeID); err != nil {
			return err
		}
	}
	if err := m.PebbleStore.AdvisoryLock(ctx, inodeID, owner); err != nil {
		return err
	}
	if m.afterAdvisoryLock != nil {
		m.afterAdvisoryLock()
	}
	return nil
}

func (m *quotaTrackingMetadata) AdvisoryLockShared(ctx context.Context, inodeID metadata.InodeID, owner string) error {
	if err := m.PebbleStore.AdvisoryLockShared(ctx, inodeID, owner); err != nil {
		return err
	}
	if m.afterSharedLock != nil {
		m.afterSharedLock()
	}
	return nil
}

func (m *quotaTrackingMetadata) Unlink(ctx context.Context, parent metadata.InodeID, name string) error {
	if err := m.PebbleStore.Unlink(ctx, parent, name); err != nil {
		return err
	}
	if m.afterUnlink != nil {
		m.afterUnlink()
	}
	return nil
}

func (m *quotaTrackingMetadata) CheckBucketQuota(ctx context.Context, bucket string, additionalBytes, additionalObjects int64) error {
	if m.checkBucketQuota != nil {
		return m.checkBucketQuota(ctx, bucket, additionalBytes, additionalObjects)
	}
	return m.PebbleStore.CheckBucketQuota(ctx, bucket, additionalBytes, additionalObjects)
}

func (m *quotaTrackingMetadata) PutWriteAttempt(ctx context.Context, attempt *metadata.ObjectWriteAttempt) error {
	if m.putWriteAttemptHook != nil {
		if err := m.putWriteAttemptHook(ctx, attempt); err != nil {
			return err
		}
	}
	return m.PebbleStore.PutWriteAttempt(ctx, attempt)
}

type controlledChunkStore struct {
	chunkstore.ChunkStore
	writeErr   error
	afterWrite func()
}

func (s *controlledChunkStore) WriteChunk(_ context.Context, _ *metadata.ChunkMeta, _ []byte) error {
	if s.afterWrite != nil {
		s.afterWrite()
	}
	return s.writeErr
}

type errorReader struct {
	err error
}

func (r errorReader) Read([]byte) (int, error) {
	return 0, r.err
}

type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	clear(p)
	return len(p), nil
}

func newTestMetadataWithQuota(t *testing.T) *metadata.PebbleStore {
	t.Helper()
	store, err := metadata.NewPebbleStore(metadata.PebbleStoreConfig{
		Dir:            t.TempDir(),
		UseInMemory:    true,
		NodeID:         1,
		UseBucketStats: true,
	})
	if err != nil {
		t.Fatalf("NewPebbleStore: %v", err)
	}
	store.SetQuotaManager(metadata.NewQuotaManager())
	if err := store.RegisterNode(context.Background(), &metadata.NodeInfo{
		ID:    1,
		Addr:  "127.0.0.1:9001",
		State: metadata.NodeOnline,
	}); err != nil {
		_ = store.Close()
		t.Fatalf("RegisterNode: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func createQuotaTestBucket(t *testing.T, store *metadata.PebbleStore, bucket string) {
	t.Helper()
	if err := store.CreateBucket(context.Background(), bucket, metadata.PlacementPolicy{ReplicationFactor: 1}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
}

func quotaBucketRoot(t *testing.T, store *metadata.PebbleStore, bucket string) metadata.InodeID {
	t.Helper()
	info, err := store.GetBucket(context.Background(), bucket)
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}
	return info.RootInode
}

func putQuotaTestObject(t *testing.T, committer *metadataObjectCommitter, bucket, key, body string, contentLength int64) {
	t.Helper()
	if _, err := committer.Put(context.Background(), PutObjectRequest{
		Bucket:        bucket,
		Key:           key,
		Body:          bytes.NewBufferString(body),
		ContentLength: contentLength,
	}); err != nil {
		t.Fatalf("Put %s/%s: %v", bucket, key, err)
	}
}

func assertQuotaUsage(t *testing.T, store *metadata.PebbleStore, bucket string, bytes int64, objects int) {
	t.Helper()
	usage, err := store.GetBucketUsage(context.Background(), bucket)
	if err != nil {
		t.Fatalf("GetBucketUsage: %v", err)
	}
	if usage.UsedBytes != bytes || usage.Objects != objects {
		t.Fatalf("usage = (bytes=%d objects=%d), want (bytes=%d objects=%d)", usage.UsedBytes, usage.Objects, bytes, objects)
	}
}

func quotaAttemptsInState(t *testing.T, store *metadata.PebbleStore, state metadata.WriteAttemptState) []metadata.ObjectWriteAttempt {
	t.Helper()
	attempts, err := store.ListWriteAttemptsByState(context.Background(), state, 100)
	if err != nil {
		t.Fatalf("ListWriteAttemptsByState(%s): %v", state, err)
	}
	return attempts
}

func equalChunkRefs(a, b []metadata.ChunkRef) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// Task 1 added BucketQuotaService to MetadataService. Keep the package-wide
// test double compatible; quota behavior in this file uses PebbleStore.
func (m *mockMetaService) GetBucketQuota(context.Context, string) (*metadata.BucketQuota, error) {
	return nil, nil
}

func (m *mockMetaService) SetBucketQuota(context.Context, string, *metadata.BucketQuota) error {
	return nil
}

func (m *mockMetaService) DeleteBucketQuota(context.Context, string) error {
	return nil
}

func (m *mockMetaService) CheckBucketQuota(context.Context, string, int64, int64) error {
	return nil
}

func (m *mockMetaService) GetBucketUsage(ctx context.Context, bucket string) (*metadata.BucketUsage, error) {
	info, err := m.GetBucket(ctx, bucket)
	if err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	usedBytes, objects := m.mockBucketUsage(info.RootInode)
	return &metadata.BucketUsage{Name: bucket, UsedBytes: usedBytes, Objects: objects}, nil
}

func TestObjectCommitterAllocationFailuresUseCleanupIntent(t *testing.T) {
	tests := []struct {
		name        string
		configure   func(*quotaTrackingMetadata, *controlledChunkStore)
		rejectEmpty bool
		wantErr     error
	}{
		{
			name: "empty replicas",
			configure: func(meta *quotaTrackingMetadata, _ *controlledChunkStore) {
				meta.emptyReplicas = true
			},
			rejectEmpty: true,
			wantErr:     ErrObjectNoReplicas,
		},
		{
			name: "write chunk",
			configure: func(_ *quotaTrackingMetadata, chunks *controlledChunkStore) {
				chunks.writeErr = errors.New("injected write failure")
			},
			wantErr: ErrObjectWriteFailed,
		},
		{
			name: "commit chunk",
			configure: func(meta *quotaTrackingMetadata, _ *controlledChunkStore) {
				meta.commitChunkErr = errors.New("injected commit failure")
			},
			wantErr: ErrObjectCommitFailed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			store := newTestMetadataWithQuota(t)
			createQuotaTestBucket(t, store, "photos")
			root := quotaBucketRoot(t, store, "photos")
			tracked := &quotaTrackingMetadata{PebbleStore: store}
			chunks := &controlledChunkStore{}
			tt.configure(tracked, chunks)

			_, err := newMetadataObjectCommitter(tracked, chunks, tt.rejectEmpty).Put(ctx, PutObjectRequest{
				Bucket:        "photos",
				Key:           "failed.txt",
				Body:          strings.NewReader("data"),
				ContentLength: 4,
				MaxObjectSize: DefaultMaxObjectSize,
			})
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Put err = %v, want %v", err, tt.wantErr)
			}
			if _, err := store.Lookup(ctx, root, "failed.txt"); !errors.Is(err, metadata.ErrEntryNotFound) {
				t.Fatalf("Lookup failed object = %v, want %v", err, metadata.ErrEntryNotFound)
			}
			if got := quotaAttemptsInState(t, store, metadata.WriteAttemptRecoveryNeeded); len(got) != 0 {
				t.Fatalf("recovery-needed attempts = %+v, want none after successful cleanup", got)
			}
			failed := quotaAttemptsInState(t, store, metadata.WriteAttemptFailed)
			if len(failed) != 1 {
				t.Fatalf("failed attempts = %d, want 1", len(failed))
			}
			if failed[0].RecoveryIntent != metadata.WriteAttemptRecoveryCleanup || len(failed[0].Chunks) != 1 {
				t.Fatalf("failed cleanup plan = %+v, want one cleanup allocation", failed[0])
			}
			if _, err := store.GetChunk(ctx, failed[0].Chunks[0].ID); err != nil {
				t.Fatalf("GetChunk after cleanup = %v, want retained metadata", err)
			}
		})
	}
}

func TestObjectCommitterBatchBodyReadFailureCleansEveryAllocation(t *testing.T) {
	ctx := context.Background()
	store := newTestMetadataWithQuota(t)
	createQuotaTestBucket(t, store, "photos")
	root := quotaBucketRoot(t, store, "photos")
	tracked := &quotaTrackingMetadata{PebbleStore: store}
	readErr := errors.New("injected body read failure")

	_, err := newMetadataObjectCommitter(tracked, &controlledChunkStore{}, false).Put(ctx, PutObjectRequest{
		Bucket:        "photos",
		Key:           "failed.txt",
		Body:          errorReader{err: readErr},
		ContentLength: metadata.MaxChunkSize + 1,
		MaxObjectSize: DefaultMaxObjectSize,
	})
	if !errors.Is(err, ErrObjectMetadataFailed) {
		t.Fatalf("Put err = %v, want %v", err, ErrObjectMetadataFailed)
	}
	if _, err := store.Lookup(ctx, root, "failed.txt"); !errors.Is(err, metadata.ErrEntryNotFound) {
		t.Fatalf("Lookup failed object = %v, want %v", err, metadata.ErrEntryNotFound)
	}
	failed := quotaAttemptsInState(t, store, metadata.WriteAttemptFailed)
	if len(failed) != 1 || failed[0].RecoveryIntent != metadata.WriteAttemptRecoveryCleanup {
		t.Fatalf("failed attempts = %+v, want cleanup attempt", failed)
	}
	if len(failed[0].Chunks) != 2 {
		t.Fatalf("cleanup allocations = %+v, want both batch allocations", failed[0].Chunks)
	}
	for _, ref := range failed[0].Chunks {
		if _, err := store.GetChunk(ctx, ref.ID); err != nil {
			t.Fatalf("GetChunk(%d) after cleanup = %v, want retained metadata", ref.ID, err)
		}
	}
}

func TestObjectCommitterBatchCommitFailureRecoversByCleanupOnly(t *testing.T) {
	ctx := context.Background()
	store := newTestMetadataWithQuota(t)
	createQuotaTestBucket(t, store, "photos")
	root := quotaBucketRoot(t, store, "photos")
	tracked := &quotaTrackingMetadata{
		PebbleStore:    store,
		commitChunkErr: errors.New("injected commit failure"),
		deleteChunkErr: errors.New("injected cleanup failure"),
	}
	body := io.LimitReader(zeroReader{}, metadata.MaxChunkSize+1)

	_, err := newMetadataObjectCommitter(tracked, &controlledChunkStore{}, false).Put(ctx, PutObjectRequest{
		Bucket:        "photos",
		Key:           "failed.txt",
		Body:          body,
		ContentLength: metadata.MaxChunkSize + 1,
		MaxObjectSize: DefaultMaxObjectSize,
	})
	if !errors.Is(err, ErrObjectCommitFailed) {
		t.Fatalf("Put err = %v, want %v", err, ErrObjectCommitFailed)
	}
	recovery := quotaAttemptsInState(t, store, metadata.WriteAttemptRecoveryNeeded)
	if len(recovery) != 1 {
		t.Fatalf("recovery attempts = %+v, want one", recovery)
	}
	if recovery[0].RecoveryIntent != metadata.WriteAttemptRecoveryCleanup {
		t.Fatalf("RecoveryIntent = %q, want %q", recovery[0].RecoveryIntent, metadata.WriteAttemptRecoveryCleanup)
	}
	if len(recovery[0].Chunks) != 2 || recovery[0].Chunks[1].Length != 0 {
		t.Fatalf("cleanup allocations = %+v, want two refs including unused zero-length ref", recovery[0].Chunks)
	}

	tracked.commitChunkErr = nil
	tracked.deleteChunkErr = nil
	result, err := NewObjectWriteRecoveryWorker(tracked).RecoverOnce(ctx, 10)
	if err != nil {
		t.Fatalf("RecoverOnce: %v", err)
	}
	if result.Cleaned != 1 || result.Committed != 0 {
		t.Fatalf("recovery result = %+v, want cleanup only", result)
	}
	if _, err := store.Lookup(ctx, root, "failed.txt"); !errors.Is(err, metadata.ErrEntryNotFound) {
		t.Fatalf("Lookup failed object = %v, want %v", err, metadata.ErrEntryNotFound)
	}
}

func TestObjectCommitterPersistsCleanupPlanBeforeImmediateCleanup(t *testing.T) {
	ctx := context.Background()
	store := newTestMetadataWithQuota(t)
	createQuotaTestBucket(t, store, "photos")
	var events []string
	tracked := &quotaTrackingMetadata{PebbleStore: store}
	tracked.putWriteAttemptHook = func(_ context.Context, attempt *metadata.ObjectWriteAttempt) error {
		if attempt.State == metadata.WriteAttemptRecoveryNeeded &&
			attempt.RecoveryIntent == metadata.WriteAttemptRecoveryCleanup {
			events = append(events, "plan")
		}
		return nil
	}
	tracked.deleteChunkHook = func(metadata.ChunkID) {
		events = append(events, "delete")
	}

	_, err := newMetadataObjectCommitter(tracked, &controlledChunkStore{
		writeErr: errors.New("injected write failure"),
	}, false).Put(ctx, PutObjectRequest{
		Bucket:        "photos",
		Key:           "failed.txt",
		Body:          strings.NewReader("data"),
		ContentLength: 4,
		MaxObjectSize: DefaultMaxObjectSize,
	})
	if !errors.Is(err, ErrObjectWriteFailed) {
		t.Fatalf("Put err = %v, want %v", err, ErrObjectWriteFailed)
	}
	if got, want := strings.Join(events, ","), "plan,delete"; got != want {
		t.Fatalf("events = %q, want %q", got, want)
	}
}

func TestObjectCommitterCleanupUsesDetachedContextAndReturnsPersistenceError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store := newTestMetadataWithQuota(t)
	createQuotaTestBucket(t, store, "photos")
	root := quotaBucketRoot(t, store, "photos")
	persistErr := errors.New("injected cleanup plan persistence failure")
	var cleanupPlanContextErr error
	tracked := &quotaTrackingMetadata{PebbleStore: store}
	tracked.putWriteAttemptHook = func(planCtx context.Context, attempt *metadata.ObjectWriteAttempt) error {
		if attempt.State == metadata.WriteAttemptRecoveryNeeded &&
			attempt.RecoveryIntent == metadata.WriteAttemptRecoveryCleanup {
			cleanupPlanContextErr = planCtx.Err()
			return persistErr
		}
		return nil
	}

	_, err := newMetadataObjectCommitter(tracked, &controlledChunkStore{
		writeErr:   errors.New("injected write failure"),
		afterWrite: cancel,
	}, false).Put(ctx, PutObjectRequest{
		Bucket:        "photos",
		Key:           "failed.txt",
		Body:          strings.NewReader("data"),
		ContentLength: 4,
		MaxObjectSize: DefaultMaxObjectSize,
	})
	if !errors.Is(err, ErrObjectWriteFailed) || !errors.Is(err, persistErr) {
		t.Fatalf("Put err = %v, want joined %v and %v", err, ErrObjectWriteFailed, persistErr)
	}
	if cleanupPlanContextErr != nil {
		t.Fatalf("cleanup plan context err = %v, want nil", cleanupPlanContextErr)
	}
	if _, err := store.Lookup(context.Background(), root, "failed.txt"); !errors.Is(err, metadata.ErrEntryNotFound) {
		t.Fatalf("Lookup failed object = %v, want %v", err, metadata.ErrEntryNotFound)
	}
}

func TestObjectCommitterPreflightQuotaBackendFailureIsMetadataError(t *testing.T) {
	ctx := context.Background()
	store := newTestMetadataWithQuota(t)
	createQuotaTestBucket(t, store, "photos")
	backendErr := errors.New("injected quota backend failure")
	tracked := &quotaTrackingMetadata{
		PebbleStore: store,
		checkBucketQuota: func(context.Context, string, int64, int64) error {
			return backendErr
		},
	}

	_, err := newMetadataObjectCommitter(tracked, &controlledChunkStore{}, false).Put(ctx, PutObjectRequest{
		Bucket:        "photos",
		Key:           "failed.txt",
		Body:          strings.NewReader("data"),
		ContentLength: 4,
		MaxObjectSize: DefaultMaxObjectSize,
	})
	if !errors.Is(err, ErrObjectMetadataFailed) || errors.Is(err, ErrObjectQuotaExceeded) {
		t.Fatalf("Put err = %v, want metadata failure only", err)
	}
	if tracked.createFileCalls != 0 {
		t.Fatalf("CreateFile calls = %d, want 0", tracked.createFileCalls)
	}
}

func TestObjectCommitterFinalQuotaBackendFailureCleansWriteAsMetadataError(t *testing.T) {
	ctx := context.Background()
	store := newTestMetadataWithQuota(t)
	createQuotaTestBucket(t, store, "photos")
	root := quotaBucketRoot(t, store, "photos")
	backendErr := errors.New("injected final quota backend failure")
	checks := 0
	tracked := &quotaTrackingMetadata{PebbleStore: store}
	tracked.checkBucketQuota = func(context.Context, string, int64, int64) error {
		checks++
		if checks == 3 {
			return backendErr
		}
		return nil
	}

	_, err := newMetadataObjectCommitter(tracked, &controlledChunkStore{}, false).Put(ctx, PutObjectRequest{
		Bucket:        "photos",
		Key:           "failed.txt",
		Body:          strings.NewReader("data"),
		ContentLength: 4,
		MaxObjectSize: DefaultMaxObjectSize,
	})
	if !errors.Is(err, ErrObjectMetadataFailed) || errors.Is(err, ErrObjectQuotaExceeded) {
		t.Fatalf("Put err = %v, want metadata failure only", err)
	}
	if _, err := store.Lookup(ctx, root, "failed.txt"); !errors.Is(err, metadata.ErrEntryNotFound) {
		t.Fatalf("Lookup failed object = %v, want %v", err, metadata.ErrEntryNotFound)
	}
}

func TestObjectCommitterRejectsKnownLengthAboveMaximumBeforeCreatingResources(t *testing.T) {
	ctx := context.Background()
	store := newTestMetadataWithQuota(t)
	createQuotaTestBucket(t, store, "photos")
	tracked := &quotaTrackingMetadata{PebbleStore: store}

	_, err := newMetadataObjectCommitter(tracked, &controlledChunkStore{}, false).Put(ctx, PutObjectRequest{
		Bucket:        "photos",
		Key:           "too-large.txt",
		Body:          strings.NewReader("data"),
		ContentLength: 4,
		MaxObjectSize: 3,
	})
	if !errors.Is(err, ErrObjectBodyTooLarge) {
		t.Fatalf("Put err = %v, want %v", err, ErrObjectBodyTooLarge)
	}
	if tracked.createFileCalls != 0 || tracked.allocateChunkCalls != 0 || tracked.allocateBatchCalls != 0 {
		t.Fatalf(
			"resource calls = create:%d allocate:%d batch:%d, want all zero",
			tracked.createFileCalls,
			tracked.allocateChunkCalls,
			tracked.allocateBatchCalls,
		)
	}
}

func TestObjectCommitterExtremeKnownLengthAvoidsOverflowAndBatchPreallocation(t *testing.T) {
	ctx := context.Background()
	store := newTestMetadataWithQuota(t)
	createQuotaTestBucket(t, store, "photos")
	tracked := &quotaTrackingMetadata{PebbleStore: store}
	readErr := errors.New("stop before allocation")

	_, err := newMetadataObjectCommitter(tracked, &controlledChunkStore{}, false).Put(ctx, PutObjectRequest{
		Bucket:        "photos",
		Key:           "extreme.txt",
		Body:          errorReader{err: readErr},
		ContentLength: math.MaxInt64,
		MaxObjectSize: math.MaxInt64,
	})
	if !errors.Is(err, ErrObjectMetadataFailed) {
		t.Fatalf("Put err = %v, want %v", err, ErrObjectMetadataFailed)
	}
	if tracked.allocateChunkCalls != 0 || tracked.allocateBatchCalls != 0 {
		t.Fatalf(
			"allocation calls = single:%d batch:%d, want zero before first successful body read",
			tracked.allocateChunkCalls,
			tracked.allocateBatchCalls,
		)
	}
}
