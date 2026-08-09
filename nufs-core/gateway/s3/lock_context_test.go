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

func TestObjectCommitterReleasesLockAfterRequestContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store := newTestMetadataWithQuota(t)
	createQuotaTestBucket(t, store, "photos")
	tracked := &quotaTrackingMetadata{
		PebbleStore:       store,
		afterAdvisoryLock: cancel,
	}

	_, err := newMetadataObjectCommitter(tracked, chunkstore.NewMemoryChunkStore(), false).Put(ctx, PutObjectRequest{
		Bucket:        "photos",
		Key:           "empty.txt",
		Body:          strings.NewReader(""),
		ContentLength: 0,
		MaxObjectSize: DefaultMaxObjectSize,
	})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	assertNoAdvisoryLocks(t, store, tracked.createdInode.ID)
}

func TestGetObjectReleasesLockAfterRequestContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store := newTestMetadataWithQuota(t)
	createQuotaTestBucket(t, store, "photos")
	root := quotaBucketRoot(t, store, "photos")
	inode, err := store.CreateFile(context.Background(), root, "empty.txt", 0644)
	if err != nil {
		t.Fatalf("CreateFile: %v", err)
	}
	tracked := &quotaTrackingMetadata{
		PebbleStore:     store,
		afterSharedLock: cancel,
	}
	gw := NewGateway(GatewayConfig{
		MetaService: tracked,
		ChunkStore:  chunkstore.NewMemoryChunkStore(),
	})
	req := httptest.NewRequest(http.MethodGet, "/photos/empty.txt", nil).WithContext(ctx)

	gw.handleGetObject(httptest.NewRecorder(), req, "photos", "empty.txt", "get-request")

	assertNoAdvisoryLocks(t, store, inode.ID)
}

func TestDeleteObjectReleasesLockAfterRequestContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store := newTestMetadataWithQuota(t)
	createQuotaTestBucket(t, store, "photos")
	root := quotaBucketRoot(t, store, "photos")
	inode, err := store.CreateFile(context.Background(), root, "old.txt", 0644)
	if err != nil {
		t.Fatalf("CreateFile: %v", err)
	}
	tracked := &quotaTrackingMetadata{
		PebbleStore: store,
		afterUnlink: cancel,
	}
	gw := NewGateway(GatewayConfig{
		MetaService: tracked,
		ChunkStore:  chunkstore.NewMemoryChunkStore(),
	})
	req := httptest.NewRequest(http.MethodDelete, "/photos/old.txt", nil).WithContext(ctx)
	rec := httptest.NewRecorder()

	gw.handleDeleteObject(rec, req, "photos", "old.txt", "delete-request")

	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusNoContent, rec.Body.String())
	}
	assertNoAdvisoryLocks(t, store, inode.ID)
}

func TestObjectWriteRecoveryReleasesLockAfterTaskContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store := newTestMetadataWithQuota(t)
	createQuotaTestBucket(t, store, "photos")
	root := quotaBucketRoot(t, store, "photos")
	inode, err := store.CreateFile(context.Background(), root, "rejected.txt", 0644)
	if err != nil {
		t.Fatalf("CreateFile: %v", err)
	}
	if err := store.PutWriteAttempt(context.Background(), &metadata.ObjectWriteAttempt{
		ID:               "cleanup-canceled-context",
		Bucket:           "photos",
		Key:              "rejected.txt",
		InodeID:          inode.ID,
		InodeCTime:       inode.CTime,
		RecoveryIntent:   metadata.WriteAttemptRecoveryCleanup,
		CleanupParent:    root,
		CleanupNewObject: true,
		State:            metadata.WriteAttemptRecoveryNeeded,
	}); err != nil {
		t.Fatalf("PutWriteAttempt: %v", err)
	}
	tracked := &quotaTrackingMetadata{
		PebbleStore:       store,
		afterAdvisoryLock: cancel,
	}

	result, err := NewObjectWriteRecoveryWorker(tracked).RecoverOnce(ctx, 1)
	if err != nil {
		t.Fatalf("RecoverOnce: %v", err)
	}
	if result.Cleaned != 1 {
		t.Fatalf("result = %+v, want one cleaned attempt", result)
	}
	assertNoAdvisoryLocks(t, store, inode.ID)
}

func assertNoAdvisoryLocks(t *testing.T, store *metadata.PebbleStore, inode metadata.InodeID) {
	t.Helper()
	locks, err := store.AdvisoryListLocks(context.Background(), inode)
	if err != nil {
		t.Fatalf("AdvisoryListLocks(%d): %v", inode, err)
	}
	if len(locks) != 0 {
		t.Fatalf("inode %d locks = %+v, want none", inode, locks)
	}
}
