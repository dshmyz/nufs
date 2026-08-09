package datanode

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/dshmyz/nufs/nufs-core/metadata"
)

func TestGCScan_OnlyDeletesOnChunkNotFound(t *testing.T) {
	cs, _ := newTestChunkStore(t)

	chunkID := metadata.ChunkID(88888)
	data := []byte("do not delete me")
	if err := cs.Write(chunkID, data); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Mock: GetChunk returns a transient error (not ErrChunkNotFound)
	mock := newMockMetadataService()
	mock.getChunkErr = fmt.Errorf("metadata service temporarily unavailable")

	s := &OpsServer{
		store: cs,
		meta:  mock,
	}

	orphanCount, err := s.triggerGCScan(context.Background())
	if err != nil {
		t.Fatalf("triggerGCScan: %v", err)
	}

	if orphanCount != 0 {
		t.Errorf("expected 0 orphans deleted, got %d — GC deleted chunk on transient error", orphanCount)
	}

	_, _, readErr := cs.Read(chunkID, 0, 0)
	if readErr != nil {
		t.Errorf("chunk should still exist after transient error, but Read failed: %v", readErr)
	}
}

func TestGCScan_DeletesOnChunkNotFound(t *testing.T) {
	cs, _ := newTestChunkStore(t)

	chunkID := metadata.ChunkID(88889)
	data := []byte("delete me please")
	if err := cs.Write(chunkID, data); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Mock: GetChunk returns ErrChunkNotFound
	mock := newMockMetadataService()
	// Don't add chunk to mock — GetChunk will return ErrChunkNotFound

	s := &OpsServer{
		store: cs,
		meta:  mock,
	}

	orphanCount, err := s.triggerGCScan(context.Background())
	if err != nil {
		t.Fatalf("triggerGCScan: %v", err)
	}

	if orphanCount != 1 {
		t.Errorf("expected 1 orphan deleted, got %d", orphanCount)
	}

	_, _, readErr := cs.Read(chunkID, 0, 0)
	if readErr == nil {
		t.Error("chunk should have been deleted but Read succeeded")
	}
}

func TestChunkStoreDeleteDiscardsDescriptorOpenedBeforeUnlink(t *testing.T) {
	cs, _ := newTestChunkStore(t)
	chunkID := metadata.ChunkID(88991)
	if err := cs.Write(chunkID, []byte("descriptor race")); err != nil {
		t.Fatal(err)
	}
	reached := make(chan struct{})
	release := make(chan struct{})
	cs.disks[0].getFdBeforeInsert = func() { close(reached); <-release }
	readDone := make(chan error, 1)
	go func() { _, _, err := cs.Read(chunkID, 0, 0); readDone <- err }()
	<-reached
	if err := cs.Delete(chunkID); err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := <-readDone; err == nil {
		t.Fatal("read succeeded through descriptor opened before delete")
	}
	cs.disks[0].fdMu.RLock()
	_, cached := cs.disks[0].fdCache[chunkID]
	cs.disks[0].fdMu.RUnlock()
	if cached {
		t.Fatal("deleted chunk descriptor remained cached")
	}
}

func TestGCScan_SkipsOnContextCancelled(t *testing.T) {
	cs, _ := newTestChunkStore(t)

	chunkID := metadata.ChunkID(88890)
	data := []byte("should survive")
	if err := cs.Write(chunkID, data); err != nil {
		t.Fatalf("Write: %v", err)
	}

	mock := newMockMetadataService()

	s := &OpsServer{
		store: cs,
		meta:  mock,
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	orphanCount, err := s.triggerGCScan(ctx)
	if err != nil {
		t.Fatalf("triggerGCScan: %v", err)
	}

	if orphanCount != 0 {
		t.Errorf("expected 0 orphans deleted with cancelled context, got %d", orphanCount)
	}
}

func TestIsChunkNotFound(t *testing.T) {
	if !isChunkNotFound(metadata.ErrChunkNotFound) {
		t.Error("isChunkNotFound should return true for ErrChunkNotFound")
	}
	wrapped := fmt.Errorf("wrap: %w", metadata.ErrChunkNotFound)
	if !isChunkNotFound(wrapped) {
		t.Error("isChunkNotFound should return true for wrapped ErrChunkNotFound")
	}
	if isChunkNotFound(errors.New("some other error")) {
		t.Error("isChunkNotFound should return false for other errors")
	}
	if isChunkNotFound(nil) {
		t.Error("isChunkNotFound should return false for nil")
	}
}
