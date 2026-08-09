package s3

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/dshmyz/nufs/nufs-core/chunkstore"
	"github.com/dshmyz/nufs/nufs-core/metadata"
)

func TestNewGatewayInstallsDefaultObjectCommitter(t *testing.T) {
	meta := newMockMetaService()
	gw := NewGateway(GatewayConfig{
		MetaService: meta,
		ChunkStore:  chunkstore.NewMemoryChunkStore(),
	})

	if gw.committer == nil {
		t.Fatal("expected gateway to install an ObjectCommitter")
	}
}

type recordingCommitter struct {
	putCalled bool
}

func (r *recordingCommitter) Put(ctx context.Context, req PutObjectRequest) (PutObjectResult, error) {
	r.putCalled = true
	if req.Bucket != "bucket" || req.Key != "object.txt" {
		return PutObjectResult{}, errors.New("unexpected request")
	}
	return PutObjectResult{ETag: "\"etag\"", Size: 7}, nil
}

func (r *recordingCommitter) Get(ctx context.Context, req GetObjectRequest) (ObjectReader, error) {
	return nil, errors.New("not used")
}

func TestPutObjectDelegatesToObjectCommitter(t *testing.T) {
	meta := newMockMetaService()
	if err := meta.CreateBucket(context.Background(), "bucket", metadata.PlacementPolicy{ReplicationFactor: 1}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}

	rec := &recordingCommitter{}
	gw := NewGateway(GatewayConfig{
		MetaService: meta,
		ChunkStore:  chunkstore.NewMemoryChunkStore(),
	})
	gw.committer = rec
	ts := httptest.NewServer(gw.Handler())
	defer ts.Close()

	req, err := http.NewRequest(http.MethodPut, ts.URL+"/bucket/object.txt", strings.NewReader("payload"))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("put object: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	if got := resp.Header.Get("ETag"); got != "\"etag\"" {
		t.Fatalf("ETag = %q, want %q", got, "\"etag\"")
	}
	if !rec.putCalled {
		t.Fatal("expected ObjectCommitter.Put to be called")
	}
}

func TestObjectCommitterPersistsCommittedWriteAttempt(t *testing.T) {
	store := newMemoryAttemptStore()
	meta := newMockMetaService()
	if err := meta.CreateBucket(context.Background(), "bucket", metadata.PlacementPolicy{ReplicationFactor: 1}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}

	committer := newMetadataObjectCommitter(&attemptMetaService{
		mockMetaService: meta,
		attemptStore:    store,
	}, chunkstore.NewMemoryChunkStore(), false)

	if _, err := committer.Put(context.Background(), PutObjectRequest{
		Bucket: "bucket",
		Key:    "object.txt",
		Body:   strings.NewReader("payload"),
	}); err != nil {
		t.Fatalf("put object: %v", err)
	}

	if got := store.lastState(); got != metadata.WriteAttemptCommitted {
		t.Fatalf("last attempt state = %s, want %s", got, metadata.WriteAttemptCommitted)
	}
}

type attemptMetaService struct {
	*mockMetaService
	attemptStore *memoryAttemptStore
}

func (m *attemptMetaService) PutWriteAttempt(ctx context.Context, attempt *metadata.ObjectWriteAttempt) error {
	return m.attemptStore.PutWriteAttempt(ctx, attempt)
}

type memoryAttemptStore struct {
	attempts []*metadata.ObjectWriteAttempt
}

func newMemoryAttemptStore() *memoryAttemptStore {
	return &memoryAttemptStore{}
}

func (s *memoryAttemptStore) PutWriteAttempt(_ context.Context, attempt *metadata.ObjectWriteAttempt) error {
	cp := *attempt
	s.attempts = append(s.attempts, &cp)
	return nil
}

func (s *memoryAttemptStore) GetWriteAttempt(_ context.Context, id string) (*metadata.ObjectWriteAttempt, error) {
	for i := len(s.attempts) - 1; i >= 0; i-- {
		if s.attempts[i].ID == id {
			cp := *s.attempts[i]
			return &cp, nil
		}
	}
	return nil, metadata.ErrEntryNotFound
}

func (s *memoryAttemptStore) ListWriteAttemptsByState(_ context.Context, state metadata.WriteAttemptState, limit int) ([]metadata.ObjectWriteAttempt, error) {
	if limit <= 0 {
		limit = 100
	}
	latest := make(map[string]*metadata.ObjectWriteAttempt)
	order := make([]string, 0)
	for _, attempt := range s.attempts {
		if _, ok := latest[attempt.ID]; !ok {
			order = append(order, attempt.ID)
		}
		latest[attempt.ID] = attempt
	}
	result := make([]metadata.ObjectWriteAttempt, 0)
	for _, id := range order {
		attempt := latest[id]
		if attempt.State != state {
			continue
		}
		cp := *attempt
		result = append(result, cp)
		if len(result) >= limit {
			break
		}
	}
	return result, nil
}

func (s *memoryAttemptStore) DeleteWriteAttempt(_ context.Context, id string) error {
	filtered := s.attempts[:0]
	deleted := false
	for _, attempt := range s.attempts {
		if attempt.ID == id {
			deleted = true
			continue
		}
		filtered = append(filtered, attempt)
	}
	s.attempts = filtered
	if !deleted {
		return metadata.ErrEntryNotFound
	}
	return nil
}

func (s *memoryAttemptStore) lastState() metadata.WriteAttemptState {
	if len(s.attempts) == 0 {
		return ""
	}
	return s.attempts[len(s.attempts)-1].State
}
