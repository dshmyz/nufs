package s3

import (
	"context"
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/example/dfs/metadata"
)

// ========== Mock MetadataService ==========

type mockMetaService struct {
	mu      sync.RWMutex
	buckets map[string]*metadata.BucketInfo
	inodes  map[metadata.InodeID]*metadata.InodeMeta
	entries map[string]map[string]*metadata.InodeMeta // parentID -> name -> inode
	nodes   []metadata.NodeInfo
	chunks  map[metadata.ChunkID]*metadata.ChunkMeta
	nextID  uint64
}

func newMockMetaService() *mockMetaService {
	m := &mockMetaService{
		buckets: make(map[string]*metadata.BucketInfo),
		inodes:  make(map[metadata.InodeID]*metadata.InodeMeta),
		entries: make(map[string]map[string]*metadata.InodeMeta),
		chunks:  make(map[metadata.ChunkID]*metadata.ChunkMeta),
		nextID:  100,
	}
	return m
}

func (m *mockMetaService) allocID() metadata.InodeID {
	m.nextID++
	return metadata.InodeID(m.nextID)
}

func (m *mockMetaService) parentKey(parent metadata.InodeID) string {
	return string(rune(parent))
}

func (m *mockMetaService) Close() error { return nil }

// ---- Bucket operations ----

func (m *mockMetaService) CreateBucket(_ context.Context, name string, policy metadata.PlacementPolicy) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.buckets[name]; ok {
		return metadata.ErrBucketExists
	}
	rootID := m.allocID()
	root := &metadata.InodeMeta{
		ID:    rootID,
		Type:  metadata.FileDirectory,
		CTime: time.Now().UnixNano(),
		MTime: time.Now().UnixNano(),
		ATime: time.Now().UnixNano(),
	}
	m.inodes[rootID] = root
	m.entries[m.parentKey(rootID)] = make(map[string]*metadata.InodeMeta)
	m.buckets[name] = &metadata.BucketInfo{
		Name:         name,
		RootInode:    rootID,
		Policy:       policy,
		CreationDate: time.Now(),
	}
	return nil
}

func (m *mockMetaService) DeleteBucket(_ context.Context, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.buckets[name]
	if !ok {
		return metadata.ErrBucketNotFound
	}
	key := m.parentKey(b.RootInode)
	if children, ok := m.entries[key]; ok && len(children) > 0 {
		return metadata.ErrBucketNotEmpty
	}
	delete(m.buckets, name)
	return nil
}

func (m *mockMetaService) ListBuckets(_ context.Context) ([]metadata.BucketInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var result []metadata.BucketInfo
	for _, b := range m.buckets {
		result = append(result, *b)
	}
	return result, nil
}

func (m *mockMetaService) GetBucket(_ context.Context, name string) (*metadata.BucketInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	b, ok := m.buckets[name]
	if !ok {
		return nil, metadata.ErrBucketNotFound
	}
	return b, nil
}

// ---- Directory operations ----

func (m *mockMetaService) MkDir(_ context.Context, parent metadata.InodeID, name string, mode uint32) (*metadata.InodeMeta, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	pk := m.parentKey(parent)
	if _, ok := m.entries[pk]; !ok {
		m.entries[pk] = make(map[string]*metadata.InodeMeta)
	}
	if _, ok := m.entries[pk][name]; ok {
		return nil, metadata.ErrEntryExists
	}
	id := m.allocID()
	inode := &metadata.InodeMeta{ID: id, Type: metadata.FileDirectory, Mode: mode, CTime: time.Now().UnixNano(), MTime: time.Now().UnixNano(), ATime: time.Now().UnixNano()}
	m.inodes[id] = inode
	m.entries[pk][name] = inode
	m.entries[m.parentKey(id)] = make(map[string]*metadata.InodeMeta)
	return inode, nil
}

func (m *mockMetaService) RmDir(_ context.Context, parent metadata.InodeID, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	pk := m.parentKey(parent)
	if _, ok := m.entries[pk][name]; !ok {
		return metadata.ErrEntryNotFound
	}
	delete(m.entries[pk], name)
	return nil
}

func (m *mockMetaService) ReadDir(_ context.Context, parent metadata.InodeID, offset, limit int) ([]metadata.DirEntry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	pk := m.parentKey(parent)
	children, ok := m.entries[pk]
	if !ok {
		return nil, nil
	}
	var result []metadata.DirEntry
	for name, inode := range children {
		result = append(result, metadata.DirEntry{InodeID: inode.ID, Type: inode.Type, Name: name})
	}
	if offset >= len(result) {
		return nil, nil
	}
	end := offset + limit
	if end > len(result) {
		end = len(result)
	}
	return result[offset:end], nil
}

// ---- File operations ----

func (m *mockMetaService) CreateFile(_ context.Context, parent metadata.InodeID, name string, mode uint32) (*metadata.InodeMeta, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	pk := m.parentKey(parent)
	if _, ok := m.entries[pk]; !ok {
		m.entries[pk] = make(map[string]*metadata.InodeMeta)
	}
	if _, ok := m.entries[pk][name]; ok {
		return nil, metadata.ErrEntryExists
	}
	id := m.allocID()
	now := time.Now().UnixNano()
	inode := &metadata.InodeMeta{ID: id, Type: metadata.FileRegular, Mode: mode, NLink: 1, CTime: now, MTime: now, ATime: now}
	m.inodes[id] = inode
	m.entries[pk][name] = inode
	return inode, nil
}

func (m *mockMetaService) Unlink(_ context.Context, parent metadata.InodeID, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	pk := m.parentKey(parent)
	inode, ok := m.entries[pk][name]
	if !ok {
		return metadata.ErrEntryNotFound
	}
	inode.NLink--
	delete(m.entries[pk], name)
	if inode.NLink == 0 {
		delete(m.inodes, inode.ID)
	}
	return nil
}

func (m *mockMetaService) Lookup(_ context.Context, parent metadata.InodeID, name string) (*metadata.InodeMeta, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	pk := m.parentKey(parent)
	inode, ok := m.entries[pk][name]
	if !ok {
		return nil, metadata.ErrEntryNotFound
	}
	return inode, nil
}

func (m *mockMetaService) GetInode(_ context.Context, id metadata.InodeID) (*metadata.InodeMeta, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	inode, ok := m.inodes[id]
	if !ok {
		return nil, metadata.ErrInodeNotFound
	}
	return inode, nil
}

func (m *mockMetaService) UpdateInode(_ context.Context, meta *metadata.InodeMeta) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.inodes[meta.ID] = meta
	return nil
}

func (m *mockMetaService) Rename(_ context.Context, oldParent metadata.InodeID, oldName string, newParent metadata.InodeID, newName string) error {
	return errors.New("not implemented")
}

func (m *mockMetaService) Symlink(_ context.Context, parent metadata.InodeID, name, target string) (*metadata.InodeMeta, error) {
	return nil, errors.New("not implemented")
}

func (m *mockMetaService) Readlink(_ context.Context, id metadata.InodeID) (string, error) {
	return "", errors.New("not implemented")
}

func (m *mockMetaService) Link(_ context.Context, parent metadata.InodeID, name string, target metadata.InodeID) (*metadata.InodeMeta, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	inode, ok := m.inodes[target]
	if !ok {
		return nil, metadata.ErrInodeNotFound
	}
	pk := m.parentKey(parent)
	if _, ok := m.entries[pk]; !ok {
		m.entries[pk] = make(map[string]*metadata.InodeMeta)
	}
	inode.NLink++
	m.entries[pk][name] = inode
	return inode, nil
}

// ---- Chunk operations ----

func (m *mockMetaService) AllocateChunk(_ context.Context, inodeID metadata.InodeID, offset int64, policy metadata.PlacementPolicy) (*metadata.ChunkMeta, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	id := metadata.ChunkID(m.nextID)
	m.nextID++
	chunk := &metadata.ChunkMeta{
		ID:         id,
		State:      metadata.ChunkSealing,
		CreateTime: time.Now().UnixNano(),
	}
	m.chunks[id] = chunk
	return chunk, nil
}

func (m *mockMetaService) CommitChunk(_ context.Context, chunkID metadata.ChunkID, checksum uint32) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	chunk, ok := m.chunks[chunkID]
	if !ok {
		return metadata.ErrChunkNotFound
	}
	chunk.State = metadata.ChunkReady
	chunk.Checksum = checksum
	return nil
}

func (m *mockMetaService) GetChunk(_ context.Context, chunkID metadata.ChunkID) (*metadata.ChunkMeta, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	chunk, ok := m.chunks[chunkID]
	if !ok {
		return nil, metadata.ErrChunkNotFound
	}
	return chunk, nil
}

func (m *mockMetaService) UpdateChunk(_ context.Context, chunk *metadata.ChunkMeta) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.chunks[chunk.ID] = chunk
	return nil
}

func (m *mockMetaService) SealChunk(_ context.Context, chunkID metadata.ChunkID) error {
	return nil
}

func (m *mockMetaService) ListChunks(_ context.Context, inodeID metadata.InodeID) ([]metadata.ChunkRef, error) {
	return nil, nil
}

func (m *mockMetaService) DeleteChunk(_ context.Context, chunkID metadata.ChunkID) error {
	return nil
}

func (m *mockMetaService) ReportChunkState(_ context.Context, nodeID metadata.NodeID, states map[metadata.ChunkID]metadata.ReplicaState) error {
	return nil
}

// ---- Cluster operations ----

func (m *mockMetaService) RegisterNode(_ context.Context, info *metadata.NodeInfo) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nodes = append(m.nodes, *info)
	return nil
}

func (m *mockMetaService) Heartbeat(_ context.Context, nodeID metadata.NodeID, report *metadata.NodeReport) error {
	return nil
}

func (m *mockMetaService) DecommissionNode(_ context.Context, nodeID metadata.NodeID) error {
	return nil
}

func (m *mockMetaService) ListNodes(_ context.Context) ([]metadata.NodeInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.nodes, nil
}

func (m *mockMetaService) GetNode(_ context.Context, nodeID metadata.NodeID) (*metadata.NodeInfo, error) {
	return nil, metadata.ErrNodeNotFound
}

func (m *mockMetaService) SetPolicy(_ context.Context, bucket string, policy metadata.PlacementPolicy) error {
	return nil
}

func (m *mockMetaService) GetPolicy(_ context.Context, bucket string) (*metadata.PlacementPolicy, error) {
	return &metadata.PlacementPolicy{ID: "default", ReplicationFactor: 1}, nil
}

func (m *mockMetaService) TriggerRepair(_ context.Context, chunkID metadata.ChunkID) error {
	return nil
}

func (m *mockMetaService) TriggerRebalance(_ context.Context) error {
	return nil
}

func (m *mockMetaService) RemoveRepairTask(_ context.Context, chunkID metadata.ChunkID) error {
	return nil
}

func (m *mockMetaService) GetRepairQueue(_ context.Context) ([]metadata.RepairTask, error) {
	return nil, nil
}

func (m *mockMetaService) ChunksByNode(_ context.Context, _ metadata.NodeID) ([]metadata.ChunkMeta, error) {
	return nil, nil
}

func (m *mockMetaService) MigrateChunkReplica(_ context.Context, _ metadata.ChunkID, _, _ metadata.NodeID) error {
	return nil
}

// ========== Test Helpers ==========

func newTestGateway(t *testing.T) (*Gateway, *httptest.Server, *mockMetaService) {
	t.Helper()
	return newTestGatewayWithStore(t, NewMemoryChunkStore())
}

// newTestGatewayWithStore allows tests to inject a custom ChunkStore.
// When store is nil a DatanodeChunkStore is used (only suitable for
// tests that bring up a real datanode).
func newTestGatewayWithStore(t *testing.T, store ChunkStore) (*Gateway, *httptest.Server, *mockMetaService) {
	t.Helper()
	meta := newMockMetaService()
	gw := NewGateway(GatewayConfig{
		MetaService: meta,
		Creds:       NewCredentialStore(),
		ChunkStore:  store,
	})
	ts := httptest.NewServer(gw.Handler())
	return gw, ts, meta
}

// ========== Tests ==========

func TestListBuckets_Empty(t *testing.T) {
	_, ts, _ := newTestGateway(t)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var result ListAllMyBucketsResult
	body, _ := io.ReadAll(resp.Body)
	if err := xml.Unmarshal(body, &result); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	if len(result.Buckets) != 0 {
		t.Errorf("expected 0 buckets, got %d", len(result.Buckets))
	}
}

func TestCreateAndListBucket(t *testing.T) {
	_, ts, _ := newTestGateway(t)
	defer ts.Close()

	// Create bucket
	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/test-bucket", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("create bucket: expected 200, got %d", resp.StatusCode)
	}

	// List buckets
	resp, err = http.Get(ts.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	var result ListAllMyBucketsResult
	body, _ := io.ReadAll(resp.Body)
	if err := xml.Unmarshal(body, &result); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	if len(result.Buckets) != 1 {
		t.Errorf("expected 1 bucket, got %d", len(result.Buckets))
	}
	if result.Buckets[0].Name != "test-bucket" {
		t.Errorf("expected bucket name 'test-bucket', got '%s'", result.Buckets[0].Name)
	}
}

func TestCreateDuplicateBucket(t *testing.T) {
	_, ts, _ := newTestGateway(t)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/dup-bucket", nil)
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()

	// Create again
	req, _ = http.NewRequest(http.MethodPut, ts.URL+"/dup-bucket", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("expected 409, got %d", resp.StatusCode)
	}
}

func TestDeleteBucket(t *testing.T) {
	_, ts, _ := newTestGateway(t)
	defer ts.Close()

	// Create
	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/del-bucket", nil)
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()

	// Delete
	req, _ = http.NewRequest(http.MethodDelete, ts.URL+"/del-bucket", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("delete: expected 204, got %d", resp.StatusCode)
	}

	// Verify gone
	req, _ = http.NewRequest(http.MethodHead, ts.URL+"/del-bucket", nil)
	resp, _ = http.DefaultClient.Do(req)
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("head after delete: expected 404, got %d", resp.StatusCode)
	}
}

func TestHeadBucket(t *testing.T) {
	_, ts, _ := newTestGateway(t)
	defer ts.Close()

	// Head nonexistent
	req, _ := http.NewRequest(http.MethodHead, ts.URL+"/nonexistent", nil)
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}

	// Create then head
	req, _ = http.NewRequest(http.MethodPut, ts.URL+"/exists", nil)
	resp, _ = http.DefaultClient.Do(req)
	resp.Body.Close()

	req, _ = http.NewRequest(http.MethodHead, ts.URL+"/exists", nil)
	resp, _ = http.DefaultClient.Do(req)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}

func TestDeleteNonexistentBucket(t *testing.T) {
	_, ts, _ := newTestGateway(t)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodDelete, ts.URL+"/no-such", nil)
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}
}

func TestPutAndHeadObject(t *testing.T) {
	_, ts, meta := newTestGateway(t)
	defer ts.Close()

	// Register a node
	meta.RegisterNode(context.Background(), &metadata.NodeInfo{
		ID: 1, Addr: "localhost:9001", State: metadata.NodeOnline,
	})

	// Create bucket
	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/mybucket", nil)
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()

	// Put object
	body := strings.NewReader("hello world")
	req, _ = http.NewRequest(http.MethodPut, ts.URL+"/mybucket/hello.txt", body)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("put object: expected 200, got %d", resp.StatusCode)
	}
	if etag := resp.Header.Get("ETag"); etag == "" {
		t.Error("expected ETag header")
	}

	// Head object
	req, _ = http.NewRequest(http.MethodHead, ts.URL+"/mybucket/hello.txt", nil)
	resp, _ = http.DefaultClient.Do(req)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("head object: expected 200, got %d", resp.StatusCode)
	}
	if resp.Header.Get("Content-Length") != "11" {
		t.Errorf("expected Content-Length 11, got %s", resp.Header.Get("Content-Length"))
	}
}

func TestGetObject_NotFound(t *testing.T) {
	_, ts, _ := newTestGateway(t)
	defer ts.Close()

	// Create bucket
	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/b", nil)
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()

	// Get nonexistent key
	resp, err := http.Get(ts.URL + "/b/nokey")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}
}

func TestDeleteObject(t *testing.T) {
	_, ts, meta := newTestGateway(t)
	defer ts.Close()

	meta.RegisterNode(context.Background(), &metadata.NodeInfo{
		ID: 1, Addr: "localhost:9001", State: metadata.NodeOnline,
	})

	// Create bucket
	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/b2", nil)
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()

	// Put object
	req, _ = http.NewRequest(http.MethodPut, ts.URL+"/b2/obj1", strings.NewReader("data"))
	resp, _ = http.DefaultClient.Do(req)
	resp.Body.Close()

	// Delete object
	req, _ = http.NewRequest(http.MethodDelete, ts.URL+"/b2/obj1", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("delete object: expected 204, got %d", resp.StatusCode)
	}

	// Head should be 404
	req, _ = http.NewRequest(http.MethodHead, ts.URL+"/b2/obj1", nil)
	resp, _ = http.DefaultClient.Do(req)
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("head after delete: expected 404, got %d", resp.StatusCode)
	}
}

func TestDeleteObject_Nonexistent(t *testing.T) {
	_, ts, _ := newTestGateway(t)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/b3", nil)
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()

	// Delete nonexistent — S3 returns 204
	req, _ = http.NewRequest(http.MethodDelete, ts.URL+"/b3/nokey", nil)
	resp, _ = http.DefaultClient.Do(req)
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", resp.StatusCode)
	}
}

func TestListObjects(t *testing.T) {
	_, ts, meta := newTestGateway(t)
	defer ts.Close()

	meta.RegisterNode(context.Background(), &metadata.NodeInfo{
		ID: 1, Addr: "localhost:9001", State: metadata.NodeOnline,
	})

	// Create bucket
	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/listbucket", nil)
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()

	// Put 3 objects
	for _, key := range []string{"a.txt", "b.txt", "c.txt"} {
		req, _ = http.NewRequest(http.MethodPut, ts.URL+"/listbucket/"+key, strings.NewReader("data"))
		resp, _ = http.DefaultClient.Do(req)
		resp.Body.Close()
	}

	// List
	resp, err := http.Get(ts.URL + "/listbucket")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	var result ListBucketResult
	body, _ := io.ReadAll(resp.Body)
	if err := xml.Unmarshal(body, &result); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	if len(result.Contents) != 3 {
		t.Errorf("expected 3 objects, got %d", len(result.Contents))
	}
}

func TestMultipartUpload(t *testing.T) {
	_, ts, _ := newTestGateway(t)
	defer ts.Close()

	// Create bucket
	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/mpbucket", nil)
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()

	// Initiate multipart upload
	req, _ = http.NewRequest(http.MethodPost, ts.URL+"/mpbucket/bigfile.dat?uploads", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	var initResult InitiateMultipartUploadResult
	body, _ := io.ReadAll(resp.Body)
	if err := xml.Unmarshal(body, &initResult); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	if initResult.UploadID == "" {
		t.Fatal("expected non-empty upload ID")
	}
	if initResult.Bucket != "mpbucket" {
		t.Errorf("expected bucket 'mpbucket', got '%s'", initResult.Bucket)
	}

	uploadID := initResult.UploadID

	// Upload 2 parts
	for i := 1; i <= 2; i++ {
		partBody := strings.NewReader("part data " + strings.Repeat("x", i*100))
		url := ts.URL + "/mpbucket/bigfile.dat?uploadId=" + uploadID + "&partNumber=" + string(rune('0'+i))
		req, _ = http.NewRequest(http.MethodPut, url, partBody)
		resp, err = http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("upload part %d: expected 200, got %d", i, resp.StatusCode)
		}
		if resp.Header.Get("ETag") == "" {
			t.Errorf("part %d: expected ETag", i)
		}
	}

	// List parts
	url := ts.URL + "/mpbucket/bigfile.dat?uploadId=" + uploadID
	resp, err = http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	var listParts ListPartsResult
	body, _ = io.ReadAll(resp.Body)
	if err := xml.Unmarshal(body, &listParts); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	if len(listParts.Parts) != 2 {
		t.Errorf("expected 2 parts, got %d", len(listParts.Parts))
	}

	// Complete multipart upload
	completeXML := `<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>"etag1"</ETag></Part><Part><PartNumber>2</PartNumber><ETag>"etag2"</ETag></Part></CompleteMultipartUpload>`
	url = ts.URL + "/mpbucket/bigfile.dat?uploadId=" + uploadID
	req, _ = http.NewRequest(http.MethodPost, url, strings.NewReader(completeXML))
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("complete: expected 200, got %d", resp.StatusCode)
	}
}

func TestAbortMultipartUpload(t *testing.T) {
	_, ts, _ := newTestGateway(t)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/abort-bucket", nil)
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()

	// Initiate
	req, _ = http.NewRequest(http.MethodPost, ts.URL+"/abort-bucket/obj?uploads", nil)
	resp, _ = http.DefaultClient.Do(req)
	var initResult InitiateMultipartUploadResult
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	xml.Unmarshal(body, &initResult)

	// Abort
	url := ts.URL + "/abort-bucket/obj?uploadId=" + initResult.UploadID
	req, _ = http.NewRequest(http.MethodDelete, url, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("abort: expected 204, got %d", resp.StatusCode)
	}

	// Verify upload gone
	resp, _ = http.Get(url)
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("list parts after abort: expected 404, got %d", resp.StatusCode)
	}
}

func TestCORSHeaders(t *testing.T) {
	_, ts, _ := newTestGateway(t)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	if resp.Header.Get("Access-Control-Allow-Origin") == "" {
		t.Error("expected Access-Control-Allow-Origin header")
	}
}

func TestRequestIDHeader(t *testing.T) {
	_, ts, _ := newTestGateway(t)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	if resp.Header.Get("x-amz-request-id") == "" {
		t.Error("expected x-amz-request-id header")
	}
}

func TestOptionsPreflight(t *testing.T) {
	_, ts, _ := newTestGateway(t)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodOptions, ts.URL+"/bucket/key", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	if resp.Header.Get("Access-Control-Allow-Methods") == "" {
		t.Error("expected Access-Control-Allow-Methods header")
	}
}

func TestParsePath(t *testing.T) {
	tests := []struct {
		path       string
		wantBucket string
		wantKey    string
	}{
		{"/", "", ""},
		{"/bucket", "bucket", ""},
		{"/bucket/key", "bucket", "key"},
		{"/bucket/path/to/file.txt", "bucket", "path/to/file.txt"},
		{"", "", ""},
	}
	for _, tt := range tests {
		bucket, key := parsePath(tt.path)
		if bucket != tt.wantBucket || key != tt.wantKey {
			t.Errorf("parsePath(%q) = (%q, %q), want (%q, %q)",
				tt.path, bucket, key, tt.wantBucket, tt.wantKey)
		}
	}
}

func TestParseRange(t *testing.T) {
	tests := []struct {
		header string
		size   int64
		start  int64
		end    int64
	}{
		{"", 100, 0, 99},
		{"bytes=0-49", 100, 0, 49},
		{"bytes=50-99", 100, 50, 99},
		{"bytes=50-", 100, 50, 99},
		{"bytes=-50", 100, 0, 50},
		{"bytes=0-200", 100, 0, 99}, // clamped
	}
	for _, tt := range tests {
		start, end := parseRange(tt.header, tt.size)
		if start != tt.start || end != tt.end {
			t.Errorf("parseRange(%q, %d) = (%d, %d), want (%d, %d)",
				tt.header, tt.size, start, end, tt.start, tt.end)
		}
	}
}

func TestFormatETag(t *testing.T) {
	etag := FormatETag(0x12345678)
	if etag != `"12345678"` {
		t.Errorf("expected \"12345678\", got %s", etag)
	}
}

func TestFormatS3Time(t *testing.T) {
	ts := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	s := FormatS3Time(ts)
	if s != "2024-01-15T10:30:00.000Z" {
		t.Errorf("unexpected format: %s", s)
	}
}
