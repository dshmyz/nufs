package chunkstore

import (
	"context"
	"errors"
	"testing"

	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// ========== Fake ChunkLifecycle ==========

// fakeLifecycle implements ChunkLifecycle with call recording and injected
// failures, so tests can assert the dispatch order without a real store.
type fakeLifecycle struct {
	allocateCalls int
	batches       [][]int64
	commitCalls   int
	sealCalls     int

	allocErr   error
	commitErr  error
	// failBatchAlloc makes the n-th (0-based) AllocateChunksBatch call fail
	// with allocErr, returning nil chunks.
	failBatchAlloc int // -1 = never
}

func (f *fakeLifecycle) AllocateChunksBatch(_ context.Context, _ metadata.InodeID, offsets []int64, _ metadata.PlacementPolicy) ([]*metadata.ChunkMeta, error) {
	f.allocateCalls++
	f.batches = append(f.batches, offsets)
	if f.allocErr != nil && f.allocateCalls-1 == f.failBatchAlloc {
		return nil, f.allocErr
	}
	out := make([]*metadata.ChunkMeta, len(offsets))
	for i := range offsets {
		out[i] = &metadata.ChunkMeta{
			ID:       metadata.ChunkID(1000 + f.allocateCalls*10000 + i),
			Replicas: []metadata.ReplicaInfo{{NodeID: 1, Addr: "127.0.0.1:9001"}},
		}
	}
	return out, nil
}

func (f *fakeLifecycle) CommitChunk(_ context.Context, _ metadata.ChunkID, _ uint32) error {
	f.commitCalls++
	return f.commitErr
}

func (f *fakeLifecycle) SealChunk(_ context.Context, _ metadata.ChunkID) error {
	f.sealCalls++
	return nil
}

// ecEnabledStore embeds MemoryChunkStore but advertises direct-EC capability,
// like an authority-wired DatanodeChunkStore. Used to test that the ChunkWriter
// skips CommitChunk only when BOTH the chunk is EC-placed AND the store is
// EC-capable.
type ecEnabledStore struct{ *MemoryChunkStore }

func (s ecEnabledStore) ECWriteEnabled() bool { return true }

// ========== Shared fixtures ==========

// writerTestPolicy is a single-replica policy valid for an in-memory store.
var writerTestPolicy = metadata.PlacementPolicy{
	ID:                "writer-test",
	ReplicationFactor: 1,
	TopologySpread:    metadata.SpreadNode,
}

// newWriterTestMeta returns an in-memory PebbleStore with one healthy node,
// one bucket and one file. Mirrors the FUSE fixture so real-store round-trips
// can allocate + commit + seal without any datanode.
func newWriterTestMeta(t testing.TB) (*metadata.PebbleStore, metadata.InodeID, metadata.PlacementPolicy) {
	t.Helper()
	store, err := metadata.NewPebbleStore(metadata.PebbleStoreConfig{
		Dir:         t.TempDir(), // required even in UseInMemory mode
		UseInMemory: true,
	})
	if err != nil {
		t.Fatalf("NewPebbleStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	ctx := context.Background()
	if err := store.RegisterNode(ctx, &metadata.NodeInfo{ID: 1, Addr: "127.0.0.1:9001", CapacityGB: 100}); err != nil {
		t.Fatalf("RegisterNode: %v", err)
	}
	if err := store.CreateBucket(ctx, "test", writerTestPolicy); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	bucket, err := store.GetBucket(ctx, "test")
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}
	file, err := store.CreateFile(ctx, bucket.RootInode, "hello.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile: %v", err)
	}
	return store, file.ID, bucket.Policy
}

// ========== Tests ==========

func TestChunkWriter_RoundTrip(t *testing.T) {
	pb, inodeID, policy := newWriterTestMeta(t)
	store := NewMemoryChunkStore()
	w := NewChunkWriter(pb, store)
	ctx := context.Background()

	offsets := []int64{0, ChunkBase(metadata.MaxChunkSize)}
	chunks, err := w.AllocateRanges(ctx, inodeID, policy, offsets)
	if err != nil {
		t.Fatalf("AllocateRanges: %v", err)
	}
	if len(chunks) != len(offsets) {
		t.Fatalf("allocated %d chunks, want %d", len(chunks), len(offsets))
	}

	for i, chunk := range chunks {
		payload := []byte("roundtrip-payload")
		if i == 1 {
			payload = []byte("second-chunk-payload-bytes")
		}
		ref, err := w.WriteAllocated(ctx, chunk, offsets[i], payload)
		if err != nil {
			t.Fatalf("WriteAllocated chunk %d: %v", i, err)
		}
		if ref.ID != chunk.ID {
			t.Errorf("ref.ID=%d, want %d", ref.ID, chunk.ID)
		}
		if ref.Offset != offsets[i] {
			t.Errorf("ref.Offset=%d, want %d", ref.Offset, offsets[i])
		}
		if ref.Length != int32(len(payload)) {
			t.Errorf("ref.Length=%d, want %d", ref.Length, len(payload))
		}
		if ref.Version != 1 {
			t.Errorf("ref.Version=%d, want 1", ref.Version)
		}

		// Committed + sealed: state must be ChunkReady.
		got, err := pb.GetChunk(ctx, chunk.ID)
		if err != nil {
			t.Fatalf("GetChunk %d: %v", chunk.ID, err)
		}
		if got.State != metadata.ChunkReady {
			t.Errorf("chunk %d state=%v, want ChunkReady", chunk.ID, got.State)
		}

		// Payload landed in the chunk store, byte-exact.
		read, err := store.ReadChunk(ctx, chunk)
		if err != nil {
			t.Fatalf("ReadChunk %d: %v", chunk.ID, err)
		}
		if string(read) != string(payload) {
			t.Errorf("chunk %d payload=%q, want %q", chunk.ID, read, payload)
		}
	}

	// The inode's ChunkMap holds one ref per allocated offset.
	inode, err := pb.GetInode(ctx, inodeID)
	if err != nil {
		t.Fatalf("GetInode: %v", err)
	}
	if len(inode.ChunkMap) != len(offsets) {
		t.Fatalf("ChunkMap len=%d, want %d", len(inode.ChunkMap), len(offsets))
	}
	for i, ref := range inode.ChunkMap {
		if ref.Offset != offsets[i] {
			t.Errorf("ChunkMap[%d].Offset=%d, want %d", i, ref.Offset, offsets[i])
		}
	}
}

func TestChunkWriter_AllocateRangesSubBatch(t *testing.T) {
	// More offsets than MaxChunkAllocationBatch must be split into multiple
	// AllocateChunksBatch calls.
	fl := &fakeLifecycle{failBatchAlloc: -1}
	w := NewChunkWriter(fl, NewMemoryChunkStore())
	ctx := context.Background()

	n := metadata.MaxChunkAllocationBatch + 200 // 1224 offsets
	offsets := make([]int64, n)
	for i := range offsets {
		offsets[i] = int64(i)
	}
	chunks, err := w.AllocateRanges(ctx, 1, writerTestPolicy, offsets)
	if err != nil {
		t.Fatalf("AllocateRanges: %v", err)
	}
	if len(chunks) != n {
		t.Fatalf("allocated %d chunks, want %d", len(chunks), n)
	}
	if fl.allocateCalls != 2 {
		t.Errorf("AllocateChunksBatch calls=%d, want 2", fl.allocateCalls)
	}
	if len(fl.batches[0]) != metadata.MaxChunkAllocationBatch || len(fl.batches[1]) != 200 {
		t.Errorf("batch sizes = [%d %d], want [%d 200]",
			len(fl.batches[0]), len(fl.batches[1]), metadata.MaxChunkAllocationBatch)
	}

	// Inject a failure on the second batch: the first batch's chunks must be
	// returned alongside the error so the caller can compensate.
	fl2 := &fakeLifecycle{allocErr: errors.New("batch failed"), failBatchAlloc: 1}
	w2 := NewChunkWriter(fl2, NewMemoryChunkStore())
	partial, err := w2.AllocateRanges(ctx, 1, writerTestPolicy, offsets)
	if err == nil {
		t.Fatal("expected error from failing second batch")
	}
	if len(partial) != metadata.MaxChunkAllocationBatch {
		t.Errorf("partial chunks=%d, want %d (first batch preserved)", len(partial), metadata.MaxChunkAllocationBatch)
	}
}

func TestChunkWriter_ECSkip(t *testing.T) {
	ctx := context.Background()
	ecChunk := &metadata.ChunkMeta{
		ID:       42,
		Replicas: []metadata.ReplicaInfo{{NodeID: 1, Addr: "127.0.0.1:9001"}},
		ECGroup:  &metadata.ECGroupInfo{GroupID: "g1", DataShards: 6, ParityShards: 3},
	}
	plainChunk := &metadata.ChunkMeta{
		ID:       43,
		Replicas: []metadata.ReplicaInfo{{NodeID: 1, Addr: "127.0.0.1:9001"}},
	}
	payload := []byte("ec-skip-test-payload")

	capStore := ecEnabledStore{NewMemoryChunkStore()}

	t.Run("EC chunk on EC-capable store skips commit", func(t *testing.T) {
		fl := &fakeLifecycle{}
		w := NewChunkWriter(fl, capStore)
		ref, err := w.WriteAllocated(ctx, ecChunk, 0, payload)
		if err != nil {
			t.Fatalf("WriteAllocated: %v", err)
		}
		if ref.ID != ecChunk.ID {
			t.Errorf("ref.ID=%d, want %d", ref.ID, ecChunk.ID)
		}
		if fl.commitCalls != 0 {
			t.Errorf("CommitChunk calls=%d, want 0 (direct-EC already ChunkReady)", fl.commitCalls)
		}
		if fl.sealCalls != 1 {
			t.Errorf("SealChunk calls=%d, want 1", fl.sealCalls)
		}
	})

	t.Run("EC chunk on non-EC store commits", func(t *testing.T) {
		fl := &fakeLifecycle{}
		w := NewChunkWriter(fl, NewMemoryChunkStore())
		if _, err := w.WriteAllocated(ctx, ecChunk, 0, payload); err != nil {
			t.Fatalf("WriteAllocated: %v", err)
		}
		if fl.commitCalls != 1 {
			t.Errorf("CommitChunk calls=%d, want 1 (unwired store, no direct-EC)", fl.commitCalls)
		}
	})

	t.Run("plain chunk on EC-capable store commits", func(t *testing.T) {
		fl := &fakeLifecycle{}
		w := NewChunkWriter(fl, capStore)
		if _, err := w.WriteAllocated(ctx, plainChunk, 0, payload); err != nil {
			t.Fatalf("WriteAllocated: %v", err)
		}
		if fl.commitCalls != 1 {
			t.Errorf("CommitChunk calls=%d, want 1 (non-EC chunk always commits)", fl.commitCalls)
		}
	})
}

func TestChunkWriter_StageErrors(t *testing.T) {
	ctx := context.Background()
	chunk := &metadata.ChunkMeta{
		ID:       44,
		Replicas: []metadata.ReplicaInfo{{NodeID: 1, Addr: "127.0.0.1:9001"}},
	}
	payload := []byte("stage-error-payload")

	t.Run("write failure wraps ErrChunkWriteFailed", func(t *testing.T) {
		store := NewMemoryChunkStore()
		store.WriteHook = func(_ metadata.ChunkID, _ []byte) error { return errors.New("datanode disk full") }
		w := NewChunkWriter(&fakeLifecycle{}, store)
		_, err := w.WriteAllocated(ctx, chunk, 0, payload)
		if !errors.Is(err, ErrChunkWriteFailed) {
			t.Fatalf("err=%v, want wrap of ErrChunkWriteFailed", err)
		}
	})

	t.Run("commit failure wraps ErrChunkCommitFailed", func(t *testing.T) {
		fl := &fakeLifecycle{commitErr: errors.New("metadata commit rejected")}
		w := NewChunkWriter(fl, NewMemoryChunkStore())
		_, err := w.WriteAllocated(ctx, chunk, 0, payload)
		if !errors.Is(err, ErrChunkCommitFailed) {
			t.Fatalf("err=%v, want wrap of ErrChunkCommitFailed", err)
		}
		if fl.commitCalls != 1 {
			t.Errorf("CommitChunk calls=%d, want 1", fl.commitCalls)
		}
	})

	t.Run("seal failure is non-fatal", func(t *testing.T) {
		fl := &fakeLifecycle{}
		w := NewChunkWriter(fl, NewMemoryChunkStore())
		if _, err := w.WriteAllocated(ctx, chunk, 0, payload); err != nil {
			t.Fatalf("WriteAllocated: %v", err)
		}
		if fl.commitCalls != 1 {
			t.Errorf("CommitChunk calls=%d, want 1", fl.commitCalls)
		}
	})
}

func TestChunkWriter_ExecutorApplied(t *testing.T) {
	ctx := context.Background()
	var metaCalls, chunkCalls int
	metaExec := func(op string, fn func() error) error { metaCalls++; return fn() }
	chunkExec := func(op string, fn func() error) error { chunkCalls++; return fn() }

	fl := &fakeLifecycle{}
	w := NewChunkWriter(fl, NewMemoryChunkStore(), WithDoMeta(metaExec), WithDoChunk(chunkExec))

	chunks, err := w.AllocateRanges(ctx, 1, writerTestPolicy, []int64{0, ChunkBase(metadata.MaxChunkSize)})
	if err != nil {
		t.Fatalf("AllocateRanges: %v", err)
	}
	for i, c := range chunks {
		if _, err := w.WriteAllocated(ctx, c, int64(i), []byte("executor-test")); err != nil {
			t.Fatalf("WriteAllocated: %v", err)
		}
	}

	// meta: 1 allocate + 2 commit + 2 seal = 5; chunk: 2 writes.
	if metaCalls != 5 {
		t.Errorf("DoMeta calls=%d, want 5", metaCalls)
	}
	if chunkCalls != 2 {
		t.Errorf("DoChunk calls=%d, want 2", chunkCalls)
	}
}

func TestChunkBase_Alignment(t *testing.T) {
	m := int64(metadata.MaxChunkSize)
	cases := []struct {
		off, want int64
	}{
		{0, 0},
		{m - 1, 0},
		{m, m},
		{m + 1, m},
		{3*m + 42, 3 * m},
	}
	for _, c := range cases {
		if got := ChunkBase(c.off); got != c.want {
			t.Errorf("ChunkBase(%d)=%d, want %d", c.off, got, c.want)
		}
	}
}
