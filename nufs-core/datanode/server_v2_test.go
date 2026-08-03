package datanode

import (
	"context"
	"testing"
	"time"

	"github.com/example/dfs/datanode/storage"
	"github.com/example/dfs/datanode/storage/segment"
	"github.com/example/dfs/metadata"
)

// newTestMultiStore builds a V2Store over n in-memory segment stores
// (one per disk), returning the store and a cleanup that closes each
// backend. Data topics use StreamID 1.
func newTestMultiStore(t *testing.T, n int) (*V2Store, []*segment.Store) {
	t.Helper()
	dirs := make([]string, n)
	for i := 0; i < n; i++ {
		dirs[i] = t.TempDir()
	}
	backends := make([]*segment.Store, n)
	for i := 0; i < n; i++ {
		s, err := segment.New(segment.Config{
			Dir:         dirs[i],
			UseMemIndex: true,
			StreamID:    1,
		})
		if err != nil {
			t.Fatalf("segment.New disk %d: %v", i, err)
		}
		backends[i] = s
	}
	stores := make([]storage.Store, n)
	for i := 0; i < n; i++ {
		stores[i] = backends[i]
	}
	v := NewMultiV2Store(stores)
	t.Cleanup(func() {
		for _, s := range backends {
			s.Close()
		}
	})
	return v, backends
}

func TestV2StoreMultiDiskPlacement(t *testing.T) {
	v, _ := newTestMultiStore(t, 2)

	// First write lands on disk 0 (least-used tie broken by index).
	if err := v.Write(metadata.ChunkID(101), []byte("first-on-disk-zero")); err != nil {
		t.Fatalf("write 101: %v", err)
	}
	// After disk 0 has bytes, the next new chunk goes to the empty disk 1.
	if err := v.Write(metadata.ChunkID(202), []byte("second-on-disk-one")); err != nil {
		t.Fatalf("write 202: %v", err)
	}

	v.mu.RLock()
	d101, ok101 := v.locOf[metadata.ChunkID(101)]
	d202, ok202 := v.locOf[metadata.ChunkID(202)]
	v.mu.RUnlock()
	if !ok101 || d101.disk != 0 {
		t.Fatalf("chunk 101 on disk %d (ok=%v), want 0", d101.disk, ok101)
	}
	if !ok202 || d202.disk != 1 {
		t.Fatalf("chunk 202 on disk %d (ok=%v), want 1", d202.disk, ok202)
	}

	// A rewrite of an existing chunk stays on its owning disk (here 0)
	// and advances to a fresh generation (it no longer overwrites at gen 1,
	// which the segment store rejects as stale).
	beforeGen := d101.gen
	if err := v.Write(metadata.ChunkID(101), []byte("rewrite-101-longer-payload")); err != nil {
		t.Fatalf("rewrite 101: %v", err)
	}
	v.mu.RLock()
	d101 = v.locOf[metadata.ChunkID(101)]
	v.mu.RUnlock()
	if d101.disk != 0 {
		t.Fatalf("rewritten chunk 101 on disk %d, want 0 (owning disk)", d101.disk)
	}
	if d101.gen != beforeGen+1 {
		t.Fatalf("rewritten chunk 101 gen=%d, want %d", d101.gen, beforeGen+1)
	}

	// The data actually landed on the expected backends (read through).
	if _, _, err := v.Read(metadata.ChunkID(202), 0, 0); err != nil {
		t.Fatalf("read 202 from disk 1: %v", err)
	}

	// Per-disk accounting reflects the spread.
	ds := v.DiskStats()
	if len(ds) != 2 {
		t.Fatalf("DiskStats len=%d, want 2", len(ds))
	}
	if ds[0].ChunkCount == 0 || ds[1].ChunkCount == 0 {
		t.Fatalf("expected chunks on both disks, got disk0=%d disk1=%d", ds[0].ChunkCount, ds[1].ChunkCount)
	}
}

func TestV2StoreStatsListSnapshot(t *testing.T) {
	v, _ := newTestMultiStore(t, 2)

	// Two distinct chunks.
	if err := v.Write(metadata.ChunkID(1), []byte("aaaaaaaa")); err != nil {
		t.Fatalf("write 1: %v", err)
	}
	if err := v.Write(metadata.ChunkID(2), []byte("bbbb")); err != nil {
		t.Fatalf("write 2: %v", err)
	}

	totalBytes, chunkCount := v.Stats()
	if chunkCount != 2 {
		t.Fatalf("Stats chunkCount=%d, want 2", chunkCount)
	}
	// Sum of logical payloads (12 bytes).
	if totalBytes != 12 {
		t.Fatalf("Stats totalBytes=%d, want 12", totalBytes)
	}

	chunks := v.ListChunks()
	if len(chunks) != 2 {
		t.Fatalf("ListChunks len=%d, want 2", len(chunks))
	}
	sizes := map[metadata.ChunkID]int64{}
	for _, c := range chunks {
		sizes[c.ChunkID] = c.Size
	}
	if sizes[metadata.ChunkID(1)] != 8 || sizes[metadata.ChunkID(2)] != 4 {
		t.Fatalf("unexpected sizes: %v", sizes)
	}

	snap := v.ChunkStateSnapshot()
	if len(snap) != 2 {
		t.Fatalf("ChunkStateSnapshot len=%d, want 2", len(snap))
	}
	for id, st := range snap {
		if st != metadata.ReplicaReady {
			t.Fatalf("chunk %d state=%v, want ReplicaReady", id, st)
		}
	}

	// Delete drops it from stats/snapshot and bumps the version.
	v0 := v.StateVersion()
	if err := v.Delete(metadata.ChunkID(1)); err != nil {
		t.Fatalf("delete 1: %v", err)
	}
	if v.StateVersion() <= v0 {
		t.Fatalf("StateVersion did not advance after delete")
	}
	_, chunkCount = v.Stats()
	if chunkCount != 1 {
		t.Fatalf("Stats chunkCount after delete=%d, want 1", chunkCount)
	}
	if _, ok := v.ChunkStateSnapshot()[metadata.ChunkID(1)]; ok {
		t.Fatalf("deleted chunk 1 still in snapshot")
	}
}

func TestV2StoreWriteErrorRateAndInterface(t *testing.T) {
	v, _ := newTestMultiStore(t, 1)
	if rate := v.WriteErrorRate(); rate != 0 {
		t.Fatalf("WriteErrorRate on succcess=%v, want 0", rate)
	}
	// Compile-time: V2Store satisfies both serving interfaces.
	var _ LocalChunkStore = v
	var _ HeartbeatStore = v
}

func TestV2StoreOverwriteAccounting(t *testing.T) {
	v, _ := newTestMultiStore(t, 1)

	// Write 4 bytes, then overwrite with 12 bytes. used-bytes must track
	// the LIVE size (12), not the sum of both generations, and the count
	// stays one chunk.
	if err := v.Write(metadata.ChunkID(10), []byte("abcd")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := v.Write(metadata.ChunkID(10), []byte("efghijklmnop")); err != nil {
		t.Fatalf("overwrite: %v", err)
	}

	total, count := v.Stats()
	if count != 1 {
		t.Fatalf("count=%d, want 1", count)
	}
	if total != 12 {
		t.Fatalf("totalBytes=%d, want 12 (live size, not 16)", total)
	}

	// The read must return the latest payload (8+12 = the overwrite).
	data, _, err := v.Read(metadata.ChunkID(10), 0, 0)
	if err != nil {
		t.Fatalf("read after overwrite: %v", err)
	}
	if string(data) != "efghijklmnop" {
		t.Fatalf("read returned %q, want %q", data, "efghijklmnop")
	}

	// Delete frees the live size; the chunk disappears from enumeration.
	if err := v.Delete(metadata.ChunkID(10)); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, count := v.Stats(); count != 0 {
		t.Fatalf("count after delete=%d, want 0", count)
	}
	if chunks := v.ListChunks(); len(chunks) != 0 {
		t.Fatalf("ListChunks after delete=%d, want 0", len(chunks))
	}
}

// TestV2StoreReadWriteBytes verifies the cumulative served-byte counters
// the heartbeat samples for disk I/O utilization: writes count the payload
// on the serving path, reads count the bytes returned, and multiple disks
// aggregate.
func TestV2StoreReadWriteBytes(t *testing.T) {
	v, _ := newTestMultiStore(t, 2)

	if r, w := v.ReadWriteBytes(); r != 0 || w != 0 {
		t.Fatalf("initial ReadWriteBytes = (%d,%d), want (0,0)", r, w)
	}
	// Write 5 bytes to disk 0, then 7 bytes to disk 1.
	if err := v.Write(metadata.ChunkID(1), []byte("aaaaa")); err != nil {
		t.Fatalf("write 1: %v", err)
	}
	if err := v.Write(metadata.ChunkID(2), []byte("bbbbbbb")); err != nil {
		t.Fatalf("write 2: %v", err)
	}
	if r, w := v.ReadWriteBytes(); r != 0 || w != 12 {
		t.Fatalf("after writes ReadWriteBytes = (%d,%d), want (0,12)", r, w)
	}
	// Read 5 bytes back from chunk 1.
	if _, _, err := v.Read(metadata.ChunkID(1), 0, 0); err != nil {
		t.Fatalf("read 1: %v", err)
	}
	if r, w := v.ReadWriteBytes(); r != 5 || w != 12 {
		t.Fatalf("after read ReadWriteBytes = (%d,%d), want (5,12)", r, w)
	}
}

// TestV2StoreImplementsDiskIOProvider pins that V2Store exposes the
// capability heartbeat samples (and ChunkStore does not, so V1 falls back
// to its always-zero DiskManager counters).
func TestV2StoreImplementsDiskIOProvider(t *testing.T) {
	var _ diskIOProvider = (*V2Store)(nil)
}

// TestHeartbeatSamplerDiskIO_V2Store verifies the heartbeat's disk-I/O
// sampling path feeds a real (nonzero) utilization from a V2Store that
// exposes ReadWriteBytes — closing the parity gap where ChunkStore's
// always-zero DiskManager counters left DiskIO perpetually 0.
func TestHeartbeatSamplerDiskIO_V2Store(t *testing.T) {
	v, _ := newTestMultiStore(t, 1)
	if err := v.Write(metadata.ChunkID(7), []byte("diskio-payload-16")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, _, err := v.Read(metadata.ChunkID(7), 0, 0); err != nil {
		t.Fatalf("read: %v", err)
	}

	// Sample with a fresh reporter (baseline lastIOBytes starts at 0), so
	// the first sample's delta equals the cumulative bytes served so far.
	sample := func() float64 {
		h := NewHeartbeatReporter(Config{NodeID: 1, HeartbeatInterval: time.Second}, &mockHeartbeatMeta{}, v)
		defer h.Stop()
		return h.sampleDiskIO()
	}

	util0 := sample()
	if err := v.Write(metadata.ChunkID(8), []byte("more-traffic-13")); err != nil {
		t.Fatalf("write 2: %v", err)
	}
	if _, _, err := v.Read(metadata.ChunkID(8), 0, 0); err != nil {
		t.Fatalf("read 2: %v", err)
	}
	util1 := sample()

	// Both samples must be non-negative and, given the served traffic is
	// nonzero, at least one must be above zero — proving the V2.1 path
	// produces a live DiskIO figure rather than V1's perpetual 0.
	if util0 < 0 || util1 < 0 {
		t.Fatalf("negative utilization: t0=%v t1=%v", util0, util1)
	}
	if util0 == 0 && util1 == 0 {
		t.Fatalf("V2Store DiskIO not sampled (both 0) despite served traffic")
	}
}

// failStore wraps a storage.Store and forces every Write to fail, letting
// tests drive write-error rate / disk-health without a real wedge.
type failStore struct{ storage.Store }

func (f *failStore) Write(_ context.Context, _ *storage.WriteRequest) (*storage.DurableReceipt, error) {
	return nil, storage.ErrStaleGeneration
}

// TestV2StoreWriteErrorRateRollingWindow pins that WriteErrorRate is a
// rolling window: each call consumes (resets) the per-disk write counters,
// matching the legacy ChunkStore (Swap(0)) rather than a lifetime ratio.
func TestV2StoreWriteErrorRateRollingWindow(t *testing.T) {
	v := NewV2Store(&failStore{})

	// Three consecutive failing writes in the current window.
	for i := 0; i < 3; i++ {
		if err := v.Write(metadata.ChunkID(1), []byte("x")); err == nil {
			t.Fatalf("expected write %d to fail", i)
		}
	}
	if rate := v.WriteErrorRate(); rate != 1.0 {
		t.Fatalf("rate after 3 failures=%v, want 1.0", rate)
	}
	// The window is now reset: a second call with no new writes reports 0
	// (empty ops), proving the counters were consumed rather than cumulative.
	if rate := v.WriteErrorRate(); rate != 0 {
		t.Fatalf("rate after empty window=%v, want 0 (window reset)", rate)
	}
}

// TestV2StoreDiskFailedPersistsAcrossRateReset verifies diskFailed and
// WriteErrorRate use independent counters: a consecutive-failure streak
// flags the disk even after WriteErrorRate resets its rolling window
// (mirroring V1, where the rolling perf window and the disk health flag
// never share state).
func TestV2StoreDiskFailedPersistsAcrossRateReset(t *testing.T) {
	v := NewV2Store(&failStore{})

	for i := 0; i < 6; i++ {
		if err := v.Write(metadata.ChunkID(1), []byte("x")); err == nil {
			t.Fatalf("expected write %d to fail", i)
		}
	}
	// Wedged: consecutive failures exceed the threshold.
	if !v.diskFailed(0) {
		t.Fatalf("disk 0 not flagged after 6 consecutive failures")
	}
	// Consuming the rolling rate window must NOT clear the health flag.
	if r := v.WriteErrorRate(); r != 1.0 {
		t.Fatalf("rate=%v want 1.0", r)
	}
	if !v.diskFailed(0) {
		t.Fatalf("diskFailed cleared by WriteErrorRate reset")
	}
}

// TestV2StoreDiskInfos verifies the management interface reports per-disk
// Index/Dir/UsedBytes/ChunkCount, deriving the values from the real
// accounting and the dirs passed at construction.
func TestV2StoreDiskInfos(t *testing.T) {
	// Build two stores with explicit dirs so DiskInfos can report them.
	dirs := []string{t.TempDir(), t.TempDir()}
	backends := make([]storage.Store, 2)
	for i := range dirs {
		s, err := segment.New(segment.Config{Dir: dirs[i], UseMemIndex: true, StreamID: 1})
		if err != nil {
			t.Fatalf("segment.New disk %d: %v", i, err)
		}
		backends[i] = s
		defer s.Close()
	}
	v := NewMultiV2Store(backends, dirs...)
	if err := v.Write(metadata.ChunkID(1), []byte("abc")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := v.Write(metadata.ChunkID(2), []byte("de")); err != nil {
		t.Fatalf("write: %v", err)
	}

	ds := v.DiskInfos()
	if len(ds) != 2 {
		t.Fatalf("DiskInfos len=%d, want 2", len(ds))
	}
	if ds[0].Dir != dirs[0] || ds[1].Dir != dirs[1] {
		t.Fatalf("DiskInfos dirs=%q/%q, want %q/%q", ds[0].Dir, ds[1].Dir, dirs[0], dirs[1])
	}
	// Both chunks land on disk 0 (least-used, tie → index 0); across disks
	// the chunk count and used bytes reflect the live payloads.
	total := ds[0].ChunkCount + ds[1].ChunkCount
	if total != 2 {
		t.Fatalf("DiskInfos ChunkCount total=%d, want 2", total)
	}
	if ds[0].UsedBytes+ds[1].UsedBytes != 5 {
		t.Fatalf("DiskInfos UsedBytes total=%d, want 5", ds[0].UsedBytes+ds[1].UsedBytes)
	}
	if ds[0].Failed || ds[1].Failed {
		t.Fatalf("DiskInfos flagged healthy disks as failed: %v", ds)
	}
}

// TestV2StoreVerifyChunkData verifies checksum validation re-reads the
// chunk and reports integrity matching (or mismatch) against the recorded
// checksum.
func TestV2StoreVerifyChunkData(t *testing.T) {
	v, _ := newTestMultiStore(t, 1)
	if err := v.Write(metadata.ChunkID(5), []byte("verify me")); err != nil {
		t.Fatalf("write: %v", err)
	}

	ok, cksum, err := v.VerifyChunkData(metadata.ChunkID(5))
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if !ok {
		t.Fatalf("verify reported mismatch (cksum=%x)", cksum)
	}
	if cksum != storage.CRC32C([]byte("verify me")) {
		t.Fatalf("verify checksum=%x, want %x", cksum, storage.CRC32C([]byte("verify me")))
	}

	// A missing chunk reports not-found.
	if _, _, err := v.VerifyChunkData(metadata.ChunkID(999)); err == nil {
		t.Fatalf("verify of missing chunk returned nil error")
	}
}

// TestV2StoreWriteGen_MetadataAuthoritativeGeneration proves the Metadata V2
// fencing contract: WriteGen places a chunk under the generation issued by the
// metadata service, so all replicas land on the same authoritative generation,
// and an overwrite changes generation based on the metadata value, not a local
// gen+1 bump.
func TestV2StoreWriteGen_MetadataAuthoritativeGeneration(t *testing.T) {
	v, _ := newTestMultiStore(t, 2)

	// Fresh chunk written under metadata generation 1.
	if err := v.WriteGen(metadata.ChunkID(501), 1, []byte("gen-one")); err != nil {
		t.Fatalf("WriteGen gen1: %v", err)
	}
	v.mu.RLock()
	loc := v.locOf[metadata.ChunkID(501)]
	v.mu.RUnlock()
	if loc.gen != 1 {
		t.Fatalf("chunk 501 gen=%d, want 1 (metadata-issued)", loc.gen)
	}

	// Overwrite with a metadata-issued generation 3 (not local 2). The store
	// must honor exactly that generation.
	if err := v.WriteGen(metadata.ChunkID(501), 3, []byte("gen-three-payload")); err != nil {
		t.Fatalf("WriteGen gen3: %v", err)
	}
	v.mu.RLock()
	loc = v.locOf[metadata.ChunkID(501)]
	v.mu.RUnlock()
	if loc.gen != 3 {
		t.Fatalf("chunk 501 gen=%d after overwrite, want 3 (metadata-issued, not local bump)", loc.gen)
	}

	// The latest generation is what reads resolve.
	data, _, err := v.Read(metadata.ChunkID(501), 0, 0)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(data) != "gen-three-payload" {
		t.Fatalf("read=%q, want latest metadata-issued generation payload", data)
	}
}

// TestV2StoreWriteGen_StaleGenerationFenced proves that an idempotent replay
// (same generation + same data) succeeds, while a stale generation write whose
// payload differs from the already-committed one is rejected by the segment
// store's fencing — the datanode reflects that error upward instead of
// silently overwriting.
func TestV2StoreWriteGen_StaleGenerationFenced(t *testing.T) {
	v, _ := newTestMultiStore(t, 1)

	if err := v.WriteGen(metadata.ChunkID(601), 1, []byte("committed-at-gen-1")); err != nil {
		t.Fatalf("initial WriteGen: %v", err)
	}

	// Idempotent replay of the same (gen, payload) is accepted.
	if err := v.WriteGen(metadata.ChunkID(601), 1, []byte("committed-at-gen-1")); err != nil {
		t.Fatalf("idempotent replay should succeed, got: %v", err)
	}

	// Writing gen 1 again with a DIFFERENT payload must be fenced (the older
	// generation has already been committed with a different checksum).
	if err := v.WriteGen(metadata.ChunkID(601), 1, []byte("different-payload")); err == nil {
		t.Fatalf("expected stale-generation write at gen 1 to be fenced, but it succeeded")
	}
}
