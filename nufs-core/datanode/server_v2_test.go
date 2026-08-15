package datanode

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dshmyz/nufs/nufs-core/datanode/storage"
	"github.com/dshmyz/nufs/nufs-core/datanode/storage/segment"
	"github.com/dshmyz/nufs/nufs-core/metadata"
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
	// Pass the per-disk dirs through so the V2Store's disk Dir fields are
	// populated (as in runDataNodeV21's NewMultiV2Store(stores, dataDirs...)),
	// which DiskInfos/capacity-overview rely on for the Statfs total.
	v := NewMultiV2Store(stores, dirs...)
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

// TestV2StoreDrainWrites verifies the V2.1-internal write barrier
// (QuiesceWrites — the §4 shutdown drain): it blocks until an in-flight
// write completes, then blocks new writes until the returned release func is
// called, after which writes resume. Reads are never blocked by a drain
// (they do not take drainMu).
func TestV2StoreDrainWrites(t *testing.T) {
	v, _ := newTestMultiStore(t, 2)

	// Simulate an in-flight write: hold drainMu.RLock exactly as writeTo /
	// writeShardAt / RebalanceOne hold it for their duration.
	v.drainMu.RLock()

	drainDone := make(chan func(), 1)
	go func() {
		release, err := v.QuiesceWrites(context.Background())
		if err != nil {
			t.Errorf("DrainWrites: %v", err)
			close(drainDone)
			return
		}
		drainDone <- release
	}()

	// While the write is in flight, the drain must not have returned.
	select {
	case _, ok := <-drainDone:
		if ok {
			t.Fatal("DrainWrites returned while a write was still in flight")
		}
		t.Fatal("DrainWrites errored")
	case <-time.After(50 * time.Millisecond):
		// Good: drain is blocked on the in-flight write.
	}

	// The in-flight write completes; the drain now acquires the barrier.
	v.drainMu.RUnlock()
	var release func()
	select {
	case release = <-drainDone:
	case <-time.After(2 * time.Second):
		t.Fatal("DrainWrites did not acquire the barrier after the write drained")
	}

	// While drained, a concurrent write must block.
	writeStarted := make(chan struct{})
	writeDone := make(chan error, 1)
	go func() {
		close(writeStarted)
		writeDone <- v.Write(metadata.ChunkID(3033), []byte("blocked-while-drained"))
	}()
	<-writeStarted
	select {
	case <-time.After(50 * time.Millisecond):
		// Good: the write stayed blocked behind the held barrier.
	case <-writeDone:
		t.Fatal("write proceeded while the store was drained")
	}

	// Release: the blocked write now lands and is readable. Reads were never
	// gated, so a read issued during the drain already returns the old state.
	release()
	if err := <-writeDone; err != nil {
		t.Fatalf("post-drain write: %v", err)
	}
	got, _, err := v.Read(metadata.ChunkID(3033), 0, 0)
	if err != nil || string(got) != "blocked-while-drained" {
		t.Fatalf("post-drain write not readable: got=%q err=%v", got, err)
	}
}

// TestV2StoreDrainWritesTimeoutSelfHeals verifies the timeout path of the
// drain barrier: when QuiesceWrites cannot acquire the barrier before its
// deadline, it returns the deadline error and the store self-heals (the
// barrier is not leaked), so writes resume once the in-flight write clears.
func TestV2StoreDrainWritesTimeoutSelfHeals(t *testing.T) {
	v, _ := newTestMultiStore(t, 1)

	// Hold a write in flight so the barrier cannot be acquired, forcing the
	// deadline to fire deterministically.
	v.drainMu.RLock()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	release, err := v.QuiesceWrites(ctx)
	if err == nil {
		if release != nil {
			release()
		}
		t.Fatal("DrainWrites with held write returned nil error, want timeout")
	}
	// The in-flight write clears; the DrainWrites goroutine acquires then
	// releases the barrier (self-heal), so the store accepts writes again.
	v.drainMu.RUnlock()

	var werr error
	for i := 0; i < 50; i++ {
		if werr = v.Write(metadata.ChunkID(4044), []byte("after-timeout")); werr == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if werr != nil {
		t.Fatalf("write after drained timeout: %v", werr)
	}
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

// mockHeartbeatMeta is a minimal HeartbeatMeta for tests that only need
// the sampler path (no real metadata round-trip).
type mockHeartbeatMeta struct{}

func (m *mockHeartbeatMeta) Heartbeat(_ context.Context, _ metadata.NodeID, _ *metadata.NodeReport) error {
	return nil
}
func (m *mockHeartbeatMeta) AckChangeEvents(_ context.Context, _ metadata.NodeID, _ uint64) (uint64, error) {
	return 0, nil
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

// ============ Program 4 V1-c: V2Store disk health state machine ============

// toggleFailStore wraps a storage.Store and can be flipped to make every Write
// fail, driving the V2Store's failCount/diskState health signal deterministically.
// It embeds the interface so all non-Write methods forward unchanged; only Write
// is overridden to delegate through the real store when not failing.
type toggleFailStore struct {
	storage.Store
	real storage.Store
	fail atomic.Bool
}

func (t *toggleFailStore) Write(ctx context.Context, r *storage.WriteRequest) (*storage.DurableReceipt, error) {
	if t.fail.Load() {
		return nil, storage.ErrStaleGeneration
	}
	return t.real.Write(ctx, r)
}

// TestV2StoreDiskStateTiers proves the 3-tier health model: failCount 0 ->
// DiskOnline, 1..4 -> DiskDegraded, >=5 -> DiskFailed, and that DiskInfos /
// DiskStats surface the derived State (and Failed boolean) to the ops/heartbeat
// channels — closing the V1.DiskManager parity gap without a DiskManager slot.
func TestV2StoreDiskStateTiers(t *testing.T) {
	v, _ := newTestMultiStore(t, 2)

	// Online at failCount 0.
	if st := v.diskState(0); st != DiskOnline {
		t.Fatalf("diskState(0) with no failures=%v, want online", st)
	}
	if v.diskFailed(0) {
		t.Fatalf("diskFailed true at failCount 0")
	}

	// Degraded across the 1..4 band.
	for _, n := range []int64{1, 2, 4} {
		v.disks[0].failCount.Store(n)
		if st := v.diskState(0); st != DiskDegraded {
			t.Fatalf("diskState at failCount=%d=%v, want degraded", n, st)
		}
		if v.diskFailed(0) {
			t.Fatalf("diskFailed true at degraded failCount=%d", n)
		}
	}

	// Failed at >=5.
	v.disks[0].failCount.Store(5)
	if st := v.diskState(0); st != DiskFailed {
		t.Fatalf("diskState at failCount=5=%v, want failed", st)
	}
	if !v.diskFailed(0) {
		t.Fatalf("diskFailed false at failCount=5")
	}

	// DiskInfos / DiskStats surface State and Failed for the failed disk only.
	infos := v.DiskInfos()
	if len(infos) != 2 {
		t.Fatalf("DiskInfos len=%d, want 2", len(infos))
	}
	if !infos[0].Failed || infos[0].State != DiskFailed {
		t.Fatalf("DiskInfos[0] failed=%v state=%v, want failed/failed", infos[0].Failed, infos[0].State)
	}
	if infos[1].Failed || infos[1].State != DiskOnline {
		t.Fatalf("DiskInfos[1] failed=%v state=%v, want healthy/online", infos[1].Failed, infos[1].State)
	}

	ds := v.DiskStats()
	if !ds[0].Failed || ds[0].State != DiskFailed || ds[1].Failed || ds[1].State != DiskOnline {
		t.Fatalf("DiskStats wrong: %+v", ds)
	}
}

// TestV2StoreNextLocAndAdmissionSkipFailedDisk proves the placement-gap fix:
// once a disk enters DiskFailed, nextLoc routes NEW chunks to a healthy disk
// (matching leastUsedDisk), and writeTo ADMISSION rejects writing to the failed
// disk (an overwrite of a chunk already living on it). A merely-degraded disk
// stays eligible so it isn't starved of the success that clears its streak.
func TestV2StoreNextLocAndAdmissionSkipFailedDisk(t *testing.T) {
	v, _ := newTestMultiStore(t, 2)

	// Write one chunk so disk 0 has bytes; disk 0 is the least-used healthy target.
	if err := v.Write(metadata.ChunkID(701), []byte("seed")); err != nil {
		t.Fatalf("seed write: %v", err)
	}

	// The seed chunk lives on disk 0 (least-used tie -> index 0).
	v.mu.RLock()
	d0 := v.locOf[metadata.ChunkID(701)].disk
	v.mu.RUnlock()
	if d0 != 0 {
		t.Fatalf("seed chunk on disk %d, want 0", d0)
	}

	// Mark disk 0 failed (simulates the streak crossing the >=5 threshold).
	v.disks[0].failCount.Store(5)

	// NEW chunk: nextLoc must skip the failed disk and land on disk 1.
	disk, _ := v.nextLoc(metadata.ChunkID(9999))
	if disk != 1 {
		t.Fatalf("nextLoc on failed disk0 -> disk %d, want 1", disk)
	}
	if err := v.Write(metadata.ChunkID(702), []byte("new-lives-on-healthy-disk")); err != nil {
		t.Fatalf("write on healthy disk: %v", err)
	}
	v.mu.RLock()
	loc := v.locOf[metadata.ChunkID(702)]
	v.mu.RUnlock()
	if loc.disk != 1 {
		t.Fatalf("new chunk 702 on disk %d, want 1 (failed disk skipped)", loc.disk)
	}

	// OVERWRITE of the chunk living on the failed disk: admission must reject it.
	// The store knows writeTo(disk=0) targets the FAILED disk -> error propagates.
	if err := v.Write(metadata.ChunkID(701), []byte("rewrite-failed-disk-payload")); err == nil {
		t.Fatalf("expected write to FAILED disk to be rejected, but it succeeded")
	}

	// A DEGRADED disk (failCount 1..4) remains an eligible placement target.
	v.disks[0].failCount.Store(2)
	disk, _ = v.nextLoc(metadata.ChunkID(9998))
	if disk != 0 {
		t.Fatalf("nextLoc on degraded disk0 -> disk %d, want 0 (degraded stays eligible)", disk)
	}
}

// TestV2StoreFailStreakDrivesHealth proves the organic path: real failing writes
// to a disk climb failCount and degrade it, a success clears the streak and
// recovers a DEGRADED disk back to online, and a sustained streak crossing the
// >=5 threshold flags the disk FAILED — after which writeTo admission refuses
// further writes (the failed disk is read-only). Uses a toggle-failing backend
// so the streak is driven by the actual write path rather than poking internals.
func TestV2StoreFailStreakDrivesHealth(t *testing.T) {
	// Build a 2-disk store where disk 0's backend can be flipped to fail writes.
	dirs := []string{t.TempDir(), t.TempDir()}
	backends := make([]storage.Store, 2)
	cleanup := make([]*segment.Store, 2)
	for i := range dirs {
		s, err := segment.New(segment.Config{Dir: dirs[i], UseMemIndex: true, StreamID: 1})
		if err != nil {
			t.Fatalf("segment.New disk %d: %v", i, err)
		}
		cleanup[i] = s
		backends[i] = s
	}
	defer func() {
		for _, s := range cleanup {
			s.Close()
		}
	}()
	tog := &toggleFailStore{Store: backends[0], real: backends[0]}
	backends[0] = tog
	v := NewMultiV2Store(backends)

	// Degrade disk 0 via the real write path (writeTo -> store.Write).
	tog.fail.Store(true)
	for i := 1; i <= 2; i++ {
		if err := v.writeTo(metadata.ChunkID(800), 0, storage.Generation(i), []byte("failing-write")); err == nil {
			t.Fatalf("write %d to failed-over backend succeeded, want error", i)
		}
		if st := v.diskState(0); st != DiskDegraded {
			t.Fatalf("after %d failed writes diskState=%v, want degraded", i, st)
		}
	}

	// A degraded disk is still eligible for writes; a success clears the streak,
	// recovering the disk back to online (health recovers organically).
	tog.fail.Store(false)
	if err := v.writeTo(metadata.ChunkID(801), 0, storage.Generation(1), []byte("recovery")); err != nil {
		t.Fatalf("recovery write failed: %v", err)
	}
	if st := v.diskState(0); st != DiskOnline {
		t.Fatalf("diskState after recovery=%v, want online", st)
	}

	// A sustained streak crossing the >=5 threshold flags the disk FAILED; writeTo
	// admission then refuses further writes (the failed disk is read-only).
	tog.fail.Store(true)
	for i := 1; i <= 5; i++ {
		v.writeTo(metadata.ChunkID(802), 0, storage.Generation(i), []byte("failing-write"))
	}
	if !v.diskFailed(0) {
		t.Fatalf("disk 0 not failed after 5-failure streak")
	}
	tog.fail.Store(false)
	if err := v.writeTo(metadata.ChunkID(803), 0, storage.Generation(1), []byte("post-fail")); err == nil {
		t.Fatalf("expected writeTo to reject write to FAILED disk, but it succeeded")
	}
}

// TestV2Store_DeleteShard_CleansShardDiskOf verifies that DeleteShard removes
// the per-shard entry from shardDiskOf, and cleans up the parent map entry
// when the last shard for a chunk is deleted.
func TestV2Store_DeleteShard_CleansShardDiskOf(t *testing.T) {
	v, _ := newTestShardMultiStore(t, 3)
	cid := metadata.ChunkID(9001)

	// Populate shardDiskOf manually (mimicking what WriteChunkEC does).
	v.mu.Lock()
	v.shardDiskOf[cid] = map[int]int{0: 0, 1: 1, 2: 2}
	v.mu.Unlock()

	// Delete shard 1 — inner map should still have entries 0 and 2.
	if err := v.DeleteShard(cid, 1); err != nil {
		t.Fatalf("DeleteShard: %v", err)
	}
	v.mu.Lock()
	m := v.shardDiskOf[cid]
	if len(m) != 2 {
		t.Fatalf("after deleting shard 1: expected 2 entries, got %d", len(m))
	}
	if _, ok := m[1]; ok {
		t.Fatal("shard 1 entry should have been removed")
	}
	v.mu.Unlock()

	// Delete remaining shards — parent map entry should be cleaned up.
	if err := v.DeleteShard(cid, 0); err != nil {
		t.Fatalf("DeleteShard 0: %v", err)
	}
	if err := v.DeleteShard(cid, 2); err != nil {
		t.Fatalf("DeleteShard 2: %v", err)
	}
	v.mu.Lock()
	if _, ok := v.shardDiskOf[cid]; ok {
		t.Fatal("shardDiskOf parent entry should have been removed after all shards deleted")
	}
	v.mu.Unlock()
}

// TestV2Store_Delete_CleansShardDiskOf verifies that Delete removes the
// chunk's entire shardDiskOf entry alongside the locOf entry.
func TestV2Store_Delete_CleansShardDiskOf(t *testing.T) {
	v, _ := newTestMultiStore(t, 2)
	cid := metadata.ChunkID(9002)
	payload := []byte("delete-with-shard-cleanup")

	if err := v.Write(cid, payload); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Manually add a shardDiskOf entry (simulating a chunk that was EC-encoded).
	v.mu.Lock()
	v.shardDiskOf[cid] = map[int]int{0: 0, 1: 1}
	v.mu.Unlock()

	if err := v.Delete(cid); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	v.mu.Lock()
	defer v.mu.Unlock()
	if _, ok := v.locOf[cid]; ok {
		t.Fatal("locOf entry should have been removed")
	}
	if _, ok := v.shardDiskOf[cid]; ok {
		t.Fatal("shardDiskOf entry should have been removed")
	}
}
