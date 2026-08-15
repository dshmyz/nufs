package datanode

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"

	"github.com/dshmyz/nufs/nufs-core/datanode/storage"
	"github.com/dshmyz/nufs/nufs-core/datanode/storage/segment"
	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// newSmallTestV2Store builds a single-disk V2Store with an attached
// small-file stream over real segment stores, returning the V2Store plus the
// raw data/small stores so tests can assert where extents actually landed.
func newSmallTestV2Store(t *testing.T) (*V2Store, *segment.SmallStore, *segment.Store) {
	t.Helper()
	d := t.TempDir()
	ds, err := segment.New(segment.Config{Dir: d, UseMemIndex: true, StreamID: 1})
	if err != nil {
		t.Fatalf("data store: %v", err)
	}
	sm, err := segment.NewSmallStore(segment.Config{Dir: d, UseMemIndex: true})
	if err != nil {
		t.Fatalf("small store: %v", err)
	}
	t.Cleanup(func() { _ = sm.Close() })
	t.Cleanup(func() { _ = ds.Close() })
	v := NewMultiV2Store([]storage.Store{ds})
	if err := v.AttachSmallStores([]storage.Store{sm}); err != nil {
		t.Fatalf("AttachSmallStores: %v", err)
	}
	return v, sm, ds
}

func storeHas(t *testing.T, s storage.Store, cid metadata.ChunkID, gen storage.Generation, want []byte) bool {
	t.Helper()
	res, err := s.Read(context.Background(), &storage.ReadRequest{
		ExtentID: storage.ExtentID(cid), Generation: gen,
	})
	if err != nil {
		return false
	}
	return bytes.Equal(res.Data, want)
}

// TestV2SmallStore_RoutesSmallChunksToSmallStream verifies a new chunk ≤
// SmallFileThreshold lands in the small stream (not the data stream) and is
// served byte-exact through the V2Store read path, plus Info/ListChunks/Stats
// visibility.
func TestV2SmallStore_RoutesSmallChunksToSmallStream(t *testing.T) {
	v, sm, ds := newSmallTestV2Store(t)
	cid := metadata.ChunkID(60001)
	payload := make([]byte, 4096)
	for i := range payload {
		payload[i] = byte(i * 13)
	}
	if err := v.Write(cid, payload); err != nil {
		t.Fatalf("Write: %v", err)
	}
	got, _, err := v.Read(cid, 0, 0)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("read mismatch: got %d bytes, want %d", len(got), len(payload))
	}
	if !storeHas(t, sm, cid, 1, payload) {
		t.Fatalf("small chunk missing from small stream")
	}
	if storeHas(t, ds, cid, 1, payload) {
		t.Fatalf("small chunk leaked into data stream")
	}
	info, ok := v.Info(cid)
	if !ok || info.Size != int64(len(payload)) {
		t.Fatalf("Info: ok=%v size=%d", ok, info.Size)
	}
	found := false
	for _, ci := range v.ListChunks() {
		if ci.ChunkID == cid {
			found = true
		}
	}
	if !found {
		t.Fatalf("ListChunks missing small chunk")
	}
	total, count := v.Stats()
	if count < 1 || total < int64(len(payload)) {
		t.Fatalf("Stats: bytes=%d count=%d", total, count)
	}
}

// TestV2SmallStore_OverwriteStaysSmall verifies a same-size rewrite of a
// small chunk stays in the small stream at the next generation.
func TestV2SmallStore_OverwriteStaysSmall(t *testing.T) {
	v, sm, ds := newSmallTestV2Store(t)
	cid := metadata.ChunkID(60002)
	if err := v.Write(cid, bytes.Repeat([]byte{1}, 100)); err != nil {
		t.Fatalf("Write: %v", err)
	}
	v2 := bytes.Repeat([]byte{2}, 100)
	if err := v.Write(cid, v2); err != nil {
		t.Fatalf("rewrite: %v", err)
	}
	got, _, err := v.Read(cid, 0, 0)
	if err != nil || !bytes.Equal(got, v2) {
		t.Fatalf("read after rewrite: got %d err %v", len(got), err)
	}
	if !storeHas(t, sm, cid, 2, v2) {
		t.Fatalf("rewrite did not stay in small stream")
	}
	if storeHas(t, ds, cid, 2, v2) {
		t.Fatalf("rewrite leaked into data stream")
	}
}

// TestV2SmallStore_GrowthMigratesToDataStream verifies a small chunk that
// outgrows SmallFileThreshold migrates to the data stream and the stale
// small record is tombstoned.
func TestV2SmallStore_GrowthMigratesToDataStream(t *testing.T) {
	v, sm, ds := newSmallTestV2Store(t)
	cid := metadata.ChunkID(60003)
	small := bytes.Repeat([]byte{3}, 100)
	if err := v.Write(cid, small); err != nil {
		t.Fatalf("Write: %v", err)
	}
	big := bytes.Repeat([]byte{4}, storage.SmallFileThreshold+1)
	if err := v.Write(cid, big); err != nil {
		t.Fatalf("growth write: %v", err)
	}
	got, _, err := v.Read(cid, 0, 0)
	if err != nil || !bytes.Equal(got, big) {
		t.Fatalf("read after growth: got %d err %v", len(got), err)
	}
	if !storeHas(t, ds, cid, 2, big) {
		t.Fatalf("grown payload missing from data stream")
	}
	if storeHas(t, sm, cid, 1, small) {
		t.Fatalf("stale small record not tombstoned")
	}
	info, ok := v.Info(cid)
	if !ok || info.Size != int64(len(big)) {
		t.Fatalf("Info after growth: ok=%v size=%d want %d", ok, info.Size, len(big))
	}
}

// TestV2SmallStore_WriteGenHonorsMetadataGen verifies the metadata-issued
// generation path routes and fences small writes the same way.
func TestV2SmallStore_WriteGenHonorsMetadataGen(t *testing.T) {
	v, sm, _ := newSmallTestV2Store(t)
	cid := metadata.ChunkID(60004)
	payload := bytes.Repeat([]byte{5}, 200)
	if err := v.WriteGen(cid, 7, payload); err != nil {
		t.Fatalf("WriteGen: %v", err)
	}
	if !storeHas(t, sm, cid, 7, payload) {
		t.Fatalf("WriteGen small chunk not in small stream at gen 7")
	}
	if err := v.WriteGen(cid, 7, bytes.Repeat([]byte{9}, 200)); err == nil {
		t.Fatalf("expected same-generation fence rejection, got nil")
	}
}

// TestV2SmallStore_DeleteRemovesFromSmallStream verifies Delete resolves the
// small location and removes it from enumeration.
func TestV2SmallStore_DeleteRemovesFromSmallStream(t *testing.T) {
	v, _, _ := newSmallTestV2Store(t)
	cid := metadata.ChunkID(60005)
	if err := v.Write(cid, bytes.Repeat([]byte{6}, 100)); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := v.Delete(cid); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	for _, ci := range v.ListChunks() {
		if ci.ChunkID == cid {
			t.Fatalf("deleted small chunk still listed")
		}
	}
	if _, _, err := v.Read(cid, 0, 0); err == nil {
		t.Fatalf("read after delete succeeded")
	}
}

// TestV2SmallStore_RestartReconstruction verifies a fresh V2Store over the
// same stores (startup enumeration) resolves small chunks' locations.
func TestV2SmallStore_RestartReconstruction(t *testing.T) {
	d := t.TempDir()
	ds, err := segment.New(segment.Config{Dir: d, UseMemIndex: true, StreamID: 1})
	if err != nil {
		t.Fatalf("data store: %v", err)
	}
	sm, err := segment.NewSmallStore(segment.Config{Dir: d, UseMemIndex: true})
	if err != nil {
		t.Fatalf("small store: %v", err)
	}
	t.Cleanup(func() { _ = sm.Close() })
	t.Cleanup(func() { _ = ds.Close() })

	v1 := NewMultiV2Store([]storage.Store{ds})
	if err := v1.AttachSmallStores([]storage.Store{sm}); err != nil {
		t.Fatalf("AttachSmallStores: %v", err)
	}
	cid := metadata.ChunkID(60006)
	payload := bytes.Repeat([]byte{7}, 300)
	if err := v1.Write(cid, payload); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Simulate restart: a brand-new V2Store enumerates the same stores.
	v2 := NewMultiV2Store([]storage.Store{ds})
	if err := v2.AttachSmallStores([]storage.Store{sm}); err != nil {
		t.Fatalf("re-attach: %v", err)
	}
	got, _, err := v2.Read(cid, 0, 0)
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("post-restart read: got %d err %v", len(got), err)
	}
}

// TestV2SmallStore_NoSmallStoresFallsBackToDataStream verifies the legacy
// behavior is unchanged when no small streams are attached.
func TestV2SmallStore_NoSmallStoresFallsBackToDataStream(t *testing.T) {
	d := t.TempDir()
	ds, err := segment.New(segment.Config{Dir: d, UseMemIndex: true, StreamID: 1})
	if err != nil {
		t.Fatalf("data store: %v", err)
	}
	t.Cleanup(func() { _ = ds.Close() })
	v := NewMultiV2Store([]storage.Store{ds}) // no small stores attached
	cid := metadata.ChunkID(60007)
	payload := bytes.Repeat([]byte{8}, 100)
	if err := v.Write(cid, payload); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if !storeHas(t, ds, cid, 1, payload) {
		t.Fatalf("chunk missing from data stream without small stores")
	}
}

// TestV2SmallStore_AttachCountMismatch verifies AttachSmallStores rejects a
// count that does not match the data disks.
func TestV2SmallStore_AttachCountMismatch(t *testing.T) {
	d := t.TempDir()
	ds, err := segment.New(segment.Config{Dir: d, UseMemIndex: true, StreamID: 1})
	if err != nil {
		t.Fatalf("data store: %v", err)
	}
	t.Cleanup(func() { _ = ds.Close() })
	v := NewMultiV2Store([]storage.Store{ds})
	if err := v.AttachSmallStores([]storage.Store{ds, ds}); err == nil {
		t.Fatalf("expected count-mismatch error, got nil")
	}
}

// TestV2SmallStore_AddDiskAttachesSmallStream verifies runtime hot-add of a
// disk (DiskLifecycleOps.AddDisk) builds AND attaches a small-file stream for
// the new disk, exactly like the startup disks: len(v.small) stays parallel to
// len(v.disks), and a new chunk ≤ SmallFileThreshold routes to the new disk's
// small stream (not the old ones). This closes the 1.2 gap where AddDisk built
// only data + EC-shard stores, leaving a small-less disk that never received
// small files.
func TestV2SmallStore_AddDiskAttachesSmallStream(t *testing.T) {
	d := t.TempDir()
	ds, err := segment.New(segment.Config{Dir: d, UseMemIndex: true, StreamID: 1})
	if err != nil {
		t.Fatalf("data store: %v", err)
	}
	sm, err := segment.NewSmallStore(segment.Config{Dir: d, UseMemIndex: true})
	if err != nil {
		t.Fatalf("small store: %v", err)
	}
	t.Cleanup(func() { _ = sm.Close() })
	t.Cleanup(func() { _ = ds.Close() })
	v := NewMultiV2Store([]storage.Store{ds})
	if err := v.AttachSmallStores([]storage.Store{sm}); err != nil {
		t.Fatalf("AttachSmallStores: %v", err)
	}

	// A factory mirroring runDataNodeV21.newDiskStores: data (StreamID 1),
	// shard (StreamID 2) and small (StreamID 0) segment stores per adopted dir.
	newDir := t.TempDir()
	var adoptedData, adoptedShard, adoptedSmall storage.Store
	v.SetDiskFactory(func(dir string) (storage.Store, storage.Store, storage.Store, error) {
		data, err := segment.New(segment.Config{Dir: dir, IndexDir: filepath.Join(dir, "index"), UseMemIndex: true, StreamID: 1})
		if err != nil {
			return nil, nil, nil, err
		}
		shard, err := segment.New(segment.Config{Dir: dir, IndexDir: filepath.Join(dir, "index-ecshard"), UseMemIndex: true, StreamID: 2})
		if err != nil {
			_ = data.Close()
			return nil, nil, nil, err
		}
		small, err := segment.NewSmallStore(segment.Config{Dir: dir, IndexDir: filepath.Join(dir, "index-small"), UseMemIndex: true})
		if err != nil {
			_ = data.Close()
			_ = shard.Close()
			return nil, nil, nil, err
		}
		adoptedData, adoptedShard, adoptedSmall = data, shard, small
		return data, shard, small, nil
	})
	t.Cleanup(func() { closeStore(adoptedSmall) })
	t.Cleanup(func() { closeStore(adoptedShard) })
	t.Cleanup(func() { closeStore(adoptedData) })

	// Seed disk 0's small stream so the fresh adopted disk is the strict
	// least-used small target for the post-adopt write.
	seed := bytes.Repeat([]byte{7}, 100)
	if err := v.Write(metadata.ChunkID(70000), seed); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	if loc, ok := v.currentLoc(metadata.ChunkID(70000)); !ok || loc.disk != 0 || !loc.small {
		t.Fatalf("seed chunk not on disk 0 small stream: ok=%v loc=%+v", ok, loc)
	}

	idx, err := v.AddDisk(newDir, 8, 8)
	if err != nil {
		t.Fatalf("AddDisk: %v", err)
	}
	if idx != 1 {
		t.Fatalf("AddDisk returned index %d, want 1", idx)
	}
	if len(v.small) != len(v.disks) {
		t.Fatalf("after AddDisk small=%d disks=%d, want parallel (small-capable store)", len(v.small), len(v.disks))
	}

	cid := metadata.ChunkID(70001)
	payload := make([]byte, 4096)
	for i := range payload {
		payload[i] = byte(i * 17)
	}
	if err := v.Write(cid, payload); err != nil {
		t.Fatalf("Write: %v", err)
	}
	loc, ok := v.currentLoc(cid)
	if !ok || !loc.small || loc.disk != idx {
		t.Fatalf("small chunk after AddDisk: ok=%v small=%v disk=%d, want small=true disk=%d", ok, loc.small, loc.disk, idx)
	}
	got, _, err := v.Read(cid, 0, 0)
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("read after AddDisk: got %d bytes err %v", len(got), err)
	}
	if !storeHas(t, adoptedSmall, cid, 1, payload) {
		t.Fatalf("adopted disk's small stream missing the chunk")
	}
	if storeHas(t, adoptedData, cid, 1, payload) {
		t.Fatalf("small chunk leaked into adopted disk's data stream")
	}
}
