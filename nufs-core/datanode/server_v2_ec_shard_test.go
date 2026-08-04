package datanode

import (
	"testing"

	"github.com/example/dfs/datanode/storage"
	"github.com/example/dfs/datanode/storage/segment"
	"github.com/example/dfs/metadata"
)

// newTestShardMultiStore builds a V2Store over n disks, each hosting both a
// data segment store (StreamID 1) and an EC-shard segment store (StreamID 2,
// class dir "ecshard") on a temp dir. Shard stores are attached so
// WriteShard/ReadShard route into the shard stream. Returns the store and the
// per-disk dirs (so a crash-restart test can reopen both stores).
func newTestShardMultiStore(t *testing.T, n int) (*V2Store, []string) {
	t.Helper()
	dirs := make([]string, n)
	var dataStores, shardStores []storage.Store
	for i := 0; i < n; i++ {
		d := t.TempDir()
		dirs[i] = d
		ds, err := segment.New(segment.Config{Dir: d, UseMemIndex: true, StreamID: 1})
		if err != nil {
			t.Fatalf("segment.New data %d: %v", i, err)
		}
		ss, err := segment.New(segment.Config{Dir: d, UseMemIndex: true, StreamID: 2})
		if err != nil {
			t.Fatalf("segment.New shard %d: %v", i, err)
		}
		dataStores = append(dataStores, ds)
		shardStores = append(shardStores, ss)
	}
	v := NewMultiV2Store(dataStores, dirs...)
	if err := v.AttachShardStores(shardStores); err != nil {
		t.Fatalf("AttachShardStores: %v", err)
	}
	t.Cleanup(func() {
		for _, s := range shardStores {
			if c, ok := s.(interface{ Close() error }); ok {
				c.Close()
			}
		}
		for _, s := range dataStores {
			if c, ok := s.(interface{ Close() error }); ok {
				c.Close()
			}
		}
	})
	return v, dirs
}

// TestV2StoreShardIO verifies EC shards are stored as independent,
// byte-exact extents in a shard stream distinct from the data stream: multi
// shards of one chunk are distinct, and a whole-chunk write under the same
// chunk ID never collides with its shards (the separate "ecshard" namespace,
// not any bit layout, guarantees disjointness).
func TestV2StoreShardIO(t *testing.T) {
	v, _ := newTestShardMultiStore(t, 2)
	cid := metadata.ChunkID(1001)

	shardData := map[int]string{0: "shard-zero-payload", 1: "shard-one-payload", 8: "parity-shard"}
	for s, p := range shardData {
		if err := v.WriteShard(cid, s, []byte(p)); err != nil {
			t.Fatalf("WriteShard(%d,%d): %v", cid, s, err)
		}
		data, _, err := v.ReadShard(cid, s)
		if err != nil {
			t.Fatalf("ReadShard(%d,%d): %v", cid, s, err)
		}
		if string(data) != p {
			t.Fatalf("ReadShard(%d,%d) = %q, want %q", cid, s, data, p)
		}
	}

	// Whole-chunk write under the same chunk ID reads back its own payload (a
	// data-stream extent), not a shard fragment; shards stay intact.
	if err := v.Write(cid, []byte("whole-chunk")); err != nil {
		t.Fatalf("Write chunk: %v", err)
	}
	wp, _, err := v.Read(cid, 0, 0)
	if err != nil {
		t.Fatalf("Read chunk: %v", err)
	}
	if string(wp) != "whole-chunk" {
		t.Fatalf("whole-chunk read = %q, want %q", wp, "whole-chunk")
	}
	for s, p := range shardData {
		if data, _, err := v.ReadShard(cid, s); err != nil || string(data) != p {
			t.Fatalf("shard %d corrupted after whole-chunk write: data=%q err=%v", s, data, err)
		}
	}

	// Shards are individually deletable without touching the chunk or other
	// shards.
	if err := v.DeleteShard(cid, 0); err != nil {
		t.Fatalf("DeleteShard: %v", err)
	}
	if _, _, err := v.ReadShard(cid, 0); err == nil {
		t.Fatal("expected deleted shard 0 to be unreadable")
	}
	if data, _, err := v.ReadShard(cid, 1); err != nil || string(data) != shardData[1] {
		t.Fatalf("shard 1 should survive deletion of shard 0: data=%q err=%v", data, err)
	}
}

// TestV2StoreShardRestart verifies a shard extent is durable and routable
// after a restart: closing the data+shard segment stores and reconstructing
// the V2Store (startup reconstruction re-derives the shard stripe home for
// each chunk from the shard stores' committed extents) makes every shard read
// back byte-exact — the same replay property as a data extent.
func TestV2StoreShardRestart(t *testing.T) {
	dirs := make([]string, 2)
	for i := range dirs {
		dirs[i] = t.TempDir()
	}
	newPair := func(i int) (*segment.Store, *segment.Store) {
		ds, err := segment.New(segment.Config{Dir: dirs[i], UseMemIndex: true, StreamID: 1})
		if err != nil {
			t.Fatalf("segment.New data: %v", err)
		}
		ss, err := segment.New(segment.Config{Dir: dirs[i], UseMemIndex: true, StreamID: 2})
		if err != nil {
			t.Fatalf("segment.New shard: %v", err)
		}
		return ds, ss
	}

	d0, s0 := newPair(0)
	d1, s1 := newPair(1)
	v := NewMultiV2Store([]storage.Store{d0, d1}, dirs...)
	if err := v.AttachShardStores([]storage.Store{s0, s1}); err != nil {
		t.Fatalf("AttachShardStores: %v", err)
	}

	cid := metadata.ChunkID(2002)
	payloads := map[int]string{0: "d0", 1: "d1", 2: "d2", 3: "d3", 4: "d4", 5: "d5", 6: "p0", 7: "p1", 8: "p2"}
	for s, p := range payloads {
		if err := v.WriteShard(cid, s, []byte(p)); err != nil {
			t.Fatalf("WriteShard(%d): %v", s, err)
		}
	}

	for _, s := range []*segment.Store{d0, s0, d1, s1} {
		if err := s.Close(); err != nil {
			t.Fatalf("close backend: %v", err)
		}
	}

	// Reopen and reconstruct from scratch (dirs only — the V2Store rebuilds
	// both location maps and the shard stripe home from committed extents).
	rd0, rs0 := newPair(0)
	rd1, rs1 := newPair(1)
	defer rd0.Close()
	defer rs0.Close()
	defer rd1.Close()
	defer rs1.Close()
	rv := NewMultiV2Store([]storage.Store{rd0, rd1}, dirs...)
	if err := rv.AttachShardStores([]storage.Store{rs0, rs1}); err != nil {
		t.Fatalf("re-AttachShardStores: %v", err)
	}

	for s, p := range payloads {
		data, _, err := rv.ReadShard(cid, s)
		if err != nil {
			t.Fatalf("ReadShard(%d) after restart: %v", s, err)
		}
		if string(data) != p {
			t.Fatalf("shard %d after restart = %q, want %q", s, data, p)
		}
	}
}

// TestV2StoreShardNotReportedAsReplica verifies EC shard extents are excluded
// from every whole-chunk snapshot surface (heartbeat replica state, chunk
// listing), because shard stores are not part of v.disks and shard extents
// are fragments — while whole-chunk extents still appear.
func TestV2StoreShardNotReportedAsReplica(t *testing.T) {
	v, _ := newTestShardMultiStore(t, 2)

	whole := metadata.ChunkID(3003)
	if err := v.Write(whole, []byte("replica")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	cid := metadata.ChunkID(4004)
	for s := 0; s < 9; s++ {
		if err := v.WriteShard(cid, s, []byte("shard")); err != nil {
			t.Fatalf("WriteShard(%d): %v", s, err)
		}
	}

	snap := v.ChunkStateSnapshot()
	if _, ok := snap[whole]; !ok {
		t.Fatalf("whole-chunk replica %d missing from snapshot", whole)
	}
	// The EC chunk has no whole-chunk data extent (only shards in the separate
	// shard stream), so it must not appear as a replica at all.
	if _, ok := snap[cid]; ok {
		t.Fatalf("EC chunk %d (shards only) reported as a whole-chunk replica", cid)
	}

	// ListChunks/Stats likewise see only the whole-chunk extent.
	listed := v.ListChunks()
	if len(listed) != 1 || listed[0].ChunkID != whole {
		t.Fatalf("ListChunks = %+v, want exactly the whole-chunk extent", listed)
	}
	totalBytes, chunkCount := v.Stats()
	if chunkCount != 1 {
		t.Fatalf("Stats chunkCount = %d, want 1 (shards excluded)", chunkCount)
	}
	_ = totalBytes
}
