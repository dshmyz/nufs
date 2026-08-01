package manifest

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/example/dfs/datanode/storage"
)

func TestSuperblockRoundtrip(t *testing.T) {
	sb := &Superblock{
		Magic:         storage.SuperblockMagic,
		FormatVersion: storage.FormatVersion,
		ClusterID:     99,
		DiskID:        3,
		NodeID:        7,
		CreatedAtUnix: 1234567890,
	}
	buf := make([]byte, 41)
	if err := sb.Encode(buf); err != nil {
		t.Fatal(err)
	}
	var decoded Superblock
	if err := decoded.Decode(buf); err != nil {
		t.Fatal(err)
	}
	if decoded != *sb {
		t.Fatalf("roundtrip mismatch: got %+v want %+v", decoded, *sb)
	}
	// Reject foreign cluster.
	if err := sb.Validate(100, 7); err == nil {
		t.Fatal("expected cluster mismatch to be rejected")
	}
	if err := sb.Validate(99, 7); err != nil {
		t.Fatalf("valid identity rejected: %v", err)
	}
}

func TestManifestRoundtrip(t *testing.T) {
	m := &Manifest{
		Version:        ManifestVersion,
		Generation:     5,
		PrevGeneration: 4,
		Segments: []SegmentRecord{
			{ID: 1, Class: storage.SegmentData, Path: "segments/data/sealed/1.seg", SizeBytes: 4096, SealedAt: 100, RecordCount: 10},
			{ID: 2, Class: storage.SegmentSmall, Path: "segments/small/sealed/2.seg", SizeBytes: 512, SealedAt: 101, RecordCount: 100},
		},
	}
	buf := make([]byte, m.EncodeAlloc())
	if err := m.Encode(buf); err != nil {
		t.Fatal(err)
	}
	var decoded Manifest
	if err := decoded.Decode(buf); err != nil {
		t.Fatal(err)
	}
	if decoded.Generation != 5 || len(decoded.Segments) != 2 {
		t.Fatalf("decode mismatch: %+v", decoded)
	}
	if decoded.Segments[0] != m.Segments[0] || decoded.Segments[1] != m.Segments[1] {
		t.Fatalf("segment records mismatch: %+v", decoded.Segments)
	}
}

func TestPublishLoad(t *testing.T) {
	dir := t.TempDir()
	m := &Manifest{
		Version:    ManifestVersion,
		Generation: 1,
		Segments: []SegmentRecord{
			{ID: 9, Class: storage.SegmentData, Path: "segments/data/sealed/9.seg", SizeBytes: 100, SealedAt: 5, RecordCount: 1},
		},
	}
	if err := Publish(dir, m); err != nil {
		t.Fatal(err)
	}

	// Fresh load must find generation 1.
	loaded, cur, err := Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	if cur.Generation != 1 || loaded.Generation != 1 || len(loaded.Segments) != 1 {
		t.Fatalf("load mismatch: cur=%+v man=%+v", cur, loaded)
	}

	// Publish a new generation; load must see it, not the old one.
	m2 := &Manifest{Version: ManifestVersion, Generation: 2, PrevGeneration: 1}
	if err := Publish(dir, m2); err != nil {
		t.Fatal(err)
	}
	loaded2, cur2, err := Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	if cur2.Generation != 2 || loaded2.Generation != 2 {
		t.Fatalf("load after publish mismatch: cur=%+v man=%+v", cur2, loaded2)
	}

	// Corrupt the CURRENT file's manifest; Load must fail (no silent
	// fallback to an older manifest).
	if err := os.WriteFile(filepath.Join(dir, "manifests", "MANIFEST-2"), []byte("junk"), 0644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := Load(dir); err == nil {
		t.Fatal("expected Load to fail on corrupt manifest")
	}
}

func TestLoadNoManifest(t *testing.T) {
	dir := t.TempDir()
	if _, _, err := Load(dir); err != ErrNoManifest {
		t.Fatalf("expected ErrNoManifest on fresh dir, got %v", err)
	}
}

func TestWriteSuperblock(t *testing.T) {
	dir := t.TempDir()
	sb := &Superblock{Magic: storage.SuperblockMagic, FormatVersion: storage.FormatVersion, ClusterID: 1, DiskID: 2, NodeID: 3}
	if err := WriteSuperblock(dir, sb); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadSuperblock(dir)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.ClusterID != 1 || loaded.DiskID != 2 || loaded.NodeID != 3 {
		t.Fatalf("superblock roundtrip mismatch: %+v", loaded)
	}
}
