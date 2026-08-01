package recovery

import (
	"testing"

	"github.com/example/dfs/datanode/storage"
	"github.com/example/dfs/datanode/storage/manifest"
)

// TestRecover_NoSuperblock verifies a foreign/absent disk is rejected
// rather than adopted (§5.2).
func TestRecover_NoSuperblock(t *testing.T) {
	dir := t.TempDir()
	if _, err := Recover(Options{Dir: dir}); err == nil {
		t.Fatal("expected recovery to fail without a superblock")
	}
}

// TestRecover_EmptyValidDisk verifies recovery succeeds on a disk that
// has a superblock but no manifest (fresh disk).
func TestRecover_EmptyValidDisk(t *testing.T) {
	dir := t.TempDir()
	sb := &manifest.Superblock{
		Magic:         storage.SuperblockMagic,
		FormatVersion: storage.FormatVersion,
		ClusterID:     1,
		DiskID:        1,
		NodeID:        1,
	}
	if err := manifest.WriteSuperblock(dir, sb); err != nil {
		t.Fatal(err)
	}
	res, err := Recover(Options{Dir: dir})
	if err != nil {
		t.Fatalf("recovery failed on fresh disk: %v", err)
	}
	if res.CheckpointLoaded {
		t.Fatal("fresh disk should not report a loaded checkpoint")
	}
}
