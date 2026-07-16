package metadata

import (
	"context"
	"testing"
)

// ============================================================
// TDD: Small File Optimization — Write Path Integration
// ============================================================
// When a file is small (≤64KB), it should be stored in a shared block
// instead of allocating a dedicated chunk. This saves metadata overhead
// and reduces the number of small chunks on disk.

func TestSmallFile_IsSmallFile(t *testing.T) {
	tests := []struct {
		size int64
		want bool
	}{
		{0, true},
		{1, true},
		{64 * 1024, true},       // exactly 64KB
		{64*1024 + 1, false},    // just over 64KB
		{1024 * 1024, false},    // 1MB
		{64 * 1024 * 1024, false}, // 64MB
	}
	for _, tt := range tests {
		got := IsSmallFile(tt.size)
		if got != tt.want {
			t.Errorf("IsSmallFile(%d) = %v, want %v", tt.size, got, tt.want)
		}
	}
}

func TestSmallFileBlock_AddAndFind(t *testing.T) {
	block := &SmallFileBlockMeta{
		BlockID:   1,
		Size:      0,
		FileCount: 0,
		Sealed:    false,
	}

	// Add a small file
	ok := block.AddSmallFile("test.txt", 0, 1024, 0xABCD)
	if !ok {
		t.Error("AddSmallFile should succeed")
	}
	if block.FileCount != 1 {
		t.Errorf("FileCount = %d, want 1", block.FileCount)
	}

	// Find it
	idx := block.FindSmallFile("test.txt")
	if idx == nil {
		t.Error("FindSmallFile should find the file")
	}
	if idx.Offset != 0 || idx.Length != 1024 {
		t.Errorf("index = %+v, want offset=0 length=1024", idx)
	}

	// Find non-existent
	if block.FindSmallFile("nope.txt") != nil {
		t.Error("FindSmallFile should return nil for missing file")
	}
}

func TestSmallFileBlock_MaxFiles(t *testing.T) {
	block := &SmallFileBlockMeta{
		BlockID:   1,
		Sealed:    false,
	}

	// Fill up to max
	for i := 0; i < MaxSmallFilesPerBlock; i++ {
		name := string(rune('a' + i%26)) + string(rune('0'+i/26))
		ok := block.AddSmallFile(name, uint32(i*100), 100, 0)
		if !ok {
			t.Fatalf("AddSmallFile failed at index %d (max=%d)", i, MaxSmallFilesPerBlock)
		}
	}

	// Next one should fail
	ok := block.AddSmallFile("overflow", 0, 100, 0)
	if ok {
		t.Error("AddSmallFile should fail when block is full")
	}
}

func TestSmallFileBlock_Seal(t *testing.T) {
	block := &SmallFileBlockMeta{
		BlockID: 1,
		Sealed:  false,
	}
	block.AddSmallFile("a.txt", 0, 100, 0)

	block.Sealed = true

	// Sealed block should not accept new files
	ok := block.AddSmallFile("b.txt", 100, 100, 0)
	if ok {
		t.Error("AddSmallFile should fail on sealed block")
	}
}

func TestSmallFile_WritePathIntegration(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	// Register a node
	node := &NodeInfo{
		ID:         1,
		Addr:       "10.0.0.1:8080",
		CapacityGB: 1000,
		UsedGB:     0,
		State:      NodeOnline,
	}
	if err := store.RegisterNode(ctx, node); err != nil {
		t.Fatal(err)
	}

	// Create bucket
	if err := store.CreateBucket(ctx, "small-files", PlacementPolicy{
		ReplicationFactor: 1,
		TopologySpread:    SpreadNode,
	}); err != nil {
		t.Fatal(err)
	}

	bucket, err := store.GetBucket(ctx, "small-files")
	if err != nil {
		t.Fatal(err)
	}

	// Create a small file
	smallFile, err := store.CreateFile(ctx, bucket.RootInode, "tiny.txt", 0644)
	if err != nil {
		t.Fatal(err)
	}

	// Update inode with small size
	smallFile.Size = 128 // 128 bytes — well under threshold
	if err := store.UpdateInode(ctx, smallFile); err != nil {
		t.Fatal(err)
	}

	// Verify the file is recognized as small
	if !IsSmallFile(smallFile.Size) {
		t.Errorf("IsSmallFile(%d) = false, want true", smallFile.Size)
	}
}

func TestSmallFile_DuplicateName(t *testing.T) {
	block := &SmallFileBlockMeta{
		BlockID: 1,
		Sealed:  false,
	}

	ok := block.AddSmallFile("dup.txt", 0, 100, 0)
	if !ok {
		t.Fatal("first AddSmallFile should succeed")
	}

	ok = block.AddSmallFile("dup.txt", 100, 200, 0)
	if ok {
		t.Error("duplicate AddSmallFile should fail")
	}
}
