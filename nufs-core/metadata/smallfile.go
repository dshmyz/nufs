package metadata

import "fmt"

// ============================================================
// Small File Optimization — Block Storage for Tiny Files
// ============================================================

const (
	// SmallFileThreshold: files ≤ this size are stored in blocks
	SmallFileThreshold = 64 << 10 // 64KB

	// SmallFileBlockSize: each block is 1MB
	SmallFileBlockSize = 1 << 20 // 1MB

	// MaxSmallFilesPerBlock: max files in one block
	MaxSmallFilesPerBlock = 256
)

// SmallFileIndex tracks a file within a block.
type SmallFileIndex struct {
	Name   string `json:"name"`   // File name
	Offset uint32 `json:"offset"` // Offset within block (bytes)
	Length uint32 `json:"length"` // File size (bytes)
	CRC    uint16 `json:"crc"`    // CRC16 of file data
}

// SmallFileBlockMeta is stored in the inode for blocks.
type SmallFileBlockMeta struct {
	BlockID   ChunkID          `json:"block_id"`   // The chunk ID of the block file
	Size      int64            `json:"size"`       // Block file size
	FileCount int              `json:"file_count"` // Number of files in block
	Index     []SmallFileIndex `json:"index"`      // File index
	CreatedAt int64            `json:"created_at"` // Block creation time
	Sealed    bool             `json:"sealed"`     // True when block is full
}

// FileTypeSmallFileBlock indicates this inode holds a small file block.
const FileTypeSmallFileBlock = 9

// FindSmallFile returns the SmallFileIndex if the file exists in the block.
func (b *SmallFileBlockMeta) FindSmallFile(name string) *SmallFileIndex {
	for i := range b.Index {
		if b.Index[i].Name == name {
			return &b.Index[i]
		}
	}
	return nil
}

// AddSmallFile adds a file to the block index. Returns false if block is full or sealed.
func (b *SmallFileBlockMeta) AddSmallFile(name string, offset uint32, length uint32, crc uint16) bool {
	if b.Sealed {
		return false
	}
	if b.FileCount >= MaxSmallFilesPerBlock {
		return false
	}
	// Check for duplicate
	if b.FindSmallFile(name) != nil {
		return false
	}
	b.Index = append(b.Index, SmallFileIndex{
		Name:   name,
		Offset: offset,
		Length: length,
		CRC:    crc,
	})
	b.FileCount++
	return true
}

// IsSmallFile checks if a file should be stored in a block.
func IsSmallFile(size int64) bool {
	return size <= SmallFileThreshold
}

// ShouldCreateBlock checks if a new block should be created for a directory.
// Heuristic: if parent already has ≥ 50 small files, create a block.
func ShouldCreateBlock(smallFileCount int) bool {
	return smallFileCount >= 50
}

// BlockKey returns the metadata key for a small file block.
func BlockKey(blockID ChunkID) string {
	return fmt.Sprintf("%sblock/%d", prefixInode, blockID)
}

// SmallFileKey returns the metadata key for a small file within a block.
func SmallFileKey(parent InodeID, name string) string {
	return fmt.Sprintf("%s%d/%s", prefixNS, parent, name)
}
