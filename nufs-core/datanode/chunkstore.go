package datanode

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/example/dfs/metadata"
)

// ChunkStore manages local disk storage for data chunks.
// Each chunk is stored as a file with a binary header:
//
//	{DataDir}/chunks/{shard:02x}/{chunk_id}.dat
type ChunkStore struct {
	dataDir    string
	chunksDir  string
	mu         sync.RWMutex
	chunks     map[metadata.ChunkID]*LocalChunkInfo
	totalBytes atomic.Int64
	chunkCount atomic.Int64
	writeSem   chan struct{} // semaphore for concurrent write limiting
	readSem    chan struct{} // semaphore for concurrent read limiting

	// WAL for crash recovery — logs before write, commits after fsync
	wal *WriteAheadLog
}

// NewChunkStore creates a new chunk store at the given data directory.
// If wal is non-nil, all writes are protected by the WAL for crash recovery.
func NewChunkStore(dataDir string, maxWrites, maxReads int, wal *WriteAheadLog) (*ChunkStore, error) {
	chunksDir := filepath.Join(dataDir, "chunks")

	// Create shard directories
	for i := 0; i < MaxShards; i++ {
		shardDir := filepath.Join(chunksDir, fmt.Sprintf("%02x", i))
		if err := os.MkdirAll(shardDir, 0755); err != nil {
			return nil, fmt.Errorf("datanode: create shard dir: %w", err)
		}
	}

	cs := &ChunkStore{
		dataDir:   dataDir,
		chunksDir: chunksDir,
		chunks:    make(map[metadata.ChunkID]*LocalChunkInfo),
		writeSem:  make(chan struct{}, maxWrites),
		readSem:   make(chan struct{}, maxReads),
		wal:       wal,
	}

	// Scan existing chunks on startup
	if err := cs.scanExisting(); err != nil {
		return nil, fmt.Errorf("datanode: scan existing chunks: %w", err)
	}

	return cs, nil
}

// chunkPath returns the file path for a chunk.
func (cs *ChunkStore) chunkPath(chunkID metadata.ChunkID) string {
	shard := uint64(chunkID) % MaxShards
	return filepath.Join(cs.chunksDir, fmt.Sprintf("%02x", shard), fmt.Sprintf("%d.dat", chunkID))
}

// metaPath returns the metadata sidecar path for a chunk.
func (cs *ChunkStore) metaPath(chunkID metadata.ChunkID) string {
	shard := uint64(chunkID) % MaxShards
	return filepath.Join(cs.chunksDir, fmt.Sprintf("%02x", shard), fmt.Sprintf("%d.meta", chunkID))
}

// Write stores chunk data to local disk.
// If WAL is configured, the write is logged before and committed after for crash recovery.
func (cs *ChunkStore) Write(chunkID metadata.ChunkID, data []byte) error {
	// Acquire write semaphore
	cs.writeSem <- struct{}{}
	defer func() { <-cs.writeSem }()

	// Phase 1: Log intent to WAL (crash recovery: uncommitted writes are cleaned up)
	if cs.wal != nil {
		if err := cs.wal.LogWrite(chunkID, len(data)); err != nil {
			return fmt.Errorf("datanode: WAL log write: %w", err)
		}
	}

	checksum := crc32.ChecksumIEEE(data)
	path := cs.chunkPath(chunkID)

	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("datanode: create chunk file: %w", err)
	}
	defer f.Close()

	// Write binary header
	header := make([]byte, ChunkFileHeaderSize)
	copy(header[0:4], ChunkFileMagic)
	binary.BigEndian.PutUint64(header[4:12], uint64(chunkID))
	binary.BigEndian.PutUint32(header[12:16], uint32(len(data)))
	binary.BigEndian.PutUint32(header[16:20], checksum)

	if _, err := f.Write(header); err != nil {
		return fmt.Errorf("datanode: write header: %w", err)
	}
	if _, err := f.Write(data); err != nil {
		return fmt.Errorf("datanode: write data: %w", err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("datanode: fsync: %w", err)
	}

	// Update in-memory index
	info := &LocalChunkInfo{
		ChunkID:     chunkID,
		Size:        int64(len(data)),
		Checksum:    checksum,
		State:       LocalWritten,
		WrittenAt:   time.Now(),
		LastAccess:  time.Now(),
		AccessCount: 0,
	}

	cs.mu.Lock()
	cs.chunks[chunkID] = info
	cs.mu.Unlock()

	cs.totalBytes.Add(int64(len(data)))
	cs.chunkCount.Add(1)

	// Write metadata sidecar
	cs.writeMetaSidecar(chunkID, info)

	// Phase 2: Commit in WAL — write is durable, safe to ack
	if cs.wal != nil {
		if err := cs.wal.LogCommit(chunkID); err != nil {
			log.Printf("datanode: WAL commit failed for chunk %d: %v (data is safe on disk)", chunkID, err)
			// Data is already on disk, so we don't fail the write.
			// The next startup recovery will see this as uncommitted and verify.
		}
	}

	return nil
}

// WriteAt writes data at a specific offset within a chunk file.
// Used for partial/appending writes during replication.
func (cs *ChunkStore) WriteAt(chunkID metadata.ChunkID, offset int64, data []byte) error {
	cs.writeSem <- struct{}{}
	defer func() { <-cs.writeSem }()

	path := cs.chunkPath(chunkID)

	// If file doesn't exist, create with header
	if _, err := os.Stat(path); os.IsNotExist(err) {
		header := make([]byte, ChunkFileHeaderSize)
		copy(header[0:4], ChunkFileMagic)
		binary.BigEndian.PutUint64(header[4:12], uint64(chunkID))
		binary.BigEndian.PutUint32(header[12:16], 0) // length updated on seal
		binary.BigEndian.PutUint32(header[16:20], 0) // checksum updated on seal

		f, err := os.Create(path)
		if err != nil {
			return fmt.Errorf("datanode: create chunk file: %w", err)
		}
		if _, err := f.Write(header); err != nil {
			f.Close()
			return fmt.Errorf("datanode: write header: %w", err)
		}
		f.Close()
	}

	f, err := os.OpenFile(path, os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("datanode: open chunk file: %w", err)
	}
	defer f.Close()

	fileOffset := int64(ChunkFileHeaderSize) + offset
	if _, err := f.WriteAt(data, fileOffset); err != nil {
		return fmt.Errorf("datanode: write at offset: %w", err)
	}
	return f.Sync()
}

// Read retrieves chunk data from local disk.
// If offset and length are 0, reads the entire chunk.
func (cs *ChunkStore) Read(chunkID metadata.ChunkID, offset int64, length int32) ([]byte, uint32, error) {
	cs.readSem <- struct{}{}
	defer func() { <-cs.readSem }()

	path := cs.chunkPath(chunkID)

	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, 0, fmt.Errorf("datanode: %w: chunk %d", metadata.ErrChunkNotFound, chunkID)
		}
		return nil, 0, fmt.Errorf("datanode: open chunk file: %w", err)
	}
	defer f.Close()

	// Read and verify header
	header := make([]byte, ChunkFileHeaderSize)
	if _, err := io.ReadFull(f, header); err != nil {
		return nil, 0, fmt.Errorf("datanode: read header: %w", err)
	}
	if string(header[0:4]) != ChunkFileMagic {
		return nil, 0, fmt.Errorf("datanode: invalid chunk file magic")
	}

	dataLen := binary.BigEndian.Uint32(header[12:16])
	storedChecksum := binary.BigEndian.Uint32(header[16:20])

	// Determine read range
	readOffset := offset
	readLen := int32(dataLen)
	if length > 0 {
		readLen = length
	}
	if readOffset+int64(readLen) > int64(dataLen) {
		readLen = int32(int64(dataLen) - readOffset)
	}

	// Seek to data position
	if _, err := f.Seek(int64(ChunkFileHeaderSize)+readOffset, io.SeekStart); err != nil {
		return nil, 0, fmt.Errorf("datanode: seek: %w", err)
	}

	data := make([]byte, readLen)
	if _, err := io.ReadFull(f, data); err != nil {
		return nil, 0, fmt.Errorf("datanode: read data: %w", err)
	}

	// Update access stats
	cs.mu.RLock()
	if info, ok := cs.chunks[chunkID]; ok {
		info.LastAccess = time.Now()
		info.AccessCount++
	}
	cs.mu.RUnlock()

	return data, storedChecksum, nil
}

// Seal finalizes a chunk: computes checksum over the full data and updates the header.
func (cs *ChunkStore) Seal(chunkID metadata.ChunkID) (uint32, error) {
	cs.writeSem <- struct{}{}
	defer func() { <-cs.writeSem }()

	path := cs.chunkPath(chunkID)
	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return 0, fmt.Errorf("datanode: open chunk for seal: %w", err)
	}
	defer f.Close()

	// Read header
	header := make([]byte, ChunkFileHeaderSize)
	if _, err := io.ReadFull(f, header); err != nil {
		return 0, fmt.Errorf("datanode: read header for seal: %w", err)
	}

	dataLen := binary.BigEndian.Uint32(header[12:16])

	// Read all data for checksum
	data := make([]byte, dataLen)
	if _, err := f.ReadAt(data, int64(ChunkFileHeaderSize)); err != nil {
		return 0, fmt.Errorf("datanode: read data for seal: %w", err)
	}

	checksum := crc32.ChecksumIEEE(data)

	// Update header with checksum
	binary.BigEndian.PutUint32(header[16:20], checksum)
	if _, err := f.WriteAt(header, 0); err != nil {
		return 0, fmt.Errorf("datanode: write sealed header: %w", err)
	}
	if err := f.Sync(); err != nil {
		return 0, fmt.Errorf("datanode: fsync seal: %w", err)
	}

	// Update in-memory state
	cs.mu.Lock()
	if info, ok := cs.chunks[chunkID]; ok {
		info.State = LocalSealed
		info.Checksum = checksum
	}
	cs.mu.Unlock()

	return checksum, nil
}

// Delete removes a chunk from local disk.
func (cs *ChunkStore) Delete(chunkID metadata.ChunkID) error {
	path := cs.chunkPath(chunkID)
	metaP := cs.metaPath(chunkID)

	cs.mu.Lock()
	info, exists := cs.chunks[chunkID]
	if exists {
		cs.totalBytes.Add(-info.Size)
		cs.chunkCount.Add(-1)
		delete(cs.chunks, chunkID)
	}
	cs.mu.Unlock()

	os.Remove(metaP) // ignore error for sidecar
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("datanode: delete chunk: %w", err)
	}
	return nil
}

// Info returns the local chunk info.
func (cs *ChunkStore) Info(chunkID metadata.ChunkID) (*LocalChunkInfo, bool) {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	info, ok := cs.chunks[chunkID]
	return info, ok
}

// ListChunks returns all locally stored chunk information.
func (cs *ChunkStore) ListChunks() []LocalChunkInfo {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	result := make([]LocalChunkInfo, 0, len(cs.chunks))
	for _, info := range cs.chunks {
		result = append(result, *info)
	}
	return result
}

// Stats returns storage statistics.
func (cs *ChunkStore) Stats() (totalBytes int64, chunkCount int64) {
	return cs.totalBytes.Load(), cs.chunkCount.Load()
}

// ========== Internal Helpers ==========

// scanExisting reads existing chunk files on startup to rebuild the in-memory index.
func (cs *ChunkStore) scanExisting() error {
	for i := 0; i < MaxShards; i++ {
		shardDir := filepath.Join(cs.chunksDir, fmt.Sprintf("%02x", i))
		entries, err := os.ReadDir(shardDir)
		if err != nil {
			continue
		}

		for _, entry := range entries {
			if entry.IsDir() || filepath.Ext(entry.Name()) != ".dat" {
				continue
			}

			path := filepath.Join(shardDir, entry.Name())
			info, err := cs.readChunkFileHeader(path)
			if err != nil {
				continue // skip corrupt files
			}

			cs.chunks[info.ChunkID] = info
			cs.totalBytes.Add(info.Size)
			cs.chunkCount.Add(1)
		}
	}
	return nil
}

// readChunkFileHeader reads and parses a chunk file header.
func (cs *ChunkStore) readChunkFileHeader(path string) (*LocalChunkInfo, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	header := make([]byte, ChunkFileHeaderSize)
	if _, err := io.ReadFull(f, header); err != nil {
		return nil, err
	}
	if string(header[0:4]) != ChunkFileMagic {
		return nil, fmt.Errorf("invalid magic")
	}

	chunkID := metadata.ChunkID(binary.BigEndian.Uint64(header[4:12]))
	dataLen := binary.BigEndian.Uint32(header[12:16])
	checksum := binary.BigEndian.Uint32(header[16:20])

	state := LocalSealed
	if checksum == 0 {
		state = LocalWritten
	}

	return &LocalChunkInfo{
		ChunkID:  chunkID,
		Size:     int64(dataLen),
		Checksum: checksum,
		State:    state,
	}, nil
}

// writeMetaSidecar writes a JSON metadata sidecar file.
func (cs *ChunkStore) writeMetaSidecar(chunkID metadata.ChunkID, info *LocalChunkInfo) {
	data, err := json.Marshal(info)
	if err != nil {
		return
	}
	path := cs.metaPath(chunkID)
	_ = os.WriteFile(path, data, 0644)
}
