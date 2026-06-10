package datanode

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/example/dfs/internal/crypto"
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
	syncSem    chan struct{} // semaphore for concurrent fsync limiting

	// WAL for crash recovery — logs before write, commits after fsync
	wal *WriteAheadLog

	// At-rest encryption layer (nil = encryption disabled)
	encryptor *crypto.Encryptor

	// Disk health manager — used to reject writes when disk is in FAILED state
	disk *DiskManager

	// File descriptor cache for hot chunks — maps ChunkID to its open file.
	// Uses LRU eviction to bound the number of open file descriptors.
	// Replaces the previous sync.Pool which could return wrong chunk fds.
	fdMu    sync.RWMutex
	fdCache map[metadata.ChunkID]*os.File
	fdList  *fdLRU // LRU eviction tracker
	fdMax   int    // Maximum cached file descriptors

	perf chunkStorePerf
}

type chunkStorePerf struct {
	writeSemWaitNs     atomic.Int64
	readSemWaitNs      atomic.Int64
	fsyncNs            atomic.Int64
	fsyncCount         atomic.Int64
	readRequestedBytes atomic.Int64
	readAmplifiedBytes atomic.Int64
	fdCacheHits        atomic.Int64
	fdCacheMisses      atomic.Int64
	fdCacheEvictions   atomic.Int64
	listChunksNs       atomic.Int64
	listChunksCalls    atomic.Int64
	listChunksItems    atomic.Int64
}

type ChunkStorePerfSnapshot struct {
	WriteSemWaitNs     int64 `json:"write_sem_wait_ns"`
	ReadSemWaitNs      int64 `json:"read_sem_wait_ns"`
	FsyncNs            int64 `json:"fsync_ns"`
	FsyncCount         int64 `json:"fsync_count"`
	ReadRequestedBytes int64 `json:"read_requested_bytes"`
	ReadAmplifiedBytes int64 `json:"read_amplified_bytes"`
	FdCacheHits        int64 `json:"fd_cache_hits"`
	FdCacheMisses      int64 `json:"fd_cache_misses"`
	FdCacheEvictions   int64 `json:"fd_cache_evictions"`
	ListChunksNs       int64 `json:"list_chunks_ns"`
	ListChunksCalls    int64 `json:"list_chunks_calls"`
	ListChunksItems    int64 `json:"list_chunks_items"`
}

func (cs *ChunkStore) PerfSnapshot() ChunkStorePerfSnapshot {
	return ChunkStorePerfSnapshot{
		WriteSemWaitNs:     cs.perf.writeSemWaitNs.Load(),
		ReadSemWaitNs:      cs.perf.readSemWaitNs.Load(),
		FsyncNs:            cs.perf.fsyncNs.Load(),
		FsyncCount:         cs.perf.fsyncCount.Load(),
		ReadRequestedBytes: cs.perf.readRequestedBytes.Load(),
		ReadAmplifiedBytes: cs.perf.readAmplifiedBytes.Load(),
		FdCacheHits:        cs.perf.fdCacheHits.Load(),
		FdCacheMisses:      cs.perf.fdCacheMisses.Load(),
		FdCacheEvictions:   cs.perf.fdCacheEvictions.Load(),
		ListChunksNs:       cs.perf.listChunksNs.Load(),
		ListChunksCalls:    cs.perf.listChunksCalls.Load(),
		ListChunksItems:    cs.perf.listChunksItems.Load(),
	}
}

// SyncConcurrency limits concurrent f.Sync() calls to prevent I/O thrashing
// when many writers flush simultaneously.
const SyncConcurrency = 4

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
		syncSem:   make(chan struct{}, SyncConcurrency),
		wal:       wal,
		fdCache:   make(map[metadata.ChunkID]*os.File),
		fdList:    newFdLRU(256), // cache up to 256 file descriptors
		fdMax:     256,
	}

	// Scan existing chunks on startup — runs in background to avoid
	// blocking process initialization. Writes and reads are rejected
	// until the scan completes.
	cs.mu.Lock()
	err := cs.scanExisting()
	cs.mu.Unlock()
	if err != nil {
		return nil, fmt.Errorf("datanode: scan existing chunks: %w", err)
	}

	return cs, nil
}

// SetEncryptor configures at-rest encryption for chunk data.
// When set, Write encrypts data before persisting and Read decrypts after.
func (cs *ChunkStore) SetEncryptor(enc *crypto.Encryptor) {
	cs.encryptor = enc
}

// SetDiskManager registers the disk health manager for write admission control.
// When the disk transitions to FAILED state, all writes are rejected.
func (cs *ChunkStore) SetDiskManager(dm *DiskManager) {
	cs.disk = dm
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
	// Reject writes if disk is in FAILED state (auto read-only on disk failure)
	if cs.disk != nil {
		if err := cs.disk.CanAdmitWrite(int64(len(data))); err != nil {
			return fmt.Errorf("datanode: write rejected: %w", err)
		}
	}

	// Acquire write semaphore
	writeSemStart := time.Now()
	cs.writeSem <- struct{}{}
	cs.perf.writeSemWaitNs.Add(time.Since(writeSemStart).Nanoseconds())
	defer func() { <-cs.writeSem }()

	// Phase 1: Log intent to WAL (crash recovery: uncommitted writes are cleaned up)
	if cs.wal != nil {
		if err := cs.wal.LogWrite(chunkID, len(data)); err != nil {
			return fmt.Errorf("datanode: WAL log write: %w", err)
		}
	}

	// Encrypt data at rest if encryption is configured
	storeData := data
	if cs.encryptor != nil && cs.encryptor.Enabled() {
		encrypted, err := cs.encryptor.EncryptChunk(data)
		if err != nil {
			return fmt.Errorf("datanode: encrypt chunk: %w", err)
		}
		storeData = encrypted
	}

	checksum := crc32.ChecksumIEEE(data) // checksum of plaintext
	path := cs.chunkPath(chunkID)

	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("datanode: create chunk file: %w", err)
	}
	defer f.Close()

	// Write binary header — data_length stores the encrypted blob size
	header := make([]byte, ChunkFileHeaderSize)
	copy(header[0:4], ChunkFileMagic)
	binary.BigEndian.PutUint64(header[4:12], uint64(chunkID))
	binary.BigEndian.PutUint32(header[12:16], uint32(len(storeData)))
	binary.BigEndian.PutUint32(header[16:20], checksum)

	if _, err := f.Write(header); err != nil {
		return fmt.Errorf("datanode: write header: %w", err)
	}
	if _, err := f.Write(storeData); err != nil {
		return fmt.Errorf("datanode: write data: %w", err)
	}
	// Throttle concurrent fsyncs to prevent disk I/O thrashing.
	// Multiple concurrent writers writing to different files can
	// saturate the disk with flush commands, hurting throughput.
	cs.syncSem <- struct{}{}
	fsyncStart := time.Now()
	syncErr := f.Sync()
	cs.perf.fsyncNs.Add(time.Since(fsyncStart).Nanoseconds())
	cs.perf.fsyncCount.Add(1)
	<-cs.syncSem
	if syncErr != nil {
		return fmt.Errorf("datanode: fsync: %w", syncErr)
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
			slog.Warn("datanode: WAL commit failed for chunk, data is safe on disk", "chunkID", chunkID, "error", err)
			// Data is already on disk, so we don't fail the write.
			// The next startup recovery will see this as uncommitted and verify.
		}
	}

	return nil
}

// WriteAt writes data at a specific offset within a chunk file.
// Used for partial/appending writes during replication.
// CRC is deferred to Seal() for performance — the on-disk CRC may be
// stale until Seal() recomputes it from the full file.
func (cs *ChunkStore) WriteAt(chunkID metadata.ChunkID, offset int64, data []byte) error {
	writeSemStart := time.Now()
	cs.writeSem <- struct{}{}
	cs.perf.writeSemWaitNs.Add(time.Since(writeSemStart).Nanoseconds())
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

	// Mark chunk as dirty (CRC is stale) — Seal() will recompute.
	// Write CRC=0 to the on-disk header so Read skips CRC check.
	cs.mu.Lock()
	if info, ok := cs.chunks[chunkID]; ok {
		info.State = LocalWritten
		info.Checksum = 0
	}
	cs.mu.Unlock()

	// Write CRC=0 to file header (cheap 4-byte overwrite at offset 16).
	zeroCRC := make([]byte, 4)
	f.WriteAt(zeroCRC, 16)

	// Invalidate fd cache so next Read picks up modified data.
	cs.fdMu.Lock()
	delete(cs.fdCache, chunkID)
	cs.fdMu.Unlock()

	return f.Sync()
}

// computeFileCRC reads the chunk file, computes CRC32 of plaintext.
// Used by Seal() to finalize CRC after WriteAt modifications.
func (cs *ChunkStore) computeFileCRC(_ metadata.ChunkID, path string) (uint32, error) {
	rf, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer rf.Close()

	header := make([]byte, ChunkFileHeaderSize)
	if _, err := io.ReadFull(rf, header); err != nil {
		return 0, err
	}
	dataLen := binary.BigEndian.Uint32(header[12:16])

	storedData := make([]byte, dataLen)
	if _, err := io.ReadFull(rf, storedData); err != nil {
		return 0, err
	}

	var plainData []byte
	if cs.encryptor != nil && cs.encryptor.Enabled() {
		decrypted, err := cs.encryptor.DecryptChunk(storedData)
		if err != nil {
			return 0, err
		}
		plainData = decrypted
	} else {
		plainData = storedData
	}

	return crc32.ChecksumIEEE(plainData), nil
}

// Read retrieves chunk data from local disk.
// If offset and length are 0, reads the entire chunk.
// Uses the LRU file descriptor cache for hot chunks to avoid repeated open/close.
func (cs *ChunkStore) Read(chunkID metadata.ChunkID, offset int64, length int32) ([]byte, uint32, error) {
	readSemStart := time.Now()
	cs.readSem <- struct{}{}
	cs.perf.readSemWaitNs.Add(time.Since(readSemStart).Nanoseconds())
	defer func() { <-cs.readSem }()

	path := cs.chunkPath(chunkID)

	// Get file descriptor from LRU cache (correct chunk ID mapping)
	f := cs.getFd(chunkID, path)
	if f == nil {
		return nil, 0, fmt.Errorf("datanode: %w: chunk %d", metadata.ErrChunkNotFound, chunkID)
	}

	// Read and verify header
	header := make([]byte, ChunkFileHeaderSize)
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, 0, fmt.Errorf("datanode: seek header: %w", err)
	}
	if _, err := io.ReadFull(f, header); err != nil {
		return nil, 0, fmt.Errorf("datanode: read header: %w", err)
	}
	if string(header[0:4]) != ChunkFileMagic {
		return nil, 0, fmt.Errorf("datanode: invalid chunk file magic")
	}

	dataLen := binary.BigEndian.Uint32(header[12:16])
	storedChecksum := binary.BigEndian.Uint32(header[16:20])

	// Optimized path: unsealed chunk with a range read and no encryption.
	// CRC=0 means CRC is stale (post-WriteAt), so we skip CRC verification
	// and use io.SectionReader to read only the needed bytes.
	wantRange := (offset > 0 || length > 0) && length > 0
	if wantRange && storedChecksum == 0 && (cs.encryptor == nil || !cs.encryptor.Enabled()) {
		dataStart := int64(ChunkFileHeaderSize)
		readLen := int64(length)
		if readLen > int64(dataLen)-offset {
			readLen = int64(dataLen) - offset
		}
		sr := io.NewSectionReader(f, dataStart+offset, readLen)
		result := make([]byte, readLen)
		if _, err := io.ReadFull(sr, result); err != nil {
			return nil, 0, fmt.Errorf("datanode: range read: %w", err)
		}
		cs.perf.readRequestedBytes.Add(int64(len(result)))
		cs.perf.readAmplifiedBytes.Add(int64(len(result)))
		return result, 0, nil
	}

	// General path: read full stored payload (required for CRC or encryption).
	if _, err := f.Seek(int64(ChunkFileHeaderSize), io.SeekStart); err != nil {
		return nil, 0, fmt.Errorf("datanode: seek: %w", err)
	}

	storedData := make([]byte, dataLen)
	if _, err := io.ReadFull(f, storedData); err != nil {
		return nil, 0, fmt.Errorf("datanode: read data: %w", err)
	}

	// Decrypt if encryption is enabled
	var plainData []byte
	if cs.encryptor != nil && cs.encryptor.Enabled() {
		decrypted, err := cs.encryptor.DecryptChunk(storedData)
		if err != nil {
			return nil, 0, fmt.Errorf("datanode: decrypt chunk: %w", err)
		}
		plainData = decrypted
	} else {
		plainData = storedData
	}

	// Apply offset/length on the plaintext
	readOffset := offset
	readLen := int32(len(plainData))
	if length > 0 {
		readLen = length
	}
	if readOffset+int64(readLen) > int64(len(plainData)) {
		readLen = int32(int64(len(plainData)) - readOffset)
	}
	cs.perf.readRequestedBytes.Add(int64(readLen))
	cs.perf.readAmplifiedBytes.Add(int64(len(storedData)))

	var result []byte
	if readOffset == 0 && readLen == int32(len(plainData)) {
		result = plainData
	} else {
		result = make([]byte, readLen)
		copy(result, plainData[readOffset:readOffset+int64(readLen)])
	}

	// End-to-end integrity check: verify CRC32 of plaintext matches
	// the stored checksum. Skipped for unsealed chunks (header checksum=0)
	// where CRC is stale from partial WriteAt operations; Seal() will
	// recompute it.
	if storedChecksum != 0 {
		computedChecksum := crc32.ChecksumIEEE(plainData)
		if computedChecksum != storedChecksum {
			return nil, 0, fmt.Errorf("datanode: checksum mismatch for chunk %d: stored=%d computed=%d (possible bitrot)", chunkID, storedChecksum, computedChecksum)
		}
	}

	// Update access stats
	cs.mu.RLock()
	if info, ok := cs.chunks[chunkID]; ok {
		info.LastAccess = time.Now()
		info.AccessCount++
	}
	cs.mu.RUnlock()

	return result, storedChecksum, nil
}

// Seal finalizes a chunk: updates the state to sealed.
// For chunks written via Seal() directly (no WriteAt), CRC was already
// computed during Write and is correct. For chunks that went through
// WriteAt (dirty state), CRC is recomputed from the file now.
func (cs *ChunkStore) Seal(chunkID metadata.ChunkID) (uint32, error) {
	cs.writeSem <- struct{}{}
	defer func() { <-cs.writeSem }()

	cs.mu.Lock()
	info, ok := cs.chunks[chunkID]
	if !ok {
		cs.mu.Unlock()
		return 0, fmt.Errorf("datanode: seal: chunk %d not found", chunkID)
	}
	if info.State == LocalSealed {
		checksum := info.Checksum
		cs.mu.Unlock()
		return checksum, nil
	}

	// Compute CRC from file (required after WriteAt or first Seal).
	path := cs.chunkPath(chunkID)
	checksum, err := cs.computeFileCRC(chunkID, path)
	if err != nil {
		cs.mu.Unlock()
		return 0, fmt.Errorf("datanode: seal compute CRC: %w", err)
	}
	info.State = LocalSealed
	info.Checksum = checksum
	cs.mu.Unlock()

	// Persist CRC to file header so Read can verify it.
	crcBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(crcBuf, checksum)
	if wf, err := os.OpenFile(path, os.O_WRONLY, 0644); err == nil {
		_, _ = wf.WriteAt(crcBuf, 16)
		wf.Close()
	}

	// Update metadata sidecar with sealed state
	cs.writeMetaSidecar(chunkID, info)

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
	start := time.Now()
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	result := make([]LocalChunkInfo, 0, len(cs.chunks))
	for _, info := range cs.chunks {
		result = append(result, *info)
	}
	cs.perf.listChunksNs.Add(time.Since(start).Nanoseconds())
	cs.perf.listChunksCalls.Add(1)
	cs.perf.listChunksItems.Add(int64(len(result)))
	return result
}

// Stats returns storage statistics.
func (cs *ChunkStore) Stats() (totalBytes int64, chunkCount int64) {
	return cs.totalBytes.Load(), cs.chunkCount.Load()
}

// DrainWrites waits for all in-flight writes to release the write semaphore.
// It returns a release function that must be called to hand the acquired slots
// back if the process continues after draining.
func (cs *ChunkStore) DrainWrites(ctx context.Context) (func(), error) {
	acquired := 0
	release := func() {
		for i := 0; i < acquired; i++ {
			<-cs.writeSem
		}
	}
	for i := 0; i < cap(cs.writeSem); i++ {
		select {
		case cs.writeSem <- struct{}{}:
			acquired++
		case <-ctx.Done():
			return release, ctx.Err()
		}
	}
	return release, nil
}

// Close releases process-local resources owned by the chunk store.
func (cs *ChunkStore) Close() error {
	cs.closeFdCache()
	return nil
}

// WriteSem returns the write semaphore channel for graceful shutdown drain.
// Callers can attempt to acquire all slots to wait for in-flight writes.
func (cs *ChunkStore) WriteSem() chan struct{} {
	return cs.writeSem
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

// ============================================================
// LRU File Descriptor Cache
// ============================================================

// fdLRU implements a simple LRU tracker for file descriptor eviction.
// It tracks access order without storing the actual values (those live in fdCache).
type fdLRU struct {
	mu       sync.Mutex
	capacity int
	order    []metadata.ChunkID // front = oldest, back = newest
	set      map[metadata.ChunkID]struct{}
}

func newFdLRU(capacity int) *fdLRU {
	return &fdLRU{
		capacity: capacity,
		order:    make([]metadata.ChunkID, 0, capacity),
		set:      make(map[metadata.ChunkID]struct{}),
	}
}

// touch marks a chunk ID as recently used, moving it to the back.
func (l *fdLRU) touch(id metadata.ChunkID) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.set[id]; ok {
		// Remove from current position
		for i, v := range l.order {
			if v == id {
				l.order = append(l.order[:i], l.order[i+1:]...)
				break
			}
		}
	}
	l.order = append(l.order, id)
	l.set[id] = struct{}{}
}

// evictOne returns the least recently used chunk ID for eviction, or 0 if empty.
func (l *fdLRU) evictOne() metadata.ChunkID {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.order) == 0 {
		return 0
	}
	id := l.order[0]
	l.order = l.order[1:]
	delete(l.set, id)
	return id
}

// remove removes a chunk ID from the LRU tracker.
func (l *fdLRU) remove(id metadata.ChunkID) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.set[id]; ok {
		for i, v := range l.order {
			if v == id {
				l.order = append(l.order[:i], l.order[i+1:]...)
				break
			}
		}
		delete(l.set, id)
	}
}

// len returns the number of tracked items.
func (l *fdLRU) len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.order)
}

// getFd retrieves or opens a file descriptor for the given chunk.
// It uses the LRU cache to avoid repeated open/close syscalls and ensures
// the correct file descriptor is returned for each chunk ID.
func (cs *ChunkStore) getFd(chunkID metadata.ChunkID, path string) *os.File {
	cs.fdMu.RLock()
	f, ok := cs.fdCache[chunkID]
	cs.fdMu.RUnlock()

	if ok {
		cs.perf.fdCacheHits.Add(1)
		cs.fdList.touch(chunkID)
		return f
	}
	cs.perf.fdCacheMisses.Add(1)

	// Open the file
	newF, err := os.Open(path)
	if err != nil {
		return nil
	}

	cs.fdMu.Lock()
	defer cs.fdMu.Unlock()

	// Double-check after acquiring write lock
	if f, ok := cs.fdCache[chunkID]; ok {
		newF.Close()
		cs.perf.fdCacheHits.Add(1)
		cs.fdList.touch(chunkID)
		return f
	}

	// Evict if at capacity
	for cs.fdList.len() >= cs.fdMax {
		evictID := cs.fdList.evictOne()
		if evictID != 0 {
			if oldF, ok := cs.fdCache[evictID]; ok {
				oldF.Close()
				delete(cs.fdCache, evictID)
				cs.perf.fdCacheEvictions.Add(1)
			}
		}
	}

	cs.fdCache[chunkID] = newF
	cs.fdList.touch(chunkID)
	return newF
}

// closeFdCache closes all cached file descriptors.
func (cs *ChunkStore) closeFdCache() {
	cs.fdMu.Lock()
	defer cs.fdMu.Unlock()
	for id, f := range cs.fdCache {
		f.Close()
		delete(cs.fdCache, id)
	}
}
