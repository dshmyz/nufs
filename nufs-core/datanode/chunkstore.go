package datanode

// DEPRECATED (V1): this legacy ChunkStore engine is scheduled for retirement
// per docs/v1-retirement-roadmap.md. The V2.1 segment engine is the
// replacement (--storage-version=v2.1, V2Store). Do NOT add new features,
// protocol methods, or optimization work here — implement against the V2.1
// engine instead. Retained only as the current default engine until the
// roadmap's stage 2 (parity audit + default flip) completes.

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dshmyz/nufs/nufs-core/internal/crypto"
	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// SyncConcurrency limits concurrent f.Sync() calls per disk to prevent I/O
// thrashing when many writers flush simultaneously.
const SyncConcurrency = 4

// ChunkStore manages local disk storage for data chunks across one or more
// disks (JBOD). Each disk is a diskShard with its own directory tree, I/O
// semaphores, fd cache, and WAL; the ChunkStore holds a global in-memory
// index (chunks map) and aggregate counters.
//
// On-disk layout per disk:
//
//	{dataDir}/chunks/{shard:02x}/{chunk_id}.dat
type ChunkStore struct {
	disks []*diskShard // one entry per physical disk

	mu         sync.RWMutex
	chunks     map[metadata.ChunkID]*LocalChunkInfo
	totalBytes atomic.Int64
	chunkCount atomic.Int64

	// stateVersion counts every change to a chunk's State that would
	// alter the replica state the heartbeat reports (new chunk, seal,
	// rewrite, corruption, delete). Heartbeat reads it to skip the
	// O(N) ListChunks scan when nothing has changed since the last
	// report. Guarded by mu.
	stateVersion uint64

	// At-rest encryption layer (nil = encryption disabled). Process-level,
	// shared across all disks.
	encryptor *crypto.Encryptor

	// Disk health manager - used to reject writes when a disk is FAILED
	// and to pick the target disk for new writes (multi-disk).
	disk *DiskManager

	perf chunkStorePerf

	// Background scan state - scanDone is closed when the initial scan
	// completes. Write/Read block on this channel to avoid serving stale data.
	scanDone chan struct{}
	scanErr  error
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
	writeOps           atomic.Int64 // total write attempts since last reset
	writeFailOps       atomic.Int64 // failed writes since last reset
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

// WriteErrorRate returns the rolling write error rate (0.0 - 1.0) and resets
// the counters. Called by HeartbeatReporter during each heartbeat cycle.
// Returns 0 if no writes occurred in the window.
func (cs *ChunkStore) WriteErrorRate() float64 {
	total := cs.perf.writeOps.Swap(0)
	fails := cs.perf.writeFailOps.Swap(0)
	if total == 0 {
		return 0
	}
	return float64(fails) / float64(total)
}

// NewChunkStore creates a single-disk ChunkStore (convenience wrapper for
// tests and single-disk deployments).
func NewChunkStore(dataDir string, maxWrites, maxReads int, wal *WriteAheadLog) (*ChunkStore, error) {
	return NewMultiDiskChunkStore([]string{dataDir}, maxWrites, maxReads, []*WriteAheadLog{wal})
}

// NewMultiDiskChunkStore creates a ChunkStore spanning multiple disks. Each
// entry in dataDirs becomes a diskShard; wals[i] (if non-nil) is the WAL for
// dataDirs[i].
func NewMultiDiskChunkStore(dataDirs []string, maxWrites, maxReads int, wals []*WriteAheadLog) (*ChunkStore, error) {
	if len(dataDirs) == 0 {
		return nil, fmt.Errorf("datanode: no data dirs")
	}
	cs := &ChunkStore{
		chunks:   make(map[metadata.ChunkID]*LocalChunkInfo),
		scanDone: make(chan struct{}),
	}
	for i, dir := range dataDirs {
		var wal *WriteAheadLog
		if wals != nil && i < len(wals) {
			wal = wals[i]
		}
		shard, err := newDiskShard(i, dir, maxWrites, maxReads, SyncConcurrency, wal, &cs.perf)
		if err != nil {
			return nil, err
		}
		cs.disks = append(cs.disks, shard)
	}
	// Scan existing chunks in background to avoid blocking startup.
	// Write/Read wait on scanDone before proceeding.
	cs.scanDone = make(chan struct{})
	go func() {
		cs.mu.Lock()
		cs.scanErr = cs.scanExisting()
		cs.mu.Unlock()
		close(cs.scanDone)
	}()
	return cs, nil
}

// WaitForScan blocks until the initial background scan completes.
// Returns the scan error if any.
func (cs *ChunkStore) WaitForScan() error {
	<-cs.scanDone
	return cs.scanErr
}

// SetEncryptor configures at-rest encryption for chunk data.
func (cs *ChunkStore) SetEncryptor(enc *crypto.Encryptor) {
	cs.encryptor = enc
}

// SetDiskManager wires disk health into the chunk store so writes are
// rejected on disk failure and new writes target the least-used healthy disk.
func (cs *ChunkStore) SetDiskManager(dm *DiskManager) {
	cs.disk = dm
}

// DiskManager returns the attached disk manager (nil if not set).
func (cs *ChunkStore) DiskManager() *DiskManager {
	return cs.disk
}

// pickDiskForWrite selects the target disk for a write. If the chunk already
// exists (overwrite), it returns the disk it currently lives on so the file
// is rewritten in place; otherwise it asks the DiskManager for the least-used
// healthy disk (falling back to disk 0 for single-disk deployments).
func (cs *ChunkStore) pickDiskForWrite(chunkID metadata.ChunkID) (*diskShard, error) {
	cs.mu.RLock()
	info, ok := cs.chunks[chunkID]
	cs.mu.RUnlock()
	if ok && info.DiskIndex >= 0 && info.DiskIndex < len(cs.disks) {
		return cs.disks[info.DiskIndex], nil
	}
	if cs.disk != nil && len(cs.disks) > 1 {
		idx, err := cs.disk.PickDisk()
		if err != nil {
			return nil, err
		}
		return cs.disks[idx], nil
	}
	return cs.disks[0], nil
}

// diskOf returns the disk holding chunkID, looked up from the in-memory
// index. Falls back to the first disk if the chunk is not yet indexed.
// Holds cs.mu.RLock through the cs.disks access so that concurrent
// AddDisk calls cannot race on the slice backing array.
func (cs *ChunkStore) diskOf(chunkID metadata.ChunkID) *diskShard {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	info, ok := cs.chunks[chunkID]
	if ok && info.DiskIndex >= 0 && info.DiskIndex < len(cs.disks) {
		return cs.disks[info.DiskIndex]
	}
	return cs.disks[0]
}

// Write stores chunk data to local disk.
// If WAL is configured, the write is logged before and committed after for crash recovery.
func (cs *ChunkStore) Write(chunkID metadata.ChunkID, data []byte) (writeErr error) {
	cs.perf.writeOps.Add(1)
	defer func() {
		if writeErr != nil {
			cs.perf.writeFailOps.Add(1)
		}
	}()

	if err := cs.WaitForScan(); err != nil {
		return fmt.Errorf("datanode: scan failed: %w", err)
	}
	disk, err := cs.pickDiskForWrite(chunkID)
	if err != nil {
		return fmt.Errorf("datanode: pick disk: %w", err)
	}
	// Per-disk error tracking: increment on failure, reset on success.
	defer func() {
		if writeErr != nil {
			if disk.recordWriteError() {
				slog.Error("datanode: disk marked FAILED after consecutive errors",
					"index", disk.index, "dir", disk.dataDir, "errors", disk.writeErrors.Load())
			}
		} else {
			disk.resetWriteErrors()
		}
	}()
	if err != nil {
		return fmt.Errorf("datanode: pick disk: %w", err)
	}
	// Reject writes if disk is in FAILED state (auto read-only on disk failure)
	if cs.disk != nil {
		if err := cs.disk.CanAdmitWrite(disk.index, int64(len(data))); err != nil {
			return fmt.Errorf("datanode: write rejected: %w", err)
		}
	}

	// Acquire write semaphore
	writeSemStart := time.Now()
	disk.writeSem <- struct{}{}
	cs.perf.writeSemWaitNs.Add(time.Since(writeSemStart).Nanoseconds())
	defer func() { <-disk.writeSem }()

	// Phase 1: Log intent to WAL (crash recovery: uncommitted writes are cleaned up)
	if disk.wal != nil {
		if err := disk.wal.LogWrite(chunkID, len(data)); err != nil {
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
	path := disk.chunkPath(chunkID)

	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("datanode: create chunk file: %w", err)
	}
	defer f.Close()

	// Write binary header - data_length stores the encrypted blob size
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
	disk.syncSem <- struct{}{}
	fsyncStart := time.Now()
	syncErr := f.Sync()
	cs.perf.fsyncNs.Add(time.Since(fsyncStart).Nanoseconds())
	cs.perf.fsyncCount.Add(1)
	<-disk.syncSem
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
		DiskIndex:   disk.index,
	}

	cs.mu.Lock()
	if existing, ok := cs.chunks[chunkID]; ok {
		// Overwrite: subtract old size, don't double-count chunkCount
		cs.totalBytes.Add(-existing.Size)
		disk.usedBytes.Add(-existing.Size)
	} else {
		cs.chunkCount.Add(1)
		disk.chunkCount.Add(1)
	}
	cs.chunks[chunkID] = info
	cs.stateVersion++
	// Snapshot under the lock: a concurrent Seal on replication may mutate
	// info (the same struct now in cs.chunks) before we marshal it below.
	meta := *info
	cs.mu.Unlock()

	cs.totalBytes.Add(int64(len(data)))
	disk.usedBytes.Add(int64(len(data)))

	// Write metadata sidecar
	disk.writeMetaSidecar(chunkID, meta)

	// Phase 2: Commit in WAL - write is durable, safe to ack
	if disk.wal != nil {
		if err := disk.wal.LogCommit(chunkID); err != nil {
			slog.Warn("datanode: WAL commit failed for chunk, data is safe on disk", "chunkID", chunkID, "error", err)
		}
	}

	return nil
}

// WriteGen implements LocalChunkStore.WriteGen. The legacy V1 ChunkStore has
// no metadata-generation concept — each chunk is a single file and every
// write overwrites it — so the generation is ignored and the write proceeds
// as a plain Write. Metadata V2 fencing is a V2.1 (segment) concern.
func (cs *ChunkStore) WriteGen(chunkID metadata.ChunkID, generation uint64, data []byte) error {
	return cs.Write(chunkID, data)
}

// WriteChunkReq is a single chunk write request for WriteBatch.
type WriteChunkReq struct {
	ChunkID metadata.ChunkID
	Data    []byte
}

// WriteBatch writes multiple chunks in a single batch, sharing the
// fsync semaphore slot so that N chunks incur at most one syncSem
// acquisition instead of N. Each file is still fsynced individually
// (the OS needs per-fd Sync), but they run concurrently under a
// single group slot, reducing contention on syncSem.
//
// The whole batch targets one disk (the least-used healthy disk) so the
// fsync-group optimization stays effective.
func (cs *ChunkStore) WriteBatch(reqs []WriteChunkReq) (batchErr error) {
	if len(reqs) == 0 {
		return nil
	}

	if err := cs.WaitForScan(); err != nil {
		return fmt.Errorf("datanode: scan failed: %w", err)
	}
	disk, err := cs.pickDiskForWrite(reqs[0].ChunkID)
	if err != nil {
		return fmt.Errorf("datanode: pick disk: %w", err)
	}

	// Per-disk error tracking for the batch.
	defer func() {
		if batchErr != nil {
			if disk.recordWriteError() {
				slog.Error("datanode: disk marked FAILED after consecutive errors",
					"index", disk.index, "dir", disk.dataDir, "errors", disk.writeErrors.Load())
			}
		} else {
			disk.resetWriteErrors()
		}
	}()

	// Check disk capacity for the total batch size
	totalSize := int64(0)
	for _, r := range reqs {
		totalSize += int64(len(r.Data))
	}
	if cs.disk != nil {
		if err := cs.disk.CanAdmitWrite(disk.index, totalSize); err != nil {
			return fmt.Errorf("datanode: batch write rejected: %w", err)
		}
	}

	// Acquire a single write semaphore slot for the whole batch.
	writeSemStart := time.Now()
	disk.writeSem <- struct{}{}
	cs.perf.writeSemWaitNs.Add(time.Since(writeSemStart).Nanoseconds())
	defer func() { <-disk.writeSem }()

	// Phase 1: Log all intents to WAL (crash recovery)
	if disk.wal != nil {
		for _, r := range reqs {
			if err := disk.wal.LogWrite(r.ChunkID, len(r.Data)); err != nil {
				return fmt.Errorf("datanode: WAL log write %d: %w", r.ChunkID, err)
			}
		}
	}

	type openFile struct {
		req      WriteChunkReq
		f        *os.File
		checksum uint32
	}
	openFiles := make([]*openFile, 0, len(reqs))

	for _, r := range reqs {
		storeData := r.Data
		if cs.encryptor != nil && cs.encryptor.Enabled() {
			encrypted, err := cs.encryptor.EncryptChunk(r.Data)
			if err != nil {
				for _, of := range openFiles {
					of.f.Close()
					os.Remove(disk.chunkPath(of.req.ChunkID))
				}
				return fmt.Errorf("datanode: encrypt chunk %d: %w", r.ChunkID, err)
			}
			storeData = encrypted
		}

		checksum := crc32.ChecksumIEEE(r.Data)
		path := disk.chunkPath(r.ChunkID)

		f, err := os.Create(path)
		if err != nil {
			for _, of := range openFiles {
				of.f.Close()
				os.Remove(disk.chunkPath(of.req.ChunkID))
			}
			return fmt.Errorf("datanode: create chunk file %d: %w", r.ChunkID, err)
		}

		header := make([]byte, ChunkFileHeaderSize)
		copy(header[0:4], ChunkFileMagic)
		binary.BigEndian.PutUint64(header[4:12], uint64(r.ChunkID))
		binary.BigEndian.PutUint32(header[12:16], uint32(len(storeData)))
		binary.BigEndian.PutUint32(header[16:20], checksum)

		if _, err := f.Write(header); err != nil {
			f.Close()
			for _, of := range openFiles {
				of.f.Close()
				os.Remove(disk.chunkPath(of.req.ChunkID))
			}
			os.Remove(path)
			return fmt.Errorf("datanode: write header %d: %w", r.ChunkID, err)
		}
		if _, err := f.Write(storeData); err != nil {
			f.Close()
			for _, of := range openFiles {
				of.f.Close()
				os.Remove(disk.chunkPath(of.req.ChunkID))
			}
			os.Remove(path)
			return fmt.Errorf("datanode: write data %d: %w", r.ChunkID, err)
		}

		openFiles = append(openFiles, &openFile{req: r, f: f, checksum: checksum})
	}

	// Phase 2: Fsync all files under a SINGLE syncSem slot.
	disk.syncSem <- struct{}{}
	fsyncStart := time.Now()
	var syncErr error
	for _, of := range openFiles {
		if err := of.f.Sync(); err != nil {
			syncErr = err
			break
		}
	}
	cs.perf.fsyncNs.Add(time.Since(fsyncStart).Nanoseconds())
	cs.perf.fsyncCount.Add(1)
	<-disk.syncSem

	for _, of := range openFiles {
		of.f.Close()
	}

	if syncErr != nil {
		for _, of := range openFiles {
			os.Remove(disk.chunkPath(of.req.ChunkID))
		}
		return fmt.Errorf("datanode: batch fsync: %w", syncErr)
	}

	// Phase 3: Update in-memory index for all chunks
	cs.mu.Lock()
	for _, of := range openFiles {
		info := &LocalChunkInfo{
			ChunkID:     of.req.ChunkID,
			Size:        int64(len(of.req.Data)),
			Checksum:    of.checksum,
			State:       LocalWritten,
			WrittenAt:   time.Now(),
			LastAccess:  time.Now(),
			AccessCount: 0,
			DiskIndex:   disk.index,
		}
		if existing, ok := cs.chunks[of.req.ChunkID]; ok {
			cs.totalBytes.Add(-existing.Size)
			disk.usedBytes.Add(-existing.Size)
		} else {
			cs.chunkCount.Add(1)
			disk.chunkCount.Add(1)
		}
		cs.chunks[of.req.ChunkID] = info
	}
	cs.stateVersion++
	cs.mu.Unlock()

	for _, of := range openFiles {
		cs.totalBytes.Add(int64(len(of.req.Data)))
		disk.usedBytes.Add(int64(len(of.req.Data)))
	}

	// Write metadata sidecars
	for _, of := range openFiles {
		info := &LocalChunkInfo{
			ChunkID:    of.req.ChunkID,
			Size:       int64(len(of.req.Data)),
			Checksum:   of.checksum,
			State:      LocalWritten,
			WrittenAt:  time.Now(),
			LastAccess: time.Now(),
			DiskIndex:  disk.index,
		}
		disk.writeMetaSidecar(of.req.ChunkID, *info)
	}

	// Phase 4: Commit all in WAL
	if disk.wal != nil {
		for _, of := range openFiles {
			if err := disk.wal.LogCommit(of.req.ChunkID); err != nil {
				slog.Warn("datanode: WAL commit failed for chunk, data is safe on disk",
					"chunkID", of.req.ChunkID, "error", err)
			}
		}
	}

	return nil
}

// WriteAt writes data at a specific offset within a chunk file.
// Used for partial/appending writes during replication.
// CRC is deferred to Seal() for performance - the on-disk CRC may be
// stale until Seal() recomputes it from the full file.
func (cs *ChunkStore) WriteAt(chunkID metadata.ChunkID, offset int64, data []byte) (writeErr error) {
	disk := cs.diskOf(chunkID)
	defer func() {
		if writeErr != nil {
			if disk.recordWriteError() {
				slog.Error("datanode: disk marked FAILED after consecutive errors",
					"index", disk.index, "dir", disk.dataDir, "errors", disk.writeErrors.Load())
			}
		} else {
			disk.resetWriteErrors()
		}
	}()

	writeSemStart := time.Now()
	disk.writeSem <- struct{}{}
	cs.perf.writeSemWaitNs.Add(time.Since(writeSemStart).Nanoseconds())
	defer func() { <-disk.writeSem }()

	path := disk.chunkPath(chunkID)

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

	// Mark chunk as dirty (CRC is stale) - Seal() will recompute.
	cs.mu.Lock()
	if info, ok := cs.chunks[chunkID]; ok {
		info.State = LocalWritten
		info.Checksum = 0
		cs.stateVersion++
	}
	cs.mu.Unlock()

	// Write CRC=0 to file header (cheap 4-byte overwrite at offset 16).
	zeroCRC := make([]byte, 4)
	f.WriteAt(zeroCRC, 16)

	// Invalidate fd cache so next Read picks up modified data.
	disk.evictFd(chunkID)

	return f.Sync()
}

// Read retrieves chunk data from local disk.
// If offset and length are 0, reads the entire chunk.
// Uses the LRU file descriptor cache for hot chunks to avoid repeated open/close.
func (cs *ChunkStore) Read(chunkID metadata.ChunkID, offset int64, length int32) ([]byte, uint32, error) {
	if err := cs.WaitForScan(); err != nil {
		return nil, 0, fmt.Errorf("datanode: scan failed: %w", err)
	}

	disk := cs.diskOf(chunkID)

	readSemStart := time.Now()
	disk.readSem <- struct{}{}
	cs.perf.readSemWaitNs.Add(time.Since(readSemStart).Nanoseconds())
	defer func() { <-disk.readSem }()

	path := disk.chunkPath(chunkID)
	f := disk.getFd(chunkID, path)
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

	// Optimized path: range read without encryption reads only the requested
	// bytes using io.SectionReader, avoiding reading the full chunk into memory.
	wantRange := (offset > 0 || length > 0) && length > 0
	if wantRange && (cs.encryptor == nil || !cs.encryptor.Enabled()) {
		dataStart := int64(ChunkFileHeaderSize)
		readLen := int64(length)
		if readLen > int64(dataLen)-offset {
			readLen = int64(dataLen) - offset
		}
		if readLen <= 0 {
			cs.perf.readRequestedBytes.Add(0)
			cs.perf.readAmplifiedBytes.Add(0)
			return nil, storedChecksum, nil
		}
		sr := io.NewSectionReader(f, dataStart+offset, readLen)
		result := make([]byte, readLen)
		if _, err := io.ReadFull(sr, result); err != nil {
			return nil, 0, fmt.Errorf("datanode: range read: %w", err)
		}
		cs.perf.readRequestedBytes.Add(int64(len(result)))
		cs.perf.readAmplifiedBytes.Add(int64(len(result)))

		cs.mu.Lock()
		if info, ok := cs.chunks[chunkID]; ok {
			info.LastAccess = time.Now()
			info.AccessCount++
		}
		cs.mu.Unlock()

		return result, storedChecksum, nil
	}

	// General path: read full stored payload (required for CRC or encryption).
	if _, err := f.Seek(int64(ChunkFileHeaderSize), io.SeekStart); err != nil {
		return nil, 0, fmt.Errorf("datanode: seek: %w", err)
	}

	storedData := make([]byte, dataLen)
	if _, err := io.ReadFull(f, storedData); err != nil {
		return nil, 0, fmt.Errorf("datanode: read data: %w", err)
	}

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

	if storedChecksum != 0 {
		computedChecksum := crc32.ChecksumIEEE(plainData)
		if computedChecksum != storedChecksum {
			return nil, 0, fmt.Errorf("datanode: checksum mismatch for chunk %d: stored=%d computed=%d (possible bitrot)", chunkID, storedChecksum, computedChecksum)
		}
	}

	cs.mu.Lock()
	if info, ok := cs.chunks[chunkID]; ok {
		info.LastAccess = time.Now()
		info.AccessCount++
	}
	cs.mu.Unlock()

	return result, storedChecksum, nil
}

// Seal finalizes a chunk: updates the state to sealed.
// For chunks written via Seal() directly (no WriteAt), CRC was already
// computed during Write and is correct. For chunks that went through
// WriteAt (dirty state), CRC is recomputed from the file now.
func (cs *ChunkStore) Seal(chunkID metadata.ChunkID) (uint32, error) {
	disk := cs.diskOf(chunkID)
	disk.writeSem <- struct{}{}
	defer func() { <-disk.writeSem }()

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
	path := disk.chunkPath(chunkID)
	checksum, err := disk.computeFileCRC(path, cs.encryptor)
	if err != nil {
		cs.mu.Unlock()
		return 0, fmt.Errorf("datanode: seal compute CRC: %w", err)
	}
	info.State = LocalSealed
	info.Checksum = checksum
	// Snapshot the sealed metadata under the lock. The sidecar marshal
	// (writeMetaSidecar) must not read the shared struct after we release
	// cs.mu, because a concurrent Write on replication could mutate it.
	meta := *info
	cs.stateVersion++
	cs.mu.Unlock()

	// Persist CRC to file header so Read can verify it.
	crcBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(crcBuf, checksum)
	wf, err := os.OpenFile(path, os.O_WRONLY, 0644)
	if err != nil {
		slog.Warn("datanode: seal: cannot open file to persist CRC", "chunkID", chunkID, "error", err)
	} else {
		if _, err := wf.WriteAt(crcBuf, 16); err != nil {
			slog.Warn("datanode: seal: failed to persist CRC to file header", "chunkID", chunkID, "error", err)
		}
		wf.Close()
	}

	disk.writeMetaSidecar(chunkID, meta)

	return checksum, nil
}

// Delete removes a chunk from local disk.
func (cs *ChunkStore) Delete(chunkID metadata.ChunkID) error {
	disk := cs.diskOf(chunkID)
	path := disk.chunkPath(chunkID)
	metaP := disk.metaPath(chunkID)

	// An unlinked file stays readable through an open descriptor. Evict it
	// first so a metadata-approved orphan deletion actually retires payload IO.
	disk.evictFd(chunkID)

	cs.mu.Lock()
	info, exists := cs.chunks[chunkID]
	if exists {
		cs.totalBytes.Add(-info.Size)
		cs.chunkCount.Add(-1)
		disk.usedBytes.Add(-info.Size)
		disk.chunkCount.Add(-1)
		delete(cs.chunks, chunkID)
		cs.stateVersion++
	}
	cs.mu.Unlock()

	os.Remove(path)
	os.Remove(metaP) // ignore error for sidecar
	return nil
}

// Info returns the local chunk info.
func (cs *ChunkStore) Info(chunkID metadata.ChunkID) (*LocalChunkInfo, bool) {
	<-cs.scanDone
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	info, ok := cs.chunks[chunkID]
	return info, ok
}

// ListChunks returns all locally stored chunk information.
func (cs *ChunkStore) ListChunks() []LocalChunkInfo {
	<-cs.scanDone
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

// Stats returns aggregate storage statistics across all disks.
func (cs *ChunkStore) Stats() (totalBytes int64, chunkCount int64) {
	<-cs.scanDone
	return cs.totalBytes.Load(), cs.chunkCount.Load()
}

// StateVersion returns a counter incremented on every chunk state change
// (new chunk, seal, rewrite, corruption, delete). A stable value between
// calls means the reported replica-state set is unchanged, so heartbeat
// can skip rebuilding it.
func (cs *ChunkStore) StateVersion() uint64 {
	<-cs.scanDone
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	return cs.stateVersion
}

// ChunkStateSnapshot returns the current replica-state view of every local
// chunk, mapped to the ReplicaState heartbeat reports. Built in one pass
// under the store lock; heartbeat caches it and only rebuilds when
// StateVersion changes.
func (cs *ChunkStore) ChunkStateSnapshot() map[metadata.ChunkID]metadata.ReplicaState {
	<-cs.scanDone
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	out := make(map[metadata.ChunkID]metadata.ReplicaState, len(cs.chunks))
	for id, info := range cs.chunks {
		switch info.State {
		case LocalSealed:
			out[id] = metadata.ReplicaReady
		case LocalCorrupt:
			out[id] = metadata.ReplicaFailed
		default:
			out[id] = metadata.ReplicaSyncing
		}
	}
	return out
}

// DiskStatsItem holds per-disk usage and health, used by the heartbeat
// reporter and the management interface.
type DiskStatsItem struct {
	Index      int
	UsedBytes  int64
	TotalBytes int64
	// OnDiskBytes is the physical bytes occupied by the store's data files
	// (segment/chunk records) on this disk, including superseded record
	// generations not yet reclaimed by seal+compaction. Distinct from
	// UsedBytes (logical live bytes); a console can show both honestly.
	OnDiskBytes int64
	ChunkCount  int64
	Failed      bool
	// State is the derived 3-tier health (DiskOnline/DiskDegraded/DiskFailed).
	// V2.1 V2Store fills it from failCount thresholds; V1 ChunkStore reports
	// DiskOnline (its own DiskManager state is surfaced via the legacy channel).
	State DiskState
}

// DiskStats returns per-disk usage breakdown.
func (cs *ChunkStore) DiskStats() []DiskStatsItem {
	<-cs.scanDone
	result := make([]DiskStatsItem, len(cs.disks))
	for i, d := range cs.disks {
		result[i] = DiskStatsItem{
			Index:       i,
			UsedBytes:   d.usedBytes.Load(),
			TotalBytes:  detectCapacityBytes(d.dataDir),
			OnDiskBytes: dataFilesOnDiskBytes(d.dataDir, ".dat"),
			ChunkCount:  d.chunkCount.Load(),
			Failed:      d.failed.Load(),
			State:       DiskOnline,
		}
	}
	return result
}

// ReconcileUsage re-scans the global index to fix per-disk usedBytes
// drift. Call periodically (e.g., after cross-node replica writes) to
// ensure DiskManager.PickDisk sees accurate usage.
func (cs *ChunkStore) ReconcileUsage() {
	<-cs.scanDone
	usage := make([]int64, len(cs.disks))
	cs.mu.RLock()
	for _, info := range cs.chunks {
		if info.DiskIndex >= 0 && info.DiskIndex < len(usage) {
			usage[info.DiskIndex] += info.Size
		}
	}
	cs.mu.RUnlock()
	for i, d := range cs.disks {
		d.usedBytes.Store(usage[i])
	}
}

// DrainWrites waits for all in-flight writes across every disk to release
// their write semaphores. It returns a release function that must be called
// to hand the acquired slots back if the process continues after draining.
func (cs *ChunkStore) DrainWrites(ctx context.Context) (func(), error) {
	var acquired []chan struct{}
	release := func() {
		for _, s := range acquired {
			<-s
		}
	}
	for _, d := range cs.disks {
		for i := 0; i < cap(d.writeSem); i++ {
			select {
			case d.writeSem <- struct{}{}:
				acquired = append(acquired, d.writeSem)
			case <-ctx.Done():
				return release, ctx.Err()
			}
		}
	}
	return release, nil
}

// Close releases process-local resources owned by the chunk store.
func (cs *ChunkStore) Close() error {
	for _, d := range cs.disks {
		d.closeFdCache()
	}
	return nil
}

// WriteSem returns the write semaphore channel of the first disk, for
// graceful-shutdown drain callers that operate on a single channel.
func (cs *ChunkStore) WriteSem() chan struct{} {
	return cs.disks[0].writeSem
}

// ========== Internal Helpers ==========

// scanExisting reads existing chunk files on all disks at startup to
// rebuild the in-memory index. Disks are scanned in parallel; each disk
// scans its 256 shard directories in parallel internally.
func (cs *ChunkStore) scanExisting() error {
	// Phase 1: WAL recovery — clean up orphaned chunk files left by
	// crashes that occurred between Write and LogCommit. This must
	// happen before scanning so the index doesn't include orphans.
	for _, d := range cs.disks {
		if d.wal != nil {
			if orphans, err := d.wal.Recover(); err != nil {
				slog.Warn("datanode: WAL recovery failed", "disk", d.index, "error", err)
			} else if len(orphans) > 0 {
				slog.Info("datanode: WAL cleaned orphan chunks", "disk", d.index, "count", len(orphans))
			}
		}
	}

	// Phase 2: Scan all disks for surviving chunk files.
	type diskScan struct {
		chunks     map[metadata.ChunkID]*LocalChunkInfo
		totalBytes int64
		chunkCount int64
	}
	results := make(chan diskScan, len(cs.disks))
	for _, d := range cs.disks {
		go func(d *diskShard) {
			chunks, totalBytes, chunkCount := d.scanShard()
			results <- diskScan{chunks, totalBytes, chunkCount}
		}(d)
	}
	for i := 0; i < len(cs.disks); i++ {
		r := <-results
		for id, info := range r.chunks {
			cs.chunks[id] = info
		}
		cs.totalBytes.Add(r.totalBytes)
		cs.chunkCount.Add(r.chunkCount)
	}
	// Track per-disk chunk counts for DiskStats (was previously derived
	// by scanning the global index every call). The caller holds
	// cs.mu, so no extra lock is needed here.
	for _, d := range cs.disks {
		d.chunkCount.Store(0)
	}
	for _, info := range cs.chunks {
		if info.DiskIndex >= 0 && info.DiskIndex < len(cs.disks) {
			cs.disks[info.DiskIndex].chunkCount.Add(1)
		}
	}
	// Initial index is complete and consistent; heartbeat deltas now have
	// a stable baseline version to diff against.
	cs.stateVersion++
	return nil
}

// ========== Disk Lifecycle (hot-add / retire) ==========

// DiskInfo is a read-only snapshot of one disk shard's metadata, returned
// by DiskInfos for the management interface.
type DiskInfo struct {
	Index       int
	Dir         string
	UsedBytes   int64
	OnDiskBytes int64
	ChunkCount  int64
	Failed      bool
	// State is the derived 3-tier health (DiskOnline/DiskDegraded/DiskFailed).
	// The legacy ChunkStore derives its own DiskManager state; V2.1 V2Store
	// fills this from its failCount thresholds. Zero value = DiskOnline.
	State DiskState
}

// diskIndexByDir resolves a directory to its disk index, preferring the first
// HEALTHY (non-Failed) entry that claims that dir and falling back to a Failed
// entry only when no healthy one matches.
//
// Both engines can hold two disk slots for the same dir after a retire +
// re-adopt round-trip: RemoveDisk preserves the retired slot (Failed, lower
// index) and AddDisk appends a fresh healthy slot. The ops/mgmt commands that
// address disks by dir (retire/migrate/decommission/verify) must target the
// healthy re-adopted disk, not the preserved failed one — matching the first
// entry would hit the failed slot and report "already retired" (or verify the
// closed backend) forever. Preferring healthy keeps the single-entry case
// identical (that entry is healthy), and only when every entry for the dir is
// failed does it select the failed one, preserving the "use retire / unreadable
// disk" behavior for a genuinely-only-failed disk.
func DiskIndexByDir(infos []DiskInfo, dir string) int {
	firstAny := -1
	for _, d := range infos {
		if d.Dir != dir {
			continue
		}
		if firstAny < 0 {
			firstAny = d.Index
		}
		if !d.Failed {
			return d.Index
		}
	}
	return firstAny
}

// DiskInfos returns a snapshot of all disk shards' metadata.
func (cs *ChunkStore) DiskInfos() []DiskInfo {
	infos := make([]DiskInfo, len(cs.disks))
	for i, d := range cs.disks {
		infos[i] = DiskInfo{
			Index:       d.index,
			Dir:         d.dataDir,
			UsedBytes:   d.usedBytes.Load(),
			OnDiskBytes: dataFilesOnDiskBytes(d.dataDir, ".dat"),
			ChunkCount:  d.chunkCount.Load(),
			Failed:      d.failed.Load(),
			State:       DiskOnline,
		}
	}
	return infos
}

// AddDisk hot-adds a new disk at runtime. The directory is scanned for
// existing chunks, which are merged into the global index with the new
// disk's index. Returns the index of the newly added disk.
func (cs *ChunkStore) AddDisk(dir string, maxWrites, maxReads int, wal *WriteAheadLog) (int, error) {
	idx := len(cs.disks)
	shard, err := newDiskShard(idx, dir, maxWrites, maxReads, SyncConcurrency, wal, &cs.perf)
	if err != nil {
		return -1, fmt.Errorf("datanode: add disk: %w", err)
	}
	// Phase 1: scan the new disk (reads only the filesystem, no global state).
	chunks, totalBytes, chunkCount := shard.scanShard()
	shard.usedBytes.Store(totalBytes)  // track pre-existing usage for PickDisk
	shard.chunkCount.Store(chunkCount) // track pre-existing count for DiskStats

	// Phase 2: append + merge under lock (fast map insert).
	cs.mu.Lock()
	cs.disks = append(cs.disks, shard)
	for id, info := range chunks {
		cs.chunks[id] = info
	}
	cs.totalBytes.Add(totalBytes)
	cs.chunkCount.Add(chunkCount)
	cs.mu.Unlock()

	// Register the new disk's capacity with the DiskManager if present.
	if cs.disk != nil {
		cs.disk.AddDiskCapacity(idx, 0) // 0 = capacity unknown; will be auto-detected
	}

	slog.Info("datanode: hot-added disk", "index", idx, "dir", dir, "chunks", chunkCount, "bytes", totalBytes)
	return idx, nil
}

// RemoveDisk marks a disk as failed (retired) so no new writes target it.
// Existing chunks on the disk remain readable until physically removed.
// The disk's index slot is preserved to avoid invalidating other disks'
// DiskIndex values.
func (cs *ChunkStore) RemoveDisk(idx int) error {
	if idx < 0 || idx >= len(cs.disks) {
		return fmt.Errorf("datanode: invalid disk index %d", idx)
	}
	d := cs.disks[idx]
	if d.failed.Load() {
		return fmt.Errorf("datanode: disk %d already retired", idx)
	}
	d.failed.Store(true)
	if cs.disk != nil {
		cs.disk.MarkDiskFailed(idx)
	}
	slog.Info("datanode: retired disk", "index", idx, "dir", d.dataDir)
	return nil
}

// MigrateChunk moves a single chunk from its current disk to targetIdx.
// The chunk is read from the source, written to the target, the index is
// updated, and the source file is deleted. This is safe to call
// concurrently with reads (the old file stays readable until deletion).
func (cs *ChunkStore) MigrateChunk(chunkID metadata.ChunkID, targetIdx int) error {
	if targetIdx < 0 || targetIdx >= len(cs.disks) {
		return fmt.Errorf("datanode: invalid target disk %d", targetIdx)
	}

	// 1. Look up source info.
	cs.mu.RLock()
	info, ok := cs.chunks[chunkID]
	cs.mu.RUnlock()
	if !ok {
		return fmt.Errorf("datanode: chunk %d not in index", chunkID)
	}
	srcIdx := info.DiskIndex
	if srcIdx == targetIdx {
		return nil // already on target
	}
	if srcIdx < 0 || srcIdx >= len(cs.disks) {
		return fmt.Errorf("datanode: chunk %d on invalid disk %d", chunkID, srcIdx)
	}

	src := cs.disks[srcIdx]
	dst := cs.disks[targetIdx]

	// 2. Read full chunk data from source.
	srcPath := src.chunkPath(chunkID)
	srcFd := src.getFd(chunkID, srcPath)
	if srcFd == nil {
		return fmt.Errorf("datanode: chunk %d not found on disk %d", chunkID, srcIdx)
	}
	src.readSem <- struct{}{}
	data, _, err := cs.Read(chunkID, 0, 0)
	<-src.readSem
	if err != nil {
		return fmt.Errorf("datanode: migrate read %d: %w", chunkID, err)
	}

	// 3. Write to target disk (direct file write, bypassing ChunkStore.Write
	// to avoid infinite recursion; the data is already payload, not header).
	dst.writeSem <- struct{}{}
	dstPath := dst.chunkPath(chunkID)
	f, err := os.Create(dstPath)
	if err != nil {
		<-dst.writeSem
		return fmt.Errorf("datanode: migrate create %d on disk %d: %w", chunkID, targetIdx, err)
	}
	// Write binary header.
	checksum := info.Checksum
	if checksum == 0 {
		// For unsealed chunks, compute checksum from raw data.
		checksum = crc32.ChecksumIEEE(data)
	}
	header := make([]byte, ChunkFileHeaderSize)
	copy(header[0:4], ChunkFileMagic)
	binary.BigEndian.PutUint64(header[4:12], uint64(chunkID))
	binary.BigEndian.PutUint32(header[12:16], uint32(len(data)))
	binary.BigEndian.PutUint32(header[16:20], checksum)
	if _, err := f.Write(header); err != nil {
		f.Close()
		os.Remove(dstPath)
		<-dst.writeSem
		return fmt.Errorf("datanode: migrate write header %d: %w", chunkID, err)
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		os.Remove(dstPath)
		<-dst.writeSem
		return fmt.Errorf("datanode: migrate write data %d: %w", chunkID, err)
	}
	dst.syncSem <- struct{}{}
	syncErr := f.Sync()
	<-dst.syncSem
	f.Close()
	if syncErr != nil {
		os.Remove(dstPath)
		return fmt.Errorf("datanode: migrate fsync %d: %w", chunkID, syncErr)
	}
	<-dst.writeSem

	// 4. Update index (DiskIndex + usedBytes).
	cs.mu.Lock()
	info.DiskIndex = targetIdx
	cs.chunks[chunkID] = info
	cs.mu.Unlock()
	src.usedBytes.Add(-int64(len(data)))
	dst.usedBytes.Add(int64(len(data)))
	src.chunkCount.Add(-1)
	dst.chunkCount.Add(1)

	// 5. Delete from source.
	os.Remove(srcPath)
	os.Remove(src.metaPath(chunkID))
	src.evictFd(chunkID)

	return nil
}

// MigrateDisk moves all chunks from disk srcIdx to other disks. It picks
// the least-used target for each chunk. Returns the number of migrated
// chunks and any error encountered (migration continues past errors).
func (cs *ChunkStore) MigrateDisk(srcIdx int) (int, error) {
	if srcIdx < 0 || srcIdx >= len(cs.disks) {
		return 0, fmt.Errorf("datanode: invalid source disk %d", srcIdx)
	}

	// Collect chunk IDs on the source disk.
	cs.mu.RLock()
	var toMigrate []metadata.ChunkID
	for id, info := range cs.chunks {
		if info.DiskIndex == srcIdx {
			toMigrate = append(toMigrate, id)
		}
	}
	cs.mu.RUnlock()

	if len(toMigrate) == 0 {
		return 0, nil
	}

	slog.Info("datanode: migrating disk", "src", srcIdx, "chunks", len(toMigrate))

	migrated := 0
	var lastErr error
	for _, id := range toMigrate {
		// Pick a target disk (least-used, non-failed, not source).
		targetIdx := cs.pickMigrationTarget(srcIdx)
		if targetIdx < 0 {
			lastErr = fmt.Errorf("datanode: no healthy target disk for migration")
			slog.Error("datanode: migration aborted", "remaining", len(toMigrate)-migrated, "error", lastErr)
			break
		}
		if err := cs.MigrateChunk(id, targetIdx); err != nil {
			lastErr = err
			slog.Warn("datanode: chunk migration failed", "chunkID", id, "error", err)
			continue
		}
		migrated++
	}

	slog.Info("datanode: disk migration complete", "src", srcIdx, "migrated", migrated, "of", len(toMigrate))
	return migrated, lastErr
}

// pickMigrationTarget returns the least-used healthy disk that is NOT srcIdx.
func (cs *ChunkStore) pickMigrationTarget(srcIdx int) int {
	bestIdx := -1
	var bestUsed int64 = -1
	for i, d := range cs.disks {
		if i == srcIdx || d.failed.Load() {
			continue
		}
		used := d.usedBytes.Load()
		if bestIdx == -1 || used < bestUsed {
			bestIdx = i
			bestUsed = used
		}
	}
	return bestIdx
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
