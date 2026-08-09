package datanode

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/dshmyz/nufs/nufs-core/internal/crypto"
	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// diskShard holds the per-disk state for one physical disk (JBOD model).
// A ChunkStore manages a slice of diskShards; each shard owns its own
// directory tree, I/O semaphores, fd cache, and WAL so that a slow or
// failed disk does not block the others.
type diskShard struct {
	index     int // position in ChunkStore.disks; recorded on chunk metadata
	dataDir   string
	chunksDir string // {dataDir}/chunks

	writeSem chan struct{} // per-disk concurrent write limiting
	readSem  chan struct{} // per-disk concurrent read limiting
	syncSem  chan struct{} // per-disk fsync throttling

	wal *WriteAheadLog // per-disk crash-recovery log (nil = disabled)

	// Per-disk usage and health, read by DiskManager.PickDisk /
	// CanAdmitWrite to spread writes and isolate failed disks.
	usedBytes    atomic.Int64
	chunkCount   atomic.Int64 // mirror of the global count, per disk
	failed       atomic.Bool
	writeErrors  atomic.Int64 // consecutive write errors; marks disk failed at threshold

	// File descriptor cache for hot chunks (per-disk so fds never cross
	// disk boundaries). Uses LRU eviction to bound open file descriptors.
	fdMu              sync.RWMutex
	fdCache           map[metadata.ChunkID]*os.File
	fdList            *fdLRU
	fdMax             int
	getFdBeforeInsert func() // test hook

	// Shared performance counters (points at ChunkStore.perf so per-disk
	// methods update the same counters the store reports).
	perf *chunkStorePerf
}

// newDiskShard creates a diskShard rooted at dataDir. The 256 shard
// directories under {dataDir}/chunks are created here.
func newDiskShard(index int, dataDir string, maxWrites, maxReads, syncCap int, wal *WriteAheadLog, perf *chunkStorePerf) (*diskShard, error) {
	chunksDir := filepath.Join(dataDir, "chunks")
	for i := 0; i < MaxShards; i++ {
		shardDir := filepath.Join(chunksDir, fmt.Sprintf("%02x", i))
		if err := os.MkdirAll(shardDir, 0755); err != nil {
			return nil, fmt.Errorf("datanode: create shard dir: %w", err)
		}
	}
	if syncCap <= 0 {
		syncCap = SyncConcurrency
	}
	d := &diskShard{
		index:     index,
		dataDir:   dataDir,
		chunksDir: chunksDir,
		writeSem:  make(chan struct{}, maxWrites),
		readSem:   make(chan struct{}, maxReads),
		syncSem:   make(chan struct{}, syncCap),
		wal:       wal,
		fdCache:   make(map[metadata.ChunkID]*os.File),
		fdList:    newFdLRU(256),
		fdMax:     256,
		perf:      perf,
	}
	// Tell the WAL where this disk's chunk files live so Recover() cleans
	// up orphaned chunk files at the correct path.
	if wal != nil {
		wal.SetDataDir(dataDir)
	}
	return d, nil
}

// chunkPath returns the file path for a chunk on this disk.
func (d *diskShard) chunkPath(chunkID metadata.ChunkID) string {
	shard := uint64(chunkID) % MaxShards
	return filepath.Join(d.chunksDir, fmt.Sprintf("%02x", shard), fmt.Sprintf("%d.dat", chunkID))
}

// metaPath returns the metadata sidecar path for a chunk on this disk.
func (d *diskShard) metaPath(chunkID metadata.ChunkID) string {
	shard := uint64(chunkID) % MaxShards
	return filepath.Join(d.chunksDir, fmt.Sprintf("%02x", shard), fmt.Sprintf("%d.meta", chunkID))
}

// getFd returns a cached open file descriptor for chunkID, opening one if
// necessary. LRU-evicts to bound the number of open fds. Returns nil if
// the file does not exist.
func (d *diskShard) getFd(chunkID metadata.ChunkID, path string) *os.File {
	d.fdMu.RLock()
	f, ok := d.fdCache[chunkID]
	d.fdMu.RUnlock()

	if ok {
		d.perf.fdCacheHits.Add(1)
		d.fdList.touch(chunkID)
		return f
	}
	d.perf.fdCacheMisses.Add(1)

	newF, err := os.Open(path)
	if err != nil {
		return nil
	}
	if hook := d.getFdBeforeInsert; hook != nil {
		hook()
	}

	d.fdMu.Lock()
	defer d.fdMu.Unlock()
	if f, ok := d.fdCache[chunkID]; ok {
		newF.Close()
		d.perf.fdCacheHits.Add(1)
		d.fdList.touch(chunkID)
		return f
	}
	if _, err := os.Stat(path); err != nil {
		newF.Close()
		return nil
	}

	for d.fdList.len() >= d.fdMax {
		evictID := d.fdList.evictOne()
		if evictID != 0 {
			if oldF, ok := d.fdCache[evictID]; ok {
				oldF.Close()
				delete(d.fdCache, evictID)
				d.perf.fdCacheEvictions.Add(1)
			}
		}
	}

	d.fdCache[chunkID] = newF
	d.fdList.touch(chunkID)
	return newF
}

// evictFd removes a chunk's fd from the cache (used by Delete/WriteAt so
// the next read sees fresh data).
func (d *diskShard) evictFd(chunkID metadata.ChunkID) {
	d.fdMu.Lock()
	if f, ok := d.fdCache[chunkID]; ok {
		_ = f.Close()
		delete(d.fdCache, chunkID)
	}
	d.fdMu.Unlock()
	d.fdList.remove(chunkID)
}

// closeFdCache closes all cached file descriptors on this disk.
func (d *diskShard) closeFdCache() {
	d.fdMu.Lock()
	defer d.fdMu.Unlock()
	for _, f := range d.fdCache {
		_ = f.Close()
	}
	d.fdCache = make(map[metadata.ChunkID]*os.File)
	d.fdList = newFdLRU(d.fdMax)
}

// writeMetaSidecar writes a JSON metadata sidecar file for the chunk. It
// takes the info by value: callers must pass a snapshot captured under the
// ChunkStore lock, so the marshal here never races a concurrent Seal/Write
// mutating the shared *LocalChunkInfo in cs.chunks.
func (d *diskShard) writeMetaSidecar(chunkID metadata.ChunkID, info LocalChunkInfo) {
	data, err := json.Marshal(info)
	if err != nil {
		return
	}
	_ = os.WriteFile(d.metaPath(chunkID), data, 0644)
}

// readChunkFileHeader reads and parses a chunk file header, tagging the
// result with this shard's index so the caller knows which disk owns it.
func (d *diskShard) readChunkFileHeader(path string) (*LocalChunkInfo, error) {
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
		ChunkID:   chunkID,
		Size:      int64(dataLen),
		Checksum:  checksum,
		State:     state,
		DiskIndex: d.index,
	}, nil
}

// computeFileCRC reads a chunk file and computes the CRC32 of its plaintext
// (decrypting first if encryption is enabled). Used by Seal.
func (d *diskShard) computeFileCRC(path string, enc *crypto.Encryptor) (uint32, error) {
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
	if enc != nil && enc.Enabled() {
		decrypted, err := enc.DecryptChunk(storedData)
		if err != nil {
			return 0, err
		}
		plainData = decrypted
	} else {
		plainData = storedData
	}

	return crc32.ChecksumIEEE(plainData), nil
}

// scanShard scans this disk's 256 shard directories and returns the chunks
// found, tagged with this shard's index. Used at startup to rebuild the
// in-memory index.
func (d *diskShard) scanShard() (map[metadata.ChunkID]*LocalChunkInfo, int64, int64) {
	type shardResult struct {
		chunks     map[metadata.ChunkID]*LocalChunkInfo
		totalBytes int64
		chunkCount int64
	}

	results := make(chan shardResult, MaxShards)
	for i := 0; i < MaxShards; i++ {
		go func(shard int) {
			shardDir := filepath.Join(d.chunksDir, fmt.Sprintf("%02x", shard))
			entries, err := os.ReadDir(shardDir)
			if err != nil {
				results <- shardResult{}
				return
			}
			chunks := make(map[metadata.ChunkID]*LocalChunkInfo)
			var totalBytes, chunkCount int64
			for _, entry := range entries {
				if entry.IsDir() || filepath.Ext(entry.Name()) != ".dat" {
					continue
				}
				path := filepath.Join(shardDir, entry.Name())
				info, err := d.readChunkFileHeader(path)
				if err != nil {
					continue
				}
				chunks[info.ChunkID] = info
				totalBytes += info.Size
				chunkCount++
			}
			results <- shardResult{chunks, totalBytes, chunkCount}
		}(i)
	}

	merged := make(map[metadata.ChunkID]*LocalChunkInfo)
	var totalBytes, chunkCount int64
	for i := 0; i < MaxShards; i++ {
		r := <-results
		for id, info := range r.chunks {
			merged[id] = info
		}
		totalBytes += r.totalBytes
		chunkCount += r.chunkCount
	}
	return merged, totalBytes, chunkCount
}

// diskIOErrorThreshold is the number of consecutive write errors before
// a disk is automatically marked failed.
const diskIOErrorThreshold = 5

// recordWriteError increments the consecutive error counter. Returns true
// if the disk was just marked failed (counter reached the threshold).
func (d *diskShard) recordWriteError() bool {
	n := d.writeErrors.Add(1)
	if n >= diskIOErrorThreshold {
		d.failed.Store(true)
		return true
	}
	return false
}

// resetWriteErrors resets the consecutive error counter (called on
// successful writes to prevent stale failures from accumulating).
func (d *diskShard) resetWriteErrors() {
	d.writeErrors.Store(0)
}
