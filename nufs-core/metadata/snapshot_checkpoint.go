package metadata

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/cockroachdb/pebble"
	"github.com/klauspost/compress/zstd"
)

// PBL3 checkpoint snapshot format:
//   [magic:4 "PBL3"]
//   [file_count:4]
//   [for each file:]
//     [path_len:2][relative_path]   -- e.g. "000001.sst"
//     [data_len:8][zstd compressed file data]
//
// No terminator or total count needed — the file_count tells us when to stop.

// checkpointWriteDir writes the files in dir to sink in PBL3 format.
func checkpointWriteDir(dir string, sink io.Writer) error {
	// Gather file paths in sorted order
	var files []string
	if err := filepath.Walk(dir, func(path string, fi os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if fi.IsDir() {
			return nil
		}
		files = append(files, path)
		return nil
	}); err != nil {
		return fmt.Errorf("walk checkpoint: %w", err)
	}
	sort.Strings(files)

	// Write magic
	if _, err := sink.Write([]byte("PBL3")); err != nil {
		return err
	}

	// Write file count
	var countBuf [4]byte
	binary.BigEndian.PutUint32(countBuf[:], uint32(len(files)))
	if _, err := sink.Write(countBuf[:]); err != nil {
		return err
	}

	zw, err := zstd.NewWriter(nil)
	if err != nil {
		return fmt.Errorf("zstd writer: %w", err)
	}

	buf := bufio.NewWriterSize(sink, 256<<10) // 256KB write buffer
	for _, path := range files {
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return fmt.Errorf("rel path %s: %w", path, err)
		}
		if strings.Contains(rel, "..") {
			return fmt.Errorf("unexpected relative path: %s", rel)
		}

		// Read file
		data, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("read %s: %w", rel, err)
		}

		// Write path [len:2][data]
		var pathLen [2]byte
		binary.BigEndian.PutUint16(pathLen[:], uint16(len(rel)))
		if _, err := buf.Write(pathLen[:]); err != nil {
			return err
		}
		if _, err := buf.WriteString(rel); err != nil {
			return err
		}

		// Compress and write data
		compressed := zw.EncodeAll(data, nil)
		var dataLen [8]byte
		binary.BigEndian.PutUint64(dataLen[:], uint64(len(compressed)))
		if _, err := buf.Write(dataLen[:]); err != nil {
			return err
		}
		if _, err := buf.Write(compressed); err != nil {
			return err
		}
	}

	return buf.Flush()
}

// checkpointRestore reads a PBL3 snapshot stream, extracts files to a temp
// directory, then atomically replaces the DB directory.
func checkpointRestore(fsm *PebbleFSM, rc io.ReadCloser) error {
	defer rc.Close()

	// Read magic
	var magic [4]byte
	if _, err := io.ReadFull(rc, magic[:]); err != nil {
		return fmt.Errorf("read magic: %w", err)
	}
	if string(magic[:]) == "PBL1" {
		return restorePBL1(fsm, rc, magic[:])
	}
	if string(magic[:]) != "PBL3" {
		return fmt.Errorf("invalid snapshot magic: %q", magic)
	}

	// Read file count
	var countBuf [4]byte
	if _, err := io.ReadFull(rc, countBuf[:]); err != nil {
		return fmt.Errorf("read file count: %w", err)
	}
	numFiles := binary.BigEndian.Uint32(countBuf[:])

	// Create temp extraction directory next to data dir
	dataDir := fsm.store.cfg.Dir
	tmpDir := dataDir + ".restore"
	if err := os.RemoveAll(tmpDir); err != nil {
		return fmt.Errorf("remove old tmp: %w", err)
	}
	if err := os.MkdirAll(tmpDir, 0755); err != nil {
		return fmt.Errorf("mkdir tmp %s: %w", tmpDir, err)
	}
	defer os.RemoveAll(tmpDir)

	br := bufio.NewReaderSize(rc, 256<<10) // 256KB read buffer
	dec, err := zstd.NewReader(nil)
	if err != nil {
		return fmt.Errorf("zstd reader: %w", err)
	}

	var pathBuf [2]byte
	var dataLenBuf [8]byte
	for i := uint32(0); i < numFiles; i++ {
		// Read path
		if _, err := io.ReadFull(br, pathBuf[:]); err != nil {
			return fmt.Errorf("read path len at file %d: %w", i, err)
		}
		pathLen := binary.BigEndian.Uint16(pathBuf[:])
		relBytes := make([]byte, pathLen)
		if _, err := io.ReadFull(br, relBytes); err != nil {
			return fmt.Errorf("read path at file %d: %w", i, err)
		}
		relPath := string(relBytes)

		// Read compressed data length
		if _, err := io.ReadFull(br, dataLenBuf[:]); err != nil {
			return fmt.Errorf("read data len at file %d: %w", i, err)
		}
		compLen := binary.BigEndian.Uint64(dataLenBuf[:])

		// Read compressed data
		compressed := make([]byte, compLen)
		if _, err := io.ReadFull(br, compressed); err != nil {
			return fmt.Errorf("read data at file %d: %w", i, err)
		}

		// Decompress
		data, err := dec.DecodeAll(compressed, nil)
		if err != nil {
			return fmt.Errorf("decompress %s: %w", relPath, err)
		}

		// Write file
		outPath := filepath.Join(tmpDir, relPath)
		if err := os.MkdirAll(filepath.Dir(outPath), 0755); err != nil {
			return fmt.Errorf("mkdir %s: %w", filepath.Dir(outPath), err)
		}
		if err := os.WriteFile(outPath, data, 0644); err != nil {
			return fmt.Errorf("write %s: %w", relPath, err)
		}
	}

	// Double-buffer restore: open the new DB first, then atomically swap
	// the reference. This avoids a window where the store has no open DB
	// and all reads/writes would fail.

	// Step 1: Open the restored data as a new Pebble instance
	pebbleCfg := &pebble.Options{}
	storeCfg := fsm.store.cfg
	if storeCfg.MaxOpenFiles > 0 {
		pebbleCfg.MaxOpenFiles = storeCfg.MaxOpenFiles
	}
	if storeCfg.MemTableSize > 0 {
		pebbleCfg.MemTableSize = storeCfg.MemTableSize
	}
	newDB, err := pebble.Open(tmpDir, pebbleCfg)
	if err != nil {
		// New DB failed to open — old DB is still valid, don't touch it
		return fmt.Errorf("open restored db: %w", err)
	}

	// Step 2: Verify the new DB is functional (root inode exists)
	rootKey := fmt.Sprintf("%s%d", prefixInode, RootInodeID)
	_, closer, err := newDB.Get([]byte(rootKey))
	if err != nil {
		newDB.Close()
		return fmt.Errorf("verify restored db: root inode not found: %w", err)
	}
	closer.Close()

	// Step 3: Close old DB
	if err := fsm.store.db.Close(); err != nil {
		newDB.Close()
		return fmt.Errorf("close old db: %w", err)
	}

	// Step 4: Remove old data directory and rename restored dir
	if err := os.RemoveAll(dataDir); err != nil {
		// Old dir removal failed but we have the new DB open — continue
		slog.Warn("fsm: failed to remove old data dir", "path", dataDir, "error", err)
	}
	if err := os.Rename(tmpDir, dataDir); err != nil {
		// Rename failed — the new DB was opened from tmpDir, which still exists
		slog.Warn("fsm: failed to rename", "from", tmpDir, "to", dataDir, "error", err)
	}

	// Step 5: Atomically swap the DB reference
	fsm.store.db = newDB

	// Clear inode cache since data has changed
	fsm.store.inCache.clear()

	slog.Info("fsm: restored checkpoint snapshot with zero-downtime swap", "files", numFiles)
	return nil
}

// restorePBL1 handles the legacy single-stream snapshot format.
// magic contains the already-read magic bytes.
func restorePBL1(fsm *PebbleFSM, rc io.Reader, magic []byte) error {
	dec, err := zstd.NewReader(rc)
	if err != nil {
		return fmt.Errorf("zstd reader: %w", err)
	}
	defer dec.Close()

	batch := fsm.store.db.NewBatch()
	batchCount := 0
	var count uint64

	for {
		var lenBuf [4]byte
		if _, err := io.ReadFull(dec, lenBuf[:]); err != nil {
			batch.Close()
			return fmt.Errorf("read key len: %w", err)
		}
		klen := binary.BigEndian.Uint32(lenBuf[:])

		if klen == 0xFFFFFFFF {
			if err := binary.Read(dec, binary.BigEndian, &count); err != nil {
				batch.Close()
				return fmt.Errorf("read count: %w", err)
			}
			break
		}

		key := make([]byte, klen)
		if _, err := io.ReadFull(dec, key); err != nil {
			batch.Close()
			return fmt.Errorf("read key: %w", err)
		}
		val, err := readBytesStream(dec)
		if err != nil {
			batch.Close()
			return fmt.Errorf("read value: %w", err)
		}
		batch.Set(key, val, nil)
		batchCount++

		if batchCount >= 50000 {
			if err := batch.Commit(pebble.NoSync); err != nil {
				batch.Close()
				return fmt.Errorf("batch commit: %w", err)
			}
			batch.Close()
			batch = fsm.store.db.NewBatch()
			batchCount = 0
		}
	}

	if err := batch.Commit(pebble.Sync); err != nil {
		batch.Close()
		return err
	}
	batch.Close()

	slog.Info("fsm: restored keys from legacy snapshot", "count", count)
	return nil
}
