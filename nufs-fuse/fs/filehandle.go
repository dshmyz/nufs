// Copyright (c) 2021 MinIO, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package minfs

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"bazil.org/fuse"

	"github.com/minio/minfs/meta"
)

// readBufPool reuses read buffers across Read calls to reduce GC pressure.
// Buffers are sized at 128KB (default FUSE MaxReadahead).
var readBufPool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, 128*1024)
		return &buf
	},
}

// FileHandle - Contains an opened file which can be read from and written to
type FileHandle struct {
	// the os file handle
	*os.File

	// the fuse file
	f *File

	// cache file has been written to
	dirty bool

	// last Flush (upload to S3) succeeded
	flushed bool

	// file data has been downloaded from S3 to local cache
	downloaded bool
	// protects the lazy-download path in Read
	dlOnce sync.Once
	dlErr  error
	// context for lazy download
	dlCtx context.Context

	cachePath string

	handle uint64
}

// ensureDownloaded lazily downloads the file from S3 on first read.
// Subsequent calls are no-ops protected by sync.Once.
func (fh *FileHandle) ensureDownloaded() error {
	fh.dlOnce.Do(func() {
		if fh.downloaded {
			return
		}
		// Re-open the cache file for writing so cacheSave can populate it.
		fh.File.Close()
		fh.dlErr = fh.f.cacheSave(fh.dlCtx, fh.cachePath, &fuse.OpenRequest{})
		if fh.dlErr != nil {
			return
		}
		// Re-open the cache file for read/write.
		f, err := os.OpenFile(fh.cachePath, os.O_RDWR, fh.f.mfs.config.mode)
		if err != nil {
			fh.dlErr = err
			return
		}
		fh.File = f
		fh.downloaded = true
		fh.f.mfs.touchCache(fh.cachePath)
	})
	return fh.dlErr
}

// Read from the file handle
func (fh *FileHandle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	metricsIncRead()

	// Lazy download: fetch from S3 on first read if not already downloaded.
	if !fh.downloaded {
		if err := fh.ensureDownloaded(); err != nil {
			return err
		}
	}

	bufp := readBufPool.Get().(*[]byte)
	defer readBufPool.Put(bufp)
	buff := *bufp
	if req.Size > len(buff) {
		buff = make([]byte, req.Size)
	}
	start := time.Now()
	n, err := fh.File.ReadAt(buff[:req.Size], req.Offset)
	metricsObserveRead(time.Since(start).Seconds())
	if err != nil && err != io.EOF {
		return err
	}
	resp.Data = buff[:n]
	return nil
}

// Write to the file handle
func (fh *FileHandle) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	if fh.f.mfs.config.readOnly {
		return fuse.EPERM
	}
	metricsIncWrite()

	// Ensure file data is available before writing (partial overwrite needs existing data).
	if !fh.downloaded {
		if err := fh.ensureDownloaded(); err != nil {
			return err
		}
	}

	if _, err := fh.File.Seek(req.Offset, 0); err != nil {
		return err
	}
	start := time.Now()
	n, err := fh.File.Write(req.Data)
	metricsObserveWrite(time.Since(start).Seconds())
	if err != nil {
		return err
	}
	// Writes that grow the file are expected to update the file size
	// (as seen through Attr). Note that file size changes are
	// communicated also through Setattr.
	if fh.f.Size < uint64(req.Offset)+uint64(n) {
		fh.f.Size = uint64(req.Offset) + uint64(n)
	}
	resp.Size = n
	fh.dirty = true
	fh.f.mfs.touchCache(fh.cachePath)
	return nil
}

// Fsync flushes the local cache file to disk to ensure data durability.
func (f *File) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	// Find the open file handle for this file and sync it.
	f.mfs.m.Lock()
	var fh *FileHandle
	for _, h := range f.mfs.handles {
		if h != nil && h.f == f {
			fh = h
			break
		}
	}
	f.mfs.m.Unlock()

	if fh != nil && fh.File != nil {
		return fh.File.Sync()
	}
	return nil
}

// Release the file handle. Only deletes the local cache if the last Flush
// succeeded, preventing data loss when upload to S3 failed.
func (fh *FileHandle) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	metricsIncRelease()
	if err := fh.Close(); err != nil {
		return err
	}

	remotePath := fh.f.RemotePath()
	defer fh.f.mfs.Release(fh)

	if fh.dirty && !fh.flushed {
		// Flush failed — preserve the cache file for crash recovery.
		fh.f.mfs.log.Printf("WARNING: cache file %s retained for %s (upload to S3 failed)\n",
			fh.cachePath, remotePath)
		return fmt.Errorf("minfs: release: file %s has uncommitted changes (upload failed, cache retained at %s)",
			remotePath, fh.cachePath)
	}

	// Successful flush — clear pending upload record.
	fh.f.mfs.clearPending(fh.cachePath)

	// Safe to remove cache — either flushed successfully or never modified.
	os.Remove(fh.cachePath)
	return nil
}

// Flush uploads the modified cache file to S3. On success, marks the file
// as flushed so Release() can safely delete the local cache.
func (fh *FileHandle) Flush(ctx context.Context, req *fuse.FlushRequest) error {
	metricsIncFlush()
	if !fh.dirty {
		return nil
	}

	// Record pending upload for crash recovery BEFORE attempting upload.
	remotePath := fh.f.RemotePath()
	fh.f.mfs.recordPending(remotePath, fh.cachePath, int64(fh.f.Size))

	sr := newPutOp(fh.Name(), remotePath, int64(fh.f.Size))
	if err := fh.f.mfs.sync(&sr); err != nil {
		fh.flushed = false
		fh.f.mfs.audit("flush", remotePath, "error")
		return err
	}

	// we'll wait for the request to be uploaded and synced, before
	// releasing the file
	if err := <-sr.Error; err != nil {
		fh.flushed = false
		fh.f.mfs.audit("flush", remotePath, "error")
		return err
	}

	// Upload succeeded — clear pending record and update cache metadata.
	fh.f.mfs.clearPending(fh.cachePath)

	// update cache
	if err := fh.f.mfs.db.Update(func(tx *meta.Tx) error {
		return fh.f.store(tx)
	}); err != nil {
		fh.flushed = false
		return err
	}

	fh.dirty = false
	fh.flushed = true
	fh.f.mfs.audit("flush", remotePath, "ok")
	return nil
}
