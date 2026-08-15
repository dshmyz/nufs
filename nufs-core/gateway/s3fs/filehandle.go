package s3fs

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/minio/minio-go/v7"
)

// readBufPool reuses read buffers across Read calls.
var readBufPool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, 128*1024)
		return &buf
	},
}

// S3FileHandle manages per-open-file state.
type S3FileHandle struct {
	*os.File
	f *S3File

	handle uint64

	dirty      bool
	flushed    bool
	downloaded bool
	dlOnce     sync.Once
	dlErr      error
	dlCtx      context.Context

	cachePath string
}

var (
	_ fs.FileReader   = (*S3FileHandle)(nil)
	_ fs.FileWriter   = (*S3FileHandle)(nil)
	_ fs.FileFlusher  = (*S3FileHandle)(nil)
	_ fs.FileReleaser = (*S3FileHandle)(nil)
	_ fs.FileFsyncer  = (*S3FileHandle)(nil)
)

// ensureDownloaded lazily downloads from S3 on first read.
func (fh *S3FileHandle) ensureDownloaded() error {
	fh.dlOnce.Do(func() {
		if fh.downloaded {
			return
		}
		fh.File.Close()
		fh.dlErr = fh.f.cacheSave(fh.dlCtx, fh.cachePath)
		if fh.dlErr != nil {
			return
		}
		f, err := os.OpenFile(fh.cachePath, os.O_RDWR, fh.f.mfs.config.Mode)
		if err != nil {
			fh.dlErr = err
			return
		}
		fh.File = f
		fh.downloaded = true
	})
	return fh.dlErr
}

// Read reads data from the file handle.
func (fh *S3FileHandle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	metricsIncRead()

	if !fh.downloaded {
		fh.dlCtx = ctx
		if err := fh.ensureDownloaded(); err != nil {
			return nil, syscall.EIO
		}
	}

	start := time.Now()
	bufp := readBufPool.Get().(*[]byte)
	defer readBufPool.Put(bufp)
	buf := *bufp
	if len(dest) > len(buf) {
		buf = make([]byte, len(dest))
	}
	n, err := fh.File.ReadAt(buf[:len(dest)], off)
	metricsObserveRead(time.Since(start).Seconds())
	if err != nil && err != io.EOF {
		if err == io.ErrUnexpectedEOF {
			return fuse.ReadResultData(buf[:n]), 0
		}
		return nil, syscall.EIO
	}
	return fuse.ReadResultData(buf[:n]), 0
}

// Write writes data to the file handle.
func (fh *S3FileHandle) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	if fh.f.mfs.config.ReadOnly {
		return 0, syscall.EPERM
	}
	metricsIncWrite()

	// Ensure file is downloaded before writing (partial overwrite needs existing data).
	if !fh.downloaded {
		fh.dlCtx = ctx
		if err := fh.ensureDownloaded(); err != nil {
			return 0, syscall.EIO
		}
	}

	start := time.Now()
	if _, err := fh.File.Seek(off, 0); err != nil {
		return 0, syscall.EIO
	}
	n, err := fh.File.Write(data)
	metricsObserveWrite(time.Since(start).Seconds())
	if err != nil {
		return 0, syscall.EIO
	}

	if fh.f.Size < uint64(off)+uint64(n) {
		fh.f.Size = uint64(off) + uint64(n)
	}
	fh.dirty = true
	return uint32(n), 0
}

// Flush uploads the modified cache file to S3.
func (fh *S3FileHandle) Flush(ctx context.Context) syscall.Errno {
	if !fh.dirty {
		return 0
	}
	metricsIncFlush()

	remotePath := fh.f.RemotePath()

	// Record pending upload for crash recovery.
	fh.f.mfs.cache.RecordPending(&PendingUpload{
		CachePath:  fh.cachePath,
		RemotePath: remotePath,
		Size:       int64(fh.f.Size),
	})

	sr := newPutOp(fh.cachePath, remotePath, int64(fh.f.Size))
	if err := fh.f.mfs.sync(sr); err != nil {
		fh.flushed = false
		return syscall.EIO
	}
	if err := <-sr.Error; err != nil {
		fh.flushed = false
		return syscall.EIO
	}

	fh.f.mfs.cache.ClearPending(fh.cachePath)

	// Update cache.
	_ = fh.f.mfs.cache.PutInode(fh.f.toCacheInode())

	fh.dirty = false
	fh.flushed = true
	return 0
}

// Release closes the file handle and cleans up cache.
func (fh *S3FileHandle) Release(ctx context.Context) syscall.Errno {
	metricsIncRelease()
	fh.Close()
	fh.f.mfs.Release(fh)

	// Clear the file's lastFH reference if it points to this handle,
	// so truncateCacheFile doesn't use a stale/closed file descriptor.
	fh.f.mfs.mu.Lock()
	if fh.f.lastFH == fh {
		fh.f.lastFH = nil
	}
	fh.f.mfs.mu.Unlock()

	if fh.dirty && !fh.flushed {
		// Flush failed — keep cache file for crash recovery.
		return syscall.EIO
	}

	fh.f.mfs.cache.ClearPending(fh.cachePath)
	os.Remove(fh.cachePath)
	return 0
}

// Fsync flushes the local cache file to disk.
func (fh *S3FileHandle) Fsync(ctx context.Context, flags uint32) syscall.Errno {
	if fh.File != nil {
		if err := fh.File.Sync(); err != nil {
			return syscall.EIO
		}
	}
	return 0
}

func minioPutOptions() minio.PutObjectOptions {
	return minio.PutObjectOptions{}
}

func minioRemoveOptions() minio.RemoveObjectOptions {
	return minio.RemoveObjectOptions{}
}

// Ensure S3FileHandle implements the required interfaces at compile time.
var _ = fmt.Sprintf // ensure fmt is used
