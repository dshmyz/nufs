package s3fs

import (
	"context"
	"io"
	"os"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/minio/minio-go/v7"
)

// S3File implements file operations for S3-backed filesystem.
type S3File struct {
	fs.Inode

	mfs     *S3FileSystem
	dir     *S3Dir
	Path    string
	InodeID uint64
	Size    uint64
	Mode    os.FileMode
	UID     uint32
	GID     uint32
	ETag    string
	Mtime   time.Time
	Chgtime time.Time
	Crtime  time.Time
	Atime   time.Time

	// lastFH is the most recently opened file handle for this inode.
	// truncateCacheFile uses it for O(1) lookup instead of scanning all
	// open handles. Set by Open, nil after Release closes the file.
	lastFH *S3FileHandle
}

var (
	_ fs.NodeOpener    = (*S3File)(nil)
	_ fs.NodeGetattrer = (*S3File)(nil)
	_ fs.NodeSetattrer = (*S3File)(nil)
	_ fs.NodeAccesser  = (*S3File)(nil)
)

func (f *S3File) FullPath() string {
	return f.dir.FullPath() + "/" + f.Path
}

func (f *S3File) RemotePath() string {
	return f.dir.RemotePath() + "/" + f.Path
}

func (f *S3File) toCacheInode() *CacheInode {
	return &CacheInode{
		ID:    f.InodeID,
		IsDir: false,
		Name:  f.Path,
		Size:  f.Size,
		Mode:  uint32(f.Mode),
		UID:   f.UID,
		GID:   f.GID,
		Mtime: f.Mtime.UnixNano(),
		Ctime: f.Chgtime.UnixNano(),
		Atime: f.Atime.UnixNano(),
		ETag:  f.ETag,
	}
}

// Open opens the file. Returns a FileHandle for read/write.
func (f *S3File) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	metricsIncOpen()

	if err := f.mfs.Wait(f.FullPath()); err != nil {
		return nil, 0, syscall.EPERM
	}

	fh, err := f.mfs.Acquire(f)
	if err != nil {
		return nil, 0, syscall.EPERM
	}

	fh.cachePath, err = f.mfs.NewCachePath()
	if err != nil {
		return nil, 0, syscall.EIO
	}

	truncate := flags&uint32(os.O_TRUNC) != 0
	writeOnly := flags&uint32(os.O_WRONLY|os.O_RDWR) == uint32(os.O_WRONLY)
	skipDownload := truncate || writeOnly

	if skipDownload {
		// Download immediately for truncate.
		if dlErr := f.cacheSave(ctx, fh.cachePath); dlErr != nil {
			return nil, 0, syscall.EIO
		}
		fh.downloaded = true
	}
	// For read opens, download is deferred to Read (lazy).

	fh.File, err = os.OpenFile(fh.cachePath, int(flags)|os.O_CREATE, f.mfs.config.Mode)
	if err != nil {
		return nil, 0, syscall.EIO
	}

	f.lastFH = fh
	return fh, fuse.FOPEN_KEEP_CACHE, 0
}

// cacheSave downloads the S3 object to a local cache file.
func (f *S3File) cacheSave(ctx context.Context, cachePath string) error {
	file, err := os.Create(cachePath)
	if err != nil {
		return err
	}
	defer file.Close()

	err = f.mfs.breaker.Execute(func() error {
		return retryWithBackoff(func() error {
			start := time.Now()
			object, getErr := f.mfs.api.GetObject(ctx, f.mfs.config.Bucket, f.RemotePath(), minio.GetObjectOptions{})
			metricsObserveS3Get(time.Since(start).Seconds())
			if getErr != nil {
				return getErr
			}
			defer object.Close()
			n, copyErr := io.Copy(file, object)
			if copyErr != nil {
				return copyErr
			}
			f.Size = uint64(n)
			return nil
		})
	})
	return err
}

// Getattr returns file attributes.
func (f *S3File) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Attr = fuse.Attr{
		Ino:   f.InodeID,
		Size:  f.Size,
		Mode:  uint32(f.Mode),
		Owner: fuse.Owner{Uid: f.UID, Gid: f.GID},
		Mtime: uint64(f.Mtime.Unix()),
		Ctime: uint64(f.Chgtime.Unix()),
		Nlink: 1,
	}
	return 0
}

// Setattr updates file attributes.
func (f *S3File) Setattr(ctx context.Context, fh fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	if mode, ok := in.GetMode(); ok {
		f.Mode = os.FileMode(mode)
	}
	if uid, ok := in.GetUID(); ok {
		f.UID = uid
	}
	if gid, ok := in.GetGID(); ok {
		f.GID = gid
	}
	if size, ok := in.GetSize(); ok {
		f.Size = size
		// Truncate the local cache file if it's open, so Flush uploads
		// the correct (truncated) content. Without this, a truncate
		// followed by Flush would upload the full old file.
		f.truncateCacheFile(size)
	}
	if mt, ok := in.GetMTime(); ok {
		f.Mtime = mt
	}
	f.Chgtime = time.Now()

	_ = f.mfs.cache.PutInode(f.toCacheInode())

	out.Attr = fuse.Attr{
		Ino:   f.InodeID,
		Size:  f.Size,
		Mode:  uint32(f.Mode),
		Owner: fuse.Owner{Uid: f.UID, Gid: f.GID},
		Mtime: uint64(f.Mtime.Unix()),
		Ctime: uint64(f.Chgtime.Unix()),
		Nlink: 1,
	}
	return 0
}

// truncateCacheFile finds the open handle for this inode and truncates
// its local cache file to the given size. This ensures that a subsequent
// Flush uploads the truncated content to S3.
func (f *S3File) truncateCacheFile(size uint64) {
	f.mfs.mu.RLock()
	h := f.lastFH
	f.mfs.mu.RUnlock()
	if h != nil && h.File != nil {
		h.File.Truncate(int64(size))
	}
}

// Access always returns 0 (allow-all).
func (f *S3File) Access(ctx context.Context, mask uint32) syscall.Errno {
	return 0
}
