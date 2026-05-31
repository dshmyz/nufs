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
	"bytes"
	"context"
	"crypto/sha256"
	"io"
	"os"
	"path"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/minio/minfs/meta"
	minio "github.com/minio/minio-go/v7"
)

// File implements both Node and Handle for the hello file.
type File struct {
	mfs *MinFS

	dir *Dir

	Path string

	Inode uint64

	Mode os.FileMode

	Size uint64
	ETag string

	Atime time.Time
	Mtime time.Time

	UID uint32
	GID uint32

	// OS X only
	Bkuptime time.Time
	Chgtime  time.Time
	Crtime   time.Time
	Flags    uint32 // see chflags(2)

	Hash []byte
}

func (f *File) store(tx *meta.Tx) error {
	b := f.bucket(tx)
	return b.Put(path.Base(f.Path), f)
}

// Attr - attr file context.
func (f *File) Attr(ctx context.Context, a *fuse.Attr) error {
	*a = fuse.Attr{
		Inode:  f.Inode,
		Size:   f.Size,
		Atime:  f.Atime,
		Mtime:  f.Mtime,
		Ctime:  f.Chgtime,
		Crtime: f.Crtime,
		Mode:   f.Mode,
		Uid:    f.UID,
		Gid:    f.GID,
		Flags:  f.Flags,
	}

	return nil
}

// Setattr - set attribute.
func (f *File) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	// Handle truncate: must happen outside the DB transaction.
	if req.Valid.Size() {
		newSize := req.Size
		if f.mfs.config.readOnly {
			return fuse.EPERM
		}

		// Find an open handle for this file.
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
			// File is open — truncate the cache file.
			if err := fh.File.Truncate(int64(newSize)); err != nil {
				return err
			}
		} else if newSize == 0 {
			// File is closed, truncate to 0 — upload empty object to S3.
			if err := f.mfs.breaker.Execute(func() error {
				return retryWithBackoff(func() error {
					metricsIncS3Put()
					start := time.Now()
					_, err := f.mfs.api.PutObject(ctx, f.mfs.config.bucket, f.RemotePath(),
						bytes.NewReader(nil), 0, minio.PutObjectOptions{})
					metricsObserveS3Put(time.Since(start).Seconds())
					return err
				})
			}); err != nil {
				return err
			}
		}
		// For closed files with newSize > 0, the actual truncation
		// requires a download-modify-upload cycle which is too expensive.
		// The metadata update below is sufficient; the next Open+Write
		// will produce the correct result.
	}

	// Update metadata in DB.
	return f.mfs.db.Update(func(tx *meta.Tx) error {
		if req.Valid.Mode() {
			f.Mode = req.Mode
		}

		if req.Valid.Uid() {
			f.UID = req.Uid
		}

		if req.Valid.Gid() {
			f.GID = req.Gid
		}

		if req.Valid.Size() {
			f.Size = req.Size
		}

		if req.Valid.Atime() {
			f.Atime = req.Atime
		}

		if req.Valid.Mtime() {
			f.Mtime = req.Mtime
		}

		if req.Valid.Crtime() {
			f.Crtime = req.Crtime
		}

		if req.Valid.Chgtime() {
			f.Chgtime = req.Chgtime
		}

		if req.Valid.Bkuptime() {
			f.Bkuptime = req.Bkuptime
		}

		if req.Valid.Flags() {
			f.Flags = req.Flags
		}

		return f.store(tx)
	})
}

// RemotePath will return the full path on bucket
func (f *File) RemotePath() string {
	return path.Join(f.dir.RemotePath(), f.Path)
}

// FullPath will return the full path
func (f *File) FullPath() string {
	return path.Join(f.dir.FullPath(), f.Path)
}

// Saves a new file at cached path and fetches the object based on
// the incoming fuse request.
func (f *File) cacheSave(ctx context.Context, path string, req *fuse.OpenRequest) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	if req.Flags&fuse.OpenTruncate == fuse.OpenTruncate {
		f.Size = 0
		return nil
	}

	err = f.mfs.breaker.Execute(func() error {
		return retryWithBackoff(func() error {
			// Reset file position for retry.
			if _, seekErr := file.Seek(0, 0); seekErr != nil {
				return seekErr
			}
			if seekErr := file.Truncate(0); seekErr != nil {
				return seekErr
			}

			metricsIncS3Get()
			start := time.Now()
			object, getErr := f.mfs.api.GetObject(ctx, f.mfs.config.bucket, f.RemotePath(), minio.GetObjectOptions{})
			metricsObserveS3Get(time.Since(start).Seconds())
			if getErr != nil {
				return getErr
			}
			defer object.Close()

			hasher := sha256.New()
			size, copyErr := io.Copy(file, io.TeeReader(object, hasher))
			if copyErr != nil {
				return copyErr
			}

			// update actual file size
			f.Size = uint64(size)
			_ = hasher.Sum(nil)
			return nil
		})
	})
	if err != nil {
		if meta.IsNoSuchObject(err) {
			return fuse.ENOENT
		}
		return err
	}
	return nil
}

// Open return a file handle of the opened file
func (f *File) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	metricsIncOpen()
	if err := f.dir.mfs.wait(f.Path); err != nil {
		return nil, err
	}

	// Start a writable transaction.
	tx, err := f.mfs.db.Begin(true)
	if err != nil {
		return nil, err
	}

	defer tx.Rollback()

	cachePath, err := f.dir.mfs.NewCachePath()
	if err != nil {
		return nil, err
	}

	// Create an empty cache file placeholder.
	// For truncate or write-only opens, we don't need to download.
	// For read opens, we use lazy download (deferred to first Read call).
	truncate := req.Flags&fuse.OpenTruncate == fuse.OpenTruncate
	writeOnly := req.Flags.IsWriteOnly()
	skipDownload := truncate || writeOnly

	if skipDownload {
		// Download immediately for truncate (cacheSave handles truncate logic).
		err = f.cacheSave(ctx, cachePath, req)
		if err != nil {
			return nil, err
		}
	}
	// For read opens, cacheSave is deferred to FileHandle.Read (lazy download).

	fh, err := f.mfs.Acquire(f)
	if err != nil {
		return nil, err
	}

	fh.cachePath = cachePath
	fh.dlCtx = ctx
	fh.downloaded = skipDownload

	openFlags := int(req.Flags)
	if skipDownload {
		fh.File, err = os.OpenFile(fh.cachePath, openFlags, f.mfs.config.mode)
	} else {
		// For lazy download: create empty placeholder, will be replaced on first Read.
		fh.File, err = os.OpenFile(fh.cachePath, os.O_CREATE|os.O_RDWR, f.mfs.config.mode)
	}
	if err != nil {
		return nil, err
	}

	if err = f.store(tx); err != nil {
		return nil, err
	}

	if err = tx.Commit(); err != nil {
		return nil, err
	}

	// Enforce cache disk quota after downloading a new file.
	if skipDownload {
		go f.mfs.evictCache()
	}

	resp.Handle = fuse.HandleID(fh.handle)
	return fh, nil
}

func (f *File) bucket(tx *meta.Tx) *meta.Bucket {
	b := f.dir.bucket(tx)
	return b
}

// Getattr returns the file attributes
func (f *File) Getattr(ctx context.Context, req *fuse.GetattrRequest, resp *fuse.GetattrResponse) error {
	resp.Attr = fuse.Attr{
		Inode:  f.Inode,
		Size:   f.Size,
		Atime:  f.Atime,
		Mtime:  f.Mtime,
		Ctime:  f.Chgtime,
		Crtime: f.Crtime,
		Mode:   f.Mode,
		Uid:    f.UID,
		Gid:    f.GID,
		Flags:  f.Flags,
	}

	return nil
}

// Dirent returns the File object as a fuse.Dirent
func (f *File) Dirent() fuse.Dirent {
	return fuse.Dirent{
		Inode: f.Inode, Name: f.Path, Type: fuse.DT_File,
	}
}

// Access checks permission for the file node.
func (f *File) Access(ctx context.Context, req *fuse.AccessRequest) error {
	return nil // Handled by kernel via DefaultPermissions.
}

func (f *File) delete(tx *meta.Tx) error {
	// purge from cache
	b := f.bucket(tx)
	return b.Delete(f.Path)
}
