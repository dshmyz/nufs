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
	"os"
	"path"
	"strings"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"

	"github.com/minio/minfs/meta"
	minio "github.com/minio/minio-go/v7"
)

// Dir implements both Node and Handle for the root directory.
type Dir struct {
	mfs *MinFS

	dir *Dir

	Path  string
	Inode uint64
	Mode  os.FileMode

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

	scanned  bool
	lastScan time.Time
}

func (dir *Dir) needsScan() bool {
	if !dir.scanned {
		return true
	}
	// Re-scan if the cached result has exceeded the TTL.
	ttl := dir.mfs.config.scanTTL
	if ttl <= 0 {
		return false // TTL disabled, never re-scan
	}
	return time.Since(dir.lastScan) > ttl
}

// Attr returns the attributes for the directory
func (dir *Dir) Attr(ctx context.Context, a *fuse.Attr) error {
	*a = fuse.Attr{
		Inode:  dir.Inode,
		Size:   dir.Size,
		Atime:  dir.Atime,
		Mtime:  dir.Mtime,
		Ctime:  dir.Chgtime,
		Crtime: dir.Crtime,
		Mode:   dir.Mode,
		Uid:    dir.UID,
		Gid:    dir.GID,
		Flags:  dir.Flags,
	}

	return nil
}

// Setattr updates directory attributes (chmod, chown, utimes).
func (dir *Dir) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	return dir.mfs.db.Update(func(tx *meta.Tx) error {
		if req.Valid.Mode() {
			dir.Mode = req.Mode | os.ModeDir
		}
		if req.Valid.Uid() {
			dir.UID = req.Uid
		}
		if req.Valid.Gid() {
			dir.GID = req.Gid
		}
		if req.Valid.Atime() {
			dir.Atime = req.Atime
		}
		if req.Valid.Mtime() {
			dir.Mtime = req.Mtime
		}
		if req.Valid.Crtime() {
			dir.Crtime = req.Crtime
		}
		if req.Valid.Chgtime() {
			dir.Chgtime = req.Chgtime
		}
		if req.Valid.Bkuptime() {
			dir.Bkuptime = req.Bkuptime
		}
		if req.Valid.Flags() {
			dir.Flags = req.Flags
		}
		return dir.store(tx)
	})
}

// Lookup returns the file node, and scans the current dir if necessary
func (dir *Dir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	metricsIncLookup()
	if err := dir.scan(ctx); err != nil {
		return nil, err
	}

	// we are not statting each object here because of performance reasons
	var o interface{} // meta.Object
	if err := dir.mfs.db.View(func(tx *meta.Tx) error {
		b := dir.bucket(tx)
		return b.Get(name, &o)
	}); err == nil {
	} else if meta.IsNoSuchObject(err) {
		return nil, fuse.ENOENT
	} else if err != nil {
		return nil, err
	}

	if file, ok := o.(File); ok {
		file.mfs = dir.mfs
		file.dir = dir
		return &file, nil
	} else if subdir, ok := o.(Dir); ok {
		subdir.mfs = dir.mfs
		subdir.dir = dir
		return &subdir, nil
	} else if sym, ok := o.(Symlink); ok {
		sym.mfs = dir.mfs
		sym.dir = dir
		return &sym, nil
	}

	return nil, fuse.ENOENT
}

// RemotePath returns the full path including parent paths for current dir on the remote
func (dir *Dir) RemotePath() string {
	return path.Join(dir.mfs.config.basePath, dir.FullPath())
}

// FullPath returns the full path including parent paths for current dir
func (dir *Dir) FullPath() string {
	fullPath := ""

	p := dir
	for {
		if p == nil {
			break
		}

		fullPath = path.Join(p.Path, fullPath)

		p = p.dir
	}

	return fullPath
}

func (dir *Dir) storeFile(bucket *meta.Bucket, tx *meta.Tx, baseKey string, objInfo minio.ObjectInfo) error {
	var f File
	err := bucket.Get(baseKey, &f)
	if err == nil {
		// Object already exists and accessible, update values as needed.
		f.dir = dir
		f.mfs = dir.mfs
		f.Size = uint64(objInfo.Size)
		f.ETag = objInfo.ETag
		if objInfo.LastModified.After(f.Chgtime) {
			f.Chgtime = objInfo.LastModified
		}
		if objInfo.LastModified.After(f.Crtime) {
			f.Crtime = objInfo.LastModified
		}
		if objInfo.LastModified.After(f.Mtime) {
			f.Mtime = objInfo.LastModified
		}
		if objInfo.LastModified.After(f.Atime) {
			f.Atime = objInfo.LastModified
		}
	} else if meta.IsNoSuchObject(err) {
		// Object not found, allocate a new inode.
		var seq uint64
		seq, err = dir.mfs.NextSequence(tx)
		if err != nil {
			return err
		}
		f = File{
			dir:     dir,
			Path:    baseKey,
			Size:    uint64(objInfo.Size),
			Inode:   seq,
			Mode:    dir.mfs.config.mode,
			GID:     dir.mfs.config.gid,
			UID:     dir.mfs.config.uid,
			Chgtime: objInfo.LastModified,
			Crtime:  objInfo.LastModified,
			Mtime:   objInfo.LastModified,
			Atime:   objInfo.LastModified,
			ETag:    objInfo.ETag,
		}
		if err = f.store(tx); err != nil {
			return err
		}
	} else {
		// Returns failure for all other errors.
		return err
	}
	return nil
}

func (dir *Dir) storeDir(bucket *meta.Bucket, tx *meta.Tx, baseKey string, objInfo minio.ObjectInfo) error {
	var d Dir
	err := bucket.Get(baseKey, &d)
	if err == nil {
		// Prefix already exists and accessible, update values as needed.
		d.dir = dir
		d.mfs = dir.mfs
	} else if meta.IsNoSuchObject(err) {
		// Prefix not found allocate a new inode and create a new directory.
		var seq uint64
		seq, err = dir.mfs.NextSequence(tx)
		if err != nil {
			return err
		}
		d = Dir{
			dir:   dir,
			Path:  baseKey,
			Inode: seq,
			Mode:  0770 | os.ModeDir,
			GID:   dir.mfs.config.gid,
			UID:   dir.mfs.config.uid,

			Chgtime: objInfo.LastModified,
			Crtime:  objInfo.LastModified,
			Mtime:   objInfo.LastModified,
			Atime:   objInfo.LastModified,
		}
		if err = d.store(tx); err != nil {
			return err
		}
	} else {
		// For all other errors this operation fails.
		return err
	}
	return nil
}

func (dir *Dir) scan(ctx context.Context) error {
	if !dir.needsScan() {
		return nil
	}

	tx, err := dir.mfs.db.Begin(true)
	if err != nil {
		return err
	}

	defer tx.Rollback()

	b := dir.bucket(tx)

	objects := map[string]interface{}{}

	// we'll compare the current bucket contents against our cache folder, and update the cache
	if err := b.ForEach(func(k string, o interface{}) error {
		if k[len(k)-1] != '/' {
			objects[k] = &o
		}
		return nil
	}); err != nil {
		return err
	}

	prefix := dir.RemotePath()
	if prefix != "" {
		prefix = prefix + "/"
	}

	if dir.mfs.breaker.IsOpen() {
		return errCircuitOpen
	}

	ch := dir.mfs.api.ListObjects(ctx, dir.mfs.config.bucket, minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: false,
	})
	metricsIncS3List()

	listHadError := false
	for objInfo := range ch {
		if objInfo.Err != nil {
			dir.mfs.log.Printf("WARNING: scan %s: listing error: %s\n", dir.RemotePath(), objInfo.Err)
			listHadError = true
			continue
		}

		key := objInfo.Key[len(prefix):]
		baseKey := path.Base(key)

		// object still exists
		objects[baseKey] = nil

		if strings.HasSuffix(key, "/") {
			if err := dir.storeDir(b, tx, baseKey, objInfo); err != nil {
				dir.mfs.log.Printf("WARNING: scan %s: storeDir(%s) failed: %s\n", dir.RemotePath(), baseKey, err)
			}
		} else {
			if err := dir.storeFile(b, tx, baseKey, objInfo); err != nil {
				dir.mfs.log.Printf("WARNING: scan %s: storeFile(%s) failed: %s\n", dir.RemotePath(), baseKey, err)
			}
		}
	}

	// cache housekeeping
	for k, o := range objects {
		if o == nil {
			continue
		}

		// Preserve symlinks — they are local-only, not in S3.
		if _, isSymlink := o.(Symlink); isSymlink {
			continue
		}

		// purge from cache
		b.Delete(k)

		if _, ok := o.(Dir); !ok {
			continue
		}

		b.DeleteBucket(k + "/")
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	// Feed back listing result to the circuit breaker.
	if listHadError {
		dir.mfs.breaker.Execute(func() error { return errCircuitOpen })
	} else {
		dir.mfs.breaker.Execute(func() error { return nil })
	}

	dir.scanned = true
	dir.lastScan = time.Now()
	return nil
}

// ReadDirAll will return all files in current dir
func (dir *Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	metricsIncReadDir()
	if err := dir.scan(ctx); err != nil {
		return nil, err
	}

	var entries = []fuse.Dirent{}

	// update cache folder with bucket list
	if err := dir.mfs.db.View(func(tx *meta.Tx) error {
		return dir.bucket(tx).ForEach(func(k string, o interface{}) error {
			if file, ok := o.(File); ok {
				file.dir = dir
				entries = append(entries, file.Dirent())
			} else if subdir, ok := o.(Dir); ok {
				subdir.dir = dir
				entries = append(entries, subdir.Dirent())
			} else if sym, ok := o.(Symlink); ok {
				sym.dir = dir
				entries = append(entries, sym.Dirent())
			} else {
				return fmt.Errorf("unknown cache entry type for key %q: %T (try removing cache)", k, o)
			}

			return nil
		})
	}); err != nil {
		return nil, err
	}

	return entries, nil
}

func (dir *Dir) bucket(tx *meta.Tx) *meta.Bucket {
	// Root folder.
	if dir.dir == nil {
		return tx.Bucket("minio/")
	}

	b := dir.dir.bucket(tx)
	if b == nil {
		return nil
	}

	return b.Bucket(dir.Path + "/")
}

// Mkdir will make a new directory below current dir
func (dir *Dir) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
	if dir.mfs.config.readOnly {
		return nil, fuse.EPERM
	}
	metricsIncMkdir()
	subdir := Dir{
		dir: dir,
		mfs: dir.mfs,

		Path: req.Name,

		Mode: 0770 | os.ModeDir,
		GID:  dir.mfs.config.gid,
		UID:  dir.mfs.config.uid,

		Chgtime: time.Now(),
		Crtime:  time.Now(),
		Mtime:   time.Now(),
		Atime:   time.Now(),
	}

	tx, err := dir.mfs.db.Begin(true)
	if err != nil {
		return nil, err
	}

	defer tx.Rollback()

	if err := subdir.store(tx); err != nil {
		return nil, err
	}

	// Commit the transaction and check for error.
	if err := tx.Commit(); err != nil {
		dir.mfs.audit("mkdir", path.Join(dir.FullPath(), req.Name), "error")
		return nil, err
	}

	dir.mfs.audit("mkdir", path.Join(dir.FullPath(), req.Name), "ok")
	return &subdir, nil
}

// Remove will delete a file or directory from current directory
func (dir *Dir) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	if dir.mfs.config.readOnly {
		return fuse.EPERM
	}
	metricsIncRemove()
	if err := dir.mfs.wait(path.Join(dir.FullPath(), req.Name)); err != nil {
		return err
	}

	tx, err := dir.mfs.db.Begin(true)
	if err != nil {
		return err
	}

	defer tx.Rollback()

	b := dir.bucket(tx)

	var o interface{}
	if err := b.Get(req.Name, &o); meta.IsNoSuchObject(err) {
		return fuse.ENOENT
	} else if err != nil {
		return err
	} else if err := b.Delete(req.Name); err != nil {
		return err
	}

	if req.Dir {
		b.DeleteBucket(req.Name + "/")
	}

	// Symlinks are local-only — no S3 object to remove.
	if _, isSymlink := o.(Symlink); isSymlink {
		if err := tx.Commit(); err != nil {
			dir.mfs.audit("remove", path.Join(dir.FullPath(), req.Name), "error")
			return err
		}
		dir.mfs.audit("remove", path.Join(dir.FullPath(), req.Name)+" (symlink)", "ok")
		return nil
	}

	if err := dir.mfs.breaker.Execute(func() error {
		return retryWithBackoff(func() error {
			metricsIncS3Remove()
			return dir.mfs.api.RemoveObject(ctx, dir.mfs.config.bucket, path.Join(dir.RemotePath(), req.Name), minio.RemoveObjectOptions{})
		})
	}); err != nil {
		metricsIncS3Error()
		dir.mfs.audit("remove", path.Join(dir.FullPath(), req.Name), "error")
		return err
	}

	if err := tx.Commit(); err != nil {
		dir.mfs.audit("remove", path.Join(dir.FullPath(), req.Name), "error")
		return err
	}
	dir.mfs.audit("remove", path.Join(dir.FullPath(), req.Name), "ok")
	return nil
}

// store the dir object in cache
func (dir *Dir) store(tx *meta.Tx) error {
	// directories will be stored in their parent buckets
	b := dir.dir.bucket(tx)

	subbucketPath := path.Base(dir.Path)
	if _, err := b.CreateBucketIfNotExists(subbucketPath + "/"); err != nil {
		return err
	}

	return b.Put(subbucketPath, dir)
}

// Dirent will return the fuse Dirent for current dir
func (dir *Dir) Dirent() fuse.Dirent {
	return fuse.Dirent{
		Inode: dir.Inode, Name: dir.Path, Type: fuse.DT_Dir,
	}
}

// Create will return a new empty file in current dir, if the file is currently locked, it will
// wait for the lock to be freed.
func (dir *Dir) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	if dir.mfs.config.readOnly {
		return nil, nil, fuse.EPERM
	}
	metricsIncCreate()
	if err := dir.mfs.wait(path.Join(dir.FullPath(), req.Name)); err != nil {
		return nil, nil, err
	}

	tx, err := dir.mfs.db.Begin(true)
	if err != nil {
		return nil, nil, err
	}

	defer tx.Rollback()

	b := dir.bucket(tx)

	name := req.Name

	var f File
	if gerr := b.Get(name, &f); gerr == nil {
		f.mfs = dir.mfs
		f.dir = dir
	} else if i, nerr := dir.mfs.NextSequence(tx); nerr != nil {
		return nil, nil, nerr
	} else {
		f = File{
			mfs: dir.mfs,
			dir: dir,

			Size:    uint64(0),
			Inode:   i,
			Path:    req.Name,
			Mode:    req.Mode, // dir.mfs.config.mode, // should we use same mode for scan?
			UID:     dir.mfs.config.uid,
			GID:     dir.mfs.config.gid,
			Chgtime: time.Now().UTC(),
			Crtime:  time.Now().UTC(),
			Mtime:   time.Now().UTC(),
			Atime:   time.Now().UTC(),
			ETag:    "",

			// req.Umask
		}
	}

	if serr := f.store(tx); serr != nil {
		return nil, nil, serr
	}

	var fh *FileHandle
	if fh, err = dir.mfs.Acquire(&f); err != nil {
		return nil, nil, err
	}
	fh.dirty = true
	if fh.cachePath, err = dir.mfs.NewCachePath(); err != nil {
		return nil, nil, err
	}
	if fh.File, err = os.OpenFile(fh.cachePath, int(req.Flags), dir.mfs.config.mode); err != nil {
		return nil, nil, err
	}

	// Commit the transaction and check for error.
	if err = tx.Commit(); err != nil {
		dir.mfs.audit("create", path.Join(dir.FullPath(), req.Name), "error")
		return nil, nil, err
	}

	dir.mfs.audit("create", path.Join(dir.FullPath(), req.Name), "ok")
	resp.Handle = fuse.HandleID(fh.handle)
	return &f, fh, nil
}

// Rename will rename files
func (dir *Dir) Rename(ctx context.Context, req *fuse.RenameRequest, nd fs.Node) error {
	if dir.mfs.config.readOnly {
		return fuse.EPERM
	}
	metricsIncRename()
	tx, err := dir.mfs.db.Begin(true)
	if err != nil {
		return err
	}

	defer tx.Rollback()

	b := dir.bucket(tx)

	newDir := nd.(*Dir)

	var o interface{}
	if err := b.Get(req.OldName, &o); err != nil {
		return err
	} else if file, ok := o.(File); ok {
		file.dir = dir

		if err := b.Delete(file.Path); err != nil {
			return err
		}

		oldPath := file.RemotePath()

		file.Path = req.NewName
		file.dir = newDir
		file.mfs = dir.mfs

		sr := newMoveOp(oldPath, file.RemotePath())
		if err := dir.mfs.sync(&sr); err == nil {
		} else if meta.IsNoSuchObject(err) {
			return fuse.ENOENT
		} else if err != nil {
			return err
		}

		// we'll wait for the request to be uploaded and synced, before
		// releasing the file
		if err := <-sr.Error; err != nil {
			return err
		}

		if err := file.store(tx); err != nil {
			return err
		}

	} else if subdir, ok := o.(Dir); ok {
		// rescan in case of abort / partial / failure
		// this will repair the cache
		dir.scanned = false

		if err := b.Delete(req.OldName); err != nil {
			return err
		}

		if err := b.DeleteBucket(req.OldName + "/"); err != nil {
			return err
		}

		newDir.scanned = false

		// fusebug?
		// the cached node is still invalid, contains the old name
		// but there is no way to retrieve the old node to update the new
		// name. refreshing the parent node won't fix the issue when
		// direct access. Fuse should add the targetnode (subdir) as well,
		// that can be updated.

		subdir.Path = req.NewName
		subdir.dir = newDir
		subdir.mfs = dir.mfs

		if err := subdir.store(tx); err != nil {
			return err
		}

		oldPath := path.Join(dir.RemotePath(), req.OldName)

		ch := dir.mfs.api.ListObjects(ctx, dir.mfs.config.bucket, minio.ListObjectsOptions{
			Prefix:    oldPath + "/",
			Recursive: true,
		})

		for message := range ch {
			newPath := path.Join(newDir.RemotePath(), req.NewName, message.Key[len(oldPath):])

			sr := newMoveOp(message.Key, newPath)
			if err := dir.mfs.sync(&sr); err == nil {
			} else if meta.IsNoSuchObject(err) {
				return fuse.ENOENT
			} else if err != nil {
				return err
			}

			// we'll wait for the request to be uploaded and synced, before
			// releasing the file
			if err := <-sr.Error; err != nil {
				return err
			}
		}
	} else if sym, ok := o.(Symlink); ok {
		// Symlinks are local-only — just update the DB entry.
		if err := b.Delete(req.OldName); err != nil {
			return err
		}
		sym.Path = req.NewName
		sym.dir = newDir
		sym.mfs = dir.mfs
		if err := sym.store(tx); err != nil {
			return err
		}
	} else {
		return fuse.ENOSYS
	}

	// Commit the transaction and check for error.
	return tx.Commit()
}

// Symlink creates a new symbolic link in this directory.
func (dir *Dir) Symlink(ctx context.Context, req *fuse.SymlinkRequest) (fs.Node, error) {
	if dir.mfs.config.readOnly {
		return nil, fuse.EPERM
	}

	tx, err := dir.mfs.db.Begin(true)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	seq, err := dir.mfs.NextSequence(tx)
	if err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	sym := Symlink{
		mfs:     dir.mfs,
		dir:     dir,
		Path:    req.NewName,
		Target:  req.Target,
		Inode:   seq,
		Mode:    0777, // symlinks are always accessible
		UID:     dir.mfs.config.uid,
		GID:     dir.mfs.config.gid,
		Atime:   now,
		Mtime:   now,
		Chgtime: now,
		Crtime:  now,
	}

	if err := sym.store(tx); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		dir.mfs.audit("symlink", path.Join(dir.FullPath(), req.NewName), "error")
		return nil, err
	}

	dir.mfs.audit("symlink", path.Join(dir.FullPath(), req.NewName)+" -> "+req.Target, "ok")
	return &sym, nil
}

// Link creates a hardlink to an existing file in this directory.
// Since S3 doesn't support hardlinks natively, this copies the object.
func (dir *Dir) Link(ctx context.Context, req *fuse.LinkRequest, old fs.Node) (fs.Node, error) {
	if dir.mfs.config.readOnly {
		return nil, fuse.EPERM
	}

	oldFile, ok := old.(*File)
	if !ok {
		return nil, fuse.EPERM // can only hardlink regular files
	}

	tx, err := dir.mfs.db.Begin(true)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	seq, err := dir.mfs.NextSequence(tx)
	if err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	newFile := File{
		mfs:     dir.mfs,
		dir:     dir,
		Path:    req.NewName,
		Inode:   seq,
		Size:    oldFile.Size,
		Mode:    oldFile.Mode,
		UID:     oldFile.UID,
		GID:     oldFile.GID,
		ETag:    oldFile.ETag,
		Atime:   now,
		Mtime:   now,
		Chgtime: now,
		Crtime:  now,
	}

	// Copy the S3 object to the new path.
	sr := newCopyOp(oldFile.RemotePath(), newFile.RemotePath())
	if err := dir.mfs.sync(&sr); err != nil {
		return nil, err
	}
	if err := <-sr.Error; err != nil {
		return nil, err
	}

	if err := newFile.store(tx); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		dir.mfs.audit("link", path.Join(dir.FullPath(), req.NewName), "error")
		return nil, err
	}

	dir.mfs.audit("link", path.Join(dir.FullPath(), req.NewName)+" -> "+oldFile.RemotePath(), "ok")
	return &newFile, nil
}

// Access checks permission for the directory node.
func (dir *Dir) Access(ctx context.Context, req *fuse.AccessRequest) error {
	return nil // Handled by kernel via DefaultPermissions.
}

func newCopyOp(sourcePath, targetPath string) *CopyOperation {
	return &CopyOperation{
		Source: sourcePath,
		Target: targetPath,
		Operation: &Operation{
			Error: make(chan error),
		},
	}
}
