package s3fs

import (
	"context"
	"os"
	"path"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/minio/minio-go/v7"
)

// S3Dir implements directory operations for S3-backed filesystem.
type S3Dir struct {
	fs.Inode

	mfs     *S3FileSystem
	dir     *S3Dir // parent directory (nil for root)
	Path    string // relative path within parent
	InodeID uint64
	Mode    os.FileMode
	Size    uint64
	UID     uint32
	GID     uint32
	Chgtime time.Time
	Crtime  time.Time
	Mtime   time.Time
	Atime   time.Time

	scanned  bool
	lastScan time.Time
}

var (
	_ fs.NodeOnAdder   = (*S3Dir)(nil)
	_ fs.NodeLookuper  = (*S3Dir)(nil)
	_ fs.NodeReaddirer = (*S3Dir)(nil)
	_ fs.NodeMkdirer   = (*S3Dir)(nil)
	_ fs.NodeCreater   = (*S3Dir)(nil)
	_ fs.NodeUnlinker  = (*S3Dir)(nil)
	_ fs.NodeRenamer   = (*S3Dir)(nil)
	_ fs.NodeRmdirer   = (*S3Dir)(nil)
	_ fs.NodeSymlinker = (*S3Dir)(nil)
	_ fs.NodeLinker    = (*S3Dir)(nil)
	_ fs.NodeStatfser  = (*S3Dir)(nil)
	_ fs.NodeGetattrer = (*S3Dir)(nil)
	_ fs.NodeSetattrer = (*S3Dir)(nil)
	_ fs.NodeAccesser  = (*S3Dir)(nil)
)

func (d *S3Dir) FullPath() string {
	if d.dir == nil {
		return ""
	}
	return path.Join(d.dir.FullPath(), d.Path)
}

func (d *S3Dir) RemotePath() string {
	return d.mfs.config.RemotePath(d.FullPath())
}

func (d *S3Dir) toCacheInode() *CacheInode {
	children := make(map[string]uint64)
	return &CacheInode{
		ID:       d.InodeID,
		IsDir:    true,
		Name:     d.Path,
		Size:     d.Size,
		Mode:     uint32(d.Mode),
		UID:      d.UID,
		GID:      d.GID,
		Mtime:    d.Mtime.UnixNano(),
		Ctime:    d.Chgtime.UnixNano(),
		Atime:    d.Atime.UnixNano(),
		Children: children,
	}
}

// OnAdd is called when the inode is initialized. No-op for lazy scan.
func (d *S3Dir) OnAdd(ctx context.Context) {}

// Lookup finds a child by name.
func (d *S3Dir) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	metricsIncLookup()

	if err := d.scan(ctx); err != nil {
		return nil, syscall.EIO
	}

	// Look up in local cache.
	childID, err := d.mfs.cache.GetDirEntry(d.InodeID, name)
	if err != nil {
		return nil, syscall.ENOENT
	}

	child, err := d.mfs.cache.GetInode(childID)
	if err != nil {
		return nil, syscall.ENOENT
	}

	out.Attr = cacheToAttr(child)

	if child.IsDir {
		subdir := &S3Dir{
			mfs:     d.mfs,
			dir:     d,
			Path:    name,
			InodeID: child.ID,
			Mode:    os.FileMode(child.Mode) | os.ModeDir,
			UID:     child.UID,
			GID:     child.GID,
			Chgtime: time.Unix(0, child.Ctime),
			Crtime:  time.Unix(0, child.Ctime),
			Mtime:   time.Unix(0, child.Mtime),
			Atime:   time.Unix(0, child.Atime),
		}
		node := d.NewPersistentInode(ctx, subdir, fs.StableAttr{Mode: fuse.S_IFDIR})
		out.NodeId = node.StableAttr().Ino
		return node, 0
	}

	// Check if it's a symlink.
	if child.SymlinkTarget != "" {
		sym := &S3Symlink{
			mfs:     d.mfs,
			dir:     d,
			Path:    name,
			Target:  child.SymlinkTarget,
			InodeID: child.ID,
			Mode:    os.FileMode(child.Mode),
			UID:     child.UID,
			GID:     child.GID,
		}
		node := d.NewPersistentInode(ctx, sym, fs.StableAttr{Mode: fuse.S_IFLNK})
		out.NodeId = node.StableAttr().Ino
		return node, 0
	}

	file := &S3File{
		mfs:     d.mfs,
		dir:     d,
		Path:    name,
		InodeID: child.ID,
		Size:    child.Size,
		Mode:    os.FileMode(child.Mode),
		UID:     child.UID,
		GID:     child.GID,
		ETag:    child.ETag,
		Mtime:   time.Unix(0, child.Mtime),
		Chgtime: time.Unix(0, child.Ctime),
		Crtime:  time.Unix(0, child.Ctime),
		Atime:   time.Unix(0, child.Atime),
	}
	node := d.NewPersistentInode(ctx, file, fs.StableAttr{})
	out.NodeId = node.StableAttr().Ino
	return node, 0
}

// Readdir lists directory contents.
func (d *S3Dir) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	metricsIncReadDir()

	if err := d.scan(ctx); err != nil {
		return nil, syscall.EIO
	}

	entries, err := d.mfs.cache.ListDirEntries(d.InodeID)
	if err != nil {
		return nil, syscall.EIO
	}

	dirEntries := make([]fuse.DirEntry, 0, len(entries))
	for name, childID := range entries {
		child, err := d.mfs.cache.GetInode(childID)
		if err != nil {
			continue
		}
		var typ uint32
		if child.IsDir {
			typ = fuse.S_IFDIR
		} else if child.SymlinkTarget != "" {
			typ = fuse.S_IFLNK
		} else {
			typ = fuse.S_IFREG
		}
		dirEntries = append(dirEntries, fuse.DirEntry{
			Mode: typ,
			Name: name,
		})
	}

	return fs.NewListDirStream(dirEntries), 0
}

// Mkdir creates a new directory.
func (d *S3Dir) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if d.mfs.config.ReadOnly {
		return nil, syscall.EPERM
	}
	metricsIncMkdir()

	childID := d.mfs.cache.NextID()
	now := time.Now()
	subdir := &S3Dir{
		mfs:     d.mfs,
		dir:     d,
		Path:    name,
		InodeID: childID,
		Mode:    os.FileMode(mode) | os.ModeDir,
		UID:     d.mfs.config.UID,
		GID:     d.mfs.config.GID,
		Chgtime: now,
		Crtime:  now,
		Mtime:   now,
		Atime:   now,
	}

	if err := d.mfs.cache.PutInode(subdir.toCacheInode()); err != nil {
		return nil, syscall.EIO
	}
	if err := d.mfs.cache.PutDirEntry(d.InodeID, name, childID); err != nil {
		return nil, syscall.EIO
	}

	// Create empty directory marker in S3.
	remotePath := path.Join(d.RemotePath(), name, "/")
	opts := minioPutOptions()
	_, err := d.mfs.api.PutObject(ctx, d.mfs.config.Bucket, remotePath, nil, 0, opts)
	if err != nil {
		return nil, syscall.EIO
	}

	node := d.NewPersistentInode(ctx, subdir, fs.StableAttr{Mode: fuse.S_IFDIR})
	out.NodeId = node.StableAttr().Ino
	return node, 0
}

// Create creates a new file.
func (d *S3Dir) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	if d.mfs.config.ReadOnly {
		return nil, nil, 0, syscall.EPERM
	}
	metricsIncCreate()

	if err := d.mfs.Wait(path.Join(d.FullPath(), name)); err != nil {
		return nil, nil, 0, syscall.EPERM
	}

	childID := d.mfs.cache.NextID()
	now := time.Now()
	file := &S3File{
		mfs:     d.mfs,
		dir:     d,
		Path:    name,
		InodeID: childID,
		Size:    0,
		Mode:    os.FileMode(mode),
		UID:     d.mfs.config.UID,
		GID:     d.mfs.config.GID,
		Chgtime: now,
		Crtime:  now,
		Mtime:   now,
		Atime:   now,
	}

	if err := d.mfs.cache.PutInode(file.toCacheInode()); err != nil {
		return nil, nil, 0, syscall.EIO
	}
	if err := d.mfs.cache.PutDirEntry(d.InodeID, name, childID); err != nil {
		return nil, nil, 0, syscall.EIO
	}

	fh, err := d.mfs.Acquire(file)
	if err != nil {
		return nil, nil, 0, syscall.EPERM
	}
	fh.dirty = true
	fh.cachePath, err = d.mfs.NewCachePath()
	if err != nil {
		return nil, nil, 0, syscall.EIO
	}
	fh.File, err = os.OpenFile(fh.cachePath, int(flags)|os.O_CREATE|os.O_RDWR, d.mfs.config.Mode)
	if err != nil {
		return nil, nil, 0, syscall.EIO
	}

	node := d.NewPersistentInode(ctx, file, fs.StableAttr{})
	out.NodeId = node.StableAttr().Ino
	return node, fh, 0, 0
}

// Unlink removes a file.
func (d *S3Dir) Unlink(ctx context.Context, name string) syscall.Errno {
	if d.mfs.config.ReadOnly {
		return syscall.EPERM
	}
	metricsIncRemove()

	if err := d.mfs.Wait(path.Join(d.FullPath(), name)); err != nil {
		return syscall.EPERM
	}

	childID, err := d.mfs.cache.GetDirEntry(d.InodeID, name)
	if err != nil {
		return syscall.ENOENT
	}

	child, err := d.mfs.cache.GetInode(childID)
	if err != nil {
		return syscall.ENOENT
	}

	// Delete from S3 (skip symlinks — they're local only).
	if child.SymlinkTarget == "" {
		remotePath := path.Join(d.RemotePath(), name)
		if delErr := d.mfs.breaker.Execute(func() error {
			return retryWithBackoff(func() error {
				return d.mfs.api.RemoveObject(ctx, d.mfs.config.Bucket, remotePath, minioRemoveOptions())
			})
		}); delErr != nil {
			metricsIncS3Error()
		}
	}

	d.mfs.cache.DeleteInode(childID)
	d.mfs.cache.DeleteDirEntry(d.InodeID, name)
	return 0
}

// Rename moves a child to a new parent with a new name.
func (d *S3Dir) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	if d.mfs.config.ReadOnly {
		return syscall.EPERM
	}
	metricsIncRename()

	newDir, ok := newParent.(*S3Dir)
	if !ok {
		return syscall.EINVAL
	}

	childID, err := d.mfs.cache.GetDirEntry(d.InodeID, name)
	if err != nil {
		return syscall.ENOENT
	}
	child, err := d.mfs.cache.GetInode(childID)
	if err != nil {
		return syscall.ENOENT
	}

	oldPath := path.Join(d.RemotePath(), name)
	newPath := path.Join(newDir.RemotePath(), newName)

	// Symlinks are local-only.
	if child.SymlinkTarget != "" {
		d.mfs.cache.DeleteDirEntry(d.InodeID, name)
		d.mfs.cache.PutDirEntry(newDir.InodeID, newName, childID)
		return 0
	}

	sr := newMoveOp(oldPath, newPath)
	if err := d.mfs.sync(sr); err != nil {
		return syscall.EIO
	}
	if err := <-sr.Error; err != nil {
		return syscall.EIO
	}

	d.mfs.cache.DeleteDirEntry(d.InodeID, name)
	d.mfs.cache.PutDirEntry(newDir.InodeID, newName, childID)
	return 0
}

// Rmdir removes a directory.
func (d *S3Dir) Rmdir(ctx context.Context, name string) syscall.Errno {
	if d.mfs.config.ReadOnly {
		return syscall.EPERM
	}

	childID, err := d.mfs.cache.GetDirEntry(d.InodeID, name)
	if err != nil {
		return syscall.ENOENT
	}

	child, err := d.mfs.cache.GetInode(childID)
	if err != nil || !child.IsDir {
		return syscall.ENOTDIR
	}

	// Remove directory marker from S3.
	remotePath := path.Join(d.RemotePath(), name, "/")
	d.mfs.breaker.Execute(func() error {
		return retryWithBackoff(func() error {
			return d.mfs.api.RemoveObject(ctx, d.mfs.config.Bucket, remotePath, minioRemoveOptions())
		})
	})

	d.mfs.cache.DeleteInode(childID)
	d.mfs.cache.DeleteDirEntry(d.InodeID, name)
	return 0
}

// Symlink creates a symbolic link stored in the local Pebble metadata cache.
// The symlink is locally persistent (survives remount as long as CacheDir
// is reused) but is NOT stored in S3 — other mount points will not see it.
// This is the standard trade-off for S3-backed FUSE: local metadata that
// doesn't have a native S3 primitive.
func (d *S3Dir) Symlink(ctx context.Context, target, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if d.mfs.config.ReadOnly {
		return nil, syscall.EPERM
	}

	childID := d.mfs.cache.NextID()
	now := time.Now()
	sym := &S3Symlink{
		mfs:     d.mfs,
		dir:     d,
		Path:    name,
		Target:  target,
		InodeID: childID,
		Mode:    0777,
		UID:     d.mfs.config.UID,
		GID:     d.mfs.config.GID,
	}

	inode := &CacheInode{
		ID:            childID,
		Name:          name,
		Mode:          uint32(os.ModeSymlink | 0777),
		UID:           d.mfs.config.UID,
		GID:           d.mfs.config.GID,
		Mtime:         now.UnixNano(),
		Ctime:         now.UnixNano(),
		Atime:         now.UnixNano(),
		SymlinkTarget: target,
	}
	if err := d.mfs.cache.PutInode(inode); err != nil {
		return nil, syscall.EIO
	}
	if err := d.mfs.cache.PutDirEntry(d.InodeID, name, childID); err != nil {
		return nil, syscall.EIO
	}

	node := d.NewPersistentInode(ctx, sym, fs.StableAttr{Mode: fuse.S_IFLNK})
	out.NodeId = node.StableAttr().Ino
	return node, 0
}

// Link is not supported — S3 has no hard link primitive.
// Returning ENOSYS tells the kernel to fall back to copy semantics
// (cp + unlink) which is the only correct behavior for S3.
func (d *S3Dir) Link(ctx context.Context, target fs.InodeEmbedder, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	return nil, syscall.ENOSYS
}

// Getattr returns directory attributes.
func (d *S3Dir) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Attr = fuse.Attr{
		Ino:   d.InodeID,
		Size:  d.Size,
		Mode:  uint32(d.Mode),
		Owner: fuse.Owner{Uid: d.UID, Gid: d.GID},
		Mtime: uint64(d.Mtime.Unix()),
		Ctime: uint64(d.Chgtime.Unix()),
		Nlink: 2,
	}
	return 0
}

// Setattr updates directory attributes.
func (d *S3Dir) Setattr(ctx context.Context, fh fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	if mode, ok := in.GetMode(); ok {
		d.Mode = os.FileMode(mode) | os.ModeDir
	}
	if uid, ok := in.GetUID(); ok {
		d.UID = uid
	}
	if gid, ok := in.GetGID(); ok {
		d.GID = gid
	}
	if mt, ok := in.GetMTime(); ok {
		d.Mtime = mt
	}
	d.Chgtime = time.Now()

	// Update cache.
	inode := d.toCacheInode()
	inode.Mode = uint32(d.Mode)
	inode.UID = d.UID
	inode.GID = d.GID
	inode.Mtime = d.Mtime.UnixNano()
	inode.Ctime = d.Chgtime.UnixNano()
	_ = d.mfs.cache.PutInode(inode)

	out.Attr = fuse.Attr{
		Ino:   d.InodeID,
		Size:  d.Size,
		Mode:  uint32(d.Mode),
		Owner: fuse.Owner{Uid: d.UID, Gid: d.GID},
		Mtime: uint64(d.Mtime.Unix()),
		Ctime: uint64(d.Chgtime.Unix()),
		Nlink: 2,
	}
	return 0
}

// Access always returns 0 (allow-all).
func (d *S3Dir) Access(ctx context.Context, mask uint32) syscall.Errno {
	return 0
}

// Statfs reports the bucket's usage as observed from the server: on
// MinIO with admin credentials this is the server-side data-usage
// accounting plus the configured bucket quota (df shows
// total/used/free); on any other S3 server it is a lazy ListObjectsV2
// sweep, cached for ScanTTL (df shows used only - capacity is
// unbounded). See statfs.go for why no locally maintained counter is
// used: the bucket is shared state, only the server knows the truth.
func (d *S3Dir) Statfs(ctx context.Context, out *fuse.StatfsOut) syscall.Errno {
	const blockSize = uint32(4096)
	out.Bsize = blockSize
	out.Frsize = blockSize
	out.NameLen = 255
	if d.mfs == nil {
		return 0
	}
	usage, quota := d.mfs.statfsCache.get(ctx)
	used := usage / uint64(blockSize)
	if quota > 0 {
		capBlocks := quota / uint64(blockSize)
		out.Blocks = capBlocks
		if capBlocks > used {
			out.Bfree = capBlocks - used
			out.Bavail = capBlocks - used
		}
	} else {
		// S3 capacity is genuinely unbounded. Reporting Blocks=used and
		// Bfree=0 would make statvfs say the mount is 100% full, breaking
		// every space-checking writer (installers, backups, runtimes).
		// Instead add a nominal 1 TiB free headroom: the df total is
		// slightly larger than used, use% stays small and honest.
		free := statfsNominalFree / uint64(blockSize)
		out.Blocks = used + free
		out.Bfree = free
		out.Bavail = free
	}
	return 0
}

// scan refreshes the directory listing from S3 if the TTL has expired.
func (d *S3Dir) scan(ctx context.Context) error {
	if !d.mfs.config.Debug && d.scanned && time.Since(d.lastScan) < d.mfs.config.ScanTTL {
		return nil
	}

	prefix := d.RemotePath()
	if prefix != "" {
		prefix += "/"
	}

	metricsIncS3List()
	ch := d.mfs.api.ListObjects(ctx, d.mfs.config.Bucket, minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: false,
	})

	seen := make(map[string]bool)

	for obj := range ch {
		if obj.Err != nil {
			continue
		}
		key := obj.Key[len(prefix):]
		if key == "" {
			continue
		}
		name := path.Base(key)
		seen[name] = true

		if obj.LastModified.IsZero() && obj.Size == 0 && len(key) > 0 && key[len(key)-1] == '/' {
			// Directory marker.
			childID, err := d.mfs.cache.GetDirEntry(d.InodeID, name)
			if err != nil {
				childID = d.mfs.cache.NextID()
			}
			subdir := &S3Dir{
				mfs:     d.mfs,
				dir:     d,
				Path:    name,
				InodeID: childID,
				Mode:    os.ModeDir | 0750,
				UID:     d.mfs.config.UID,
				GID:     d.mfs.config.GID,
				Chgtime: time.Now(),
				Crtime:  time.Now(),
				Mtime:   time.Now(),
				Atime:   time.Now(),
			}
			_ = d.mfs.cache.PutInode(subdir.toCacheInode())
			_ = d.mfs.cache.PutDirEntry(d.InodeID, name, childID)
		} else {
			childID, err := d.mfs.cache.GetDirEntry(d.InodeID, name)
			if err != nil {
				childID = d.mfs.cache.NextID()
			}
			file := &S3File{
				mfs:     d.mfs,
				dir:     d,
				Path:    name,
				InodeID: childID,
				Size:    uint64(obj.Size),
				Mode:    d.mfs.config.Mode,
				UID:     d.mfs.config.UID,
				GID:     d.mfs.config.GID,
				ETag:    obj.ETag,
				Mtime:   obj.LastModified,
				Chgtime: obj.LastModified,
				Crtime:  obj.LastModified,
				Atime:   obj.LastModified,
			}
			_ = d.mfs.cache.PutInode(file.toCacheInode())
			_ = d.mfs.cache.PutDirEntry(d.InodeID, name, childID)
		}
	}

	// Remove entries that no longer exist in S3.
	entries, _ := d.mfs.cache.ListDirEntries(d.InodeID)
	for name := range entries {
		if !seen[name] {
			childID, _ := d.mfs.cache.GetDirEntry(d.InodeID, name)
			d.mfs.cache.DeleteInode(childID)
			d.mfs.cache.DeleteDirEntry(d.InodeID, name)
		}
	}

	d.scanned = true
	d.lastScan = time.Now()
	return nil
}

func cacheToAttr(in *CacheInode) fuse.Attr {
	return fuse.Attr{
		Ino:   in.ID,
		Size:  in.Size,
		Mode:  in.Mode,
		Owner: fuse.Owner{Uid: in.UID, Gid: in.GID},
		Mtime: uint64(time.Unix(0, in.Mtime).Unix()),
		Ctime: uint64(time.Unix(0, in.Ctime).Unix()),
		Nlink: 1,
	}
}
