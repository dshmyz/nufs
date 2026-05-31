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
	"os"
	"path"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"

	"github.com/minio/minfs/meta"
)

// Symlink represents a symbolic link in the filesystem.
// Symlinks are stored locally in BadgerDB and are not persisted to S3.
type Symlink struct {
	mfs *MinFS
	dir *Dir

	Path   string
	Target string

	Inode uint64
	Mode  os.FileMode

	Atime time.Time
	Mtime time.Time

	UID uint32
	GID uint32

	Chgtime time.Time
	Crtime  time.Time
}

var _ fs.Node = (*Symlink)(nil)
var _ fs.NodeReadlinker = (*Symlink)(nil)
var _ fs.NodeSetattrer = (*Symlink)(nil)
var _ fs.NodeAccesser = (*Symlink)(nil)

func (s *Symlink) store(tx *meta.Tx) error {
	b := s.dir.bucket(tx)
	return b.Put(s.Path, s)
}

// Attr returns symlink attributes.
func (s *Symlink) Attr(ctx context.Context, a *fuse.Attr) error {
	*a = fuse.Attr{
		Inode: s.Inode,
		Size:  uint64(len(s.Target)),
		Atime: s.Atime,
		Mtime: s.Mtime,
		Ctime: s.Chgtime,
		Mode:  s.Mode | os.ModeSymlink,
		Uid:   s.UID,
		Gid:   s.GID,
	}
	return nil
}

// Readlink returns the symlink target.
func (s *Symlink) Readlink(ctx context.Context, req *fuse.ReadlinkRequest) (string, error) {
	return s.Target, nil
}

// Setattr updates symlink attributes (chown, utimes).
func (s *Symlink) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	return s.mfs.db.Update(func(tx *meta.Tx) error {
		if req.Valid.Uid() {
			s.UID = req.Uid
		}
		if req.Valid.Gid() {
			s.GID = req.Gid
		}
		if req.Valid.Atime() {
			s.Atime = req.Atime
		}
		if req.Valid.Mtime() {
			s.Mtime = req.Mtime
		}
		if req.Valid.Chgtime() {
			s.Chgtime = req.Chgtime
		}
		return s.store(tx)
	})
}

// Access checks permission for the symlink node.
func (s *Symlink) Access(ctx context.Context, req *fuse.AccessRequest) error {
	return nil // Handled by kernel via DefaultPermissions.
}

// Dirent returns the Symlink as a fuse.Dirent.
func (s *Symlink) Dirent() fuse.Dirent {
	return fuse.Dirent{
		Inode: s.Inode, Name: s.Path, Type: fuse.DT_Link,
	}
}

// FullPath returns the full local path of the symlink.
func (s *Symlink) FullPath() string {
	return path.Join(s.dir.FullPath(), s.Path)
}

func (s *Symlink) delete(tx *meta.Tx) error {
	b := s.dir.bucket(tx)
	return b.Delete(s.Path)
}
