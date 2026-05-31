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
	"strings"

	"bazil.org/fuse"

	"github.com/minio/minfs/meta"
)

const xattrSuffix = "__xattr__/"

// xattrBucket returns the xattr sub-bucket for a given node bucket.
func xattrBucket(b *meta.Bucket) *meta.Bucket {
	return b.Bucket(xattrSuffix)
}

// xattrGet retrieves a single extended attribute.
func xattrGet(b *meta.Bucket, name string) ([]byte, error) {
	xb := xattrBucket(b)
	var val []byte
	if err := xb.Get(name, &val); err != nil {
		if meta.IsNoSuchObject(err) {
			return nil, fuse.ErrNoXattr
		}
		return nil, err
	}
	return val, nil
}

// xattrSet sets an extended attribute.
func xattrSet(b *meta.Bucket, name string, value []byte) error {
	xb := xattrBucket(b)
	return xb.Put(name, value)
}

// xattrList lists all extended attribute names.
func xattrList(b *meta.Bucket) ([]string, error) {
	xb := xattrBucket(b)
	var names []string
	err := xb.ForEach(func(k string, _ interface{}) error {
		names = append(names, k)
		return nil
	})
	return names, err
}

// xattrRemove removes an extended attribute.
func xattrRemove(b *meta.Bucket, name string) error {
	xb := xattrBucket(b)
	err := xb.Delete(name)
	if meta.IsNoSuchObject(err) {
		return fuse.ErrNoXattr
	}
	return err
}

// xattrCopyAll copies all xattrs from src bucket to dst bucket.
func xattrCopyAll(src, dst *meta.Bucket) error {
	xbSrc := xattrBucket(src)
	xbDst := xattrBucket(dst)
	return xbSrc.ForEach(func(k string, v interface{}) error {
		if data, ok := v.([]byte); ok {
			return xbDst.Put(k, data)
		}
		return nil
	})
}

// --- File xattr methods ---

// Getxattr gets an extended attribute from a file.
func (f *File) Getxattr(ctx context.Context, req *fuse.GetxattrRequest, resp *fuse.GetxattrResponse) error {
	return f.mfs.db.View(func(tx *meta.Tx) error {
		b := f.bucket(tx)
		val, err := xattrGet(b, req.Name)
		if err != nil {
			return err
		}
		resp.Xattr = val
		return nil
	})
}

// Setxattr sets an extended attribute on a file.
func (f *File) Setxattr(ctx context.Context, req *fuse.SetxattrRequest) error {
	if f.mfs.config.readOnly {
		return fuse.EPERM
	}
	return f.mfs.db.Update(func(tx *meta.Tx) error {
		b := f.bucket(tx)
		return xattrSet(b, req.Name, req.Xattr)
	})
}

// Listxattr lists all extended attributes on a file.
func (f *File) Listxattr(ctx context.Context, req *fuse.ListxattrRequest, resp *fuse.ListxattrResponse) error {
	return f.mfs.db.View(func(tx *meta.Tx) error {
		b := f.bucket(tx)
		names, err := xattrList(b)
		if err != nil {
			return err
		}
		resp.Xattr = []byte(strings.Join(names, "\x00"))
		return nil
	})
}

// Removexattr removes an extended attribute from a file.
func (f *File) Removexattr(ctx context.Context, req *fuse.RemovexattrRequest) error {
	if f.mfs.config.readOnly {
		return fuse.EPERM
	}
	return f.mfs.db.Update(func(tx *meta.Tx) error {
		b := f.bucket(tx)
		return xattrRemove(b, req.Name)
	})
}

// --- Dir xattr methods ---

// Getxattr gets an extended attribute from a directory.
func (dir *Dir) Getxattr(ctx context.Context, req *fuse.GetxattrRequest, resp *fuse.GetxattrResponse) error {
	return dir.mfs.db.View(func(tx *meta.Tx) error {
		b := dir.bucket(tx)
		val, err := xattrGet(b, req.Name)
		if err != nil {
			return err
		}
		resp.Xattr = val
		return nil
	})
}

// Setxattr sets an extended attribute on a directory.
func (dir *Dir) Setxattr(ctx context.Context, req *fuse.SetxattrRequest) error {
	if dir.mfs.config.readOnly {
		return fuse.EPERM
	}
	return dir.mfs.db.Update(func(tx *meta.Tx) error {
		b := dir.bucket(tx)
		return xattrSet(b, req.Name, req.Xattr)
	})
}

// Listxattr lists all extended attributes on a directory.
func (dir *Dir) Listxattr(ctx context.Context, req *fuse.ListxattrRequest, resp *fuse.ListxattrResponse) error {
	return dir.mfs.db.View(func(tx *meta.Tx) error {
		b := dir.bucket(tx)
		names, err := xattrList(b)
		if err != nil {
			return err
		}
		resp.Xattr = []byte(strings.Join(names, "\x00"))
		return nil
	})
}

// Removexattr removes an extended attribute from a directory.
func (dir *Dir) Removexattr(ctx context.Context, req *fuse.RemovexattrRequest) error {
	if dir.mfs.config.readOnly {
		return fuse.EPERM
	}
	return dir.mfs.db.Update(func(tx *meta.Tx) error {
		b := dir.bucket(tx)
		return xattrRemove(b, req.Name)
	})
}
