package s3

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// resolveObjectKey resolves an S3 key to (parentInode, name, isDir) inside the
// bucket for WRITES, creating intermediate directories for "/"-separated keys
// so that S3 PUT folder/a.txt and FUSE mkdir+write converge on the same real
// directory layout instead of a flat key. Idempotent: existing directories are
// reused (concurrent PUTs racing on the same mkdir are tolerated).
//
// A key ending in "/" is treated as a directory-marker object (creates the
// directory and returns isDir=true, matching the S3 zero-byte dir convention).
// Empty components (leading or doubled "//") are rejected — the namespace
// cannot name an empty segment.
func resolveObjectKey(ctx context.Context, meta metadata.MetadataService, rootInode metadata.InodeID, key string) (parent metadata.InodeID, name string, isDir bool, err error) {
	trimmed := strings.Trim(key, " ")
	if trimmed == "" {
		return 0, "", false, fmt.Errorf("invalid object key: empty")
	}
	if !strings.Contains(key, "/") {
		return rootInode, key, false, nil
	}
	// trailing slash = directory-marker object
	isDir = strings.HasSuffix(key, "/")
	path := strings.TrimSuffix(key, "/")
	parts := strings.Split(path, "/")
	for _, p := range parts {
		if p == "" {
			return 0, "", false, fmt.Errorf("invalid object key %q: empty path component", key)
		}
	}

	p := rootInode
	for i := 0; i < len(parts)-1; i++ {
		node, lerr := meta.Lookup(ctx, p, parts[i])
		if lerr != nil {
			if !errors.Is(lerr, metadata.ErrEntryNotFound) && !errors.Is(lerr, metadata.ErrInodeNotFound) {
				return 0, "", false, lerr
			}
			created, mkErr := meta.MkDir(ctx, p, parts[i], 0o755)
			if mkErr != nil {
				if errors.Is(mkErr, metadata.ErrEntryExists) {
					node, lerr = meta.Lookup(ctx, p, parts[i])
					if lerr != nil {
						return 0, "", false, lerr
					}
				} else {
					return 0, "", false, mkErr
				}
			} else {
				node = created
			}
		}
		if node.Type != metadata.FileDirectory {
			return 0, "", false, fmt.Errorf("object key %q: path component %q is not a directory", key, parts[i])
		}
		p = node.ID
	}
	return p, parts[len(parts)-1], isDir, nil
}

// resolveObjectForRead resolves an S3 key to its inode for reads/head/delete.
// It prefers the hierarchical layout (real directories created by writes);
// when any path component cannot be resolved as a directory it falls back to
// the legacy flat lookup (key stored as a single child of the bucket root),
// preserving access to keys written before directory-creating writes existed.
func resolveObjectForRead(ctx context.Context, meta metadata.MetadataService, rootInode metadata.InodeID, key string) (*metadata.InodeMeta, error) {
	flat := func() (*metadata.InodeMeta, error) { return meta.Lookup(ctx, rootInode, key) }
	if !strings.Contains(key, "/") {
		return flat()
	}
	path := strings.TrimSuffix(key, "/")
	parts := strings.Split(path, "/")
	for _, p := range parts {
		if p == "" {
			return flat()
		}
	}
	parent := rootInode
	for i := 0; i < len(parts)-1; i++ {
		node, err := meta.Lookup(ctx, parent, parts[i])
		if err != nil {
			return flat()
		}
		if node.Type != metadata.FileDirectory {
			return flat()
		}
		parent = node.ID
	}
	inode, err := meta.Lookup(ctx, parent, parts[len(parts)-1])
	if err != nil {
		return flat()
	}
	return inode, nil
}
