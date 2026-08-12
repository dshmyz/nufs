//go:build linux

package fuse

import (
	"context"
	"os/user"
	"strconv"
	"syscall"

	"github.com/dshmyz/nufs/nufs-core/metadata"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// checkPOSIXAccess evaluates an inode's POSIX permission bits against the
// requesting caller's uid/gid for the given access mask, and returns EACCES if
// the mode bits deny the request (0 when granted).
//
// Every backend (DFSFile/DFSDir/DFSSymlink/DFSFifo) implements NodeAccesser,
// which would otherwise short-circuit go-fuse's default permission check with
// a blanket allow-all. This helper restores that check using the same POSIX
// semantics go-fuse applies when no NodeAccesser is present: owner bits when
// the caller owns the inode, group bits on gid match, otherwise other bits;
// uid 0 (root) bypasses everything.
func checkPOSIXAccess(ctx context.Context, dfs *DFSFileSystem, inodeID metadata.InodeID, mask uint32) syscall.Errno {
	caller, ok := ctx.(*fuse.Context)
	if !ok {
		// No caller info (e.g. unit test constructing a bare context): be
		// permissive rather than breaking existing access paths.
		return 0
	}

	metaInode, err := dfs.Meta().GetInode(ctx, inodeID)
	if err != nil {
		return syscall.EIO
	}
	attr := inodeMetaToAttr(metaInode)

	if hasPOSIXAccess(caller.Uid, caller.Gid, attr.Owner.Uid, attr.Owner.Gid, attr.Mode, mask) {
		return 0
	}
	return syscall.EACCES
}

// POSIX permission and special-bit masks.
const (
	posixR = 0400 // owner read
	posixW = 0200 // owner write
	posixX = 0100 // owner execute

	sIsuid = 0o4000 // set-user-ID
	sIsgid = 0o2000 // set-group-ID
	sIsvtx = 0o1000 // sticky bit
)

// otherwise the owner/group/other permission bits are consulted in the kernel's
// usual order, with a supplementary-group lookup when the file group doesn't
// match the caller's primary group.
func hasPOSIXAccess(callerUid, callerGid, fileUid, fileGid, mode, mask uint32) bool {
	if callerUid == 0 {
		return true
	}
	mask &= 7
	if mask == 0 {
		return true
	}

	if callerUid == fileUid {
		// POSIX: ownership decides access entirely by the owner bits. Once the
		// caller matches the file owner, group/other bits must not be consulted
		// (even on owner-bits denial) — otherwise an owner denied by their own
		// bits could be re-granted via group/other bits (over-authorization).
		return mode&(mask<<6) != 0
	}
	if callerGid == fileGid {
		if mode&(mask<<3) != 0 {
			return true
		}
	}
	if mode&mask != 0 {
		return true
	}

	// Check supplementary groups of the caller against the file group
	// (group-read bits are a fast-path rejection before the lookup).
	if mode&(mask<<3) == 0 {
		return false
	}
	u, err := user.LookupId(strconv.Itoa(int(callerUid)))
	if err != nil {
		return false
	}
	gs, err := u.GroupIds()
	if err != nil {
		return false
	}
	fileGidStr := strconv.Itoa(int(fileGid))
	for _, gidStr := range gs {
		if gidStr == fileGidStr && mode&(mask<<3) != 0 {
			return true
		}
	}
	return false
}
