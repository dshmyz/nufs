//go:build linux

package fuse

import (
	"context"
	"syscall"
	"testing"

	"github.com/dshmyz/nufs/nufs-core/metadata"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// caller builds a *fuse.Context carrying the given uid/gid, the caller identity
// go-fuse would normally populate from each request.
func caller(uid, gid uint32) *fuse.Context {
	return &fuse.Context{Caller: fuse.Caller{Owner: fuse.Owner{Uid: uid, Gid: gid}}}
}

// setOwner updates an inode's uid/gid/mode so the Access check runs against a
// caller other than the default (uid 0/gid 0).
func setOwner(t *testing.T, store *metadata.PebbleStore, id metadata.InodeID, uid, gid uint32, mode uint32) {
	t.Helper()
	mi, err := store.GetInode(context.Background(), id)
	if err != nil {
		t.Fatalf("GetInode(%d): %v", id, err)
	}
	mi.UID = uid
	mi.GID = gid
	mi.Mode = mode
	if err := store.UpdateInode(context.Background(), mi); err != nil {
		t.Fatalf("UpdateInode(%d): %v", id, err)
	}
}

// TestAccess_HasNoCaller_IsPermissive pins that a bare context (no caller, as in
// some tooling paths) does not fail the Access check.
func TestAccess_HasNoCaller_IsPermissive(t *testing.T) {
	store, root := newTestMetaStore(t)
	dfs := NewDFSFileSystem(store, nil, nil, nil, nil)
	f := &DFSFile{fs: dfs, inodeID: root}
	if errno := f.Access(context.Background(), 0o4); errno != 0 {
		t.Fatalf("Access(no caller) = %d, want 0 (permissive)", errno)
	}
}

// TestHasPOSIXAccess_OwnerBitsGovernOverGroupOther verifies that once a caller
// matches the file owner, access is decided solely by the owner bits — an owner
// denied by their own bits must NOT be re-granted via group/other bits
// (POSIX owner-short-circuit; guards against over-authorization).
func TestHasPOSIXAccess_OwnerBitsGovernOverGroupOther(t *testing.T) {
	// Mode 0o470: owner r--, group rwx, other none. Owner requests write (0o2):
	// owner write bit is clear, so the owner must be DENIED — even though the
	// group write bit grants it and the caller's gid matches the file gid.
	if hasPOSIXAccess(1000, 1000, 1000, 1000, 0o470, 0o2) {
		t.Errorf("owner write on 0o470 (owner write bit clear): granted, want denied")
	}
	// Same inode, owner read (0o4): owner read bit set -> granted.
	if !hasPOSIXAccess(1000, 1000, 1000, 1000, 0o470, 0o4) {
		t.Errorf("owner read on 0o470: denied, want granted")
	}
	// A non-owner in the file's group is governed by the group bits: group rwx
	// grants write even though the owner write bit is clear.
	if !hasPOSIXAccess(2000, 1000, 1000, 1000, 0o470, 0o2) {
		t.Errorf("group member write on 0o470: denied, want granted")
	}
	// Root bypasses everything.
	if !hasPOSIXAccess(0, 0, 1000, 1000, 0o000, 0o2) {
		t.Errorf("root write on 0o000: denied, want granted")
	}
}

// TestAccess_FileModeBits verifies the Access handler reflects POSIX mode bits:
// the owner may read/write an owned 0640 file but not execute it, and a
// different uid without group membership is denied both read and write.
func TestAccess_FileModeBits(t *testing.T) {
	store, _ := newTestMetaStore(t)
	ctx := context.Background()

	dfs := NewDFSFileSystem(store, nil, nil, nil, nil)

	bucket, err := store.GetBucket(ctx, "test")
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}
	fid, err := store.CreateFile(ctx, bucket.RootInode, "mode1.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile: %v", err)
	}
	fileID := fid.ID
	setOwner(t, store, fileID, 1000, 1000, 0o640)

	f := &DFSFile{fs: dfs, inodeID: fileID}

	// Owner (uid 1000): read+write granted (rw-), execute denied.
	if errno := f.Access(caller(1000, 1000), 0o4); errno != 0 {
		t.Errorf("owner read = %d, want 0", errno)
	}
	if errno := f.Access(caller(1000, 1000), 0o2); errno != 0 {
		t.Errorf("owner write = %d, want 0", errno)
	}
	if errno := f.Access(caller(1000, 1000), 0o1); errno != syscall.EACCES {
		t.Errorf("owner execute = %d, want EACCES", errno)
	}

	// A uid not matching owner/gid (uid 2000, gid 2000): no access bits → denied.
	if errno := f.Access(caller(2000, 2000), 0o4); errno != syscall.EACCES {
		t.Errorf("other read = %d, want EACCES", errno)
	}
	if errno := f.Access(caller(2000, 2000), 0o2); errno != syscall.EACCES {
		t.Errorf("other write = %d, want EACCES", errno)
	}

	// Root bypasses everything.
	if errno := f.Access(caller(0, 0), 0o1); errno != 0 {
		t.Errorf("root execute = %d, want 0 (root bypass)", errno)
	}
}

// TestAccess_DirExecute verifies the directory Access handler enforces execute
// (search) permission for lookup, and masks meaningless bits via mask&7.
func TestAccess_DirExecute(t *testing.T) {
	store, root := newTestMetaStore(t)
	dfs := NewDFSFileSystem(store, nil, nil, nil, nil)

	mi, err := store.GetInode(context.Background(), root)
	if err != nil {
		t.Fatalf("GetInode(root): %v", err)
	}
	mi.UID = 1000
	mi.GID = 1000
	mi.Mode = 0o700
	if err := store.UpdateInode(context.Background(), mi); err != nil {
		t.Fatalf("UpdateInode(root): %v", err)
	}

	d := &DFSDir{fs: dfs, inodeID: root}

	// Owner can search (x) and list (r); a non-owner cannot.
	if errno := d.Access(caller(1000, 1000), 0o1); errno != 0 {
		t.Errorf("owner execute = %d, want 0", errno)
	}
	if errno := d.Access(caller(1000, 1000), 0o5); errno != 0 {
		t.Errorf("owner read+execute = %d, want 0", errno)
	}
	if errno := d.Access(caller(2000, 2000), 0o1); errno != syscall.EACCES {
		t.Errorf("other execute = %d, want EACCES", errno)
	}
}

// TestOpen_EnforcesPermissionBits regresses gap 1: POSIX open(2) must check the
// requested access mode (read/write) against the file's permission bits before
// returning a file descriptor.  A mode-0000 file must not be openable for
// reading or writing, even by the file's owner.
func TestOpen_EnforcesPermissionBits(t *testing.T) {
	store, _ := newTestMetaStore(t)
	ctx := context.Background()
	dfs := NewDFSFileSystem(store, nil, nil, nil, nil)

	bucket, err := store.GetBucket(ctx, "test")
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}
	fid, err := store.CreateFile(ctx, bucket.RootInode, "noperm.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile: %v", err)
	}
	setOwner(t, store, fid.ID, 1000, 1000, 0o000)

	f := &DFSFile{fs: dfs, inodeID: fid.ID}

	// Mode 0000: owner cannot read, write, or execute.
	if _, _, errno := f.Open(caller(1000, 1000), syscall.O_RDONLY); errno != syscall.EACCES {
		t.Errorf("open O_RDONLY on 0000 file: errno=%d, want EACCES", errno)
	}
	if _, _, errno := f.Open(caller(1000, 1000), syscall.O_WRONLY); errno != syscall.EACCES {
		t.Errorf("open O_WRONLY on 0000 file: errno=%d, want EACCES", errno)
	}
	if _, _, errno := f.Open(caller(1000, 1000), syscall.O_RDWR); errno != syscall.EACCES {
		t.Errorf("open O_RDWR on 0000 file: errno=%d, want EACCES", errno)
	}

	// Root bypasses the check.
	if _, _, errno := f.Open(caller(0, 0), syscall.O_RDONLY); errno != 0 {
		t.Errorf("root open O_RDONLY on 0000 file: errno=%d, want 0", errno)
	}

	// Now set mode 0640: owner can read+write but not execute.
	setOwner(t, store, fid.ID, 1000, 1000, 0o640)
	if _, _, errno := f.Open(caller(1000, 1000), syscall.O_RDONLY); errno != 0 {
		t.Errorf("open O_RDONLY on 0640 file (owner): errno=%d, want 0", errno)
	}
	if _, _, errno := f.Open(caller(1000, 1000), syscall.O_WRONLY); errno != 0 {
		t.Errorf("open O_WRONLY on 0640 file (owner): errno=%d, want 0", errno)
	}

	// Non-owner (uid 2000, gid 2000): no read, no write on 0640.
	if _, _, errno := f.Open(caller(2000, 2000), syscall.O_RDONLY); errno != syscall.EACCES {
		t.Errorf("open O_RDONLY on 0640 file (other): errno=%d, want EACCES", errno)
	}
}

// TestSetattr_ChownClearsSetuidSetgid regresses gap 2: POSIX requires chown
// (uid/gid change via Setattr) to always clear the setuid/setgid bits, even
// when the caller is root.
func TestSetattr_ChownClearsSetuidSetgid(t *testing.T) {
	store, _ := newTestMetaStore(t)
	ctx := context.Background()
	dfs := NewDFSFileSystem(store, nil, nil, nil, nil)

	bucket, err := store.GetBucket(ctx, "test")
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}
	fid, err := store.CreateFile(ctx, bucket.RootInode, "suid.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile: %v", err)
	}
	setOwner(t, store, fid.ID, 1000, 1000, 0o6755) // setuid + setgid + rwxr-xr-x

	f := &DFSFile{fs: dfs, inodeID: fid.ID}
	out := &fuse.AttrOut{}

	// chown: change uid to 2000 → setuid/setgid bits must be cleared.
	in := &fuse.SetAttrIn{SetAttrInCommon: fuse.SetAttrInCommon{Valid: fuse.FATTR_UID, Owner: fuse.Owner{Uid: 2000}}}
	if errno := f.Setattr(caller(1000, 1000), nil, in, out); errno != 0 {
		t.Fatalf("Setattr(chown): errno=%d", errno)
	}
	mi, _ := store.GetInode(ctx, fid.ID)
	if mi.Mode&(sIsuid|sIsgid) != 0 {
		t.Errorf("after chown: mode=%o, setuid/setgid not cleared", mi.Mode)
	}

	// Restore setuid+setgid and chown by gid.
	setOwner(t, store, fid.ID, 1000, 1000, 0o6755)
	in2 := &fuse.SetAttrIn{SetAttrInCommon: fuse.SetAttrInCommon{Valid: fuse.FATTR_GID, Owner: fuse.Owner{Gid: 2000}}}
	if errno := f.Setattr(caller(1000, 1000), nil, in2, out); errno != 0 {
		t.Fatalf("Setattr(chown gid): errno=%d", errno)
	}
	mi2, _ := store.GetInode(ctx, fid.ID)
	if mi2.Mode&(sIsuid|sIsgid) != 0 {
		t.Errorf("after chown(gid): mode=%o, setuid/setgid not cleared", mi2.Mode)
	}

	// chmod does NOT clear setuid/setgid (it sets the mode directly).
	setOwner(t, store, fid.ID, 1000, 1000, 0o6755)
	in3 := &fuse.SetAttrIn{SetAttrInCommon: fuse.SetAttrInCommon{Valid: fuse.FATTR_MODE, Mode: 0o4755}}
	if errno := f.Setattr(caller(1000, 1000), nil, in3, out); errno != 0 {
		t.Fatalf("Setattr(chmod): errno=%d", errno)
	}
	mi3, _ := store.GetInode(ctx, fid.ID)
	if mi3.Mode&sIsuid == 0 {
		t.Errorf("after chmod 4755: mode=%o, setuid should still be set", mi3.Mode)
	}
}

// TestUnlink_StickyBitBlocksNonOwner regresses gap 3: POSIX sticky-bit
// semantics on unlink — when the parent directory has S_ISVTX set, only the
// file's owner (or root) may unlink it.
func TestUnlink_StickyBitBlocksNonOwner(t *testing.T) {
	store, root := newTestMetaStore(t)
	ctx := context.Background()
	dfs := NewDFSFileSystem(store, nil, nil, nil, nil)

	// Make root a sticky directory owned by uid 1000.
	setOwner(t, store, root, 1000, 1000, 0o1777) // sticky bit set

	// Create a file owned by uid 1000.
	fid, err := store.CreateFile(ctx, root, "victim.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile: %v", err)
	}
	setOwner(t, store, fid.ID, 1000, 1000, 0o644)

	d := &DFSDir{fs: dfs, inodeID: root}

	// Owner (uid 1000) can unlink their own file.
	if errno := d.Unlink(caller(1000, 1000), "victim.txt"); errno != 0 {
		t.Errorf("owner unlink: errno=%d, want 0", errno)
	}

	// Create another file owned by uid 1000.
	fid2, err := store.CreateFile(ctx, root, "victim2.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile: %v", err)
	}
	setOwner(t, store, fid2.ID, 1000, 1000, 0o644)

	// Non-owner (uid 2000) cannot unlink in a sticky directory.
	if errno := d.Unlink(caller(2000, 2000), "victim2.txt"); errno != syscall.EACCES {
		t.Errorf("non-owner unlink in sticky dir: errno=%d, want EACCES", errno)
	}

	// Root can unlink anything in a sticky directory.
	if errno := d.Unlink(caller(0, 0), "victim2.txt"); errno != 0 {
		t.Errorf("root unlink in sticky dir: errno=%d, want 0", errno)
	}
}

// TestSetgidInheritance regresses gap 4: POSIX setgid inheritance — new
// entries created in a setgid directory inherit the directory's gid, not the
// caller's gid.  We test the metadata-level logic directly since FUSE bridge
// is not available in unit tests.
func TestSetgidInheritance(t *testing.T) {
	store, root := newTestMetaStore(t)
	ctx := context.Background()

	// Make root a setgid directory owned by gid 500.
	setOwner(t, store, root, 1000, 500, 0o2755) // setgid + rwxr-xr-x

	// Simulate the setgid-inheritance path that DFSDir.Mkdir applies:
	// read parent mode, if setgid → override child's gid with parent's gid.
	parentMeta, err := store.GetInode(ctx, root)
	if err != nil {
		t.Fatalf("GetInode(parent): %v", err)
	}

	// Create a child directory.
	childDir, err := store.MkDir(ctx, root, "subdir", 0o755)
	if err != nil {
		t.Fatalf("MkDir: %v", err)
	}

	// Apply mount-level uid/gid override (simulates applyMountOwner).
	childDir.UID = 1000
	childDir.GID = 1000 // mount caller's gid
	if err := store.UpdateInode(ctx, childDir); err != nil {
		t.Fatalf("UpdateInode: %v", err)
	}

	// Apply setgid inheritance: if parent has setgid, override child gid.
	if parentMeta.Mode&sIsgid != 0 {
		childDir.GID = parentMeta.GID
		if err := store.UpdateInode(ctx, childDir); err != nil {
			t.Fatalf("UpdateInode(setgid): %v", err)
		}
	}

	result, _ := store.GetInode(ctx, childDir.ID)
	if result.GID != 500 {
		t.Errorf("mkdir setgid: child gid=%d, want 500 (inherited from parent)", result.GID)
	}

	// Create a file in the setgid directory — should also inherit gid 500.
	childFile, err := store.CreateFile(ctx, root, "child.txt", 0o644)
	if err != nil {
		t.Fatalf("CreateFile: %v", err)
	}
	childFile.UID = 1000
	childFile.GID = 1000
	if err := store.UpdateInode(ctx, childFile); err != nil {
		t.Fatalf("UpdateInode: %v", err)
	}
	if parentMeta.Mode&sIsgid != 0 {
		childFile.GID = parentMeta.GID
		if err := store.UpdateInode(ctx, childFile); err != nil {
			t.Fatalf("UpdateInode(setgid): %v", err)
		}
	}

	result2, _ := store.GetInode(ctx, childFile.ID)
	if result2.GID != 500 {
		t.Errorf("create in setgid dir: file gid=%d, want 500 (inherited)", result2.GID)
	}
}
