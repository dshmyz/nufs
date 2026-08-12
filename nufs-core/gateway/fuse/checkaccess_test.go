//go:build linux

package fuse

import (
	"context"
	"syscall"
	"testing"

	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// TestCheckAccess_DefaultDeny regresses the security fix: a bucket with no
// explicit policy must DENY an authenticated mount rather than falling open
// (the old "no policy = open" behavior). A principal that is the bucket owner,
// or explicitly allowed, passes; everything else is denied.
func TestCheckAccess_DefaultDeny(t *testing.T) {
	store, _ := newTestMetaStore(t)
	ctx := context.Background()

	dfs := NewDFSFileSystem(store, nil, nil, nil, nil)
	dfs.owner = "app-server-1"

	// The "test" bucket exists but has no policy yet → must be denied.
	if err := dfs.checkAccess("test", metadata.PermWrite); err != syscall.EACCES {
		t.Fatalf("no-policy bucket: checkAccess = %v, want EACCES (default deny)", err)
	}

	// An unknown bucket is also denied (GetBucketPolicy reports access denied).
	if err := dfs.checkAccess("does-not-exist", metadata.PermWrite); err != syscall.EACCES {
		t.Fatalf("missing bucket: checkAccess = %v, want EACCES", err)
	}

	// Owner with an explicit policy is allowed (owner always has access).
	ownerPolicy := metadata.BucketPolicy{
		Bucket:        "test",
		Owner:         "app-server-1",
		DefaultAccess: "deny",
	}
	if err := store.SetBucketPolicy(ctx, "test", ownerPolicy); err != nil {
		t.Fatalf("SetBucketPolicy: %v", err)
	}
	if err := dfs.checkAccess("test", metadata.PermWrite); err != nil {
		t.Fatalf("owner with policy: checkAccess = %v, want nil", err)
	}

	// A non-owner principal with no allow statement is denied.
	dfs.owner = "other-user"
	if err := dfs.checkAccess("test", metadata.PermWrite); err != syscall.EACCES {
		t.Fatalf("non-owner no-allow: checkAccess = %v, want EACCES", err)
	}

	// Same non-owner explicitly allowed → passes.
	if err := store.SetBucketPolicy(ctx, "test", metadata.BucketPolicy{
		Bucket: "test",
		Owner:  "app-server-1",
		Statements: []metadata.Statement{
			{Effect: "allow", Principal: "other-user", Permissions: []metadata.Permission{metadata.PermWrite}, Resource: "test"},
		},
		DefaultAccess: "deny",
	}); err != nil {
		t.Fatalf("SetBucketPolicy: %v", err)
	}
	if err := dfs.checkAccess("test", metadata.PermWrite); err != nil {
		t.Fatalf("explicit allow: checkAccess = %v, want nil", err)
	}

	// An empty owner means RBAC is not enforced (dev/local mounts).
	dfs.owner = ""
	if err := dfs.checkAccess("test", metadata.PermWrite); err != nil {
		t.Fatalf("empty owner: checkAccess = %v, want nil (RBAC off in dev)", err)
	}
}
