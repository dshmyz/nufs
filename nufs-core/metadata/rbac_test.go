package metadata

import (
	"context"
	"testing"
)

func TestBucketPolicy_OwnerAlwaysAllowed(t *testing.T) {
	policy := &BucketPolicy{
		Bucket: "test",
		Owner:  "alice",
		Statements: []Statement{
			{Effect: "deny", Principal: PrincipalAll, Permissions: []Permission{PermRead}, Resource: "test"},
		},
	}
	// Owner should bypass deny
	if !policy.IsAllowed("alice", PermRead) {
		t.Error("owner should always be allowed, even with explicit deny")
	}
	if !policy.IsAllowed("alice", PermWrite) {
		t.Error("owner should have all permissions")
	}
	if !policy.IsAllowed("alice", PermAdmin) {
		t.Error("owner should have admin permission")
	}
}

func TestBucketPolicy_ExplicitDenyWinsOverAllow(t *testing.T) {
	policy := &BucketPolicy{
		Bucket: "test",
		Owner:  "admin",
		Statements: []Statement{
			{Effect: "allow", Principal: PrincipalAll, Permissions: []Permission{PermRead, PermWrite}, Resource: "test"},
			{Effect: "deny", Principal: "bob", Permissions: []Permission{PermWrite}, Resource: "test"},
		},
	}
	// Bob can read but not write
	if !policy.IsAllowed("bob", PermRead) {
		t.Error("bob should be allowed to read")
	}
	if policy.IsAllowed("bob", PermWrite) {
		t.Error("explicit deny should override allow for bob's write")
	}
	// Other users can read and write
	if !policy.IsAllowed("charlie", PermRead) {
		t.Error("charlie should be allowed to read")
	}
	if !policy.IsAllowed("charlie", PermWrite) {
		t.Error("charlie should be allowed to write")
	}
}

func TestBucketPolicy_DefaultDeny(t *testing.T) {
	policy := &BucketPolicy{
		Bucket:        "test",
		Owner:         "admin",
		Statements:    nil,
		DefaultAccess: "deny",
	}
	if policy.IsAllowed("bob", PermRead) {
		t.Error("should be denied by default")
	}
}

func TestBucketPolicy_DefaultAllow(t *testing.T) {
	policy := &BucketPolicy{
		Bucket:        "test",
		Owner:         "admin",
		Statements:    nil,
		DefaultAccess: "allow",
	}
	if !policy.IsAllowed("bob", PermRead) {
		t.Error("should be allowed by default access")
	}
}

func TestBucketPolicy_NilPolicy(t *testing.T) {
	var policy *BucketPolicy
	if policy.IsAllowed("bob", PermRead) {
		t.Error("nil policy should deny")
	}
}

func TestBucketPolicy_WildcardPrincipal(t *testing.T) {
	policy := &BucketPolicy{
		Bucket: "test",
		Owner:  "admin",
		Statements: []Statement{
			{Effect: "allow", Principal: PrincipalAll, Permissions: []Permission{PermRead}, Resource: "test"},
		},
		DefaultAccess: "deny",
	}
	if !policy.IsAllowed("anyone", PermRead) {
		t.Error("wildcard principal should match any user for read")
	}
	if policy.IsAllowed("anyone", PermWrite) {
		t.Error("write should be denied without explicit allow")
	}
}

func TestAccessController_CheckAccess(t *testing.T) {
	ac := NewAccessController()

	// No policy = deny
	if err := ac.CheckAccess("bucket1", "alice", PermRead); err == nil {
		t.Error("should deny when no policy exists")
	}

	// Set policy
	policy := &BucketPolicy{
		Bucket: "bucket1",
		Owner:  "alice",
		Statements: []Statement{
			{Effect: "allow", Principal: "bob", Permissions: []Permission{PermRead}, Resource: "bucket1"},
		},
		DefaultAccess: "deny",
	}
	ac.SetPolicy("bucket1", policy)

	// Owner always allowed
	if err := ac.CheckAccess("bucket1", "alice", PermWrite); err != nil {
		t.Error("owner should be allowed")
	}
	// Bob can read
	if err := ac.CheckAccess("bucket1", "bob", PermRead); err != nil {
		t.Error("bob should be allowed to read")
	}
	// Bob cannot write
	if err := ac.CheckAccess("bucket1", "bob", PermWrite); err == nil {
		t.Error("bob should be denied write")
	}
	// Unknown user denied
	if err := ac.CheckAccess("bucket1", "charlie", PermRead); err == nil {
		t.Error("charlie should be denied")
	}
}

func TestAccessController_CheckServiceAccess(t *testing.T) {
	ac := NewAccessController()

	// No policies = deny
	if err := ac.CheckServiceAccess("alice", PermList); err == nil {
		t.Error("should deny service access when no policies exist")
	}

	// Add policy with list permission
	policy := &BucketPolicy{
		Bucket: "bucket1",
		Owner:  "admin",
		Statements: []Statement{
			{Effect: "allow", Principal: "alice", Permissions: []Permission{PermList}, Resource: "bucket1"},
		},
	}
	ac.SetPolicy("bucket1", policy)

	if err := ac.CheckServiceAccess("alice", PermList); err != nil {
		t.Error("alice should have list access")
	}
	if err := ac.CheckServiceAccess("bob", PermList); err == nil {
		t.Error("bob should not have list access")
	}
}

func TestAccessController_DeletePolicy(t *testing.T) {
	ac := NewAccessController()
	policy := &BucketPolicy{
		Bucket: "bucket1",
		Owner:  "admin",
		Statements: []Statement{
			{Effect: "allow", Principal: "alice", Permissions: []Permission{PermRead}, Resource: "bucket1"},
		},
	}
	ac.SetPolicy("bucket1", policy)
	ac.DeletePolicy("bucket1")

	if ac.GetPolicy("bucket1") != nil {
		t.Error("policy should be deleted")
	}
}

func TestAccessController_OwnerOf(t *testing.T) {
	ac := NewAccessController()
	if owner := ac.OwnerOf("bucket1"); owner != "" {
		t.Error("unknown bucket should have empty owner")
	}

	policy := &BucketPolicy{Bucket: "bucket1", Owner: "alice"}
	ac.SetPolicy("bucket1", policy)
	if owner := ac.OwnerOf("bucket1"); owner != "alice" {
		t.Errorf("expected alice, got %s", owner)
	}
}

func TestPebbleStore_BucketPolicy(t *testing.T) {
	store := newTestPebbleStore(t)
	ctx := context.Background()

	// No policy = ErrAccessDenied
	_, err := store.GetBucketPolicy(ctx, "nobucket")
	if err != ErrAccessDenied {
		t.Errorf("expected ErrAccessDenied, got %v", err)
	}

	// Set policy
	policy := BucketPolicy{
		Owner:  "alice",
		Statements: []Statement{
			{Effect: "allow", Principal: "bob", Permissions: []Permission{PermRead}, Resource: "mybucket"},
		},
		DefaultAccess: "deny",
	}
	if err := store.SetBucketPolicy(ctx, "mybucket", policy); err != nil {
		t.Fatalf("SetBucketPolicy: %v", err)
	}

	// Get policy
	got, err := store.GetBucketPolicy(ctx, "mybucket")
	if err != nil {
		t.Fatalf("GetBucketPolicy: %v", err)
	}
	if got.Owner != "alice" {
		t.Errorf("expected owner alice, got %s", got.Owner)
	}
	if len(got.Statements) != 1 {
		t.Errorf("expected 1 statement, got %d", len(got.Statements))
	}

	// Delete policy
	if err := store.DeleteBucketPolicy(ctx, "mybucket"); err != nil {
		t.Fatalf("DeleteBucketPolicy: %v", err)
	}

	// Verify deleted
	_, err = store.GetBucketPolicy(ctx, "mybucket")
	if err != ErrAccessDenied {
		t.Errorf("expected ErrAccessDenied after delete, got %v", err)
	}
}
