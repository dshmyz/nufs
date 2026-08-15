package metadata

import (
	"context"
	"sync"
)

// ============================================================
// RBAC — Bucket-level Access Control
// ============================================================

// Permission represents an action that can be performed on a bucket.
type Permission string

const (
	// PermRead allows GetObject, ListObjects, HeadObject.
	PermRead Permission = "read"
	// PermWrite allows PutObject, DeleteObject, CreateMultipartUpload.
	PermWrite Permission = "write"
	// PermAdmin allows DeleteBucket, SetPolicy, GetPolicy.
	PermAdmin Permission = "admin"
	// PermList allows ListBuckets at the service level.
	PermList Permission = "list"
)

// Principal identifies who is making a request. It can be an
// access key, a wildcard "*", or a canonical user ID.
type Principal string

const (
	// PrincipalAll represents any authenticated or anonymous user.
	PrincipalAll Principal = "*"
	// PrincipalAnonymous represents unauthenticated access.
	PrincipalAnonymous Principal = "anonymous"
)

// Statement is a single permission grant or denial.
type Statement struct {
	// Effect is "allow" or "deny". Deny takes precedence over Allow.
	Effect string `json:"effect"`
	// Principal is the access key or "*" for all users.
	Principal Principal `json:"principal"`
	// Permissions are the actions being granted or denied.
	Permissions []Permission `json:"permissions"`
	// Resource is the bucket name. Empty means "all buckets".
	Resource string `json:"resource"`
}

// BucketPolicy is a collection of statements that define access
// control for a bucket. It follows the S3 bucket policy model:
// explicit deny > explicit allow > default deny.
type BucketPolicy struct {
	// Bucket is the bucket this policy applies to.
	Bucket string `json:"bucket"`
	// Owner is the access key that created the bucket.
	Owner string `json:"owner"`
	// Statements are the policy rules, evaluated in order.
	// Deny statements take precedence over Allow.
	Statements []Statement `json:"statements"`
	// DefaultAccess controls the default permission for authenticated
	// users when no explicit statement matches. "deny" (default) or "allow".
	DefaultAccess string `json:"default_access"`
}

// IsAllowed checks whether a principal has the given permission on the bucket.
// Evaluation order: explicit deny > explicit allow > default deny.
func (p *BucketPolicy) IsAllowed(principal Principal, perm Permission) bool {
	if p == nil {
		return false
	}

	// Bucket owner always has all permissions
	if string(principal) == p.Owner {
		return true
	}

	hasAllow := false
	for _, stmt := range p.Statements {
		if !stmtMatches(stmt, principal, perm) {
			continue
		}
		if stmt.Effect == "deny" {
			return false // explicit deny wins
		}
		if stmt.Effect == "allow" {
			hasAllow = true
		}
	}

	if hasAllow {
		return true
	}

	// Default: if DefaultAccess is "allow", permit; otherwise deny
	return p.DefaultAccess == "allow"
}

func stmtMatches(stmt Statement, principal Principal, perm Permission) bool {
	// Check principal match
	if stmt.Principal != PrincipalAll && stmt.Principal != principal {
		return false
	}
	// Check permission match
	for _, p := range stmt.Permissions {
		if p == perm {
			return true
		}
	}
	return false
}

// AccessController manages bucket policies and performs authorization checks.
type AccessController struct {
	mu       sync.RWMutex
	policies map[string]*BucketPolicy // bucket -> policy
}

// NewAccessController creates a new AccessController.
func NewAccessController() *AccessController {
	return &AccessController{
		policies: make(map[string]*BucketPolicy),
	}
}

// SetPolicy sets the access policy for a bucket.
func (ac *AccessController) SetPolicy(bucket string, policy *BucketPolicy) {
	ac.mu.Lock()
	defer ac.mu.Unlock()
	policy.Bucket = bucket
	ac.policies[bucket] = policy
}

// GetPolicy returns the access policy for a bucket.
func (ac *AccessController) GetPolicy(bucket string) *BucketPolicy {
	ac.mu.RLock()
	defer ac.mu.RUnlock()
	return ac.policies[bucket]
}

// DeletePolicy removes the access policy for a bucket.
func (ac *AccessController) DeletePolicy(bucket string) {
	ac.mu.Lock()
	defer ac.mu.Unlock()
	delete(ac.policies, bucket)
}

// CheckAccess checks whether a principal has the given permission on a bucket.
// Returns nil if allowed, ErrAccessDenied if denied.
func (ac *AccessController) CheckAccess(bucket string, principal Principal, perm Permission) error {
	ac.mu.RLock()
	policy := ac.policies[bucket]
	ac.mu.RUnlock()

	if policy == nil {
		// No policy = default deny (except owner, which is handled by the caller)
		return ErrAccessDenied
	}

	if policy.IsAllowed(principal, perm) {
		return nil
	}
	return ErrAccessDenied
}

// CheckServiceAccess checks service-level permissions (e.g., ListBuckets).
// A principal with PermList on any bucket or a wildcard allow can list buckets.
func (ac *AccessController) CheckServiceAccess(principal Principal, perm Permission) error {
	ac.mu.RLock()
	defer ac.mu.RUnlock()

	for _, policy := range ac.policies {
		if policy.IsAllowed(principal, perm) {
			return nil
		}
	}
	return ErrAccessDenied
}

// OwnerOf returns the owner of the given bucket, or empty string if unknown.
func (ac *AccessController) OwnerOf(bucket string) string {
	ac.mu.RLock()
	defer ac.mu.RUnlock()
	if p, ok := ac.policies[bucket]; ok {
		return p.Owner
	}
	return ""
}

// AccessControlService is the interface for persisting bucket policies.
// This is added to MetadataService so policies can be stored in Pebble.
type AccessControlService interface {
	SetBucketPolicy(ctx context.Context, bucket string, policy BucketPolicy) error
	GetBucketPolicy(ctx context.Context, bucket string) (*BucketPolicy, error)
	DeleteBucketPolicy(ctx context.Context, bucket string) error
}
