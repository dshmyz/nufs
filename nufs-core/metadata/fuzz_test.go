package metadata

import (
	"context"
	"encoding/json"
	"testing"
)

// ============================================================
// Fuzz tests — metadata boundary inputs
// ============================================================

// FuzzBucketPolicyEval fuzzes the RBAC policy evaluation engine.
// It tests that IsAllowed never panics regardless of policy content
// or principal/permission inputs.
func FuzzBucketPolicyEval(f *testing.F) {
	// Seed: valid allow-all policy
	f.Add(`{"bucket":"b1","owner":"admin","statements":[{"effect":"allow","principal":"*","permissions":["read","write"],"resource":"b1"}],"default_access":"deny"}`,
		"user1", "read")
	// Seed: valid deny policy
	f.Add(`{"bucket":"b1","owner":"admin","statements":[{"effect":"deny","principal":"user1","permissions":["write"],"resource":"b1"}],"default_access":"allow"}`,
		"user1", "write")
	// Seed: empty policy
	f.Add(`{}`, "user1", "read")
	// Seed: malformed JSON
	f.Add(`{invalid json}`, "user1", "read")
	// Seed: null
	f.Add(`null`, "", "")
	// Seed: array instead of object
	f.Add(`[]`, "user1", "read")
	// Seed: statement with empty fields
	f.Add(`{"bucket":"","owner":"","statements":[{"effect":"","principal":"","permissions":null,"resource":""}],"default_access":""}`,
		"", "")
	// Seed: very long principal
	f.Add(`{"bucket":"b1","owner":"admin","statements":[{"effect":"allow","principal":"*","permissions":["read"],"resource":"b1"}],"default_access":"deny"}`,
		"user1", "read")

	f.Fuzz(func(t *testing.T, policyJSON, principalStr, permStr string) {
		// Limit input size to avoid OOM
		if len(policyJSON) > 64*1024 || len(principalStr) > 64*1024 {
			t.Skip()
		}

		var policy BucketPolicy
		if err := json.Unmarshal([]byte(policyJSON), &policy); err != nil {
			return // invalid JSON, skip
		}

		// IsAllowed should never panic
		_ = policy.IsAllowed(Principal(principalStr), Permission(permStr))
	})
}

// FuzzBucketName fuzzes bucket name handling with boundary inputs.
// It verifies that CreateBucket never panics on unusual names.
func FuzzBucketName(f *testing.F) {
	f.Add("valid-bucket-123")
	f.Add("")
	f.Add("a")
	f.Add(".")
	f.Add("a.b.c")
	f.Add("192.168.1.1")
	f.Add("bucket-with-dashes")
	f.Add("BUCKET-UPPERCASE")
	f.Add("bucket_with_underscore")
	f.Add("bucket with spaces")
	f.Add("bucket/with/slashes")
	f.Add("bucket..double-dots")

	f.Fuzz(func(t *testing.T, name string) {
		if len(name) > 256 {
			t.Skip()
		}

		store, err := NewPebbleStore(PebbleStoreConfig{Dir: t.TempDir()})
		if err != nil {
			t.Fatal(err)
		}
		defer store.Close()

		// CreateBucket should never panic
		_ = store.CreateBucket(context.Background(), name, PlacementPolicy{
			ReplicationFactor: 1,
		})
	})
}

// FuzzInodePath fuzzes inode path parsing with boundary inputs.
func FuzzInodePath(f *testing.F) {
	f.Add("/dir/file.txt")
	f.Add("/")
	f.Add("")
	f.Add("/a/b/c/d/e/f/g/h")
	f.Add("/dir with spaces/file")
	f.Add("/dir\x00null/file")
	f.Add("/../etc/passwd")
	f.Add("/dir/./file")
	f.Add("//double//slash")

	f.Fuzz(func(t *testing.T, path string) {
		if len(path) > 4096 {
			t.Skip()
		}
		// Path parsing should never panic
		_ = parseInodePath(path)
	})
}

// parseInodePath is a simple path splitter for fuzzing.
// This exercises the boundary conditions of path parsing.
func parseInodePath(path string) []string {
	if path == "" {
		return nil
	}
	var parts []string
	start := 0
	for i := 0; i < len(path); i++ {
		if path[i] == '/' {
			if i > start {
				parts = append(parts, path[start:i])
			}
			start = i + 1
		}
	}
	if start < len(path) {
		parts = append(parts, path[start:])
	}
	return parts
}
