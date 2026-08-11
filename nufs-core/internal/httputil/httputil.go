// Package httputil provides HTTP middleware utilities for NUFS services.
package httputil

import (
	"crypto/subtle"
	"log/slog"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// BearerTokenOK verifies an Authorization header against the expected bearer
// token using a constant-time comparison. Empty expected tokens never match.
func BearerTokenOK(header, expected string) bool {
	if expected == "" {
		return false
	}
	const prefix = "Bearer "
	if !strings.HasPrefix(header, prefix) {
		return false
	}
	got := strings.TrimSpace(strings.TrimPrefix(header, prefix))
	if len(got) != len(expected) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(got), []byte(expected)) == 1
}

// extractBearer returns the token portion of an "Authorization: Bearer <tok>"
// header, or "" if the header is absent or malformed.
func extractBearer(header string) string {
	const prefix = "Bearer "
	if !strings.HasPrefix(header, prefix) {
		return ""
	}
	return strings.TrimSpace(strings.TrimPrefix(header, prefix))
}

// BearerAuth wraps next with bearer-token authentication. It accepts two
// credential forms, which are deliberately NOT equivalent in scope:
//
//   - Simple bearer (the raw --auth-token value): the operator credential.
//     Grants every non-public route.
//   - Signed token (from metadata.SignToken): a mount credential, issued to
//     whoever holds an accessKey/secretKey. Grants ONLY the data-plane routes
//     matched by dataPlaneRoute.
//
// The split matters because metad serves the data plane and the operator API
// off one mux. Accepting a mount token everywhere would let any application
// credential enumerate and rewrite the credential registry
// (/api/v1/auth/creds/), drain nodes, or trigger a rebalance — i.e. turn a
// mount secret into an admin secret. A mount only ever needs namespace, chunk,
// inode, lock and bucket-read calls, so signed tokens are confined to those.
//
// Requests whose path is listed in publicPaths bypass authentication entirely.
// signingKeys is tried in order, so a caller can pass (current, previous) to
// keep honoring tokens minted before a key rotation. Pass no keys to disable
// signed-token verification.
//
// All path decisions here are made on the CLEANED path (see cleanPath), which
// is what http.ServeMux routes on. Deciding on the raw r.URL.Path instead would
// make this a confused deputy: "/api/v1/namespace/../auth/creds/x" looks like a
// permitted namespace route to a raw-prefix check but routes to the credential
// registry.
func BearerAuth(token string, signingKeys []string, publicPaths map[string]struct{}, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		routedPath := cleanPath(r.URL.Path)
		if _, ok := publicPaths[routedPath]; ok {
			next.ServeHTTP(w, r)
			return
		}
		bearer := extractBearer(r.Header.Get("Authorization"))
		if bearer == "" {
			slog.Warn("auth rejected: missing bearer", "remote", r.RemoteAddr, "path", routedPath)
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		// Operator credential: full access.
		if token != "" && subtle.ConstantTimeCompare([]byte(bearer), []byte(token)) == 1 {
			next.ServeHTTP(w, r)
			return
		}
		// Mount credential: signed token (HMAC + TTL), current key first.
		// Confined to the data plane — see the doc comment above.
		if len(signingKeys) > 0 {
			if claims, err := metadata.ParseTokenAny(bearer, signingKeys...); err == nil {
				if dataPlaneRoute(r.Method, routedPath) {
					next.ServeHTTP(w, r)
					return
				}
				slog.Warn("auth rejected: mount token used on an operator route",
					"remote", r.RemoteAddr, "path", routedPath, "principal", claims.Principal)
				http.Error(w, "Forbidden", http.StatusForbidden)
				return
			}
		}
		slog.Warn("auth rejected", "remote", r.RemoteAddr, "path", routedPath)
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
	})
}

// cleanPath returns the path http.ServeMux will route on: "." and ".."
// segments resolved and duplicate slashes collapsed. Authorization must be
// decided on this value, never on the raw request path — otherwise a traversal
// segment lets a request be authorized as one route and served as another.
func cleanPath(p string) string {
	if p == "" {
		return "/"
	}
	if p[0] != '/' {
		p = "/" + p
	}
	cleaned := path.Clean(p)
	// path.Clean strips a meaningful trailing slash; ServeMux keeps it for
	// subtree matches, so restore it.
	if strings.HasSuffix(p, "/") && !strings.HasSuffix(cleaned, "/") {
		cleaned += "/"
	}
	return cleaned
}

// dataPlanePrefixes are the route prefixes a mount legitimately needs to serve
// filesystem traffic with full read/write method access. Everything absent from
// this list — the credential registry, node lifecycle, backups, rebalance/repair
// triggers, audit, the admin SPA — is operator-only and requires the static
// --auth-token.
//
// This is an allowlist on purpose: a new operator route added later is closed
// to mount tokens by default, whereas a denylist would silently expose it.
//
// Bucket and ACL routes are deliberately NOT here: a mount only ever READS
// bucket metadata, quota, and the bucket policy (for its own access check). The
// mutating calls — CreateBucket, DeleteBucket, SetBucketQuota, and crucially
// SetBucketPolicy/DeleteBucketPolicy — are operator-only. Letting a signed mount
// token reach PUT/DELETE on /api/v1/acl/ or /api/v1/buckets would let any
// application credential rewrite the very policy that authorizes it (or delete a
// bucket out from under other mounts), turning a mount secret into an admin
// secret. Those routes live in dataPlaneReadOnlyPrefixes below.
var dataPlanePrefixes = []string{
	"/api/v1/namespace/",
	"/api/v1/inodes/",
	"/api/v1/chunks",
	"/api/v1/ec/",
	"/api/v1/locks",
}

// dataPlaneReadOnly are routes a mount may only READ. /api/v1/nodes is matched
// exactly and restricted to GET: the mount lists datanodes to reach them for
// chunk I/O, but POST on the same path is RegisterNode (datanodes register with
// the static --metadata-auth-token, never a mount token), and the
// /api/v1/nodes/{id}/ subtree carries decommission and restore.
var dataPlaneReadOnly = map[string]struct{}{
	"/api/v1/nodes": {},
}

// dataPlaneReadOnlyPrefixes are route prefixes a mount may only READ (GET/HEAD).
// /api/v1/buckets and /api/v1/acl/ cover the mount's legitimate needs —
// GetBucket, GetBucketByRoot, GetBucketQuota, GetBucketUsage (for statfs), and
// GetBucketPolicy (the access check the FUSE gateway performs) — while blocking
// the mutating CreateBucket/DeleteBucket/SetBucketQuota and
// SetBucketPolicy/DeleteBucketPolicy calls that would let a mount escalate.
var dataPlaneReadOnlyPrefixes = []string{
	"/api/v1/buckets",
	"/api/v1/acl/",
}

// dataPlaneRoute reports whether method+path is reachable with a signed mount
// token.
func dataPlaneRoute(method, path string) bool {
	if _, ok := dataPlaneReadOnly[path]; ok {
		return method == http.MethodGet || method == http.MethodHead
	}
	for _, p := range dataPlaneReadOnlyPrefixes {
		if strings.HasPrefix(path, p) {
			return method == http.MethodGet || method == http.MethodHead
		}
	}
	for _, p := range dataPlanePrefixes {
		if strings.HasPrefix(path, p) {
			return true
		}
	}
	return false
}

// threshold. It wraps the next handler and emits a structured warning when a
// request takes longer than threshold.
//
// Usage:
//
//	mux := http.NewServeMux()
//	mux.HandleFunc("/api/buckets", handler)
//	srv := &http.Server{Handler: httputil.SlowRequestLogger(mux, 200*time.Millisecond)}
func SlowRequestLogger(next http.Handler, threshold time.Duration) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		elapsed := time.Since(start)

		if elapsed > threshold {
			slog.Warn("http: slow request",
				"method", r.Method,
				"path", r.URL.Path,
				"remote", r.RemoteAddr,
				"duration", elapsed,
				"threshold", threshold,
			)
		}
	})
}
