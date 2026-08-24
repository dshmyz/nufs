package s3

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/dshmyz/nufs/nufs-core/metadata"
	"gopkg.in/yaml.v3"
)

// gatewayCredential is one entry of the in-memory credential table: the
// plaintext secret SigV4 verification needs, plus the RBAC principal bound to
// the access key (from the metad credential registry; defaults to the access
// key itself for locally-added credentials).
type gatewayCredential struct {
	secretKey string
	principal string
}

// CredentialStore holds access key / secret key pairs for authentication.
// Safe for concurrent use: hot-reload safe via LoadCredentials and
// ReplaceAll (the metad credential-sync path).
type CredentialStore struct {
	mu          sync.RWMutex
	credentials map[string]gatewayCredential // accessKey -> credential
	// authMode, once enabled (metad registry sync authoritative), keeps the
	// gateway in verify-signatures mode even when the registry is temporarily
	// empty. Without it, revoking the last credential would flip the gateway
	// back to anonymous (no auth at all) — the opposite of revocation.
	authMode bool
}

// NewCredentialStore creates a new credential store.
func NewCredentialStore() *CredentialStore {
	return &CredentialStore{
		credentials: make(map[string]gatewayCredential),
	}
}

// AddCredential adds an access key / secret key pair. The RBAC principal
// defaults to the access key.
func (cs *CredentialStore) AddCredential(accessKey, secretKey string) {
	cs.AddCredentialWithPrincipal(accessKey, secretKey, accessKey)
}

// AddCredentialWithPrincipal adds an access key / secret key pair with an
// explicit RBAC principal.
func (cs *CredentialStore) AddCredentialWithPrincipal(accessKey, secretKey, principal string) {
	if principal == "" {
		principal = accessKey
	}
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.credentials[accessKey] = gatewayCredential{secretKey: secretKey, principal: principal}
}

// ReplaceAll atomically replaces the entire credential table with the given
// set — a full swap, the source of truth for the metad credential sync.
// An empty list clears the table (returns the gateway to anonymous mode).
func (cs *CredentialStore) ReplaceAll(creds []metadata.GatewayCredential) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	clear(cs.credentials)
	for _, c := range creds {
		if c.AccessKey == "" || c.SecretKey == "" {
			continue
		}
		principal := c.Principal
		if principal == "" {
			principal = c.AccessKey
		}
		cs.credentials[c.AccessKey] = gatewayCredential{secretKey: c.SecretKey, principal: principal}
	}
}

// PrincipalFor returns the RBAC principal bound to an access key, falling
// back to the access key itself when the key is unknown (or anonymous).
func (cs *CredentialStore) PrincipalFor(accessKey string) string {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	if c, ok := cs.credentials[accessKey]; ok && c.principal != "" {
		return c.principal
	}
	return accessKey
}

// Count returns the number of configured credentials.
func (cs *CredentialStore) Count() int {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	return len(cs.credentials)
}

// credentialFile is the YAML format for LoadCredentials.
type credentialFile struct {
	Credentials []credentialPair `yaml:"credentials"`
}

type credentialPair struct {
	AccessKey string `yaml:"access-key"`
	SecretKey string `yaml:"secret-key"`
}

// LoadCredentials reads a YAML credentials file and replaces the current set.
// Multiple callers can safely reload the same store at runtime.
func (cs *CredentialStore) LoadCredentials(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read credentials: %w", err)
	}
	var cf credentialFile
	if err := yaml.Unmarshal(data, &cf); err != nil {
		return fmt.Errorf("parse credentials: %w", err)
	}
	cs.mu.Lock()
	defer cs.mu.Unlock()
	clear(cs.credentials)
	for _, c := range cf.Credentials {
		if c.AccessKey != "" && c.SecretKey != "" {
			cs.credentials[c.AccessKey] = gatewayCredential{secretKey: c.SecretKey, principal: c.AccessKey}
		}
	}
	return nil
}

// AuthPosture classifies the gateway's boot-time auth posture after the
// credential source is resolved. Startup logging derives its wording from it
// so operators never mistake a locked-shut gateway for an open one.
type AuthPosture int

const (
	// AuthPostureSynced: the metad registry sync is authoritative and
	// returned at least one credential.
	AuthPostureSynced AuthPosture = iota
	// AuthPostureSyncedEmpty: the registry sync succeeded but returned zero
	// credentials (fresh registry, or a rotated --credential-secret-key on
	// metad that makes every stored secret undecryptable). Auth is pinned
	// on — every request is rejected with 403 — so this must never be
	// reported as "anonymous mode".
	AuthPostureSyncedEmpty
	// AuthPostureLocal: the registry sync was not configured or failed at
	// boot; credentials came from the legacy local sources.
	AuthPostureLocal
	// AuthPostureAnonymous: no credential source produced anything; the
	// gateway serves unauthenticated.
	AuthPostureAnonymous
)

// StartupAuthPosture classifies the boot-time auth posture from whether the
// metad registry is authoritative and how many credentials it produced.
func StartupAuthPosture(syncAuthoritative bool, credentialCount int) AuthPosture {
	switch {
	case syncAuthoritative && credentialCount > 0:
		return AuthPostureSynced
	case syncAuthoritative:
		return AuthPostureSyncedEmpty
	case credentialCount > 0:
		return AuthPostureLocal
	default:
		return AuthPostureAnonymous
	}
}

// String returns a log-safe description of the posture.
func (p AuthPosture) String() string {
	switch p {
	case AuthPostureSynced:
		return "auth enabled (metad registry sync)"
	case AuthPostureSyncedEmpty:
		return "auth pinned on (metad registry sync) but zero credentials — every request will be rejected (403)"
	case AuthPostureLocal:
		return "auth enabled (local credentials)"
	case AuthPostureAnonymous:
		return "running in anonymous mode (no auth)"
	default:
		return "auth posture unknown"
	}
}

// HasCredentials returns true if at least one credential is configured.
func (cs *CredentialStore) HasCredentials() bool {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	return len(cs.credentials) > 0
}

// SetAuthMode pins the gateway into verify-signatures mode (used by the metad
// registry sync once it is authoritative). After a successful sync, auth stays
// on even if the registry is emptied by revocations; only a process without a
// credential source ever sees the anonymous (no-auth) mode.
func (cs *CredentialStore) SetAuthMode(enabled bool) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.authMode = enabled
}

// AuthEnabled reports whether the gateway requires signatures: either authMode
// is pinned, or at least one credential is configured. It is what route() and
// the admin handlers gate on, replacing the bare HasCredentials() check so
// "all credentials revoked" means "every request is rejected", never "auth is
// off".
func (cs *CredentialStore) AuthEnabled() bool {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	return cs.authMode || len(cs.credentials) > 0
}

// VerifySignatureV4 verifies AWS Signature Version 4.
// Returns the access key if valid, empty string if auth is disabled/missing.
func (cs *CredentialStore) VerifySignatureV4(r *http.Request) (string, error) {
	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		// Check for query-string auth (pre-signed URLs)
		if r.URL.Query().Get("X-Amz-Algorithm") != "" {
			return cs.verifyPresignedURL(r)
		}
		// No auth — allow if no credentials configured (anonymous mode)
		if !cs.HasCredentials() {
			return "anonymous", nil
		}
		return "", fmt.Errorf("missing Authorization header")
	}

	// Parse: AWS4-HMAC-SHA256 Credential=AK/..., SignedHeaders=..., Signature=...
	if !strings.HasPrefix(authHeader, "AWS4-HMAC-SHA256 ") {
		return "", fmt.Errorf("unsupported auth scheme")
	}

	parts := parseAuthHeader(authHeader)
	accessKey := parts["accessKey"]
	region := parts["region"]
	service := parts["service"]
	signedHeaders := parts["signedHeaders"]
	providedSig := parts["signature"]

	if accessKey == "" || providedSig == "" {
		return "", fmt.Errorf("invalid Authorization header")
	}

	secretKey, ok := cs.getCredential(accessKey)
	if !ok {
		return "", fmt.Errorf("unknown access key: %s", accessKey)
	}

	// Get the date from headers
	dateStr := r.Header.Get("X-Amz-Date")
	if dateStr == "" {
		dateStr = r.Header.Get("Date")
	}
	dateStamp := dateStr[:8] // YYYYMMDD

	// Build canonical request
	canonicalHeaders := buildCanonicalHeaders(r, signedHeaders)
	payloadHash := r.Header.Get("X-Amz-Content-Sha256")
	if payloadHash == "" {
		payloadHash = "UNSIGNED-PAYLOAD"
	}

	canonicalRequest := buildCanonicalRequest(r, canonicalHeaders, signedHeaders, payloadHash)

	// Build string to sign
	scope := fmt.Sprintf("%s/%s/%s/aws4_request", dateStamp, region, service)
	stringToSign := fmt.Sprintf("AWS4-HMAC-SHA256\n%s\n%s\n%s",
		dateStr, scope, hashSHA256(canonicalRequest))

	// Calculate signature
	signingKey := deriveSigningKey(secretKey, dateStamp, region, service)
	expectedSig := hex.EncodeToString(hmacSHA256(signingKey, stringToSign))

	if !hmac.Equal([]byte(providedSig), []byte(expectedSig)) {
		return "", fmt.Errorf("signature mismatch")
	}

	return accessKey, nil
}

func (cs *CredentialStore) verifyPresignedURL(r *http.Request) (string, error) {
	q := r.URL.Query()
	credential := q.Get("X-Amz-Credential")
	if credential == "" {
		return "", fmt.Errorf("missing X-Amz-Credential")
	}

	parts := strings.Split(credential, "/")
	if len(parts) < 2 {
		return "", fmt.Errorf("invalid X-Amz-Credential format")
	}
	accessKey := parts[0]

	_, ok := cs.getCredential(accessKey)
	if !ok {
		return "", fmt.Errorf("unknown access key: %s", accessKey)
	}

	// For presigned URLs, simplified verification
	return accessKey, nil
}

func (cs *CredentialStore) getCredential(accessKey string) (string, bool) {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	c, ok := cs.credentials[accessKey]
	return c.secretKey, ok
}

// parseAuthHeader parses the AWS Signature V4 Authorization header.
func parseAuthHeader(header string) map[string]string {
	result := make(map[string]string)

	// Remove prefix
	header = strings.TrimPrefix(header, "AWS4-HMAC-SHA256 ")

	for _, part := range strings.Split(header, ", ") {
		part = strings.TrimSpace(part)
		if strings.HasPrefix(part, "Credential=") {
			cred := strings.TrimPrefix(part, "Credential=")
			credParts := strings.Split(cred, "/")
			if len(credParts) >= 4 {
				result["accessKey"] = credParts[0]
				result["region"] = credParts[2]
				result["service"] = credParts[3]
			}
		} else if strings.HasPrefix(part, "SignedHeaders=") {
			result["signedHeaders"] = strings.TrimPrefix(part, "SignedHeaders=")
		} else if strings.HasPrefix(part, "Signature=") {
			result["signature"] = strings.TrimPrefix(part, "Signature=")
		}
	}
	return result
}

func buildCanonicalHeaders(r *http.Request, signedHeaders string) string {
	headers := strings.Split(signedHeaders, ";")
	sort.Strings(headers)

	var builder strings.Builder
	for _, h := range headers {
		h = strings.ToLower(strings.TrimSpace(h))
		var val string
		if h == "host" {
			val = r.Host
		} else {
			val = r.Header.Get(h)
		}
		builder.WriteString(h)
		builder.WriteString(":")
		builder.WriteString(strings.TrimSpace(val))
		builder.WriteString("\n")
	}
	return builder.String()
}

func buildCanonicalRequest(r *http.Request, canonicalHeaders, signedHeaders, payloadHash string) string {
	return strings.Join([]string{
		r.Method,
		r.URL.Path,
		r.URL.RawQuery,
		canonicalHeaders,
		signedHeaders,
		payloadHash,
	}, "\n")
}

func deriveSigningKey(secretKey, dateStamp, region, service string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+secretKey), dateStamp)
	kRegion := hmacSHA256(kDate, region)
	kService := hmacSHA256(kRegion, service)
	kSigning := hmacSHA256(kService, "aws4_request")
	return kSigning
}

func hmacSHA256(key []byte, data string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(data))
	return h.Sum(nil)
}

func hashSHA256(data string) string {
	h := sha256.Sum256([]byte(data))
	return hex.EncodeToString(h[:])
}

// ParseAmzDate parses the X-Amz-Date header value.
func ParseAmzDate(dateStr string) (time.Time, error) {
	return time.Parse("20060102T150405Z", dateStr)
}
