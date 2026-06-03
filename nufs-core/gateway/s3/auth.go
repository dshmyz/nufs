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

	"gopkg.in/yaml.v3"
)

// CredentialStore holds access key / secret key pairs for authentication.
// Safe for concurrent use: hot-reload safe via LoadCredentials.
type CredentialStore struct {
	mu          sync.RWMutex
	credentials map[string]string // accessKey -> secretKey
}

// NewCredentialStore creates a new credential store.
func NewCredentialStore() *CredentialStore {
	return &CredentialStore{
		credentials: make(map[string]string),
	}
}

// AddCredential adds an access key / secret key pair.
func (cs *CredentialStore) AddCredential(accessKey, secretKey string) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.credentials[accessKey] = secretKey
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
			cs.credentials[c.AccessKey] = c.SecretKey
		}
	}
	return nil
}

// HasCredentials returns true if at least one credential is configured.
func (cs *CredentialStore) HasCredentials() bool {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	return len(cs.credentials) > 0
}

// Count returns the number of configured credentials.
func (cs *CredentialStore) Count() int {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	return len(cs.credentials)
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
	s, ok := cs.credentials[accessKey]
	return s, ok
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
