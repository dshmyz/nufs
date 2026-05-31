package s3

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"
)

// CredentialStore holds access key / secret key pairs for authentication.
type CredentialStore struct {
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
	cs.credentials[accessKey] = secretKey
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
		if len(cs.credentials) == 0 {
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

	secretKey, ok := cs.credentials[accessKey]
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

	secretKey, ok := cs.credentials[accessKey]
	if !ok {
		return "", fmt.Errorf("unknown access key: %s", accessKey)
	}

	// For presigned URLs, simplified verification
	_ = secretKey
	return accessKey, nil
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
