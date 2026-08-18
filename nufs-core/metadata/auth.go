package metadata

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// ============================================================
// Authentication authority — credential registry + token signing
// ============================================================
//
// metad is the single authentication authority for the DFS data plane
// (and, in a later phase, the S3 gateway). A mount authenticates by
// presenting an accessKey/secretKey, and — if they match a registered
// credential — receives a short-lived, principal-bound, bucket-scoped
// signed token (a bearer it then presents on every metadata request).
// The principal bound into the token is what the FUSE gateway uses for
// bucket-policy authorization, closing the old bare --owner hole (where
// claiming a principal name required no secret at all).

// Credential is a registered access key in the metad credential registry.
// SecretKey is stored only as a salted hash, never in the clear.
type Credential struct {
	// AccessKey is the public identifier a client authenticates with.
	AccessKey string `json:"access_key"`
	// Principal is the RBAC principal this credential is bound to. It
	// defaults to Principal(accessKey) but can be overridden (e.g. to
	// bind a single application credential to a human-named principal).
	Principal Principal `json:"principal"`
	// SecretHash is a salted SHA-256 hash of SecretKey ("salt$hexhash").
	// A salted hash is used (rather than bcrypt) so the authentication
	// authority stays free of new module dependencies; access secrets are
	// high-entropy, so the slow-hashing property of bcrypt is not the
	// deciding factor here, and the constant-time compare in Authenticate
	// protects the token endpoint against timing side channels.
	SecretHash string `json:"secret_hash"`
	// SecretCiphertext is the same secret sealed with the credential
	// encryption key (AES-256-GCM, nonce||ct+tag, see SealSecret). It is
	// kept alongside the hash for consumers that mathematically need the
	// plaintext secret — the S3 gateway's SigV4 verification derives its
	// signing key from the raw secret (gateway/s3/auth.go) — and is only
	// ever released through the ops-authenticated /api/v1/auth/credentials
	// endpoint, never through the public token exchange. Empty for
	// credentials registered before the key was configured: they keep
	// working for token exchange (hash path) but are not visible to the
	// S3 gateway.
	SecretCiphertext []byte `json:"secret_ciphertext,omitempty"`
}

// CredentialService is the interface for persisting access credentials.
// It mirrors AccessControlService: implemented by PebbleStore and
// ShardedStore so the ops API serves either wiring.
type CredentialService interface {
	PutCredential(ctx context.Context, cred Credential) error
	GetCredential(ctx context.Context, accessKey string) (*Credential, error)
	DeleteCredential(ctx context.Context, accessKey string) error
	ListCredentials(ctx context.Context) ([]Credential, error)
	// Authenticate verifies accessKey/secretKey against the registry and
	// returns the bound principal. Returns ErrAccessDenied on mismatch.
	Authenticate(ctx context.Context, accessKey, secretKey string) (Principal, error)
}

var _ CredentialService = (*PebbleStore)(nil)

// ========== Credential registry (PebbleStore) ==========

// PutCredential stores a credential keyed by access key.
func (s *PebbleStore) PutCredential(_ context.Context, cred Credential) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}
	if cred.AccessKey == "" || cred.SecretHash == "" {
		return ErrInvalidArgument
	}
	if cred.Principal == "" {
		cred.Principal = Principal(cred.AccessKey)
	}
	return s.applyBatchMsgpack([]batchOp{
		{Key: prefixCredential + cred.AccessKey, Value: &cred},
	}, nil)
}

// GetCredential retrieves a credential by access key.
func (s *PebbleStore) GetCredential(_ context.Context, accessKey string) (*Credential, error) {
	if s.closed.Load() {
		return nil, ErrServiceClosed
	}
	var cred Credential
	exists, err := s.getValue(prefixCredential+accessKey, &cred)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, ErrAccessDenied
	}
	return &cred, nil
}

// DeleteCredential removes a credential by access key.
func (s *PebbleStore) DeleteCredential(_ context.Context, accessKey string) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}
	return s.applyBatchMsgpack(nil, []string{prefixCredential + accessKey})
}

// ListCredentials returns every registered credential.
func (s *PebbleStore) ListCredentials(_ context.Context) ([]Credential, error) {
	if s.closed.Load() {
		return nil, ErrServiceClosed
	}
	var creds []Credential
	err := s.scanPrefix(prefixCredential, func(_ []byte, val []byte) error {
		var cred Credential
		if err := unmarshalValue(val, &cred); err != nil {
			return err
		}
		creds = append(creds, cred)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return creds, nil
}

// Authenticate verifies a secret against the stored hash and returns the
// bound principal. Comparison is constant-time to avoid a timing oracle on
// the hash. Wrong or missing credentials return ErrAccessDenied.
func (s *PebbleStore) Authenticate(_ context.Context, accessKey, secretKey string) (Principal, error) {
	if s.closed.Load() {
		return "", ErrServiceClosed
	}
	cred, err := s.GetCredential(context.Background(), accessKey)
	if err != nil {
		// Only an explicitly missing credential is a client error (401). A real
		// store/decoding failure is a server-side availability problem and must
		// surface as a 5xx — folding it into ErrAccessDenied would record true
		// outages as 4xx and keep the 5xx availability SLI green.
		if errors.Is(err, ErrAccessDenied) {
			return "", ErrAccessDenied
		}
		return "", fmt.Errorf("authenticate: read credential: %w", err)
	}
	if !verifySecretHash(cred.SecretHash, secretKey) {
		return "", ErrAccessDenied
	}
	return cred.Principal, nil
}

// GatewayCredential is one entry of the S3 gateway credential sync: the
// plaintext secret + bound principal as released by metad's
// /api/v1/auth/credentials endpoint (ops-authenticated). It is shared between
// the metadata HTTP client (fetch side) and the S3 gateway's CredentialStore
// (consume side).
type GatewayCredential struct {
	AccessKey string `json:"access_key"`
	SecretKey string `json:"secret_key"`
	Principal string `json:"principal"`
}

// ========== Secret hashing (stdlib-only, high-entropy secrets) ==========

// hashSecret derives a salted SHA-256 hash ("salt$hexhash") of a secret. The
// salt is 16 random bytes; the hash is HMAC-SHA256(salt -> secret) so that even
// two identical secrets produce distinct stored values.
func hashSecret(secret string) (string, error) {
	salt := make([]byte, 16)
	if _, err := rand.Read(salt); err != nil {
		return "", fmt.Errorf("auth: generate salt: %w", err)
	}
	mac := hmac.New(sha256.New, salt)
	mac.Write([]byte(secret))
	sum := mac.Sum(nil)
	return fmt.Sprintf("%x$%x", salt, sum), nil
}

// HashSecret exposes salted hashing for callers outside the metadata package
// (e.g. the metad ops server seeding a credential via --dev-seed-cred). The
// result is format-compatible with the Authenticate hash check.
func HashSecret(secret string) (string, error) {
	return hashSecret(secret)
}

// ========== Secret sealing (AES-256-GCM, stdlib-only) ==========
//
// The credential registry keeps secrets as salted hashes for authentication,
// but SigV4 verification on the S3 gateway needs the plaintext secret to
// derive signing keys. Rather than storing plaintext, metad stores the secret
// sealed with a dedicated credential encryption key (--credential-secret-key,
// never shared with clients) and releases it only through the
// ops-authenticated gateway-credentials endpoint.

// SealSecret encrypts a secret for at-rest storage in the credential registry.
// Output format: 12-byte random nonce followed by the AES-256-GCM ciphertext
// (which includes the authentication tag). key must be 32 bytes.
func SealSecret(key []byte, secret string) ([]byte, error) {
	if len(key) != 32 {
		return nil, errors.New("auth: credential encryption key must be 32 bytes")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("auth: new cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("auth: new gcm: %w", err)
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("auth: generate nonce: %w", err)
	}
	return gcm.Seal(nonce, nonce, []byte(secret), nil), nil
}

// OpenSecret decrypts a sealed secret produced by SealSecret. It returns an
// error for malformed or tampered ciphertext and for ciphertext sealed with a
// different key (e.g. after a credential encryption key rotation).
func OpenSecret(key []byte, ciphertext []byte) (string, error) {
	if len(key) != 32 {
		return "", errors.New("auth: credential encryption key must be 32 bytes")
	}
	if len(ciphertext) == 0 {
		return "", errors.New("auth: empty sealed secret")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", fmt.Errorf("auth: new cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", fmt.Errorf("auth: new gcm: %w", err)
	}
	nonceSize := gcm.NonceSize()
	if len(ciphertext) < nonceSize {
		return "", errors.New("auth: malformed sealed secret")
	}
	nonce, sealed := ciphertext[:nonceSize], ciphertext[nonceSize:]
	plain, err := gcm.Open(nil, nonce, sealed, nil)
	if err != nil {
		return "", fmt.Errorf("auth: unseal secret: %w", err)
	}
	return string(plain), nil
}

func verifySecretHash(stored, secret string) bool {
	saltHex, hashHex, ok := strings.Cut(stored, "$")
	if !ok {
		return false
	}
	salt, err := hex.DecodeString(saltHex)
	if err != nil {
		return false
	}
	want, err := hex.DecodeString(hashHex)
	if err != nil {
		return false
	}
	mac := hmac.New(sha256.New, salt)
	mac.Write([]byte(secret))
	return hmac.Equal(want, mac.Sum(nil))
}

// ========== Token signing (HMAC-SHA256 opaque token) ==========

// tokenVersion marks the token format so future formats can be introduced
// without breaking verification of outstanding tokens.
const tokenVersion = "v1"

// defaultTokenTTL is how long a signed mount token stays valid before the
// fuse must re-authenticate. Kept modest so a leaked or revoked token has a
// bounded blast radius.
const defaultTokenTTL = 6 * time.Hour

// DefaultTokenTTL returns the token lifetime used when SignToken is called
// with ttl<=0. Exposed so callers in other packages can echo the expiry.
func DefaultTokenTTL() time.Duration { return defaultTokenTTL }

// TokenClaims is the payload bound into a signed token.
type TokenClaims struct {
	// Principal is the RBAC principal the token was issued to.
	Principal Principal `json:"p"`
	// Bucket is the bucket the token was minted for. It is signed into the
	// token — a holder cannot forge a different bucket without the signing key —
	// and is exposed in the token payload for audit/logging and any future
	// server-side per-bucket enforcement. It is NOT currently enforced per
	// request: data-plane requests (namespace/inodes/chunks) carry no bucket
	// identifier in the path for the HTTP layer to compare this claim against,
	// so cross-bucket isolation today relies on each mount being pinned to a
	// single bucket by the FUSE and on the operator trust boundary (a mount
	// secret does not reach the credential registry or other buckets).
	Bucket string `json:"b,omitempty"`
	// Exp is the Unix-nanosecond expiration time.
	Exp int64 `json:"e"`
}

// SignToken issues a signed, principal-bound, bucket-scoped token.
// The outer value is "<version>.<sighex>.<payloadhex>", where sig = HMAC-SHA256
// over "<version>.<payloadhex>" using the signing key. The token is opaque to
// the fuse; only metad (with the signing key) can mint or verify it.
func SignToken(signingKey string, principal Principal, bucket string, ttl time.Duration) (string, error) {
	if signingKey == "" {
		return "", errors.New("auth: empty signing key")
	}
	if principal == "" {
		return "", errors.New("auth: empty principal")
	}
	if ttl <= 0 {
		ttl = defaultTokenTTL
	}
	claims := TokenClaims{Principal: principal, Bucket: bucket, Exp: time.Now().Add(ttl).UnixNano()}
	payload, err := json.Marshal(claims)
	if err != nil {
		return "", fmt.Errorf("auth: marshal claims: %w", err)
	}
	payloadHex := hex.EncodeToString(payload)
	sigHex := hex.EncodeToString(hmacSHA256(signingKey, []byte(tokenVersion+"."+payloadHex)))
	return tokenVersion + "." + sigHex + "." + payloadHex, nil
}

// ParseToken verifies a token's signature and returns its claims. It rejects
// tokens that are expired, malformed, or signed with a different key
// (e.g. after a key rotation).
func ParseToken(signingKey, token string) (*TokenClaims, error) {
	if signingKey == "" {
		return nil, errors.New("auth: empty signing key")
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 || parts[0] != tokenVersion {
		return nil, errors.New("auth: malformed token")
	}
	_, sigHex, payloadHex := parts[0], parts[1], parts[2]

	want := hmacSHA256(signingKey, []byte(tokenVersion+"."+payloadHex))
	got, err := hex.DecodeString(sigHex)
	if err != nil || !hmac.Equal(want, got) {
		return nil, errors.New("auth: invalid token signature")
	}

	payload, err := hex.DecodeString(payloadHex)
	if err != nil {
		return nil, errors.New("auth: malformed token payload")
	}
	var claims TokenClaims
	if err := json.Unmarshal(payload, &claims); err != nil {
		return nil, errors.New("auth: malformed token claims")
	}
	if claims.Exp > 0 && time.Now().UnixNano() > claims.Exp {
		return nil, errors.New("auth: token expired")
	}
	if claims.Principal == "" {
		return nil, errors.New("auth: token missing principal")
	}
	return &claims, nil
}

// ParseTokenAny verifies a token against several signing keys in order,
// returning the claims from the first key that validates it. It exists so a
// signing key can be rotated without invalidating tokens already in flight:
// metad is started with the new key first and the previous key second, mints
// every new token with the new key, and keeps honoring outstanding tokens
// until they expire on their own. Empty keys are skipped. The error returned
// is the one from the first non-empty key, which is the common case.
func ParseTokenAny(token string, signingKeys ...string) (*TokenClaims, error) {
	var firstErr error
	for _, key := range signingKeys {
		if key == "" {
			continue
		}
		claims, err := ParseToken(key, token)
		if err == nil {
			return claims, nil
		}
		if firstErr == nil {
			firstErr = err
		}
	}
	if firstErr == nil {
		return nil, errors.New("auth: empty signing key")
	}
	return nil, firstErr
}

func hmacSHA256(key string, msg []byte) []byte {
	mac := hmac.New(sha256.New, []byte(key))
	mac.Write(msg)
	return mac.Sum(nil)
}
