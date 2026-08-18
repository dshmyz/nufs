package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// registerOpsAuthHandlers wires the mount-authentication endpoints.
//
// Two families live here:
//   - POST /api/v1/auth/token — the fuse presents an accessKey/secretKey and
//     receives a short-lived, principal-bound signed bearer. This route is
//     public (self-authenticating): it never trusts a bare principal claim,
//     only a verified secret. It must be reached even when the ops API has a
//     static --auth-token, so main.go lists it in the public map.
//   - /api/v1/auth/creds/{accessKey} — operator management of the credential
//     registry (nufs-cli `auth cred`). These are NOT public: they sit behind
//     the ops API's static bearer auth and the leader boundary.
//   - GET /api/v1/auth/credentials — the S3 gateway's credential sync source.
//     Also operator-gated (static bearer + leader boundary): it returns the
//     plaintext secrets the gateway needs for SigV4 verification, decrypted
//     from the registry's sealed blobs. Holding the ops bearer already grants
//     the ability to rewrite the registry, so releasing the secrets to the
//     same boundary is consistent with the existing trust model.
//
// The signing key is only ever held here (metad); clients never learn it.
// They read `principal` from the token response and present the opaque token
// to metad on data-plane requests via SetAuthToken.
//
// Because /api/v1/auth/token is public and self-authenticating, it gets its own
// stricter per-IP limiter beyond the general ops limiter: a credential check is
// a fast HMAC, so without this a burst of ~200 wrong-guess requests (the general
// 100 req/s burst) could be tried before backoff. The token endpoint instead
// allows ~10 requests then backs off to a couple per second per source IP,
// which is ample for legitimate mounts (they exchange once per token TTL) while
// throttling blind credential guessing.
func registerOpsAuthHandlers(mux *http.ServeMux, store *metadata.PebbleStore, signingKey string, credKey []byte) (stopAuthLimiter func()) {
	s := &opsAuthHandlers{
		store:      store,
		signingKey: signingKey,
		credKey:    credKey,
		// Tight per-IP budget for credential-guess attempts. The general ops
		// limiter (100/s) still applies on top; this is the first, stricter gate.
		tokenLimiter: metadata.NewRateLimiter(2, 10),
	}
	stopAuthLimiter = s.tokenLimiter.StartCleanup(1 * time.Minute)

	mut := func(fn http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if !requireLeaderRedirect(w, r, store) {
				return
			}
			fn(w, r)
		}
	}

	mux.HandleFunc("/api/v1/auth/token", s.handleAuthToken)
	mux.HandleFunc("/api/v1/auth/creds", mut(s.handleCredsList))
	mux.HandleFunc("/api/v1/auth/creds/", mut(s.handleCreds))
	mux.HandleFunc("/api/v1/auth/credentials", mut(s.handleGatewayCredentials))
	return stopAuthLimiter
}

type opsAuthHandlers struct {
	store        *metadata.PebbleStore
	signingKey   string
	credKey      []byte // --credential-secret-key; nil => seal disabled (hash-only creds)
	tokenLimiter *metadata.RateLimiter
}

type authTokenRequest struct {
	AccessKey string `json:"access_key"`
	SecretKey string `json:"secret_key"`
	Bucket    string `json:"bucket,omitempty"`
}

type authTokenResponse struct {
	Token      string `json:"token"`
	Principal  string `json:"principal"`
	Bucket     string `json:"bucket,omitempty"`
	ExpiresAt  int64  `json:"expires_at"`
	TTLSeconds int64  `json:"ttl_seconds"`
}

// handleAuthToken exchanges an accessKey/secretKey for a signed bearer token.
// It never trusts a bare principal string; the principal it returns is the one
// bound to the credential that actually passed Authenticate.
func (h *opsAuthHandlers) handleAuthToken(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	// Per-IP credential-guess throttle (see registerOpsAuthHandlers). Rejecting
	// with 429 and the same retry semantics as the general limiter both slows a
	// brute-force attempt and makes it observable.
	if !h.tokenLimiter.Allow(r.RemoteAddr) {
		retryAfter := h.tokenLimiter.WaitTime(r.RemoteAddr)
		if retryAfter < time.Second {
			retryAfter = time.Second
		}
		w.Header().Set("Retry-After", fmt.Sprintf("%.0f", retryAfter.Seconds()))
		writeJSONErrorC(w, http.StatusTooManyRequests, "slow_down", "too many token requests from this address")
		return
	}
	if h.signingKey == "" {
		writeJSONErrorC(w, http.StatusServiceUnavailable, "token_signing_disabled", "metad has no --token-signing-key configured")
		return
	}
	var req authTokenRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	principal, err := h.store.Authenticate(r.Context(), req.AccessKey, req.SecretKey)
	if err != nil {
		if errors.Is(err, metadata.ErrAccessDenied) {
			writeJSONErrorC(w, http.StatusUnauthorized, "access_denied", "invalid credentials")
		} else {
			writeJSONError(w, http.StatusInternalServerError, err.Error())
		}
		return
	}

	token, err := metadata.SignToken(h.signingKey, principal, req.Bucket, 0)
	if err != nil {
		writeJSONErrorC(w, http.StatusInternalServerError, "token_generation_failed", err.Error())
		return
	}
	expiresAt := time.Now().Add(metadata.DefaultTokenTTL()).UnixNano()
	writeJSON(w, authTokenResponse{
		Token:      token,
		Principal:  string(principal),
		Bucket:     req.Bucket,
		ExpiresAt:  expiresAt,
		TTLSeconds: int64(metadata.DefaultTokenTTL().Seconds()),
	})
}

// --- Credential registry management (operator, behind static ops auth) ---

type credResponse struct {
	AccessKey string `json:"access_key"`
	Principal string `json:"principal"`
}

type credUpsertRequest struct {
	SecretKey string `json:"secret_key"`
	Principal string `json:"principal,omitempty"`
}

// handleCredsList returns every registered credential. Only the public
// access-key/principal pair is returned — never the secret hash.
func (h *opsAuthHandlers) handleCredsList(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	creds, err := h.store.ListCredentials(r.Context())
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	out := make([]credResponse, 0, len(creds))
	for _, c := range creds {
		out = append(out, credResponse{AccessKey: c.AccessKey, Principal: string(c.Principal)})
	}
	writeJSON(w, out)
}

func (h *opsAuthHandlers) handleCreds(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, "/api/v1/auth/creds/")
	switch r.Method {
	case http.MethodGet:
		if name == "" {
			writeJSONError(w, http.StatusBadRequest, "access key required")
			return
		}
		cred, err := h.store.GetCredential(r.Context(), name)
		if err != nil {
			if errors.Is(err, metadata.ErrAccessDenied) {
				writeJSONErrorC(w, http.StatusNotFound, "not_found", "no such credential")
			} else {
				writeJSONError(w, http.StatusInternalServerError, err.Error())
			}
			return
		}
		writeJSON(w, credResponse{AccessKey: cred.AccessKey, Principal: string(cred.Principal)})

	case http.MethodPut:
		if name == "" {
			writeJSONError(w, http.StatusBadRequest, "access key required")
			return
		}
		var req credUpsertRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid request body")
			return
		}
		if req.SecretKey == "" {
			writeJSONError(w, http.StatusBadRequest, "secret_key required")
			return
		}
		hash, err := metadata.HashSecret(req.SecretKey)
		if err != nil {
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
		cred := metadata.Credential{AccessKey: name, SecretHash: hash}
		if h.credKey != nil {
			sealed, sealErr := metadata.SealSecret(h.credKey, req.SecretKey)
			if sealErr != nil {
				writeJSONError(w, http.StatusInternalServerError, sealErr.Error())
				return
			}
			cred.SecretCiphertext = sealed
		} else {
			// No --credential-secret-key configured: the credential still works
			// for token exchange (hash path) but is not visible to the S3
			// gateway, which needs the plaintext secret for SigV4.
			slog.Warn("credential stored without sealed secret (no --credential-secret-key); S3 gateway cannot use it",
				"access_key", name)
		}
		if req.Principal != "" {
			cred.Principal = metadata.Principal(req.Principal)
		}
		if err := h.store.PutCredential(r.Context(), cred); err != nil {
			if errors.Is(err, metadata.ErrInvalidArgument) {
				writeJSONError(w, http.StatusBadRequest, err.Error())
			} else {
				writeJSONError(w, http.StatusInternalServerError, err.Error())
			}
			return
		}
		writeJSON(w, map[string]string{"status": "updated", "access_key": name})

	case http.MethodDelete:
		if name == "" {
			writeJSONError(w, http.StatusBadRequest, "access key required")
			return
		}
		if err := h.store.DeleteCredential(r.Context(), name); err != nil {
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, map[string]string{"status": "deleted"})

	default:
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

// gatewayCredentialResponse is one entry of the S3 gateway credential sync.
// The secret here is decrypted from the registry's sealed blob on demand; the
// response only exists inside the metad→gateway trust boundary (this route is
// static-bearer + leader-gated, never in the data-plane allowlist).
type gatewayCredentialResponse struct {
	AccessKey string `json:"access_key"`
	SecretKey string `json:"secret_key"`
	Principal string `json:"principal"`
}

// handleGatewayCredentials returns every credential with its plaintext secret
// sealed-blob decrypted, for the S3 gateway's SigV4 credential sync
// (GET /api/v1/auth/credentials, operator-only). Credentials that carry no
// sealed blob (registered before --credential-secret-key, or with sealing
// disabled) are skipped — they remain usable for fuse token exchange.
func (h *opsAuthHandlers) handleGatewayCredentials(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if len(h.credKey) == 0 {
		writeJSONError(w, http.StatusServiceUnavailable, "credential encryption key not configured")
		return
	}
	creds, err := h.store.ListCredentials(r.Context())
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	out := make([]gatewayCredentialResponse, 0, len(creds))
	for _, c := range creds {
		secret, err := metadata.OpenSecret(h.credKey, c.SecretCiphertext)
		if err != nil {
			// No sealed blob or stale key: the credential still authenticates
			// mounts (hash path) but cannot be released to the S3 gateway.
			slog.Warn("skipping credential without unsealable secret",
				"access_key", c.AccessKey, "error", err)
			continue
		}
		out = append(out, gatewayCredentialResponse{
			AccessKey: c.AccessKey,
			SecretKey: secret,
			Principal: string(c.Principal),
		})
	}
	writeJSON(w, out)
}
