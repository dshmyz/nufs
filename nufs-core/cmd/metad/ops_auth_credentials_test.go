package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// testCredKey returns a fixed 32-byte key for the handler tests.
func testCredKey() []byte {
	k := make([]byte, 32)
	for i := range k {
		k[i] = byte(i)
	}
	return k
}

// TestHandleGatewayCredentials returns unsealed plaintext secrets for the S3
// gateway credential sync, and skips credentials that have no sealed blob
// (registered before --credential-secret-key, or sealing disabled).
func TestHandleGatewayCredentials(t *testing.T) {
	store, _ := newOpsTestStore(t)
	ctx := t.Context()

	// Two credentials: one with a sealed blob (via the PUT handler), one
	// hash-only written directly to the store (simulating a pre-key row).
	putBody := bytes.NewReader([]byte(`{"secret_key":"sk-one","principal":"svc-1"}`))
	req := httptest.NewRequest(http.MethodPut, "/api/v1/auth/creds/ak-one", putBody)
	rr := httptest.NewRecorder()
	h := &opsAuthHandlers{store: store, signingKey: "", credKey: testCredKey()}
	h.handleCreds(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("PUT creds = %d, body=%s", rr.Code, rr.Body.String())
	}

	hash, err := metadata.HashSecret("sk-two")
	if err != nil {
		t.Fatalf("HashSecret: %v", err)
	}
	if err := store.PutCredential(ctx, metadata.Credential{AccessKey: "ak-two", SecretHash: hash}); err != nil {
		t.Fatalf("PutCredential(ak-two): %v", err)
	}

	get := httptest.NewRequest(http.MethodGet, "/api/v1/auth/credentials", nil)
	rr = httptest.NewRecorder()
	h.handleGatewayCredentials(rr, get)
	if rr.Code != http.StatusOK {
		t.Fatalf("GET credentials = %d, body=%s", rr.Code, rr.Body.String())
	}
	var out []gatewayCredentialResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("returned %d credentials, want 1 (hash-only row must be skipped), body=%s", len(out), rr.Body.String())
	}
	if out[0].AccessKey != "ak-one" || out[0].SecretKey != "sk-one" || out[0].Principal != "svc-1" {
		t.Fatalf("credential = %+v, want ak-one/sk-one/svc-1", out[0])
	}
}

// TestHandleGatewayCredentialsWithoutKey returns 503 when no credential
// encryption key is configured (nothing can be unsealed).
func TestHandleGatewayCredentialsWithoutKey(t *testing.T) {
	store, _ := newOpsTestStore(t)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/auth/credentials", nil)
	rr := httptest.NewRecorder()
	(&opsAuthHandlers{store: store, signingKey: ""}).handleGatewayCredentials(rr, req)
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("GET credentials without key = %d, want 503", rr.Code)
	}
}

// TestHandleCredsPUTSealsSecret verifies the PUT path stores both the hash
// (fuse token exchange) and the sealed plaintext (S3 gateway sync).
func TestHandleCredsPUTSealsSecret(t *testing.T) {
	store, _ := newOpsTestStore(t)
	ctx := t.Context()

	req := httptest.NewRequest(http.MethodPut, "/api/v1/auth/creds/ak-sealed",
		bytes.NewReader([]byte(`{"secret_key":"sk-sealed"}`)))
	rr := httptest.NewRecorder()
	(&opsAuthHandlers{store: store, credKey: testCredKey()}).handleCreds(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("PUT creds = %d, body=%s", rr.Code, rr.Body.String())
	}

	cred, err := store.GetCredential(ctx, "ak-sealed")
	if err != nil {
		t.Fatalf("GetCredential: %v", err)
	}
	if cred.SecretHash == "" {
		t.Fatal("PUT creds did not store the hash")
	}
	if len(cred.SecretCiphertext) == 0 {
		t.Fatal("PUT creds did not seal the secret")
	}
	opened, err := metadata.OpenSecret(testCredKey(), cred.SecretCiphertext)
	if err != nil || opened != "sk-sealed" {
		t.Fatalf("OpenSecret = %q, %v", opened, err)
	}
	// Still authenticates on the hash (fuse path).
	if p, err := store.Authenticate(ctx, "ak-sealed", "sk-sealed"); err != nil || p != "ak-sealed" {
		t.Fatalf("Authenticate = %q, %v", p, err)
	}
}
