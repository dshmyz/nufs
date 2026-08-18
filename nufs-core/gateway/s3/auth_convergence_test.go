package s3

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/dshmyz/nufs/nufs-core/chunkstore"
	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// --- CredentialStore: registry-sync semantics ---

func TestCredentialStore_ReplaceAllAndPrincipalFor(t *testing.T) {
	cs := NewCredentialStore()
	cs.AddCredential("local-ak", "local-sk")
	if cs.PrincipalFor("local-ak") != "local-ak" {
		t.Fatal("locally added credential should have principal = access key")
	}

	// Registry sync replaces the whole table and carries bound principals.
	cs.ReplaceAll([]metadata.GatewayCredential{
		{AccessKey: "ak-1", SecretKey: "sk-one", Principal: "svc-1"},
		{AccessKey: "ak-2", SecretKey: "sk-two", Principal: ""}, // empty -> falls back to ak
	})
	if cs.Count() != 2 {
		t.Fatalf("count = %d, want 2", cs.Count())
	}
	if cs.PrincipalFor("ak-1") != "svc-1" {
		t.Fatalf("PrincipalFor(ak-1) = %q, want svc-1", cs.PrincipalFor("ak-1"))
	}
	if cs.PrincipalFor("ak-2") != "ak-2" {
		t.Fatalf("PrincipalFor(ak-2) = %q, want ak-2 fallback", cs.PrincipalFor("ak-2"))
	}
	// The local credential was wiped by the full swap (registry is the truth).
	if cs.PrincipalFor("local-ak") != "local-ak" {
		t.Fatal("local-ak should be gone after ReplaceAll")
	}
	// SigV4 still verifies against the synced secret.
	if cs.PrincipalFor("unknown") != "unknown" {
		t.Fatal("unknown key should fall back to itself")
	}

	// Empty sync clears the table -> anonymous mode.
	cs.ReplaceAll(nil)
	if cs.HasCredentials() {
		t.Fatal("empty ReplaceAll should clear the table")
	}
}

// --- CredentialSyncer ---

func TestCredentialSyncer_SyncAndRefresh(t *testing.T) {
	cs := NewCredentialStore()
	var served []metadata.GatewayCredential
	fetch := func(_ context.Context) ([]metadata.GatewayCredential, error) {
		return served, nil
	}
	syncer := NewCredentialSyncer(cs, fetch, 20*time.Millisecond)

	served = []metadata.GatewayCredential{{AccessKey: "ak-1", SecretKey: "s1", Principal: "p1"}}
	if err := syncer.SyncOnce(context.Background()); err != nil {
		t.Fatalf("SyncOnce: %v", err)
	}
	if !cs.HasCredentials() || cs.PrincipalFor("ak-1") != "p1" {
		t.Fatal("SyncOnce did not populate the store")
	}

	// Run ticks on interval; rotate the credential and watch it land.
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	go func() {
		time.Sleep(40 * time.Millisecond)
		served = []metadata.GatewayCredential{{AccessKey: "ak-2", SecretKey: "s2", Principal: "p2"}}
	}()
	if err := syncer.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if cs.PrincipalFor("ak-2") != "p2" {
		t.Fatal("periodic refresh did not apply the rotated credential")
	}
	if cs.HasCredentials() && cs.PrincipalFor("ak-1") != "ak-1" {
		t.Fatal("rotated-out ak-1 should no longer be present")
	}
}

func TestCredentialSyncer_KeepsPreviousOnFailure(t *testing.T) {
	cs := NewCredentialStore()
	calls := 0
	fetch := func(_ context.Context) ([]metadata.GatewayCredential, error) {
		calls++
		if calls > 1 {
			return nil, fmt.Errorf("metad down")
		}
		return []metadata.GatewayCredential{{AccessKey: "ak-1", SecretKey: "s1", Principal: "p1"}}, nil
	}
	syncer := NewCredentialSyncer(cs, fetch, 20*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	if err := syncer.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	// The failed refreshes must not have cleared the table.
	if !cs.HasCredentials() || cs.PrincipalFor("ak-1") != "p1" {
		t.Fatal("failed refresh wiped the previous credential set")
	}
}

// --- Gateway: SigV4 principal binding + bucket owner ---

// sigV4Request builds an AWS SigV4-signed request using the gateway's own
// canonical-request helpers, so the test exercises the real verification path.
func sigV4Request(t *testing.T, method, target, accessKey, secretKey string, body []byte) *http.Request {
	t.Helper()
	req := httptest.NewRequest(method, target, bytes.NewReader(body))
	req.RequestURI = "" // httptest sets it; http.Client.Do rejects a set RequestURI
	now := time.Now().UTC()
	amzDate := now.Format("20060102T150405Z")
	dateStamp := now.Format("20060102")
	payloadHash := hashSHA256(string(body))
	req.Header.Set("X-Amz-Date", amzDate)
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)
	req.Host = "s3.example.com"

	signedHeaders := "host;x-amz-content-sha256;x-amz-date"
	canonicalHeaders := buildCanonicalHeaders(req, signedHeaders)
	canonicalRequest := buildCanonicalRequest(req, canonicalHeaders, signedHeaders, payloadHash)
	scope := dateStamp + "/us-east-1/s3/aws4_request"
	stringToSign := "AWS4-HMAC-SHA256\n" + amzDate + "\n" + scope + "\n" + hashSHA256(canonicalRequest)
	signingKey := deriveSigningKey(secretKey, dateStamp, "us-east-1", "s3")
	sig := hex.EncodeToString(hmacSHA256(signingKey, stringToSign))
	req.Header.Set("Authorization",
		"AWS4-HMAC-SHA256 Credential="+accessKey+"/"+scope+
			", SignedHeaders="+signedHeaders+
			", Signature="+sig)
	return req
}

func TestGatewayCreateBucketOwnerFromRegisteredPrincipal(t *testing.T) {
	meta := newMockMetaService()
	creds := NewCredentialStore()
	// Registry-bind ak-owner -> principal "svc-bucket-owner" (not the raw key).
	creds.ReplaceAll([]metadata.GatewayCredential{
		{AccessKey: "ak-owner", SecretKey: "sk-owner", Principal: "svc-bucket-owner"},
	})
	gw := NewGateway(GatewayConfig{
		MetaService: meta,
		Creds:       creds,
		ChunkStore:  chunkstore.NewMemoryChunkStore(),
	})
	mux := http.NewServeMux()
	mux.HandleFunc("/", gw.route)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	req := sigV4Request(t, http.MethodPut, ts.URL+"/owner-bucket", "ak-owner", "sk-owner", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body := new(bytes.Buffer)
		body.ReadFrom(resp.Body)
		t.Fatalf("PUT bucket = %d, body=%s", resp.StatusCode, body.String())
	}

	// The default policy's owner must be the registered principal, not the
	// raw access key (and not the X-Owner header, which is no longer read).
	if got := gw.acl.OwnerOf("owner-bucket"); got != "svc-bucket-owner" {
		t.Fatalf("bucket owner = %q, want svc-bucket-owner", got)
	}

	// Wrong signature is rejected before reaching the handler.
	bad := sigV4Request(t, http.MethodPut, ts.URL+"/bad-bucket", "ak-owner", "wrong-secret", nil)
	badResp, err := http.DefaultClient.Do(bad)
	if err != nil {
		t.Fatalf("Do(bad): %v", err)
	}
	defer badResp.Body.Close()
	if badResp.StatusCode != http.StatusForbidden {
		t.Fatalf("bad-signature PUT = %d, want 403", badResp.StatusCode)
	}
}

// --- Gateway: policy preload ---

func TestGatewayLoadPoliciesPreloadsRegistry(t *testing.T) {
	meta := newMockMetaService()
	// Seed a persisted policy in the mock registry (as if created earlier).
	_ = meta.SetBucketPolicy(context.Background(), "seeded-bucket", metadata.BucketPolicy{
		Bucket: "seeded-bucket",
		Owner:  "svc-owner",
		Statements: []metadata.Statement{{
			Effect:      "allow",
			Principal:   metadata.Principal("svc-user"),
			Permissions: []metadata.Permission{metadata.PermRead},
			Resource:    "seeded-bucket",
		}},
		DefaultAccess: "deny",
	})
	_ = meta.CreateBucket(context.Background(), "seeded-bucket", metadata.PlacementPolicy{})

	gw := NewGateway(GatewayConfig{
		MetaService: meta,
		Creds:       NewCredentialStore(),
		ChunkStore:  chunkstore.NewMemoryChunkStore(),
	})
	// Before LoadPolicies the in-memory ACL is empty (the pre-fix bug: a
	// restart lost every policy and authenticated users were let through).
	if gw.acl.GetPolicy("seeded-bucket") != nil {
		t.Fatal("expected empty ACL cache before LoadPolicies")
	}
	gw.LoadPolicies(context.Background())
	if gw.acl.GetPolicy("seeded-bucket") == nil {
		t.Fatal("LoadPolicies did not preload the persisted policy")
	}
	if err := gw.acl.CheckAccess("seeded-bucket", metadata.Principal("svc-user"), metadata.PermRead); err != nil {
		t.Fatalf("seeded policy should grant PermRead to svc-user: %v", err)
	}
	if err := gw.acl.CheckAccess("seeded-bucket", metadata.Principal("other"), metadata.PermRead); err == nil {
		t.Fatal("default-deny should block other")
	}
}

func TestCredentialStore_AuthModeKeepsAuthOnAfterRevocation(t *testing.T) {
	cs := NewCredentialStore()
	if cs.AuthEnabled() {
		t.Fatal("fresh store should be anonymous (no auth)")
	}
	cs.ReplaceAll([]metadata.GatewayCredential{{AccessKey: "ak-1", SecretKey: "s1", Principal: "p1"}})
	if !cs.AuthEnabled() {
		t.Fatal("store with a credential should require auth")
	}
	// Pinning auth mode (registry sync authoritative) survives full revocation.
	cs.SetAuthMode(true)
	cs.ReplaceAll(nil)
	if cs.HasCredentials() {
		t.Fatal("credentials should be gone after revocation")
	}
	if !cs.AuthEnabled() {
		t.Fatal("auth mode must stay on after revoking the last credential — revocation rejects, it does not open the door")
	}
	// Without pinning, an empty store is anonymous again (legacy local sources).
	cs2 := NewCredentialStore()
	cs2.ReplaceAll(nil)
	if cs2.AuthEnabled() {
		t.Fatal("unpinned empty store should be anonymous")
	}
}
