package metadata

import (
	"bytes"
	"context"
	"testing"
	"time"
)

func TestCredential_AuthenticateRoundTrip(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	hash, err := hashSecret("supersecret")
	if err != nil {
		t.Fatalf("hashSecret: %v", err)
	}
	cred := Credential{AccessKey: "ak-1", Principal: "app-server-1", SecretHash: hash}
	if err := store.PutCredential(ctx, cred); err != nil {
		t.Fatalf("PutCredential: %v", err)
	}

	// Correct secret → returns bound principal.
	p, err := store.Authenticate(ctx, "ak-1", "supersecret")
	if err != nil {
		t.Fatalf("Authenticate(good): %v", err)
	}
	if p != "app-server-1" {
		t.Fatalf("Authenticate returned principal %q, want app-server-1", p)
	}

	// Wrong secret → denied.
	if _, err := store.Authenticate(ctx, "ak-1", "wrong"); err == nil {
		t.Fatal("Authenticate(bad secret) succeeded, want ErrAccessDenied")
	}

	// Unknown key → denied.
	if _, err := store.Authenticate(ctx, "nope", "supersecret"); err == nil {
		t.Fatal("Authenticate(unknown key) succeeded, want ErrAccessDenied")
	}
}

func TestCredential_SecretNotStoredPlaintext(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	hash, _ := hashSecret("sensitive-secret")
	if hash == "sensitive-secret" {
		t.Fatal("secret hash equals plaintext secret")
	}
	if err := store.PutCredential(ctx, Credential{AccessKey: "ak-1", SecretHash: hash}); err != nil {
		t.Fatalf("PutCredential: %v", err)
	}
	got, err := store.GetCredential(ctx, "ak-1")
	if err != nil {
		t.Fatalf("GetCredential: %v", err)
	}
	if got.SecretHash == "sensitive-secret" {
		t.Fatal("stored credential exposes plaintext secret")
	}
}

func TestCredential_DefaultPrincipalIsAccessKey(t *testing.T) {
	store := newTestStore(t)
	hash, _ := hashSecret("secret")
	if err := store.PutCredential(context.Background(), Credential{AccessKey: "ak-default", SecretHash: hash}); err != nil {
		t.Fatalf("PutCredential: %v", err)
	}
	got, err := store.GetCredential(context.Background(), "ak-default")
	if err != nil {
		t.Fatalf("GetCredential: %v", err)
	}
	if got.Principal != "ak-default" {
		t.Fatalf("default principal = %q, want access key", got.Principal)
	}
}

func TestCredential_DeleteAndList(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	for _, ak := range []string{"ak-a", "ak-b"} {
		hash, _ := hashSecret("s")
		if err := store.PutCredential(ctx, Credential{AccessKey: ak, SecretHash: hash}); err != nil {
			t.Fatalf("PutCredential(%s): %v", ak, err)
		}
	}
	creds, err := store.ListCredentials(ctx)
	if err != nil {
		t.Fatalf("ListCredentials: %v", err)
	}
	if len(creds) != 2 {
		t.Fatalf("ListCredentials len = %d, want 2", len(creds))
	}
	if err := store.DeleteCredential(ctx, "ak-a"); err != nil {
		t.Fatalf("DeleteCredential: %v", err)
	}
	if _, err := store.Authenticate(ctx, "ak-a", "s"); err == nil {
		t.Fatal("deleted credential still authenticates")
	}
}

func TestSignToken_RoundTripAndClaims(t *testing.T) {
	principal := Principal("app-server-1")
	tok, err := SignToken("signing-key", principal, "secdata", 5*time.Minute)
	if err != nil {
		t.Fatalf("SignToken: %v", err)
	}
	claims, err := ParseToken("signing-key", tok)
	if err != nil {
		t.Fatalf("ParseToken: %v", err)
	}
	if claims.Principal != principal {
		t.Fatalf("claims.Principal = %q, want %q", claims.Principal, principal)
	}
	if claims.Bucket != "secdata" {
		t.Fatalf("claims.Bucket = %q, want secdata", claims.Bucket)
	}
}

func TestSignToken_KeyRotationInvalidates(t *testing.T) {
	tok, err := SignToken("old-key", "p1", "", time.Minute)
	if err != nil {
		t.Fatalf("SignToken: %v", err)
	}
	// Verify with the same key works.
	if _, err := ParseToken("old-key", tok); err != nil {
		t.Fatalf("ParseToken(same key): %v", err)
	}
	// Verify with a different key must fail.
	if _, err := ParseToken("new-key", tok); err == nil {
		t.Fatal("token verified under a different signing key")
	}
}

func TestSignToken_Expiry(t *testing.T) {
	tok, err := SignToken("k", "p1", "", 1*time.Second)
	if err != nil {
		t.Fatalf("SignToken: %v", err)
	}
	// Immediately valid.
	if _, err := ParseToken("k", tok); err != nil {
		t.Fatalf("ParseToken before expiry: %v", err)
	}
	time.Sleep(2 * time.Second)
	if _, err := ParseToken("k", tok); err == nil {
		t.Fatal("expired token still verifies")
	}
}

func TestSignToken_EmptySigningKeyRejected(t *testing.T) {
	if _, err := SignToken("", "p1", "", time.Minute); err == nil {
		t.Fatal("SignToken with empty key succeeded")
	}
	if tok, _ := SignToken("k", "p1", "", time.Minute); tok != "" {
		if _, err := ParseToken("", tok); err == nil {
			t.Fatal("ParseToken with empty key succeeded")
		}
	}
}

func TestParseToken_Malformed(t *testing.T) {
	if _, err := ParseToken("k", "not-a-token"); err == nil {
		t.Fatal("malformed token verified")
	}
	if _, err := ParseToken("k", "v2.sig.payload"); err == nil {
		t.Fatal("wrong-version token verified")
	}
}

func TestParseTokenAny_AcceptsPreviousKeyDuringRotation(t *testing.T) {
	oldTok, err := SignToken("old-key", "p-old", "", time.Minute)
	if err != nil {
		t.Fatalf("SignToken(old): %v", err)
	}
	newTok, err := SignToken("new-key", "p-new", "", time.Minute)
	if err != nil {
		t.Fatalf("SignToken(new): %v", err)
	}

	// metad post-rotation: current key first, retired key second.
	keys := []string{"new-key", "old-key"}

	claims, err := ParseTokenAny(newTok, keys...)
	if err != nil {
		t.Fatalf("ParseTokenAny(new token): %v", err)
	}
	if claims.Principal != "p-new" {
		t.Fatalf("Principal = %q, want p-new", claims.Principal)
	}

	claims, err = ParseTokenAny(oldTok, keys...)
	if err != nil {
		t.Fatalf("ParseTokenAny(outstanding token minted with the retired key): %v", err)
	}
	if claims.Principal != "p-old" {
		t.Fatalf("Principal = %q, want p-old", claims.Principal)
	}

	// Once the retired key is dropped, the old token must stop verifying.
	if _, err := ParseTokenAny(oldTok, "new-key"); err == nil {
		t.Fatal("old token still verifies after the previous key was removed")
	}
}

func TestParseTokenAny_SkipsEmptyKeys(t *testing.T) {
	tok, err := SignToken("k", "p1", "", time.Minute)
	if err != nil {
		t.Fatalf("SignToken: %v", err)
	}
	if _, err := ParseTokenAny(tok, "", "k", ""); err != nil {
		t.Fatalf("ParseTokenAny with empty keys interleaved: %v", err)
	}
	if _, err := ParseTokenAny(tok); err == nil {
		t.Fatal("ParseTokenAny with no keys succeeded")
	}
	if _, err := ParseTokenAny(tok, "", ""); err == nil {
		t.Fatal("ParseTokenAny with only empty keys succeeded")
	}
}

func TestCredential_SealOpenRoundTrip(t *testing.T) {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}
	secret := "s3cr3t-ÄÖ-密钥"

	sealed, err := SealSecret(key, secret)
	if err != nil {
		t.Fatalf("SealSecret: %v", err)
	}
	if len(sealed) != 12+len(secret)+16 { // nonce + plaintext + GCM tag
		t.Fatalf("sealed length = %d, want %d", len(sealed), 12+len(secret)+16)
	}
	if string(sealed) == secret {
		t.Fatal("sealed output equals plaintext secret")
	}

	opened, err := OpenSecret(key, sealed)
	if err != nil {
		t.Fatalf("OpenSecret: %v", err)
	}
	if opened != secret {
		t.Fatalf("OpenSecret = %q, want %q", opened, secret)
	}

	// Same secret under the same key seals differently (random nonce) but opens
	// back to the same value.
	sealed2, _ := SealSecret(key, secret)
	if bytes.Equal(sealed, sealed2) {
		t.Fatal("two seals of the same secret are identical (nonce not random?)")
	}
	if opened2, err := OpenSecret(key, sealed2); err != nil || opened2 != secret {
		t.Fatalf("OpenSecret(sealed2) = %q, %v", opened2, err)
	}

	// Wrong key → error.
	wrong := make([]byte, 32)
	if _, err := OpenSecret(wrong, sealed); err == nil {
		t.Fatal("OpenSecret with wrong key succeeded")
	}

	// Tampered ciphertext → error (GCM auth tag).
	tampered := append([]byte(nil), sealed...)
	tampered[len(tampered)-1] ^= 0x01
	if _, err := OpenSecret(key, tampered); err == nil {
		t.Fatal("OpenSecret with tampered ciphertext succeeded")
	}

	// Empty / malformed → error.
	if _, err := OpenSecret(key, nil); err == nil {
		t.Fatal("OpenSecret(nil) succeeded")
	}
	if _, err := OpenSecret(key, []byte("short")); err == nil {
		t.Fatal("OpenSecret(short) succeeded")
	}

	// Wrong key length → error.
	if _, err := SealSecret([]byte("short"), secret); err == nil {
		t.Fatal("SealSecret with short key succeeded")
	}
}

func TestCredential_SealedSecretRoundTripsThroughStore(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	key := make([]byte, 32)
	for i := range key {
		key[i] = 0xAB
	}
	hash, err := hashSecret("registry-secret")
	if err != nil {
		t.Fatalf("hashSecret: %v", err)
	}
	sealed, err := SealSecret(key, "registry-secret")
	if err != nil {
		t.Fatalf("SealSecret: %v", err)
	}
	cred := Credential{AccessKey: "ak-sealed", Principal: "svc-1", SecretHash: hash, SecretCiphertext: sealed}
	if err := store.PutCredential(ctx, cred); err != nil {
		t.Fatalf("PutCredential: %v", err)
	}

	got, err := store.GetCredential(ctx, "ak-sealed")
	if err != nil {
		t.Fatalf("GetCredential: %v", err)
	}
	if got.SecretHash != hash {
		t.Fatalf("secret hash changed through the store")
	}
	opened, err := OpenSecret(key, got.SecretCiphertext)
	if err != nil {
		t.Fatalf("OpenSecret after store round-trip: %v", err)
	}
	if opened != "registry-secret" {
		t.Fatalf("opened = %q, want registry-secret", opened)
	}
	// Authenticate still works on the hash (the fuse path is unaffected by
	// whether a sealed blob exists).
	p, err := store.Authenticate(ctx, "ak-sealed", "registry-secret")
	if err != nil || p != "svc-1" {
		t.Fatalf("Authenticate = %q, %v", p, err)
	}
}

func TestCredential_HashOnlyCredentialAuthenticatesButNotSealed(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	// A credential written before --credential-secret-key existed has no sealed
	// blob: it still authenticates mounts (hash path) but OpenSecret must fail.
	hash, _ := hashSecret("legacy-secret")
	if err := store.PutCredential(ctx, Credential{AccessKey: "ak-legacy", SecretHash: hash}); err != nil {
		t.Fatalf("PutCredential: %v", err)
	}
	if _, err := store.Authenticate(ctx, "ak-legacy", "legacy-secret"); err != nil {
		t.Fatalf("Authenticate on hash-only credential: %v", err)
	}
	got, err := store.GetCredential(ctx, "ak-legacy")
	if err != nil {
		t.Fatalf("GetCredential: %v", err)
	}
	if len(got.SecretCiphertext) != 0 {
		t.Fatalf("hash-only credential unexpectedly carries a sealed blob")
	}
	if _, err := OpenSecret(make([]byte, 32), got.SecretCiphertext); err == nil {
		t.Fatal("OpenSecret on empty ciphertext succeeded")
	}
}
