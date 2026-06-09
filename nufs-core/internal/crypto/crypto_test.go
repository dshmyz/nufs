package crypto

import (
	"bytes"
	"testing"
)

func TestEncryptDecryptRoundTrip(t *testing.T) {
	kms, err := NewLocalKMS()
	if err != nil {
		t.Fatalf("NewLocalKMS: %v", err)
	}

	keyID, key, err := kms.ActiveDEK()
	if err != nil {
		t.Fatalf("ActiveDEK: %v", err)
	}

	tests := []struct {
		name string
		data []byte
	}{
		{"small", []byte("hello world")},
		{"empty", []byte{}},
		{"binary", []byte{0x00, 0x01, 0x02, 0xff, 0xfe}},
		{"1KB", make([]byte, 1024)},
		{"64KB", make([]byte, 64*1024)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			blob, err := Encrypt(tt.data, keyID, key)
			if err != nil {
				t.Fatalf("Encrypt: %v", err)
			}

			// Encrypted data should differ from plaintext (unless empty)
			if len(tt.data) > 0 && bytes.Equal(blob, tt.data) {
				t.Fatal("encrypted data should differ from plaintext")
			}

			plain, err := Decrypt(blob, kms)
			if err != nil {
				t.Fatalf("Decrypt: %v", err)
			}

			if !bytes.Equal(plain, tt.data) {
				t.Fatalf("round-trip mismatch: got %d bytes, want %d bytes", len(plain), len(tt.data))
			}
		})
	}
}

func TestEncryptWithAdditionalData(t *testing.T) {
	kms, err := NewLocalKMS()
	if err != nil {
		t.Fatalf("NewLocalKMS: %v", err)
	}

	keyID, key, err := kms.ActiveDEK()
	if err != nil {
		t.Fatalf("ActiveDEK: %v", err)
	}

	data := []byte("secret data")
	blob, err := Encrypt(data, keyID, key)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}

	// Tamper with the key ID in the blob — should fail decryption
	tampered := make([]byte, len(blob))
	copy(tampered, blob)
	tampered[3] ^= 0xff // flip a bit in the key ID

	_, err = Decrypt(tampered, kms)
	if err == nil {
		t.Fatal("expected error decrypting tampered blob")
	}
}

func TestEncryptorRoundTrip(t *testing.T) {
	kms, err := NewLocalKMS()
	if err != nil {
		t.Fatalf("NewLocalKMS: %v", err)
	}

	enc := NewEncryptor(kms)
	if !enc.Enabled() {
		t.Fatal("Encryptor should be enabled")
	}

	data := []byte("test data for encryptor")
	encrypted, err := enc.EncryptChunk(data)
	if err != nil {
		t.Fatalf("EncryptChunk: %v", err)
	}

	decrypted, err := enc.DecryptChunk(encrypted)
	if err != nil {
		t.Fatalf("DecryptChunk: %v", err)
	}

	if !bytes.Equal(decrypted, data) {
		t.Fatalf("round-trip mismatch")
	}
}

func TestEncryptorNil(t *testing.T) {
	var enc *Encryptor
	if enc.Enabled() {
		t.Fatal("nil Encryptor should not be enabled")
	}

	// EncryptChunk on nil should return data unchanged
	data := []byte("plaintext")
	result, err := enc.EncryptChunk(data)
	if err != nil {
		t.Fatalf("EncryptChunk on nil: %v", err)
	}
	if !bytes.Equal(result, data) {
		t.Fatal("nil Encryptor should pass through data")
	}

	result, err = enc.DecryptChunk(data)
	if err != nil {
		t.Fatalf("DecryptChunk on nil: %v", err)
	}
	if !bytes.Equal(result, data) {
		t.Fatal("nil Encryptor should pass through data")
	}
}

func TestLocalKMSKeyRotation(t *testing.T) {
	kms, err := NewLocalKMS()
	if err != nil {
		t.Fatalf("NewLocalKMS: %v", err)
	}

	// Get the first active key
	id1, key1, err := kms.ActiveDEK()
	if err != nil {
		t.Fatalf("ActiveDEK: %v", err)
	}

	// Encrypt with the first key
	data := []byte("data encrypted with key 1")
	blob, err := Encrypt(data, id1, key1)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}

	// Rotate: generate a new DEK
	id2, key2, err := kms.GenerateDEK()
	if err != nil {
		t.Fatalf("GenerateDEK: %v", err)
	}
	if id2 == id1 {
		t.Fatal("new DEK should have different ID")
	}

	// Active key should now be the new one
	activeID, _, err := kms.ActiveDEK()
	if err != nil {
		t.Fatalf("ActiveDEK after rotation: %v", err)
	}
	if activeID != id2 {
		t.Fatal("active DEK should be the new key after rotation")
	}

	// Old data should still be decryptable
	plain, err := Decrypt(blob, kms)
	if err != nil {
		t.Fatalf("Decrypt old data after rotation: %v", err)
	}
	if !bytes.Equal(plain, data) {
		t.Fatal("old data should still decrypt correctly after key rotation")
	}

	// New encryption should use the new key
	newData := []byte("data encrypted with key 2")
	newBlob, err := Encrypt(newData, id2, key2)
	if err != nil {
		t.Fatalf("Encrypt with new key: %v", err)
	}
	plain2, err := Decrypt(newBlob, kms)
	if err != nil {
		t.Fatalf("Decrypt new data: %v", err)
	}
	if !bytes.Equal(plain2, newData) {
		t.Fatal("new data should decrypt correctly")
	}
}

func TestInvalidKeySize(t *testing.T) {
	_, err := Encrypt([]byte("test"), "key-1", []byte("short"))
	if err == nil {
		t.Fatal("expected error for invalid key size")
	}
}

func TestDecryptTruncatedBlob(t *testing.T) {
	kms, _ := NewLocalKMS()
	_, err := Decrypt([]byte{0x00}, kms)
	if err == nil {
		t.Fatal("expected error for truncated blob")
	}

	// Valid key_id_len but truncated key ID
	_, err = Decrypt([]byte{0x00, 0x05, 0x01}, kms)
	if err == nil {
		t.Fatal("expected error for truncated key ID")
	}
}
