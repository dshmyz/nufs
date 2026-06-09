// Package crypto provides at-rest data encryption for NUFS chunk storage.
// It implements AES-256-GCM authenticated encryption with a pluggable KMS
// (Key Management Service) abstraction so that DEKs (Data Encryption Keys)
// can be managed by an external KMS provider in production.
package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
)

// ---------- KMS Abstraction ----------

// KeyID identifies a data encryption key (DEK) in the KMS.
type KeyID string

// KMS is the interface for a key management service. Implementations
// may wrap a local file, HashiCorp Vault, AWS KMS, etc.
type KMS interface {
	// GenerateDEK creates a new data encryption key and returns its ID
	// and the plaintext 32-byte AES-256 key.
	GenerateDEK() (KeyID, []byte, error)

	// DecryptDEK retrieves the plaintext key for the given key ID.
	DecryptDEK(id KeyID) ([]byte, error)

	// ActiveDEK returns the current active DEK ID and plaintext key.
	// This is the key used for new encryptions.
	ActiveDEK() (KeyID, []byte, error)
}

// LocalKMS is a development KMS that stores keys in memory.
// NOT suitable for production — keys are lost on process restart.
// For production, use VaultKMS or AWSKMS implementations.
type LocalKMS struct {
	mu       sync.RWMutex
	keys     map[KeyID][]byte
	activeID KeyID
}

// NewLocalKMS creates an in-memory KMS with a single generated DEK.
func NewLocalKMS() (*LocalKMS, error) {
	kms := &LocalKMS{keys: make(map[KeyID][]byte)}
	if _, _, err := kms.GenerateDEK(); err != nil {
		return nil, err
	}
	return kms, nil
}

// GenerateDEK creates a new AES-256 key and makes it the active key.
func (k *LocalKMS) GenerateDEK() (KeyID, []byte, error) {
	key := make([]byte, 32) // AES-256
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		return "", nil, fmt.Errorf("crypto: generate DEK: %w", err)
	}
	id := KeyID(fmt.Sprintf("local-%d", len(k.keys)+1))
	k.mu.Lock()
	k.keys[id] = key
	k.activeID = id
	k.mu.Unlock()
	return id, key, nil
}

// DecryptDEK returns the plaintext key for the given ID.
func (k *LocalKMS) DecryptDEK(id KeyID) ([]byte, error) {
	k.mu.RLock()
	defer k.mu.RUnlock()
	key, ok := k.keys[id]
	if !ok {
		return nil, fmt.Errorf("crypto: DEK %s not found", id)
	}
	return key, nil
}

// ActiveDEK returns the current active DEK.
func (k *LocalKMS) ActiveDEK() (KeyID, []byte, error) {
	k.mu.RLock()
	defer k.mu.RUnlock()
	if k.activeID == "" {
		return "", nil, errors.New("crypto: no active DEK")
	}
	key, ok := k.keys[k.activeID]
	if !ok {
		return "", nil, fmt.Errorf("crypto: active DEK %s not found", k.activeID)
	}
	return k.activeID, key, nil
}

// ---------- Encryption / Decryption ----------

const (
	// NonceSize is the GCM nonce size (12 bytes).
	NonceSize = 12
	// KeyIDLenSize is the byte length of the key ID length prefix (2 bytes, big-endian).
	KeyIDLenSize = 2
	// Overhead is the total encryption overhead per chunk: key_id_len(2) + key_id(N) + nonce(12) + GCM tag(16).
	// Actual overhead depends on key ID length.
)

// EncryptedBlob is the wire/disk format for an encrypted chunk:
//
//	[2 bytes: key_id_length (big-endian)]
//	[N bytes: key_id]
//	[12 bytes: GCM nonce]
//	[remaining: ciphertext + GCM tag (16 bytes)]
//
// The key_id allows the decrypter to look up the correct DEK from the KMS.

// Encrypt encrypts plaintext using AES-256-GCM with the given key.
// It returns the encrypted blob in the format described above.
func Encrypt(plaintext []byte, keyID KeyID, key []byte) ([]byte, error) {
	if len(key) != 32 {
		return nil, fmt.Errorf("crypto: invalid key size %d, want 32", len(key))
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("crypto: aes new cipher: %w", err)
	}

	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("crypto: gcm new: %w", err)
	}

	// Generate nonce
	nonce := make([]byte, NonceSize)
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("crypto: generate nonce: %w", err)
	}

	// Build the blob: key_id_len + key_id + nonce + ciphertext
	keyIDBytes := []byte(keyID)
	blob := make([]byte, 0, KeyIDLenSize+len(keyIDBytes)+NonceSize+len(plaintext)+aead.Overhead())
	blob = binary.BigEndian.AppendUint16(blob, uint16(len(keyIDBytes)))
	blob = append(blob, keyIDBytes...)
	blob = append(blob, nonce...)

	// Seal appends the ciphertext and tag to the nonce (additional data = nil)
	// We use keyID as additional data to bind the ciphertext to the key.
	ciphertext := aead.Seal(nil, nonce, plaintext, keyIDBytes)
	blob = append(blob, ciphertext...)

	return blob, nil
}

// Decrypt decrypts an encrypted blob using the provided KMS to resolve the DEK.
func Decrypt(blob []byte, kms KMS) ([]byte, error) {
	if len(blob) < KeyIDLenSize {
		return nil, errors.New("crypto: blob too short")
	}

	// Parse key ID
	keyIDLen := binary.BigEndian.Uint16(blob[:KeyIDLenSize])
	offset := KeyIDLenSize
	if len(blob) < offset+int(keyIDLen) {
		return nil, errors.New("crypto: truncated key ID")
	}
	keyID := KeyID(blob[offset : offset+int(keyIDLen)])
	offset += int(keyIDLen)

	// Parse nonce
	if len(blob) < offset+NonceSize {
		return nil, errors.New("crypto: truncated nonce")
	}
	nonce := blob[offset : offset+NonceSize]
	offset += NonceSize

	// Remaining is ciphertext + GCM tag
	ciphertext := blob[offset:]
	if len(ciphertext) < 16 { // GCM tag is 16 bytes minimum
		return nil, errors.New("crypto: ciphertext too short")
	}

	// Resolve DEK from KMS
	key, err := kms.DecryptDEK(keyID)
	if err != nil {
		return nil, fmt.Errorf("crypto: decrypt DEK: %w", err)
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("crypto: aes new cipher: %w", err)
	}

	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("crypto: gcm new: %w", err)
	}

	// Open verifies the tag and decrypts. Additional data = keyID bytes.
	plaintext, err := aead.Open(nil, nonce, ciphertext, []byte(keyID))
	if err != nil {
		return nil, fmt.Errorf("crypto: gcm open: %w", err)
	}

	return plaintext, nil
}

// ---------- ChunkStore Integration Layer ----------

// Encryptor provides at-rest encryption for the ChunkStore.
// When Enabled is true, Write data is encrypted before hitting disk
// and Read data is decrypted after reading from disk.
type Encryptor struct {
	kms KMS
}

// NewEncryptor creates a new Encryptor with the given KMS.
// If kms is nil, encryption is disabled.
func NewEncryptor(kms KMS) *Encryptor {
	if kms == nil {
		return nil
	}
	return &Encryptor{kms: kms}
}

// Enabled returns true when encryption is active.
func (e *Encryptor) Enabled() bool {
	return e != nil && e.kms != nil
}

// EncryptChunk encrypts data using the active DEK from the KMS.
func (e *Encryptor) EncryptChunk(plaintext []byte) ([]byte, error) {
	if !e.Enabled() {
		return plaintext, nil
	}
	keyID, key, err := e.kms.ActiveDEK()
	if err != nil {
		return nil, fmt.Errorf("crypto: get active DEK: %w", err)
	}
	return Encrypt(plaintext, keyID, key)
}

// DecryptChunk decrypts data using the KMS to resolve the DEK.
func (e *Encryptor) DecryptChunk(blob []byte) ([]byte, error) {
	if !e.Enabled() {
		return blob, nil
	}
	return Decrypt(blob, e.kms)
}
