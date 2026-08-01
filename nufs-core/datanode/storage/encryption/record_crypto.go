// Package encryption implements per-frame record encryption for the V2.1
// storage engine (§5.3/§9): each frame carries its own AEAD tag, and the
// record header stores the KeyID. The frame layout is
//
//	frame = nonce || ciphertext || tag
//
// with the key ID kept in the record header (not repeated per frame).
// The KMS resolves KeyID → AES-256 key; key rotation issues a new DEK.
package encryption

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	"github.com/example/dfs/datanode/storage"
	"github.com/example/dfs/internal/crypto"
)

// NonceSize is the AES-GCM nonce length.
const NonceSize = 12

// AEADOverhead is the AES-GCM tag length.
const AEADOverhead = 16

// FrameHeaderSize is the per-frame AEAD framing overhead
// (nonce + tag).
const FrameHeaderSize = NonceSize + AEADOverhead

// KeyRegistry resolves KeyIDs to AES-256 keys. It wraps a crypto.KMS,
// caches decrypted keys for the process lifetime, and maintains the
// numeric-ID ↔ string-ID mapping stored in record headers.
type KeyRegistry struct {
	kms    crypto.KMS
	mu     sync.RWMutex
	cache  map[crypto.KeyID][]byte
	// numericToID maps the numeric key ID stored in record headers to
	// the string KeyID used by the KMS.
	numericToID map[uint64]crypto.KeyID
	idToNumeric map[crypto.KeyID]uint64
	nextNumeric uint64
}

// NewKeyRegistry creates a registry backed by a KMS.
func NewKeyRegistry(kms crypto.KMS) *KeyRegistry {
	return &KeyRegistry{
		kms:         kms,
		cache:       make(map[crypto.KeyID][]byte),
		numericToID: make(map[uint64]crypto.KeyID),
		idToNumeric: make(map[crypto.KeyID]uint64),
		nextNumeric: 1,
	}
}

// ActiveKey returns the current active DEK for new writes, plus its
// numeric ID for the record header.
func (r *KeyRegistry) ActiveKey() (uint64, []byte, error) {
	if r == nil || r.kms == nil {
		return 0, nil, nil
	}
	id, key, err := r.kms.ActiveDEK()
	if err != nil {
		return 0, nil, err
	}
	return r.numericOf(id), key, nil
}

// numericOf assigns (or returns) the stable numeric ID for a KeyID.
func (r *KeyRegistry) numericOf(id crypto.KeyID) uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	if n, ok := r.idToNumeric[id]; ok {
		return n
	}
	n := r.nextNumeric
	r.nextNumeric++
	r.idToNumeric[id] = n
	r.numericToID[n] = id
	return n
}

// ResolveNumeric returns the AES-256 key for a numeric record-header
// key ID, resolving through the string KeyID.
func (r *KeyRegistry) ResolveNumeric(numeric uint64) ([]byte, error) {
	if r == nil || r.kms == nil {
		return nil, nil
	}
	r.mu.RLock()
	id, ok := r.numericToID[numeric]
	r.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("encryption: unknown numeric key id %d", numeric)
	}
	return r.Resolve(id)
}

// Resolve returns the AES-256 key for a KeyID, cached.
func (r *KeyRegistry) Resolve(id crypto.KeyID) ([]byte, error) {
	if r == nil || r.kms == nil {
		return nil, nil
	}
	r.mu.RLock()
	key, ok := r.cache[id]
	r.mu.RUnlock()
	if ok {
		return key, nil
	}
	key, err := r.kms.DecryptDEK(id)
	if err != nil {
		return nil, fmt.Errorf("encryption: resolve key %s: %w", id, err)
	}
	r.mu.Lock()
	r.cache[id] = key
	r.mu.Unlock()
	return key, nil
}

// Enabled reports whether encryption is active.
func (r *KeyRegistry) Enabled() bool {
	return r != nil && r.kms != nil
}

// EncryptFrame seals one frame with AES-GCM. Returns
// nonce || ciphertext || tag.
func EncryptFrame(key []byte, plaintext []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("encryption: aes new: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("encryption: gcm new: %w", err)
	}
	nonce := make([]byte, NonceSize)
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("encryption: nonce: %w", err)
	}
	out := make([]byte, 0, NonceSize+len(plaintext)+AEADOverhead)
	out = append(out, nonce...)
	return aead.Seal(out, nonce, plaintext, nil), nil
}

// DecryptFrame unseals a frame previously sealed by EncryptFrame.
func DecryptFrame(key []byte, blob []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("encryption: aes new: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("encryption: gcm new: %w", err)
	}
	if len(blob) < NonceSize+AEADOverhead {
		return nil, fmt.Errorf("%w: frame too short for AEAD", storage.ErrDecryptFailed)
	}
	nonce := blob[:NonceSize]
	ciphertext := blob[NonceSize:]
	return aead.Open(nil, nonce, ciphertext, nil)
}

// FrameStoredLen returns the stored length of an encrypted frame given
// its plaintext length.
func FrameStoredLen(plainLen int) int {
	return plainLen + FrameHeaderSize
}

// EncodeKeyID writes a 2-byte length-prefixed key ID.
func EncodeKeyID(dst []byte, id crypto.KeyID) (int, error) {
	b := []byte(id)
	if len(b) > 0xFFFF {
		return 0, fmt.Errorf("encryption: key id too long")
	}
	binary.BigEndian.PutUint16(dst[0:2], uint16(len(b)))
	copy(dst[2:], b)
	return 2 + len(b), nil
}

// DecodeKeyID reads a 2-byte length-prefixed key ID.
func DecodeKeyID(src []byte) (crypto.KeyID, int, error) {
	if len(src) < 2 {
		return "", 0, fmt.Errorf("encryption: key id truncated")
	}
	n := int(binary.BigEndian.Uint16(src[0:2]))
	if 2+n > len(src) {
		return "", 0, fmt.Errorf("encryption: key id body truncated")
	}
	return crypto.KeyID(src[2 : 2+n]), 2 + n, nil
}
