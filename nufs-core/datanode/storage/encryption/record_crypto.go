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
	"hash/fnv"
	"io"
	"strconv"
	"strings"
	"sync"

	"github.com/dshmyz/nufs/nufs-core/datanode/storage"
	"github.com/dshmyz/nufs/nufs-core/internal/crypto"
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
	kms   crypto.KMS
	mu    sync.RWMutex
	cache map[crypto.KeyID][]byte
	// numericToID maps the numeric key ID stored in record headers to
	// the string KeyID used by the KMS.
	numericToID map[uint64]crypto.KeyID
	idToNumeric map[crypto.KeyID]uint64
}

// NewKeyRegistry creates a registry backed by a KMS. It eagerly primes the
// active DEK's numeric↔string mapping so record reads resolve immediately
// after a process restart (the active key is what new data was written with).
func NewKeyRegistry(kms crypto.KMS) *KeyRegistry {
	r := &KeyRegistry{
		kms:         kms,
		cache:       make(map[crypto.KeyID][]byte),
		numericToID: make(map[uint64]crypto.KeyID),
		idToNumeric: make(map[crypto.KeyID]uint64),
	}
	if kms != nil {
		// Prime the active key's mapping. KeyIDs from FileKMS/LocalKMS are
		// "name-N", whose stable numeric is the N suffix, so a fresh registry
		// derives the same numeric as the one that wrote the records.
		if id, _, err := kms.ActiveDEK(); err == nil {
			r.numericOf(id)
		}
	}
	return r
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

// numericFor derives a stable, deterministic numeric ID for a KeyID so that
// any registry process—across restarts—maps the string id to the same numeric
// stored in record headers. For the "name-N" ids emitted by LocalKMS and
// FileKMS the numeric is the decimal N suffix. For any other string it falls
// back to a stable FNV-1a hash (never 0, which means plaintext).
func numericFor(id crypto.KeyID) uint64 {
	s := string(id)
	if i := strings.LastIndexByte(s, '-'); i >= 0 && i+1 < len(s) {
		if n, err := strconv.ParseUint(s[i+1:], 10, 64); err == nil && n > 0 {
			return n
		}
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	n := h.Sum64()
	if n == 0 {
		n = 1
	}
	return n
}

// numericOf returns the stable numeric ID for a KeyID, recording the
// numeric↔string mapping for Reverse lookup.
func (r *KeyRegistry) numericOf(id crypto.KeyID) uint64 {
	n := numericFor(id)
	r.mu.Lock()
	defer r.mu.Unlock()
	if existing, ok := r.numericToID[n]; ok && existing != id {
		// Deterministic collision: two ids hashing to the same numeric. This
		// cannot happen for the "name-N" form (suffix is unique per id); it
		// would only matter for a pathological hash collision on non-suffixed
		// ids, which is not a production path. Keep the first mapping and log
		// nothing—the collision is theoretically possible but astronomically
		// unlikely (FNV-1a 64-bit).
		return n
	}
	r.numericToID[n] = id
	r.idToNumeric[id] = n
	return n
}

// KeyEnumerator is the optional interface a KMS may implement to enumerate its
// known DEK ids. KeyRegistry uses it to rebuild the numeric→string mapping on
// a fresh process, so records written under a rotated (non-active) key still
// resolve after restart. FileKMS implements it.
type KeyEnumerator interface {
	ListDEKIDs() ([]crypto.KeyID, error)
}

// ResolveNumeric returns the AES-256 key for a numeric record-header
// key ID, resolving through the string KeyID. On a fresh process the
// mapping is rebuilt from the KMS's known ids when the numeric is not
// already known.
func (r *KeyRegistry) ResolveNumeric(numeric uint64) ([]byte, error) {
	if r == nil || r.kms == nil {
		return nil, nil
	}
	r.mu.RLock()
	id, ok := r.numericToID[numeric]
	r.mu.RUnlock()
	if ok {
		return r.Resolve(id)
	}
	// Mapping not primed (e.g. a rotated key's record read first). Rebuild
	// from the KMS's enumerable id set if available.
	if enum, ok := r.kms.(KeyEnumerator); ok {
		if ids, err := enum.ListDEKIDs(); err == nil {
			for _, cand := range ids {
				if numericFor(cand) == numeric {
					id = cand
					r.mu.Lock()
					r.numericToID[numeric] = id
					r.idToNumeric[id] = numeric
					r.mu.Unlock()
					return r.Resolve(id)
				}
			}
		}
	}
	return nil, fmt.Errorf("encryption: unknown numeric key id %d", numeric)
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
