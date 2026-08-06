package crypto

// FileKMS is a production-usable KMS backed by a key-encryption-key (KEK)
// held in a local file, an environment variable, or an explicit hex string.
//
// It implements envelope encryption: each generated DEK (data encryption key)
// is wrapped with the KEK using AES-256-GCM and persisted to `<dataDir>/kms/`
// on disk, so the DEKs survive process restarts. The KEK itself never touches
// disk unless the operator chooses --kms-key-file; with --kms-key-env or
// --kms-key-hex the KEK is supplied out-of-band (env var / command line) and
// only the wrapped DEKs are stored. This makes at-rest encryption usable in
// production where an in-memory LocalKMS would lose its keys on restart.
//
// Reuse and rotation are left to the operator: the active DEK pointer is
// persisted so decrypting old chunks (which embed their DEK id) stays possible
// across restarts and DEK rotation.

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// FileKMSConfig selects the KEK (key-encryption-key) source. Exactly one is
// used, in this priority order: KeyFile, then KeyEnv, then KeyHex. The KEK
// must be a 32-byte AES-256 key (raw bytes; env/hex may be given as hex).
type FileKMSConfig struct {
	// KeyFile is a path to a 32-byte raw KEK file. If the file does not
	// exist it is created with 0600 permissions and filled with a fresh
	// random key on first startup; if it exists it must hold exactly 32
	// bytes.
	KeyFile string
	// KeyEnv names an environment variable holding the KEK (32 raw bytes
	// or 64 hex characters).
	KeyEnv string
	// KeyHex is an explicit 64-hex-character KEK (dev/test convenience).
	KeyHex string
}

// FileKMS implements the KMS interface with KEK-wrapped, on-disk DEKs.
type FileKMS struct {
	mu       sync.RWMutex
	dek      []byte // 32-byte key-encryption-key (KEK)
	dir      string // <dataDir>/kms — where wrapped DEKs live
	activeID KeyID
	seq      uint64 // monotonic DEK counter, initialized from existing records
}

// FileKMSPath returns the directory where a FileKMS rooted at dataDir stores
// its wrapped DEK records.
func FileKMSPath(dataDir string) string {
	return filepath.Join(dataDir, "kms")
}

var dekIDRe = regexp.MustCompile(`^dek-([0-9]+)\.bin$`)

// NewFileKMS builds a FileKMS rooted at dataDir using the KEK selected by cfg.
// It creates the kms/ directory, loads any existing active-DEK pointer, and
// scans existing records to continue the DEK counter across restarts.
func NewFileKMS(cfg FileKMSConfig, dataDir string) (*FileKMS, error) {
	kek, err := resolveKEK(cfg)
	if err != nil {
		return nil, err
	}
	dir := FileKMSPath(dataDir)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("crypto: kms dir %s: %w", dir, err)
	}
	k := &FileKMS{dek: kek, dir: dir}
	if err := k.scanExisting(); err != nil {
		return nil, err
	}
	// Load the persisted active-DEK pointer if present (best effort: the
	// pointer may legitimately be absent on first startup).
	if id, err := os.ReadFile(filepath.Join(dir, "active.dek")); err == nil {
		k.activeID = KeyID(strings.TrimSpace(string(id)))
	}
	return k, nil
}

// scanExisting walks the kms/ directory, collecting the next DEK sequence
// number from any existing dek-*.bin records.
func (k *FileKMS) scanExisting() error {
	entries, err := os.ReadDir(k.dir)
	if err != nil {
		return fmt.Errorf("crypto: scan kms dir: %w", err)
	}
	max := uint64(0)
	for _, e := range entries {
		m := dekIDRe.FindStringSubmatch(e.Name())
		if m == nil {
			continue
		}
		n, err := strconv.ParseUint(m[1], 10, 64)
		if err != nil {
			continue
		}
		if n > max {
			max = n
		}
	}
	k.seq = max
	return nil
}

// resolveKEK returns the 32-byte KEK from the configured source.
func resolveKEK(cfg FileKMSConfig) ([]byte, error) {
	var raw []byte
	var err error
	switch {
	case cfg.KeyFile != "":
		raw, err = kekFromFile(cfg.KeyFile)
	case cfg.KeyEnv != "":
		v := os.Getenv(cfg.KeyEnv)
		if v == "" {
			return nil, fmt.Errorf("crypto: kms key env %q is empty", cfg.KeyEnv)
		}
		raw, err = parseKEK([]byte(v))
		if err != nil {
			return nil, fmt.Errorf("crypto: kms key env %q: %w", cfg.KeyEnv, err)
		}
	case cfg.KeyHex != "":
		raw, err = hex.DecodeString(cfg.KeyHex)
		if err != nil {
			return nil, fmt.Errorf("crypto: kms key hex: %w", err)
		}
		if len(raw) != 32 {
			return nil, fmt.Errorf("crypto: kms key hex must be 64 hex chars (32 bytes), got %d", len(raw))
		}
	default:
		return nil, errors.New("crypto: no KEK source configured (set kms-key-file, kms-key-env, or kms-key-hex)")
	}
	if err != nil {
		return nil, err
	}
	if len(raw) != 32 {
		return nil, fmt.Errorf("crypto: KEK must be 32 bytes (AES-256), got %d", len(raw))
	}
	return raw, nil
}

// kekFromFile reads a 32-byte KEK file, creating it with 0600 permissions and
// a fresh random key on first startup.
func kekFromFile(path string) ([]byte, error) {
	if b, err := os.ReadFile(path); err == nil {
		if len(b) != 32 {
			return nil, fmt.Errorf("crypto: kms key file %s must contain exactly 32 bytes, got %d", path, len(b))
		}
		return b, nil
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("crypto: read kms key file %s: %w", path, err)
	}
	// First startup: create a fresh key with restrictive permissions.
	key := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		return nil, fmt.Errorf("crypto: generate KEK: %w", err)
	}
	if err := os.WriteFile(path, key, 0o600); err != nil {
		return nil, fmt.Errorf("crypto: write kms key file %s: %w", path, err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		return nil, fmt.Errorf("crypto: chmod kms key file %s: %w", path, err)
	}
	return key, nil
}

// parseKEK accepts 32 raw bytes or 64 hex characters.
func parseKEK(b []byte) ([]byte, error) {
	if len(b) == 32 {
		return b, nil
	}
	if len(b) == 64 {
		out, err := hex.DecodeString(string(b))
		if err != nil {
			return nil, fmt.Errorf("invalid hex KEK: %w", err)
		}
		return out, nil
	}
	return nil, fmt.Errorf("KEK must be 32 raw bytes or 64 hex chars, got %d", len(b))
}

// GenerateDEK creates a fresh DEK, wraps it with the KEK, persists the wrapped
// record, and makes it the active DEK.
func (k *FileKMS) GenerateDEK() (KeyID, []byte, error) {
	plain := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, plain); err != nil {
		return "", nil, fmt.Errorf("crypto: generate DEK: %w", err)
	}

	k.mu.Lock()
	defer k.mu.Unlock()
	k.seq++
	id := KeyID(fmt.Sprintf("dek-%d", k.seq))
	if err := k.persistDEKLocked(id, plain); err != nil {
		return "", nil, err
	}
	if err := k.writeActiveLocked(id); err != nil {
		return "", nil, err
	}
	return id, plain, nil
}

// DecryptDEK reads the wrapped DEK record for id and unseals it with the KEK.
func (k *FileKMS) DecryptDEK(id KeyID) ([]byte, error) {
	if id == "" {
		return nil, errors.New("crypto: empty DEK id")
	}
	blob, err := os.ReadFile(k.dekPath(id))
	if err != nil {
		return nil, fmt.Errorf("crypto: read DEK record %s: %w", id, err)
	}
	return unwrapDEK(blob, id, k.dek)
}

// ActiveDEK returns the current active DEK, generating and persisting one on
// first use.
func (k *FileKMS) ActiveDEK() (KeyID, []byte, error) {
	k.mu.RLock()
	active := k.activeID
	k.mu.RUnlock()
	if active == "" {
		return k.GenerateDEK()
	}
	plain, err := k.DecryptDEK(active)
	if err != nil {
		return "", nil, err
	}
	return active, plain, nil
}

func (k *FileKMS) dekPath(id KeyID) string {
	// The id is a scan-controlled "dek-N" value; path-safety is implied by
	// the generator, but guard against crafted ids anyway.
	if strings.ContainsAny(string(id), "/\\") {
		return filepath.Join(k.dir, "invalid")
	}
	return filepath.Join(k.dir, string(id)+".bin")
}

// persistDEKLocked wraps plain with the KEK and writes it under id. Caller
// holds k.mu.
func (k *FileKMS) persistDEKLocked(id KeyID, plain []byte) error {
	blob, err := wrapDEK(plain, id, k.dek)
	if err != nil {
		return err
	}
	path := k.dekPath(id)
	if err := os.WriteFile(path, blob, 0o600); err != nil {
		return fmt.Errorf("crypto: write DEK record %s: %w", id, err)
	}
	return nil
}

// writeActiveLocked persists the active-DEK pointer. Caller holds k.mu.
func (k *FileKMS) writeActiveLocked(id KeyID) error {
	k.activeID = id
	if err := os.WriteFile(filepath.Join(k.dir, "active.dek"), []byte(id), 0o600); err != nil {
		return fmt.Errorf("crypto: write active DEK pointer: %w", err)
	}
	return nil
}

// wrapDEK seals plain with the KEK using AES-256-GCM. The on-disk record is:
//
//	[2 bytes: id length (big-endian)]
//	[N bytes: id]
//	[12 bytes: GCM nonce]
//	[remaining: ciphertext + GCM tag (16 bytes)]
//
// The id is bound as GCM additional data, so a record cannot be re-associated
// with a different id without failing authentication.
func wrapDEK(plain []byte, id KeyID, kek []byte) ([]byte, error) {
	block, err := aes.NewCipher(kek)
	if err != nil {
		return nil, fmt.Errorf("crypto: aes new cipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("crypto: gcm new: %w", err)
	}
	nonce := make([]byte, NonceSize)
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("crypto: generate nonce: %w", err)
	}
	idBytes := []byte(id)
	rec := make([]byte, 0, KeyIDLenSize+len(idBytes)+NonceSize+len(plain)+aead.Overhead())
	rec = binary.BigEndian.AppendUint16(rec, uint16(len(idBytes)))
	rec = append(rec, idBytes...)
	rec = append(rec, nonce...)
	ct := aead.Seal(nil, nonce, plain, idBytes)
	rec = append(rec, ct...)
	return rec, nil
}

// unwrapDEK parses a wrapped DEK record and unseals it with the KEK, verifying
// the GAAD-bound id matches.
func unwrapDEK(rec []byte, wantID KeyID, kek []byte) ([]byte, error) {
	if len(rec) < KeyIDLenSize {
		return nil, errors.New("crypto: DEK record too short")
	}
	idLen := binary.BigEndian.Uint16(rec[:KeyIDLenSize])
	off := KeyIDLenSize
	if len(rec) < off+int(idLen) {
		return nil, errors.New("crypto: DEK record truncated id")
	}
	id := KeyID(rec[off : off+int(idLen)])
	if id != wantID {
		return nil, fmt.Errorf("crypto: DEK record id %q does not match expected %q", id, wantID)
	}
	off += int(idLen)
	if len(rec) < off+NonceSize {
		return nil, errors.New("crypto: DEK record truncated nonce")
	}
	nonce := rec[off : off+NonceSize]
	ct := rec[off+NonceSize:]
	if len(ct) < 16 {
		return nil, errors.New("crypto: DEK record ciphertext too short")
	}
	block, err := aes.NewCipher(kek)
	if err != nil {
		return nil, fmt.Errorf("crypto: aes new cipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("crypto: gcm new: %w", err)
	}
	plain, err := aead.Open(nil, nonce, ct, []byte(id))
	if err != nil {
		return nil, fmt.Errorf("crypto: KEK failed to unwrap DEK (wrong key?): %w", err)
	}
	return plain, nil
}

// ListDEKIDs returns the persisted DEK ids in kms/ (used by tests and tooling).
func (k *FileKMS) ListDEKIDs() ([]KeyID, error) {
	entries, err := os.ReadDir(k.dir)
	if err != nil {
		return nil, fmt.Errorf("crypto: list kms dir: %w", err)
	}
	var ids []KeyID
	for _, e := range entries {
		if m := dekIDRe.FindStringSubmatch(e.Name()); m != nil {
			ids = append(ids, KeyID(e.Name()[:len(e.Name())-len(".bin")]))
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids, nil
}
