package segment

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"

	"github.com/dshmyz/nufs/nufs-core/datanode/storage"
	"github.com/dshmyz/nufs/nufs-core/datanode/storage/encryption"
	"github.com/dshmyz/nufs/nufs-core/internal/crypto"
)

// TestEncryptionRoundtrip verifies per-frame AEAD encryption round-trips
// through write + reopen (recovery replay path).
func TestEncryptionRoundtrip(t *testing.T) {
	kms, err := crypto.NewLocalKMS()
	if err != nil {
		t.Fatal(err)
	}
	reg := encryption.NewKeyRegistry(kms)

	dir := t.TempDir()
	s, err := New(Config{Dir: dir, UseMemIndex: false, Enc: reg})
	if err != nil {
		t.Fatal(err)
	}
	data := bytes.Repeat([]byte("encrypted-secret-payload"), 8192) // compressible + encryptable
	if _, err := s.Write(context.Background(), &storage.WriteRequest{ExtentID: 1, Generation: 1, Data: data}); err != nil {
		t.Fatal(err)
	}
	// In-memory read must decrypt correctly.
	got, err := s.Read(context.Background(), &storage.ReadRequest{ExtentID: 1, Generation: 1})
	if err != nil {
		t.Fatalf("in-memory encrypted read: %v", err)
	}
	if !bytes.Equal(got.Data, data) {
		t.Fatal("in-memory encrypted roundtrip mismatch")
	}
	s.Close()

	// Reopen with the same registry: recovery replay + decrypt.
	s2, err := New(Config{Dir: dir, UseMemIndex: false, Enc: reg})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	got2, err := s2.Read(context.Background(), &storage.ReadRequest{ExtentID: 1, Generation: 1})
	if err != nil {
		t.Fatalf("reopen encrypted read: %v", err)
	}
	if !bytes.Equal(got2.Data, data) {
		t.Fatal("reopen encrypted roundtrip mismatch")
	}
}

// TestEncryptionRoundtripFileKMS proves the FileKMS restart-survivability
// property through the real frame-encryption path: the store writes with one
// FileKMS instance, then is reopened with a *brand-new* FileKMS built over the
// same <dataDir>/kms + KEK file (simulating a process restart). The DEK is
// recovered from disk, so the encrypted bytes decrypt back to the original —
// unlike LocalKMS, which only works because the same in-memory key is reused.
func TestEncryptionRoundtripFileKMS(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "seg")     // segment store data dir
	keyFile := filepath.Join(root, "kek") // KEK file (0600, created on first start)

	// First "process": a FileKMS that generates + persists a DEK.
	kms1, err := crypto.NewFileKMS(crypto.FileKMSConfig{KeyFile: keyFile}, dir)
	if err != nil {
		t.Fatal(err)
	}
	reg1 := encryption.NewKeyRegistry(kms1)
	s1, err := New(Config{Dir: dir, UseMemIndex: false, Enc: reg1})
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("filekms-restart-survivable-write")
	if _, err := s1.Write(context.Background(), &storage.WriteRequest{ExtentID: 1, Generation: 1, Data: data}); err != nil {
		t.Fatal(err)
	}
	if _, err := s1.Read(context.Background(), &storage.ReadRequest{ExtentID: 1, Generation: 1}); err != nil {
		t.Fatalf("in-memory read: %v", err)
	}
	s1.Close()

	// Second "process": a NEW FileKMS over the same KEK file and data dir.
	// No in-memory key is reused — everything comes back from disk.
	kms2, err := crypto.NewFileKMS(crypto.FileKMSConfig{KeyFile: keyFile}, dir)
	if err != nil {
		t.Fatal(err)
	}
	reg2 := encryption.NewKeyRegistry(kms2)
	s2, err := New(Config{Dir: dir, UseMemIndex: false, Enc: reg2})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	got, err := s2.Read(context.Background(), &storage.ReadRequest{ExtentID: 1, Generation: 1})
	if err != nil {
		t.Fatalf("reopen-with-fresh-FileKMS read: %v (DEK not recovered from disk?)", err)
	}
	if !bytes.Equal(got.Data, data) {
		t.Fatal("FileKMS restart roundtrip mismatch")
	}
}

// TestEncryptionWrongKey verifies a decrypt failure never falls back to
// unverified bytes (§3: "Corrupt bytes are never returned as successful
// reads"). A registry with a different KMS must fail the read.
func TestEncryptionWrongKey(t *testing.T) {
	kms1, _ := crypto.NewLocalKMS()
	kms2, _ := crypto.NewLocalKMS()
	reg1 := encryption.NewKeyRegistry(kms1)
	reg2 := encryption.NewKeyRegistry(kms2)

	dir := t.TempDir()
	s, err := New(Config{Dir: dir, UseMemIndex: false, Enc: reg1})
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("encrypted with key one")
	if _, err := s.Write(context.Background(), &storage.WriteRequest{ExtentID: 1, Generation: 1, Data: data}); err != nil {
		t.Fatal(err)
	}
	s.Close()

	// Reopen with a different registry (different key): the read must
	// fail, never return bytes.
	s2, err := New(Config{Dir: dir, UseMemIndex: false, Enc: reg2})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if _, err := s2.Read(context.Background(), &storage.ReadRequest{ExtentID: 1, Generation: 1}); err == nil {
		t.Fatal("read with wrong key must fail, not return unverified bytes")
	}
}

// TestEncryptionFrameFormat verifies the AEAD frame layout is
// nonce || ciphertext || tag with the expected overhead.
func TestEncryptionFrameFormat(t *testing.T) {
	key := bytes.Repeat([]byte{0x42}, 32)
	sealed, err := EncryptFrame(key, []byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	if len(sealed) != len("hello")+encryption.FrameHeaderSize {
		t.Fatalf("sealed len = %d, want %d", len(sealed), len("hello")+encryption.FrameHeaderSize)
	}
	open, err := DecryptFrame(key, sealed)
	if err != nil {
		t.Fatal(err)
	}
	if string(open) != "hello" {
		t.Fatalf("decrypt = %q, want hello", open)
	}
	// Tampering must fail.
	sealed[5] ^= 0xFF
	if _, err := DecryptFrame(key, sealed); err == nil {
		t.Fatal("tampered frame must fail AEAD")
	}
}
