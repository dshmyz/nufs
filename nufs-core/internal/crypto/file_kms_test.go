package crypto

import (
	"os"
	"path/filepath"
	"testing"
)

func TestFileKMS_RoundTripAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	keyFile := filepath.Join(dir, "kek.key")
	cfg := FileKMSConfig{KeyFile: keyFile}
	dataDir := filepath.Join(dir, "data")

	// First instance: generates and persists a DEK.
	k1, err := NewFileKMS(cfg, dataDir)
	if err != nil {
		t.Fatalf("NewFileKMS 1: %v", err)
	}
	id1, plain1, err := k1.GenerateDEK()
	if err != nil {
		t.Fatalf("GenerateDEK: %v", err)
	}
	if len(plain1) != 32 {
		t.Fatalf("DEK len = %d, want 32", len(plain1))
	}

	// Second instance over the SAME KEK file + data dir: must recover the
	// same DEK from disk (proves keys survive a process restart).
	k2, err := NewFileKMS(cfg, dataDir)
	if err != nil {
		t.Fatalf("NewFileKMS 2: %v", err)
	}
	got, err := k2.DecryptDEK(id1)
	if err != nil {
		t.Fatalf("DecryptDEK after restart: %v", err)
	}
	if string(got) != string(plain1) {
		t.Errorf("DEK changed across restart: got %x want %x", got, plain1)
	}

	// ActiveDEK on the restarted instance returns the persisted active key.
	aid, akey, err := k2.ActiveDEK()
	if err != nil {
		t.Fatalf("ActiveDEK after restart: %v", err)
	}
	if aid != id1 {
		t.Errorf("ActiveDEK id = %q, want %q", aid, id1)
	}
	if string(akey) != string(plain1) {
		t.Errorf("ActiveDEK key differs from generated DEK")
	}
}

func TestFileKMS_KeyFilePerm0600(t *testing.T) {
	dir := t.TempDir()
	keyFile := filepath.Join(dir, "kek.key")
	dataDir := filepath.Join(dir, "data")
	if _, err := NewFileKMS(FileKMSConfig{KeyFile: keyFile}, dataDir); err != nil {
		t.Fatalf("NewFileKMS: %v", err)
	}
	fi, err := os.Stat(keyFile)
	if err != nil {
		t.Fatalf("stat key file: %v", err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Errorf("KEK file mode = %#o, want 0600", fi.Mode().Perm())
	}
	// The active DEK record and pointer should also be 0600.
	kdir := FileKMSPath(dataDir)
	ents, _ := os.ReadDir(kdir)
	for _, e := range ents {
		fi, err := os.Stat(filepath.Join(kdir, e.Name()))
		if err != nil {
			continue
		}
		if fi.Mode().Perm() != 0o600 && e.Name() != "." && e.Name() != ".." {
			t.Errorf("kms record %s mode = %#o, want 0600", e.Name(), fi.Mode().Perm())
		}
	}
}

func TestFileKMS_WrongKEKFails(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")

	k1, err := NewFileKMS(FileKMSConfig{KeyFile: filepath.Join(dir, "kek1.key")}, dataDir)
	if err != nil {
		t.Fatalf("NewFileKMS 1: %v", err)
	}
	id1, _, err := k1.GenerateDEK()
	if err != nil {
		t.Fatalf("GenerateDEK: %v", err)
	}

	// A different KEK cannot unwrap the DEK (GCM authentication fails).
	k2, err := NewFileKMS(FileKMSConfig{KeyFile: filepath.Join(dir, "kek2.key")}, dataDir)
	if err != nil {
		t.Fatalf("NewFileKMS 2: %v", err)
	}
	if _, err := k2.DecryptDEK(id1); err == nil {
		t.Error("DecryptDEK with wrong KEK succeeded, want error")
	}
}

func TestFileKMS_EnvAndHexSources(t *testing.T) {
	dir := t.TempDir()

	// KeyEnv: a 64-hex-char env var.
	const hexKey = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"
	t.Setenv("NUFS_TEST_KEK", hexKey)
	k, err := NewFileKMS(FileKMSConfig{KeyEnv: "NUFS_TEST_KEK"}, filepath.Join(dir, "d1"))
	if err != nil {
		t.Fatalf("env KEK: %v", err)
	}
	id, plain, err := k.GenerateDEK()
	if err != nil || len(plain) != 32 {
		t.Fatalf("env KEK GenerateDEK: id=%v err=%v", id, err)
	}
	if _, err := k.DecryptDEK(id); err != nil {
		t.Fatalf("env KEK DecryptDEK: %v", err)
	}

	// KeyHex: direct hex string.
	k2, err := NewFileKMS(FileKMSConfig{KeyHex: hexKey}, filepath.Join(dir, "d2"))
	if err != nil {
		t.Fatalf("hex KEK: %v", err)
	}
	if _, _, err := k2.GenerateDEK(); err != nil {
		t.Fatalf("hex KEK GenerateDEK: %v", err)
	}

	// No source → error.
	if _, err := NewFileKMS(FileKMSConfig{}, filepath.Join(dir, "d3")); err == nil {
		t.Error("no KEK source should error")
	}
}

func TestFileKMS_RawEnvKey(t *testing.T) {
	dir := t.TempDir()
	// A raw 32-byte KEK held directly in an env var. Env values cannot
	// contain NUL or '=', so use printable bytes (the practical contract is
	// hex-encoded keys via env; raw is allowed when it is env-safe).
	raw := []byte("0123456789abcdef0123456789abcdef")
	if len(raw) != 32 {
		t.Fatalf("raw key len = %d", len(raw))
	}
	t.Setenv("NUFS_TEST_RAWKEK", string(raw))
	k, err := NewFileKMS(FileKMSConfig{KeyEnv: "NUFS_TEST_RAWKEK"}, filepath.Join(dir, "d"))
	if err != nil {
		t.Fatalf("raw env KEK: %v", err)
	}
	if _, _, err := k.GenerateDEK(); err != nil {
		t.Fatalf("GenerateDEK: %v", err)
	}
}

func TestFileKMS_ActiveDEKIdempotent(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	keyFile := filepath.Join(dir, "kek.key")

	k, err := NewFileKMS(FileKMSConfig{KeyFile: keyFile}, dataDir)
	if err != nil {
		t.Fatalf("NewFileKMS: %v", err)
	}
	id1, p1, err := k.ActiveDEK()
	if err != nil {
		t.Fatalf("ActiveDEK 1: %v", err)
	}
	id2, p2, err := k.ActiveDEK()
	if err != nil {
		t.Fatalf("ActiveDEK 2: %v", err)
	}
	if id1 != id2 || string(p1) != string(p2) {
		t.Errorf("ActiveDEK not idempotent: %q/%q", id1, id2)
	}

	// Restart still yields the same active key.
	k2, err := NewFileKMS(FileKMSConfig{KeyFile: keyFile}, dataDir)
	if err != nil {
		t.Fatalf("NewFileKMS restart: %v", err)
	}
	id3, p3, err := k2.ActiveDEK()
	if err != nil {
		t.Fatalf("ActiveDEK 3: %v", err)
	}
	if id3 != id1 || string(p3) != string(p1) {
		t.Errorf("active DEK changed across restart: %q vs %q", id3, id1)
	}
}
