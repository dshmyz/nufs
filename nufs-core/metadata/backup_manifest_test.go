package metadata

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func validBackupManifest() *BackupManifest {
	return &BackupManifest{
		FormatVersion: BackupFormatVersion,
		BackupID:      "backup-1",
		CreatedAt:     time.Unix(0, 0).UTC(),
		Files: []BackupFile{{
			Path:   "CURRENT",
			Size:   1,
			SHA256: strings.Repeat("a", 64),
		}},
		TotalBytes: 1,
	}
}

func TestBackupManifestRejectsUnsafePaths(t *testing.T) {
	for _, path := range []string{"", "/etc/passwd", "../escape", "a/../../escape", "a\\..\\escape"} {
		t.Run(path, func(t *testing.T) {
			m := validBackupManifest()
			m.Files[0].Path = path
			if err := m.Validate(); err == nil {
				t.Fatalf("Validate(%q) returned nil", path)
			}
		})
	}
}

func TestBackupManifestValidateRejectsInvalidContract(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*BackupManifest)
	}{
		{"unsupported format", func(m *BackupManifest) { m.FormatVersion++ }},
		{"duplicate path", func(m *BackupManifest) { m.Files = append(m.Files, m.Files[0]); m.TotalBytes = 2 }},
		{"non-normalized path", func(m *BackupManifest) { m.Files[0].Path = "dir/../CURRENT" }},
		{"windows volume path", func(m *BackupManifest) { m.Files[0].Path = "C:/escape" }},
		{"negative size", func(m *BackupManifest) { m.Files[0].Size = -1; m.TotalBytes = -1 }},
		{"uppercase checksum", func(m *BackupManifest) { m.Files[0].SHA256 = strings.Repeat("A", 64) }},
		{"short checksum", func(m *BackupManifest) { m.Files[0].SHA256 = strings.Repeat("a", 63) }},
		{"non hex checksum", func(m *BackupManifest) { m.Files[0].SHA256 = strings.Repeat("g", 64) }},
		{"wrong total", func(m *BackupManifest) { m.TotalBytes++ }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			m := validBackupManifest()
			test.mutate(m)
			err := m.Validate()
			if err == nil {
				t.Fatal("Validate returned nil")
			}
			if test.name == "negative size" && !strings.Contains(err.Error(), "negative size") {
				t.Fatalf("Validate error = %v, want negative size", err)
			}
		})
	}
}

func TestBuildBackupManifestSortsFiles(t *testing.T) {
	dir, manifest := createManifestFixture(t)
	for i := 1; i < len(manifest.Files); i++ {
		if manifest.Files[i-1].Path >= manifest.Files[i].Path {
			t.Fatalf("files are not sorted: %q before %q", manifest.Files[i-1].Path, manifest.Files[i].Path)
		}
	}
	if manifest.DurationMillis != 0 {
		t.Fatalf("DurationMillis = %d, want deterministic zero", manifest.DurationMillis)
	}
	again, err := BuildBackupManifest(context.Background(), dir, BackupSnapshotMetadata{
		BackupID:  "fixture",
		CreatedAt: time.Unix(0, 0).UTC(),
	})
	if err != nil {
		t.Fatal(err)
	}
	firstJSON, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	secondJSON, err := json.Marshal(again)
	if err != nil {
		t.Fatal(err)
	}
	if string(firstJSON) != string(secondJSON) {
		t.Fatalf("same checkpoint produced different manifests:\n%s\n%s", firstJSON, secondJSON)
	}
}
