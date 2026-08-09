package segment

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/dshmyz/nufs/nufs-core/datanode/storage"
)

func TestValidateSealedSegmentAcceptsWriterCRC(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sealed.seg")
	w, err := OpenWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	header := SegmentHeader{Magic: storage.SegmentMagic, Version: storage.FormatVersion, ID: 7, SegmentClass: storage.SegmentSmall}
	headerBytes := make([]byte, SegmentHeaderSize)
	if err := header.Encode(headerBytes); err != nil {
		_ = w.Close()
		t.Fatal(err)
	}
	if _, err := w.WriteAt(headerBytes, 0); err != nil {
		_ = w.Close()
		t.Fatal(err)
	}
	body := []byte("sealed segment body")
	footerOffset := int64(SegmentHeaderSize + len(body))
	if _, err := w.WriteAt(body, int64(SegmentHeaderSize)); err != nil {
		_ = w.Close()
		t.Fatal(err)
	}
	footer := &SegmentFooter{Magic: storage.SegmentMagic, Version: storage.FormatVersion, RecordCount: 1, TotalPayload: uint64(len(body))}
	if err := w.WriteFooter(footerOffset, footer); err != nil {
		_ = w.Close()
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if footer.SegmentCRC == 0 {
		t.Fatal("writer left sealed SegmentCRC unset")
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := ValidateSealedSegment(f); err != nil {
		t.Fatalf("valid sealed segment rejected: %v", err)
	}
}

func TestOpenReaderRejectsSealedSegmentCRCFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "7.seg")
	w, err := OpenWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	header := SegmentHeader{Magic: storage.SegmentMagic, Version: storage.FormatVersion, ID: 7, SegmentClass: storage.SegmentSmall}
	headerBytes := make([]byte, SegmentHeaderSize)
	if err := header.Encode(headerBytes); err != nil {
		_ = w.Close()
		t.Fatal(err)
	}
	if _, err := w.WriteAt(headerBytes, 0); err != nil {
		_ = w.Close()
		t.Fatal(err)
	}
	if _, err := w.WriteAt([]byte("body"), int64(SegmentHeaderSize)); err != nil {
		_ = w.Close()
		t.Fatal(err)
	}
	if err := w.WriteFooter(SegmentHeaderSize+int64(len("body")), &SegmentFooter{Magic: storage.SegmentMagic, Version: storage.FormatVersion}); err != nil {
		_ = w.Close()
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte{0xff}, info.Size()-1); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := OpenReader(path)
	if r != nil {
		_ = r.Close()
		t.Fatal("opened a sealed segment with a corrupted SegmentCRC field")
	}
	if !errors.Is(err, storage.ErrChecksumMismatch) {
		t.Fatalf("open error = %v, want checksum mismatch", err)
	}
}

func TestValidateSealedSegmentRejectsCoveredCorruptionAndUnsetCRC(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sealed.seg")
	w, err := OpenWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	header := SegmentHeader{Magic: storage.SegmentMagic, Version: storage.FormatVersion, ID: 7, SegmentClass: storage.SegmentSmall}
	headerBytes := make([]byte, SegmentHeaderSize)
	if err := header.Encode(headerBytes); err != nil {
		_ = w.Close()
		t.Fatal(err)
	}
	if _, err := w.WriteAt(headerBytes, 0); err != nil {
		_ = w.Close()
		t.Fatal(err)
	}
	body := []byte("sealed segment body")
	footerOffset := int64(SegmentHeaderSize + len(body))
	if _, err := w.WriteAt(body, int64(SegmentHeaderSize)); err != nil {
		_ = w.Close()
		t.Fatal(err)
	}
	if err := w.WriteFooter(footerOffset, &SegmentFooter{Magic: storage.SegmentMagic, Version: storage.FormatVersion, RecordCount: 1, TotalPayload: uint64(len(body))}); err != nil {
		_ = w.Close()
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	valid, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	for name, mutate := range map[string]func([]byte){
		"header":         func(b []byte) { b[0] ^= 0x80 },
		"body":           func(b []byte) { b[SegmentHeaderSize] ^= 0x80 },
		"footer covered": func(b []byte) { b[len(b)-SegmentFooterSize+60] ^= 0x80 },
		"crc field":      func(b []byte) { b[len(b)-1] ^= 0x80 },
		"unset crc":      func(b []byte) { copy(b[len(b)-4:], []byte{0, 0, 0, 0}) },
	} {
		t.Run(name, func(t *testing.T) {
			corrupt := append([]byte(nil), valid...)
			mutate(corrupt)
			if err := os.WriteFile(path, corrupt, 0644); err != nil {
				t.Fatal(err)
			}
			f, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			_, err = ValidateSealedSegment(f)
			if closeErr := f.Close(); err == nil {
				err = closeErr
			}
			if err == nil {
				t.Fatal("corrupt sealed segment accepted")
			}
			if name == "crc field" || name == "unset crc" {
				if !errors.Is(err, storage.ErrChecksumMismatch) {
					t.Fatalf("validation error = %v, want checksum mismatch", err)
				}
			}
		})
	}
}
