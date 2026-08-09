package index

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/dshmyz/nufs/nufs-core/datanode/storage"
)

var errCheckpointInjected = errors.New("injected recovery checkpoint failure")

func TestRecoveryCheckpointChecksumRejectsIEEE(t *testing.T) {
	checkpoint := RecoveryCheckpoint{FormatVersion: recoveryCheckpointVersion, StreamID: 1, SegmentID: 7, SafeOffset: storage.SegmentHeaderSize, SafeSeq: 3}
	buf := encodeRecoveryCheckpoint(checkpoint)
	castagnoli := crc32.MakeTable(crc32.Castagnoli)
	if got, want := binary.BigEndian.Uint32(buf[30:34]), crc32.Checksum(buf[0:30], castagnoli); got != want {
		t.Fatalf("checkpoint CRC = %08x, want Castagnoli %08x", got, want)
	}
	if _, err := decodeRecoveryCheckpoint(buf[:]); err != nil {
		t.Fatalf("Castagnoli-encoded checkpoint rejected: %v", err)
	}

	binary.BigEndian.PutUint32(buf[30:34], crc32.ChecksumIEEE(buf[0:30]))
	if _, err := decodeRecoveryCheckpoint(buf[:]); err == nil {
		t.Fatal("IEEE-encoded checkpoint accepted")
	}
}

func TestStoreRecoveryCheckpoint_CleansTemporaryFileOnFailure(t *testing.T) {
	for _, stage := range []string{"write", "sync", "close", "rename"} {
		t.Run(stage, func(t *testing.T) {
			dir := t.TempDir()
			ix, err := Open(Options{Dir: dir})
			if err != nil {
				t.Fatal(err)
			}
			defer ix.Close()

			ops := recoveryCheckpointFileOps
			switch stage {
			case "write":
				recoveryCheckpointFileOps.write = func(*os.File, []byte) (int, error) { return 0, errCheckpointInjected }
			case "sync":
				recoveryCheckpointFileOps.sync = func(*os.File) error { return errCheckpointInjected }
			case "close":
				recoveryCheckpointFileOps.close = func(f *os.File) error {
					_ = f.Close()
					return errCheckpointInjected
				}
			case "rename":
				recoveryCheckpointFileOps.rename = func(string, string) error { return errCheckpointInjected }
			}
			t.Cleanup(func() { recoveryCheckpointFileOps = ops })

			err = StoreRecoveryCheckpoint(ix, RecoveryCheckpoint{StreamID: 0, SegmentID: 1, SafeOffset: storage.SegmentHeaderSize, SafeSeq: 1})
			if !errors.Is(err, errCheckpointInjected) {
				t.Fatalf("error = %v, want injected failure", err)
			}
			if _, err := os.Stat(filepath.Join(dir, "recovery-checkpoint-0.tmp")); !os.IsNotExist(err) {
				t.Fatalf("temporary checkpoint remains after %s failure: %v", stage, err)
			}
		})
	}
}

func TestStoreRecoveryCheckpoint_ShortWriteDoesNotPublish(t *testing.T) {
	dir := t.TempDir()
	ix, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer ix.Close()

	ops := recoveryCheckpointFileOps
	recoveryCheckpointFileOps.write = func(_ *os.File, b []byte) (int, error) {
		return len(b) - 1, nil
	}
	renameCalls := 0
	recoveryCheckpointFileOps.rename = func(oldPath, newPath string) error {
		renameCalls++
		return ops.rename(oldPath, newPath)
	}
	t.Cleanup(func() { recoveryCheckpointFileOps = ops })

	err = StoreRecoveryCheckpoint(ix, RecoveryCheckpoint{StreamID: 0, SegmentID: 1, SafeOffset: storage.SegmentHeaderSize, SafeSeq: 1})
	if !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("error = %v, want io.ErrShortWrite", err)
	}
	if renameCalls != 0 {
		t.Fatalf("rename calls = %d, want 0 after short write", renameCalls)
	}
	if _, err := os.Stat(filepath.Join(dir, "recovery-checkpoint-0")); !os.IsNotExist(err) {
		t.Fatalf("checkpoint published after short write: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "recovery-checkpoint-0.tmp")); !os.IsNotExist(err) {
		t.Fatalf("temporary checkpoint remains after short write: %v", err)
	}
}
