package index

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"

	"github.com/example/dfs/datanode/storage"
)

// RecoveryCheckpoint is the durable index-owned boundary from which active
// segment replay may safely resume. It is written only after Pebble has been
// flushed and the matching INDEX_SAFE marker has been synced to the segment.
// The stream ID makes the sidecar safe for the small and data streams sharing
// one index directory.
type RecoveryCheckpoint struct {
	FormatVersion uint8
	StreamID      uint8
	SegmentID     storage.SegmentID
	SafeOffset    int64
	SafeSeq       uint64
}

const (
	recoveryCheckpointMagic   uint32 = 0x52435054 // "RCPT"
	recoveryCheckpointVersion uint8  = 1
	recoveryCheckpointSize           = 4 + 1 + 1 + 8 + 8 + 8 + 4
)

var recoveryCheckpointFileOps = struct {
	open   func(string, int, os.FileMode) (*os.File, error)
	write  func(*os.File, []byte) (int, error)
	sync   func(*os.File) error
	close  func(*os.File) error
	rename func(string, string) error
	remove func(string) error
}{
	open:   os.OpenFile,
	write:  func(f *os.File, b []byte) (int, error) { return f.Write(b) },
	sync:   func(f *os.File) error { return f.Sync() },
	close:  func(f *os.File) error { return f.Close() },
	rename: os.Rename,
	remove: os.Remove,
}

// StoreRecoveryCheckpoint atomically publishes a recovery checkpoint for one
// stream. The temporary file is synced before rename and the index directory
// is synced after rename, so a returned checkpoint is durable across restart.
// In-memory indexes deliberately have no restart authority and therefore do
// not publish a sidecar.
func StoreRecoveryCheckpoint(ix *Index, checkpoint RecoveryCheckpoint) (err error) {
	if ix == nil {
		return fmt.Errorf("storage: nil index recovery checkpoint")
	}
	if !ix.persistent {
		return nil
	}
	if checkpoint.FormatVersion == 0 {
		checkpoint.FormatVersion = recoveryCheckpointVersion
	}
	if checkpoint.FormatVersion != recoveryCheckpointVersion || checkpoint.SegmentID == 0 ||
		checkpoint.SafeOffset < int64(storage.SegmentHeaderSize) || checkpoint.SafeSeq == 0 {
		return fmt.Errorf("storage: invalid recovery checkpoint")
	}

	buf := encodeRecoveryCheckpoint(checkpoint)
	path := recoveryCheckpointPath(ix.dir, checkpoint.StreamID)
	tmp := path + ".tmp"
	f, err := recoveryCheckpointFileOps.open(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	published := false
	closeTemp := func() error {
		closeErr := recoveryCheckpointFileOps.close(f)
		f = nil
		return closeErr
	}
	defer func() {
		if f != nil {
			err = errors.Join(err, closeTemp())
		}
		if !published {
			_ = recoveryCheckpointFileOps.remove(tmp)
		}
	}()
	if n, writeErr := recoveryCheckpointFileOps.write(f, buf[:]); writeErr != nil {
		err = writeErr
		return err
	} else if n != len(buf) {
		err = io.ErrShortWrite
		return err
	}
	if err = recoveryCheckpointFileOps.sync(f); err != nil {
		return err
	}
	if err = closeTemp(); err != nil {
		return err
	}
	if err = recoveryCheckpointFileOps.rename(tmp, path); err != nil {
		return err
	}
	published = true
	if err := syncDir(ix.dir); err != nil {
		return err
	}
	return nil
}

// LoadRecoveryCheckpoint loads the stream's published checkpoint. Absence is
// safe and means replay starts from the segment header. Any malformed sidecar
// is returned as an error so startup never trusts corrupted safety metadata.
func LoadRecoveryCheckpoint(ix *Index, streamID uint8) (*RecoveryCheckpoint, error) {
	if ix == nil || !ix.persistent {
		return nil, nil
	}
	data, err := os.ReadFile(recoveryCheckpointPath(ix.dir, streamID))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	checkpoint, err := decodeRecoveryCheckpoint(data)
	if err != nil {
		return nil, err
	}
	if checkpoint.StreamID != streamID {
		return nil, fmt.Errorf("storage: recovery checkpoint stream mismatch: got %d want %d", checkpoint.StreamID, streamID)
	}
	return &checkpoint, nil
}

func recoveryCheckpointPath(dir string, streamID uint8) string {
	return filepath.Join(dir, fmt.Sprintf("recovery-checkpoint-%d", streamID))
}

func encodeRecoveryCheckpoint(checkpoint RecoveryCheckpoint) [recoveryCheckpointSize]byte {
	var buf [recoveryCheckpointSize]byte
	binary.BigEndian.PutUint32(buf[0:4], recoveryCheckpointMagic)
	buf[4] = checkpoint.FormatVersion
	buf[5] = checkpoint.StreamID
	binary.BigEndian.PutUint64(buf[6:14], uint64(checkpoint.SegmentID))
	binary.BigEndian.PutUint64(buf[14:22], uint64(checkpoint.SafeOffset))
	binary.BigEndian.PutUint64(buf[22:30], checkpoint.SafeSeq)
	binary.BigEndian.PutUint32(buf[30:34], crc32.ChecksumIEEE(buf[0:30]))
	return buf
}

func decodeRecoveryCheckpoint(data []byte) (RecoveryCheckpoint, error) {
	if len(data) != recoveryCheckpointSize {
		return RecoveryCheckpoint{}, fmt.Errorf("storage: invalid recovery checkpoint size %d", len(data))
	}
	if binary.BigEndian.Uint32(data[0:4]) != recoveryCheckpointMagic {
		return RecoveryCheckpoint{}, fmt.Errorf("storage: invalid recovery checkpoint magic")
	}
	if data[4] != recoveryCheckpointVersion {
		return RecoveryCheckpoint{}, fmt.Errorf("storage: unsupported recovery checkpoint version %d", data[4])
	}
	if got, want := binary.BigEndian.Uint32(data[30:34]), crc32.ChecksumIEEE(data[0:30]); got != want {
		return RecoveryCheckpoint{}, fmt.Errorf("storage: recovery checkpoint crc mismatch")
	}
	checkpoint := RecoveryCheckpoint{
		FormatVersion: data[4],
		StreamID:      data[5],
		SegmentID:     storage.SegmentID(binary.BigEndian.Uint64(data[6:14])),
		SafeOffset:    int64(binary.BigEndian.Uint64(data[14:22])),
		SafeSeq:       binary.BigEndian.Uint64(data[22:30]),
	}
	if checkpoint.SegmentID == 0 || checkpoint.SafeOffset < int64(storage.SegmentHeaderSize) || checkpoint.SafeSeq == 0 {
		return RecoveryCheckpoint{}, fmt.Errorf("storage: invalid recovery checkpoint fields")
	}
	return checkpoint, nil
}
