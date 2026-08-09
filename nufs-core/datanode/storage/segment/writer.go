package segment

import (
	"fmt"
	"os"

	"github.com/dshmyz/nufs/nufs-core/datanode/storage/journal"
)

// Writer appends records to an active segment file and provides the
// single durability barrier (fdatasync) of V2.1 §6.1. The caller
// batches records + BatchCommit and syncs once; that one barrier makes
// the whole batch durable.
type Writer struct {
	f *os.File
	// Path to the segment file.
	path string
}

// OpenWriter opens (creating if needed) the segment file for writes.
// The allocator owns offsets, so the file is opened without O_APPEND to
// allow WriteAt.
func OpenWriter(path string) (*Writer, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	return &Writer{f: f, path: path}, nil
}

// WriteRecordFramed writes header + frame index + frames + trailer at
// the given offset (V2.1 §5.3). frameSize is the per-frame payload
// size; payload is split into frames and written sequentially.
func (w *Writer) WriteRecordFramed(offset int64, header *RecordHeader, frameIndex []byte, payload []byte, frameSize int) (int64, error) {
	framing := RecordFraming(header.StoredLen, frameSize, int(header.FrameCount))
	buf := make([]byte, 0, framing)
	hb := make([]byte, RecordHeaderSize)
	if err := header.Encode(hb); err != nil {
		return 0, err
	}
	buf = append(buf, hb...)
	buf = append(buf, frameIndex...)
	buf = append(buf, payload...)
	tb := make([]byte, RecordTrailerSize)
	trailer := RecordTrailer{FramingLen: framing}
	if err := trailer.Encode(tb); err != nil {
		return 0, err
	}
	buf = append(buf, tb...)

	n, err := w.f.WriteAt(buf, offset)
	if err != nil {
		return int64(n), err
	}
	return int64(n), nil
}

// WriteBatchCommit appends a BatchCommit record at the given offset.
// This is the durability point: the following single Sync makes the
// batch durable (§6.1 step 4-5).
func (w *Writer) WriteBatchCommit(offset int64, bc *journal.BatchCommit) error {
	buf := make([]byte, journal.BatchCommitSize)
	if err := bc.Encode(buf); err != nil {
		return err
	}
	_, err := w.f.WriteAt(buf, offset)
	return err
}

// WriteAt writes raw bytes at an offset (used for tombstone markers).
func (w *Writer) WriteAt(buf []byte, offset int64) (int64, error) {
	n, err := w.f.WriteAt(buf, offset)
	return int64(n), err
}

// Sync issues the single durability barrier (fdatasync-equivalent).
func (w *Writer) Sync() error {
	return w.f.Sync()
}

// Close closes the segment file.
func (w *Writer) Close() error {
	return w.f.Close()
}

// Path returns the segment file path.
func (w *Writer) Path() string { return w.path }

// WriteFooter appends the footer at the sealed offset and syncs.
func (w *Writer) WriteFooter(offset int64, footer *SegmentFooter) error {
	info, err := w.f.Stat()
	if err != nil {
		return err
	}
	if info.Size() != offset {
		return fmt.Errorf("storage: seal offset %d does not match segment size %d", offset, info.Size())
	}
	var buf [SegmentFooterSize]byte
	footer.SegmentCRC = 0
	if err := footer.Encode(buf[:]); err != nil {
		return err
	}
	crc, err := sealedSegmentCRC(w.f, offset, buf[:segmentFooterCRCOffset])
	if err != nil {
		return err
	}
	if crc == 0 {
		return fmt.Errorf("storage: refusing to seal segment with unset crc")
	}
	footer.SegmentCRC = crc
	if err := footer.Encode(buf[:]); err != nil {
		return err
	}
	if _, err := w.f.WriteAt(buf[:], offset); err != nil {
		return err
	}
	return w.f.Sync()
}
