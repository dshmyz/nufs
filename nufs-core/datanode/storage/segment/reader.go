package segment

import (
	"io"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/example/dfs/datanode/storage"
	"github.com/example/dfs/datanode/storage/encryption"
)

// readerAt is the subset of *os.File the reader needs. It exists so
// tests can substitute a counting implementation and assert how many
// payload bytes a range read actually pulls off disk (§19).
type readerAt interface {
	io.ReaderAt
	io.Closer
}

// Reader reads and validates records from a segment via pread.
// Range reads authenticate and checksum only the frames they return,
// never an entire large extent (V2.1 §8, §19).
type Reader struct {
	f readerAt
	// Path to the segment file.
	path string
	// SizeBytes is the current file size (may grow for active segments);
	// used to bound reads. Atomic because a descriptor-cache reader is
	// shared across concurrent reads and may be refreshed when the active
	// segment grows.
	sizeBytes atomic.Int64
	// enc is the record encryption registry (nil = plaintext).
	enc *encryption.KeyRegistry
}

// OpenReader opens a segment for reading.
func OpenReader(path string) (*Reader, error) {
	return OpenReaderWithEnc(path, nil)
}

// OpenReaderWithEnc opens a segment for reading with an encryption
// registry so encrypted records can be decrypted.
func OpenReaderWithEnc(path string, enc *encryption.KeyRegistry) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	sealed := filepath.Base(filepath.Dir(path)) == "sealed"
	if !sealed {
		sealed, err = hasEncodedSegmentFooter(f, st.Size())
		if err != nil {
			f.Close()
			return nil, err
		}
	}
	if sealed {
		if _, err := ValidateSealedSegment(f); err != nil {
			f.Close()
			return nil, err
		}
	}
	r := &Reader{f: f, path: path, enc: enc}
	r.sizeBytes.Store(st.Size())
	return r, nil
}

// recordLayout is the metadata needed to locate and authenticate any
// single frame of a record: everything except the frame payloads
// themselves. Reading it costs the header, the frame index, and the
// trailer — never the payload — so a range read can resolve which
// frames it needs without paying for the whole extent.
type recordLayout struct {
	header       RecordHeader
	frameIndex   FrameIndex
	firstFrameAt int64
	totalStored  int64
	decKey       []byte
}

// ReadPayloadFrames reads and validates the full record payload,
// verifying the header, frame index, every frame CRC, and the framing.
func (r *Reader) ReadPayloadFrames(offset int64, storedLen uint32, logicalLen uint32) ([]byte, error) {
	layout, err := r.readRecordLayout(offset, storedLen, logicalLen)
	if err != nil {
		return nil, err
	}
	payload := make([]byte, logicalLen)
	// Unencrypted AND uncompressed record: pread each frame straight into
	// the payload in place, one copy per frame off disk with no temp buffer
	// and no final append (zero-copy fast path).
	if layout.decKey == nil && recordIsPlain(layout) {
		write := 0
		for i := range layout.frameIndex.Entries {
			e := layout.frameIndex.Entries[i]
			if int64(e.Offset)+int64(e.StoredLen) > layout.totalStored {
				return nil, storage.ErrIndexCorrupt
			}
			dst := payload[write : write+int(e.StoredLen)]
			if _, err := r.f.ReadAt(dst, layout.firstFrameAt+int64(e.Offset)); err != nil {
				return nil, err
			}
			if err := VerifyFrameCRC(dst, e.CRC); err != nil {
				return nil, err
			}
			write += int(e.StoredLen)
		}
		if write != int(logicalLen) {
			return nil, storage.ErrIndexCorrupt
		}
		return payload, nil
	}
	write := 0
	for i := range layout.frameIndex.Entries {
		logical, err := r.readLogicalFrame(layout, i)
		if err != nil {
			return nil, err
		}
		write += copy(payload[write:], logical)
	}
	if write != int(logicalLen) {
		return nil, storage.ErrIndexCorrupt
	}
	return payload, nil
}

// ReadRangeFrames reads a sub-range of the logical payload, fetching
// and authenticating only the intersecting frames (§8). Read
// amplification is bounded by the requested length plus at most two
// partially-overlapped frames (§19): frames outside the range are
// never pulled off disk.
func (r *Reader) ReadRangeFrames(offset int64, storedLen uint32, logicalLen uint32, logicalOffset int64, length int32) ([]byte, error) {
	// An out-of-range request used to fall back to returning the ENTIRE
	// extent, which both breaks the amplification bound and hands the
	// caller bytes it never asked for. Reject nonsense outright; clamp an
	// overshooting length to the end of the payload, as a tail read at
	// EOF legitimately asks for more than remains (POSIX short read).
	if length <= 0 || logicalOffset < 0 || logicalOffset >= int64(logicalLen) {
		return nil, storage.ErrInvalidRange
	}
	if logicalOffset+int64(length) > int64(logicalLen) {
		length = int32(int64(logicalLen) - logicalOffset)
	}

	layout, err := r.readRecordLayout(offset, storedLen, logicalLen)
	if err != nil {
		return nil, err
	}

	// Frames partition the LOGICAL payload at fixed frameSize boundaries
	// (see BuildFrames), so the intersecting frames are computed from the
	// logical offsets alone — no need to decode preceding frames.
	frameSize := int64(layout.header.EffectiveFrameSize())
	first := int(logicalOffset / frameSize)
	last := int((logicalOffset + int64(length) - 1) / frameSize)
	if first < 0 || last >= len(layout.frameIndex.Entries) {
		return nil, storage.ErrIndexCorrupt
	}

	out := make([]byte, length)
	write := 0
	for i := first; i <= last; i++ {
		e := layout.frameIndex.Entries[i]
		if int64(e.Offset)+int64(e.StoredLen) > layout.totalStored {
			return nil, storage.ErrIndexCorrupt
		}
		frameStart := int64(i) * frameSize
		lo := logicalOffset - frameStart
		if lo < 0 {
			lo = 0
		}
		hi := logicalOffset + int64(length) - frameStart
		// Zero-copy fast path: a frame that is entirely inside the request
		// AND stored verbatim (unencrypted, uncompressed) is pread straight
		// into `out` at its final position — no temp frame buffer and no
		// trailing append. Anything else (an overlapped boundary frame, or
		// one needing decrypt/decompress) falls through to the buffered path
		// so CRC/checksum semantics are byte-for-byte unchanged.
		if layout.decKey == nil && e.Codec == storage.CompressionNone && lo == 0 && hi >= int64(e.StoredLen) {
			dst := out[write : write+int(e.StoredLen)]
			if _, err := r.f.ReadAt(dst, layout.firstFrameAt+int64(e.Offset)); err != nil {
				return nil, err
			}
			if err := VerifyFrameCRC(dst, e.CRC); err != nil {
				return nil, err
			}
			write += int(e.StoredLen)
			continue
		}
		logical, err := r.readLogicalFrame(layout, i)
		if err != nil {
			return nil, err
		}
		if hi > int64(len(logical)) {
			hi = int64(len(logical))
		}
		if lo >= hi {
			return nil, storage.ErrIndexCorrupt
		}
		write += copy(out[write:], logical[lo:hi])
	}
	if write != int(length) {
		return nil, storage.ErrIndexCorrupt
	}
	return out, nil
}

// readLogicalFrame reads exactly one frame off disk and returns its
// logical bytes, verifying its CRC and then decrypting and
// decompressing it (§5.3: both are per-frame and independent).
func (r *Reader) readLogicalFrame(layout *recordLayout, i int) ([]byte, error) {
	e := layout.frameIndex.Entries[i]
	if int64(e.Offset)+int64(e.StoredLen) > layout.totalStored {
		return nil, storage.ErrIndexCorrupt
	}
	frame := make([]byte, e.StoredLen)
	if _, err := r.f.ReadAt(frame, layout.firstFrameAt+int64(e.Offset)); err != nil {
		return nil, err
	}
	if err := VerifyFrameCRC(frame, e.CRC); err != nil {
		return nil, err
	}
	if layout.decKey != nil {
		open, err := DecryptFrame(layout.decKey, frame)
		if err != nil {
			return nil, err
		}
		frame = open
	}
	if e.Codec == storage.CompressionZstd {
		return DecompressFrame(frame)
	}
	return frame, nil
}

// recordIsPlain reports whether every frame in the record is stored
// uncompressed. Combined with decKey == nil at the call site, this means
// the whole record is stored verbatim (stored bytes == logical bytes), so
// it can be read straight into the destination buffer with no per-frame
// temp allocation and no second copy.
func recordIsPlain(layout *recordLayout) bool {
	for _, e := range layout.frameIndex.Entries {
		if e.Codec != storage.CompressionNone {
			return false
		}
	}
	return true
}

// readRecordLayout reads and verifies the header, frame index, and
// trailer of a record. It deliberately does NOT read frame payloads:
// callers fetch only the frames they need via readLogicalFrame.
func (r *Reader) readRecordLayout(offset int64, storedLen uint32, logicalLen uint32) (*recordLayout, error) {
	// Read the header first to learn the frame layout.
	if offset < int64(SegmentHeaderSize) {
		return nil, storage.ErrIndexCorrupt
	}
	hb := make([]byte, RecordHeaderSize)
	if _, err := r.f.ReadAt(hb, offset); err != nil {
		return nil, err
	}
	var header RecordHeader
	if err := header.Decode(hb); err != nil {
		return nil, err
	}
	if header.LogicalLen != logicalLen {
		return nil, storage.ErrIndexCorrupt
	}

	frameCount := int(header.FrameCount)
	indexBytes := frameCount * FrameIndexEntrySize
	idxBuf := make([]byte, indexBytes)
	if _, err := r.f.ReadAt(idxBuf, offset+int64(RecordHeaderSize)); err != nil {
		return nil, err
	}
	var fi FrameIndex
	if err := fi.Decode(idxBuf, header.FrameIndexCRC); err != nil {
		return nil, err
	}

	// Frames start after the index. Sum the stored frame lengths; this
	// must match the header's stored length.
	firstFrameOff := offset + int64(RecordHeaderSize) + int64(indexBytes)
	var totalStored int64
	for _, e := range fi.Entries {
		totalStored += int64(e.StoredLen)
	}
	if totalStored != int64(storedLen) {
		return nil, storage.ErrIndexCorrupt
	}
	if firstFrameOff+totalStored+int64(RecordTrailerSize) > r.sizeBytes.Load() {
		// The cached size may be stale for an ACTIVE segment that has been
		// appended since the reader was opened (the reader cache holds one
		// descriptor per segment; sealed segments never change, active ones
		// grow). Refresh the live size before declaring corruption — a false
		// ErrIndexCorrupt here would make freshly-written records unreadable.
		if sz, err := r.refreshSize(); err == nil {
			if firstFrameOff+totalStored+int64(RecordTrailerSize) > sz {
				return nil, storage.ErrIndexCorrupt
			}
		} else {
			return nil, err
		}
	}

	var decKey []byte
	if header.KeyID != 0 {
		if r.enc == nil {
			return nil, storage.ErrDecryptFailed
		}
		var err error
		decKey, err = r.enc.ResolveNumeric(header.KeyID)
		if err != nil {
			return nil, err
		}
	}

	// Verify the trailer: it binds the framing length, so a truncated or
	// mis-sized record is rejected before any frame is trusted.
	trailerOff := firstFrameOff + totalStored
	tr := make([]byte, RecordTrailerSize)
	if _, err := r.f.ReadAt(tr, trailerOff); err != nil {
		return nil, err
	}
	var trailer RecordTrailer
	if err := trailer.Decode(tr); err != nil {
		return nil, err
	}
	framing := RecordHeaderSize + indexBytes + int(totalStored) + RecordTrailerSize
	if trailer.FramingLen != uint32(framing) {
		return nil, storage.ErrIndexCorrupt
	}

	return &recordLayout{
		header:       header,
		frameIndex:   fi,
		firstFrameAt: firstFrameOff,
		totalStored:  totalStored,
		decKey:       decKey,
	}, nil
}

// Close closes the reader.
func (r *Reader) Close() error { return r.f.Close() }

// Size returns the segment file size in bytes.
func (r *Reader) Size() int64 { return r.sizeBytes.Load() }

// statter is implemented by *os.File to report its current size; fakes
// used in tests may omit it, in which case refreshSize is a no-op.
type statter interface {
	Stat() (os.FileInfo, error)
}

// refreshSize re-stats the underlying file and updates the cached size.
// Used when the cached size is stale for an ACTIVE segment that grew after
// the reader was opened (the descriptor cache holds one reader per segment;
// sealed segments never change, active ones are appended to).
func (r *Reader) refreshSize() (int64, error) {
	sf, ok := r.f.(statter)
	if !ok {
		return r.sizeBytes.Load(), nil
	}
	st, err := sf.Stat()
	if err != nil {
		return r.sizeBytes.Load(), err
	}
	r.sizeBytes.Store(st.Size())
	return r.sizeBytes.Load(), nil
}
