package segment

import (
	"bytes"
	"sync"

	"github.com/klauspost/compress/zstd"

	"github.com/example/dfs/datanode/storage"
	"github.com/example/dfs/datanode/storage/encryption"
)

// Re-export the encryption helpers so callers can seal/unseal frames
// without importing the encryption package at every call site.
var (
	EncryptFrame = encryption.EncryptFrame
	DecryptFrame = encryption.DecryptFrame
)

// Compression implements per-frame zstd compression with sampling
// (V2.1 §9):
//
//   - files smaller than CompressionNoCompressionThreshold (4 KiB) are
//     never compressed;
//   - files from 4 KiB through SmallFileThreshold (64 KiB) are sampled
//     and compressed only when the estimated savings are at least
//     CompressionMinSavingsRatio (10%);
//   - larger extent records are compressed per-frame (frames are the
//     compression unit).
//
// Frame offsets for compressed records come from the checksummed frame
// index; uncompressed offsets are computed directly (§5.3).
type Compression struct {
	enc *zstd.Encoder
	dec *zstd.Decoder
}

// zstd pool to reuse encoders/decoders. Each is constructed once and
// guarded by a mutex: zstd.Encoder/Decoder are not safe for concurrent
// use, and the storage write/read paths call them from multiple
// goroutines.
var (
	zstdEncOnce sync.Once
	zstdDecOnce sync.Once
	zstdEnc     *zstd.Encoder
	zstdDec     *zstd.Decoder
	zstdMu      sync.Mutex
)

func getZstdEncoder() *zstd.Encoder {
	zstdEncOnce.Do(func() {
		zstdEnc, _ = zstd.NewWriter(nil)
	})
	return zstdEnc
}

func getZstdDecoder() *zstd.Decoder {
	zstdDecOnce.Do(func() {
		zstdDec, _ = zstd.NewReader(nil)
	})
	return zstdDec
}

// CompressFrame compresses a frame payload with zstd. Returns the
// compressed bytes and whether compression was beneficial.
func CompressFrame(payload []byte) ([]byte, bool) {
	zstdMu.Lock()
	out := getZstdEncoder().EncodeAll(payload, nil)
	zstdMu.Unlock()
	// Only use compression if it actually saves space.
	if len(out) >= len(payload) {
		return payload, false
	}
	return out, true
}

// DecompressFrame decompresses a zstd frame payload.
func DecompressFrame(payload []byte) ([]byte, error) {
	zstdMu.Lock()
	defer zstdMu.Unlock()
	return getZstdDecoder().DecodeAll(payload, nil)
}

// ShouldCompress decides whether to compress a logical payload based on
// size and a sampling estimate (V2.1 §9). Smaller than the threshold:
// never. Within the sampled range: compress only if estimated savings
// meet the minimum ratio.
func ShouldCompress(logicalLen int, sample []byte) bool {
	if logicalLen < storage.CompressionNoCompressionThreshold {
		return false
	}
	if logicalLen > storage.SmallFileThreshold {
		// Extent records are always compressed per-frame.
		return true
	}
	// Sampled range (4 KiB..64 KiB): compress if the sample saves ≥10%.
	zstdMu.Lock()
	compressed := getZstdEncoder().EncodeAll(sample, nil)
	zstdMu.Unlock()
	if len(compressed) >= len(sample) {
		return false
	}
	savings := 1.0 - float64(len(compressed))/float64(len(sample))
	return savings >= storage.CompressionMinSavingsRatio
}

// SampledBytes returns a bounded sample of a payload for the sampling
// decision.
func SampledBytes(payload []byte, max int) []byte {
	if len(payload) <= max {
		return payload
	}
	return payload[:max]
}

// CompressionStats reports compression outcomes (for metrics §17).
type CompressionStats struct {
	FramesIn  int64
	FramesOut int64
	BytesIn   int64
	BytesOut  int64
}

// frameCompressor compresses one frame and returns the stored (possibly
// compressed) frame bytes plus the codec used.
func frameCompress(frame []byte, forceZstd bool) ([]byte, storage.CompressionCodec) {
	if forceZstd {
		out, ok := CompressFrame(frame)
		if ok {
			return out, storage.CompressionZstd
		}
	}
	return frame, storage.CompressionNone
}

// BuildFramedRecord splits a logical payload into frames, optionally
// compresses and encrypts each frame, and builds the frame index with
// per-frame stored lengths, offsets, and codecs. The frame index CRC is
// computed by the caller via Encode.
//
// Encryption is applied after compression, per frame: the stored frame
// is nonce || ciphertext || tag, and its CRC covers the encrypted bytes.
// When encKey is non-nil, every frame is sealed.
func BuildFramedRecord(payload []byte, frameSize int, compressed bool, encKey []byte) ([]byte, *FrameIndex, storage.CompressionCodec, uint64, error) {
	if frameSize <= 0 {
		frameSize = DefaultFrameSize
	}
	n := (len(payload) + frameSize - 1) / frameSize
	if n == 0 {
		// Empty payload: one empty frame.
		n = 1
	}
	codec := storage.CompressionNone
	if compressed {
		codec = storage.CompressionZstd
	}
	fi := &FrameIndex{Entries: make([]FrameIndexEntry, 0, n)}
	var stored []byte
	offset := uint32(0)
	for i := 0; i < n; i++ {
		start := i * frameSize
		end := start + frameSize
		if end > len(payload) {
			end = len(payload)
		}
		frame := payload[start:end]
		frameCodec := storage.CompressionNone
		storedFrame := frame
		if compressed {
			if cf, ok := CompressFrame(frame); ok {
				storedFrame = cf
				frameCodec = storage.CompressionZstd
			}
		}
		if encKey != nil {
			sealed, err := EncryptFrame(encKey, storedFrame)
			if err != nil {
				return nil, nil, 0, 0, err
			}
			storedFrame = sealed
		}
		fi.Entries = append(fi.Entries, FrameIndexEntry{
			Offset:    offset,
			StoredLen: uint32(len(storedFrame)),
			Codec:     frameCodec,
			CRC:       storage.CRC32C(storedFrame),
		})
		offset += uint32(len(storedFrame))
		stored = append(stored, storedFrame...)
	}
	return stored, fi, codec, 0, nil
}

var _ = bytes.Equal // reserved
