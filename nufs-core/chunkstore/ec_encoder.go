package chunkstore

import (
	"fmt"
	"io"
	"sync"

	"github.com/klauspost/reedsolomon"
)

// ECEncoder provides erasure coding using klauspost/reedsolomon.
// SIMD-accelerated (AVX2/NEON). Thread-safe — the underlying
// reedsolomon.Encoder is stateless and can be shared across goroutines.
type ECEncoder struct {
	DataShards   int // K
	ParityShards int // M
	enc          reedsolomon.Encoder
}

// ECResult holds the output of an EC encode operation.
type ECResult struct {
	DataShards   [][]byte
	ParityShards [][]byte
	TotalShards  int
}

// NewECEncoder creates a new erasure coder with the given data and parity shard counts.
func NewECEncoder(dataShards, parityShards int) *ECEncoder {
	enc, err := reedsolomon.New(dataShards, parityShards,
		reedsolomon.WithAutoGoroutines(0),
		reedsolomon.WithMaxGoroutines(4),
	)
	if err != nil {
		panic(fmt.Sprintf("ec: invalid parameters data=%d parity=%d: %v", dataShards, parityShards, err))
	}
	return &ECEncoder{
		DataShards:   dataShards,
		ParityShards: parityShards,
		enc:          enc,
	}
}

// ecEncoderCache avoids allocating a new ECEncoder on every read/write.
// Key: "K-M" (e.g. "4-2"); safe for concurrent use.
var ecEncoderCache sync.Map

// GetECEncoder returns a cached ECEncoder for the given shard counts.
func GetECEncoder(dataShards, parityShards int) *ECEncoder {
	key := fmt.Sprintf("%d-%d", dataShards, parityShards)
	if v, ok := ecEncoderCache.Load(key); ok {
		return v.(*ECEncoder)
	}
	enc := NewECEncoder(dataShards, parityShards)
	ecEncoderCache.Store(key, enc)
	return enc
}

// Encode splits data into K data shards and computes M parity shards.
// The data is padded to a multiple of K shard size.
func (ec *ECEncoder) Encode(data []byte) (*ECResult, error) {
	k := ec.DataShards
	m := ec.ParityShards

	shardSize := (len(data) + k - 1) / k
	paddedLen := shardSize * k
	padded := make([]byte, paddedLen)
	copy(padded, data)

	// Split into shards — data shards slice into padded (no copy),
	// parity shards get their own buffers.
	shards := make([][]byte, k+m)
	for i := 0; i < k; i++ {
		shards[i] = padded[i*shardSize : (i+1)*shardSize]
	}
	for j := k; j < k+m; j++ {
		shards[j] = make([]byte, shardSize)
	}

	// Compute parity
	if err := ec.enc.Encode(shards); err != nil {
		return nil, fmt.Errorf("ec: encode: %w", err)
	}

	result := &ECResult{
		DataShards:   shards[:k],
		ParityShards: shards[k:],
		TotalShards:  k + m,
	}
	return result, nil
}

// EncodeStream encodes data from a reader, producing shards of the given size.
func (ec *ECEncoder) EncodeStream(r io.Reader, shardSize int) (*ECResult, error) {
	k := ec.DataShards
	m := ec.ParityShards

	shards := make([][]byte, k+m)
	for i := 0; i < k; i++ {
		shards[i] = make([]byte, shardSize)
		n, err := io.ReadFull(r, shards[i])
		if err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
			return nil, fmt.Errorf("ec: read shard %d: %w", i, err)
		}
		for b := n; b < shardSize; b++ {
			shards[i][b] = 0
		}
	}
	for j := k; j < k+m; j++ {
		shards[j] = make([]byte, shardSize)
	}

	if err := ec.enc.Encode(shards); err != nil {
		return nil, fmt.Errorf("ec: encode: %w", err)
	}

	result := &ECResult{
		DataShards:   shards[:k],
		ParityShards: shards[k:],
		TotalShards:  k + m,
	}
	return result, nil
}

// Decode reconstructs data from available shards.
// shardPresent[i] = true means shards[i] has valid data.
// shards[i] should be nil for missing shards.
func (ec *ECEncoder) Decode(shards [][]byte, shardPresent []bool, originalLen int) ([]byte, error) {
	k := ec.DataShards
	m := ec.ParityShards

	if len(shards) != k+m || len(shardPresent) != k+m {
		return nil, fmt.Errorf("ec: shard count mismatch (got %d, want %d)", len(shards), k+m)
	}

	available := 0
	for _, p := range shardPresent {
		if p {
			available++
		}
	}
	if available < k {
		return nil, fmt.Errorf("ec: insufficient shards (have %d, need %d)", available, k)
	}

	// Prepare shards: nil out missing ones for reedsolomon
	rsShards := make([][]byte, k+m)
	for i := 0; i < k+m; i++ {
		if shardPresent[i] {
			rsShards[i] = shards[i]
		}
	}

	// Reconstruct missing data shards
	if err := ec.enc.Reconstruct(rsShards); err != nil {
		return nil, fmt.Errorf("ec: reconstruct: %w", err)
	}

	// Verify integrity
	ok, err := ec.enc.Verify(rsShards)
	if err != nil {
		return nil, fmt.Errorf("ec: verify: %w", err)
	}
	if !ok {
		return nil, fmt.Errorf("ec: verification failed — data may be corrupted")
	}

	// Concatenate data shards and trim to original length
	result := make([]byte, 0, originalLen)
	for i := 0; i < k; i++ {
		result = append(result, rsShards[i]...)
	}
	if len(result) < originalLen {
		return nil, fmt.Errorf("ec: reconstructed data too short (%d < %d)", len(result), originalLen)
	}
	return result[:originalLen], nil
}

// Verify checks parity consistency of an ECResult.
func (ec *ECEncoder) Verify(result *ECResult) bool {
	k := ec.DataShards
	shards := make([][]byte, k+ec.ParityShards)
	copy(shards[:k], result.DataShards)
	copy(shards[k:], result.ParityShards)

	ok, err := ec.enc.Verify(shards)
	if err != nil {
		return false
	}
	return ok
}
