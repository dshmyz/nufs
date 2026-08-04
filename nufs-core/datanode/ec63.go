package datanode

import (
	"fmt"
	"sync"

	"github.com/klauspost/reedsolomon"
)

// EC 6+3 (V2.1 §14) local coder.
//
// The datanode package cannot import chunkstore (chunkstore imports datanode,
// so a direct import would be a cycle), so the 6+3 encode/decode used by the
// V2Store aggregate write/read path is implemented here on the underlying
// reedsolomon primitive. It is the same 6+3 layout as the gateway's
// chunkstore coder — six data shards plus three parity shards, any six
// reconstruct the data — but package-scoped to the storage layer that writes
// shard extents.

const (
	// ec63Data / ec63Parity / ec63Shards are the 6+3 configuration (§14).
	ec63Data   = 6
	ec63Parity = 3
	ec63Shards = ec63Data + ec63Parity
)

// ec63Encoder is a cached, thread-safe reedsolomon encoder for 6+3.
// reedsolomon.Encoder is stateless and safe to share across goroutines.
var (
	ec63Once sync.Once
	ec63Enc  reedsolomon.Encoder
	ec63Err  error
)

func ec63RS() (reedsolomon.Encoder, error) {
	ec63Once.Do(func() {
		ec63Enc, ec63Err = reedsolomon.New(ec63Data, ec63Parity,
			reedsolomon.WithAutoGoroutines(0),
			reedsolomon.WithMaxGoroutines(4),
		)
	})
	return ec63Enc, ec63Err
}

// encodeEC63 splits data into six data shards and three parity shards,
// returning all nine in order [0..5]=data, [6..8]=parity. The data is padded
// to a multiple of the shard size.
func encodeEC63(data []byte) ([][]byte, error) {
	enc, err := ec63RS()
	if err != nil {
		return nil, fmt.Errorf("ec63: new encoder: %w", err)
	}
	shardSize := (len(data) + ec63Data - 1) / ec63Data
	paddedLen := shardSize * ec63Data
	padded := make([]byte, paddedLen)
	copy(padded, data)

	shards := make([][]byte, ec63Shards)
	for i := 0; i < ec63Data; i++ {
		shards[i] = padded[i*shardSize : (i+1)*shardSize]
	}
	for j := ec63Data; j < ec63Shards; j++ {
		shards[j] = make([]byte, shardSize)
	}
	if err := enc.Encode(shards); err != nil {
		return nil, fmt.Errorf("ec63: encode: %w", err)
	}
	return shards, nil
}

// decodeEC63 reconstructs the original (unpadded) data from the nine shards.
// shards[i] must hold shard i's bytes (present); at least six shards must be
// present for reconstruction (§14). Returns the original payload of length
// originalLen.
func decodeEC63(shards [][]byte, originalLen int) ([]byte, error) {
	enc, err := ec63RS()
	if err != nil {
		return nil, fmt.Errorf("ec63: new encoder: %w", err)
	}
	if len(shards) != ec63Shards {
		return nil, fmt.Errorf("ec63: shard count %d, want %d", len(shards), ec63Shards)
	}
	available := 0
	for _, s := range shards {
		if len(s) > 0 {
			available++
		}
	}
	if available < ec63Data {
		return nil, fmt.Errorf("ec63: insufficient shards (have %d, need %d)", available, ec63Data)
	}
	rsShards := make([][]byte, ec63Shards)
	for i, s := range shards {
		if len(s) > 0 {
			rsShards[i] = s
		}
	}
	if err := enc.Reconstruct(rsShards); err != nil {
		return nil, fmt.Errorf("ec63: reconstruct: %w", err)
	}
	ok, err := enc.Verify(rsShards)
	if err != nil {
		return nil, fmt.Errorf("ec63: verify: %w", err)
	}
	if !ok {
		return nil, fmt.Errorf("ec63: verification failed — data may be corrupted")
	}
	result := make([]byte, 0, originalLen)
	for i := 0; i < ec63Data; i++ {
		result = append(result, rsShards[i]...)
	}
	if len(result) < originalLen {
		return nil, fmt.Errorf("ec63: reconstructed data too short (%d < %d)", len(result), originalLen)
	}
	return result[:originalLen], nil
}

// reconstructEC63 rebuilds a degraded stripe and returns the full nine-shard
// set (missing shards repopulated) plus the original payload. At least six
// shards must be present (those empty are considered lost). This is the E5
// repair material: the returned shards let the caller write back the lost
// ones to their owning stores (repair/reheat), and the payload is what a
// degraded read returns.
func reconstructEC63(shards [][]byte, originalLen int) ([][]byte, []byte, error) {
	enc, err := ec63RS()
	if err != nil {
		return nil, nil, fmt.Errorf("ec63: new encoder: %w", err)
	}
	if len(shards) != ec63Shards {
		return nil, nil, fmt.Errorf("ec63: shard count %d, want %d", len(shards), ec63Shards)
	}
	available := 0
	for _, s := range shards {
		if len(s) > 0 {
			available++
		}
	}
	if available < ec63Data {
		return nil, nil, fmt.Errorf("ec63: insufficient shards (have %d, need %d)", available, ec63Data)
	}
	rsShards := make([][]byte, ec63Shards)
	for i, s := range shards {
		if len(s) > 0 {
			rsShards[i] = s
		}
	}
	if err := enc.Reconstruct(rsShards); err != nil {
		return nil, nil, fmt.Errorf("ec63: reconstruct: %w", err)
	}
	ok, err := enc.Verify(rsShards)
	if err != nil {
		return nil, nil, fmt.Errorf("ec63: verify: %w", err)
	}
	if !ok {
		return nil, nil, fmt.Errorf("ec63: verification failed — data may be corrupted")
	}
	result := make([]byte, 0, originalLen)
	for i := 0; i < ec63Data; i++ {
		result = append(result, rsShards[i]...)
	}
	if len(result) < originalLen {
		return nil, nil, fmt.Errorf("ec63: reconstructed data too short (%d < %d)", len(result), originalLen)
	}
	// Return copies of the rebuilt set so callers can persist lost shards
	// without aliasing the read buffers.
	rebuilt := make([][]byte, ec63Shards)
	for i := range rsShards {
		rebuilt[i] = append([]byte(nil), rsShards[i]...)
	}
	return rebuilt, result[:originalLen], nil
}
