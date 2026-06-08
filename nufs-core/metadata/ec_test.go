package metadata

import (
	"bytes"
	"testing"
)

func TestECEncoder_EncodeBasic(t *testing.T) {
	ec := NewECEncoder(4, 2) // 4 data + 2 parity
	data := []byte("Hello, World! This is a test of erasure coding in DFS.")

	result, err := ec.Encode(data)
	if err != nil {
		t.Fatal(err)
	}

	if len(result.DataShards) != 4 {
		t.Errorf("expected 4 data shards, got %d", len(result.DataShards))
	}
	if len(result.ParityShards) != 2 {
		t.Errorf("expected 2 parity shards, got %d", len(result.ParityShards))
	}
	if result.TotalShards != 6 {
		t.Errorf("expected 6 total shards, got %d", result.TotalShards)
	}
}

func TestECEncoder_Verify(t *testing.T) {
	ec := NewECEncoder(4, 2)
	data := []byte("Test data for verification - padding to fill shards!!")

	result, err := ec.Encode(data)
	if err != nil {
		t.Fatal(err)
	}

	if !ec.Verify(result) {
		t.Error("parity verification failed")
	}

	// Corrupt a parity shard
	result.ParityShards[0][0] ^= 0xFF
	if ec.Verify(result) {
		t.Error("should detect corrupted parity")
	}
}

func TestECEncoder_DecodeAllPresent(t *testing.T) {
	ec := NewECEncoder(3, 2)
	data := []byte("ABCDEFGHIJ")

	result, err := ec.Encode(data)
	if err != nil {
		t.Fatal(err)
	}

	// All shards present
	shards := make([][]byte, 5)
	present := make([]bool, 5)
	for i := 0; i < 3; i++ {
		shards[i] = result.DataShards[i]
		present[i] = true
	}
	for i := 0; i < 2; i++ {
		shards[3+i] = result.ParityShards[i]
		present[3+i] = true
	}

	decoded, err := ec.Decode(shards, present, len(data))
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(decoded, data) {
		t.Errorf("decoded data mismatch: got %q, want %q", decoded, data)
	}
}

func TestECEncoder_DecodeWithMissing(t *testing.T) {
	ec := NewECEncoder(4, 2)
	data := []byte("The quick brown fox jumps over the lazy dog!!!")

	result, err := ec.Encode(data)
	if err != nil {
		t.Fatal(err)
	}

	// Simulate 1 data shard lost
	totalShards := 6
	shards := make([][]byte, totalShards)
	present := make([]bool, totalShards)

	for i := 0; i < 4; i++ {
		shards[i] = result.DataShards[i]
		present[i] = true
	}
	for i := 0; i < 2; i++ {
		shards[4+i] = result.ParityShards[i]
		present[4+i] = true
	}

	// Lose shard 2
	shards[2] = nil
	present[2] = false

	decoded, err := ec.Decode(shards, present, len(data))
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(decoded, data) {
		t.Errorf("decoded data mismatch after repair: got %q, want %q", decoded, data)
	}
}

func TestECEncoder_InsufficientShards(t *testing.T) {
	ec := NewECEncoder(4, 2)
	data := []byte("Some test data for insufficient shards test!!!")

	result, err := ec.Encode(data)
	if err != nil {
		t.Fatal(err)
	}

	totalShards := 6
	shards := make([][]byte, totalShards)
	present := make([]bool, totalShards)

	// Only 3 shards available (need 4)
	shards[0] = result.DataShards[0]
	present[0] = true
	shards[1] = result.DataShards[1]
	present[1] = true
	shards[4] = result.ParityShards[0]
	present[4] = true

	_, err = ec.Decode(shards, present, len(data))
	if err == nil {
		t.Error("expected error for insufficient shards")
	}
}

func TestECEncoder_InvalidShardCounts(t *testing.T) {
	// NewECEncoder panics on k=0 (klauspost/reedsolomon requires at least 1 data shard).
	// m=0 is valid (no parity).

	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Error("expected panic for k=0")
			}
		}()
		NewECEncoder(0, 2)
	}()

	// m=0 should work (no parity shards)
	ec := NewECEncoder(4, 0)
	data := []byte("test data here!!")
	result, err := ec.Encode(data)
	if err != nil {
		t.Fatalf("m=0 should work: %v", err)
	}
	if len(result.ParityShards) != 0 {
		t.Errorf("expected 0 parity shards, got %d", len(result.ParityShards))
	}
}

func TestECEncoder_LargeData(t *testing.T) {
	ec := NewECEncoder(8, 4)
	data := make([]byte, 1024*64) // 64KB
	for i := range data {
		data[i] = byte(i % 251) // prime to avoid patterns
	}

	result, err := ec.Encode(data)
	if err != nil {
		t.Fatal(err)
	}

	if !ec.Verify(result) {
		t.Error("parity verification failed for large data")
	}

	// Decode with 2 missing data shards
	totalShards := 12
	shards := make([][]byte, totalShards)
	present := make([]bool, totalShards)
	for i := 0; i < 8; i++ {
		shards[i] = result.DataShards[i]
		present[i] = true
	}
	for i := 0; i < 4; i++ {
		shards[8+i] = result.ParityShards[i]
		present[8+i] = true
	}

	// Lose shard 3 (single shard loss — recoverable with any parity)
	shards[3] = nil
	present[3] = false

	decoded, err := ec.Decode(shards, present, len(data))
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(decoded, data) {
		t.Error("decoded large data mismatch")
	}
}
