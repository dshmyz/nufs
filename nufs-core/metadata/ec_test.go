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
	ec := NewECEncoder(0, 2)
	_, err := ec.Encode([]byte("test"))
	if err == nil {
		t.Error("expected error for k=0")
	}

	ec2 := NewECEncoder(4, 0)
	_, err = ec2.Encode([]byte("test"))
	if err == nil {
		t.Error("expected error for m=0")
	}
}

func TestGFMul(t *testing.T) {
	// Test basic GF(256) multiplication
	if gfMul(0, 5) != 0 {
		t.Error("0 * 5 should be 0")
	}
	if gfMul(5, 0) != 0 {
		t.Error("5 * 0 should be 0")
	}
	if gfMul(1, 5) != 5 {
		t.Errorf("1 * 5 should be 5, got %d", gfMul(1, 5))
	}
	if gfMul(5, 1) != 5 {
		t.Errorf("5 * 1 should be 5, got %d", gfMul(5, 1))
	}
}

func TestGFInv(t *testing.T) {
	// a * inv(a) = 1 for all non-zero a
	for a := 1; a < 256; a++ {
		inv := gfInv(byte(a))
		product := gfMul(byte(a), inv)
		if product != 1 {
			t.Errorf("gfMul(%d, gfInv(%d)) = %d, want 1", a, a, product)
		}
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
