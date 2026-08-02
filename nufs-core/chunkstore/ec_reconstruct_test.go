package chunkstore

import (
	"bytes"
	"testing"
)

// TestReconstructEC63_DropThree verifies that any 6 of 9 shards
// reconstruct the original data (§14: "Any six shards reconstruct
// data"). We drop 3 different shards in turn and reconstruct each time.
func TestReconstructEC63_DropThree(t *testing.T) {
	enc := GetECEncoder(EC6Plus3DataShards, EC6Plus3ParityShards)
	original := bytes.Repeat([]byte("ec-63-payload"), 4096) // 52 KiB

	res, err := enc.Encode(original)
	if err != nil {
		t.Fatal(err)
	}
	if res.TotalShards != 9 {
		t.Fatalf("total shards = %d, want 9", res.TotalShards)
	}

	// Drop 3 shards (the maximum tolerated), reconstruct from 6.
	// Try several combinations: 3 data, 3 parity, mixed.
	cases := [][]int{
		{0, 1, 2}, // first 3 data shards
		{6, 7, 8}, // all 3 parity shards
		{2, 5, 8}, // mixed
	}
	for _, drop := range cases {
		shards := make([][]byte, 9)
		for i := 0; i < 6; i++ {
			shards[i] = append([]byte(nil), res.DataShards[i]...)
		}
		for i := 0; i < 3; i++ {
			shards[6+i] = append([]byte(nil), res.ParityShards[i]...)
		}
		for _, d := range drop {
			shards[d] = nil
		}
		got, err := ReconstructEC63(shards, len(original))
		if err != nil {
			t.Fatalf("reconstruct after dropping %v: %v", drop, err)
		}
		if !bytes.Equal(got, original) {
			t.Fatalf("reconstruction mismatch after dropping %v", drop)
		}
	}
}

// TestReconstructEC63_TooFewFails verifies fewer than 6 shards cannot
// reconstruct.
func TestReconstructEC63_TooFewFails(t *testing.T) {
	enc := GetECEncoder(EC6Plus3DataShards, EC6Plus3ParityShards)
	original := []byte("short data")
	res, err := enc.Encode(original)
	if err != nil {
		t.Fatal(err)
	}
	// Keep only 5 shards.
	shards := make([][]byte, 9)
	for i := 0; i < 5; i++ {
		shards[i] = append([]byte(nil), res.DataShards[i]...)
	}
	if _, err := ReconstructEC63(shards, len(original)); err == nil {
		t.Fatal("reconstruct with 5 shards should fail")
	}
}
