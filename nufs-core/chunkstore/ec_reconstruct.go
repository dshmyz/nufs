package chunkstore

import (
	"fmt"
)

// EC 6+3 degraded-read helpers (V2.1 §14): any six shards reconstruct
// the data. The reconstruction is a pure function of the shards and the
// original length, so it can be shared by the read path and repair.

// EC6Plus3DataShards / EC6Plus3ParityShards are the 6+3 configuration.
const (
	EC6Plus3DataShards   = 6
	EC6Plus3ParityShards = 3
)

// ReconstructEC63 rebuilds the original data from any EC6Plus3DataShards
// surviving shards. shards[i] is nil/empty when shard i is missing;
// present shards are the raw (padded) shard bytes as produced by
// Encode. Returns the original (unpadded) data.
//
// The shard list must have length 9 (6 data + 3 parity); at least 6
// shards must be present to reconstruct (§14: "Any six shards
// reconstruct data").
func ReconstructEC63(shards [][]byte, originalLen int) ([]byte, error) {
	enc := GetECEncoder(EC6Plus3DataShards, EC6Plus3ParityShards)
	reconstructed, err := enc.Decode(shards, shardPresence(shards), originalLen)
	if err != nil {
		return nil, fmt.Errorf("ec63 reconstruct: %w", err)
	}
	return reconstructed, nil
}

// shardPresence builds the presence vector for Decode.
func shardPresence(shards [][]byte) []bool {
	present := make([]bool, len(shards))
	for i, s := range shards {
		present[i] = len(s) > 0
	}
	return present
}
