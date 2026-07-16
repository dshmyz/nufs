package metadata

import (
	"testing"
)

// ============================================================
// TDD: Write Quorum — Default Sync Write
// ============================================================
// When WriteQuorum > 0, a write must be confirmed by at least
// WriteQuorum replicas before returning success. This prevents
// data loss when the primary replica fails before async replication
// completes.

func TestPlacementPolicy_WriteQuorum_Default(t *testing.T) {
	// When WriteQuorum is not explicitly set, it should default to
	// a safe value: min(ReplicationFactor, 2) for replication,
	// or DataShards for EC.
	tests := []struct {
		name     string
		policy   PlacementPolicy
		expected int
	}{
		{
			name:     "3-replication defaults to quorum 2",
			policy:   PlacementPolicy{ReplicationFactor: 3},
			expected: 2,
		},
		{
			name:     "1-replication defaults to quorum 1",
			policy:   PlacementPolicy{ReplicationFactor: 1},
			expected: 1,
		},
		{
			name:     "2-replication defaults to quorum 2",
			policy:   PlacementPolicy{ReplicationFactor: 2},
			expected: 2,
		},
		{
			name: "EC(4+2) defaults to quorum 4 (data shards)",
			policy: PlacementPolicy{
				ReplicationFactor: 0,
				ECConfig:          &ECConfig{DataShards: 4, ParityShards: 2},
			},
			expected: 4,
		},
		{
			name: "EC(8+4) defaults to quorum 8",
			policy: PlacementPolicy{
				ReplicationFactor: 0,
				ECConfig:          &ECConfig{DataShards: 8, ParityShards: 4},
			},
			expected: 8,
		},
		{
			name: "explicit WriteQuorum overrides default",
			policy: PlacementPolicy{
				ReplicationFactor: 3,
				WriteQuorum:       3,
			},
			expected: 3,
		},
		{
			name: "WriteQuorum=1 means async (fire-and-forget)",
			policy: PlacementPolicy{
				ReplicationFactor: 3,
				WriteQuorum:       1,
			},
			expected: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.policy.EffectiveWriteQuorum()
			if got != tt.expected {
				t.Errorf("EffectiveWriteQuorum() = %d, want %d", got, tt.expected)
			}
		})
	}
}

func TestPlacementPolicy_ReadQuorum(t *testing.T) {
	// Read quorum ensures that a read sees the latest write.
	// For consistency: W + R > N (where N = total replicas)
	tests := []struct {
		name     string
		policy   PlacementPolicy
		expected int
	}{
		{
			name:     "3-replication with quorum-2, read needs 2",
			policy:   PlacementPolicy{ReplicationFactor: 3, WriteQuorum: 2},
			expected: 2, // 2+2 > 3
		},
		{
			name:     "3-replication with quorum-3, read needs 1",
			policy:   PlacementPolicy{ReplicationFactor: 3, WriteQuorum: 3},
			expected: 1, // 3+1 > 3
		},
		{
			name:     "1-replication, read needs 1",
			policy:   PlacementPolicy{ReplicationFactor: 1},
			expected: 1,
		},
		{
			name: "EC(4+2) read needs 3",
			policy: PlacementPolicy{
				ECConfig: &ECConfig{DataShards: 4, ParityShards: 2},
			},
			expected: 3, // 4+3 > 6
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.policy.EffectiveReadQuorum()
			if got != tt.expected {
				t.Errorf("EffectiveReadQuorum() = %d, want %d", got, tt.expected)
			}
		})
	}
}

func TestPlacementPolicy_IsSyncWrite(t *testing.T) {
	// WriteQuorum > 1 means synchronous write
	policy1 := PlacementPolicy{ReplicationFactor: 3, WriteQuorum: 1}
	if policy1.IsSyncWrite() {
		t.Error("WriteQuorum=1 should not be sync write")
	}

	policy2 := PlacementPolicy{ReplicationFactor: 3, WriteQuorum: 2}
	if !policy2.IsSyncWrite() {
		t.Error("WriteQuorum=2 should be sync write")
	}

	policy3 := PlacementPolicy{ReplicationFactor: 3} // default
	if !policy3.IsSyncWrite() {
		t.Error("default 3-replication should be sync write (quorum=2)")
	}
}
