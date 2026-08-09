package main

import (
	"fmt"
	"path/filepath"

	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// shardedOpsStore adapts a *metadata.ShardedStore to the opsDataStore
// surface for the in-process non-raft wiring. It reports itself as the
// leader (a single non-raft metad process owns every shard) with no leader
// redirect addresses, mirroring how *metadata.PebbleStore behaves when
// Raft is disabled. The data plane — namespace/bucket/chunk/node/repair
// calls from the gateway — routes through the ShardedStore's consistent
// hash ring rather than a single PebbleStore.
type shardedOpsStore struct {
	*metadata.ShardedStore
}

var _ opsDataStore = (*shardedOpsStore)(nil)

func (s *shardedOpsStore) IsLeader() bool        { return true }
func (s *shardedOpsStore) LeaderAddr() string    { return "" }
func (s *shardedOpsStore) LeaderOpsAddr() string { return "" }

// buildShardedDataPlane creates N in-process PebbleStore shards under
// dir/shard-<i>/ and wraps them in a ShardedStore, the metadata data
// plane. All control-plane subsystems (ops admin, service bundle,
// prometheus) keep running on the primary PebbleStore at dir; this sharded
// store replaces it only where the gateway's data-plane calls are served.
// N is bounded to a sane fan-out; keys are distributed by vnode ring.
func buildShardedDataPlane(dir string, n int, nodeID uint64, memTableSize uint64, bucketStats bool) (*shardedOpsStore, error) {
	if n < 2 {
		return nil, fmt.Errorf("shards must be >= 2")
	}
	ring := metadata.NewHashRing(0) // 0 => default vnodes per shard
	sharded := metadata.NewShardedStore(ring)
	// Install the node registration/heartbeat rate limiter on the data
	// plane too, so RegisterNode/Heartbeat are throttled exactly as on the
	// single-store path.
	sharded.SetNodeThrottle(metadata.NewNodeRegistrationThrottle(nil))
	// Bucket quota checks must work end-to-end across the sharded plane.
	sharded.SetQuotaManager(metadata.NewQuotaManager())

	for i := 0; i < n; i++ {
		shardDir := filepath.Join(dir, fmt.Sprintf("shard-%d", i))
		st, err := metadata.NewPebbleStore(metadata.PebbleStoreConfig{
			Dir:            shardDir,
			NodeID:         nodeID,
			MemTableSize:   memTableSize,
			UseBucketStats: bucketStats,
		})
		if err != nil {
			return nil, fmt.Errorf("create shard %d: %w", i, err)
		}
		shardID := metadata.ShardID(i + 1)
		ring.AddShard(metadata.ShardInfo{ID: shardID})
		sharded.AddShard(shardID, st)
	}
	return &shardedOpsStore{ShardedStore: sharded}, nil
}
