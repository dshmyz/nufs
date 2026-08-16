package smoke

import (
	"fmt"
	"sync"
	"testing"

	"github.com/dshmyz/nufs/nufs-core/datanode"
	"github.com/dshmyz/nufs/nufs-core/datanode/storage"
	"github.com/dshmyz/nufs/nufs-core/datanode/storage/segment"
	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// v21Datanode is a running V2.1 data node: a V2Store over one segment data
// stream (StreamID 1) per dir, served by the TCP server on an ephemeral port.
// It is the V2.1 analogue of the retired V1 NewChunkStore-based node — the
// smoke tests construct real nodes through this instead of the deleted V1
// engine APIs.
type v21Datanode struct {
	Store  *datanode.V2Store
	Server *datanode.Server

	closeOnce sync.Once
	stores    []storage.Store
}

// closeStore closes a storage.Store if it implements Close. The storage.Store
// interface only declares the extent ops, but the segment stores this helper
// constructs expose Close() to release the index and segment files so the same
// dir can be re-opened by a restart.
func closeStore(s storage.Store) {
	if c, ok := s.(interface{ Close() error }); ok {
		_ = c.Close()
	}
}

// Close releases the underlying segment stores so the same dir can be
// re-opened by a restart. Idempotent; also runs via the t.Cleanup registered
// by startV21Datanode and via newV21Node's error path.
func (n *v21Datanode) Close() {
	n.closeOnce.Do(func() {
		for _, s := range n.stores {
			closeStore(s)
		}
	})
}

// newV21Node builds a V2.1 datanode (store over dirs + TCP server on an
// ephemeral port) and returns it, registering no test cleanup — so it can be
// called from a goroutine (the chaos smoke test restarts a node mid-run).
// Mirrors runDataNodeV21's construction: one data stream (StreamID 1), one EC
// shard stream (StreamID 2) and one small-file stream (StreamID 0, ≤ 64 KiB)
// per dir, the shard and small stores attached so Program A EC writes can
// place shards and ≤ SmallFileThreshold commits route to the small stream,
// and a disk factory so DiskLifecycleOps.AddDisk builds the sibling
// data/shard/small trio at runtime. On error any already-open stores are
// closed.
func newV21Node(nodeID metadata.NodeID, dirs ...string) (*v21Datanode, error) {
	n := &v21Datanode{}
	var dataStores, shardStores, smallStores []storage.Store
	for _, d := range dirs {
		data, err := segment.New(segment.Config{Dir: d, UseMemIndex: true, StreamID: 1})
		if err != nil {
			n.Close()
			return nil, fmt.Errorf("segment.New(data %s): %w", d, err)
		}
		n.stores = append(n.stores, data)
		dataStores = append(dataStores, data)

		shard, err := segment.New(segment.Config{Dir: d, UseMemIndex: true, StreamID: 2})
		if err != nil {
			n.Close()
			return nil, fmt.Errorf("segment.New(shard %s): %w", d, err)
		}
		n.stores = append(n.stores, shard)
		shardStores = append(shardStores, shard)

		small, err := segment.NewSmallStore(segment.Config{Dir: d, UseMemIndex: true})
		if err != nil {
			n.Close()
			return nil, fmt.Errorf("segment.NewSmallStore(small %s): %w", d, err)
		}
		n.stores = append(n.stores, small)
		smallStores = append(smallStores, small)
	}
	v := datanode.NewMultiV2Store(dataStores, dirs...)
	if err := v.AttachShardStores(shardStores); err != nil {
		n.Close()
		return nil, fmt.Errorf("attach shard stores: %w", err)
	}
	if err := v.AttachSmallStores(smallStores); err != nil {
		n.Close()
		return nil, fmt.Errorf("attach small stores: %w", err)
	}
	// AddDisk builds the same data+shard+small trio for a runtime-adopted dir.
	v.SetDiskFactory(func(dir string) (data, shard, small storage.Store, err error) {
		data, err = segment.New(segment.Config{Dir: dir, UseMemIndex: true, StreamID: 1})
		if err != nil {
			return nil, nil, nil, err
		}
		shard, err = segment.New(segment.Config{Dir: dir, UseMemIndex: true, StreamID: 2})
		if err != nil {
			closeStore(data)
			return nil, nil, nil, err
		}
		small, err = segment.NewSmallStore(segment.Config{Dir: dir, UseMemIndex: true})
		if err != nil {
			closeStore(data)
			closeStore(shard)
			return nil, nil, nil, err
		}
		return data, shard, small, nil
	})
	n.Store = v

	cfg := datanode.DefaultConfig()
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.NodeID = nodeID
	srv := datanode.NewServer(cfg, v)
	if err := srv.Start(); err != nil {
		n.Close()
		return nil, fmt.Errorf("start datanode %d: %w", nodeID, err)
	}
	n.Server = srv
	return n, nil
}

// startV21Datanode builds a V2.1 datanode over dirs and registers cleanup to
// stop the server and close the stores at test end. Stop and Close are both
// idempotent, so a test may also stop/close the node mid-run (failover, disk
// lifecycle) without double-close.
func startV21Datanode(t *testing.T, nodeID metadata.NodeID, dirs ...string) *v21Datanode {
	t.Helper()
	n, err := newV21Node(nodeID, dirs...)
	if err != nil {
		t.Fatalf("%v", err)
	}
	t.Cleanup(func() {
		n.Server.Stop()
		n.Close()
	})
	return n
}
