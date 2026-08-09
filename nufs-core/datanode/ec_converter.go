package datanode

import (
	"bytes"
	"fmt"
	"time"

	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// ECConverter drives the replication→EC 6+3 conversion service path
// (Task #74 Phase E4): it reads a replicated (whole-extent) chunk, encodes it
// into six data + three parity shards, runs the metadata ECStore conversion
// transaction (Begin → PlanShards → write real shards → MarkSyncing → verify →
// Complete), and rolls the transaction back on any failure. This is the "write
// real shards" half of the RF→EC layout switch; the completed stripe's
// ECShard placements are what a caller lifts into ChunkMeta.Replicas/ECGroup
// for the atomically-visible metadata flip.
//
// It lives in the datanode package because it must drive V2Store shard writes
// that chunkstore cannot reach (datanode↔chunkstore import cycle); the
// metadata ECStore transaction is consumed through metadata's exported types.
type ECConverter struct {
	// ec is the metadata conversion transaction authority (see ECAuthority).
	// The serving path injects the local *metadata.ECStore (S1) or an HTTP
	// implementation of the same seam (S2); tests inject the in-memory store.
	ec ECAuthority
	// v is the V2Store whose attached shard stores receive the encoded shards.
	v *V2Store
	// resolveDisk maps a planned ECShard's metadata placement (NodeID/DiskID)
	// to the local shard-store index to actually write to. Production routes
	// this to the local store owning the (node,disk); tests map the planner's
	// multi-node topology onto a node's shard stores. Defaults to DiskID when
	// nil (single-node where DiskID is the store index).
	resolveDisk func(metadata.ECDisk) int
	// shardWriter, when non-nil, is used instead of the default write loop. It
	// lets the coordinator decide per-shard whether to write locally or push a
	// shard to a peer datanode over the wire (S3 cross-node conversion). The
	// default writes to the resolved local store index.
	shardWriter func(i int, shard []byte, loc metadata.ECDisk) error
	// verifyAggregate, when non-nil, is used instead of the default local
	// aggregate read to validate the written shards before completing. The S3
	// cross-node coordinator sets it to assemble every shard from whichever
	// node owns it (the default ReadChunkEC only sees local shard stores);
	// shards is the planned placement (from st.Shards) so ownership can be
	// resolved per shard.
	verifyAggregate func(cid metadata.ChunkID, shards []metadata.ECShard, originalLen int) ([]byte, uint32, error)
}

// NewECConverter creates a conversion coordinator over the given metadata EC
// transaction authority and V2Store.
func NewECConverter(ec ECAuthority, v *V2Store, resolveDisk func(metadata.ECDisk) int) *ECConverter {
	if resolveDisk == nil {
		resolveDisk = func(d metadata.ECDisk) int { return int(d.DiskID) }
	}
	return &ECConverter{ec: ec, v: v, resolveDisk: resolveDisk}
}

// ConvertReplica converts one replicated chunk (extent extentID at metadata
// generation gen, currently stored as a whole extent readable through
// v.Read) into a completed 6+3 EC stripe across the candidate disks. It drives
// the full transaction:
//
//	Begin (Preparing) → PlanShards (Encoding) → write real shards
//	→ MarkSyncing (Syncing) → verify aggregate → Complete (Complete).
//
// On any failure after Begin the transaction is rolled back (RolledBack):
// metadata still points at the replicas and any partially written shards are
// reclaimable orphans (§14). On success the caller may build the EC layout
// (Replicas/ECGroup) from the returned stripe's Shards and atomically switch
// the chunk to it.
func (c *ECConverter) ConvertReplica(stripeID string, extentID uint64, gen uint64, disks []metadata.ECDisk) (*metadata.ECStripe, error) {
	cid := metadata.ChunkID(extentID)

	// Read the whole replicated extent — this is the RF layout being replaced.
	replica, checksum, err := c.v.Read(cid, 0, 0)
	if err != nil {
		return nil, fmt.Errorf("ec convert: read replica %d: %w", cid, err)
	}

	st, err := c.ec.BeginConversion(stripeID, extentID, gen, checksum)
	if err != nil {
		return nil, fmt.Errorf("ec convert: begin: %w", err)
	}
	fail := func(stage string, err error) (*metadata.ECStripe, error) {
		if rb := c.ec.RollbackConversion(st, stage); rb != nil {
			return nil, fmt.Errorf("ec convert: %s: %v (rollback: %v)", stage, err, rb)
		}
		return nil, fmt.Errorf("ec convert: %s: %w", stage, err)
	}

	// PlanShards fills st.Shards with real §14-diverse placement (≥3 nodes,
	// ≤3 per node) and advances Preparing → Encoding.
	if err := c.ec.PlanShards(st, disks); err != nil {
		return fail("plan", err)
	}

	all, err := encodeEC63(replica)
	if err != nil {
		return fail("encode", err)
	}
	// Write each shard to the physical store owning its planned (node,disk).
	// With a cross-node coordinator (S3) the injected shardWriter routes each
	// shard to the node that owns it: write locally, or push over the wire.
	for i, shard := range all {
		loc := metadata.ECDisk{NodeID: st.Shards[i].NodeID, DiskID: st.Shards[i].DiskID}
		if c.shardWriter != nil {
			if err := c.shardWriter(i, shard, loc); err != nil {
				return fail(fmt.Sprintf("write shard %d", i), err)
			}
			continue
		}
		local := c.resolveDisk(loc)
		if err := c.v.WriteShardAtDisk(cid, i, local, shard); err != nil {
			return fail(fmt.Sprintf("write shard %d", i), err)
		}
	}

	if err := c.ec.MarkSyncing(st); err != nil {
		return fail("mark syncing", err)
	}

	// Verify the aggregate decodes byte- and checksum-exact before committing
	// the layout switch (a degraded read must recover the original, §14).
	var got []byte
	var gotSum uint32
	if c.verifyAggregate != nil {
		got, gotSum, err = c.verifyAggregate(cid, st.Shards, len(replica))
	} else {
		got, gotSum, err = c.v.ReadChunkEC(cid, len(replica))
	}
	if err != nil {
		return fail("verify", err)
	}
	if !bytes.Equal(got, replica) {
		return fail("verify", fmt.Errorf("aggregate decoded %d bytes, want %d", len(got), len(replica)))
	}
	if gotSum != checksum {
		return fail("verify", fmt.Errorf("aggregate checksum %#x, want %#x", gotSum, checksum))
	}

	if err := c.ec.CompleteConversion(st, time.Now()); err != nil {
		return fail("complete", err)
	}
	return st, nil
}

// BuildECGroup derives the atomically-switchable EC layout from a completed
// stripe: the Replicas slice (one ReplicaInfo per shard, carrying its NodeID
// and ShardIndex) plus the ECGroup descriptor. After this is written to the
// chunk's metadata the chunk is served from the 6+3 shards, not the old
// replicas (§14 atomic switch).
func BuildECGroup(st *metadata.ECStripe, size int32, tier metadata.StorageTier) *metadata.ChunkMeta {
	cm := &metadata.ChunkMeta{
		Size:       size,
		State:      metadata.ChunkReady,
		ECGroup:    metadata.ECGroupFromProfile(nil, st.StripeID),
		Checksum:   st.OriginalChecksum,
		Tier:       tier,
		ECStripeID: st.StripeID,
	}
	for _, s := range st.Shards {
		cm.Replicas = append(cm.Replicas, metadata.ReplicaInfo{
			NodeID:     metadata.NodeID(s.NodeID),
			ShardIndex: s.Index,
		})
	}
	return cm
}
