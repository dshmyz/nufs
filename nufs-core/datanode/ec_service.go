package datanode

import (
	"context"
	"fmt"
	"time"

	"github.com/dshmyz/nufs/nufs-core/datanode/storage"
	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// ECAuthority is the narrow authority-seam the EC conversion transaction is
// driven through. It is the set of ECStore lifecycle methods that
// ECConverter.ConvertReplica needs, carved out so the conversion driver on the
// serving path can inject either the local *metadata.ECStore (single-node / S1)
// or an HTTP-backed implementation of the same seam (production topology / S2)
// without the datanode depending on a concrete store.
//
// Two realizations satisfy it:
//   - *metadata.ECStore — Pebble-backed, in-process authority (S1).
//   - an HTTP transport exposing Begin/Plan/Complete/Rollback as RPCs (S2).
//
// The seam deliberately exposes the coalesced transaction steps (not individual
// ledger ops) so both the local and remote authorities can own the §14 plan
// and the lifecycle state machine exactly once.
type ECAuthority interface {
	// BeginConversion starts a conversion transaction for one extent.
	BeginConversion(stripeID string, extentID uint64, gen uint64, checksum uint32) (*metadata.ECStripe, error)
	// PlanShards fills the stripe's §14-diverse shard placement and advances
	// the state.
	PlanShards(st *metadata.ECStripe, disks []metadata.ECDisk) error
	// MarkSyncing marks all shards written.
	MarkSyncing(st *metadata.ECStripe) error
	// CompleteConversion finalizes the stripe as durable.
	CompleteConversion(st *metadata.ECStripe, at time.Time) error
	// RollbackConversion aborts a non-durable transaction.
	RollbackConversion(st *metadata.ECStripe, reason string) error
}

// ECService is the V2.1 serving-path EC driver. It owns the local conversion
// workflow (replicated chunk → 6+3 stripe across the node's attached shard
// stores, coordinated by an injected ECAuthority) and the serving-side
// degraded read, so callers (ops control plane, storage consumers) can drive
// EC without depending on the metadata transport.
//
// On the single-node dev/serving path the authority is a local
// *metadata.ECStore and the candidate disk topology is synthesized from the
// node's attached shard stores (§14 requires ≥3 distinct NodeIDs in the plan,
// so the topology presents 3 fault-domain slots mapped onto the local stores —
// the same single-node simulation the E4 tests use). This is the /S1 step; a
// production deployment injects an HTTP authority and filters each shard to
// the node's own (NodeID,DiskID) as /S2.
type ECService struct {
	v    *V2Store
	ec   ECAuthority
	// resolveDisk maps a planned (NodeID,DiskID) to a local shard-store index.
	resolveDisk func(metadata.ECDisk) int
	// candidateDisks returns the disk set handed to PlanShards. Defaults to a
	// synthetic multi-slot topology over the node's attached shard stores.
	candidateDisks func() []metadata.ECDisk
	// publish is called with the completed stripe after a successful
	// conversion, so the caller can lift the EC layout into chunk metadata
	// (atomic §14 switch). nil → no-op (local-serving default until /S2 wires
	// the remote layout switch).
	publish func(context.Context, *metadata.ECStripe) error

	// Cross-node conversion (S3) fields. When set, ConvertToEC runs on this
	// datanode as the coordinator, pushing shards this node does not own to
	// peers over the TCP wire instead of writing them to local shard stores.
	ownNodeID  uint64
	peerClient func(uint64) (*Client, bool)
	crossNode  bool
}

// NewECService wires the serving-path EC driver over a V2Store whose shard
// stores are already attached and an injected conversion authority. The
// default resolveDisk decodes the synthetic candidate topology's DiskID
// (slot*1000+localIndex) back to the local shard-store index, matching
// CandidateDisks.
func NewECService(v *V2Store, ec ECAuthority) *ECService {
	return &ECService{
		v: v, ec: ec,
		resolveDisk: func(d metadata.ECDisk) int { return int(d.DiskID % 1000) },
		publish:     func(context.Context, *metadata.ECStripe) error { return nil },
	}
}

// SetResolveDisk overrides how a planned (NodeID,DiskID) maps to a local
// shard-store index (default: DiskID is the local store index).
func (s *ECService) SetResolveDisk(fn func(metadata.ECDisk) int) {
	if fn != nil {
		s.resolveDisk = fn
	}
}

// SetCandidateDisks overrides the candidate disk set handed to PlanShards.
func (s *ECService) SetCandidateDisks(fn func() []metadata.ECDisk) {
	if fn != nil {
		s.candidateDisks = fn
	}
}

// SetPublish installs the completed-stripe publication hook (atomic §14 layout
// switch into chunk metadata).
func (s *ECService) SetPublish(fn func(context.Context, *metadata.ECStripe) error) {
	if fn != nil {
		s.publish = fn
	}
}

// SetCrossNode configures the service as a cross-node EC conversion coordinator
// (S3 / multi-datanode). ownNodeID is this datanode's NodeID (metadata
// namespace); peerClient resolves a peer NodeID to a ready datanode *Client so
// the coordinator can push shards this node does not own over the TCP wire.
//
// The candidate topology must then be supplied via SetCandidateDisks (the
// cluster's real nodes and per-node disk indices) rather than the default
// single-node synthetic topology. DiskID within each NodeID encodes that node's
// shard-store index (DiskID % 1000 == node-local disk); the coordinator writes a
// shard locally (WriteShardAtDisk) when it owns the node, else pushes it to the
// owning peer (ReplicateECShard) with the planned disk index.
func (s *ECService) SetCrossNode(ownNodeID uint64, peerClient func(uint64) (*Client, bool)) {
	s.ownNodeID = ownNodeID
	s.peerClient = peerClient
	s.crossNode = true
	s.resolveDisk = func(d metadata.ECDisk) int {
		if d.DiskID >= 1000 {
			return int(d.DiskID % 1000)
		}
		return int(d.DiskID)
	}
}

// CandidateDisks returns the disk set used for planning (overridable via
// SetCandidateDisks). The default synthesizes a 3-slot × N-disk topology over
// the node's attached shard stores so the §14 diversity check (≥3 distinct
// NodeIDs) is satisfiable on a single physical node while every planned shard
// still routes back to a real local store (the single-node simulation the E4
// tests use). Slots are NodeID 1..3; each slot k maps to local store
// k % shardStoreCount, and DiskID is a unique synthetic disk per (slot, disk).
func (s *ECService) CandidateDisks() []metadata.ECDisk {
	if s.candidateDisks != nil {
		return s.candidateDisks()
	}
	n := s.v.ShardStoreCount()
	if n <= 0 {
		return nil
	}
	// 3 fault-domain slots × n disks each: PlanShards needs ≥9 total and ≥3
	// distinct nodes; each node holds ≤3 shards by construction (≤n ≥ 3 disks).
	var disks []metadata.ECDisk
	for slot := uint64(1); slot <= 3; slot++ {
		for disk := 0; disk < n; disk++ {
			disks = append(disks, metadata.ECDisk{
				NodeID: slot,
				DiskID: slot*1000 + uint64(disk),
			})
		}
	}
	return disks
}

// ShardStoreCount returns the number of attached shard stores (0 if none).
func (v *V2Store) ShardStoreCount() int { return len(v.shards) }

// ECShardChunks returns the set of chunk IDs that hold at least one EC shard on
// this node, discovered by enumerating each shard store's committed extents
// (ListExtents coalesces to one live generation per extent ID — the chunk ID —
// so each returned ID is a distinct chunk with shard(s) here). It is the
// discovery input for the EC self-healer (Program 6 F2).
func (v *V2Store) ECShardChunks() (map[metadata.ChunkID]bool, error) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	seen := make(map[metadata.ChunkID]bool)
	for i, b := range v.shards {
		if b.lister == nil {
			continue
		}
		// Skip a retired/failed shard store. RemoveDisk marks the shard backend
		// at the disk's index as FAILED (read-only, closed on re-adopt), and
		// ListExtents on a closed store errors. A retired store contributes no
		// reachable shards, so skipping it is correct — and one retired store
		// must not abort self-heal discovery for the whole node, or the EC
		// reaper would skip every sweep until the disk is re-adopted.
		if v.diskFailed(i) {
			continue
		}
		extents, err := b.lister.ListExtents()
		if err != nil {
			return nil, fmt.Errorf("ec shard chunks: list shard store: %w", err)
		}
		for _, e := range extents {
			seen[metadata.ChunkID(e.ExtentID)] = true
		}
	}
	return seen, nil
}

// ConvertToEC converts one replicated chunk (chunkID, currently a whole extent
// readable through v.Read) into a completed 6+3 stripe. The authority owns the
// §14 placement and lifecycle; the local service encodes and writes the shards
// to the node's attached shard stores, verifies the aggregate, and publishes
// the completed stripe via the publish hook. Returns the COMpleted stripe.
func (s *ECService) ConvertToEC(ctx context.Context, chunkID metadata.ChunkID, generation uint64) (*metadata.ECStripe, error) {
	if s.v == nil {
		return nil, fmt.Errorf("ec convert: no store")
	}
	if s.ec == nil {
		return nil, fmt.Errorf("ec convert: no authority")
	}
	disks := s.CandidateDisks()
	if len(disks) < ec63Shards {
		return nil, fmt.Errorf("ec convert: need >=%d candidate shard disks, datanode has %d attached shard stores", ec63Shards, len(disks))
	}
	cv := NewECConverter(s.ec, s.v, s.resolveDisk)
	if s.crossNode {
		// Coordinator mode: each shard goes to the node that owns it — written
		// locally if we own it, else pushed to the peer over the wire.
		cid := chunkID
		cv.shardWriter = func(i int, shard []byte, loc metadata.ECDisk) error {
			if loc.NodeID == s.ownNodeID {
				return s.v.WriteShardAtDisk(cid, i, int(loc.DiskID%1000), shard)
			}
			peer, ok := s.peerClient(loc.NodeID)
			if !ok {
				return fmt.Errorf("ec convert: no client for peer node %d (shard %d)", loc.NodeID, i)
			}
			resp, err := peer.ReplicateECShard(cid, i, int(loc.DiskID%1000), shard)
			if err != nil {
				return fmt.Errorf("ec convert: push shard %d to node %d: %w", i, loc.NodeID, err)
			}
			if resp.Status != StatusOK {
				return fmt.Errorf("ec convert: push shard %d to node %d: %s", i, loc.NodeID, resp.Error)
			}
			return nil
		}
		// Cross-node aggregate verify: assemble every shard from the node that
		// owns it and decode — the coordinator cannot read peers' shards from
		// its own local shard stores.
		cv.verifyAggregate = func(cid metadata.ChunkID, shards []metadata.ECShard, originalLen int) ([]byte, uint32, error) {
			all := make([][]byte, len(shards))
			for i, sh := range shards {
				var data []byte
				if sh.NodeID == s.ownNodeID {
					d, _, err := s.v.ReadShard(cid, sh.Index)
					if err != nil {
						return nil, 0, fmt.Errorf("verify: read local shard %d: %w", sh.Index, err)
					}
					data = d
				} else {
					peer, ok := s.peerClient(sh.NodeID)
					if !ok {
						return nil, 0, fmt.Errorf("verify: no client for node %d (shard %d)", sh.NodeID, sh.Index)
					}
					resp, err := peer.ReadECShard(cid, sh.Index)
					if err != nil {
						return nil, 0, fmt.Errorf("verify: read peer shard %d node %d: %w", sh.Index, sh.NodeID, err)
					}
					if resp.Status != StatusOK {
						return nil, 0, fmt.Errorf("verify: read peer shard %d node %d: %s", sh.Index, sh.NodeID, resp.Error)
					}
					data = resp.Data
				}
				all[i] = data
			}
			dec, err := decodeEC63(all, originalLen)
			if err != nil {
				return nil, 0, fmt.Errorf("verify: decode aggregate: %w", err)
			}
			// The segment store persists its extent checksum as CRC32C
			// (Castagnoli), matching V2Store.Read/ReadChunkEC; the original
			// checksum recorded at BeginConversion comes from that same store.
			return dec, storage.CRC32C(dec), nil
		}
	}
	stripeID := fmt.Sprintf("stripe-%d", uint64(chunkID))
	st, err := cv.ConvertReplica(stripeID, uint64(chunkID), generation, disks)
	if err != nil {
		return nil, err
	}
	if p := s.publish; p != nil {
		if perr := p(ctx, st); perr != nil {
			return nil, fmt.Errorf("ec convert: publish stripe %s: %w", stripeID, perr)
		}
	}
	return st, nil
}
