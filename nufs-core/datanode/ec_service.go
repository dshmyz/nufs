package datanode

import (
	"context"
	"fmt"
	"time"

	"github.com/example/dfs/metadata"
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
