package metadata

import (
	"context"
	"fmt"
	"sort"
	"time"
)

// EC 6+3 lifecycle (V2.1 §14): all new data begins as three replicas.
// Conversion requires an immutable file version, 30 days without
// modification, no active transaction/snapshot conflict, healthy
// replicas, a completed scrub, and sufficient fault-domain diversity.
//
// The conversion transaction builds six data and three parity shards on
// nine distinct physical disks across at least three machines. No
// machine stores more than three shards, so loss of one machine loses
// at most the three shards tolerated by 6+3. Any six shards reconstruct
// data.

const (
	// ECDataShards / ECParityShards are the 6+3 configuration (§14).
	ECDataShards   = 6
	ECParityShards = 3
	// ECQuorum is the minimum shards to reconstruct (K).
	ECQuorum = ECDataShards
	// ECMinMachines is the minimum distinct machines for 6+3 diversity.
	ECMinMachines = 3
	// ECMaxShardsPerMachine bounds shards per machine (§14).
	ECMaxShardsPerMachine = 3
	// ECConversionIdleAge is the 30-day no-modification default (§14).
	ECConversionIdleAge = 30 * 24 * time.Hour
)

// ECConversionState is the conversion transaction lifecycle.
type ECConversionState uint8

const (
	ECConversionPreparing ECConversionState = iota
	ECConversionEncoding
	ECConversionSyncing
	ECConversionSwitching
	ECConversionComplete
	ECConversionRolledBack
)

func (s ECConversionState) String() string {
	switch s {
	case ECConversionPreparing:
		return "preparing"
	case ECConversionEncoding:
		return "encoding"
	case ECConversionSyncing:
		return "syncing"
	case ECConversionSwitching:
		return "switching"
	case ECConversionComplete:
		return "complete"
	case ECConversionRolledBack:
		return "rolled_back"
	default:
		return "unknown"
	}
}

// ECStripe describes one encoded stripe and its shard placement.
type ECStripe struct {
	// StripeID identifies the stripe.
	StripeID string `json:"stripe_id"`
	// ExtentID/Generation is the source extent converted.
	ExtentID   uint64 `json:"extent_id"`
	Generation uint64 `json:"generation"`
	// OriginalChecksum is the extent's end-to-end checksum, verified on
	// degraded reads (§14: "A degraded read verifies the original extent
	// checksum").
	OriginalChecksum uint32 `json:"original_checksum"`
	// Shards lists the 9 shard placements: [0..5] data, [6..8] parity.
	Shards []ECShard `json:"shards"`
	// State is the conversion lifecycle.
	State ECConversionState `json:"state"`
	// ConvertedAt is when the stripe became durable.
	ConvertedAt int64 `json:"converted_at"`
	// RolledBackAt is when RollbackConversion marked the stripe rolled back.
	// It gates stripe-orphan GC: a rolled-back stripe may still be mid-repair
	// (its partial shards briefly referenced), so reclaim is deferred until the
	// stripe has been rolled back for a configured age (§14 orphan reclamation).
	RolledBackAt int64 `json:"rolled_back_at,omitempty"`
}

// ECShard is one shard's location.
type ECShard struct {
	// Index is the shard index (0..5 data, 6..8 parity).
	Index int `json:"index"`
	// NodeID is the machine storing this shard.
	NodeID uint64 `json:"node_id"`
	// DiskID identifies the physical disk.
	DiskID uint64 `json:"disk_id"`
	// SegmentID/Offset locate the shard record on disk.
	SegmentID uint64 `json:"segment_id"`
	Offset    int64  `json:"offset"`
	// Checksum is the shard's own checksum.
	Checksum uint32 `json:"checksum"`
}

// ECConversionCheck reports whether conversion eligibility holds (§14).
type ECConversionCheck struct {
	Immutable       bool
	Idle            bool
	NoConflict      bool
	HealthyReplicas bool
	Scrubbed        bool
	Diverse         bool
}

// All returns true if every precondition passes.
func (c ECConversionCheck) All() bool {
	return c.Immutable && c.Idle && c.NoConflict && c.HealthyReplicas && c.Scrubbed && c.Diverse
}

// ECPlacementValidator validates fault-domain diversity for a 6+3
// placement (§14).
type ECPlacementValidator struct {
	// MinMachines is the minimum distinct machines.
	MinMachines int
	// MaxPerMachine bounds shards per machine.
	MaxPerMachine int
}

// Validate checks a proposed shard placement: ≥3 distinct machines, no
// machine >3 shards. Returns the number of distinct machines.
func (v *ECPlacementValidator) Validate(shards []ECShard) (int, error) {
	perMachine := make(map[uint64]int)
	for _, s := range shards {
		perMachine[s.NodeID]++
	}
	if len(perMachine) < v.MinMachines {
		return 0, fmt.Errorf("ec: need >=%d distinct machines, got %d", v.MinMachines, len(perMachine))
	}
	for node, n := range perMachine {
		if n > v.MaxPerMachine {
			return 0, fmt.Errorf("ec: machine %d holds %d shards, max %d", node, n, v.MaxPerMachine)
		}
	}
	return len(perMachine), nil
}

// ECStore persists EC stripes and conversion state.
type ECStore struct {
	store *PebbleStore
}

// NewECStore creates the EC store.
func NewECStore(store *PebbleStore) *ECStore {
	return &ECStore{store: store}
}

// ecStripeKey formats a stripe key.
func ecStripeKey(stripeID string) string {
	return "ec-stripe/" + stripeID
}

// GetStripe reads an EC stripe.
func (s *ECStore) GetStripe(stripeID string) (*ECStripe, error) {
	var st ECStripe
	exists, err := s.store.getValue(ecStripeKey(stripeID), &st)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, nil
	}
	return &st, nil
}

// PutStripe writes an EC stripe.
func (s *ECStore) PutStripe(st *ECStripe) error {
	return s.store.putMsgpack(ecStripeKey(st.StripeID), st)
}

// BeginConversion starts a conversion transaction: records the source
// extent + original checksum, transitions to Preparing.
func (s *ECStore) BeginConversion(stripeID string, extentID uint64, gen uint64, checksum uint32) (*ECStripe, error) {
	st := &ECStripe{
		StripeID:         stripeID,
		ExtentID:         extentID,
		Generation:       gen,
		OriginalChecksum: checksum,
		State:            ECConversionPreparing,
	}
	if err := s.PutStripe(st); err != nil {
		return nil, err
	}
	return st, nil
}

// ECDisk is a candidate shard storage target: one physical disk on a node.
// The 6+3 planner distributes the nine shard extents across a set of these
// so that loss of a whole node (and its disks) is tolerated (§14).
type ECDisk struct {
	// DiskID identifies the physical disk (the datanode's shard store disk).
	DiskID uint64
	// NodeID is the machine hosting the disk (the fault domain).
	NodeID uint64
}

// PlanShards assigns the nine shards (six data, three parity) of one stripe
// across candidate disks with fault-domain diversity: at least MinMachines
// distinct nodes take part and no node holds more than MaxShardsPerMachine
// shards (per §14, 6+3 tolerates losing at most three shards / one machine).
//
// It fills st.Shards (Index, NodeID, DiskID — location/checksum are filled
// when the shard is durably written), validates the diversity, advances the
// transaction Preparing → Encoding, and persists. It returns an error when
// the candidate set cannot meet §14 bounds (too few distinct nodes or too few
// disks to place all nine shards).
func (s *ECStore) PlanShards(st *ECStripe, disks []ECDisk) error {
	if st.State != ECConversionPreparing {
		return fmt.Errorf("ec: plan shards requires state preparing, have %s", st.State)
	}
	total := ECDataShards + ECParityShards

	// Group available disks by node, and require enough distinct machines.
	byNode := make(map[uint64][]ECDisk)
	for _, d := range disks {
		byNode[d.NodeID] = append(byNode[d.NodeID], d)
	}
	if len(byNode) < ECMinMachines {
		return fmt.Errorf("ec: plan shards needs >=%d distinct nodes, got %d (%d shards to place)",
			ECMinMachines, len(byNode), total)
	}
	if len(disks) < total {
		return fmt.Errorf("ec: plan shards needs >=%d disks, got %d", total, len(disks))
	}

	// Deterministic round-robin across nodes ordered by NodeID: one shard gets
	// one disk from each node per pass, skipping a node once it hits its disk
	// count or the per-machine shard cap. This spreads the stripe evenly and
	// never exceeds MaxShardsPerMachine on any node.
	nodeOrder := make([]uint64, 0, len(byNode))
	for n := range byNode {
		nodeOrder = append(nodeOrder, n)
	}
	sort.Slice(nodeOrder, func(i, j int) bool { return nodeOrder[i] < nodeOrder[j] })

	type nodeQueue struct {
		nodeID   uint64
		disks    []ECDisk
		used     int
		nextDisk int
	}
	queues := make([]*nodeQueue, 0, len(nodeOrder))
	for _, n := range nodeOrder {
		ds := byNode[n]
		sort.Slice(ds, func(i, j int) bool { return ds[i].DiskID < ds[j].DiskID })
		queues = append(queues, &nodeQueue{nodeID: n, disks: ds})
	}

	plan := make([]ECShard, 0, total)
	i := 0
	for placed := 0; placed < total; placed++ {
		q := queues[i%len(queues)]
		if q.used >= ECMaxShardsPerMachine || q.nextDisk >= len(q.disks) {
			i++
			placed-- // retry with next node; bounded by total passes below
			continue
		}
		d := q.disks[q.nextDisk]
		q.nextDisk++
		q.used++
		plan = append(plan, ECShard{Index: placed, NodeID: d.NodeID, DiskID: d.DiskID})
		i++
	}
	if len(plan) != total {
		return fmt.Errorf("ec: plan shards could not place %d shards (disk/cap exhaustion)", total)
	}

	st.Shards = plan
	if _, err := (&ECPlacementValidator{MinMachines: ECMinMachines, MaxPerMachine: ECMaxShardsPerMachine}).Validate(plan); err != nil {
		return fmt.Errorf("ec: plan shards: %w", err)
	}
	st.State = ECConversionEncoding
	return s.PutStripe(st)
}

// MarkSyncing transitions encoding → syncing (all shards written).
func (s *ECStore) MarkSyncing(st *ECStripe) error {
	st.State = ECConversionSyncing
	return s.PutStripe(st)
}

// CompleteConversion atomically switches metadata to EC and records the
// conversion time. After this, the three replicas are scheduled for
// delayed deletion (§14).
func (s *ECStore) CompleteConversion(st *ECStripe, at time.Time) error {
	st.State = ECConversionComplete
	st.ConvertedAt = at.UnixNano()
	return s.PutStripe(st)
}

// RollbackConversion marks a failed conversion (metadata still points at
// the three replicas; partial EC shards are reclaimable orphans, §14). The
// RolledBackAt timestamp gates orphan reclamation: GC defers reclaiming
// partial shards until the stripe has been rolled back for a configured age.
func (s *ECStore) RollbackConversion(st *ECStripe, reason string) error {
	st.State = ECConversionRolledBack
	st.RolledBackAt = time.Now().UnixNano()
	return s.PutStripe(st)
}

// MarkExtentColdEC writes the dormant V2.1 extent EC fields (§11.2): it marks
// the inode's inline extent as ColdEC and points it at an EC stripe. This is
// the persistence path that turns ExtentMetaV2.StorageClass/ECStripeID/
// Lifecycle from recorded-but-unwritten to durable metadata when a single
// extent converts 3-replica → 6+3.
//
// Only the inline (single-extent) layout is supported here: conversion targets
// small idle files (≤ 16 MiB, one extent), which is exactly the EC demographic.
// Multi-extent (paged) files report ErrExtentNotInline — the service path (E4)
// resolves those through the placement group.
// SwitchChunkToEC is the atomic §14 layout switch that lifts a durable,
// completed conversion stripe into the chunk's authoritative metadata: it
// replaces the chunk's 3-replica layout (ChunkMeta.Replicas) with the stripe's
// nine shard placements, sets ECGroup, and records the original checksum, so
// the chunk is thereafter served from the 6+3 shards rather than the old
// replicas. It is the server-side twin of datanode.BuildECGroup, driven here
// off the durable stripe so the switch is made from authoritative state.
//
// The write runs through the same tombstone-safe, exact-value conditional
// update (updateLiveChunkMetadata + readChunkTombstoneRaw) every other chunk
// mutation uses, so a concurrent heartbeat replica-state report or tombstone
// is never silently clobbered; non-layout fields (Size, Tier, CreateTime,
// Generation, PGID/Epoch, replica states) are preserved as-is.
//
// Transition-form note: this stores the full nine-shard layout per chunk
// (O(N×9) metadata). It is the PG-level-convergence *transition* form — long
// term the EC layout should converge to a placement-group / EC-profile level
// so a chunk references the profile rather than embedding all nine shards.
func (s *ECStore) SwitchChunkToEC(ctx context.Context, stripeID string) (*ChunkMeta, error) {
	if s.store.closed.Load() {
		return nil, ErrServiceClosed
	}
	st, err := s.GetStripe(stripeID)
	if err != nil {
		return nil, err
	}
	if st == nil {
		return nil, fmt.Errorf("ec: publish: stripe %q not found", stripeID)
	}
	if st.State != ECConversionComplete {
		return nil, fmt.Errorf("ec: publish: stripe %q state %s, want complete", stripeID, st.State)
	}
	chunkID := ChunkID(st.ExtentID)
	key := fmt.Sprintf("%s%d", prefixChunk, chunkID)

	raw, exists, err := s.store.readChunkTombstoneRaw(key)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, ErrChunkNotFound
	}
	var chunk ChunkMeta
	if err := unmarshalValue(raw, &chunk); err != nil {
		return nil, fmt.Errorf("ec: publish: decode chunk %d: %w", chunkID, err)
	}

	// Build the EC layout from the durable stripe, preserving every field the
	// datanode does not own (Size, Tier, CreateTime, Generation, PG/Epoch).
	// The layout references the shared ECProfile (shared config lives in the
	// profile row, not on every chunk) and the chunk points at the durable
	// stripe (ECStripeID) that holds the authoritative per-shard actual
	// landing (§14 / Program 5). ChunkMeta.Replicas is still materialized
	// below for V1 gateway / repair / ha / tombstone / migrate consumers —
	// and this publish step cross-checks it against the authoritative stripe
	// so the materialized copy can never silently diverge from the landing.
	layout := &ChunkMeta{
		ID:         chunk.ID,
		Size:       chunk.Size,
		State:      ChunkReady,
		ECGroup:    ECGroupFromProfile(nil, st.StripeID),
		Tier:       chunk.Tier,
		CreateTime: chunk.CreateTime,
		Checksum:   st.OriginalChecksum,
		PGID:       chunk.PGID,
		Epoch:      chunk.Epoch,
		Generation: chunk.Generation,
		ECStripeID: st.StripeID,
	}
	for _, sh := range st.Shards {
		layout.Replicas = append(layout.Replicas, ReplicaInfo{
			NodeID:     NodeID(sh.NodeID),
			ShardIndex: sh.Index,
		})
	}
	// Cross-check the materialized landing copy against the authoritative
	// stripe: same shard count and same node per shard index. A mismatch
	// means the denormalized Replicas would disagree with where the shards
	// actually landed — refuse to publish rather than persist a stale view.
	if len(layout.Replicas) != len(st.Shards) {
		return nil, fmt.Errorf("ec: publish: stripe %q has %d shards, built %d replicas", stripeID, len(st.Shards), len(layout.Replicas))
	}
	for i, sh := range st.Shards {
		if layout.Replicas[i].NodeID != NodeID(sh.NodeID) || layout.Replicas[i].ShardIndex != sh.Index {
			return nil, fmt.Errorf("ec: publish: stripe %q shard %d landing mismatch (stripe node %d idx %d vs replica node %d idx %d)",
				stripeID, i, sh.NodeID, sh.Index, layout.Replicas[i].NodeID, layout.Replicas[i].ShardIndex)
		}
	}
	if err := s.store.updateLiveChunkMetadata(ctx, raw, layout); err != nil {
		return nil, fmt.Errorf("ec: publish: switch chunk %d to EC: %w", chunkID, err)
	}
	s.store.publishEvent(Event{Type: EventSet, Key: fmt.Sprintf("chunk:%d", chunkID)})
	return layout, nil
}

func (s *ECStore) MarkExtentColdEC(id InodeID, extentID ExtentIDV2, stripeID string) error {
	inodes := NewInodeStoreV2(s.store)
	in, err := inodes.Get(id)
	if err != nil {
		return err
	}
	if in == nil {
		return ErrInodeNotFound
	}
	if in.InlineExtent == nil {
		return ErrExtentNotInline
	}
	if in.InlineExtent.ID != extentID {
		return fmt.Errorf("ec: inode %d has inline extent %d, not %d", id, in.InlineExtent.ID, extentID)
	}
	in.InlineExtent.Lifecycle = LifecycleECConverting
	in.InlineExtent.StorageClass = StorageClassColdEC
	in.InlineExtent.ECStripeID = stripeID
	return inodes.Put(in)
}
