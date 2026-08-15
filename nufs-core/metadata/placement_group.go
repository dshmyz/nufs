package metadata

import (
	"encoding/binary"
	"fmt"
	"sort"
)

// PlacementGroup is a stable replica set (V2.1 §11.3). The 100-million
// tier uses 4096 PGs; the 1-billion tier uses 16384-65536 after
// capacity modeling. Each hot-data PG maps to three replica nodes across
// fault domains. Rebalance changes PG assignments (epoch bump) without
// rewriting every extent metadata record.
type PlacementGroup struct {
	// ID is the placement group ID.
	ID uint32 `json:"id"`
	// Epoch is the current placement epoch. Extents store the epoch
	// they were placed under; reads resolve through it.
	Epoch uint64 `json:"epoch"`
	// ReplicaNodes lists the node IDs for the current epoch, ordered
	// across fault domains.
	ReplicaNodes []NodeID `json:"replica_nodes"`
	// PrevEpoch / PrevReplicas remain resolvable until migration
	// completes (§11.3: "old epochs remain resolvable until migration
	// completes").
	PrevEpoch    uint64   `json:"prev_epoch,omitempty"`
	PrevReplicas []NodeID `json:"prev_replicas,omitempty"`
	// State tracks lifecycle.
	State PGState `json:"state"`
}

// PGState is the lifecycle state of a placement group.
type PGState uint8

const (
	PGStable PGState = iota
	PGMigrating
	PGDraining
)

func (s PGState) String() string {
	switch s {
	case PGStable:
		return "stable"
	case PGMigrating:
		return "migrating"
	case PGDraining:
		return "draining"
	default:
		return "unknown"
	}
}

// PGMigration is a PG epoch migration record (§11.3). Writes after the
// cutover sequence use the target epoch; reads resolve the extent's
// stored epoch and may consult both source and target while its
// inventory partition is migrating.
type PGMigration struct {
	PGID               uint32         `json:"pg_id"`
	SourceEpoch        uint64         `json:"source_epoch"`
	TargetEpoch        uint64         `json:"target_epoch"`
	CutoverSeq         uint64         `json:"cutover_sequence"`
	SourceReplicas     []NodeID       `json:"source_replicas"`
	TargetReplicas     []NodeID       `json:"target_replicas"`
	InventoryPartition uint32         `json:"inventory_partition"`
	MigrationCursor    uint64         `json:"migration_cursor"`
	State              MigrationState `json:"state"`
}

// MigrationState is the PG migration lifecycle.
type MigrationState uint8

const (
	MigPending MigrationState = iota
	MigInProgress
	MigCommitted
	MigComplete
)

func (s MigrationState) String() string {
	switch s {
	case MigPending:
		return "pending"
	case MigInProgress:
		return "in_progress"
	case MigCommitted:
		return "committed"
	case MigComplete:
		return "complete"
	default:
		return "unknown"
	}
}

// PlacementGroupStore manages placement groups and their epochs.
type PlacementGroupStore struct {
	store *PebbleStore
}

// NewPlacementGroupStore creates the PG store.
func NewPlacementGroupStore(store *PebbleStore) *PlacementGroupStore {
	return &PlacementGroupStore{store: store}
}

// pgKey formats the PG key.
func pgKey(id uint32) string {
	return fmt.Sprintf("%s%d", prefixPlacementGroup, id)
}

// Get reads a placement group.
func (s *PlacementGroupStore) Get(id uint32) (*PlacementGroup, error) {
	var pg PlacementGroup
	exists, err := s.store.getValue(pgKey(id), &pg)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, nil
	}
	return &pg, nil
}

// Put writes a placement group.
func (s *PlacementGroupStore) Put(pg *PlacementGroup) error {
	return s.store.putMsgpack(pgKey(pg.ID), pg)
}

// CreatePG creates a placement group with an initial replica set.
func (s *PlacementGroupStore) CreatePG(id uint32, replicas []NodeID) (*PlacementGroup, error) {
	pg := &PlacementGroup{ID: id, Epoch: 1, ReplicaNodes: replicas, State: PGStable}
	if err := s.Put(pg); err != nil {
		return nil, err
	}
	return pg, nil
}

// SelectOrCreatePG returns the placement group with id, creating it with the
// given initial replica set if it does not yet exist. This is the convergent
// path the serving layer uses: the caller derives id deterministically from
// the replica set (e.g. a content hash of the sorted node set), so the same
// replica set always resolves to the same PG regardless of which node first
// placed an extent — bounded PG growth, no unbounded per-extent scoring.
func (s *PlacementGroupStore) SelectOrCreatePG(id uint32, replicas []NodeID) (*PlacementGroup, error) {
	pg, err := s.Get(id)
	if err != nil {
		return nil, err
	}
	if pg != nil {
		return pg, nil
	}
	return s.CreatePG(id, replicas)
}

// Rebalance starts a PG migration to a new replica set (§11.3): it
// bumps the epoch, records the previous epoch for resolution, and marks
// the PG migrating. Returns the migration record.
func (s *PlacementGroupStore) Rebalance(pgID uint32, newReplicas []NodeID, cutoverSeq uint64, inventoryPartition uint32) (*PGMigration, error) {
	pg, err := s.Get(pgID)
	if err != nil {
		return nil, err
	}
	if pg == nil {
		return nil, fmt.Errorf("metadata: placement group %d not found", pgID)
	}
	pg.PrevEpoch = pg.Epoch
	pg.PrevReplicas = pg.ReplicaNodes
	pg.Epoch++
	pg.ReplicaNodes = newReplicas
	pg.State = PGMigrating
	if err := s.Put(pg); err != nil {
		return nil, err
	}
	// Persist the migration record for resumable progress.
	mig := &PGMigration{
		PGID:               pgID,
		SourceEpoch:        pg.PrevEpoch,
		TargetEpoch:        pg.Epoch,
		CutoverSeq:         cutoverSeq,
		SourceReplicas:     pg.PrevReplicas,
		TargetReplicas:     newReplicas,
		InventoryPartition: inventoryPartition,
		State:              MigInProgress,
	}
	if err := s.store.putMsgpack(migrationKey(pgID), mig); err != nil {
		return nil, err
	}
	return mig, nil
}

// ResolveReplicas returns the replica set for an extent placed under a
// specific PG epoch. If the epoch is the current one, the current set is
// returned; if it is a previous epoch still migrating, the source set is
// returned so reads can consult both (§11.3).
func (s *PlacementGroupStore) ResolveReplicas(pgID uint32, epoch uint64) ([]NodeID, bool, error) {
	pg, err := s.Get(pgID)
	if err != nil {
		return nil, false, err
	}
	if pg == nil {
		return nil, false, fmt.Errorf("metadata: placement group %d not found", pgID)
	}
	if epoch == pg.Epoch {
		return pg.ReplicaNodes, false, nil
	}
	if pg.PrevEpoch != 0 && epoch == pg.PrevEpoch {
		// Migration in progress: source set still resolvable.
		return pg.PrevReplicas, true, nil
	}
	return nil, false, fmt.Errorf("metadata: placement group %d epoch %d no longer resolvable", pgID, epoch)
}

// CompleteMigration finalizes a PG migration once inventory proves the
// target epoch complete (§11.3: source replicas removed only after
// every partition cursor completes).
func (s *PlacementGroupStore) CompleteMigration(pgID uint32) error {
	pg, err := s.Get(pgID)
	if err != nil {
		return err
	}
	if pg == nil {
		return nil
	}
	pg.PrevEpoch = 0
	pg.PrevReplicas = nil
	pg.State = PGStable
	if err := s.Put(pg); err != nil {
		return err
	}
	return s.store.db.Delete([]byte(migrationKey(pgID)), nil)
}

// migrationKey formats the migration record key.
func migrationKey(pgID uint32) string {
	return fmt.Sprintf("%s%d", prefixPGRebalance, pgID)
}

// EncodeExtentIDV2 builds an extent ID with the owning logical partition
// in the high 16 bits (§11.4).
func EncodeExtentIDV2(partition uint16, low uint64) ExtentIDV2 {
	return ExtentIDV2(uint64(partition)<<48 | (low & 0x0000FFFFFFFFFFFF))
}

// placementGroupIDForNodes derives a stable placement-group ID from a replica
// node set: the FNV hash of the node IDs sorted ascending. Content-addressed
// so the same replica set always maps to the same PG regardless of which node
// first placed an extent — deterministic across leader failover and bounded
// (distinct replica sets converge to shared PGs rather than one PG per extent).
func placementGroupIDForNodes(nodeIDs []NodeID) uint32 {
	sorted := make([]NodeID, len(nodeIDs))
	copy(sorted, nodeIDs)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	var h uint32 = 2166136261
	for _, id := range sorted {
		v := uint64(id)
		for i := 0; i < 8; i++ {
			h ^= uint32(v & 0xff)
			h *= 16777619
			v >>= 8
		}
	}
	return h
}

var _ = binary.BigEndian
var _ = sort.Strings
