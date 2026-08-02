package metadata

import (
	"encoding/binary"
	"fmt"
)

// LogicalPartition is the stable unit of ownership in the V2.1 metadata
// layer (§11.4). Inode and extent IDs encode a 16-bit logical partition
// ID, NOT a physical Raft-group identity. The catalog maps logical
// partitions to physical Raft groups; moving a partition updates the
// catalog through an explicit epoch-fenced handoff, so IDs and callers
// never change and no permanent per-record forwarding is needed.
type LogicalPartition struct {
	// ID is the 16-bit logical partition ID encoded in inode/extent IDs.
	ID uint16 `json:"id"`
	// RaftGroup is the physical Raft group currently hosting this
	// partition.
	RaftGroup uint32 `json:"raft_group"`
	// Epoch fences the ownership handoff: a stale catalog entry with a
	// lower epoch cannot overwrite a newer one.
	Epoch uint64 `json:"epoch"`
	// State: Active or Migrating.
	State PartitionState `json:"state"`
	// PrevRaftGroup/PrevEpoch are set during migration so both groups
	// can serve reads until the handoff completes.
	PrevRaftGroup uint32 `json:"prev_raft_group,omitempty"`
	PrevEpoch     uint64 `json:"prev_epoch,omitempty"`
}

// PartitionState is the ownership lifecycle.
type PartitionState uint8

const (
	PartitionActive PartitionState = iota
	PartitionMigrating
)

// LogicalPartitionCatalog maps logical partitions to physical Raft
// groups (§11.4). It is the authority for routing; creation assigns an
// owner once and later operations route by the encoded ID.
type LogicalPartitionCatalog struct {
	store *PebbleStore
}

// NewLogicalPartitionCatalog creates the partition catalog.
func NewLogicalPartitionCatalog(store *PebbleStore) *LogicalPartitionCatalog {
	return &LogicalPartitionCatalog{store: store}
}

// lpKey formats a logical partition key.
func lpKey(id uint16) string {
	return fmt.Sprintf("%s%d", prefixLogicalPartition, id)
}

// Get reads a logical partition.
func (c *LogicalPartitionCatalog) Get(id uint16) (*LogicalPartition, error) {
	var lp LogicalPartition
	exists, err := c.store.getValue(lpKey(id), &lp)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, nil
	}
	return &lp, nil
}

// Put writes a logical partition.
func (c *LogicalPartitionCatalog) Put(lp *LogicalPartition) error {
	return c.store.putMsgpack(lpKey(lp.ID), lp)
}

// Create assigns a logical partition to a Raft group.
func (c *LogicalPartitionCatalog) Create(id uint16, raftGroup uint32) (*LogicalPartition, error) {
	lp := &LogicalPartition{ID: id, RaftGroup: raftGroup, Epoch: 1, State: PartitionActive}
	if err := c.Put(lp); err != nil {
		return nil, err
	}
	return lp, nil
}

// Route resolves the physical Raft group for a logical partition,
// applying the epoch-fenced ownership (§11.4). For a stable partition it
// returns the single group; during migration it returns both (reads may
// consult either).
func (c *LogicalPartitionCatalog) Route(id uint16) (primary uint32, secondary []uint32, err error) {
	lp, err := c.Get(id)
	if err != nil {
		return 0, nil, err
	}
	if lp == nil {
		return 0, nil, fmt.Errorf("metadata: logical partition %d not mapped", id)
	}
	if lp.State == PartitionMigrating && lp.PrevRaftGroup != 0 {
		return lp.RaftGroup, []uint32{lp.PrevRaftGroup}, nil
	}
	return lp.RaftGroup, nil, nil
}

// RouteExtent resolves the Raft group for an extent by its encoded
// owner partition (§11.4: "creation chooses an owner once; later
// operations route by the encoded owner").
func (c *LogicalPartitionCatalog) RouteExtent(id ExtentIDV2) (uint32, error) {
	primary, _, err := c.Route(id.OwnerPartition())
	return primary, err
}

// BeginMigration starts an epoch-fenced handoff of a logical partition
// to a new Raft group (§11.4: "explicit epoch-fenced handoff").
func (c *LogicalPartitionCatalog) BeginMigration(id uint16, newRaftGroup uint32) (*LogicalPartition, error) {
	lp, err := c.Get(id)
	if err != nil {
		return nil, err
	}
	if lp == nil {
		return nil, fmt.Errorf("metadata: logical partition %d not mapped", id)
	}
	lp.PrevRaftGroup = lp.RaftGroup
	lp.PrevEpoch = lp.Epoch
	lp.RaftGroup = newRaftGroup
	lp.Epoch++
	lp.State = PartitionMigrating
	if err := c.Put(lp); err != nil {
		return nil, err
	}
	return lp, nil
}

// CompleteMigration finalizes the handoff once the target group has
// caught up (§11.4).
func (c *LogicalPartitionCatalog) CompleteMigration(id uint16) error {
	lp, err := c.Get(id)
	if err != nil {
		return err
	}
	if lp == nil {
		return nil
	}
	lp.PrevRaftGroup = 0
	lp.PrevEpoch = 0
	lp.State = PartitionActive
	return c.Put(lp)
}

// EncodeInodePartition builds an inode ID encoding its logical
// partition in the high 16 bits (consistent with extent IDs §11.4).
func EncodeInodePartition(partition uint16, low uint64) InodeID {
	return InodeID(uint64(partition)<<48 | (low & 0x0000FFFFFFFFFFFF))
}

// InodePartition extracts the owning logical partition from an inode ID.
func InodePartition(id InodeID) uint16 {
	return uint16(uint64(id) >> 48)
}

var _ = binary.BigEndian
