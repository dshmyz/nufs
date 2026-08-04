package metadata

import "fmt"

// ECProfile is the shared, referenced EC configuration (Program 5, §14
// consolidation). It captures the *common* structure of an erasure-coded
// group — the parameters that are identical across every EC chunk (6 data, 3
// parity, fault-domain bounds) — so that structure is stored once in a profile
// row and each ChunkMeta references it by ID (ChunkMeta.ECGroup.ProfileID)
// rather than repeating the config on every chunk row.
//
// The profile deliberately does NOT hold "which disks a particular stripe
// actually landed on": that per-stripe actual-landing state is authoritative in
// the durable, write-once ECStripe registry (ECStripe.Shards) and is referenced
// by ChunkMeta.ECStripeID. Sharing the structural config must never collapse
// away the per-stripe landing.
type ECProfile struct {
	// ID identifies the profile (key "ec-profile/<id>").
	ID string `json:"id"`
	// DataShards / ParityShards are the k/m of the EC scheme (6+3).
	DataShards   int `json:"data_shards"`
	ParityShards int `json:"parity_shards"`
	// MinMachines / MaxPerMachine are the fault-domain diversity bounds
	// (§14): at least MinMachines distinct nodes, no node over
	// MaxPerMachine shards.
	MinMachines   int `json:"min_machines"`
	MaxPerMachine int `json:"max_per_machine"`
}

// DefaultECProfileID is the ID of the canonical 6+3 profile.
const DefaultECProfileID = "ec-6-3"

// DefaultECProfile returns the canonical 6+3 EC profile, built from the
// already-defined §14 constants (ECDataShards/ECParityShards/ECMinMachines/
// ECMaxShardsPerMachine) so there is a single source of truth.
func DefaultECProfile() *ECProfile {
	return &ECProfile{
		ID:            DefaultECProfileID,
		DataShards:    ECDataShards,
		ParityShards:  ECParityShards,
		MinMachines:   ECMinMachines,
		MaxPerMachine: ECMaxShardsPerMachine,
	}
}

// ecProfileKey formats a profile key.
func ecProfileKey(id string) string {
	return "ec-profile/" + id
}

// GetProfile reads an EC profile by ID, or nil when it does not exist.
func (s *ECStore) GetProfile(id string) (*ECProfile, error) {
	var p ECProfile
	exists, err := s.store.getValue(ecProfileKey(id), &p)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, nil
	}
	return &p, nil
}

// SelectOrCreateProfile returns the profile with the given ID, creating it
// from cfg if it does not yet exist. It is a read-mostly pattern: profiles are
// created once and then shared by every referencing chunk, so hot allocations
// converge on a single row rather than one profile per chunk.
func (s *ECStore) SelectOrCreateProfile(id string, cfg *ECProfile) (*ECProfile, error) {
	p, err := s.GetProfile(id)
	if err != nil {
		return nil, err
	}
	if p != nil {
		return p, nil
	}
	return cfg, s.store.putMsgpack(ecProfileKey(id), cfg)
}

// ECGroupFromProfile materializes a ChunkMeta.ECGroup pointer that references
// the given profile (ProfileID) and retains the legacy embedded
// DataShards/ParityShards for read-compatible consumers that do not resolve
// the profile. groupID is the group/stripe identifier carried alongside.
func ECGroupFromProfile(p *ECProfile, groupID string) *ECGroupInfo {
	if p == nil {
		p = DefaultECProfile()
	}
	return &ECGroupInfo{
		GroupID:      groupID,
		ProfileID:    p.ID,
		DataShards:   p.DataShards,
		ParityShards: p.ParityShards,
	}
}

// ResolveStripeLanding returns the authoritative *actual landing* for an EC
// chunk from the durable, write-once ECStripe registry: the per-shard
// (Index, NodeID, DiskID, SegmentID, Offset, Checksum) placements that record
// which disks this stripe actually landed on. It resolves the stripe through
// ChunkMeta.ECStripeID (falling back to ECGroup.GroupID for rows written
// before the explicit pointer existed) and is the preferred entry for any
// consumer that wants the full landing rather than the materialized
// ChunkMeta.Replicas copy.
//
// It returns nil, nil when the chunk is not an EC chunk (no stripe reference).
func (s *ECStore) ResolveStripeLanding(chunk *ChunkMeta) ([]ECShard, error) {
	stripeID := chunk.ECStripeID
	if stripeID == "" && chunk.ECGroup != nil {
		stripeID = chunk.ECGroup.GroupID
	}
	if stripeID == "" {
		return nil, nil
	}
	st, err := s.GetStripe(stripeID)
	if err != nil {
		return nil, err
	}
	if st == nil {
		return nil, fmt.Errorf("ec: resolve landing: stripe %q not found", stripeID)
	}
	return st.Shards, nil
}
