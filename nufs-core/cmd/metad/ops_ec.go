package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// timeFromUnixNano converts the wire time (unix nanoseconds) to time.Time.
func timeFromUnixNano(ns int64) time.Time {
	if ns == 0 {
		return time.Now()
	}
	return time.Unix(0, ns)
}

// --- EC conversion lifecycle handlers (Program A / S2) ---
//
// These endpoints expose the metadata ECStore conversion transaction over the
// ops HTTP surface, so a datanode on the V2.1 serving path can drive a
// replication→6+3 conversion against a *remote* authority (the production
// topology) instead of the in-process local Pebble stand-in (S1). The
// authority owns the §14 placement decision and the transaction state machine
// (Preparing → Encoding → Syncing → Complete | RolledBack); the datanode
// supplies the shard payload and writes the shard extents it is assigned.
//
// The contract mirrors metadata.ECAuthority (datanode/ec_service.go), one
// transaction step per endpoint. Each step that mutates an existing stripe
// (plan / mark-syncing / complete / rollback) loads the authoritative copy
// from the store by StripeID, applies the transition, persists, and returns
// the full updated stripe so the caller's in-memory copy stays authoritative.
// Begin is the only step that creates the stripe.

// ecStore lazily builds the EC authority over the backing Pebble store.
func (h *opsHandlers) ecStore() *metadata.ECStore {
	return metadata.NewECStore(h.store)
}

// beginECConvert: POST /api/v1/ec/convert/begin
// {stripe_id, extent_id, generation, checksum} → ECStripe (Preparing).
func (h *opsHandlers) handleECConvertBegin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req struct {
		StripeID string `json:"stripe_id"`
		ExtentID uint64 `json:"extent_id"`
		Gen      uint64 `json:"generation"`
		Checksum uint32 `json:"checksum"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.StripeID == "" {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	st, err := h.ecStore().BeginConversion(req.StripeID, req.ExtentID, req.Gen, req.Checksum)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, st)
}

// planECConvert: POST /api/v1/ec/convert/plan
// {stripe_id, disks:[]ECDisk} → ECStripe (Encoding, Shards filled §14).
func (h *opsHandlers) handleECConvertPlan(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req struct {
		StripeID string            `json:"stripe_id"`
		Disks    []metadata.ECDisk `json:"disks"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.StripeID == "" {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	st, err := h.ecStore().GetStripe(req.StripeID)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if st == nil {
		writeJSONError(w, http.StatusNotFound, "stripe not found")
		return
	}
	if err := h.ecStore().PlanShards(st, req.Disks); err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, st)
}

// markSyncingECConvert: POST /api/v1/ec/convert/mark-syncing
// {stripe_id} → ECStripe (Syncing).
func (h *opsHandlers) handleECConvertMarkSyncing(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req struct {
		StripeID string `json:"stripe_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.StripeID == "" {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	st, err := h.ecStore().GetStripe(req.StripeID)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if st == nil {
		writeJSONError(w, http.StatusNotFound, "stripe not found")
		return
	}
	if err := h.ecStore().MarkSyncing(st); err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, st)
}

// completeECConvert: POST /api/v1/ec/convert/complete
// {stripe_id, at} → ECStripe (Complete).
func (h *opsHandlers) handleECConvertComplete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req struct {
		StripeID string `json:"stripe_id"`
		At       int64  `json:"at"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.StripeID == "" {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	st, err := h.ecStore().GetStripe(req.StripeID)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if st == nil {
		writeJSONError(w, http.StatusNotFound, "stripe not found")
		return
	}
	if err := h.ecStore().CompleteConversion(st, timeFromUnixNano(req.At)); err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, st)
}

// publishECConvert: POST /api/v1/ec/convert/publish
// {stripe_id} → atomic §14 chunk layout switch + updated ECStripe.
func (h *opsHandlers) handleECConvertPublish(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req struct {
		StripeID string `json:"stripe_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.StripeID == "" {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	st, err := h.ecStore().GetStripe(req.StripeID)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if st == nil {
		writeJSONError(w, http.StatusNotFound, "stripe not found")
		return
	}
	if _, err := h.ecStore().SwitchChunkToEC(r.Context(), req.StripeID); err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, st)
}

// rollbackECConvert: POST /api/v1/ec/convert/rollback
// {stripe_id, reason} → ECStripe (RolledBack).
func (h *opsHandlers) handleECConvertRollback(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req struct {
		StripeID string `json:"stripe_id"`
		Reason   string `json:"reason"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.StripeID == "" {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	st, err := h.ecStore().GetStripe(req.StripeID)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if st == nil {
		writeJSONError(w, http.StatusNotFound, "stripe not found")
		return
	}
	if err := h.ecStore().RollbackConversion(st, req.Reason); err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, st)
}

// resolveLandingECConvert: POST /api/v1/ec/convert/resolve-landing
// {chunk_id} → {shards} the chunk's authoritative per-shard landing (§14).
//
// Exposes the local *metadata.ECStore.ResolveStripeLanding over HTTP so a
// V2.1 datanode's ECSelfHealer can run its repair-landing against a *remote*
// metadata authority (production topology) instead of the in-process local
// Pebble stand-in (Program 7). The server stays authoritative: it loads the
// chunk itself and resolves the stripe from durable ECStripe.Shards. A chunk
// with no stripe (not yet converted to EC) yields {"shards":null} — the caller
// falls back to least-used disk placement; a referenced-but-missing stripe
// yields an error (500), which the caller also degrades on.
func (h *opsHandlers) handleECResolveLanding(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req struct {
		ChunkID uint64 `json:"chunk_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.ChunkID == 0 {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	chunk, err := h.store.GetChunk(r.Context(), metadata.ChunkID(req.ChunkID))
	if err != nil {
		writeJSONError(w, http.StatusNotFound, "chunk not found")
		return
	}
	shards, err := h.ecStore().ResolveStripeLanding(chunk)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, struct {
		Shards []metadata.ECShard `json:"shards"`
	}{Shards: shards})
}

// planECWrite: POST /api/v1/ec/plan-write
// {chunk_id, data_shards, parity_shards} → {shards:[]ECShard{Index,NodeID,DiskID}}
//
// The write-path direct-EC authority (§14, Program 10): when the gateway writes
// an object into an ECConfig bucket it encodes K+M shards up front and pushes
// each shard straight to the owning node's shard store — no intermediate
// replica. It needs to know *which disk on which node* each shard lands on,
// and that decision is the metadata authority's (§14 fault-domain diversity),
// not the gateway's. This endpoint computes it via PebbleStore.PlanECWrite: it
// loads the chunk (whose allocation already fixed Replicas[i].NodeID + Addr for
// each shard index) and the current cluster topology, then assigns each shard a
// disk on its owning node via a deterministic per-node round-robin. No state is
// mutated — this is a pure placement query; the durable stripe is only recorded
// after the shards actually land (record-direct).
//
// When the topology cannot meet §14 bounds (too few V2.1 nodes / shard disks,
// or a node missing from the topology) it fails with 424 so the gateway fails
// the ECConfig write: V1 whole-shard EC is retired (docs/v1-retirement-roadmap.md
// stage 3), so a write must never silently degrade to a layout the V2.1 serving
// path cannot range-read.
func (h *opsHandlers) handleECPlanWrite(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req struct {
		ChunkID      uint64 `json:"chunk_id"`
		DataShards   int    `json:"data_shards"`
		ParityShards int    `json:"parity_shards"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.ChunkID == 0 {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	plan, err := h.store.PlanECWrite(r.Context(), metadata.ChunkID(req.ChunkID), req.DataShards, req.ParityShards)
	if err != nil {
		switch {
		case errors.Is(err, metadata.ErrECPlanChunkNotFound):
			writeJSONError(w, http.StatusNotFound, "chunk not found")
		case errors.Is(err, metadata.ErrECPlanShardMismatch):
			writeJSONError(w, http.StatusBadRequest, err.Error())
		case errors.Is(err, metadata.ErrECPlanTopology):
			writeJSONError(w, http.StatusFailedDependency, err.Error())
		default:
			writeJSONError(w, http.StatusInternalServerError, err.Error())
		}
		return
	}
	writeJSON(w, struct {
		Shards []metadata.ECShard `json:"shards"`
	}{Shards: plan})
}

// recordDirectEC: POST /api/v1/ec/record-direct
// {chunk_id, shards:[]ECShard, data_shards, parity_shards, original_checksum}
// → ChunkMeta (ECStripeID set) + ECStripe (Complete).
//
// The second half of write-path direct EC (§14, Program 10): after the gateway
// has encoded and pushed every shard to its owning node's shard store, it
// reports here so the metadata authority durably lifts the chunk into EC — a
// Complete stripe + ChunkMeta.ECStripeID — which is exactly the state a
// converted chunk reaches through publish. The server stays authoritative: it
// builds the durable stripe from the supplied plan + checksum (in the same
// write-once PutStripe keyed by the chunk's allocation group ID) and atomically
// switches the chunk's metadata, preserving the allocated Replicas (with live
// Addr) that the gateway read path dials. The shards' node per index must match
// the allocation (PlanECWrite produced it), enforced by ECStore.RecordDirect.
func (h *opsHandlers) handleECRecordDirect(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req struct {
		ChunkID          uint64             `json:"chunk_id"`
		Shards           []metadata.ECShard `json:"shards"`
		DataShards       int                `json:"data_shards"`
		ParityShards     int                `json:"parity_shards"`
		OriginalChecksum uint32             `json:"original_checksum"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.ChunkID == 0 || len(req.Shards) == 0 {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	layout, st, err := h.ecStore().RecordDirect(r.Context(), metadata.ChunkID(req.ChunkID), req.Shards, req.OriginalChecksum)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, struct {
		Chunk  *metadata.ChunkMeta `json:"chunk"`
		Stripe *metadata.ECStripe  `json:"stripe"`
	}{Chunk: layout, Stripe: st})
}

// isOrphanECConvert: POST /api/v1/ec/convert/is-orphan
// {chunk_id, older_than_ns} → {orphaned}.
//
// Exposes the local *metadata.ECStore.IsChunkShardsOrphaned over HTTP so a
// V2.1 datanode's ECSelfHealer.ReclaimOrphans runs against a *remote* metadata
// authority (production topology) instead of the in-process local Pebble
// stand-in (Program 7). older_than_ns is the age gate below which a
// rolled-back stripe's shards are not yet reclaimable, encoded as nanoseconds.
func (h *opsHandlers) handleECIsOrphan(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req struct {
		ChunkID     uint64 `json:"chunk_id"`
		OlderThanNS int64  `json:"older_than_ns"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.ChunkID == 0 {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	orphaned, err := h.ecStore().IsChunkShardsOrphaned(
		r.Context(), metadata.ChunkID(req.ChunkID), time.Duration(req.OlderThanNS))
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, struct {
		Orphaned bool `json:"orphaned"`
	}{Orphaned: orphaned})
}

// handleECConversionQueue: GET /api/v1/ec/convert/queue
//
// Returns the queued EC conversion background tasks (eligible extents
// discovered by the ECConversionScheduler, awaiting datanode-side processing).
// Read-only; no mutation. Used by operators and the future ConversionWorker
// datanode-side poll loop.
func (h *opsHandlers) handleECConversionQueue(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	tasks, err := h.store.ConversionQueue(r.Context())
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, tasks)
}
