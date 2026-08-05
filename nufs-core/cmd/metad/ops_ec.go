package main

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/example/dfs/metadata"
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
