package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// --- Repair handlers ---

// extentLifecycleName maps an ExtentLifecycle to the operator-facing name
// used to annotate repair-queue entries, so extent-triggered repairs are
// distinguishable from legacy chunk repairs at a glance.
func extentLifecycleName(lc metadata.ExtentLifecycle) string {
	switch lc {
	case metadata.LifecycleReady:
		return "ready"
	case metadata.LifecycleReadyDegraded:
		return "ready_degraded"
	case metadata.LifecycleMigrating:
		return "migrating"
	case metadata.LifecycleDeleting:
		return "deleting"
	case metadata.LifecycleDeleted:
		return "deleted"
	case metadata.LifecycleECConverting:
		return "ec_converting"
	default:
		return "unknown"
	}
}

func (h *opsHandlers) handleRepairQueue(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	tasks, err := h.store.GetRepairQueue(r.Context())
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	// Annotate each task with whether its chunk ID is backed by a V2 extent
	// row (extent ID == chunk ID invariant) and that extent's lifecycle. The
	// response stays backward compatible: embedded RepairTask serializes as
	// before, and Go consumers ignore unknown fields.
	entries := make([]repairQueueEntry, 0, len(tasks))
	for _, t := range tasks {
		entry := repairQueueEntry{RepairTask: t}
		ext, err := h.store.GetExtentMeta(r.Context(), metadata.ExtentIDV2(t.ChunkID))
		if err == nil {
			entry.IsExtent = true
			entry.ExtentLifecycle = extentLifecycleName(ext.Lifecycle)
		}
		entries = append(entries, entry)
	}
	writeJSON(w, entries)
}

// repairQueueEntry is a RepairTask with extent-model annotations.
type repairQueueEntry struct {
	metadata.RepairTask
	IsExtent        bool   `json:"is_extent,omitempty"`
	ExtentLifecycle string `json:"extent_lifecycle,omitempty"`
}

func (h *opsHandlers) handleTriggerRepair(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req struct {
		ChunkID  metadata.ChunkID    `json:"chunk_id"`
		ExtentID metadata.ExtentIDV2 `json:"extent_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	// extent_id and chunk_id are the same numeric space (extent ID == chunk ID
	// invariant); the extent variant validates the /extent-meta row first so a
	// stale ID fails fast instead of queueing a repair for a GC'd chunk.
	switch {
	case req.ExtentID != 0:
		if err := h.store.TriggerExtentRepair(r.Context(), req.ExtentID); err != nil {
			if errors.Is(err, metadata.ErrExtentNotFound) {
				writeJSONError(w, http.StatusNotFound, err.Error())
				return
			}
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
	case req.ChunkID != 0:
		if err := h.store.TriggerRepair(r.Context(), req.ChunkID); err != nil {
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
	default:
		writeJSONError(w, http.StatusBadRequest, "chunk_id or extent_id required")
		return
	}
	writeJSON(w, map[string]string{"status": "triggered"})
}

func (h *opsHandlers) handleRepairByID(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path[len("/api/v1/repair/"):]
	var chunkID metadata.ChunkID
	if _, err := fmt.Sscanf(path, "%d", &chunkID); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid chunk ID")
		return
	}
	switch r.Method {
	case http.MethodDelete:
		if err := h.store.RemoveRepairTask(r.Context(), chunkID); err != nil {
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, map[string]string{"status": "removed"})
	default:
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

// --- Rebalance handlers ---

func (h *opsHandlers) handleTriggerRebalance(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if err := h.store.TriggerRebalance(r.Context()); err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, map[string]string{"status": "triggered"})
}
