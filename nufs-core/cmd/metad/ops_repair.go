package main

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// --- Repair handlers ---

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
	writeJSON(w, tasks)
}

func (h *opsHandlers) handleTriggerRepair(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req struct {
		ChunkID metadata.ChunkID `json:"chunk_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := h.store.TriggerRepair(r.Context(), req.ChunkID); err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
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
