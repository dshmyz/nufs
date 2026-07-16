package main

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/example/dfs/metadata"
)

// --- Chunk handlers ---

func (h *opsHandlers) handleChunks(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		inodeIDStr := r.URL.Query().Get("inode_id")
		if inodeIDStr != "" {
			var inodeID metadata.InodeID
			if _, err := fmt.Sscanf(inodeIDStr, "%d", &inodeID); err != nil {
				writeJSONError(w, http.StatusBadRequest, "invalid inode_id")
				return
			}
			refs, err := h.store.ListChunks(r.Context(), inodeID)
			if err != nil {
				writeJSONError(w, http.StatusInternalServerError, err.Error())
				return
			}
			writeJSON(w, refs)
			return
		}
		writeJSONError(w, http.StatusBadRequest, "inode_id required")
	case http.MethodPost:
		// Allocate chunk
		var req struct {
			InodeID metadata.InodeID        `json:"inode_id"`
			Offset  int64                   `json:"offset"`
			Policy  metadata.PlacementPolicy `json:"policy"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid request body")
			return
		}
		chunk, err := h.store.AllocateChunk(r.Context(), req.InodeID, req.Offset, req.Policy)
		if err != nil {
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
		w.WriteHeader(http.StatusCreated)
		writeJSON(w, chunk)
	default:
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (h *opsHandlers) handleChunksByID(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path[len("/api/v1/chunks/"):]

	var chunkID metadata.ChunkID
	if _, err := fmt.Sscanf(path, "%d", &chunkID); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid chunk ID")
		return
	}

	// Check for sub-paths (e.g., /api/v1/chunks/123/commit)
	var rest string
	if _, err := fmt.Sscanf(path, "%d/%s", &chunkID, &rest); err != nil {
		rest = ""
	}

	switch {
	case rest == "":
		switch r.Method {
		case http.MethodGet:
			chunk, err := h.store.GetChunk(r.Context(), chunkID)
			if err != nil {
				writeJSONError(w, http.StatusNotFound, err.Error())
				return
			}
			writeJSON(w, chunk)
		case http.MethodPut:
			var chunk metadata.ChunkMeta
			if err := json.NewDecoder(r.Body).Decode(&chunk); err != nil {
				writeJSONError(w, http.StatusBadRequest, "invalid request body")
				return
			}
			chunk.ID = chunkID
			if err := h.store.UpdateChunk(r.Context(), &chunk); err != nil {
				writeJSONError(w, http.StatusInternalServerError, err.Error())
				return
			}
			writeJSON(w, map[string]string{"status": "updated"})
		case http.MethodDelete:
			if err := h.store.DeleteChunk(r.Context(), chunkID); err != nil {
				writeJSONError(w, http.StatusInternalServerError, err.Error())
				return
			}
			writeJSON(w, map[string]string{"status": "deleted"})
		default:
			writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	case rest == "commit":
		if r.Method != http.MethodPost {
			writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		var req struct {
			Checksum uint32 `json:"checksum"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid request body")
			return
		}
		if err := h.store.CommitChunk(r.Context(), chunkID, req.Checksum); err != nil {
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, map[string]string{"status": "committed"})
	case rest == "seal":
		if r.Method != http.MethodPost {
			writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		if err := h.store.SealChunk(r.Context(), chunkID); err != nil {
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, map[string]string{"status": "sealed"})
	default:
		writeJSONError(w, http.StatusNotFound, "unknown chunk sub-path")
	}
}

func (h *opsHandlers) handleMigrateReplica(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req struct {
		ChunkID  metadata.ChunkID `json:"chunk_id"`
		FromNode metadata.NodeID  `json:"from_node"`
		ToNode   metadata.NodeID  `json:"to_node"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := h.store.MigrateChunkReplica(r.Context(), req.ChunkID, req.FromNode, req.ToNode); err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, map[string]string{"status": "migrated"})
}

func (h *opsHandlers) handleReportChunkState(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req struct {
		NodeID metadata.NodeID                           `json:"node_id"`
		States map[metadata.ChunkID]metadata.ReplicaState `json:"states"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := h.store.ReportChunkState(r.Context(), req.NodeID, req.States); err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, map[string]string{"status": "reported"})
}

func (h *opsHandlers) handleChunksBatch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req struct {
		InodeID metadata.InodeID        `json:"inode_id"`
		Offsets []int64                 `json:"offsets"`
		Policy  metadata.PlacementPolicy `json:"policy"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	chunks, err := h.store.AllocateChunksBatch(r.Context(), req.InodeID, req.Offsets, req.Policy)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusCreated)
	writeJSON(w, chunks)
}
