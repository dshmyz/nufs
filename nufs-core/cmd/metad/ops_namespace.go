package main

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/example/dfs/metadata"
)

// --- Namespace handlers (mkdir/rmdir/readdir/create/unlink/lookup/
//     rename/symlink/readlink/link) and the per-inode operations.

// These are POST-only or GET-only endpoints with small JSON request
// bodies. They follow the same pattern: decode, call the metadata
// service, write the JSON response.

func (h *opsHandlers) handleMkDir(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req struct {
		Parent metadata.InodeID `json:"parent"`
		Name   string           `json:"name"`
		Mode   uint32           `json:"mode"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	meta, err := h.store.MkDir(r.Context(), req.Parent, req.Name, req.Mode)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusCreated)
	writeJSON(w, meta)
}

func (h *opsHandlers) handleRmDir(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req struct {
		Parent metadata.InodeID `json:"parent"`
		Name   string           `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := h.store.RmDir(r.Context(), req.Parent, req.Name); err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, map[string]string{"status": "removed"})
}

func (h *opsHandlers) handleReadDir(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var parent metadata.InodeID
	offset := 0
	limit := 1000
	fmt.Sscanf(r.URL.Query().Get("parent"), "%d", &parent)
	fmt.Sscanf(r.URL.Query().Get("offset"), "%d", &offset)
	fmt.Sscanf(r.URL.Query().Get("limit"), "%d", &limit)
	entries, err := h.store.ReadDir(r.Context(), parent, offset, limit)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, entries)
}

func (h *opsHandlers) handleCreateFile(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req struct {
		Parent metadata.InodeID `json:"parent"`
		Name   string           `json:"name"`
		Mode   uint32           `json:"mode"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	meta, err := h.store.CreateFile(r.Context(), req.Parent, req.Name, req.Mode)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusCreated)
	writeJSON(w, meta)
}

func (h *opsHandlers) handleUnlink(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req struct {
		Parent metadata.InodeID `json:"parent"`
		Name   string           `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := h.store.Unlink(r.Context(), req.Parent, req.Name); err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, map[string]string{"status": "unlinked"})
}

func (h *opsHandlers) handleLookup(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var parent metadata.InodeID
	name := r.URL.Query().Get("name")
	fmt.Sscanf(r.URL.Query().Get("parent"), "%d", &parent)
	meta, err := h.store.Lookup(r.Context(), parent, name)
	if err != nil {
		writeJSONError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, meta)
}

func (h *opsHandlers) handleRename(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req struct {
		OldParent metadata.InodeID `json:"old_parent"`
		OldName   string           `json:"old_name"`
		NewParent metadata.InodeID `json:"new_parent"`
		NewName   string           `json:"new_name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := h.store.Rename(r.Context(), req.OldParent, req.OldName, req.NewParent, req.NewName); err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, map[string]string{"status": "renamed"})
}

func (h *opsHandlers) handleSymlink(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req struct {
		Parent metadata.InodeID `json:"parent"`
		Name   string           `json:"name"`
		Target string           `json:"target"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	meta, err := h.store.Symlink(r.Context(), req.Parent, req.Name, req.Target)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusCreated)
	writeJSON(w, meta)
}

func (h *opsHandlers) handleReadlink(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var id metadata.InodeID
	fmt.Sscanf(r.URL.Query().Get("id"), "%d", &id)
	target, err := h.store.Readlink(r.Context(), id)
	if err != nil {
		writeJSONError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, map[string]string{"target": target})
}

func (h *opsHandlers) handleLink(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req struct {
		Parent metadata.InodeID `json:"parent"`
		Name   string           `json:"name"`
		Target metadata.InodeID `json:"target"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	meta, err := h.store.Link(r.Context(), req.Parent, req.Name, req.Target)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusCreated)
	writeJSON(w, meta)
}

// --- Per-inode operations ---

func (h *opsHandlers) handleInodesByID(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path[len("/api/v1/inodes/"):]
	var inodeID metadata.InodeID
	if _, err := fmt.Sscanf(path, "%d", &inodeID); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid inode ID")
		return
	}
	switch r.Method {
	case http.MethodGet:
		meta, err := h.store.GetInode(r.Context(), inodeID)
		if err != nil {
			writeJSONError(w, http.StatusNotFound, err.Error())
			return
		}
		writeJSON(w, meta)
	case http.MethodPut:
		var meta metadata.InodeMeta
		if err := json.NewDecoder(r.Body).Decode(&meta); err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid request body")
			return
		}
		meta.ID = inodeID
		if err := h.store.UpdateInode(r.Context(), &meta); err != nil {
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, map[string]string{"status": "updated"})
	default:
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}
