package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/dshmyz/nufs/nufs-core/metadata"
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
	meta, err := h.dataStore.MkDir(r.Context(), req.Parent, req.Name, req.Mode)
	if err != nil {
		if errors.Is(err, metadata.ErrEntryExists) {
			writeEntryExistsError(w, err)
			return
		}
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
	if err := h.dataStore.RmDir(r.Context(), req.Parent, req.Name); err != nil {
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
	entries, err := h.dataStore.ReadDir(r.Context(), parent, offset, limit)
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
	meta, err := h.dataStore.CreateFile(r.Context(), req.Parent, req.Name, req.Mode)
	if err != nil {
		if errors.Is(err, metadata.ErrEntryExists) {
			writeEntryExistsError(w, err)
			return
		}
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusCreated)
	writeJSON(w, meta)
}

// handleCreateNode creates a special (non-regular) namespace entry —
// FIFO (type 3), char device (4), block device (5) or socket (6). rdev is
// the device number for the char/block device types. Same JSON contract as
// createfile plus the ftype/rdev fields; HTTPClient.CreateNode decodes it.
func (h *opsHandlers) handleCreateNode(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req struct {
		Parent metadata.InodeID  `json:"parent"`
		Name   string            `json:"name"`
		Type   metadata.FileType `json:"type"`
		Mode   uint32            `json:"mode"`
		Rdev   uint32            `json:"rdev"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	meta, err := h.dataStore.CreateNode(r.Context(), req.Parent, req.Name, req.Type, req.Mode, req.Rdev)
	if err != nil {
		if errors.Is(err, metadata.ErrEntryExists) {
			writeEntryExistsError(w, err)
			return
		}
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
	if err := h.dataStore.Unlink(r.Context(), req.Parent, req.Name); err != nil {
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
	meta, err := h.dataStore.Lookup(r.Context(), parent, name)
	if err != nil {
		// Use writeJSONErrorC so the HTTP client can map the 404 to
		// ErrEntryNotFound via the machine-readable code (readResponse
		// matches on code=="entry_not_found"). Without the code field,
		// the client returns a generic error and callers cannot
		// distinguish "key absent" from an unexpected failure — which
		// breaks the S3 PUT new-object path (committer_put.go Lookup).
		if errors.Is(err, metadata.ErrEntryNotFound) {
			writeJSONErrorC(w, http.StatusNotFound, "entry_not_found", err.Error())
			return
		}
		if errors.Is(err, metadata.ErrInodeNotFound) {
			writeJSONErrorC(w, http.StatusNotFound, "inode_not_found", err.Error())
			return
		}
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
	if err := h.dataStore.Rename(r.Context(), req.OldParent, req.OldName, req.NewParent, req.NewName); err != nil {
		if errors.Is(err, metadata.ErrEntryExists) {
			writeEntryExistsError(w, err)
			return
		}
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
	meta, err := h.dataStore.Symlink(r.Context(), req.Parent, req.Name, req.Target)
	if err != nil {
		if errors.Is(err, metadata.ErrEntryExists) {
			writeEntryExistsError(w, err)
			return
		}
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
	target, err := h.dataStore.Readlink(r.Context(), id)
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
	meta, err := h.dataStore.Link(r.Context(), req.Parent, req.Name, req.Target)
	if err != nil {
		if errors.Is(err, metadata.ErrEntryExists) {
			writeEntryExistsError(w, err)
			return
		}
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusCreated)
	writeJSON(w, meta)
}

// --- Per-inode operations ---

func (h *opsHandlers) handleInodesByID(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path[len("/api/v1/inodes/"):]
	parts := strings.SplitN(path, "/", 3)
	rawID, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil || rawID == 0 {
		writeJSONError(w, http.StatusBadRequest, "invalid inode ID")
		return
	}
	inodeID := metadata.InodeID(rawID)
	if len(parts) >= 2 && parts[1] == "xattrs" {
		h.handleXAttrs(w, r, inodeID, parts)
		return
	}
	switch r.Method {
	case http.MethodGet:
		meta, err := h.dataStore.GetInode(r.Context(), inodeID)
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
		if err := h.dataStore.UpdateInode(r.Context(), &meta); err != nil {
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, map[string]string{"status": "updated"})
	default:
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (h *opsHandlers) handleXAttrs(w http.ResponseWriter, r *http.Request, inodeID metadata.InodeID, parts []string) {
	if r.Method == http.MethodPut || r.Method == http.MethodDelete {
		if !h.requireLeader(w, r) {
			return
		}
	}

	if len(parts) == 2 {
		if r.Method != http.MethodGet {
			writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		attrs, err := h.dataStore.ListXAttr(r.Context(), inodeID)
		if err != nil {
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
		if attrs == nil {
			attrs = map[string][]byte{}
		}
		writeJSON(w, attrs)
		return
	}

	name, err := url.PathUnescape(parts[2])
	if err != nil || name == "" {
		writeJSONError(w, http.StatusBadRequest, "invalid xattr name")
		return
	}

	switch r.Method {
	case http.MethodGet:
		value, err := h.dataStore.GetXAttr(r.Context(), inodeID, name)
		if errors.Is(err, metadata.ErrXAttrNotFound) {
			writeJSONErrorC(w, http.StatusNotFound, "xattr_not_found", err.Error())
			return
		}
		if err != nil {
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, map[string][]byte{"value": value})
	case http.MethodPut:
		var req struct {
			Value []byte `json:"value"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid request body")
			return
		}
		if err := h.dataStore.SetXAttr(r.Context(), inodeID, name, req.Value); err != nil {
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, map[string]string{"status": "updated"})
	case http.MethodDelete:
		if err := h.dataStore.RemoveXAttr(r.Context(), inodeID, name); err != nil {
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, map[string]string{"status": "removed"})
	default:
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}
