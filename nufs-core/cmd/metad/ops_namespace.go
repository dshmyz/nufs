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
	if len(parts) >= 2 {
		// Sub-resources under /api/v1/inodes/{id}/: xattrs (V1) and the
		// V2.1 extent-layout surface (extents/inline/promote/append-extent).
		switch parts[1] {
		case "xattrs":
			h.handleXAttrs(w, r, inodeID, parts)
			return
		case "extents":
			h.handleInodeExtents(w, r, inodeID)
			return
		case "inline":
			h.handleInodeInline(w, r, inodeID)
			return
		case "promote":
			h.handleInodePromote(w, r, inodeID)
			return
		case "append-extent":
			h.handleInodeAppendExtent(w, r, inodeID)
			return
		case "replace-extents":
			h.handleInodeReplaceExtents(w, r, inodeID)
			return
		}
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

// --- V2.1 extent-layout inode surface (roadmap stage 1 §1.3) ---

// extentInodeService resolves the V2 extent-inode surface from the data
// plane store (the primary PebbleStore for --shards 1, a ShardedStore for
// --shards N>1). Both implement metadata.ExtentInodeService.
func (h *opsHandlers) extentInodeService(w http.ResponseWriter) (metadata.ExtentInodeService, bool) {
	es, ok := h.dataStore.(metadata.ExtentInodeService)
	if !ok {
		writeJSONError(w, http.StatusNotImplemented, "extent inode surface not configured")
		return nil, false
	}
	return es, true
}

// handleInodeExtents serves GET /api/v1/inodes/{id}/extents (ResolveExtents).
func (h *opsHandlers) handleInodeExtents(w http.ResponseWriter, r *http.Request, inodeID metadata.InodeID) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	es, ok := h.extentInodeService(w)
	if !ok {
		return
	}
	refs, err := es.ResolveExtents(r.Context(), inodeID)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if refs == nil {
		refs = []metadata.ExtentRef{}
	}
	writeJSON(w, map[string]interface{}{"extents": refs})
}

// handleInodeInline serves PUT /api/v1/inodes/{id}/inline (SetInlineExtent).
func (h *opsHandlers) handleInodeInline(w http.ResponseWriter, r *http.Request, inodeID metadata.InodeID) {
	if r.Method != http.MethodPut {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if !h.requireLeader(w, r) {
		return
	}
	var body struct {
		Extent *metadata.ExtentMetaV2 `json:"extent"`
		Size   int64                  `json:"size"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Extent == nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	es, ok := h.extentInodeService(w)
	if !ok {
		return
	}
	if err := es.SetInlineExtent(r.Context(), inodeID, body.Extent, body.Size); err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, map[string]string{"status": "updated"})
}

// handleInodePromote serves PUT /api/v1/inodes/{id}/promote (PromoteToPages).
func (h *opsHandlers) handleInodePromote(w http.ResponseWriter, r *http.Request, inodeID metadata.InodeID) {
	if r.Method != http.MethodPut {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if !h.requireLeader(w, r) {
		return
	}
	es, ok := h.extentInodeService(w)
	if !ok {
		return
	}
	if err := es.PromoteToPages(r.Context(), inodeID); err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, map[string]string{"status": "updated"})
}

// handleInodeAppendExtent serves PUT /api/v1/inodes/{id}/append-extent
// (AppendExtent).
func (h *opsHandlers) handleInodeAppendExtent(w http.ResponseWriter, r *http.Request, inodeID metadata.InodeID) {
	if r.Method != http.MethodPut {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if !h.requireLeader(w, r) {
		return
	}
	var body struct {
		Extent *metadata.ExtentMetaV2 `json:"extent"`
		Offset int64                  `json:"offset"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Extent == nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	es, ok := h.extentInodeService(w)
	if !ok {
		return
	}
	root, err := es.AppendExtent(r.Context(), inodeID, body.Extent, body.Offset)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, map[string]interface{}{"extent_root": root})
}

// handleInodeReplaceExtents serves PUT /api/v1/inodes/{id}/replace-extents
// (ReplaceExtents). The endpoint is a distinct sub-path because the mux
// dispatches by path segment, and "extents" is already claimed by
// ResolveExtents (GET).
func (h *opsHandlers) handleInodeReplaceExtents(w http.ResponseWriter, r *http.Request, inodeID metadata.InodeID) {
	if r.Method != http.MethodPut {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if !h.requireLeader(w, r) {
		return
	}
	var body struct {
		Writes []metadata.ExtentWrite `json:"writes"`
		Size   int64                  `json:"size"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	es, ok := h.extentInodeService(w)
	if !ok {
		return
	}
	if err := es.ReplaceExtents(r.Context(), inodeID, body.Writes, body.Size); err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, map[string]string{"status": "updated"})
}

// handleExtentByID serves GET /api/v1/extents/{id} (GetExtentMeta).
func (h *opsHandlers) handleExtentByID(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	path := r.URL.Path[len("/api/v1/extents/"):]
	rawID, err := strconv.ParseUint(path, 10, 64)
	if err != nil || rawID == 0 {
		writeJSONError(w, http.StatusBadRequest, "invalid extent ID")
		return
	}
	es, ok := h.extentInodeService(w)
	if !ok {
		return
	}
	m, err := es.GetExtentMeta(r.Context(), metadata.ExtentIDV2(rawID))
	if errors.Is(err, metadata.ErrExtentNotFound) {
		// Machine-readable code so HTTPClient.GetExtentMeta maps the 404 back
		// to ErrExtentNotFound (readResponse matches on code=="extent_not_found").
		writeJSONErrorC(w, http.StatusNotFound, "extent_not_found", err.Error())
		return
	}
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, m)
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
