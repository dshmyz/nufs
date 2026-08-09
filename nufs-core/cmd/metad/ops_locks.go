package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// --- Advisory lock handlers ---
//
// These three endpoints back the HTTPClient.Advisory* methods so
// remote gateways (nufs-fuse, nufs-s3, nufs-cli) can coordinate with each
// other through a single source of truth — the metad process that
// owns the PebbleStore. The lock state itself is in-memory in
// PebbleStore.advisoryLocks; see metadata/lock.go for the model.

type advisoryAcquireReq struct {
	Inode metadata.InodeID `json:"inode"`
	Owner string           `json:"owner"`
	Mode  string           `json:"mode"` // "exclusive" or "shared"
}

type advisoryReleaseReq struct {
	Inode metadata.InodeID `json:"inode"`
	Owner string           `json:"owner"`
}

func (h *opsHandlers) handleAdvisoryAcquire(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req advisoryAcquireReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	var err error
	switch req.Mode {
	case "exclusive":
		err = h.store.AdvisoryLock(r.Context(), req.Inode, req.Owner)
	case "shared":
		err = h.store.AdvisoryLockShared(r.Context(), req.Inode, req.Owner)
	default:
		writeJSONError(w, http.StatusBadRequest, "mode must be 'exclusive' or 'shared'")
		return
	}
	if err != nil {
		// Translate ErrLockBusy to HTTP 409 so the HTTP client can
		// recognise it with a status check; other errors fall through
		// to a generic 500.
		if errors.Is(err, metadata.ErrLockBusy) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusConflict)
			json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, map[string]string{"status": "acquired"})
}

func (h *opsHandlers) handleAdvisoryRelease(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req advisoryReleaseReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := h.store.AdvisoryUnlock(r.Context(), req.Inode, req.Owner); err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, map[string]string{"status": "released"})
}

func (h *opsHandlers) handleAdvisoryList(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	inodeStr := r.URL.Query().Get("inode")
	var inode metadata.InodeID
	if inodeStr == "" {
		writeJSONError(w, http.StatusBadRequest, "inode query parameter required")
		return
	}
	parsed, err := strconv.ParseUint(inodeStr, 10, 64)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "inode must be a non-negative integer")
		return
	}
	inode = metadata.InodeID(parsed)
	locks, err := h.store.AdvisoryListLocks(r.Context(), inode)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, locks)
}
