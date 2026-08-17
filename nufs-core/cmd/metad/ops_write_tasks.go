package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/dshmyz/nufs/nufs-core/metadata"
)

func (h *opsHandlers) handleWriteAttempts(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	state := metadata.WriteAttemptState(r.URL.Query().Get("state"))
	if state == "" {
		writeJSONError(w, http.StatusBadRequest, "state required")
		return
	}
	limit := queryInt(r, "limit", 100)
	attempts, err := h.store.ListWriteAttemptsByState(r.Context(), state, limit)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, attempts)
}

func (h *opsHandlers) handleWriteOpsStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	writeJSON(w, h.writeOpsStatus(r.Context()))
}

func (h *opsHandlers) writeOpsStatus(ctx context.Context) metadata.WriteOpsStatus {
	states := []metadata.WriteAttemptState{
		metadata.WriteAttemptPending,
		metadata.WriteAttemptChunksAllocated,
		metadata.WriteAttemptChunksDurable,
		metadata.WriteAttemptCommitted,
		metadata.WriteAttemptFailed,
		metadata.WriteAttemptRecoveryNeeded,
	}
	counts := make(map[string]int64, len(states))
	for _, state := range states {
		count, err := h.store.CountWriteAttemptsByState(ctx, state)
		if err != nil {
			count = 0
		}
		counts[string(state)] = count
	}

	recoveryTask, _ := h.store.GetBackgroundTask(ctx, "object-write-recovery-periodic")
	gcTask, _ := h.store.GetBackgroundTask(ctx, "object-write-gc-periodic")
	return metadata.WriteOpsStatus{
		Attempts:     counts,
		RecoveryTask: metadata.NewBackgroundTaskStatus(recoveryTask),
		GCTask:       metadata.NewBackgroundTaskStatus(gcTask),
	}
}

func (h *opsHandlers) handleWriteAttemptByID(w http.ResponseWriter, r *http.Request) {
	id, err := url.PathUnescape(strings.TrimPrefix(r.URL.Path, "/api/v1/write-attempts/"))
	if err != nil || id == "" {
		writeJSONError(w, http.StatusBadRequest, "invalid write attempt id")
		return
	}
	switch r.Method {
	case http.MethodPut:
		var attempt metadata.ObjectWriteAttempt
		if err := json.NewDecoder(r.Body).Decode(&attempt); err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid request body")
			return
		}
		attempt.ID = id
		if err := h.store.PutWriteAttempt(r.Context(), &attempt); err != nil {
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, map[string]string{"status": "updated"})
	case http.MethodGet:
		attempt, err := h.store.GetWriteAttempt(r.Context(), id)
		if err != nil {
			writeJSONErrorC(w, http.StatusNotFound, "entry_not_found", err.Error())
			return
		}
		writeJSON(w, attempt)
	case http.MethodDelete:
		if err := h.store.DeleteWriteAttempt(r.Context(), id); err != nil {
			writeJSONErrorC(w, http.StatusNotFound, "entry_not_found", err.Error())
			return
		}
		writeJSON(w, map[string]string{"status": "deleted"})
	default:
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (h *opsHandlers) handleLeaseBackgroundTask(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req struct {
		Type       metadata.BackgroundTaskType `json:"type"`
		Owner      string                      `json:"owner"`
		LeaseNanos int64                       `json:"lease_nanos"`
		// NodeID, when set, restricts the lease to tasks whose OwnerNodes
		// include this datanode (the conversion worker's owner-routed lease).
		NodeID *uint64 `json:"node_id,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	var (
		task *metadata.BackgroundTask
		err  error
	)
	if req.NodeID != nil {
		task, err = h.store.LeaseBackgroundTaskForNode(r.Context(), req.Type, *req.NodeID, req.Owner, time.Duration(req.LeaseNanos))
	} else {
		task, err = h.store.LeaseBackgroundTask(r.Context(), req.Type, req.Owner, time.Duration(req.LeaseNanos))
	}
	if err != nil {
		writeJSONErrorC(w, http.StatusNotFound, "entry_not_found", err.Error())
		return
	}
	writeJSON(w, task)
}

func (h *opsHandlers) handleBackgroundTaskByID(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/background-tasks/")
	parts := strings.SplitN(path, "/", 2)
	id, err := url.PathUnescape(parts[0])
	if err != nil || id == "" {
		writeJSONError(w, http.StatusBadRequest, "invalid background task id")
		return
	}
	action := ""
	if len(parts) == 2 {
		action = parts[1]
	}

	switch {
	case action == "" && r.Method == http.MethodPut:
		var task metadata.BackgroundTask
		if err := json.NewDecoder(r.Body).Decode(&task); err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid request body")
			return
		}
		task.ID = id
		if err := h.store.PutBackgroundTask(r.Context(), &task); err != nil {
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, map[string]string{"status": "updated"})
	case action == "" && r.Method == http.MethodGet:
		task, err := h.store.GetBackgroundTask(r.Context(), id)
		if err != nil {
			writeJSONErrorC(w, http.StatusNotFound, "entry_not_found", err.Error())
			return
		}
		writeJSON(w, task)
	case action == "complete" && r.Method == http.MethodPost:
		if err := h.store.CompleteBackgroundTask(r.Context(), id); err != nil {
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, map[string]string{"status": "completed"})
	case action == "fail" && r.Method == http.MethodPost:
		var req struct {
			LastError   string `json:"last_error"`
			MaxAttempts int    `json:"max_attempts"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid request body")
			return
		}
		if err := h.store.FailBackgroundTask(r.Context(), id, req.LastError, req.MaxAttempts); err != nil {
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, map[string]string{"status": "failed"})
	default:
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func queryInt(r *http.Request, name string, fallback int) int {
	value := r.URL.Query().Get(name)
	if value == "" {
		return fallback
	}
	n, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return n
}
