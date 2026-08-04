package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/example/dfs/metadata"
)

// retryAfterSeconds is the Retry-After value set on 429 responses.
// We use a small integer (seconds, RFC 7231) so clients can do a
// simple time.Sleep before retrying.
const retryAfterSeconds = 2

// --- Node handlers ---

func (h *opsHandlers) handleNodes(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		nodes, err := h.store.ListNodes(r.Context())
		if err != nil {
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, nodes)
	case http.MethodPost:
		var info metadata.NodeInfo
		if err := json.NewDecoder(r.Body).Decode(&info); err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid request body")
			return
		}
		if err := h.store.RegisterNode(r.Context(), &info); err != nil {
			if errors.Is(err, metadata.ErrNodeAlreadyExists) {
				writeJSONErrorC(w, http.StatusConflict, "node_already_registered", err.Error())
			} else if errors.Is(err, metadata.ErrTooManyRequests) {
				w.Header().Set("Retry-After", fmt.Sprintf("%d", retryAfterSeconds))
				writeJSONErrorC(w, http.StatusTooManyRequests, "too_many_requests", err.Error())
			} else {
				writeJSONError(w, http.StatusInternalServerError, err.Error())
			}
			return
		}
		w.WriteHeader(http.StatusCreated)
		writeJSON(w, map[string]string{"status": "registered"})
	default:
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (h *opsHandlers) handleNodesByID(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path[len("/api/v1/nodes/"):]
	var nodeID metadata.NodeID
	if _, err := fmt.Sscanf(path, "%d", &nodeID); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid node ID")
		return
	}

	// Check for sub-paths
	rest := path
	var sub string
	if _, err := fmt.Sscanf(path, "%d/%s", &nodeID, &sub); err == nil {
		rest = sub
	} else {
		rest = ""
	}

	switch {
	case rest == "":
		switch r.Method {
		case http.MethodGet:
			node, err := h.store.GetNode(r.Context(), nodeID)
			if err != nil {
				writeJSONError(w, http.StatusNotFound, err.Error())
				return
			}
			writeJSON(w, node)
		default:
			writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	case rest == "heartbeat":
		if r.Method != http.MethodPost {
			writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		var report metadata.NodeReport
		if err := json.NewDecoder(r.Body).Decode(&report); err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid request body")
			return
		}
		if err := h.store.Heartbeat(r.Context(), nodeID, &report); err != nil {
			if errors.Is(err, metadata.ErrTooManyRequests) {
				w.Header().Set("Retry-After", fmt.Sprintf("%d", retryAfterSeconds))
				writeJSONErrorC(w, http.StatusTooManyRequests, "too_many_requests", err.Error())
				return
			}
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, map[string]string{"status": "ok"})
	case rest == "change-ack":
		if r.Method != http.MethodPost {
			writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		var req struct {
			Seq uint64 `json:"seq"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid request body")
			return
		}
		ack, err := h.store.AckChangeEvents(r.Context(), nodeID, req.Seq)
		if err != nil {
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, map[string]uint64{"change_ack": ack})
	case rest == "decommission":
		if r.Method != http.MethodPost {
			writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		if err := h.store.DecommissionNode(r.Context(), nodeID); err != nil {
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, map[string]string{"status": "decommissioned"})
	case rest == "chunks":
		if r.Method != http.MethodGet {
			writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		chunks, err := h.store.ChunksByNode(r.Context(), nodeID)
		if err != nil {
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, chunks)
	default:
		writeJSONError(w, http.StatusNotFound, "unknown node sub-path")
	}
}
