package main

import (
	"net/http"
	"strconv"
	"time"

	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// handleAudit returns audit records within a time range.
// Query params: start (unix seconds), end (unix seconds), limit (default 1000)
func (h *opsHandlers) handleAudit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	if h.bundle.Audit == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "audit logger not enabled")
		return
	}

	// Parse query params
	q := r.URL.Query()
	startTs := int64(0)
	endTs := time.Now().UnixNano()
	limit := 1000

	if s := q.Get("start"); s != "" {
		if v, err := strconv.ParseInt(s, 10, 64); err == nil {
			startTs = v * int64(time.Second) // accept unix seconds
		}
	}
	if e := q.Get("end"); e != "" {
		if v, err := strconv.ParseInt(e, 10, 64); err == nil {
			endTs = v * int64(time.Second)
		}
	}
	if l := q.Get("limit"); l != "" {
		if v, err := strconv.Atoi(l); err == nil && v > 0 {
			limit = v
		}
	}

	records, err := h.bundle.Audit.QueryAudit(r.Context(), startTs, endTs, limit)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if records == nil {
		records = []metadata.AuditRecord{}
	}
	writeJSON(w, records)
}
