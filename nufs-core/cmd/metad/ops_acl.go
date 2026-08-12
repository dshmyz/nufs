package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// --- ACL (Bucket Policy) handlers ---

func (h *opsHandlers) handleACL(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, "/api/v1/acl/")
	if name == "" {
		writeJSONError(w, http.StatusBadRequest, "bucket name required")
		return
	}

	switch r.Method {
	case http.MethodGet:
		policy, err := h.dataStore.GetBucketPolicy(r.Context(), name)
		if err != nil {
			if errors.Is(err, metadata.ErrAccessDenied) {
				// No policy set → return empty default-deny policy.
				writeJSON(w, metadata.BucketPolicy{
					Bucket:        name,
					DefaultAccess: "deny",
				})
				return
			}
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, policy)

	case http.MethodPut:
		var policy metadata.BucketPolicy
		if err := json.NewDecoder(r.Body).Decode(&policy); err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid request body")
			return
		}
		policy.Bucket = name
		if err := h.dataStore.SetBucketPolicy(r.Context(), name, policy); err != nil {
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, map[string]string{"status": "updated"})

	case http.MethodDelete:
		if err := h.dataStore.DeleteBucketPolicy(r.Context(), name); err != nil {
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, map[string]string{"status": "deleted"})

	default:
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}
