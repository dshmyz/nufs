package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/example/dfs/metadata"
)

func (h *opsHandlers) handleScrub(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	start := time.Now()
	var scanned, healthy, unhealthy int

	err := h.store.ScrubAllChunks(func(chunkID metadata.ChunkID, replicaCount, healthyCount int) {
		scanned++
		if healthyCount == 0 {
			unhealthy++
		} else {
			healthy++
		}
	})
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, fmt.Sprintf("scrub failed: %v", err))
		return
	}

	result := map[string]interface{}{
		"scanned":   scanned,
		"healthy":   healthy,
		"unhealthy": unhealthy,
		"duration":  time.Since(start).Round(time.Millisecond).String(),
		"timestamp": time.Now().UTC(),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}
