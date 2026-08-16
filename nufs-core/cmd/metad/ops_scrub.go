package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/dshmyz/nufs/nufs-core/metadata"
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

	// V2 extent counts (roadmap §1.4): read-only Lifecycle + backing-chunk
	// health summary. Recovery is the periodic ExtentScrubber worker's job.
	var extScanned, extReady, extDegraded, extDangling, extUnhealthy int
	err = h.store.ScrubExtents(func(extentID metadata.ExtentIDV2, lifecycle metadata.ExtentLifecycle, healthy, orphan bool) {
		extScanned++
		switch lifecycle {
		case metadata.LifecycleReady:
			extReady++
		case metadata.LifecycleReadyDegraded:
			extDegraded++
		}
		if orphan {
			extDangling++
		} else if !healthy {
			extUnhealthy++
		}
	})
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, fmt.Sprintf("scrub extents failed: %v", err))
		return
	}

	result := map[string]interface{}{
		"scanned":           scanned,
		"healthy":           healthy,
		"unhealthy":         unhealthy,
		"extents_scanned":   extScanned,
		"extents_ready":     extReady,
		"extents_degraded":  extDegraded,
		"extents_dangling":  extDangling,
		"extents_unhealthy": extUnhealthy,
		"duration":          time.Since(start).Round(time.Millisecond).String(),
		"timestamp":         time.Now().UTC(),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}
