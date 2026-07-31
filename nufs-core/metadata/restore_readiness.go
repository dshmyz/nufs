package metadata

import (
	"context"
	"fmt"
)

type RestoreReplicaProbe interface {
	ReachableReplicas(context.Context, *ChunkMeta) (int, error)
}

type RestoreReadinessReport struct {
	Ready             bool                            `json:"ready"`
	MinimumReplicas   int                             `json:"minimum_replicas"`
	TotalChunks       int                             `json:"total_chunks"`
	UnavailableChunks int                             `json:"unavailable_chunks"`
	Issues            []RestoreChunkAvailabilityIssue `json:"issues,omitempty"`
}

type RestoreChunkAvailabilityStatus string

const (
	RestoreChunkMissing    RestoreChunkAvailabilityStatus = "missing"
	RestoreChunkDegraded   RestoreChunkAvailabilityStatus = "degraded"
	RestoreChunkProbeError RestoreChunkAvailabilityStatus = "probe_error"
)

type RestoreChunkAvailabilityIssue struct {
	ChunkID         ChunkID                        `json:"chunk_id"`
	RequiredMinimum int                            `json:"required_minimum"`
	Reachable       int                            `json:"reachable"`
	Status          RestoreChunkAvailabilityStatus `json:"status"`
	Error           string                         `json:"error,omitempty"`
}

func VerifyRestoredChunkAvailability(ctx context.Context, store *PebbleStore, probe RestoreReplicaProbe, minimumReplicas int) (*RestoreReadinessReport, error) {
	if store == nil {
		return nil, fmt.Errorf("restore readiness: store is required")
	}
	if probe == nil {
		return nil, fmt.Errorf("restore readiness: replica probe is required")
	}
	if minimumReplicas <= 0 {
		return nil, fmt.Errorf("restore readiness: minimum replicas must be positive")
	}
	report := &RestoreReadinessReport{MinimumReplicas: minimumReplicas, Ready: true}
	err := store.ScanAllChunks(ctx, func(chunk *ChunkMeta) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		report.TotalChunks++
		reachable, err := probe.ReachableReplicas(ctx, chunk)
		if err != nil {
			report.addIssue(chunk.ID, minimumReplicas, 0, RestoreChunkProbeError, err.Error())
			return nil
		}
		if reachable < minimumReplicas {
			status := RestoreChunkDegraded
			if reachable == 0 {
				status = RestoreChunkMissing
			}
			report.addIssue(chunk.ID, minimumReplicas, reachable, status, "")
		}
		return nil
	})
	if err != nil {
		return report, fmt.Errorf("restore readiness: verify chunks: %w", err)
	}
	if !report.Ready {
		return report, fmt.Errorf("restore readiness: %d chunks below %d reachable replicas", report.UnavailableChunks, minimumReplicas)
	}
	return report, nil
}

func (r *RestoreReadinessReport) addIssue(chunkID ChunkID, required, reachable int, status RestoreChunkAvailabilityStatus, message string) {
	r.Ready = false
	r.UnavailableChunks++
	r.Issues = append(r.Issues, RestoreChunkAvailabilityIssue{
		ChunkID:         chunkID,
		RequiredMinimum: required,
		Reachable:       reachable,
		Status:          status,
		Error:           message,
	})
}
