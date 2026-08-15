package datanode

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// CrossZoneTransferer implements metadata.DataTransferer for cross-zone replication.
// It reuses the existing Replicator to transfer chunk data between datanodes.
type CrossZoneTransferer struct {
	localAddr  string
	replicator *Replicator
}

// NewCrossZoneTransferer creates a new CrossZoneTransferer.
func NewCrossZoneTransferer(localAddr string, replicator *Replicator) *CrossZoneTransferer {
	return &CrossZoneTransferer{
		localAddr:  localAddr,
		replicator: replicator,
	}
}

// TransferChunk implements metadata.DataTransferer.
// It triggers replication of a chunk from the local node to a remote datanode.
func (t *CrossZoneTransferer) TransferChunk(ctx context.Context, chunkID metadata.ChunkID, remoteAddr string) error {
	if t.replicator == nil {
		return fmt.Errorf("cross-zone transfer: replicator not configured")
	}

	slog.Debug("cross-zone: initiating chunk transfer",
		"chunkID", chunkID,
		"source", t.localAddr,
		"target", remoteAddr)

	// Create a replication task from local node to remote node.
	// The Replicator handles connection pooling, retries, and throttling.
	task := ReplicationTask{
		ChunkID:    chunkID,
		SourceAddr: t.localAddr,
		TargetAddr: remoteAddr,
		CreatedAt:  time.Now(),
	}

	// Use SubmitWait to block until replication completes or times out.
	// Cross-zone replication has higher latency, so use a longer timeout.
	const crossZoneTimeout = 5 * time.Minute
	if err := t.replicator.SubmitWait(task, crossZoneTimeout); err != nil {
		return fmt.Errorf("cross-zone transfer: replication failed: %w", err)
	}

	slog.Info("cross-zone: chunk transferred successfully",
		"chunkID", chunkID,
		"source", t.localAddr,
		"target", remoteAddr)

	return nil
}
