package datanode

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// ecConvertTaskMeta is the subset of the metadata authority the conversion
// worker needs to drive a task through its lifecycle. It is satisfied by
// *metadata.PebbleStore (in-process/S1) and *metadata.HTTPClient (S2), the
// same dual implementation the repair worker's unifiedRepairTaskMeta follows.
type ecConvertTaskMeta interface {
	LeaseBackgroundTaskForNode(context.Context, metadata.BackgroundTaskType, uint64, string, time.Duration) (*metadata.BackgroundTask, error)
	CompleteBackgroundTask(context.Context, string) error
	FailBackgroundTask(context.Context, string, string, int) error
}

// ConversionWorker is the datanode-side consumer of the metad EC conversion
// scheduler's TaskECConvert background tasks (§14): it polls for a conversion
// task this node is allowed to own (its chunk replica lives on this node —
// ConvertReplica's source read must be LOCAL), runs the 5-step replication→EC
// transaction via ECService.ConvertToEC, and completes or fails the task.
//
// It mirrors the RepairWorker lifecycle (Start/Stop/Stats + ticker-driven
// scanLoop) and processes one task per tick, matching the repair worker.
type ConversionWorker struct {
	meta     ecConvertTaskMeta
	ec       *ECService
	nodeID   uint64
	interval time.Duration

	mu      sync.Mutex
	running bool
	stopCh  chan struct{}
	wg      sync.WaitGroup

	converted int64
	failed    int64
}

// NewConversionWorker creates a conversion worker over the metadata authority
// and the node's ECService. interval defaults to 30s when zero.
func NewConversionWorker(meta ecConvertTaskMeta, ec *ECService, nodeID uint64, interval time.Duration) *ConversionWorker {
	if interval == 0 {
		interval = 30 * time.Second
	}
	return &ConversionWorker{
		meta:     meta,
		ec:       ec,
		nodeID:   nodeID,
		interval: interval,
		stopCh:   make(chan struct{}),
	}
}

// Start launches the background scan loop.
func (w *ConversionWorker) Start(ctx context.Context) {
	w.mu.Lock()
	if w.running {
		w.mu.Unlock()
		return
	}
	w.running = true
	w.mu.Unlock()

	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		w.scanLoop(ctx)
	}()
	slog.Info("ec-convert: worker started", "nodeID", w.nodeID, "interval", w.interval)
}

// Stop halts the scan loop and waits for it to exit.
func (w *ConversionWorker) Stop() {
	w.mu.Lock()
	if w.running {
		w.running = false
		close(w.stopCh)
	}
	w.mu.Unlock()
	w.wg.Wait()
}

// Stats returns (converted, failed) task counts.
func (w *ConversionWorker) Stats() (converted, failed int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.converted, w.failed
}

func (w *ConversionWorker) scanLoop(ctx context.Context) {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-w.stopCh:
			return
		case <-ticker.C:
			w.processConversionQueue(ctx)
		}
	}
}

// processConversionQueue leases one TaskECConvert task this node is allowed
// to own and converts it. A lease miss (no eligible task) is a no-op.
func (w *ConversionWorker) processConversionQueue(ctx context.Context) {
	task, err := w.meta.LeaseBackgroundTaskForNode(ctx, metadata.TaskECConvert, w.nodeID, w.owner(), w.interval)
	if err != nil {
		if errors.Is(err, metadata.ErrEntryNotFound) {
			return
		}
		slog.Error("ec-convert: lease failed", "nodeID", w.nodeID, "error", err)
		return
	}

	extentID, err := conversionExtentFromTask(task)
	if err != nil {
		w.fail(ctx, task, err)
		return
	}

	st, err := w.ec.ConvertToEC(ctx, metadata.ChunkID(extentID), 0)
	if err != nil {
		w.fail(ctx, task, fmt.Errorf("ec-convert: convert chunk %d: %w", extentID, err))
		return
	}

	if err := w.meta.CompleteBackgroundTask(ctx, task.ID); err != nil {
		// Conversion itself landed (the §14 transaction is durable on the
		// authority); only the task bookkeeping failed. The lease expires and
		// the task becomes re-leasable — a re-run is idempotent from the
		// authority's side. Log loudly rather than swallowing.
		slog.Error("ec-convert: complete failed", "task", task.ID, "stripe", st.StripeID, "error", err)
		return
	}

	w.mu.Lock()
	w.converted++
	w.mu.Unlock()
	slog.Info("ec-convert: converted", "task", task.ID, "extent", extentID, "stripe", st.StripeID)
}

func (w *ConversionWorker) fail(ctx context.Context, task *metadata.BackgroundTask, cause error) {
	w.mu.Lock()
	w.failed++
	w.mu.Unlock()
	slog.Error("ec-convert: conversion failed", "task", task.ID, "error", cause)
	if err := w.meta.FailBackgroundTask(ctx, task.ID, cause.Error(), 3); err != nil {
		slog.Error("ec-convert: fail-task error", "task", task.ID, "error", err)
	}
}

func (w *ConversionWorker) owner() string {
	return fmt.Sprintf("ec-convert-worker-%d", w.nodeID)
}

// conversionExtentFromTask parses the extent ID from a conversion task. The
// scheduler's task ID and Target are both "ec-convert-{extentID}" (§14;
// extent ID == chunk ID in the V2 inline model).
func conversionExtentFromTask(task *metadata.BackgroundTask) (uint64, error) {
	const prefix = "ec-convert-"
	if task == nil {
		return 0, fmt.Errorf("ec-convert: nil task")
	}
	if !strings.HasPrefix(task.Target, prefix) {
		return 0, fmt.Errorf("ec-convert: unexpected task target %q", task.Target)
	}
	id, err := strconv.ParseUint(strings.TrimPrefix(task.Target, prefix), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("ec-convert: parse target %q: %w", task.Target, err)
	}
	return id, nil
}
