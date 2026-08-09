package s3

import (
	"context"
	"log"
	"time"

	"github.com/dshmyz/nufs/nufs-core/metadata"
)

const (
	ObjectWriteRecoveryPeriodicTaskID = "object-write-recovery-periodic"
	ObjectWriteGCPeriodicTaskID       = "object-write-gc-periodic"
)

type ObjectWriteBackgroundWorkerConfig struct {
	Enabled        bool
	Interval       time.Duration
	Lease          time.Duration
	RecoveryLimit  int
	GCLimit        int
	GCAbandonAge   time.Duration
	GCInitialDelay time.Duration
	RecoveryOwner  string
	GCWorkerOwner  string
}

func (cfg ObjectWriteBackgroundWorkerConfig) withDefaults() ObjectWriteBackgroundWorkerConfig {
	if cfg.Interval <= 0 {
		cfg.Interval = time.Minute
	}
	if cfg.Lease <= 0 {
		cfg.Lease = 30 * time.Second
	}
	if cfg.RecoveryLimit <= 0 {
		cfg.RecoveryLimit = 100
	}
	if cfg.GCLimit <= 0 {
		cfg.GCLimit = 100
	}
	if cfg.GCAbandonAge <= 0 {
		cfg.GCAbandonAge = time.Hour
	}
	if cfg.RecoveryOwner == "" {
		cfg.RecoveryOwner = "s3gw-write-recovery"
	}
	if cfg.GCWorkerOwner == "" {
		cfg.GCWorkerOwner = "s3gw-write-gc"
	}
	return cfg
}

func (gw *Gateway) startObjectWriteBackgroundWorkers(ctx context.Context) {
	cfg := gw.backgroundWorkers
	if !cfg.Enabled {
		return
	}
	cfg = cfg.withDefaults()
	go gw.runObjectWriteRecoveryLoop(ctx, cfg)
	go gw.runObjectWriteGCLoop(ctx, cfg)
}

func (gw *Gateway) runObjectWriteRecoveryLoop(ctx context.Context, cfg ObjectWriteBackgroundWorkerConfig) {
	worker := NewObjectWriteRecoveryWorker(gw.meta)
	runObjectWriteWorkerTick(ctx, cfg.Interval, func(tickCtx context.Context) {
		if err := gw.ensureObjectWriteBackgroundTask(tickCtx, NewBackgroundObjectWriteRecoveryTask(ObjectWriteRecoveryPeriodicTaskID, time.Now())); err != nil {
			log.Printf("s3gw: ensure object write recovery task: %v", err)
			return
		}
		if _, err := worker.RunBackgroundTaskOnce(tickCtx, cfg.RecoveryOwner, cfg.Lease, cfg.RecoveryLimit); err != nil {
			log.Printf("s3gw: object write recovery worker: %v", err)
		}
	})
}

func (gw *Gateway) runObjectWriteGCLoop(ctx context.Context, cfg ObjectWriteBackgroundWorkerConfig) {
	worker := NewObjectWriteGCWorker(gw.meta)
	run := func(tickCtx context.Context) {
		if err := gw.ensureObjectWriteBackgroundTask(tickCtx, NewBackgroundObjectWriteGCTask(ObjectWriteGCPeriodicTaskID, time.Now())); err != nil {
			log.Printf("s3gw: ensure object write gc task: %v", err)
			return
		}
		_, err := worker.RunBackgroundTaskOnce(tickCtx, cfg.GCWorkerOwner, cfg.Lease, ObjectWriteGCSweepOptions{
			Limit:      cfg.GCLimit,
			AbandonAge: cfg.GCAbandonAge,
		})
		if err != nil {
			log.Printf("s3gw: object write gc worker: %v", err)
		}
	}
	if cfg.GCInitialDelay > 0 {
		timer := time.NewTimer(cfg.GCInitialDelay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
			run(ctx)
		}
	}
	runObjectWriteWorkerTick(ctx, cfg.Interval, run)
}

func (gw *Gateway) ensureObjectWriteBackgroundTask(ctx context.Context, task metadata.BackgroundTask) error {
	meta, ok := gw.meta.(metadata.BackgroundTaskService)
	if !ok {
		return nil
	}
	existing, err := meta.GetBackgroundTask(ctx, task.ID)
	if err == nil {
		switch existing.State {
		case metadata.TaskQueued, metadata.TaskLeased, metadata.TaskRunning:
			return nil
		}
	}
	if err != nil && err != metadata.ErrEntryNotFound {
		return err
	}
	return meta.PutBackgroundTask(ctx, &task)
}

func runObjectWriteWorkerTick(ctx context.Context, interval time.Duration, fn func(context.Context)) {
	fn(ctx)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			fn(ctx)
		}
	}
}
