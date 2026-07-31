package metadata

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"
)

type BackgroundTaskType string
type BackgroundTaskState string

const (
	TaskRepair        BackgroundTaskType = "repair"
	TaskGC            BackgroundTaskType = "gc"
	TaskScrub         BackgroundTaskType = "scrub"
	TaskRebalance     BackgroundTaskType = "rebalance"
	TaskWriteRecovery BackgroundTaskType = "write_recovery"
	TaskWriteGC       BackgroundTaskType = "write_gc"

	TaskQueued     BackgroundTaskState = "queued"
	TaskLeased     BackgroundTaskState = "leased"
	TaskRunning    BackgroundTaskState = "running"
	TaskSucceeded  BackgroundTaskState = "succeeded"
	TaskRetrying   BackgroundTaskState = "retrying"
	TaskDeadLetter BackgroundTaskState = "dead_letter"
	TaskCanceled   BackgroundTaskState = "canceled"
)

type BackgroundTask struct {
	ID             string
	Type           BackgroundTaskType
	State          BackgroundTaskState
	Target         string
	IdempotencyKey string
	LeaseOwner     string
	AttemptCount   int
	NextRunAt      int64
	LastError      string
	CreatedAt      int64
	UpdatedAt      int64
}

func backgroundTaskKey(id string) string {
	return prefixBackgroundTask + id
}

func backgroundTaskQueueKey(taskType BackgroundTaskType, state BackgroundTaskState, nextRunAt int64, id string) string {
	return fmt.Sprintf("%s%s/%s/%020d/%s", prefixBackgroundTaskQ, taskType, state, nextRunAt, id)
}

func backgroundTaskQueuePrefix(taskType BackgroundTaskType, state BackgroundTaskState) string {
	return fmt.Sprintf("%s%s/%s/", prefixBackgroundTaskQ, taskType, state)
}

func (s *PebbleStore) PutBackgroundTask(_ context.Context, task *BackgroundTask) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}
	if task == nil || task.ID == "" || task.Type == "" || task.State == "" {
		return ErrInvalidArgument
	}
	now := time.Now().UnixNano()
	if task.CreatedAt == 0 {
		task.CreatedAt = now
	}
	task.UpdatedAt = now

	var deletes []string
	if existing, err := s.GetBackgroundTask(context.Background(), task.ID); err == nil {
		deletes = append(deletes, backgroundTaskQueueKey(existing.Type, existing.State, existing.NextRunAt, existing.ID))
	} else if err != ErrEntryNotFound {
		return err
	}

	return s.applyBatchMsgpack([]batchOp{
		{Key: backgroundTaskKey(task.ID), Value: task},
		{Key: backgroundTaskQueueKey(task.Type, task.State, task.NextRunAt, task.ID), Value: task.ID},
	}, deletes)
}

func (s *PebbleStore) GetBackgroundTask(_ context.Context, id string) (*BackgroundTask, error) {
	if s.closed.Load() {
		return nil, ErrServiceClosed
	}
	var task BackgroundTask
	exists, err := s.getJSON(backgroundTaskKey(id), &task)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, ErrEntryNotFound
	}
	return &task, nil
}

func (s *PebbleStore) LeaseBackgroundTask(ctx context.Context, taskType BackgroundTaskType, owner string, lease time.Duration) (*BackgroundTask, error) {
	if s.closed.Load() {
		return nil, ErrServiceClosed
	}
	if owner == "" {
		return nil, ErrInvalidArgument
	}

	var leased *BackgroundTask
	now := time.Now().UnixNano()
	err := s.scanPrefix(backgroundTaskQueuePrefix(taskType, TaskQueued), func(key, value []byte) error {
		if leased != nil {
			return nil
		}
		nextRunAt, id, err := backgroundTaskFromQueueKey(string(key))
		if err != nil || nextRunAt > now {
			return nil
		}
		task, err := s.GetBackgroundTask(ctx, id)
		if err != nil {
			return nil
		}
		task.State = TaskLeased
		task.LeaseOwner = owner
		task.NextRunAt = now + int64(lease)
		if err := s.PutBackgroundTask(ctx, task); err != nil {
			return err
		}
		leased = task
		return nil
	})
	if err != nil {
		return nil, err
	}
	if leased == nil {
		return nil, ErrEntryNotFound
	}
	return leased, nil
}

func (s *PebbleStore) CompleteBackgroundTask(ctx context.Context, id string) error {
	task, err := s.GetBackgroundTask(ctx, id)
	if err != nil {
		return err
	}
	task.State = TaskSucceeded
	task.LeaseOwner = ""
	return s.PutBackgroundTask(ctx, task)
}

func (s *PebbleStore) FailBackgroundTask(ctx context.Context, id string, lastErr string, maxAttempts int) error {
	task, err := s.GetBackgroundTask(ctx, id)
	if err != nil {
		return err
	}
	task.AttemptCount++
	task.LastError = lastErr
	task.LeaseOwner = ""
	if maxAttempts <= 0 {
		maxAttempts = 3
	}
	if task.AttemptCount >= maxAttempts {
		task.State = TaskDeadLetter
	} else {
		task.State = TaskRetrying
		task.NextRunAt = time.Now().Add(time.Second * time.Duration(task.AttemptCount)).UnixNano()
	}
	return s.PutBackgroundTask(ctx, task)
}

func backgroundTaskFromQueueKey(key string) (int64, string, error) {
	parts := strings.SplitN(strings.TrimPrefix(key, prefixBackgroundTaskQ), "/", 4)
	if len(parts) != 4 {
		return 0, "", ErrInvalidArgument
	}
	nextRunAt, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return 0, "", err
	}
	return nextRunAt, parts[3], nil
}
