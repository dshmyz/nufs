package metadata

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"
)

type WriteAttemptState string

const (
	WriteAttemptPending         WriteAttemptState = "pending"
	WriteAttemptChunksAllocated WriteAttemptState = "chunks_allocated"
	WriteAttemptChunksDurable   WriteAttemptState = "chunks_durable"
	WriteAttemptCommitted       WriteAttemptState = "committed"
	WriteAttemptFailed          WriteAttemptState = "failed"
	WriteAttemptRecoveryNeeded  WriteAttemptState = "recovery_needed"
)

type WriteAttemptRecoveryIntent string

const (
	WriteAttemptRecoveryCommit  WriteAttemptRecoveryIntent = "commit"
	WriteAttemptRecoveryCleanup WriteAttemptRecoveryIntent = "cleanup"
)

type ObjectWriteAttempt struct {
	ID               string
	Bucket           string
	Key              string
	InodeID          InodeID
	InodeCTime       int64
	RecoveryIntent   WriteAttemptRecoveryIntent
	CleanupParent    InodeID
	CleanupNewObject bool
	RollbackInode    *InodeMeta
	Chunks           []ChunkRef
	State            WriteAttemptState
	LastError        string
	CreatedAt        int64
	UpdatedAt        int64
}

func writeAttemptKey(id string) string {
	return prefixWriteAttempt + id
}

func writeAttemptStateKey(state WriteAttemptState, updatedAt int64, id string) string {
	return fmt.Sprintf("%s%s/%020d/%s", prefixWriteAttemptState, state, updatedAt, id)
}

func writeAttemptStatePrefix(state WriteAttemptState) string {
	return fmt.Sprintf("%s%s/", prefixWriteAttemptState, state)
}

func (s *PebbleStore) PutWriteAttempt(_ context.Context, attempt *ObjectWriteAttempt) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}
	if attempt == nil || attempt.ID == "" {
		return ErrInvalidArgument
	}
	if attempt.RecoveryIntent == "" {
		attempt.RecoveryIntent = WriteAttemptRecoveryCommit
	}

	now := time.Now().UnixNano()
	if attempt.CreatedAt == 0 {
		attempt.CreatedAt = now
	}
	attempt.UpdatedAt = now

	var deletes []string
	if existing, err := s.GetWriteAttempt(context.Background(), attempt.ID); err == nil {
		deletes = append(deletes, writeAttemptStateKey(existing.State, existing.UpdatedAt, existing.ID))
	} else if err != ErrEntryNotFound {
		return err
	}

	return s.applyBatchMsgpack([]batchOp{
		{Key: writeAttemptKey(attempt.ID), Value: attempt},
		{Key: writeAttemptStateKey(attempt.State, attempt.UpdatedAt, attempt.ID), Value: attempt.ID},
	}, deletes)
}

func (s *PebbleStore) GetWriteAttempt(_ context.Context, id string) (*ObjectWriteAttempt, error) {
	if s.closed.Load() {
		return nil, ErrServiceClosed
	}
	var attempt ObjectWriteAttempt
	exists, err := s.getValue(writeAttemptKey(id), &attempt)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, ErrEntryNotFound
	}
	if attempt.RecoveryIntent == "" {
		attempt.RecoveryIntent = WriteAttemptRecoveryCommit
	}
	return &attempt, nil
}

func (s *PebbleStore) ListWriteAttemptsByState(_ context.Context, state WriteAttemptState, limit int) ([]ObjectWriteAttempt, error) {
	if s.closed.Load() {
		return nil, ErrServiceClosed
	}
	if limit <= 0 {
		limit = 100
	}

	attempts := make([]ObjectWriteAttempt, 0)
	err := s.scanPrefix(writeAttemptStatePrefix(state), func(key, value []byte) error {
		if len(attempts) >= limit {
			return nil
		}
		id, err := writeAttemptIDFromStateKey(string(key))
		if err != nil {
			return nil
		}
		attempt, err := s.GetWriteAttempt(context.Background(), id)
		if err != nil {
			return nil
		}
		attempts = append(attempts, *attempt)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return attempts, nil
}

func (s *PebbleStore) CountWriteAttemptsByState(_ context.Context, state WriteAttemptState) (int64, error) {
	if s.closed.Load() {
		return 0, ErrServiceClosed
	}
	var count int64
	err := s.scanPrefix(writeAttemptStatePrefix(state), func(key, value []byte) error {
		count++
		return nil
	})
	return count, err
}

func (s *PebbleStore) DeleteWriteAttempt(_ context.Context, id string) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}
	attempt, err := s.GetWriteAttempt(context.Background(), id)
	if err != nil {
		return err
	}
	return s.applyBatchMsgpack(nil, []string{
		writeAttemptKey(id),
		writeAttemptStateKey(attempt.State, attempt.UpdatedAt, id),
	})
}

func writeAttemptIDFromStateKey(key string) (string, error) {
	parts := strings.SplitN(strings.TrimPrefix(key, prefixWriteAttemptState), "/", 3)
	if len(parts) != 3 {
		return "", ErrInvalidArgument
	}
	if _, err := strconv.ParseInt(parts[1], 10, 64); err != nil {
		return "", err
	}
	return parts[2], nil
}
