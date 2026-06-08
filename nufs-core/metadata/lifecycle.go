package metadata

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// ============================================================
// Lifecycle Manager — Automated Data Lifecycle Management
// ============================================================

// LifecycleRule defines a lifecycle policy for a bucket.
type LifecycleRule struct {
	Bucket     string       `json:"bucket"`
	Prefix     string       `json:"prefix"`     // Empty = all files
	Transition []Transition `json:"transition"` // Tier transitions
	Expiration *Expiration  `json:"expiration"` // Auto-delete
}

// Transition defines a tier change after N days.
type Transition struct {
	Days int         `json:"days"`
	To   StorageTier `json:"to_tier"`
}

// Expiration defines when data is permanently deleted.
type Expiration struct {
	Days int `json:"days"` // Delete after this many days
}

// LifecycleEngine executes lifecycle rules automatically.
type LifecycleEngine struct {
	meta    MetadataService
	rules   map[string][]LifecycleRule // bucket -> rules
	mu      sync.RWMutex
	stopCh  chan struct{}
	running atomic.Bool
	wg      sync.WaitGroup // Tracks background goroutine

	transitions atomic.Int64
	deletions   atomic.Int64
}

// NewLifecycleEngine creates a lifecycle management engine.
func NewLifecycleEngine(meta MetadataService) *LifecycleEngine {
	return &LifecycleEngine{
		meta:   meta,
		rules:  make(map[string][]LifecycleRule),
		stopCh: make(chan struct{}),
	}
}

// AddRule registers a lifecycle rule.
func (le *LifecycleEngine) AddRule(rule LifecycleRule) {
	le.mu.Lock()
	defer le.mu.Unlock()
	le.rules[rule.Bucket] = append(le.rules[rule.Bucket], rule)
}

// RemoveRule removes all rules for a bucket.
func (le *LifecycleEngine) RemoveRule(bucket string) {
	le.mu.Lock()
	defer le.mu.Unlock()
	delete(le.rules, bucket)
}

// Start begins periodic lifecycle execution.
func (le *LifecycleEngine) Start(interval time.Duration) {
	if le.running.Swap(true) {
		return
	}
	le.wg.Add(1)
	go func() {
		defer le.wg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := le.execute(context.Background()); err != nil {
					slog.Error("lifecycle: execution error", "error", err)
				}
			case <-le.stopCh:
				return
			}
		}
	}()
}

// Stop terminates the lifecycle engine.
func (le *LifecycleEngine) Stop() {
	if le.running.Swap(false) {
		close(le.stopCh)
	}
	le.wg.Wait()
}

func (le *LifecycleEngine) execute(ctx context.Context) error {
	le.mu.RLock()
	rules := make(map[string][]LifecycleRule)
	for k, v := range le.rules {
		rules[k] = v
	}
	le.mu.RUnlock()

	for bucket, bucketRules := range rules {
		if err := le.processBucket(ctx, bucket, bucketRules); err != nil {
			slog.Error("lifecycle: bucket error", "bucket", bucket, "error", err)
		}
	}
	return nil
}

func (le *LifecycleEngine) processBucket(ctx context.Context, bucket string, rules []LifecycleRule) error {
	info, err := le.meta.GetBucket(ctx, bucket)
	if err != nil {
		return fmt.Errorf("get bucket %s: %w", bucket, err)
	}

	now := time.Now()

	// Traverse the bucket's directory tree recursively to find files belonging to this bucket.
	// This is more correct than comparing inode IDs (which can wrap around after deletions).
	return le.walkDir(ctx, info.RootInode, bucket, "", rules, now)
}

// walkDir recursively walks a directory and applies lifecycle rules to regular files.
func (le *LifecycleEngine) walkDir(ctx context.Context, dirID InodeID, bucket, prefix string, rules []LifecycleRule, now time.Time) error {
	entries, err := le.meta.ReadDir(ctx, dirID, 0, 0)
	if err != nil {
		return fmt.Errorf("read dir %d: %w", dirID, err)
	}

	for _, entry := range entries {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		switch entry.Type {
		case FileDirectory:
			// Recurse into subdirectory
			childPrefix := prefix + entry.Name + "/"
			if err := le.walkDir(ctx, entry.InodeID, bucket, childPrefix, rules, now); err != nil {
				slog.Error("lifecycle: error walking directory", "path", prefix+entry.Name, "error", err)
			}

		case FileRegular:
			// Get inode metadata
			meta, err := le.meta.GetInode(ctx, entry.InodeID)
			if err != nil {
				slog.Error("lifecycle: get inode", "inode_id", entry.InodeID, "error", err)
				continue
			}

			relPath := prefix + entry.Name
			fileAge := now.Sub(time.Unix(0, meta.CTime))

			for _, rule := range rules {
				// Prefix matching
				if !matchesPrefix(relPath, rule.Prefix) {
					continue
				}

				// Tier transitions
				for _, transition := range rule.Transition {
					if fileAge > time.Duration(transition.Days)*24*time.Hour {
						for i := range meta.ChunkMap {
							chunk, err := le.meta.GetChunk(ctx, meta.ChunkMap[i].ID)
							if err != nil {
								continue
							}
							if chunk.State == ChunkReady && chunk.Tier < transition.To {
								chunk.Tier = transition.To
								if err := le.meta.UpdateChunk(ctx, chunk); err != nil {
									slog.Error("lifecycle: transition chunk", "chunk_id", chunk.ID, "error", err)
								} else {
									le.transitions.Add(1)
								}
							}
						}
					}
				}

				// Expiration
				if rule.Expiration != nil && fileAge > time.Duration(rule.Expiration.Days)*24*time.Hour {
					meta.NLink = 0
					if err := le.meta.UpdateInode(ctx, meta); err != nil {
						slog.Error("lifecycle: expire inode", "inode_id", meta.ID, "error", err)
					} else {
						le.deletions.Add(1)
						slog.Info("lifecycle: expired file", "path", relPath, "age", fileAge)
					}
				}
			}

		case FileSymlink:
			// Symlinks are not subject to lifecycle rules
			continue
		}
	}

	return nil
}

func matchesPrefix(name, prefix string) bool {
	if prefix == "" {
		return true
	}
	return len(name) >= len(prefix) && name[:len(prefix)] == prefix
}

// Stats returns lifecycle statistics.
func (le *LifecycleEngine) Stats() (transitions, deletions int64) {
	return le.transitions.Load(), le.deletions.Load()
}
