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

// ExecuteOnce runs the currently registered lifecycle rules synchronously.
// It is used by ops paths and tests that need deterministic execution without
// waiting for the periodic ticker.
func (le *LifecycleEngine) ExecuteOnce(ctx context.Context) error {
	return le.execute(ctx)
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
// It performs directory-level prefix pruning: if no rule's prefix can possibly match
// any file under the current directory path, the subtree is skipped entirely.
func (le *LifecycleEngine) walkDir(ctx context.Context, dirID InodeID, bucket, prefix string, rules []LifecycleRule, now time.Time) error {
	// Directory-level pruning: check if any rule could possibly match files under this prefix.
	// A rule with prefix P can match files under directory D only if:
	//   - P is empty (matches everything), or
	//   - P starts with D (rule targets a deeper path under this dir), or
	//   - D starts with P (this dir is inside the rule's prefix scope)
	hasMatchingRule := false
	for _, rule := range rules {
		if rule.Prefix == "" {
			hasMatchingRule = true
			break
		}
		if len(rule.Prefix) >= len(prefix) && rule.Prefix[:len(prefix)] == prefix {
			// Rule prefix starts with current dir prefix (rule targets deeper path)
			hasMatchingRule = true
			break
		}
		if len(prefix) > len(rule.Prefix) && prefix[:len(rule.Prefix)] == rule.Prefix {
			// Current dir is inside rule's prefix scope
			hasMatchingRule = true
			break
		}
	}
	if !hasMatchingRule {
		return nil
	}

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
			ageTime := meta.MTime
			if ageTime == 0 {
				ageTime = meta.CTime
			}
			fileAge := now.Sub(time.Unix(0, ageTime))

			for _, rule := range rules {
				// Prefix matching (S3 semantics: prefix must match at path boundary)
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

// matchesPrefix checks if name matches the given prefix using S3-compatible semantics.
// A prefix "logs/" matches "logs/app.log" but not "logsapp.log".
// An empty prefix matches everything.
func matchesPrefix(name, prefix string) bool {
	if prefix == "" {
		return true
	}
	if len(name) < len(prefix) {
		return false
	}
	return name[:len(prefix)] == prefix
}

// Stats returns lifecycle statistics.
func (le *LifecycleEngine) Stats() (transitions, deletions int64) {
	return le.transitions.Load(), le.deletions.Load()
}
