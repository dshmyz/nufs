package fuse

import (
	"fmt"
	"log/slog"

	"github.com/dshmyz/nufs/nufs-core/internal/resilience/breaker"
	"github.com/dshmyz/nufs/nufs-core/internal/resilience/lock"
	"github.com/dshmyz/nufs/nufs-core/internal/resilience/retry"
)

// ReliabilityWrapper 将 retry + circuit breaker + 路径锁组合为统一接口，
// 供 FUSE 层的 metadata/chunkstore 调用使用。
//
// 设计：
//   - retry 处理瞬时错误（网络超时、5xx 等），透明重试
//   - breaker 处理持续失败，达到阈值后开路拒绝请求，避免雪崩
//   - pathLock 按 inode 串行化写操作（Flush），防止并发写冲突
//
// 调用链路：DoMeta/DoChunk → breaker.Execute → retry.Do → fn
// retry 耗尽后 breaker 才看到一次失败，避免重试期间误触熔断。
type ReliabilityWrapper struct {
	metaBreaker  *breaker.Breaker
	chunkBreaker *breaker.Breaker
	retryCfg     retry.Config
	pathLock     *lock.Manager
	recorder     MetricsRecorder
	log          *slog.Logger
}

// NewReliabilityWrapper 创建 ReliabilityWrapper。
//
//   - rec: 指标记录器，nil 时使用 noopMetricsRecorder
//   - retryCfg: 重试配置（零值用 retry 包默认值）
//   - breakerCfg: 熔断配置（同时应用于 meta 和 chunk 两个熔断器）
//     breakerCfg.OnStateChange 会被包装以递增 breaker 计数器，
//     原有回调仍然执行。
//   - log: 结构化日志器（nil 时使用 slog.Default()）
func NewReliabilityWrapper(rec MetricsRecorder, retryCfg retry.Config, breakerCfg breaker.Config, log *slog.Logger) *ReliabilityWrapper {
	if log == nil {
		log = slog.Default()
	}
	w := &ReliabilityWrapper{
		retryCfg: retryCfg,
		pathLock: lock.New(),
		recorder: rec,
		log:      log,
	}

	origOnChange := breakerCfg.OnStateChange

	metaCfg := breakerCfg
	metaCfg.OnStateChange = func(name string, from, to breaker.State) {
		if to == breaker.StateOpen {
			r := recorderFor(w.recorder)
			r.IncBreakerOpen("meta")
			w.log.Warn("circuit breaker opened", "target", "meta", "from", from)
		}
		if origOnChange != nil {
			origOnChange(name, from, to)
		}
	}

	chunkCfg := breakerCfg
	chunkCfg.OnStateChange = func(name string, from, to breaker.State) {
		if to == breaker.StateOpen {
			r := recorderFor(w.recorder)
			r.IncBreakerOpen("chunk")
			w.log.Warn("circuit breaker opened", "target", "chunk", "from", from)
		}
		if origOnChange != nil {
			origOnChange(name, from, to)
		}
	}

	w.metaBreaker = breaker.New("meta", metaCfg)
	w.chunkBreaker = breaker.New("chunk", chunkCfg)
	return w
}

// DoMeta 执行一次 metadata 操作，带 retry + circuit breaker。
// op 用于指标打点（read/write/flush/lookup 等）。
// nil receiver 时直接执行 fn（passthrough），便于兼容未注入 wrapper 的场景。
func (w *ReliabilityWrapper) DoMeta(op string, fn func() error) error {
	if w == nil {
		return fn()
	}
	return w.metaBreaker.Execute(func() error {
		err, attempts := retry.Do(w.retryCfg, fn)
		if attempts > 1 {
			r := recorderFor(w.recorder)
			r.IncRetry(op)
			if err != nil {
				w.log.Warn("retry exhausted", "op", op, "attempts", attempts, "error", err)
			} else {
				w.log.Warn("retry succeeded", "op", op, "attempts", attempts)
			}
		}
		return err
	})
}

// DoChunk 执行一次 chunkstore 操作，带 retry + circuit breaker。
// op 用于指标打点（read/write/flush 等）。
// nil receiver 时直接执行 fn（passthrough）。
func (w *ReliabilityWrapper) DoChunk(op string, fn func() error) error {
	if w == nil {
		return fn()
	}
	return w.chunkBreaker.Execute(func() error {
		err, attempts := retry.Do(w.retryCfg, fn)
		if attempts > 1 {
			r := recorderFor(w.recorder)
			r.IncRetry(op)
			if err != nil {
				w.log.Warn("retry exhausted", "op", op, "attempts", attempts, "error", err)
			} else {
				w.log.Warn("retry succeeded", "op", op, "attempts", attempts)
			}
		}
		return err
	})
}

// LockInode 获取指定 inode 的路径锁，返回 unlock 函数。
// 典型用法：unlock := w.LockInode(id); defer unlock()
// 同一 inode 的多次 LockInode 串行执行，不同 inode 之间并行。
// nil receiver 时返回 no-op unlock（不锁）。
func (w *ReliabilityWrapper) LockInode(inodeID uint64) func() {
	if w == nil {
		return func() {}
	}
	key := fmt.Sprintf("inode:%d", inodeID)
	_ = w.pathLock.Lock(key)
	return func() {
		w.pathLock.Unlock(key)
	}
}
