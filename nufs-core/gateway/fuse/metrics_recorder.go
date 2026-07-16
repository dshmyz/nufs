package fuse

import (
	"sync/atomic"
	"time"
)

// MetricsRecorder 抽象 FUSE 操作指标打点。
// 通过接口而非具体类型注入，便于单元测试用 mock 替换。
// 所有方法必须 goroutine-safe。
type MetricsRecorder interface {
	// IncOp 递增某个 FUSE 操作的计数。
	// op 取值：open/read/write/flush/release/lookup/readdir/
	//          create/mkdir/rmdir/unlink/rename/symlink/link/statfs
	IncOp(op string)

	// IncOpError 递增某个 FUSE 操作的失败计数。
	// 失败定义：返回非零 errno（EIO/ENOENT/EEXIST 等）。
	IncOpError(op string)

	// IncCacheHit 递增 chunk 缓存命中计数。
	IncCacheHit()

	// IncCacheMiss 递增 chunk 缓存未命中计数。
	IncCacheMiss()

	// IncCacheEvict 递增 chunk 缓存淘汰计数（含磁盘文件删除）。
	IncCacheEvict()

	// IncBreakerOpen 递增熔断器开路计数（dep: meta / chunkStore）。
	IncBreakerOpen(dep string)

	// IncRetry 递增重试次数（op: read/write/flush/lookup 等）。
	IncRetry(op string)
}

// noopMetricsRecorder 是空实现，用于测试和默认关闭场景。
type noopMetricsRecorder struct{}

func (noopMetricsRecorder) IncOp(string)         {}
func (noopMetricsRecorder) IncOpError(string)    {}
func (noopMetricsRecorder) IncCacheHit()         {}
func (noopMetricsRecorder) IncCacheMiss()        {}
func (noopMetricsRecorder) IncCacheEvict()       {}
func (noopMetricsRecorder) IncBreakerOpen(string) {}
func (noopMetricsRecorder) IncRetry(string)      {}

// FUSEMetrics 实现 MetricsRecorder，同时保持 HTTP /metrics 端点能力。
// 原有字段（OpsOpen 等）保留兼容；新增 error/cache/breaker/retry 计数。
var _ MetricsRecorder = (*FUSEMetrics)(nil)

// 新增计数器（不在原 Snapshot 中暴露，避免破坏现有格式）
// —— 通过新增 SnapshotV2 暴露给 OpenMetrics 端点
func (m *FUSEMetrics) IncOp(op string) {
	switch op {
	case "open":
		atomic.AddUint64(&m.OpsOpen, 1)
	case "read":
		atomic.AddUint64(&m.OpsRead, 1)
	case "write":
		atomic.AddUint64(&m.OpsWrite, 1)
	case "flush":
		atomic.AddUint64(&m.OpsFlush, 1)
	case "release":
		atomic.AddUint64(&m.OpsRelease, 1)
	case "lookup":
		atomic.AddUint64(&m.OpsLookup, 1)
	case "readdir":
		atomic.AddUint64(&m.OpsReadDir, 1)
	case "create":
		atomic.AddUint64(&m.OpsCreate, 1)
	case "mkdir":
		atomic.AddUint64(&m.OpsMkdir, 1)
	case "rmdir":
		atomic.AddUint64(&m.OpsRemove, 1) // 复用 OpsRemove
	case "unlink":
		atomic.AddUint64(&m.OpsRemove, 1)
	case "rename":
		atomic.AddUint64(&m.OpsRename, 1)
	case "symlink", "link", "statfs":
		// 这些 op 不在原有字段内，记到 error 计数器外部追踪
		// —— 通过扩展计数器支持
		atomic.AddUint64(&m.OpsOther, 1)
	}
}

func (m *FUSEMetrics) IncOpError(op string) {
	atomic.AddUint64(&m.OpsErrors, 1)
}

func (m *FUSEMetrics) IncCacheHit() {
	atomic.AddUint64(&m.CacheHits, 1)
}

func (m *FUSEMetrics) IncCacheMiss() {
	atomic.AddUint64(&m.CacheMisses, 1)
}

func (m *FUSEMetrics) IncCacheEvict() {
	atomic.AddUint64(&m.CacheEvicts, 1)
}

func (m *FUSEMetrics) IncBreakerOpen(dep string) {
	atomic.AddUint64(&m.BreakerOpens, 1)
}

func (m *FUSEMetrics) IncRetry(op string) {
	atomic.AddUint64(&m.OpsRetries, 1)
}

// ObserveOpLatency 记录操作延迟（当前仅记录到 noopMetrics 的扩展用，
// 暂未接入直方图；后续可接入 Prometheus histogram）。
func (m *FUSEMetrics) ObserveOpLatency(op string, d time.Duration) {
	// TODO: 接入 prometheus histogram
}
