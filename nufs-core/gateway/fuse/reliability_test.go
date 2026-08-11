package fuse

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dshmyz/nufs/nufs-core/internal/resilience/breaker"
	"github.com/dshmyz/nufs/nufs-core/internal/resilience/lock"
	"github.com/dshmyz/nufs/nufs-core/internal/resilience/retry"
)

// ========== ReliabilityWrapper TDD 红测试 ==========

// transientErr 实现 net.Error，被 retry.IsRetryable 识别为可重试。
type transientErr struct{ msg string }

func (e transientErr) Error() string   { return e.msg }
func (e transientErr) Timeout() bool    { return true }
func (e transientErr) Temporary() bool  { return true }

var _ net.Error = transientErr{}

// fastRetryCfg 返回快速重试配置，测试不阻塞。
func fastRetryCfg() retry.Config {
	return retry.Config{
		MaxAttempts: 3,
		BaseDelay:   1 * time.Millisecond,
		MaxDelay:    10 * time.Millisecond,
	}
}

// fastBreakerCfg 返回快速熔断配置。
func fastBreakerCfg(onChange func(string, breaker.State, breaker.State)) breaker.Config {
	return breaker.Config{
		Threshold:    3,
		Timeout:      100 * time.Millisecond,
		OnStateChange: onChange,
	}
}

// TestReliabilityWrapper_DoMeta_RetriesAndSucceeds 验证 DoMeta 在瞬时错误时重试并最终成功。
func TestReliabilityWrapper_DoMeta_RetriesAndSucceeds(t *testing.T) {
	w := NewReliabilityWrapper(nil, fastRetryCfg(), fastBreakerCfg(nil), nil)

	var calls int32
	err := w.DoMeta("read", func() error {
		n := atomic.AddInt32(&calls, 1)
		if n < 3 {
			return transientErr{msg: fmt.Sprintf("transient #%d", n)}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("DoMeta: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 3 {
		t.Errorf("calls = %d, want 3 (2 retries + 1 success)", got)
	}
}

// TestReliabilityWrapper_DoMeta_NoRetryOnNonRetryable 验证不可重试错误立即返回。
func TestReliabilityWrapper_DoMeta_NoRetryOnNonRetryable(t *testing.T) {
	w := NewReliabilityWrapper(nil, fastRetryCfg(), fastBreakerCfg(nil), nil)

	nonRetryable := errors.New("permanent failure")

	var calls int32
	err := w.DoMeta("read", func() error {
		atomic.AddInt32(&calls, 1)
		return nonRetryable
	})
	if !errors.Is(err, nonRetryable) {
		t.Fatalf("err = %v, want %v", err, nonRetryable)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Errorf("calls = %d, want 1 (no retry)", got)
	}
}

// TestReliabilityWrapper_DoMeta_BreakerOpensAfterThreshold 验证持续失败后熔断器开路。
func TestReliabilityWrapper_DoMeta_BreakerOpensAfterThreshold(t *testing.T) {
	w := NewReliabilityWrapper(nil, fastRetryCfg(), fastBreakerCfg(nil), nil)

	// 每次调用都返回不可重试错误，快速累积失败。
	failFn := func() error { return errors.New("fail") }

	// 调用 threshold=3 次，第 3 次触发熔断。
	for i := 0; i < 3; i++ {
		_ = w.DoMeta("read", failFn)
	}

	// 第 4 次调用应被熔断器拒绝（ErrOpen），fn 不应被执行。
	var called int32
	err := w.DoMeta("read", func() error {
		atomic.AddInt32(&called, 1)
		return nil
	})
	if err == nil {
		t.Fatal("expected breaker open error, got nil")
	}
	if got := atomic.LoadInt32(&called); got != 0 {
		t.Errorf("fn called = %d, want 0 (breaker should reject)", got)
	}
}

// TestReliabilityWrapper_DoChunk_RetriesAndSucceeds 验证 DoChunk 同样支持重试。
func TestReliabilityWrapper_DoChunk_RetriesAndSucceeds(t *testing.T) {
	w := NewReliabilityWrapper(nil, fastRetryCfg(), fastBreakerCfg(nil), nil)

	var calls int32
	err := w.DoChunk("write", func() error {
		n := atomic.AddInt32(&calls, 1)
		if n < 2 {
			return transientErr{msg: "transient"}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("DoChunk: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Errorf("calls = %d, want 2", got)
	}
}

// TestReliabilityWrapper_IncrementsRetryCounter 验证重试时递增 retry 计数器。
func TestReliabilityWrapper_IncrementsRetryCounter(t *testing.T) {
	rec := &FUSEMetrics{}
	w := NewReliabilityWrapper(rec, fastRetryCfg(), fastBreakerCfg(nil), nil)

	_ = w.DoMeta("read", func() error {
		return transientErr{msg: "transient"}
	})

	if got := atomic.LoadUint64(&rec.OpsRetries); got == 0 {
		t.Errorf("OpsRetries = 0, want > 0 (retries should be counted)")
	}
}

// TestReliabilityWrapper_IncrementsBreakerCounter 验证熔断器开路时递增 breaker 计数器。
func TestReliabilityWrapper_IncrementsBreakerCounter(t *testing.T) {
	rec := &FUSEMetrics{}
	w := NewReliabilityWrapper(rec, fastRetryCfg(), fastBreakerCfg(nil), nil)

	// 触发熔断（3 次失败）。
	for i := 0; i < 3; i++ {
		_ = w.DoMeta("read", func() error { return errors.New("fail") })
	}

	if got := atomic.LoadUint64(&rec.BreakerOpens); got == 0 {
		t.Errorf("BreakerOpens = 0, want > 0 (breaker open should be counted)")
	}
}

// TestReliabilityWrapper_LockInode_SerializesConcurrent 验证路径锁串行化并发访问。
func TestReliabilityWrapper_LockInode_SerializesConcurrent(t *testing.T) {
	w := NewReliabilityWrapper(nil, fastRetryCfg(), fastBreakerCfg(nil), nil)

	var (
		concurrent int32
		maxConc    int32
		wg         sync.WaitGroup
	)

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			unlock := w.LockInode(42)
			defer unlock()

			cur := atomic.AddInt32(&concurrent, 1)
			// 更新最大并发数
			for {
				old := atomic.LoadInt32(&maxConc)
				if cur <= old || atomic.CompareAndSwapInt32(&maxConc, old, cur) {
					break
				}
			}
			time.Sleep(10 * time.Millisecond)
			atomic.AddInt32(&concurrent, -1)
		}()
	}
	wg.Wait()

	if got := atomic.LoadInt32(&maxConc); got != 1 {
		t.Errorf("max concurrent = %d, want 1 (path lock should serialize)", got)
	}
}

// TestReliabilityWrapper_LockInode_DifferentInodesParallel 验证不同 inode 的锁不互斥。
func TestReliabilityWrapper_LockInode_DifferentInodesParallel(t *testing.T) {
	w := NewReliabilityWrapper(nil, fastRetryCfg(), fastBreakerCfg(nil), nil)

	var (
		concurrent int32
		maxConc    int32
		wg         sync.WaitGroup
	)

	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(id uint64) {
			defer wg.Done()
			unlock := w.LockInode(id)
			defer unlock()

			cur := atomic.AddInt32(&concurrent, 1)
			for {
				old := atomic.LoadInt32(&maxConc)
				if cur <= old || atomic.CompareAndSwapInt32(&maxConc, old, cur) {
					break
				}
			}
			time.Sleep(20 * time.Millisecond)
			atomic.AddInt32(&concurrent, -1)
		}(uint64(i + 1))
	}
	wg.Wait()

	if got := atomic.LoadInt32(&maxConc); got != 3 {
		t.Errorf("max concurrent = %d, want 3 (different inodes should be parallel)", got)
	}
}

// 确保编译时引用了 lock/breaker 包（避免 unused import）。
var _ = lock.New
var _ = breaker.New
