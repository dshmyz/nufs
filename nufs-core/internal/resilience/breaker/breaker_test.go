package breaker

import (
	"errors"
	"testing"
	"time"
)

func TestBreaker_StaysClosedOnSuccess(t *testing.T) {
	b := New("test", Config{Threshold: 3})
	if got := b.Execute(func() error { return nil }); got != nil {
		t.Fatalf("expected nil, got %v", got)
	}
	if b.State() != StateClosed {
		t.Fatalf("expected closed, got %s", b.State())
	}
}

func TestBreaker_OpensAfterThreshold(t *testing.T) {
	b := New("test", Config{Threshold: 3, Timeout: time.Minute})
	boom := errors.New("boom")
	for i := 0; i < 3; i++ {
		if err := b.Execute(func() error { return boom }); !errors.Is(err, boom) {
			t.Fatalf("call %d: expected boom, got %v", i, err)
		}
	}
	if b.State() != StateOpen {
		t.Fatalf("expected open after %d failures, got %s", 3, b.State())
	}
}

func TestBreaker_OpenFailsFast(t *testing.T) {
	b := New("test", Config{Threshold: 1, Timeout: time.Minute})
	_ = b.Execute(func() error { return errors.New("boom") })
	if !b.IsOpen() {
		t.Fatal("breaker should be open")
	}
	calls := 0
	err := b.Execute(func() error { calls++; return nil })
	if !errors.Is(err, ErrOpen) {
		t.Fatalf("expected ErrOpen, got %v", err)
	}
	if calls != 0 {
		t.Fatalf("fn should not have been invoked while open, was called %d times", calls)
	}
}

func TestBreaker_HalfOpenAfterTimeout(t *testing.T) {
	// Threshold 1, very short timeout so the test is fast.
	b := New("test", Config{Threshold: 1, Timeout: 5 * time.Millisecond})
	boom := errors.New("boom")
	_ = b.Execute(func() error { return boom })
	if !b.IsOpen() {
		t.Fatal("expected open")
	}
	time.Sleep(20 * time.Millisecond)

	// A successful probe should close the circuit.
	if err := b.Execute(func() error { return nil }); err != nil {
		t.Fatalf("probe should succeed, got %v", err)
	}
	if b.State() != StateClosed {
		t.Fatalf("expected closed after successful probe, got %s", b.State())
	}
}

func TestBreaker_HalfOpenFailureReopens(t *testing.T) {
	b := New("test", Config{Threshold: 1, Timeout: 5 * time.Millisecond})
	boom := errors.New("boom")
	_ = b.Execute(func() error { return boom })
	time.Sleep(20 * time.Millisecond)

	// Failing probe should re-open.
	if err := b.Execute(func() error { return boom }); !errors.Is(err, boom) {
		t.Fatalf("expected boom, got %v", err)
	}
	if b.State() != StateOpen {
		t.Fatalf("expected re-opened, got %s", b.State())
	}
}

func TestBreaker_SuccessResetsFailureCount(t *testing.T) {
	b := New("test", Config{Threshold: 3})
	boom := errors.New("boom")
	// 2 failures (below threshold), then a success, then 2 more failures.
	// The success in the middle should reset the counter, so we should
	// not open until 3 *consecutive* failures.
	for i := 0; i < 2; i++ {
		_ = b.Execute(func() error { return boom })
	}
	if b.State() != StateClosed {
		t.Fatalf("expected closed after 2 failures, got %s", b.State())
	}
	if err := b.Execute(func() error { return nil }); err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if b.Failures() != 0 {
		t.Fatalf("expected 0 failures after success, got %d", b.Failures())
	}
	for i := 0; i < 2; i++ {
		_ = b.Execute(func() error { return boom })
	}
	if b.State() != StateClosed {
		t.Fatalf("expected still closed (counter was reset), got %s", b.State())
	}
}

func TestBreaker_OnStateChange(t *testing.T) {
	transitions := []struct {
		from, to State
	}{}
	b := New("test", Config{
		Threshold: 2,
		Timeout:   5 * time.Millisecond,
		OnStateChange: func(_ string, from, to State) {
			transitions = append(transitions, struct{ from, to State }{from, to})
		},
	})
	boom := errors.New("boom")
	_ = b.Execute(func() error { return boom })
	_ = b.Execute(func() error { return boom })
	if len(transitions) != 1 || transitions[0].to != StateOpen {
		t.Fatalf("expected one transition to open, got %+v", transitions)
	}
	time.Sleep(20 * time.Millisecond)
	_ = b.Execute(func() error { return nil })
	// Three transitions total: closed->open, open->half-open, half-open->closed.
	if len(transitions) != 3 {
		t.Fatalf("expected 3 transitions, got %+v", transitions)
	}
	if transitions[1].from != StateOpen || transitions[1].to != StateHalfOpen {
		t.Fatalf("expected open->half-open, got %+v", transitions[1])
	}
	if transitions[2].from != StateHalfOpen || transitions[2].to != StateClosed {
		t.Fatalf("expected half-open->closed, got %+v", transitions[2])
	}
}
