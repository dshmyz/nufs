package signals

import (
	"os"
	"syscall"
	"testing"
	"time"
)

func TestTrapFires(t *testing.T) {
	ch := Trap(syscall.SIGUSR1)
	// Give the goroutine a moment to register the signal handler.
	time.Sleep(10 * time.Millisecond)

	if err := syscall.Kill(os.Getpid(), syscall.SIGUSR1); err != nil {
		t.Fatalf("Kill: %v", err)
	}

	select {
	case <-ch:
		// OK
	case <-time.After(time.Second):
		t.Fatal("Trap did not fire after SIGUSR1")
	}
}

func TestTrapSecondSignalIgnored(t *testing.T) {
	ch := Trap(syscall.SIGUSR2)
	time.Sleep(10 * time.Millisecond)

	// First signal: channel receives.
	if err := syscall.Kill(os.Getpid(), syscall.SIGUSR2); err != nil {
		t.Fatalf("Kill: %v", err)
	}
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("first SIGUSR2 not delivered")
	}

	// Second signal: no second receiver, but the goroutine has
	// already returned, so the default Go signal behaviour applies.
	// We only assert that we don't deadlock or panic.
	if err := syscall.Kill(os.Getpid(), syscall.SIGUSR2); err != nil {
		t.Fatalf("Kill 2: %v", err)
	}
}
