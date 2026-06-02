// Package signals traps OS signals and notifies the caller via a
// channel. It is the smallest possible wrapper around signal.Notify:
// the trap goroutine registers for the given signals, blocks on the
// first one, and emits a single boolean on the result channel.
//
// Lifted from nufs-fuse/fs/signals.go. The original was unexported;
// Trap is exposed so any long-running daemon (s3gw, datanode, metad)
// can use it.
package signals

import (
	"os"
	"os/signal"
)

// Trap returns a channel that receives a single value when one of the
// given signals is delivered to the process. The channel is buffered
// with size 1, so a signal delivered before the caller starts reading
// will not be lost.
//
// Typical use:
//
//	ch := signals.Trap(os.Interrupt, syscall.SIGTERM)
//	<-ch  // block until shutdown is requested
//	// ... graceful shutdown ...
func Trap(sig ...os.Signal) <-chan struct{} {
	out := make(chan struct{}, 1)
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, sig...)
	go func() {
		defer signal.Stop(sigCh)
		<-sigCh
		out <- struct{}{}
		close(out)
	}()
	return out
}
