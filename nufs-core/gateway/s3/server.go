package s3

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/example/dfs/internal/tlsutil"
)

// ServerConfig controls how Gateway.Run wires an *http.Server and
// shuts it down. The zero value is a sensible production default
// (graceful timeout = 30 s, request timeouts 60/60/120 s, signals
// SIGINT + SIGTERM).
type ServerConfig struct {
	// Addr is the listen address. Required.
	Addr string
	// GracefulTimeout bounds how long Shutdown waits for in-flight
	// requests to drain after a signal. <= 0 means 30s.
	GracefulTimeout time.Duration
	// ReadTimeout, WriteTimeout, IdleTimeout map to the http.Server
	// fields. <= 0 means 60s, 60s, 120s respectively.
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	IdleTimeout  time.Duration
	// Listen is the listen function; nil defaults to net.Listen
	// ("tcp", Addr). Tests override it with a free-port helper.
	Listen func(addr string) (net.Listener, error)
	// Trap signals is the function used to receive shutdown signals.
	// nil defaults to SIGINT+SIGTERM. Tests inject a manual channel.
	Trap func(c chan<- os.Signal)
	// Handler, if non-nil, is used in place of gw.Handler(). Mostly
	// useful for tests that want to wrap the handler in a probe.
	Handler http.Handler
	// TLSCertFile and TLSKeyFile enable TLS on the listener.
	TLSCertFile string
	TLSKeyFile  string
}

// Run starts an HTTP server on the given address, blocks until ctx
// is cancelled or a SIGINT/SIGTERM is received, and then performs a
// graceful shutdown bounded by cfg.GracefulTimeout. It returns the
// first non-nil error and is safe to call from main().
func (gw *Gateway) Run(ctx context.Context, cfg ServerConfig) error {
	if cfg.Addr == "" {
		return errors.New("s3gw: ServerConfig.Addr is required")
	}
	if cfg.GracefulTimeout <= 0 {
		cfg.GracefulTimeout = 30 * time.Second
	}
	if cfg.ReadTimeout <= 0 {
		cfg.ReadTimeout = 60 * time.Second
	}
	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = 60 * time.Second
	}
	if cfg.IdleTimeout <= 0 {
		cfg.IdleTimeout = 120 * time.Second
	}
	if cfg.Listen == nil {
		cfg.Listen = func(addr string) (net.Listener, error) {
			return net.Listen("tcp", addr)
		}
	}
	if cfg.Trap == nil {
		cfg.Trap = func(c chan<- os.Signal) {
			signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
		}
	}

	handler := cfg.Handler
	if handler == nil {
		handler = gw.Handler()
	}

	srv := &http.Server{
		Addr:         cfg.Addr,
		Handler:      handler,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
		IdleTimeout:  cfg.IdleTimeout,
	}

	ln, err := cfg.Listen(cfg.Addr)
	if err != nil {
		return fmt.Errorf("s3gw: listen %s: %w", cfg.Addr, err)
	}

	// Wrap listener with TLS if certificates are configured.
	if cfg.TLSCertFile != "" && cfg.TLSKeyFile != "" {
		tlsCfg, err := tlsutil.ServerConfig(tlsutil.Config{
			CertFile: cfg.TLSCertFile,
			KeyFile:  cfg.TLSKeyFile,
		})
		if err != nil {
			return fmt.Errorf("s3gw: tls: %w", err)
		}
		ln = tlsutil.NewListener(ln, tlsCfg)
	}

	errCh := make(chan error, 1)
	go func() {
		scheme := "http"
		if cfg.TLSCertFile != "" {
			scheme = "https"
		}
		log.Printf("s3gw: listening on %s://%s", scheme, cfg.Addr)
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
		close(errCh)
	}()

	sigCh := make(chan os.Signal, 1)
	cfg.Trap(sigCh)

	select {
	case <-ctx.Done():
		log.Printf("s3gw: context cancelled, shutting down...")
	case sig := <-sigCh:
		log.Printf("s3gw: received signal %v, shutting down...", sig)
	case err, ok := <-errCh:
		if ok && err != nil {
			log.Printf("s3gw: server error: %v", err)
			// Best-effort shutdown so we don't leave dangling conns.
			shutCtx, cancel := context.WithTimeout(context.Background(), cfg.GracefulTimeout)
			defer cancel()
			_ = srv.Shutdown(shutCtx)
			return err
		}
	}

	shutCtx, cancel := context.WithTimeout(context.Background(), cfg.GracefulTimeout)
	defer cancel()
	if err := srv.Shutdown(shutCtx); err != nil {
		log.Printf("s3gw: shutdown error: %v", err)
		return err
	}
	log.Println("s3gw: stopped")
	return nil
}
