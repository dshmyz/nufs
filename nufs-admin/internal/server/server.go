// Package server provides HTTP server lifecycle management.
package server

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/your-org/nufs-admin/internal/api"
)

// Server wraps http.Server with graceful shutdown.
type Server struct {
	http *http.Server
}

// New creates a server with router on specified address.
func New(addr string, router *api.Router) *Server {
	mux := http.NewServeMux()
	router.Setup(mux)

	// SPA fallback: serve embedded static files for non-API paths
	// This will be implemented when web/dist is available

	return &Server{
		http: &http.Server{
			Addr:    addr,
			Handler: mux,
		},
	}
}

// Run starts the server and handles graceful shutdown.
func (s *Server) Run() error {
	// Start server in goroutine
	errCh := make(chan error, 1)
	go func() {
		log.Printf("admin-server listening on %s", s.http.Addr)
		if err := s.http.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	// Wait for interrupt signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-errCh:
		return err
	case sig := <-sigCh:
		log.Printf("received signal %v, shutting down...", sig)
	}

	// Graceful shutdown with 5s timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := s.http.Shutdown(ctx); err != nil {
		return fmt.Errorf("shutdown error: %w", err)
	}

	log.Println("server stopped")
	return nil
}