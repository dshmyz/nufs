// Package server provides HTTP server lifecycle management.
package server

import (
	"context"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/your-org/nufs-admin/internal/api"
)

// Server wraps http.Server with graceful shutdown.
type Server struct {
	http *http.Server
}

// New creates a server with router on specified address.
func New(addr string, router *api.Router, staticFS ...fs.FS) *Server {
	mux := http.NewServeMux()
	router.Setup(mux)

	var handler http.Handler = mux
	if len(staticFS) > 0 && staticFS[0] != nil {
		handler = withSPAFallback(mux, staticFS[0])
	}

	return &Server{
		http: &http.Server{
			Addr:    addr,
			Handler: handler,
		},
	}
}

func withSPAFallback(apiHandler http.Handler, static fs.FS) http.Handler {
	fileServer := http.FileServer(http.FS(static))

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api/") {
			apiHandler.ServeHTTP(w, r)
			return
		}

		path := strings.TrimPrefix(r.URL.Path, "/")
		if path != "" && fs.ValidPath(path) {
			if f, err := static.Open(path); err == nil {
				_ = f.Close()
				fileServer.ServeHTTP(w, r)
				return
			}
		}

		index, err := fs.ReadFile(static, "index.html")
		if err != nil {
			http.NotFound(w, r)
			return
		}

		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(index)
	})
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
