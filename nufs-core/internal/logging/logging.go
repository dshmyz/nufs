// Package logging provides a structured logging foundation for all NUFS
// components. It wraps log/slog with a consistent configuration mechanism
// so that every daemon produces the same log format.
//
// Usage:
//
//	logging.Init(logging.Config{Level: "info", JSON: true})
//	log := logging.Named("metadata")
//	log.Info("raft leader changed", "node_id", id, "term", term)
//
// For request-scoped logging with trace IDs:
//
//	log := logging.WithRequestID(requestID)
//	log.Info("handling request", "method", "GET", "path", "/api/v1/buckets")
package logging

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync/atomic"
	"time"
)

// Config configures the global logger.
type Config struct {
	Level     string // debug, info, warn, error
	JSON      bool   // true = JSON output, false = text
	AddSource bool   // include source file/line
}

// Init initializes the global logger. Must be called once at startup.
func Init(cfg Config) {
	var level slog.Level
	switch cfg.Level {
	case "debug":
		level = slog.LevelDebug
	case "warn", "warning":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{Level: level, AddSource: cfg.AddSource}
	var h slog.Handler
	if cfg.JSON {
		h = slog.NewJSONHandler(os.Stderr, opts)
	} else {
		h = slog.NewTextHandler(os.Stderr, opts)
	}
	slog.SetDefault(slog.New(h))
}

// Named returns a logger with a component name attached.
func Named(name string) *slog.Logger {
	return slog.Default().With("component", name)
}

// ============================================================
// Request ID tracking for distributed tracing
// ============================================================

type contextKey string

const requestIDKey contextKey = "request_id"

// requestIDCounter is a monotonic counter for generating unique request IDs.
var requestIDCounter atomic.Uint64

// NewRequestID generates a unique request ID.
// Format: <timestamp_hex>-<counter_hex> (e.g., "18a3b2c1-0001")
func NewRequestID() string {
	ts := time.Now().UnixNano()
	cnt := requestIDCounter.Add(1)
	return fmt.Sprintf("%x-%04x", ts, cnt)
}

// WithRequestID stores a request ID in the context.
func WithRequestID(ctx context.Context, requestID string) context.Context {
	return context.WithValue(ctx, requestIDKey, requestID)
}

// RequestIDFromContext retrieves the request ID from the context.
func RequestIDFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(requestIDKey).(string); ok {
		return v
	}
	return ""
}

// WithRequestID returns a logger that includes the given request ID.
// Use this for request-scoped logging to trace requests across services.
func WithRequestIDLogger(requestID string) *slog.Logger {
	return slog.Default().With("request_id", requestID)
}

// LoggerFromContext returns a logger with the request ID from the context.
func LoggerFromContext(ctx context.Context) *slog.Logger {
	if id := RequestIDFromContext(ctx); id != "" {
		return WithRequestIDLogger(id)
	}
	return slog.Default()
}
