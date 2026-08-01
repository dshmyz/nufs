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
	"io"
	"log/slog"
	"os"
	"sync/atomic"
	"time"

	"gopkg.in/natefinch/lumberjack.v2"
)

// Config configures the global logger.
type Config struct {
	Level     string // debug, info, warn, error
	JSON      bool   // true = JSON output, false = text
	AddSource bool   // include source file/line
	LogFile   string // path to log file (empty = stderr). File is auto-rotated.
	MaxSize   int64  // max log file size in bytes before rotation (default 100MB)
	MaxBackups int   // max rotated files to keep (default 7)
}

// Init initializes the global logger. Must be called once at startup.
// If Config.LogFile is set, logs are written to that file with automatic
// rotation (default 100MB, 7 backups). Otherwise logs go to stderr.
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

	opts := &slog.HandlerOptions{Level: &dynamicLevel, AddSource: cfg.AddSource}
	dynamicLevel.Set(level)

	var w io.Writer
	if cfg.LogFile != "" {
		w = &lumberjack.Logger{
			Filename:   cfg.LogFile,
			MaxSize:    int(cfg.MaxSize / 1024 / 1024), // lumberjack uses MB
			MaxBackups: cfg.MaxBackups,
			MaxAge:     30, // days
			Compress:   true,
		}
	} else {
		w = os.Stderr
	}

	var h slog.Handler
	if cfg.JSON {
		h = slog.NewJSONHandler(w, opts)
	} else {
		h = slog.NewTextHandler(w, opts)
	}
	slog.SetDefault(slog.New(h))
}

// dynamicLevel is the shared log level that can be changed at runtime.
var dynamicLevel slog.LevelVar

// SetLevel dynamically changes the global log level without restarting.
// This is typically called from a SIGHUP handler to support config hot-reload.
func SetLevel(levelStr string) {
	var level slog.Level
	switch levelStr {
	case "debug":
		level = slog.LevelDebug
	case "warn", "warning":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}
	dynamicLevel.Set(level)
	slog.Info("log level changed", "new_level", levelStr)
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
