// Package logging provides a structured logging foundation for all NUFS
// components. It wraps log/slog with a consistent configuration mechanism
// so that every daemon produces the same log format.
package logging

import (
	"log/slog"
	"os"
)

type Config struct {
	Level     string
	JSON      bool
	AddSource bool
}

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

func Named(name string) *slog.Logger {
	return slog.Default().With("component", name)
}
