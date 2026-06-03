// Package config provides file-based configuration with optional hot-reload
// for NUFS daemons. Config files use YAML (or JSON) and override flag defaults
// so that CLI flags always take precedence.
package config

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/fsnotify/fsnotify"
	"gopkg.in/yaml.v3"
)

// Load reads a config file and sets the corresponding flag values.
// Supported formats: .yaml, .yml, .json.
// Only top-level keys that match registered flag names are applied.
// CLI flags already set via the command line are NOT overridden
// (flag handles this — flag.Set is a no-op when the flag was already
// set by the user).
func Load(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read config: %w", err)
	}

	var raw map[string]interface{}
	switch ext := filepath.Ext(path); ext {
	case ".yaml", ".yml":
		if err := yaml.Unmarshal(data, &raw); err != nil {
			return fmt.Errorf("parse yaml: %w", err)
		}
	case ".json":
		if err := json.Unmarshal(data, &raw); err != nil {
			return fmt.Errorf("parse json: %w", err)
		}
	default:
		return fmt.Errorf("unsupported config format: %s", ext)
	}

	return applyFlags(raw, "")
}

// applyFlags recursively sets flag values from a map.
// prefix is used for nested keys (e.g., "raft.listen").
func applyFlags(m map[string]interface{}, prefix string) error {
	for k, v := range m {
		name := k
		if prefix != "" {
			name = prefix + "." + k
		}

		switch val := v.(type) {
		case map[string]interface{}:
			if err := applyFlags(val, name); err != nil {
				return err
			}
		case []interface{}:
			// Skip slices — flags don't support them
			continue
		default:
			f := flag.Lookup(name)
			if f == nil {
				continue // unknown flag, skip
			}
			if err := f.Value.Set(fmt.Sprintf("%v", v)); err != nil {
				return fmt.Errorf("set flag %s: %w", name, err)
			}
		}
	}
	return nil
}

// Watch monitors a config file for changes and calls onReload when a
// relevant change is detected. It blocks until ctx is done or an error
// occurs. Settings that support runtime reload (e.g., log level) are
// applied automatically before onReload is called.
func Watch(ctx context.Context, path string, onReload func()) error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("fsnotify: %w", err)
	}
	defer watcher.Close()

	// Watch the containing directory — fsnotify on macOS doesn't fire
	// for file writes in all cases when watching the file directly.
	dir := filepath.Dir(path)
	if err := watcher.Add(dir); err != nil {
		return fmt.Errorf("watch dir %s: %w", dir, err)
	}

	// Debounce: coalesce multiple rapid events (e.g., editor saves).
	var debounce *time.Timer

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event, ok := <-watcher.Events:
			if !ok {
				return nil
			}
			// Filter events for our specific file name.
			if filepath.Base(event.Name) != filepath.Base(path) {
				continue
			}
			if event.Op&(fsnotify.Write|fsnotify.Create) == 0 {
				continue
			}

			// Debounce: reset timer on each event.
			if debounce != nil {
				debounce.Stop()
			}
			debounce = time.NewTimer(200 * time.Millisecond)
			go func() {
				<-debounce.C
				if err := reload(path, onReload); err != nil {
					slog.Warn("config reload", "error", err)
				}
			}()

		case err, ok := <-watcher.Errors:
			if !ok {
				return nil
			}
			slog.Warn("config watcher error", "error", err)
		}
	}
}

func reload(path string, onReload func()) error {
	// Re-read and apply flags from the file.
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("re-read config: %w", err)
	}
	var raw map[string]interface{}
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("re-parse yaml: %w", err)
	}
	if err := applyFlags(raw, ""); err != nil {
		return fmt.Errorf("re-apply flags: %w", err)
	}

	// Apply log level automatically if present.
	if lvl := flag.Lookup("log-level"); lvl != nil {
		level := lvl.Value.String()
		var slogLevel slog.Level
		if err := slogLevel.UnmarshalText([]byte(level)); err == nil {
			slog.SetLogLoggerLevel(slogLevel)
			slog.Debug("log level changed", "level", level)
		}
	}

	if onReload != nil {
		onReload()
	}
	return nil
}

// Preload scans os.Args for --config <path> (or --config=<path>), loads the
// config file to set flag defaults, and returns the config path.
// If no --config is found it returns "" without error.
// Call this BEFORE flag.Parse() so CLI flags override file values.
func Preload() string {
	return preloadFrom(os.Args[1:])
}

func preloadFrom(args []string) string {
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--config" || arg == "-config" {
			if i+1 < len(args) {
				path := args[i+1]
				if err := Load(path); err != nil {
					slog.Warn("config preload", "error", err)
				}
				return path
			}
		}
		if len(arg) > 9 && arg[:9] == "--config=" {
			path := arg[9:]
			if err := Load(path); err != nil {
				slog.Warn("config preload", "error", err)
			}
			return path
		}
	}
	return ""
}
