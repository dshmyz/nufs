package logging

import (
	"log/slog"
	"testing"
)

// TestSetLevelHotReload verifies the config hot-reload primitive that the
// SIGHUP handler drives: SetLevel must actually flip the runtime dynamic
// level (the same LevelVar hooked into every slog handler at Init), so a
// SIGHUP genuinely changes what the daemon emits without a restart.
func TestSetLevelHotReload(t *testing.T) {
	// Init wires &dynamicLevel into the handlers; call it so the level is in
	// a known, representative state before racing SetLevel.
	Init(Config{Level: "info"})

	// A few round-trips across the range of accepted level strings; each must
	// land on the matching slog level.
	cases := []struct {
		s    string
		want slog.Level
	}{
		{"debug", slog.LevelDebug},
		{"info", slog.LevelInfo},
		{"warn", slog.LevelWarn},
		{"warning", slog.LevelWarn},
		{"error", slog.LevelError},
		// Unknown strings fall back to info (default case), same as Init.
		{"bogus", slog.LevelInfo},
	}
	for _, c := range cases {
		SetLevel(c.s)
		if got := dynamicLevel.Level(); got != c.want {
			t.Errorf("SetLevel(%q) => level %v, want %v", c.s, got, c.want)
		}
	}
}
