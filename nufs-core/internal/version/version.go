// Package version holds build-time version information injected via ldflags.
//
// Build example:
//
//	go build -ldflags \
//	  "-X github.com/example/dfs/internal/version.Version=$(git describe --tags --always) \
//	   -X github.com/example/dfs/internal/version.BuildTime=$(date -u +%Y-%m-%dT%H:%M:%SZ) \
//	   -X github.com/example/dfs/internal/version.GitCommit=$(git rev-parse HEAD)" \
//	  ./cmd/datanode
package version

import "runtime"

var (
	// Version is the semantic version of the build.
	Version = "dev"
	// GitCommit is the full git commit hash.
	GitCommit = "unknown"
	// BuildTime is the UTC timestamp of the build.
	BuildTime = "unknown"
)

// Info returns a structured snapshot of all version fields.
func Info() map[string]string {
	return map[string]string{
		"version":    Version,
		"gitCommit":  GitCommit,
		"buildTime":  BuildTime,
		"goVersion":  runtime.Version(),
		"platform":   runtime.GOOS + "/" + runtime.GOARCH,
	}
}
