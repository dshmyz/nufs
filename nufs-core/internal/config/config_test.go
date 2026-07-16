package config

import (
	"flag"
	"os"
	"path/filepath"
	"testing"
)

func withFlagSet(t *testing.T, fn func()) {
	t.Helper()
	old := flag.CommandLine
	flag.CommandLine = flag.NewFlagSet(t.Name(), flag.ContinueOnError)
	defer func() { flag.CommandLine = old }()
	fn()
}

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(body), 0644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

func TestLoadNormalizesSnakeCaseKeys(t *testing.T) {
	withFlagSet(t, func() {
		dataDir := flag.String("data-dir", "default", "")
		logLevel := flag.String("log-level", "info", "")
		path := writeConfig(t, "data_dir: /var/lib/nufs\nlog_level: debug\n")

		if err := Load(path); err != nil {
			t.Fatalf("Load: %v", err)
		}
		if *dataDir != "/var/lib/nufs" {
			t.Fatalf("data-dir not loaded: %q", *dataDir)
		}
		if *logLevel != "debug" {
			t.Fatalf("log-level not loaded: %q", *logLevel)
		}
	})
}

func TestLoadAppliesNestedAliases(t *testing.T) {
	withFlagSet(t, func() {
		raft := flag.Bool("raft", false, "")
		raftAddr := flag.String("raft-addr", "", "")
		raftDir := flag.String("raft-dir", "", "")
		raftBootstrap := flag.Bool("raft-bootstrap", false, "")
		path := writeConfig(t, `raft:
  enabled: true
  listen: 0.0.0.0:7000
  data_dir: /var/lib/nufs/raft
  bootstrap: true
`)

		if err := Load(path); err != nil {
			t.Fatalf("Load: %v", err)
		}
		if !*raft {
			t.Fatal("raft alias not loaded")
		}
		if *raftAddr != "0.0.0.0:7000" {
			t.Fatalf("raft-addr not loaded: %q", *raftAddr)
		}
		if *raftDir != "/var/lib/nufs/raft" {
			t.Fatalf("raft-dir not loaded: %q", *raftDir)
		}
		if !*raftBootstrap {
			t.Fatal("raft-bootstrap not loaded")
		}
	})
}

func TestLoadIgnoresUnknownKeys(t *testing.T) {
	withFlagSet(t, func() {
		known := flag.String("known", "default", "")
		path := writeConfig(t, "known: loaded\nunknown_key: ignored\n")

		if err := Load(path); err != nil {
			t.Fatalf("Load: %v", err)
		}
		if *known != "loaded" {
			t.Fatalf("known not loaded: %q", *known)
		}
	})
}

func TestLoadDatanodeConfigEndToEnd(t *testing.T) {
	withFlagSet(t, func() {
		dataDir := flag.String("data-dir", "default", "")
		listen := flag.String("listen", "", "")
		opsAddr := flag.String("ops-addr", "", "")
		rack := flag.String("rack", "", "")
		zone := flag.String("zone", "", "")
		capacity := flag.Int64("capacity", 0, "")
		nodeID := flag.String("node-id", "", "")
		traceEnabled := flag.Bool("trace-enabled", false, "")

		path := writeConfig(t, `
listen: "0.0.0.0:9100"
node_id: "42"
data_dir: "/var/lib/dfs/data"
metadata: "metad:8091"
ops_addr: "0.0.0.0:8092"
rack: "rack1"
zone: "zone1"
capacity_gb: 2000
log_level: "info"
trace_enabled: true
trace_endpoint: "otel:4317"
`)

		if err := Load(path); err != nil {
			t.Fatalf("Load: %v", err)
		}
		if *dataDir != "/var/lib/dfs/data" {
			t.Fatalf("data-dir: %q", *dataDir)
		}
		if *listen != "0.0.0.0:9100" {
			t.Fatalf("listen: %q", *listen)
		}
		if *opsAddr != "0.0.0.0:8092" {
			t.Fatalf("ops-addr: %q", *opsAddr)
		}
		if *rack != "rack1" {
			t.Fatalf("rack: %q", *rack)
		}
		if *zone != "zone1" {
			t.Fatalf("zone: %q", *zone)
		}
		if *capacity != 2000 {
			t.Fatalf("capacity: %d", *capacity)
		}
		if *nodeID != "42" {
			t.Fatalf("node-id: %q", *nodeID)
		}
		if !*traceEnabled {
			t.Fatal("trace-enabled not loaded")
		}
	})
}
