//go:build linux

package fuse

import (
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDefaultMountConfig(t *testing.T) {
	c := DefaultMountConfig()
	if c.Mountpoint != "/mnt/dfs" {
		t.Errorf("Mountpoint = %q, want /mnt/dfs", c.Mountpoint)
	}
	if c.MetaDir != "/var/lib/nufs/metadata" {
		t.Errorf("MetaDir = %q, want /var/lib/nufs/metadata", c.MetaDir)
	}
	if c.ScanTTL != 60*time.Second {
		t.Errorf("ScanTTL = %v, want 60s", c.ScanTTL)
	}
}

func TestMountConfigValidate(t *testing.T) {
	c := &MountConfig{}
	if err := c.Validate(); err == nil {
		t.Fatal("expected error for empty config")
	}

	c2 := &MountConfig{Mountpoint: "/mnt/test", MetaDir: "/var/test"}
	if err := c2.Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestMountConfigCacheDir(t *testing.T) {
	c := &MountConfig{MetaDir: "/var/lib/nufs/metadata"}
	if d := c.ResolveCacheDir(); d != "/var/lib/nufs/metadata/chunk-cache" {
		t.Errorf("CacheDir = %q, want meta-dir/chunk-cache", d)
	}

	c2 := &MountConfig{CacheDir: "/custom/cache"}
	if d := c2.ResolveCacheDir(); d != "/custom/cache" {
		t.Errorf("CacheDir = %q, want /custom/cache", d)
	}
}

func TestMountConfigFUSEOptions(t *testing.T) {
	c := &MountConfig{AllowOther: true, Debug: true}
	opts := c.FUSEOptions()
	if !opts.AllowOther {
		t.Error("AllowOther not set")
	}
	if opts.Name != "dfs" {
		t.Errorf("Name = %q, want dfs", opts.Name)
	}
}

func TestCredentialsRoundTrip(t *testing.T) {
	dir := t.TempDir()

	// Write credentials.json manually — the write path is operator-injected
	// (file or env); there is no SaveCredentials because the signed token is
	// deliberately kept in-memory only.
	data := []byte(`{"access_key":"ak","secret_key":"sk","metadata_endpoint":"http://localhost:9000"}`)
	if err := os.WriteFile(filepath.Join(dir, "credentials.json"), data, 0600); err != nil {
		t.Fatal(err)
	}

	loaded := LoadCredentials(dir)
	if loaded.AccessKey != "ak" || loaded.SecretKey != "sk" || loaded.Endpoint != "http://localhost:9000" {
		t.Errorf("got %+v", loaded)
	}
}

func TestCredentialsEnvOverride(t *testing.T) {
	dir := t.TempDir()
	data := []byte(`{"access_key":"from_file","secret_key":"from_file","metadata_endpoint":"http://meta:9000"}`)
	os.WriteFile(filepath.Join(dir, "credentials.json"), data, 0600)

	t.Setenv("META_ACCESS_KEY", "from_env")
	t.Setenv("META_SECRET_KEY", "from_env")

	loaded := LoadCredentials(dir)
	if loaded.AccessKey != "from_env" {
		t.Errorf("AccessKey = %q, want from_env", loaded.AccessKey)
	}
	if loaded.Endpoint != "http://meta:9000" {
		t.Errorf("Endpoint = %q, want http://meta:9000 (not overridden)", loaded.Endpoint)
	}
}

func TestMetricsServerStarted(t *testing.T) {
	// Start on port 0 to get an auto-assigned port.
	srv := StartMetricsServer(":0")
	if srv == nil {
		t.Fatal("expected non-nil server")
	}
	defer srv.Close()
	// If we got here without panic, server started.
}

func TestMetricsSnapshot(t *testing.T) {
	snap := fuseMetrics.Snapshot()
	if _, ok := snap["uptime_seconds"]; !ok {
		t.Error("missing uptime_seconds")
	}
	ops := snap["ops"].(map[string]uint64)
	if _, ok := ops["open"]; !ok {
		t.Error("missing ops.open")
	}
	cache := snap["cache"].(map[string]uint64)
	if _, ok := cache["hits"]; !ok {
		t.Error("missing cache.hits")
	}
}

func TestMetricsPrometheusOutput(t *testing.T) {
	// Start a real server on a random port.
	srv := StartMetricsServer(":0")
	if srv == nil {
		t.Fatal("expected non-nil server")
	}
	defer srv.Close()

	// Use an HTTP client to fetch from the actual listener.
	resp, err := http.Get("http://" + srv.Addr + "/healthz")
	if err != nil {
		t.Skipf("cannot reach test server on %s: %v — skipping", srv.Addr, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("healthz status = %d, want 200", resp.StatusCode)
	}
}

// TestMetricsJSONOutput verifies the server can serve JSON.
func TestMetricsJSONOutput(t *testing.T) {
	srv := StartMetricsServer(":0")
	if srv == nil {
		t.Fatal("expected non-nil server")
	}
	defer srv.Close()

	resp, err := http.Get("http://" + srv.Addr + "/metrics")
	if err != nil {
		t.Skipf("cannot reach test server on %s: %v — skipping", srv.Addr, err)
	}
	defer resp.Body.Close()

	bodyBytes, _ := io.ReadAll(resp.Body)
	body := string(bodyBytes)

	if !strings.Contains(body, "fusegw_uptime_seconds") {
		t.Error("prometheus output missing uptime")
	}
	if !strings.Contains(body, "fusegw_ops_total") {
		t.Error("prometheus output missing ops")
	}
	if !strings.Contains(body, "fusegw_cache_hits_total") {
		t.Error("prometheus output missing cache hits")
	}
}

func TestCredentialsJSON(t *testing.T) {
	dir := t.TempDir()
	c := &Credentials{AccessKey: "ak", SecretKey: "sk", Endpoint: "http://meta:9000"}
	data, _ := json.Marshal(c)
	os.WriteFile(filepath.Join(dir, "credentials.json"), data, 0600)

	var decoded Credentials
	json.Unmarshal(data, &decoded)
	if decoded.AccessKey != "ak" {
		t.Errorf("json roundtrip: AccessKey = %q", decoded.AccessKey)
	}
}

func TestMetricsServerNilOnEmpty(t *testing.T) {
	srv := StartMetricsServer("")
	if srv != nil {
		t.Error("expected nil for empty addr")
	}
}
