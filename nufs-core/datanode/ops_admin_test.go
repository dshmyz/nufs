package datanode

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestCapacityLevelForUsage exercises the pure threshold logic shared by the
// V2.1 alert poll and the admin/capacity endpoints.
func TestCapacityLevelForUsage(t *testing.T) {
	cases := []struct {
		pct  float64
		want AlertLevel
	}{
		{0.0, AlertNone},
		{0.50, AlertNone},
		{0.749, AlertNone},
		{0.75, AlertWarn},
		{0.80, AlertWarn},
		{0.849, AlertWarn},
		{0.85, AlertCritical},
		{0.99, AlertCritical},
	}
	for _, c := range cases {
		if got := capacityLevelForUsage(c.pct); got != c.want {
			t.Errorf("capacityLevelForUsage(%g) = %s, want %s", c.pct, got, c.want)
		}
	}
}

// TestOpsWebhook_AlertDelivery proves capacity-alert transitions are delivered
// to the configured webhook as JSON (async) and retained in the admin ring,
// with per-level de-duplication.
func TestOpsWebhook_AlertDelivery(t *testing.T) {
	var (
		mu    sync.Mutex
		posts []map[string]interface{}
	)
	webhook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var ev map[string]interface{}
		_ = json.NewDecoder(r.Body).Decode(&ev)
		mu.Lock()
		posts = append(posts, ev)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer webhook.Close()

	s, _ := newV2OpsServer(t)
	s.SetAlertWebhook(webhook.URL)

	// Warn transition → 1 POST.
	s.NotifyCapacityAlert(AlertWarn, 0.8, 800, 1000)
	// Critical transition → 2nd POST.
	s.NotifyCapacityAlert(AlertCritical, 0.9, 900, 1000)
	// Same critical level (level nudges up) → de-duped, no 3rd POST.
	s.NotifyCapacityAlert(AlertCritical, 0.91, 910, 1000)

	// Delivery is async; wait for both dispatches to complete.
	deadline := time.Now().Add(2 * time.Second)
	for {
		mu.Lock()
		n := len(posts)
		mu.Unlock()
		if n >= 2 || time.Now().After(deadline) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(posts) != 2 {
		t.Fatalf("webhook received %d posts, want 2 (level changes only)", len(posts))
	}
	if posts[0]["level"] != "warn" {
		t.Errorf("post[0].level = %v, want warn", posts[0]["level"])
	}
	if posts[1]["level"] != "critical" {
		t.Errorf("post[1].level = %v, want critical", posts[1]["level"])
	}
	if posts[1]["node_id"] != float64(7) {
		t.Errorf("post[1].node_id = %v, want 7", posts[1]["node_id"])
	}
	if posts[1]["usage_pct"] != 0.9 {
		t.Errorf("post[1].usage_pct = %v, want 0.9", posts[1]["usage_pct"])
	}

	// Ring retains exactly the two transitions (dedup'd).
	ring := s.alertRingSnapshot()
	if len(ring) != 2 {
		t.Fatalf("ring len = %d, want 2", len(ring))
	}
}

// TestOpsWebhook_NoWebhookNoPanic ensures with no webhook configured alert
// transitions still ring-buffer and do not panic or hang.
func TestOpsWebhook_NoWebhookNoPanic(t *testing.T) {
	s, _ := newV2OpsServer(t)
	s.SetAlertWebhook("")
	for _, lvl := range []AlertLevel{AlertWarn, AlertCritical, AlertCritical, AlertNone} {
		s.NotifyCapacityAlert(lvl, 0.8, 800, 1000)
	}
	// warn, critical, and the clear (none) are all level transitions; the
	// repeated critical is de-duped.
	if got := len(s.alertRingSnapshot()); got != 3 {
		t.Fatalf("ring len = %d, want 3", got)
	}
}

// TestOpsAdminPage verifies the datanode admin web view is served and the JSON
// alert feed reports real V2.1 state (the fix for the old always-empty/"none"
// gap): the capacity overview aggregates the V2Store's actual used bytes with a
// Statfs-derived total, so the feed is populated rather than hardcoded empty.
func TestOpsAdminPage(t *testing.T) {
	_, dispatch := newV2OpsServer(t)
	rec := dispatch(http.MethodGet, "/admin/")
	if rec.Code != http.StatusOK {
		t.Fatalf("/admin/ code=%d, want 200 (body: %s)", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "Datanode") {
		t.Errorf("/admin/ does not contain 'Datanode'")
	}

	// The admin JSON feed carries the tied capacity state (node identity + a
	// populated overview from the real V2Store — 2 chunks × 4 bytes written by
	// the helper).
	rec = dispatch(http.MethodGet, "/api/v1/admin/alerts")
	if rec.Code != http.StatusOK {
		t.Fatalf("/api/v1/admin/alerts code=%d, want 200", rec.Code)
	}
	var feed struct {
		NodeID   uint64          `json:"node_id"`
		Level    string          `json:"level"`
		Version  json.RawMessage `json:"version"`
		Overview struct {
			UsedBytes int64   `json:"used_bytes"`
			UsagePct  float64 `json:"usage_pct"`
		} `json:"overview"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &feed); err != nil {
		t.Fatalf("unmarshal admin feed: %v", err)
	}
	if feed.NodeID != 7 {
		t.Errorf("admin feed node_id = %d, want 7", feed.NodeID)
	}
	if feed.Level == "" {
		t.Errorf("admin feed level is empty")
	}
	if feed.Overview.UsedBytes != 8 {
		t.Errorf("admin feed overview.used_bytes = %d, want 8 (real V2Store bytes)", feed.Overview.UsedBytes)
	}

	// The legacy capacity/alerts endpoint now aggregates real V2.1 bytes with a
	// computed total + level instead of the old empty-stat "none" gap.
	rec = dispatch(http.MethodGet, "/api/v1/capacity/alerts")
	if rec.Code != http.StatusOK {
		t.Fatalf("/api/v1/capacity/alerts code=%d, want 200", rec.Code)
	}
	var capResp struct {
		AlertLevel string `json:"alert_level"`
		UsedBytes  int64  `json:"used_bytes"`
		TotalBytes int64  `json:"total_bytes"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &capResp); err != nil {
		t.Fatalf("unmarshal capacity: %v", err)
	}
	if capResp.AlertLevel == "" {
		t.Errorf("capacity alert_level is empty")
	}
	if capResp.UsedBytes != 8 {
		t.Errorf("capacity used_bytes = %d, want 8 (V2.1 gap fix)", capResp.UsedBytes)
	}
	if capResp.TotalBytes <= 0 {
		t.Errorf("capacity total_bytes = %d, want > 0 (Statfs-derived)", capResp.TotalBytes)
	}
}

// TestCheckAndNotifyCapacityNoDisk proves the V2.1 poll path runs with a nil
// DiskManager (V2.1 engine) without panicking — the guard that fixes an empty
// overview being misread.
func TestCheckAndNotifyCapacityNoDisk(t *testing.T) {
	s, _ := newV2OpsServer(t)
	// V2.1: disk is nil; reading a near-empty real temp fs yields usage < warn,
	// so the poll records a none transition without error.
	s.CheckAndNotifyCapacity()
	if lvl := s.currentAlertLevel(); lvl != AlertNone {
		t.Fatalf("currentAlertLevel = %s, want none on empty store", lvl)
	}
}
