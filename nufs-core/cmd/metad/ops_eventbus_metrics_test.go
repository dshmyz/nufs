package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/example/dfs/metadata"
)

// The metadata change-notification bus can silently drop events when a
// watcher's buffer fills up. Those drops are the one observability gap that
// was otherwise invisible — assert the /metrics endpoint now exposes them so
// they can be alerted on.
func TestPrometheusMetricsExposesEventBusDrops(t *testing.T) {
	store, bundle := newOpsTestStore(t)

	eb := store.Events()
	if eb == nil {
		t.Fatal("expected store to have an attached event bus")
	}

	// A slow watcher with a small (256-event) buffer. Publish is non-blocking,
	// so firing well past the buffer guarantees at least one event is dropped —
	// exactly the condition the new counter exists to surface.
	watcher := eb.Watch("nufs:")
	defer watcher.Close()
	for i := 0; i < 512; i++ {
		eb.Publish(metadata.Event{Key: "nufs:/ops", Value: []byte{1}, Type: metadata.EventSet})
	}

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rr := httptest.NewRecorder()
	prometheusMetricsHandler(store, bundle.Metrics).ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status code = %d body=%s", rr.Code, rr.Body.String())
	}
	body := rr.Body.String()
	if !strings.Contains(body, "# HELP nufs_events_dropped_total Metadata change events dropped due to full watcher buffer") {
		t.Fatalf("missing nufs_events_dropped_total HELP:\n%s", body)
	}
	if !strings.Contains(body, "# TYPE nufs_events_dropped_total counter") {
		t.Fatalf("missing nufs_events_dropped_total TYPE:\n%s", body)
	}
	if !strings.Contains(body, "nufs_events_published_total ") {
		t.Fatalf("missing nufs_events_published_total metric:\n%s", body)
	}
	if !strings.Contains(body, "nufs_events_watcher_count ") {
		t.Fatalf("missing nufs_events_watcher_count metric:\n%s", body)
	}
}

// With no watchers attached, no events can be dropped and the counters must
// still render (so the metric family exists even in quiet clusters).
func TestPrometheusMetricsEventBusPresentWhenIdle(t *testing.T) {
	store, bundle := newOpsTestStore(t)
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rr := httptest.NewRecorder()
	prometheusMetricsHandler(store, bundle.Metrics).ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status code = %d body=%s", rr.Code, rr.Body.String())
	}
	for _, family := range []string{
		"nufs_events_dropped_total",
		"nufs_events_published_total",
		"nufs_events_watcher_count",
	} {
		if got := strings.Count(rr.Body.String(), "# HELP "+family+" "); got != 1 {
			t.Fatalf("%s HELP count = %d, want 1:\n%s", family, got, rr.Body.String())
		}
	}
}
