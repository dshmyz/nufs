package metadata

import (
	"context"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestPrometheusHandler_NodeAndChunkGauges guards against a regression where
// the nufs_nodes_online / nufs_nodes_total / nufs_chunks_total gauges path was
// wired to Metrics atomics that nothing ever wrote, so they were permanently 0
// and the datanode_availability / chunk_durability SLOs (and their alerts)
// evaluated against no data. ComputeClusterReadiness must populate them, and
// PrometheusHandler must then emit the real values.
func TestPrometheusHandler_NodeAndChunkGauges(t *testing.T) {
	store := newTestPebbleStore(t)
	ctx := context.Background()

	for i := NodeID(1); i <= 3; i++ {
		if err := store.RegisterNode(ctx, &NodeInfo{
			ID:    i,
			Addr:  "127.0.0.1:900" + string(rune('0'+i)),
			State: NodeOnline,
		}); err != nil {
			t.Fatalf("RegisterNode %d: %v", i, err)
		}
	}
	chunk := &ChunkMeta{
		ID:       100,
		Size:     64 * 1024 * 1024,
		State:    ChunkReady,
		Replicas: []ReplicaInfo{{NodeID: 1, State: ReplicaReady}, {NodeID: 2, State: ReplicaReady}, {NodeID: 3, State: ReplicaReady}},
	}
	if err := store.putJSON(prefixChunk+"100", chunk); err != nil {
		t.Fatalf("put chunk: %v", err)
	}

	metrics := NewMetrics()
	hc := NewHealthChecker(store, nil, metrics, "test")

	// Emit BEFORE computing readiness: gauges must be 0 (proves the test is
	// detecting real data flow, not accidental seeding).
	before := renderPrometheus(t, metrics)
	if !gaugeEquals(before, "nufs_nodes_online", 0) {
		t.Fatalf("pre-readiness nufs_nodes_online must be 0, got:\n%s", before)
	}

	// Compute readiness — this is what refreshes the atomics.
	hc.ComputeClusterReadiness()

	after := renderPrometheus(t, metrics)
	for metric, want := range map[string]int64{
		"nufs_nodes_online": 3,
		"nufs_nodes_total":  3,
		"nufs_chunks_total": 1,
	} {
		got, ok := findGauge(after, metric)
		if !ok {
			t.Errorf("%s metric missing after ComputeClusterReadiness:\n%s", metric, after)
			continue
		}
		if got != want {
			t.Errorf("%s = %d, want %d", metric, got, want)
		}
	}
}

func renderPrometheus(t *testing.T, m *Metrics) string {
	t.Helper()
	h := PrometheusHandler(m)
	req := httptest.NewRequest("GET", "/metrics", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec.Body.String()
}

func findGauge(exposition, name string) (int64, bool) {
	for _, line := range strings.Split(exposition, "\n") {
		if strings.HasPrefix(line, name+" ") {
			var v int64
			if _, err := fmt.Sscanf(line[len(name)+1:], "%d", &v); err != nil {
				return 0, false
			}
			return v, true
		}
	}
	return 0, false
}

func gaugeEquals(exposition, name string, want int64) bool {
	got, ok := findGauge(exposition, name)
	return ok && got == want
}
