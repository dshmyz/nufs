//go:build linux

package fuse

import (
	"fmt"
	"net/http"
	"sync/atomic"
	"time"
)

type FUSEMetrics struct {
	OpsOpen    uint64
	OpsRead    uint64
	OpsWrite   uint64
	OpsFlush   uint64
	OpsRelease uint64
	OpsLookup  uint64
	OpsReadDir uint64
	OpsCreate  uint64
	OpsMkdir   uint64
	OpsRemove  uint64
	OpsRename  uint64

	CacheHits   uint64
	CacheMisses uint64

	startTime time.Time
}

var fuseMetrics = &FUSEMetrics{startTime: time.Now()}

func (m *FUSEMetrics) Snapshot() map[string]interface{} {
	return map[string]interface{}{
		"uptime_seconds": time.Since(m.startTime).Seconds(),
		"ops": map[string]uint64{
			"open":    atomic.LoadUint64(&m.OpsOpen),
			"read":    atomic.LoadUint64(&m.OpsRead),
			"write":   atomic.LoadUint64(&m.OpsWrite),
			"flush":   atomic.LoadUint64(&m.OpsFlush),
			"release": atomic.LoadUint64(&m.OpsRelease),
			"lookup":  atomic.LoadUint64(&m.OpsLookup),
			"readdir": atomic.LoadUint64(&m.OpsReadDir),
			"create":  atomic.LoadUint64(&m.OpsCreate),
			"mkdir":   atomic.LoadUint64(&m.OpsMkdir),
			"remove":  atomic.LoadUint64(&m.OpsRemove),
			"rename":  atomic.LoadUint64(&m.OpsRename),
		},
		"cache": map[string]uint64{
			"hits":  atomic.LoadUint64(&m.CacheHits),
			"misses": atomic.LoadUint64(&m.CacheMisses),
		},
	}
}

func StartMetricsServer(addr string) *http.Server {
	if addr == "" {
		return nil
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		snap := fuseMetrics.Snapshot()
		fmt.Fprintf(w, "# HELP fusegw_uptime_seconds Uptime.\n# TYPE fusegw_uptime_seconds gauge\nfusegw_uptime_seconds %g\n\n",
			snap["uptime_seconds"])

		ops := snap["ops"].(map[string]uint64)
		for op, count := range ops {
			fmt.Fprintf(w, "fusegw_ops_total{op=\"%s\"} %d\n", op, count)
		}

		cache := snap["cache"].(map[string]uint64)
		fmt.Fprintf(w, "\nfusegw_cache_hits_total %d\nfusegw_cache_misses_total %d\n",
			cache["hits"], cache["misses"])
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "ok")
	})
	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		_ = srv.ListenAndServe()
	}()
	return srv
}
