package fuse

import (
	"fmt"
	"io"
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
		accept := r.Header.Get("Accept")
		if accept == "application/openmetrics-text" || r.URL.Query().Get("format") == "openmetrics" {
			w.Header().Set("Content-Type", "application/openmetrics-text; version=1.0.0; charset=utf-8")
			writeFUSEOpenMetrics(w)
			return
		}
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

func writeFUSEOpenMetrics(w io.Writer) {
	fmt.Fprintf(w, "# TYPE fusegw_uptime_seconds gauge\n# UNIT fusegw_uptime_seconds seconds\nfusegw_uptime_seconds %g\n\n",
		time.Since(fuseMetrics.startTime).Seconds())
	fmt.Fprintf(w, "# TYPE fusegw_ops_total counter\n# UNIT fusegw_ops_total operations\n")
	for _, op := range []string{"open", "read", "write", "flush", "release", "lookup", "readdir", "create", "mkdir", "remove", "rename"} {
		var v uint64
		switch op {
		case "open":
			v = atomic.LoadUint64(&fuseMetrics.OpsOpen)
		case "read":
			v = atomic.LoadUint64(&fuseMetrics.OpsRead)
		case "write":
			v = atomic.LoadUint64(&fuseMetrics.OpsWrite)
		case "flush":
			v = atomic.LoadUint64(&fuseMetrics.OpsFlush)
		case "release":
			v = atomic.LoadUint64(&fuseMetrics.OpsRelease)
		case "lookup":
			v = atomic.LoadUint64(&fuseMetrics.OpsLookup)
		case "readdir":
			v = atomic.LoadUint64(&fuseMetrics.OpsReadDir)
		case "create":
			v = atomic.LoadUint64(&fuseMetrics.OpsCreate)
		case "mkdir":
			v = atomic.LoadUint64(&fuseMetrics.OpsMkdir)
		case "remove":
			v = atomic.LoadUint64(&fuseMetrics.OpsRemove)
		case "rename":
			v = atomic.LoadUint64(&fuseMetrics.OpsRename)
		}
		fmt.Fprintf(w, "fusegw_ops_total{op=%q} %d\n", op, v)
	}
	fmt.Fprintf(w, "\n# TYPE fusegw_cache_hits_total counter\n# UNIT fusegw_cache_hits_total hits\nfusegw_cache_hits_total %d\n", atomic.LoadUint64(&fuseMetrics.CacheHits))
	fmt.Fprintf(w, "# TYPE fusegw_cache_misses_total counter\n# UNIT fusegw_cache_misses_total misses\nfusegw_cache_misses_total %d\n", atomic.LoadUint64(&fuseMetrics.CacheMisses))
	fmt.Fprintln(w, "# EOF")
}
