// metad is the metadata service daemon for the distributed storage system.
// It uses Pebble as the storage engine with optional Raft consensus.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/example/dfs/metadata"
)

func main() {
	var (
		dataDir       = flag.String("data-dir", "/var/lib/dfs/metadata", "Pebble data directory")
		cacheDir      = flag.String("cache-dir", "", "Pebble read cache directory (optional)")
		nodeID        = flag.Uint64("node-id", 1, "Metadata node ID (for chunk ID generation)")
		memTableSize  = flag.Uint64("memtable-size", 256<<20, "Pebble memtable size in bytes")
		enableRaft    = flag.Bool("raft", true, "Enable Raft consensus")
		raftAddr      = flag.String("raft-addr", "0.0.0.0:7000", "Raft bind address")
		raftDir       = flag.String("raft-dir", "/var/lib/dfs/raft", "Raft data directory")
		raftBootstrap = flag.Bool("raft-bootstrap", false, "Bootstrap a new Raft cluster")
		opsAddr       = flag.String("ops-addr", "0.0.0.0:8091", "Operations HTTP API address")
		leaseTTL      = flag.Duration("lease-ttl", 30*time.Second, "Node lease TTL")
		gcInterval    = flag.Duration("gc-interval", 10*time.Minute, "GC scan interval")
		gcDryRun      = flag.Bool("gc-dry-run", false, "GC dry-run mode (no deletes)")
		scrubInterval = flag.Duration("scrub-interval", 1*time.Hour, "Scrub interval")
	)
	flag.Parse()

	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Lshortfile)
	log.Printf("metad: starting metadata service (node_id=%d, data=%s)", *nodeID, *dataDir)
	log.Printf("metad: Go %s %s/%s", runtime.Version(), runtime.GOOS, runtime.GOARCH)

	// --- 1. Create PebbleStore (single instance) ---
	pebbleCfg := metadata.PebbleStoreConfig{
		Dir:          *dataDir,
		CacheDir:     *cacheDir,
		NodeID:       *nodeID,
		MemTableSize: *memTableSize,
	}

	store, err := metadata.NewPebbleStore(pebbleCfg)
	if err != nil {
		log.Fatalf("metad: failed to create PebbleStore: %v", err)
	}
	log.Printf("metad: PebbleStore initialized (dir=%s)", *dataDir)

	// --- 2. Configure Raft (uses the same PebbleStore) ---
	var raftNode *metadata.RaftNode

	if *enableRaft {
		raftCfg := metadata.RaftNodeConfig{
			NodeID:             fmt.Sprintf("meta-%d", *nodeID),
			BindAddr:           *raftAddr,
			RaftDir:            *raftDir,
			Bootstrap:          *raftBootstrap,
			SnapshotThreshold:  8192,
			SnapshotInterval:   2 * time.Minute,
			TrailingLogs:       10240,
		}

		raftNode, err = metadata.NewRaftNode(store, raftCfg)
		if err != nil {
			log.Fatalf("metad: failed to create Raft node: %v", err)
		}
		store.SetRaftNode(raftNode)
		log.Printf("metad: Raft node started (addr=%s, bootstrap=%v)", *raftAddr, *raftBootstrap)

		// Wait for leadership
		for i := 0; i < 30; i++ {
			if store.IsLeader() {
				log.Printf("metad: this node is the Raft leader")
				break
			}
			time.Sleep(time.Second)
		}
	} else {
		log.Printf("metad: running in single-node mode (Raft disabled)")
	}

	// --- 3. Create production service bundle (wraps the single PebbleStore) ---
	opts := []metadata.ServiceOption{
		metadata.WithLeaseTTL(*leaseTTL),
		metadata.WithGCInterval(*gcInterval),
		metadata.WithGCDryRun(*gcDryRun),
		metadata.WithScrubInterval(*scrubInterval),
	}

	bundle, err := metadata.NewPebbleServiceBundle(store, opts...)
	if err != nil {
		log.Fatalf("metad: failed to create service bundle: %v", err)
	}
	defer bundle.Close()

	// Set Raft reference on bundle (needed for health checks)
	bundle.Raft = raftNode

	log.Printf("metad: service bundle initialized")

	// --- 4. Start operations HTTP API ---
	mux := http.NewServeMux()
	registerOpsHandlers(mux, store, bundle)

	opsServer := &http.Server{
		Addr:         *opsAddr,
		Handler:      mux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	go func() {
		log.Printf("metad: ops API listening on %s", *opsAddr)
		if err := opsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("metad: ops server error: %v", err)
		}
	}()

	log.Printf("metad: metadata service ready")

	// --- 5. Wait for shutdown ---
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	log.Printf("metad: received signal %v, shutting down", sig)

	// Trigger Raft snapshot before shutdown
	if raftNode != nil {
		if err := raftNode.TriggerSnapshot(); err != nil {
			log.Printf("metad: snapshot warning: %v", err)
		}
	}

	// Graceful shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutdownCancel()
	if err := opsServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("metad: ops shutdown error: %v", err)
	}

	log.Printf("metad: shutdown complete")
}

// registerOpsHandlers sets up the operations API endpoints.
func registerOpsHandlers(mux *http.ServeMux, store *metadata.PebbleStore, bundle *metadata.ServiceBundle) {
	// Health endpoints
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		if bundle.IsReady() {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"status":"healthy"}`))
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte(`{"status":"initializing"}`))
		}
	})

	mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		if bundle.IsReady() {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	})

	// Metadata operations
	mux.HandleFunc("/api/v1/buckets", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			buckets, err := store.ListBuckets(r.Context())
			if err != nil {
				writeJSONError(w, http.StatusInternalServerError, err.Error())
				return
			}
			writeJSON(w, buckets)
		default:
			writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	})

	mux.HandleFunc("/api/v1/nodes", func(w http.ResponseWriter, r *http.Request) {
		nodes, err := store.ListNodes(r.Context())
		if err != nil {
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, nodes)
	})

	mux.HandleFunc("/api/v1/cluster/status", func(w http.ResponseWriter, r *http.Request) {
		status := map[string]interface{}{
			"is_leader":  store.IsLeader(),
			"version":    "0.2.0",
			"leader_uri": store.LeaderAddr(),
		}
		writeJSON(w, status)
	})

	mux.HandleFunc("/api/v1/metrics", func(w http.ResponseWriter, r *http.Request) {
		if bundle.Metrics != nil {
			writeJSON(w, bundle.Metrics.Snapshot())
		} else {
			writeJSON(w, map[string]string{"status": "no metrics"})
		}
	})
}

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.Encode(v)
}

func writeJSONError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
