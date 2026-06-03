// metad is the metadata service daemon for the distributed storage system.
// It uses Pebble as the storage engine with optional Raft consensus.
package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/example/dfs/internal/config"
	"github.com/example/dfs/internal/logging"
	"github.com/example/dfs/metadata"
)

func main() {
	var (
		configPath    = flag.String("config", "", "Path to YAML config file")
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
		logLevel      = flag.String("log-level", "info", "Log level (debug/info/warn/error)")
		logJSON       = flag.Bool("log-json", false, "JSON log output")
	)
	_ = configPath
	config.Preload()
	flag.Parse()

	logging.Init(logging.Config{Level: *logLevel, JSON: *logJSON, AddSource: true})
	log := logging.Named("metad")
	log.Info("starting metadata service", "node_id", *nodeID, "data", *dataDir)
	log.Info("runtime", "go", runtime.Version(), "os", runtime.GOOS, "arch", runtime.GOARCH)

	pebbleCfg := metadata.PebbleStoreConfig{
		Dir:          *dataDir,
		CacheDir:     *cacheDir,
		NodeID:       *nodeID,
		MemTableSize: *memTableSize,
	}

	store, err := metadata.NewPebbleStore(pebbleCfg)
	if err != nil {
		log.Error("failed to create PebbleStore", "error", err)
		os.Exit(1)
	}
	log.Info("PebbleStore initialized", "dir", *dataDir)

	var raftNode *metadata.RaftNode

	if *enableRaft {
		raftCfg := metadata.RaftNodeConfig{
			NodeID:            fmt.Sprintf("meta-%d", *nodeID),
			BindAddr:          *raftAddr,
			RaftDir:           *raftDir,
			Bootstrap:         *raftBootstrap,
			SnapshotThreshold: 8192,
			SnapshotInterval:  2 * time.Minute,
			TrailingLogs:      10240,
		}

		raftNode, err = metadata.NewRaftNode(store, raftCfg)
		if err != nil {
			log.Error("failed to create Raft node", "error", err)
			os.Exit(1)
		}
		store.SetRaftNode(raftNode)
		log.Info("Raft node started", "addr", *raftAddr, "bootstrap", *raftBootstrap)

		for i := 0; i < 30; i++ {
			if store.IsLeader() {
				log.Info("this node is the Raft leader")
				break
			}
			time.Sleep(time.Second)
		}
	} else {
		log.Info("running in single-node mode (Raft disabled)")
	}

	opts := []metadata.ServiceOption{
		metadata.WithLeaseTTL(*leaseTTL),
		metadata.WithGCInterval(*gcInterval),
		metadata.WithGCDryRun(*gcDryRun),
		metadata.WithScrubInterval(*scrubInterval),
	}

	bundle, err := metadata.NewPebbleServiceBundle(store, opts...)
	if err != nil {
		log.Error("failed to create service bundle", "error", err)
		os.Exit(1)
	}
	defer bundle.Close()

	bundle.Raft = raftNode

	log.Info("service bundle initialized")

	mux := http.NewServeMux()
	registerOpsHandlers(mux, store, bundle)

	admin := newAdminServer(store, bundle)
	admin.RegisterRoutes(mux)

	opsServer := &http.Server{
		Addr:         *opsAddr,
		Handler:      mux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	go func() {
		log.Info("ops API listening", "addr", *opsAddr)
		if err := opsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("ops server error", "error", err)
			os.Exit(1)
		}
	}()

	log.Info("metadata service ready")

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	log.Info("received signal, shutting down", "signal", sig)

	if raftNode != nil {
		if err := raftNode.TriggerSnapshot(); err != nil {
			log.Warn("snapshot failed", "error", err)
		}
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutdownCancel()
	if err := opsServer.Shutdown(shutdownCtx); err != nil {
		log.Warn("ops shutdown error", "error", err)
	}

	log.Info("shutdown complete")
}
