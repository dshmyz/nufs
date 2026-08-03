package main

import (
	"context"
	"flag"
	"fmt"
	"hash/fnv"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/example/dfs/datanode"
	"github.com/example/dfs/datanode/storage"
	"github.com/example/dfs/datanode/storage/encryption"
	"github.com/example/dfs/datanode/storage/segment"
	"github.com/example/dfs/internal/config"
	"github.com/example/dfs/internal/crypto"
	"github.com/example/dfs/internal/logging"
	"github.com/example/dfs/internal/tlsutil"
	"github.com/example/dfs/internal/tracing"
	"github.com/example/dfs/metadata"
)

func main() {
	// Management subcommands — skip flag.Parse entirely.
	if len(os.Args) >= 2 {
		switch os.Args[1] {
		case "status", "adopt", "retire", "decommission", "migrate", "drain", "verify", "config":
			runManagementCommand(os.Args[1], os.Args[2:])
			return
		}
	}

	var (
		configPath           = flag.String("config", "", "Path to YAML config file")
		nodeID               = flag.String("node-id", "auto", "Unique data node ID or 'auto'")
		listenAddr           = flag.String("listen", "0.0.0.0:9100", "TCP listen address")
		registerAddrFlag     = flag.String("register-addr", "", "Address registered with metadata (routable host:port; empty = listen addr)")
		opsAddr              = flag.String("ops-addr", "0.0.0.0:8091", "Operations HTTP API address")
		dataDir              = flag.String("data-dir", "/var/lib/dfs/data", "Chunk storage root directory")
		dataDirs             = flag.String("data-dirs", "", "Comma-separated data directories for JBOD multi-disk mode")
		machineID            = flag.String("machine-id", "", "Machine identifier for topology placement")
		metaAddr             = flag.String("metadata", "localhost:8091", "Metadata service HTTP address")
		rack                 = flag.String("rack", "rack-1", "Rack identifier for topology placement")
		zone                 = flag.String("zone", "zone-1", "Availability zone identifier")
		capacityGB           = flag.Int64("capacity", 1000, "Node storage capacity in GB")
		tlsCert              = flag.String("tls-cert", "", "TLS certificate file (enables TLS for chunk TCP + ops HTTP)")
		tlsKey               = flag.String("tls-key", "", "TLS private key file")
		tlsCA                = flag.String("tls-ca", "", "TLS CA certificate for mutual TLS (client verification)")
		tlsRequireClientCert = flag.Bool("tls-require-client-cert", false, "Require clients to present a certificate signed by tls-ca")
		tlsSkipVerify        = flag.Bool("tls-skip-verify", false, "Skip TLS server certificate verification (dev only)")
		metadataAuthToken    = flag.String("metadata-auth-token", "", "Bearer token for metadata service")
		opsAuthToken         = flag.String("ops-auth-token", "", "Bearer token for datanode ops API")
		enablePprof          = flag.Bool("pprof", false, "Expose /debug/pprof on ops API")
		traceEnabled         = flag.Bool("trace-enabled", false, "Enable OpenTelemetry tracing")
		traceEndpoint        = flag.String("trace-endpoint", "", "OTLP gRPC endpoint")
		traceInsecure        = flag.Bool("trace-insecure", true, "Use insecure OTLP connection")
		encryptAtRest        = flag.Bool("encrypt-at-rest", false, "Enable at-rest data encryption (AES-256-GCM)")
		allowLocalKMS        = flag.Bool("allow-local-kms", false, "Allow in-memory development KMS; not production safe")
		storageVersion       = flag.String("storage-version", "v1", "Storage engine version: v1 (legacy ChunkStore) or v2.1 (new engine)")
		logLevel             = flag.String("log-level", "info", "Log level (debug/info/warn/error)")
		logJSON              = flag.Bool("log-json", false, "JSON log output")
	)
	_ = configPath
	config.Preload()
	flag.Parse()
	logging.Init(logging.Config{Level: *logLevel, JSON: *logJSON, AddSource: true})
	log := logging.Named("datanode")

	var dirs []string
	if *dataDirs != "" {
		dirs = splitAndClean(*dataDirs)
		if len(dirs) == 0 {
			log.Error("data-dirs is empty")
			os.Exit(1)
		}
	}
	if len(dirs) == 0 && *dataDir == "" {
		log.Error("either --data-dir or --data-dirs must be set")
		os.Exit(1)
	}

	mid := *machineID
	if mid == "" {
		mid = readMachineID()
	}

	singleDir := *dataDir
	if len(dirs) == 0 {
		dirs = []string{singleDir}
	}

	log.Info("starting", "node_id", *nodeID, "addr", *listenAddr, "disks", dirs, "machine", mid)
	nid, err := resolveNodeID(*nodeID, resolveNodeIDPath(dirs[0]), mid)
	if err != nil {
		log.Error("invalid node-id", "node_id", *nodeID, "error", err)
		os.Exit(1)
	}
	runDataNode(datanode.Config{
		NodeID:              nid,
		ListenAddr:          *listenAddr,
		RegisterAddr:        *registerAddrFlag,
		OpsListenAddr:       *opsAddr,
		DataDir:             singleDir,
		DataDirs:            dirs,
		MetadataAddr:        *metaAddr,
		MetadataCacheDir:    "",
		HeartbeatInterval:   10 * time.Second,
		MaxConcurrentWrites: 64,
		MaxConcurrentReads:  256,
		Rack:                *rack,
		Zone:                *zone,
		MachineID:           mid,
		Tier:                metadata.TierHot,
		CapacityGB:          *capacityGB,
		TLS: tlsutil.Config{
			CertFile:          *tlsCert,
			KeyFile:           *tlsKey,
			CAFile:            *tlsCA,
			SkipVerify:        *tlsSkipVerify,
			RequireClientCert: *tlsRequireClientCert,
		},
		MetadataAuthToken: *metadataAuthToken,
		OpsAuthToken:      *opsAuthToken,
		EnablePprof:       *enablePprof,
		TraceEnabled:      *traceEnabled,
		TraceEndpoint:     *traceEndpoint,
		TraceInsecure:     *traceInsecure,
		EncryptAtRest:     *encryptAtRest,
		AllowLocalKMS:     *allowLocalKMS,
		StorageVersion:    *storageVersion,
		LogLevel:          *logLevel,
	})
}

func resolveNodeID(raw, idPath, machineID string) (metadata.NodeID, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" || strings.EqualFold(raw, "auto") {
		fallback := stableAutoNodeID(machineID)
		return loadOrAllocateNodeID(idPath, fallback), nil
	}
	id, err := strconv.ParseUint(raw, 10, 64)
	if err != nil || id == 0 {
		if err == nil {
			err = fmt.Errorf("must be greater than zero")
		}
		return 0, err
	}
	return metadata.NodeID(id), nil
}

// stableAutoNodeID derives a deterministic node ID from the machine ID.
// It no longer depends on the data directory path, so reordering or
// changing data dirs does not change the auto-assigned node ID.
func stableAutoNodeID(machineID string) metadata.NodeID {
	h := fnv.New64a()
	_, _ = h.Write([]byte(machineID))
	id := h.Sum64()
	if id == 0 {
		id = 1
	}
	return metadata.NodeID(id)
}

func splitAndClean(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// registerAddr returns the address registered with the metadata service:
// RegisterAddr when set (containerized deployments must register a
// routable host:port, not the 0.0.0.0 bind address), else ListenAddr.
func registerAddr(cfg datanode.Config) string {
	if cfg.RegisterAddr != "" {
		return cfg.RegisterAddr
	}
	return cfg.ListenAddr
}

func readMachineID() string {
	b, err := os.ReadFile("/etc/machine-id")
	if err == nil {
		return strings.TrimSpace(string(b))
	}
	b, err = os.ReadFile("/sys/class/dmi/id/product_uuid")
	if err == nil {
		return strings.TrimSpace(string(b))
	}
	logging.Named("datanode").Warn("could not read machine-id, using hostname")
	h, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return h
}

func runDataNode(cfg datanode.Config) {
	log := logging.Named("datanode")
	log.Info("starting data node", "node_id", cfg.NodeID, "addr", cfg.ListenAddr, "data", cfg.DataDir, "machine", cfg.MachineID)

	_, traceShutdown, err := tracing.Init(tracing.Config{
		Enabled:  cfg.TraceEnabled,
		Endpoint: cfg.TraceEndpoint,
		Service:  "datanode",
		Insecure: cfg.TraceInsecure,
	})
	if err != nil {
		log.Error("failed to initialize tracing", "error", err)
		os.Exit(1)
	}

	// Resolve the disk set: DataDirs (JBOD multi-disk) or fall back to a
	// single-element list from DataDir.
	dataDirs := cfg.DataDirs
	if len(dataDirs) == 0 {
		dataDirs = []string{cfg.DataDir}
	}

	if cfg.StorageVersion == "v2.1" {
		runDataNodeV21(cfg, dataDirs, log)
		return
	}

	// === V1 (legacy ChunkStore) path ===

	// One WAL per disk (each lives on its own disk for crash-recovery isolation).
	wals := make([]*datanode.WriteAheadLog, len(dataDirs))
	for i, dir := range dataDirs {
		w, err := datanode.NewWriteAheadLog(filepath.Join(dir, "wal"))
		if err != nil {
			log.Error("failed to init WAL", "disk", dir, "error", err)
			os.Exit(1)
		}
		wals[i] = w
	}

	chunkStore, err := datanode.NewMultiDiskChunkStore(dataDirs, cfg.MaxConcurrentWrites, cfg.MaxConcurrentReads, wals)
	if err != nil {
		log.Error("failed to init chunk store", "error", err)
		os.Exit(1)
	}

	// Configure at-rest encryption if enabled. LocalKMS is intentionally
	// fail-closed unless the operator explicitly opts into dev-only keys.
	if cfg.EncryptAtRest {
		if !cfg.AllowLocalKMS {
			log.Error("at-rest encryption requires a production KMS; LocalKMS is in-memory/dev-only and loses keys on restart", "hint", "set --allow-local-kms only for development")
			os.Exit(1)
		}
		kms, err := crypto.NewLocalKMS()
		if err != nil {
			log.Error("failed to init encryption KMS", "error", err)
			os.Exit(1)
		}
		chunkStore.SetEncryptor(crypto.NewEncryptor(kms))
		log.Warn("at-rest encryption enabled with LocalKMS; keys are in-memory and not production safe")
	}

	totalBytes, chunkCount := chunkStore.Stats()
	log.Info("chunk store ready", "disks", len(dataDirs), "chunks", chunkCount, "bytes", totalBytes)

	// Per-disk capacity: apply CapacityGB uniformly (0 = auto-detect via Statfs).
	capacities := make([]int64, len(dataDirs))
	for i := range dataDirs {
		capacities[i] = cfg.CapacityGB
	}
	diskManager, err := datanode.NewMultiDiskManager(dataDirs, chunkStore, capacities, wals)
	if err != nil {
		log.Error("failed to init disk manager", "error", err)
		os.Exit(1)
	}
	diskManager.Start()

	// Wire disk health into chunk store so writes are rejected on disk failure
	chunkStore.SetDiskManager(diskManager)

	// Start the management socket for status/adopt/retire CLI commands.
	stopMgmt, err := startManagementServer(chunkStore, diskManager, dataDirs)
	if err != nil {
		log.Warn("failed to start management socket", "error", err)
	}
	if stopMgmt != nil {
		defer stopMgmt()
	}

	// Determine scheme for metadata service based on our TLS config.
	// If the cluster uses TLS, metad should also be accessed over HTTPS.
	metaScheme := "http"
	if cfg.TLS.Enabled() {
		metaScheme = "https"
	}
	if cfg.TLS.CAFile != "" && !cfg.TLS.RequireClientCert {
		log.Warn("tls CA configured but client certificates are optional; set --tls-require-client-cert for strict mTLS")
	}
	metaSvcAddr := fmt.Sprintf("%s://%s", metaScheme, cfg.MetadataAddr)
	metaStore := metadata.NewHTTPClient(metaSvcAddr, 30*time.Second)
	metaStore.SetAuthToken(cfg.MetadataAuthToken)

	// When connecting to a TLS-enabled metad, configure the HTTP
	// client transport to use our TLS client config.
	if cfg.TLS.Enabled() {
		if err := metaStore.EnableTLS(cfg.TLS); err != nil {
			log.Error("failed to configure TLS for metadata client", "error", err)
			os.Exit(1)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	err = metaStore.RegisterNode(ctx, &metadata.NodeInfo{
		ID:         cfg.NodeID,
		Addr:       registerAddr(cfg),
		DataDir:    cfg.DataDir,
		Rack:       cfg.Rack,
		Zone:       cfg.Zone,
		MachineID:  cfg.MachineID,
		Tier:       cfg.Tier,
		CapacityGB: cfg.CapacityGB,
	})
	cancel()
	if err != nil && err != metadata.ErrNodeAlreadyExists {
		log.Error("failed to register node", "error", err)
		os.Exit(1)
	}
	log.Info("registered with metadata service", "addr", cfg.MetadataAddr)

	server := datanode.NewServer(cfg, chunkStore)
	if err := server.Start(); err != nil {
		log.Error("failed to start server", "error", err)
		os.Exit(1)
	}

	replicator := datanode.NewReplicator(cfg.ListenAddr, 4)
	replicator.SetTLS(cfg.TLS)
	replicator.Start()

	chainRepl := datanode.NewParallelReplicator(cfg.ListenAddr, cfg.NodeID, 5*time.Second)
	chainRepl.SetTLS(cfg.TLS)

	repairWorker := datanode.NewRepairWorker(datanode.RepairConfig{
		Meta:       metaStore,
		NodeID:     cfg.NodeID,
		Interval:   30 * time.Second,
		Replicator: replicator,
		LocalAddr:  cfg.ListenAddr,
	})
	repairWorker.Start(context.Background())

	antiEntropy := datanode.NewAntiEntropy(chunkStore, metaStore, cfg.NodeID)
	antiEntropy.SetTLS(cfg.TLS)
	antiEntropy.Start(30 * time.Minute)

	heartbeat := datanode.NewHeartbeatReporter(cfg, metaStore, chunkStore)
	heartbeat.Start()

	opsServer := datanode.NewOpsServerWithRepair(cfg, chunkStore, metaStore, diskManager, chainRepl, antiEntropy, repairWorker)
	if err := opsServer.Start(); err != nil {
		log.Error("failed to start ops server", "error", err)
		os.Exit(1)
	}

	log.Info("all components started successfully")

	// SIGHUP handler for config hot-reload (log level changes)
	hupCh := make(chan os.Signal, 1)
	signal.Notify(hupCh, syscall.SIGHUP)
	go func() {
		for range hupCh {
			log.Info("received SIGHUP, reloading log level")
			logging.SetLevel(cfg.LogLevel)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	log.Info("received signal, shutting down", "signal", sig)

	// Phase 1: Stop accepting new client and ops connections.
	log.Info("shutdown phase 1: stopping ops and data servers")
	opsServer.Stop()
	server.Stop()

	// Phase 2: Stop background workers that generate writes.
	log.Info("shutdown phase 2: stopping background workers")
	repairWorker.Stop()
	antiEntropy.Stop()
	heartbeat.Stop()
	replicator.Stop()

	// Phase 3: Wait for in-flight writes to complete.
	log.Info("shutdown phase 3: draining in-flight writes")
	drainCtx, drainCancel := context.WithTimeout(context.Background(), 30*time.Second)
	releaseDrain, err := chunkStore.DrainWrites(drainCtx)
	if err != nil {
		log.Warn("drain timeout, some writes may be in-flight", "error", err)
	} else {
		log.Info("all in-flight writes drained")
	}
	if releaseDrain != nil {
		releaseDrain()
	}
	drainCancel()

	// Phase 4: Stop disk manager and close chunk-store resources.
	log.Info("shutdown phase 4: stopping disk manager and closing chunk store")
	diskManager.Stop()
	if err := chunkStore.Close(); err != nil {
		log.Warn("chunk store close error", "error", err)
	}

	// Phase 5: Flush and close WAL — ensures all committed writes are durable.
	log.Info("shutdown phase 5: flushing WAL")
	for _, w := range wals {
		if err := w.Close(); err != nil {
			log.Warn("WAL close error", "error", err)
		}
	}

	// Phase 6: Deregister from metadata service.
	log.Info("shutdown phase 6: deregistering from metadata service")
	deregCtx, deregCancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := metaStore.RegisterNode(deregCtx, &metadata.NodeInfo{
		ID:    cfg.NodeID,
		State: metadata.NodeOffline,
	}); err != nil {
		log.Warn("failed to mark node offline", "error", err)
	}
	deregCancel()

	traceCtx, traceCancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := traceShutdown(traceCtx); err != nil {
		log.Warn("tracing shutdown error", "error", err)
	}
	traceCancel()

	log.Info("shutdown complete")
}

// runDataNodeV21 initializes the V2.1 storage engine for each disk and
// starts the metadata client. It is the V2.1 replacement for the legacy
// ChunkStore path, activated by --storage-version=v2.1.
func runDataNodeV21(cfg datanode.Config, dataDirs []string, log *slog.Logger) {
	// Initialize one V2.1 segment.Store per disk.
	//
	// Shutdown has a single owner: closeStores below. Initialization
	// failures close what was already opened and exit; the normal path
	// closes the same set once, after the servers and heartbeat stop. A
	// deferred per-store close here would be a second owner and would not
	// run at all on the os.Exit paths.
	stores := make([]storage.Store, 0, len(dataDirs))
	closeStores := func() {
		for _, st := range stores {
			if closer, ok := st.(interface{ Close() error }); ok {
				if err := closer.Close(); err != nil {
					log.Warn("V2.1 store close error", "error", err)
				}
			}
		}
	}
	for _, dir := range dataDirs {
		segCfg := segment.Config{
			Dir:         dir,
			SegmentSize: storage.DefaultDataSegmentSize,
			UseMemIndex: false,
			StreamID:    1, // data stream (0 = small)
		}
		// Configure at-rest encryption if enabled.
		if cfg.EncryptAtRest {
			if !cfg.AllowLocalKMS {
				log.Error("at-rest encryption requires a production KMS; LocalKMS is in-memory/dev-only and loses keys on restart")
				closeStores()
				os.Exit(1)
			}
			kms, err := crypto.NewLocalKMS()
			if err != nil {
				log.Error("failed to init encryption KMS", "error", err)
				closeStores()
				os.Exit(1)
			}
			segCfg.Enc = encryption.NewKeyRegistry(kms)
		}
		s, err := segment.New(segCfg)
		if err != nil {
			log.Error("failed to init V2.1 store", "disk", dir, "error", err)
			closeStores()
			os.Exit(1)
		}
		stores = append(stores, s)
	}
	log.Info("V2.1 storage engine ready", "disks", len(dataDirs))

	// Set up the metadata client.
	metaScheme := "http"
	if cfg.TLS.Enabled() {
		metaScheme = "https"
	}
	metaURL := fmt.Sprintf("%s://%s", metaScheme, cfg.MetadataAddr)
	metaStore := metadata.NewHTTPClient(metaURL, 30*time.Second)
	metaStore.SetAuthToken(cfg.MetadataAuthToken)

	// Register with metadata service. On re-registration after a restart
	// the node already exists (metadata persists nodes across restarts);
	// treat ErrNodeAlreadyExists as success so the TCP server and
	// heartbeat still start. The address is refreshed via heartbeat.
	ctx := context.Background()
	if err := metaStore.RegisterNode(ctx, &metadata.NodeInfo{
		ID:      cfg.NodeID,
		Addr:    registerAddr(cfg),
		State:   metadata.NodeOnline,
	}); err != nil && err != metadata.ErrNodeAlreadyExists {
		log.Error("failed to register with metadata service", "error", err)
		closeStores()
		os.Exit(1)
	}
	log.Info("registered with metadata service", "url", metaURL, "node_id", cfg.NodeID)

	// Wrap all stores in a V2 adapter and start the TCP server. The
	// adapter aggregates every disk (least-used placement, per-disk stats
	// and heartbeat data), so a multi-disk --data-dirs node actually
	// serves from all of them rather than only the first.
	v2Store := datanode.NewMultiV2Store(stores, dataDirs...)
	srvCfg := cfg
	srvCfg.ListenAddr = cfg.ListenAddr
	srv := datanode.NewServer(srvCfg, v2Store)
	if err := srv.Start(); err != nil {
		log.Error("failed to start TCP server", "error", err)
		closeStores()
		os.Exit(1)
	}
	log.Info("TCP server listening", "addr", srv.Addr())

	// Start the heartbeat reporter so the metadata service's lease keeps
	// this node online and the placement engine selects it as a replica
	// (§6.3). Without it the node would be marked offline and never
	// receive writes.
	heartbeat := datanode.NewHeartbeatReporter(cfg, metaStore, v2Store)
	heartbeat.Start()

	// Operational channels: the unix-socket management server and the HTTP
	// ops server. Both are engine-agnostic (they hold the OpsStore subset, so
	// V2Store drives the same surface V1 exposes). V2.1 has no disk
	// lifecycle/replicator/anti-entropy/repair, so those capability handlers
	// answer "unsupported" and the V1-only subsystems are nil.
	stopMgmt, err := startManagementServer(v2Store, nil, dataDirs)
	if err != nil {
		log.Error("failed to start management socket", "error", err)
		closeStores()
		os.Exit(1)
	}
	defer stopMgmt()
	opsServer := datanode.NewOpsServerWithRepair(cfg, v2Store, metaStore, nil, nil, nil, nil)
	if err := opsServer.Start(); err != nil {
		log.Error("failed to start ops HTTP server", "error", err)
		closeStores()
		os.Exit(1)
	}
	defer opsServer.Stop()

	// Wait for shutdown signal.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	log.Info("shutting down", "signal", sig)

	// Graceful shutdown, in dependency order: stop accepting requests and
	// drain in-flight ones, stop the heartbeat (it reads store state, so
	// it must not outlive the stores), then close the stores exactly once.
	srv.Stop()
	heartbeat.Stop()
	closeStores()

	deregCtx, deregCancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := metaStore.RegisterNode(deregCtx, &metadata.NodeInfo{
		ID:    cfg.NodeID,
		State: metadata.NodeOffline,
	}); err != nil {
		log.Warn("failed to mark node offline", "error", err)
	}
	deregCancel()
	log.Info("shutdown complete")
}
