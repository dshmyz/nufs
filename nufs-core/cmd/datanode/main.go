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
	"sync"
	"syscall"
	"time"

	"github.com/dshmyz/nufs/nufs-core/datanode"
	"github.com/dshmyz/nufs/nufs-core/datanode/storage"
	"github.com/dshmyz/nufs/nufs-core/datanode/storage/encryption"
	"github.com/dshmyz/nufs/nufs-core/datanode/storage/journal"
	"github.com/dshmyz/nufs/nufs-core/datanode/storage/maintenance"
	"github.com/dshmyz/nufs/nufs-core/datanode/storage/segment"
	"github.com/dshmyz/nufs/nufs-core/internal/config"
	"github.com/dshmyz/nufs/nufs-core/internal/crypto"
	"github.com/dshmyz/nufs/nufs-core/internal/logging"
	"github.com/dshmyz/nufs/nufs-core/internal/tlsutil"
	"github.com/dshmyz/nufs/nufs-core/internal/tracing"
	"github.com/dshmyz/nufs/nufs-core/metadata"
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
		kmsKeyFile           = flag.String("kms-key-file", "", "Path to a 32-byte KEK file for at-rest encryption (created with 0600 perms on first start)")
		kmsKeyEnv            = flag.String("kms-key-env", "", "Environment variable name holding the at-rest KEK (64 hex chars or 32 raw bytes)")
		kmsKeyHex            = flag.String("kms-key-hex", "", "At-rest KEK as 64 hex characters (dev/test convenience)")
		allowInsecureDev     = flag.Bool("allow-insecure-dev", false, "Allow running without TLS, auth, or production hardening (dev only)")
		alertWebhook         = flag.String("alert-webhook", "", "Optional URL that receives capacity-alert events as JSON POSTs")
		segmentSize          = flag.Int64("segment-size", 0, "V2.1 segment size in bytes (0 = DefaultDataSegmentSize, 4GiB); smaller values seal segments sooner so compaction can reclaim superseded bytes — useful for demos and CI")
		compactInterval      = flag.Duration("compaction-interval", 30*time.Second, "Background compaction scan cadence for the V2.1 worker")
		gcScanInterval       = flag.Duration("gc-scan-interval", 0, "Background orphan-chunk GC scan cadence (0 = disabled; uses only the manual POST /api/v1/gc/scan endpoint)")
		gcGraceWindow        = flag.Duration("gc-grace-window", 10*time.Minute, "Minimum local chunk age before the background orphan scan will delete it (protects in-flight writes not yet committed to metadata)")
		storageVersion       = flag.String("storage-version", "v1", "Storage engine version: v1 (DEPRECATED, legacy ChunkStore, retirement per docs/v1-retirement-roadmap.md) or v2.1 (new segment engine, recommended)")
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
		MetadataAuthToken:  *metadataAuthToken,
		OpsAuthToken:       *opsAuthToken,
		EnablePprof:        *enablePprof,
		TraceEnabled:       *traceEnabled,
		TraceEndpoint:      *traceEndpoint,
		TraceInsecure:      *traceInsecure,
		EncryptAtRest:      *encryptAtRest,
		AllowLocalKMS:      *allowLocalKMS,
		KMSKeyFile:         *kmsKeyFile,
		KMSKeyEnv:          *kmsKeyEnv,
		KMSKeyHex:          *kmsKeyHex,
		AllowInsecureDev:   *allowInsecureDev,
		AlertWebhook:       *alertWebhook,
		StorageVersion:     *storageVersion,
		SegmentSize:        *segmentSize,
		CompactionInterval: *compactInterval,
		GCScanInterval:     *gcScanInterval,
		GCGraceWindow:      *gcGraceWindow,
		LogLevel:           *logLevel,
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

// buildAtRestKMS constructs the KMS backing at-rest encryption.
//
// When a production KEK source is configured (--kms-key-file / --kms-key-env /
// --kms-key-hex) it returns a FileKMS — DEKs are wrapped with the KEK and
// persisted under <dataDir>/kms so they survive restarts, making at-rest
// encryption production-usable without --allow-local-kms.
//
// Otherwise it falls back to the in-memory dev LocalKMS, which is only
// permitted when --allow-local-kms is set (fail-closed otherwise: a LocalKMS
// loses its keys on restart and must not be silently used in production).
func buildAtRestKMS(cfg datanode.Config) (crypto.KMS, error) {
	if cfg.KMSKeyFile != "" || cfg.KMSKeyEnv != "" || cfg.KMSKeyHex != "" {
		root := cfg.DataDir
		if len(cfg.DataDirs) > 0 {
			root = cfg.DataDirs[0]
		}
		kms, err := crypto.NewFileKMS(crypto.FileKMSConfig{
			KeyFile: cfg.KMSKeyFile,
			KeyEnv:  cfg.KMSKeyEnv,
			KeyHex:  cfg.KMSKeyHex,
		}, root)
		if err != nil {
			return nil, fmt.Errorf("at-rest KMS: %w", err)
		}
		return kms, nil
	}
	if !cfg.AllowLocalKMS {
		return nil, fmt.Errorf("at-rest encryption requires a production KMS (set --kms-key-file/--kms-key-env/--kms-key-hex), or pass --allow-local-kms for development only (LocalKMS loses keys on restart)")
	}
	kms, err := crypto.NewLocalKMS()
	if err != nil {
		return nil, fmt.Errorf("init LocalKMS: %w", err)
	}
	logging.Named("datanode").Warn("at-rest encryption enabled with LocalKMS; keys are in-memory and not production safe")
	return kms, nil
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

// productionGateError returns a non-nil error when cfg would start a datanode
// with an open control plane in a production (non-dev) setting. With the
// explicit --allow-insecure-dev opt-out, no error is returned.
//
// Besides requiring TLS (the historical gate), a production datanode must also
// carry an ops auth token: without it the HTTP ops API (disk adopt/retire/
// migrate, decommission, EC convert, repair …) is completely unauthenticated,
// a full control-plane take-over. Mirrors metad's ValidateProductionConfig.
func productionGateError(cfg datanode.Config) error {
	if cfg.AllowInsecureDev {
		return nil
	}
	if cfg.TLS.CertFile == "" || cfg.TLS.KeyFile == "" {
		return fmt.Errorf("production datanode requires TLS; refusing to start in plaintext (set --tls-cert/--tls-key)")
	}
	if cfg.OpsAuthToken == "" {
		return fmt.Errorf("production datanode requires ops authentication; refusing to start with an open ops API (set --ops-auth-token)")
	}
	return nil
}

func runDataNode(cfg datanode.Config) {
	log := logging.Named("datanode")
	log.Info("starting data node", "node_id", cfg.NodeID, "addr", cfg.ListenAddr, "data", cfg.DataDir, "machine", cfg.MachineID)

	// Production safety gate (mirrors metad's ValidateProductionConfig):
	// without the explicit dev opt-out, a datanode must not run an open
	// control plane. Refuse to start so a TLS-less or unauthenticated node
	// cannot silently accept chunk/ops traffic in a production setting.
	if err := productionGateError(cfg); err != nil {
		log.Error(err.Error(), "hint", "pass --allow-insecure-dev for development only")
		os.Exit(1)
	}

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

	// Configure at-rest encryption if enabled. A production FileKMS (file/
	// env/hex KEK) is used when configured and does not require the dev-only
	// --allow-local-kms opt-in. Otherwise it is intentionally fail-closed
	// unless the operator explicitly opts into the dev-only LocalKMS.
	if cfg.EncryptAtRest {
		kms, err := buildAtRestKMS(cfg)
		if err != nil {
			log.Error("failed to configure at-rest encryption", "error", err)
			os.Exit(1)
		}
		chunkStore.SetEncryptor(crypto.NewEncryptor(kms))
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
	opsServer.SetAlertWebhook(cfg.AlertWebhook)
	// Register the V1 DiskManager's capacity-alert callback so transitions are
	// recorded in the admin ring and delivered to the webhook (the DiskManager
	// already de-dups by level change).
	diskManager.SetOnCapacityAlert(func(level datanode.AlertLevel, usagePct float64, dm *datanode.DiskManager) {
		st := dm.Stats()
		opsServer.NotifyCapacityAlert(level, st.UsagePct, st.UsedBytes, st.TotalBytes)
	})
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
	shardStores := make([]storage.Store, 0, len(dataDirs))
	closeStores := func() {
		// EC shard stores first (they piggyback on the same per-disk dir and
		// share the change journal); closing order among independent stores
		// does not matter, but keeping the same single owner is what matters.
		for _, st := range shardStores {
			if closer, ok := st.(interface{ Close() error }); ok {
				if err := closer.Close(); err != nil {
					log.Warn("V2.1 shard store close error", "error", err)
				}
			}
		}
		for _, st := range stores {
			if closer, ok := st.(interface{ Close() error }); ok {
				if err := closer.Close(); err != nil {
					log.Warn("V2.1 store close error", "error", err)
				}
			}
		}
	}
	// Open the node's async change journal (§12). A single journal shared
	// across all disks keeps one heartbeat watermark; events already carry
	// the affected disk/segment. The segment stores append corruption/
	// disk-loss events to it, and the heartbeat ships Pending() to the
	// metadata authority for reconciliation.
	changeJournal, err := journal.OpenChangeJournal(journal.JournalOptions{
		Dir:               filepath.Join(dataDirs[0], "change-journal"),
		MaxBytes:          8 << 30,
		RetainMinDuration: 24 * time.Hour,
		MaxPerHeartbeat:   10000,
		MaxHeartbeatBytes: 4 << 20,
	})
	if err != nil {
		log.Error("failed to open change journal", "error", err)
		closeStores()
		os.Exit(1)
	}
	// Effective per-stream segment size: the operator flag when set,
	// else the storage default (4GiB). A smaller value seals segments
	// sooner, so superseded (dead) bytes enter sealed segments faster and
	// the compaction worker reclaims them sooner — useful for demos/CI,
	// and harmless in production where 0 keeps the existing 4GiB default.
	segSize := int64(storage.DefaultDataSegmentSize)
	if cfg.SegmentSize > 0 {
		segSize = cfg.SegmentSize
	}
	// Configure at-rest encryption once; the same registry is shared by the
	// data stream, the EC-shard stream, and any disk adopted later via AddDisk.
	var enc *encryption.KeyRegistry
	if cfg.EncryptAtRest {
		kms, err := buildAtRestKMS(cfg)
		if err != nil {
			log.Error("failed to configure at-rest encryption", "error", err)
			closeStores()
			os.Exit(1)
		}
		enc = encryption.NewKeyRegistry(kms)
	}

	// newDiskStores builds the paired data-stream (StreamID 1) and EC-shard
	// (StreamID 2) segment stores for one disk dir. It is the single factory
	// for every disk: the startup loop below builds the configured data dirs
	// through it, and the V2Store's DiskLifecycleOps.AddDisk reuses it to
	// adopt a runtime-added dir with exactly the same engine config (change
	// journal, segment size, stream IDs, encryption) as its siblings.
	newDiskStores := func(dir string) (storage.Store, storage.Store, error) {
		segCfg := segment.Config{
			Dir:           dir,
			SegmentSize:   segSize,
			UseMemIndex:   false,
			StreamID:      1, // data stream (0 = small)
			ChangeJournal: changeJournal,
			Enc:           enc,
		}
		s, err := segment.New(segCfg)
		if err != nil {
			return nil, nil, fmt.Errorf("init V2.1 data store: %w", err)
		}
		shardCfg := segment.Config{
			Dir:           dir,
			IndexDir:      filepath.Join(dir, "index-ecshard"),
			SegmentSize:   segSize,
			UseMemIndex:   false,
			StreamID:      2, // EC shard stream
			ChangeJournal: changeJournal,
			Enc:           enc,
		}
		ss, err := segment.New(shardCfg)
		if err != nil {
			// Unwind the already-opened data store for this dir.
			_ = s.Close()
			return nil, nil, fmt.Errorf("init V2.1 shard store: %w", err)
		}
		return s, ss, nil
	}

	for _, dir := range dataDirs {
		s, ss, err := newDiskStores(dir)
		if err != nil {
			log.Error("failed to init V2.1 store", "disk", dir, "error", err)
			closeStores()
			os.Exit(1)
		}
		stores = append(stores, s)
		shardStores = append(shardStores, ss)
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
		ID:             cfg.NodeID,
		Addr:           registerAddr(cfg),
		DataDir:        cfg.DataDir,
		Rack:           cfg.Rack,
		Zone:           cfg.Zone,
		MachineID:      cfg.MachineID,
		Tier:           cfg.Tier,
		State:          metadata.NodeOnline,
		ShardDiskCount: len(shardStores),
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
	// Attach the EC shard stores so Program A's EC conversions can place
	// 6+3 shards across the node's shard stores. Requires ≥3 shard stores for
	// §14 (≤3 shards per machine across ≥3 machines / fault domains); a
	// datanode with fewer disks can still replicate, it just cannot host EC
	// stripes (ConvertToEC will fail cleanly on an under-provisioned node).
	if err := v2Store.AttachShardStores(shardStores); err != nil {
		log.Error("failed to attach EC shard stores", "error", err)
		closeStores()
		os.Exit(1)
	}
	// Let DiskLifecycleOps.AddDisk adopt a runtime-added disk dir using the
	// same engine factory that built the configured data dirs above, so an
	// adopted disk is constructed (and enumerated) exactly like its siblings.
	v2Store.SetDiskFactory(newDiskStores)
	log.Info("V2.1 EC shard stores attached", "shard_stores", len(shardStores))

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
	heartbeat.SetChangeJournal(changeJournal)
	heartbeat.Start()

	// Cross-node repair on the V2.1 serving path. The Replicator and
	// RepairWorker are engine-agnostic: they act over the datanode TCP wire
	// (read a surviving replica, rewrite a failed/new target) plus the
	// metadata RepRepairMeta surface, so they drive V2Store exactly as they
	// drive the legacy ChunkStore. Repair carries the metadata-issued chunk
	// generation so a restored replica lands on the same authoritative
	// generation as its surviving peers (Metadata V2 fencing), rather than a
	// local gen+1 bump on the V2 store.
	replicator := datanode.NewReplicator(cfg.ListenAddr, 4)
	replicator.SetTLS(cfg.TLS)
	replicator.Start()

	repairWorker := datanode.NewRepairWorker(datanode.RepairConfig{
		Meta:       metaStore,
		NodeID:     cfg.NodeID,
		Interval:   30 * time.Second,
		Replicator: replicator,
		LocalAddr:  cfg.ListenAddr,
	})
	repairWorker.Start(context.Background())

	// Operational channels: the unix-socket management server and the HTTP
	// ops server. Both are engine-agnostic (they hold the OpsStore subset, so
	// V2Store drives the same surface V1 exposes). V2.1 has no legacy
	// DiskManager lifecycle and no V1 full-scan AntiEntropy (it reconciles
	// via the change journal), so those capability handlers answer
	// "unsupported"/not-rewired and diskManager/chainRepl/antiEntropy stay nil;
	// the repair worker is wired above.
	stopMgmt, err := startManagementServer(v2Store, nil, dataDirs)
	if err != nil {
		log.Error("failed to start management socket", "error", err)
		closeStores()
		os.Exit(1)
	}
	defer stopMgmt()
	opsServer := datanode.NewOpsServerWithRepair(cfg, v2Store, metaStore, nil, nil, nil, repairWorker)

	// Program A / S2: EC conversion authority + serving-path driver. The V2.1
	// serving path drives the replication→6+3 conversion transaction against
	// the *remote* metadata authority over HTTP (the production topology): the
	// metad service owns the §14 placement decision and the transaction state
	// machine (Preparing → Encoding → Syncing → Complete | RolledBack), and
	// this node supplies the shard payloads. metaStore is the *metadata.HTTPClient
	// already used for heartbeats/repair; its HTTPClient implements the
	// metadata.ECAuthority seam (see metadata/client.go), so NewECService just
	// takes it as the authority. This is the S2 replacement for the S1
	// in-process local Pebble ECStore stand-in.
	ecService := datanode.NewECService(v2Store, metaStore)
	// Wire the publish hook: after a completed conversion, lift the stripe's EC
	// layout into the chunk's authoritative metadata (atomic §14 layout switch)
	// on the metad authority over HTTP. This closes the serving loop — a
	// converted chunk is then served from its 6+3 shards, not the old replicas.
	//
	// Note: this stores the full nine-shard layout per chunk (O(N×9)) — the
	// PG-level-convergence transition form. Long term the EC layout should
	// converge to a placement-group / EC-profile level so a chunk references
	// the profile instead of embedding all nine shards.
	ecService.SetPublish(func(_ context.Context, st *metadata.ECStripe) error {
		return metaStore.PublishConversion(st)
	})

	// Program 9: cross-node EC production topology. Without this, §14 planning
	// runs against the default *synthetic single-node* candidate topology
	// (CandidateDisks() fakes NodeID 1/2/3 "slots" that all resolve back to this
	// one physical node) — so "≥3 distinct NodeID fault domains" is fake. Here we
	// instead resolve the real cluster topology live from the metadata authority
	// at convert-time:
	//   - peerClient (SetCrossNode) lets the coordinator push each not-on-this-node
	//     shard over TCP (ReplicateECShard) to the peer that owns it, writing each
	//     node's own shard only → true §14 fault domains.
	//   - SetCandidateDisks feeds PlanShards the real candidate-disk set built from
	//     every NodeOnline peer's ShardDiskCount (DiskID = NodeID*1000 + local_disk;
	//     resolveDisk uses %1000 to recover the node-local index).
	//
	// Both are resolved lazily on first convert and cached (sync.Once), so a
	// multi-shard conversion issues one ListNodes round-trip instead of one per
	// shard. The candidate set includes every online node's shard disks — this
	// node's own disks participate too, since the coordinator writes the shards
	// it owns locally (WriteShardAtDisk) and pushes the rest to their peers; so
	// a full 6+3 stripe needs ≥3 online nodes (including this one) with ≥3 shard
	// disks each for §14. An under-provisioned cluster simply fails the convert
	// cleanly (S3 semantics) rather than fabricating a fake fault domain.
	//
	// Single-node clusters and V2.1 nodes that never convert are unaffected: the
	// synthetic topology remains the fallback for the (dev-only) local-peer path,
	// and replication (non-EC) needs none of this.
	var (
		peerAddrs     map[uint64]string
		loadPeersOnce sync.Once
	)
	loadPeers := func() {
		nodes, err := metaStore.ListNodes(ctx)
		if err != nil {
			return // leave empty; convert will fail cleanly
		}
		peerAddrs = make(map[uint64]string, len(nodes))
		for _, n := range nodes {
			if n.State == metadata.NodeOnline && n.Addr != "" {
				peerAddrs[uint64(n.ID)] = n.Addr
			}
		}
	}
	ecService.SetCrossNode(uint64(cfg.NodeID), func(nodeID uint64) (*datanode.Client, bool) {
		loadPeersOnce.Do(loadPeers)
		addr, ok := peerAddrs[nodeID]
		if !ok {
			return nil, false
		}
		return datanode.NewClient(addr), true
	})
	ecService.SetCandidateDisks(func() []metadata.ECDisk {
		loadPeersOnce.Do(loadPeers)
		nodes, err := metaStore.ListNodes(ctx)
		if err != nil {
			return nil
		}
		var disks []metadata.ECDisk
		for _, n := range nodes {
			// Include every online node with shard disks — this node too. In
			// cross-node mode the coordinator writes the shards it owns locally
			// (WriteShardAtDisk) and pushes the rest to their owning peers, so
			// the coordinator's own disks participate in §14 placement rather
			// than being a dead letter. Only offline or disk-less nodes are
			// excluded (no reachable candidate disks).
			if n.State != metadata.NodeOnline || n.ShardDiskCount <= 0 {
				continue
			}
			for d := 0; d < n.ShardDiskCount; d++ {
				disks = append(disks, metadata.ECDisk{
					NodeID: uint64(n.ID),
					DiskID: uint64(n.ID)*1000 + uint64(d),
				})
			}
		}
		return disks
	})
	opsServer.SetECService(ecService)
	// V2.1 capacity alerts: no DiskManager callback exists (disk==nil), so poll
	// the unified capacity overview on a timer and push any level transition
	// through the same NotifyCapacityAlert ring+webhook path as V1.
	opsServer.SetAlertWebhook(cfg.AlertWebhook)
	{
		alertStop := make(chan struct{})
		go func() {
			t := time.NewTicker(30 * time.Second)
			defer t.Stop()
			for {
				select {
				case <-t.C:
					opsServer.CheckAndNotifyCapacity()
				case <-alertStop:
					return
				}
			}
		}()
		defer func() { close(alertStop) }()
	}

	// Program 6 / F2: EC self-heal scan. A background sweep discovers every
	// degraded 6+3 stripe on this node (shards lost to disk/node ageing or
	// degrade) and, when the loss is within §14 tolerance and the stripe's
	// original length resolves from metadata, drives RepairChunkEC to rebuild
	// the missing shards back onto healthy shard disks. metaStore resolves the
	// original length via the chunk's authoritative Size (the padding makes it
	// unrecoverable from shard lengths alone, §14).
	healer := datanode.NewECSelfHealer(v2Store, metaStore, datanode.ECSelfHealConfig{})
	// Program 7: the production metaStore (*metadata.HTTPClient) now carries the
	// two EC resolver seams — ResolveStripeLanding (F3 repair-landing) and
	// IsChunkShardsOrphaned (F4 orphan GC) — over the metadata ops HTTP RPCs
	// (/api/v1/ec/convert/resolve-landing + is-orphan). Go interfaces are
	// structural, so metaStore satisfies ECLandingResolver/ECOrphanResolver
	// directly; wiring both turns on authoritative repair-landing (shards put
	// back on their §14 home disk) and orphan-GC (reclaim of rolled-back
	// conversion shards) against the *remote* metadata authority — the true
	// production multi-node topology, replacing the F3/F4 local-Pebble stand-in.
	healer.SetLandingResolver(metaStore)
	healer.SetOrphanResolver(metaStore, datanode.EcOrphanDefaultAge)
	// Program 13 / soak: cross-node EC shard rebuild. The self-healer otherwise
	// only sees this node's ~3 shards, so on a multi-node cluster every stripe
	// looks "loss > §14 tolerance" and a node crash that skirts a shard is never
	// auto-repaired (the soak caught this: GET truncates until an operator
	// intervenes). Wiring a peer dialer lets the healer take the cluster-wide
	// view — read every shard from its owning node and push lost shards back to
	// their authoritative landing node/disk. The dialer reuses the same lazily
	// loaded ListNodes → addr topology the EC conversion coordinator uses above;
	// datanode.NewClient connects lazily, so an unreachable peer just fails that
	// one shard (it is counted missing and retried next sweep).
	healer.SetPeerDialer(func(addr string) *datanode.Client {
		return datanode.NewClient(addr)
	})
	healer.Start(ctx)

	// Program 8 / §4 V1-c: proactive per-disk I/O health monitor. Start it
	// alongside the other background machinery so an idle or read-wedged disk
	// (one that never gets writes and so never trips the reactive failCount in
	// writeTo) still escalates to degraded/failed via periodic Stat probes.
	// Recovery stays write-path only (a real write clears the streak); probe
	// success never un-fails a disk. Stopped right before closeStores below.
	v2Store.StartDiskMonitor(ctx)

	// Program 13: background compaction + reclaim worker. Segments only
	// rotate on write-full; without this loop the physical on-disk footprint
	// grows without bound as overwritten generations leave dead bytes in
	// sealed segments. It scans each disk's sealed data-stream segments and
	// compacts the highest-value eligible one per tick, removing the source
	// so reclaimed bytes return to the filesystem. Stopped right before
	// closeStores below (it drives the stores, so it must not outlive them).
	compactionCtx, compactionCancel := context.WithCancel(context.Background())
	var compactOpts []maintenance.CompressionWorkerOption
	if cfg.CompactionInterval > 0 {
		compactOpts = append(compactOpts, maintenance.WithCompactionInterval(cfg.CompactionInterval))
	}
	compactionWorker := maintenance.NewCompressionWorker(v2Store.DataStores(), compactOpts...)
	go compactionWorker.Run(compactionCtx)

	if err := opsServer.Start(); err != nil {
		log.Error("failed to start ops HTTP server", "error", err)
		closeStores()
		os.Exit(1)
	}
	defer opsServer.Stop()

	// SIGHUP handler for config hot-reload (log level changes), mirroring the
	// V1 runDataNode handler. A SIGHUP re-applies the configured log level in
	// place, so operators can rotate verbosity without restarting the daemon.
	hupCh := make(chan os.Signal, 1)
	signal.Notify(hupCh, syscall.SIGHUP)
	go func() {
		for range hupCh {
			log.Info("received SIGHUP, reloading log level")
			logging.SetLevel(cfg.LogLevel)
		}
	}()

	// Wait for shutdown signal.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	log.Info("shutting down", "signal", sig)

	// Graceful shutdown, in dependency order: stop accepting requests and
	// draining in-flight ones, stop background write-generators (repair,
	// replicator) and the heartbeat (it reads store state, so it must not
	// outlive the stores), then close the stores exactly once.
	srv.Stop()
	repairWorker.Stop()
	replicator.Stop()
	heartbeat.Stop()
	healer.Stop()
	// Stop the proactive disk monitor FIRST (it probes the stores), then drain
	// in-flight writes before closing the stores (mirrors the V1 shutdown
	// Phase 3). The barrier quiesces writes without blocking reads.
	v2Store.StopDiskMonitor()
	// Stop the compaction worker (it drives the stores) before draining and
	// closing them below.
	compactionCancel()
	drainCtx, drainCancel := context.WithTimeout(context.Background(), 30*time.Second)
	releaseDrain, err := v2Store.QuiesceWrites(drainCtx)
	if err != nil {
		log.Warn("drain timeout, some writes may be in-flight", "error", err)
	} else {
		log.Info("all in-flight writes drained")
	}
	if releaseDrain != nil {
		releaseDrain()
	}
	drainCancel()
	closeStores()
	if err := changeJournal.Close(); err != nil {
		log.Warn("V2.1 change journal close error", "error", err)
	}

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
