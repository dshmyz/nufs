// metad is the metadata service daemon for the distributed storage system.
// It uses Pebble as the storage engine with optional Raft consensus.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/example/dfs/internal/config"
	internalhttp "github.com/example/dfs/internal/httputil"
	"github.com/example/dfs/internal/logging"
	"github.com/example/dfs/internal/tlsutil"
	"github.com/example/dfs/internal/tracing"
	"github.com/example/dfs/internal/version"
	"github.com/example/dfs/metadata"
)

func main() {
	var (
		configPath           = flag.String("config", "", "Path to YAML config file")
		dataDir              = flag.String("data-dir", "/var/lib/dfs/metadata", "Pebble data directory")
		cacheDir             = flag.String("cache-dir", "", "Pebble read cache directory (optional)")
		nodeID               = flag.String("node-id", "1", "Metadata node ID or StatefulSet pod name ending in -<ordinal>")
		memTableSize         = flag.Uint64("memtable-size", 256<<20, "Pebble memtable size in bytes")
		bucketStats          = flag.Bool("bucket-stats", true, "Enable persisted per-bucket usage counters")
		enableRaft           = flag.Bool("raft", true, "Enable Raft consensus")
		raftAddr             = flag.String("raft-addr", "0.0.0.0:7000", "Raft bind address")
		raftAdvertiseAddr    = flag.String("raft-advertise-addr", "", "Advertised Raft address for peers (default: raft-addr)")
		raftDir              = flag.String("raft-dir", "/var/lib/dfs/raft", "Raft data directory")
		raftBootstrap        = flag.Bool("raft-bootstrap", false, "Bootstrap a new Raft cluster")
		raftBootstrapPeers   = flag.String("raft-bootstrap-peers", "", "Comma-separated Raft bootstrap peers as id=host:port")
		raftPeerOps          = flag.String("raft-peer-ops", "", "Comma-separated Raft peer ops URLs as id=http://host:port")
		opsAddr              = flag.String("ops-addr", "0.0.0.0:8091", "Operations HTTP API address")
		advertiseOps         = flag.String("advertise-ops-addr", "", "Advertised ops URL for other metad nodes (default: http://<hostname>:8091)")
		raftHbTimeout        = flag.Duration("raft-heartbeat", 0, "Raft heartbeat timeout (default: 1s)")
		raftElection         = flag.Duration("raft-election", 0, "Raft election timeout (default: 1s)")
		raftLease            = flag.Duration("raft-lease", 0, "Raft leader lease timeout (default: 500ms)")
		leaseTTL             = flag.Duration("lease-ttl", 30*time.Second, "Node lease TTL")
		gcInterval           = flag.Duration("gc-interval", 10*time.Minute, "GC scan interval")
		gcDryRun             = flag.Bool("gc-dry-run", false, "GC dry-run mode (no deletes)")
		scrubInterval        = flag.Duration("scrub-interval", 1*time.Hour, "Scrub interval")
		autoBalanceInterval  = flag.Duration("auto-balance-interval", 0, "Auto rebalance interval (0 disables periodic auto balance)")
		autoBalanceThreshold = flag.Float64("auto-balance-threshold", 0.15, "Auto rebalance imbalance threshold")
		autoBalanceMax       = flag.Int("auto-balance-max-migrations", 10, "Maximum migrations per auto rebalance pass")
		tlsCert              = flag.String("tls-cert", "", "TLS certificate file (enables HTTPS)")
		tlsKey               = flag.String("tls-key", "", "TLS private key file")
		tlsCA                = flag.String("tls-ca", "", "TLS CA certificate for mutual TLS (client verification)")
		tlsRequireClientCert = flag.Bool("tls-require-client-cert", false, "Require clients to present a certificate signed by tls-ca")
		tlsSkipVerify        = flag.Bool("tls-skip-verify", false, "Skip TLS server certificate verification (dev only)")
		authToken            = flag.String("auth-token", "", "Bearer token for ops API auth (empty = no auth)")
	allowInsecureDev    = flag.Bool("allow-insecure-dev", false, "Allow running without auth, TLS, or multi-node Raft (dev only)")
		backupEnabled        = flag.Bool("backup-enabled", false, "Enable leader-only metadata backups")
		backupLocalDir       = flag.String("backup-local-dir", "/var/lib/dfs/backup-tmp", "Local temporary directory for metadata backups")
		backupInterval       = flag.Duration("backup-interval", time.Hour, "Metadata backup interval")
		backupRetention      = flag.Int("backup-retention", 24, "Number of committed metadata backups to retain")
		backupS3Bucket       = flag.String("backup-s3-bucket", "", "S3 bucket for metadata backups")
		backupS3Prefix       = flag.String("backup-s3-prefix", "", "S3 key prefix for metadata backups")
		backupS3Region       = flag.String("backup-s3-region", "", "S3 region for metadata backups")
		backupS3Endpoint     = flag.String("backup-s3-endpoint", "", "Custom S3-compatible endpoint")
		backupUploadTimeout  = flag.Duration("backup-upload-timeout", 10*time.Minute, "Maximum duration of a metadata backup run")
		backupStagingMaxAge  = flag.Duration("backup-staging-max-age", 24*time.Hour, "Maximum age of incomplete backup staging data")
		restoreMinReplicas   = flag.Int("restore-minimum-readable-replicas", 1, "Minimum readable datanode replicas required before a restored metadata cluster becomes ready")
		clusterID            = flag.String("cluster-id", "", "Stable metadata cluster identity")
		traceEnabled         = flag.Bool("trace-enabled", false, "Enable OpenTelemetry tracing")
		traceEndpoint        = flag.String("trace-endpoint", "", "OTLP gRPC endpoint")
		traceInsecure        = flag.Bool("trace-insecure", true, "Use insecure OTLP connection")
		logLevel             = flag.String("log-level", "info", "Log level (debug/info/warn/error)")
		logJSON              = flag.Bool("log-json", false, "JSON log output")
	)
	_ = configPath
	config.Preload()
	flag.Parse()
	if *restoreMinReplicas < 1 {
		fmt.Fprintln(os.Stderr, "invalid restore readiness configuration: --restore-minimum-readable-replicas must be at least 1")
		os.Exit(1)
	}

	backupCfg, err := validateBackupRuntimeConfig(backupRuntimeConfig{
		Enabled:       *backupEnabled,
		RaftEnabled:   *enableRaft,
		ClusterID:     *clusterID,
		LocalDir:      *backupLocalDir,
		Interval:      *backupInterval,
		Retention:     *backupRetention,
		S3Bucket:      *backupS3Bucket,
		S3Prefix:      *backupS3Prefix,
		S3Region:      *backupS3Region,
		S3Endpoint:    *backupS3Endpoint,
		UploadTimeout: *backupUploadTimeout,
		StagingMaxAge: *backupStagingMaxAge,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid backup configuration: %v\n", err)
		os.Exit(1)
	}

	nodeIDValue, err := resolveMetadataNodeID(*nodeID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid --node-id: %v\n", err)
		os.Exit(1)
	}

	logging.Init(logging.Config{Level: *logLevel, JSON: *logJSON, AddSource: true})
	log := logging.Named("metad")

	// Production safety validation: ensure auth, TLS, and Raft are configured
	// before starting the service. Can be bypassed with --allow-insecure-dev.
	raftNodeCount := 1
	if *enableRaft && *raftBootstrapPeers != "" {
		raftNodeCount = len(parsePeerOpsURLsForValidation(*raftBootstrapPeers)) + 1
	}
	if err := metadata.ValidateProductionConfig(metadata.ProductionValidationConfig{
		Mode:             runtimeMode(*allowInsecureDev),
		JWTSecret:        *authToken,
		RaftNodeCount:    raftNodeCount,
		TLSEnabled:       *tlsCert != "",
		AllowInsecureDev: *allowInsecureDev,
	}); err != nil {
		log.Error("production config validation failed", "error", err)
		os.Exit(1)
	}

	log.Info("starting metadata service", "node_id", nodeIDValue, "data", *dataDir)
	log.Info("runtime", "go", runtime.Version(), "os", runtime.GOOS, "arch", runtime.GOARCH)
	log.Info("version", "version", version.Version, "git_commit", version.GitCommit, "build_time", version.BuildTime)

	_, traceShutdown, err := tracing.Init(tracing.Config{
		Enabled:  *traceEnabled,
		Endpoint: *traceEndpoint,
		Service:  "metad",
		Insecure: *traceInsecure,
	})
	if err != nil {
		log.Error("failed to initialize tracing", "error", err)
		os.Exit(1)
	}

	pebbleCfg := metadata.PebbleStoreConfig{
		Dir:            *dataDir,
		CacheDir:       *cacheDir,
		NodeID:         nodeIDValue,
		MemTableSize:   *memTableSize,
		UseBucketStats: *bucketStats,
	}

	store, err := metadata.NewPebbleStore(pebbleCfg)
	if err != nil {
		log.Error("failed to create PebbleStore", "error", err)
		os.Exit(1)
	}
	// Install node registration/heartbeat rate limiter. Using
	// production-safe defaults; values can be tweaked per-environment
	// by reading config flags if needed later.
	store.SetNodeThrottle(metadata.NewNodeRegistrationThrottle(nil))
	// Install the event bus so watch API and placement engine can
	// subscribe to metadata changes. Without this, watch returns 501.
	store.SetEventBus(metadata.NewEventBus(1024))
	log.Info("PebbleStore initialized", "dir", *dataDir)

	var raftNode *metadata.RaftNode

	// Compute advertised ops URL once, used for both RaftNode config
	// and ops handler redirect. Defaults to http://<hostname>:<port>.
	advertiseOpsURL := *advertiseOps
	if advertiseOpsURL == "" {
		host, _, _ := net.SplitHostPort(*opsAddr)
		if host == "" || host == "0.0.0.0" {
			hostname, _ := os.Hostname()
			host = hostname
		}
		_, port, _ := net.SplitHostPort(*opsAddr)
		advertiseOpsURL = fmt.Sprintf("http://%s:%s", host, port)
	}

	if *enableRaft {
		bootstrapPeers, err := parseRaftBootstrapPeers(*raftBootstrapPeers)
		if err != nil {
			log.Error("invalid raft bootstrap peers", "error", err)
			os.Exit(1)
		}
		peerOps, err := parseRaftPeerOpsURLs(*raftPeerOps)
		if err != nil {
			log.Error("invalid raft peer ops URLs", "error", err)
			os.Exit(1)
		}
		raftNodeID := fmt.Sprintf("meta-%d", nodeIDValue)
		peerOps[raftNodeID] = advertiseOpsURL

		raftCfg := metadata.RaftNodeConfig{
			NodeID:             raftNodeID,
			BindAddr:           *raftAddr,
			AdvertiseAddr:      *raftAdvertiseAddr,
			RaftDir:            *raftDir,
			Bootstrap:          *raftBootstrap,
			BootstrapPeers:     bootstrapPeers,
			HeartbeatTimeout:   *raftHbTimeout,
			ElectionTimeout:    *raftElection,
			LeaderLeaseTimeout: *raftLease,
			SnapshotThreshold:  8192,
			SnapshotInterval:   2 * time.Minute,
			TrailingLogs:       10240,
			AdvertiseOpsAddr:   advertiseOpsURL,
			PeerOpsURLs:        peerOps,
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

		// Register ops URL in FSM so peers can find the leader for auto-forwarding.
		// Runs async because the cluster may not have a leader yet on first join.
		go func() {
			for i := 0; i < 30; i++ {
				if err := raftNode.StoreOpsURL(10 * time.Second); err == nil {
					log.Info("registered ops URL in FSM", "url", advertiseOpsURL)
					return
				}
				time.Sleep(time.Second)
			}
			log.Warn("failed to register ops URL in FSM after 30s", "url", advertiseOpsURL)
		}()
	} else {
		log.Info("running in single-node mode (Raft disabled)")
	}

	opts := []metadata.ServiceOption{
		metadata.WithLeaseTTL(*leaseTTL),
		metadata.WithGCInterval(*gcInterval),
		metadata.WithGCDryRun(*gcDryRun),
		metadata.WithScrubInterval(*scrubInterval),
		metadata.WithAutoBalanceInterval(*autoBalanceInterval),
		metadata.WithAutoBalanceThreshold(*autoBalanceThreshold),
		metadata.WithAutoBalanceMaxConcurrentMigrations(*autoBalanceMax),
	}

	bundle, err := metadata.NewPebbleServiceBundle(store, opts...)
	if err != nil {
		log.Error("failed to create service bundle", "error", err)
		os.Exit(1)
	}
	defer bundle.Close()

	bundle.Raft = raftNode

	log.Info("service bundle initialized")

	restoreGate, err := startRestoreReadinessGate(context.Background(), store, bundle, restoreReadinessConfig{
		MinimumReadableReplicas: *restoreMinReplicas,
		Probe:                   datanodeRestoreReplicaProbe{},
	})
	if err != nil {
		log.Error("failed to initialize restore readiness gate", "error", err)
		os.Exit(1)
	}
	defer restoreGate.Stop()

	var backupRepository metadata.BackupRepository
	backupCoordinator, err := createBackupCoordinatorRuntime(
		backupCfg,
		store,
		func(cfg metadata.S3Config) (metadata.BackupRepository, error) {
			repository, err := metadata.NewS3BackupRepository(cfg)
			if err != nil {
				return nil, err
			}
			backupRepository = repository
			return repository, nil
		},
		func(
			cfg metadata.BackupCoordinatorConfig,
			store *metadata.PebbleStore,
			repository metadata.BackupRepository,
		) backupCoordinatorLifecycle {
			return metadata.NewBackupCoordinator(cfg, store, repository)
		},
	)
	if err != nil {
		log.Error("failed to initialize backup coordinator", "error", err)
		os.Exit(1)
	}
	if backupCoordinator != nil {
		backupCoordinator.Start()
		log.Info(
			"backup coordinator started",
			"bucket", backupCfg.S3Bucket,
			"prefix", backupCfg.S3Prefix,
			"interval", backupCfg.Interval,
			"retention", backupCfg.Retention,
		)
	}

	mux := http.NewServeMux()
	var backupDeps []backupOpsDependency
	if coordinator, ok := backupCoordinator.(backupOpsCoordinator); ok {
		backupDeps = append(backupDeps, backupOpsDependency{
			coordinator: coordinator,
			repository:  backupRepository,
		})
	}
	registerOpsHandlers(mux, store, bundle, advertiseOpsURL, backupDeps...)

	admin := newAdminServer(store, bundle)
	admin.RegisterRoutes(mux)

	// Prometheus metrics endpoint
	mux.Handle("/metrics", prometheusMetricsHandler(store, bundle.Metrics, backupDeps...))
	// Health check endpoint — handled by setupDefaultRoutes in ops_handlers.
	// mux.Handle("/healthz", metadata.HealthHandler(bundle.Health))
	// Version endpoint
	mux.HandleFunc("/version", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(version.Info())
	})

	// Initialize graceful shutdown drain and wire it into request handling.
	drain := metadata.NewShutdownDrain(15 * time.Second)
	public := map[string]struct{}{
		"/health":         {},
		"/healthz":        {},
		"/ready":          {},
		"/metrics":        {},
		"/api/v1/metrics": {},
		"/api/v1/health":  {},
		"/version":        {},
	}
	var handler http.Handler = rejectEmptyBucketQuotaPath(drain.Middleware(public, mux))
	if *authToken != "" {
		log.Info("auth token enabled for ops API")
		handler = internalhttp.BearerAuth(*authToken, public, handler)
	}

	// Rate limiting middleware: 100 req/s, burst 200
	rateLimiter := metadata.NewRateLimiter(100, 200)
	stopRateLimiterCleanup := rateLimiter.StartCleanup(1 * time.Minute)
	defer stopRateLimiterCleanup()
	limitedMux := http.NewServeMux()
	limitedMux.Handle("/", rateLimitMiddleware(rateLimiter, handler))

	opsServer := &http.Server{
		Addr:         *opsAddr,
		Handler:      limitedMux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	// Configure TLS if certificates are provided
	metadTLS := tlsutil.Config{
		CertFile:          *tlsCert,
		KeyFile:           *tlsKey,
		CAFile:            *tlsCA,
		SkipVerify:        *tlsSkipVerify,
		RequireClientCert: *tlsRequireClientCert,
	}
	if metadTLS.CAFile != "" && !metadTLS.RequireClientCert {
		log.Warn("tls CA configured but client certificates are optional; set --tls-require-client-cert for strict mTLS")
	}

	go func() {
		if metadTLS.Enabled() {
			tlsCfg, err := tlsutil.ServerConfig(metadTLS)
			if err != nil {
				log.Error("tls config failed", "error", err)
				os.Exit(1)
			}
			opsServer.TLSConfig = tlsCfg
			log.Info("ops API listening", "addr", *opsAddr, "tls", true)
			if err := opsServer.ListenAndServeTLS("", ""); err != nil && err != http.ErrServerClosed {
				log.Error("ops server error", "error", err)
				os.Exit(1)
			}
		} else {
			log.Info("ops API listening", "addr", *opsAddr)
			if err := opsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Error("ops server error", "error", err)
				os.Exit(1)
			}
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	sig := <-sigCh

	// Check for SIGHUP (reload signal)
	if sig == syscall.SIGHUP {
		log.Info("received SIGHUP, reloading configuration")
		// Reload config file if provided
		if *configPath != "" {
			if err := config.Load(*configPath); err != nil {
				log.Error("failed to reload config", "path", *configPath, "error", err)
			} else {
				log.Info("config reloaded", "path", *configPath)
			}
		}
		// Re-apply log level from flag (may have been updated by config reload)
		logging.SetLevel(*logLevel)
		// Wait for termination signal after reload
		sig = <-sigCh
	}

	log.Info("received signal, shutting down", "signal", sig)

	// Begin draining before shutting down the listener so new protected
	// requests are rejected while in-flight requests finish.
	if err := drain.Shutdown(); err != nil {
		log.Warn("drain timeout", "error", err)
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutdownCancel()
	if err := opsServer.Shutdown(shutdownCtx); err != nil {
		log.Warn("ops shutdown error", "error", err)
	}

	if backupCoordinator != nil {
		backupCoordinator.Stop()
		log.Info("backup coordinator stopped")
	}

	if raftNode != nil {
		if err := raftNode.TriggerSnapshot(); err != nil {
			log.Warn("snapshot failed", "error", err)
		}
	}

	traceCtx, traceCancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := traceShutdown(traceCtx); err != nil {
		log.Warn("tracing shutdown error", "error", err)
	}
	traceCancel()

	log.Info("shutdown complete")
}

// rateLimitMiddleware applies per-IP rate limiting using a token bucket.
// Returns 429 with Retry-After and X-RateLimit-* headers when exceeded.
func rateLimitMiddleware(rl *metadata.RateLimiter, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.RemoteAddr

		// Set rate limit headers for all responses
		w.Header().Set("X-RateLimit-Limit", fmt.Sprintf("%d", rl.Burst()))
		w.Header().Set("X-RateLimit-Remaining", fmt.Sprintf("%d", rl.Available(key)))

		if !rl.Allow(key) {
			// Calculate retry-after based on refill rate
			retryAfter := rl.WaitTime(key)
			if retryAfter < time.Second {
				retryAfter = time.Second
			}
			w.Header().Set("Retry-After", fmt.Sprintf("%.0f", retryAfter.Seconds()))
			w.Header().Set("X-RateLimit-Reset", fmt.Sprintf("%d", time.Now().Add(retryAfter).Unix()))
			http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func resolveMetadataNodeID(raw string) (uint64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, fmt.Errorf("empty node id")
	}
	if id, err := strconv.ParseUint(raw, 10, 64); err == nil {
		if id == 0 {
			return 0, fmt.Errorf("node id must be greater than zero")
		}
		return id, nil
	}

	_, ordinalText, ok := strings.Cut(strings.TrimSpace(raw), "-")
	if !ok {
		return 0, fmt.Errorf("node id %q is neither numeric nor a StatefulSet pod name", raw)
	}
	parts := strings.Split(raw, "-")
	ordinalText = parts[len(parts)-1]
	ordinal, err := strconv.ParseUint(ordinalText, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse StatefulSet ordinal from %q: %w", raw, err)
	}
	return ordinal + 1, nil
}

func parseRaftBootstrapPeers(spec string) ([]metadata.RaftPeer, error) {
	pairs, err := parseRaftPeerSpecs(spec)
	if err != nil {
		return nil, err
	}
	peers := make([]metadata.RaftPeer, 0, len(pairs))
	for _, pair := range pairs {
		peers = append(peers, metadata.RaftPeer{ID: pair.id, Address: pair.value})
	}
	return peers, nil
}

func parseRaftPeerOpsURLs(spec string) (map[string]string, error) {
	pairs, err := parseRaftPeerSpecs(spec)
	if err != nil {
		return nil, err
	}
	peerOps := make(map[string]string, len(pairs))
	for _, pair := range pairs {
		peerOps[pair.id] = pair.value
	}
	return peerOps, nil
}

type raftPeerSpec struct {
	id    string
	value string
}

func parseRaftPeerSpecs(spec string) ([]raftPeerSpec, error) {
	spec = strings.TrimSpace(spec)
	if spec == "" {
		return nil, nil
	}

	entries := strings.Split(spec, ",")
	pairs := make([]raftPeerSpec, 0, len(entries))
	for _, entry := range entries {
		entry = strings.TrimSpace(entry)
		id, value, ok := strings.Cut(entry, "=")
		if !ok {
			return nil, fmt.Errorf("peer %q must use id=value format", entry)
		}
		id = strings.TrimSpace(id)
		value = strings.TrimSpace(value)
		if id == "" || value == "" {
			return nil, fmt.Errorf("peer %q must include non-empty id and value", entry)
		}
		pairs = append(pairs, raftPeerSpec{id: id, value: value})
	}
	return pairs, nil
}

type backupRuntimeConfig struct {
	Enabled       bool
	RaftEnabled   bool
	ClusterID     string
	LocalDir      string
	Interval      time.Duration
	Retention     int
	S3Bucket      string
	S3Prefix      string
	S3Region      string
	S3Endpoint    string
	UploadTimeout time.Duration
	StagingMaxAge time.Duration
}

type backupCoordinatorLifecycle interface {
	Start()
	Stop()
}

func createBackupCoordinatorRuntime(
	cfg backupRuntimeConfig,
	store *metadata.PebbleStore,
	repositoryFactory func(metadata.S3Config) (metadata.BackupRepository, error),
	coordinatorFactory func(
		metadata.BackupCoordinatorConfig,
		*metadata.PebbleStore,
		metadata.BackupRepository,
	) backupCoordinatorLifecycle,
) (backupCoordinatorLifecycle, error) {
	if !cfg.Enabled {
		return nil, nil
	}
	repository, err := repositoryFactory(metadata.S3Config{
		Bucket:   cfg.S3Bucket,
		Prefix:   cfg.S3Prefix,
		Region:   cfg.S3Region,
		Endpoint: cfg.S3Endpoint,
	})
	if err != nil {
		return nil, fmt.Errorf("create backup repository: %w", err)
	}
	coordinator := coordinatorFactory(metadata.BackupCoordinatorConfig{
		ClusterID:     cfg.ClusterID,
		Interval:      cfg.Interval,
		Retention:     cfg.Retention,
		LocalTempDir:  cfg.LocalDir,
		StagingMaxAge: cfg.StagingMaxAge,
		UploadTimeout: cfg.UploadTimeout,
	}, store, repository)
	if coordinator == nil {
		return nil, fmt.Errorf("create backup coordinator: factory returned nil")
	}
	return coordinator, nil
}

func validateBackupRuntimeConfig(cfg backupRuntimeConfig) (backupRuntimeConfig, error) {
	if !cfg.Enabled {
		return backupRuntimeConfig{}, nil
	}
	if !cfg.RaftEnabled {
		return backupRuntimeConfig{}, fmt.Errorf("backup requires Raft to be enabled")
	}
	cfg.ClusterID = strings.TrimSpace(cfg.ClusterID)
	if err := validateBackupClusterID(cfg.ClusterID); err != nil {
		return backupRuntimeConfig{}, err
	}
	cfg.S3Bucket = strings.TrimSpace(cfg.S3Bucket)
	if cfg.S3Bucket == "" {
		return backupRuntimeConfig{}, fmt.Errorf("backup S3 bucket is required")
	}
	if cfg.Interval <= 0 {
		return backupRuntimeConfig{}, fmt.Errorf("backup interval must be positive")
	}
	if cfg.Retention < 1 {
		return backupRuntimeConfig{}, fmt.Errorf("backup retention must be at least 1")
	}
	if cfg.UploadTimeout <= 0 {
		return backupRuntimeConfig{}, fmt.Errorf("backup upload timeout must be positive")
	}
	if cfg.StagingMaxAge <= 0 {
		return backupRuntimeConfig{}, fmt.Errorf("backup staging max age must be positive")
	}

	cfg.LocalDir = strings.TrimSpace(cfg.LocalDir)
	if cfg.LocalDir == "" {
		return backupRuntimeConfig{}, fmt.Errorf("backup local directory is required")
	}
	absolute, err := filepath.Abs(cfg.LocalDir)
	if err != nil {
		return backupRuntimeConfig{}, fmt.Errorf("resolve backup local directory: %w", err)
	}
	cfg.LocalDir = filepath.Clean(absolute)
	if err := os.MkdirAll(cfg.LocalDir, 0o700); err != nil {
		return backupRuntimeConfig{}, fmt.Errorf("create backup local directory: %w", err)
	}
	info, err := os.Stat(cfg.LocalDir)
	if err != nil {
		return backupRuntimeConfig{}, fmt.Errorf("inspect backup local directory: %w", err)
	}
	if !info.IsDir() {
		return backupRuntimeConfig{}, fmt.Errorf("backup local path is not a directory")
	}
	probe, err := os.CreateTemp(cfg.LocalDir, ".nufs-backup-write-test-*")
	if err != nil {
		return backupRuntimeConfig{}, fmt.Errorf("backup local directory is not writable: %w", err)
	}
	probeName := probe.Name()
	if err := probe.Sync(); err != nil {
		_ = probe.Close()
		_ = os.Remove(probeName)
		return backupRuntimeConfig{}, fmt.Errorf("sync backup local directory probe: %w", err)
	}
	if err := probe.Close(); err != nil {
		_ = os.Remove(probeName)
		return backupRuntimeConfig{}, fmt.Errorf("close backup local directory probe: %w", err)
	}
	if err := os.Remove(probeName); err != nil {
		return backupRuntimeConfig{}, fmt.Errorf("remove backup local directory probe: %w", err)
	}

	cfg.S3Prefix, err = normalizeBackupS3Prefix(cfg.S3Prefix)
	if err != nil {
		return backupRuntimeConfig{}, err
	}
	cfg.S3Region = strings.TrimSpace(cfg.S3Region)
	cfg.S3Endpoint = strings.TrimSpace(cfg.S3Endpoint)
	if cfg.S3Endpoint != "" {
		parsed, parseErr := url.ParseRequestURI(cfg.S3Endpoint)
		if parseErr != nil || parsed.Host == "" ||
			(parsed.Scheme != "http" && parsed.Scheme != "https") ||
			parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
			return backupRuntimeConfig{}, fmt.Errorf("invalid backup S3 endpoint %q", cfg.S3Endpoint)
		}
		cfg.S3Endpoint = strings.TrimSuffix(cfg.S3Endpoint, "/")
	}
	return cfg, nil
}

func validateBackupClusterID(id string) error {
	if id == "" {
		return fmt.Errorf("backup cluster ID is required")
	}
	if len(id) > 255 {
		return fmt.Errorf("backup cluster ID exceeds 255 bytes")
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		alphaNumeric := c >= 'a' && c <= 'z' ||
			c >= 'A' && c <= 'Z' ||
			c >= '0' && c <= '9'
		if i == 0 && !alphaNumeric {
			return fmt.Errorf("backup cluster ID must start with an ASCII letter or digit")
		}
		if !alphaNumeric && c != '-' && c != '_' && c != '.' && c != ':' {
			return fmt.Errorf("backup cluster ID contains an unsafe byte")
		}
	}
	return nil
}

func normalizeBackupS3Prefix(prefix string) (string, error) {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		return "", nil
	}
	normalized := strings.TrimSuffix(prefix, "/")
	if normalized == "" || strings.HasPrefix(normalized, "/") ||
		strings.Contains(normalized, `\`) || path.Clean(normalized) != normalized {
		return "", fmt.Errorf("invalid backup S3 prefix %q", prefix)
	}
	for _, part := range strings.Split(normalized, "/") {
		if part == "" || part == "." || part == ".." {
			return "", fmt.Errorf("invalid backup S3 prefix %q", prefix)
		}
	}
	return normalized, nil
}

// parsePeerOpsURLsForValidation counts Raft peer nodes from the bootstrap peers string.
func parsePeerOpsURLsForValidation(peers string) []string {
	if peers == "" {
		return nil
	}
	var result []string
	for i, start := 0, 0; ; i++ {
		idx := indexOfByte(peers[start:], ',')
		if idx < 0 {
			part := peers[start:]
			if part != "" {
				result = append(result, part)
			}
			break
		}
		result = append(result, peers[start:start+idx])
		start += idx + 1
	}
	return result
}

func indexOfByte(s string, b byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == b {
			return i
		}
	}
	return -1
}

// runtimeMode returns RuntimeDev when allowInsecureDev is true, otherwise
// RuntimeProduction.
func runtimeMode(allowInsecureDev bool) metadata.RuntimeMode {
	if allowInsecureDev {
		return metadata.RuntimeDev
	}
	return metadata.RuntimeProduction
}
