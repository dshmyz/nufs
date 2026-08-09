//go:build linux

// nufs-fuse is the unified FUSE gateway daemon. It mounts either the DFS
// distributed filesystem (default) or an external S3 bucket as a local
// directory.
//
// Usage:
//
//	nufs-fuse --backend=dfs [flags] <mountpoint>
//	nufs-fuse --backend=s3 [flags] <s3-endpoint/bucket/prefix> <mountpoint>
package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	// DFS backend
	"github.com/dshmyz/nufs/nufs-core/chunkstore"
	gofuse "github.com/dshmyz/nufs/nufs-core/gateway/fuse"
	"github.com/dshmyz/nufs/nufs-core/metadata"

	// S3 backend
	"github.com/dshmyz/nufs/nufs-core/gateway/s3fs"

	"github.com/dshmyz/nufs/nufs-core/internal/config"
	"github.com/dshmyz/nufs/nufs-core/internal/logging"
	"github.com/dshmyz/nufs/nufs-core/internal/resilience/breaker"
	"github.com/dshmyz/nufs/nufs-core/internal/resilience/retry"
)

func main() {
	var (
		configPath = flag.String("config", "", "Path to YAML config file")
		backend    = flag.String("backend", "nufs", "Backend: dfs (distributed filesystem) or s3 (external S3 bucket)")

		// DFS backend flags
		metaDir  = flag.String("meta-dir", "/var/lib/dfs/metadata", "DFS: Pebble metadata directory (local mode)")
		metaAddr = flag.String("meta-addr", "", "DFS: Remote metadata address (host:port)")

		// S3 backend flags
		scanTTL     = flag.Duration("scan-ttl", 60*time.Second, "S3: Directory scan cache TTL")
		readOnly    = flag.Bool("read-only", false, "S3: Read-only mode")
		cacheQuota  = flag.Int64("cache-quota", 0, "S3: Cache disk quota in bytes (0=unlimited)")
		metricsAddr = flag.String("metrics-addr", ":9900", "S3: Metrics/health HTTP address")
		insecure    = flag.Bool("insecure", false, "S3: Skip TLS verification")
		debug       = flag.Bool("debug", false, "S3: Debug logging")
		uid         = flag.Uint("uid", 0, "S3: File owner UID")
		gid         = flag.Uint("gid", 0, "S3: File owner GID")

		// Shared flags
		cacheDir = flag.String("cache-dir", "", "Cache directory (empty=memory only)")
		logLevel = flag.String("log-level", "info", "Log level (debug/info/warn/error)")
		logJSON  = flag.Bool("log-json", false, "JSON log output")

		// DFS metrics flag
		dfsMetricsAddr = flag.String("dfs-metrics-addr", ":9901", "DFS: Metrics/health HTTP address (empty=disabled)")

		// DFS cache quota flag
		dfsCacheQuota = flag.Int64("dfs-cache-quota", 1<<30, "DFS: Chunk cache byte quota (0=unlimited, default 1GiB)")
	)
	_ = configPath
	config.Preload()
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage:\n")
		fmt.Fprintf(os.Stderr, "  %s --backend=dfs [flags] <mountpoint>\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s --backend=s3 [flags] <s3-endpoint/bucket/prefix> <mountpoint>\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Backends:\n")
		fmt.Fprintf(os.Stderr, "  dfs  Mount the DFS distributed filesystem (default)\n")
		fmt.Fprintf(os.Stderr, "  s3   Mount an external S3 bucket\n\n")
		fmt.Fprintf(os.Stderr, "Flags:\n")
		flag.PrintDefaults()
	}
	config.Preload()
	flag.Parse()
	logging.Init(logging.Config{Level: *logLevel, JSON: *logJSON, AddSource: true})
	log := logging.Named("nufs-fuse")

	switch *backend {
	case "nufs":
		mountpoint := mountpointFromArgs(flag.Args())
		runNUFS(log, mountpoint, *metaDir, *metaAddr, *cacheDir, *dfsCacheQuota, *dfsMetricsAddr)
	case "s3":
		runS3(log, flag.Args(), *cacheDir, *scanTTL, *readOnly, *cacheQuota, *metricsAddr, *insecure, *debug, *uid, *gid)
	default:
		fmt.Fprintf(os.Stderr, "unknown backend: %q (use nufs or s3)\n", *backend)
		os.Exit(1)
	}
}

// mountpointFromArgs returns the first positional arg as the mountpoint.
func mountpointFromArgs(args []string) string {
	if len(args) == 0 {
		fmt.Fprintf(os.Stderr, "error: mountpoint is required\n\n")
		flag.Usage()
		os.Exit(1)
	}
	return args[0]
}

// runNUFS mounts the DFS distributed filesystem via FUSE.
func runNUFS(log *slog.Logger, mountpoint, metaDir, metaAddr, cacheDir string, cacheQuota int64, metricsAddr string) {
	if _, err := os.Stat(mountpoint); os.IsNotExist(err) {
		if err := os.MkdirAll(mountpoint, 0755); err != nil {
			log.Error("failed to create mountpoint", "mountpoint", mountpoint, "error", err)
			os.Exit(1)
		}
	}

	var meta metadata.MetadataService
	if metaAddr != "" {
		meta = metadata.NewHTTPClient("http://"+metaAddr, 30*time.Second)
		log.Info("remote mode", "meta_addr", metaAddr)
	} else {
		var err error
		meta, err = metadata.NewPebbleStore(metadata.PebbleStoreConfig{Dir: metaDir})
		if err != nil {
			log.Error("failed to create metadata store", "error", err)
			os.Exit(1)
		}
		log.Info("local mode (development)", "dir", metaDir)
	}
	defer meta.Close()

	chunkStore := chunkstore.NewDatanodeChunkStore()

	var chunkCache *gofuse.ChunkCache
	if cacheDir != "" {
		var err error
		chunkCache, err = gofuse.NewChunkCacheWithQuota(cacheDir, 0, cacheQuota, gofuse.GlobalMetricsRecorder())
		if err != nil {
			log.Error("failed to create chunk cache", "error", err)
			os.Exit(1)
		}
		log.Info("chunk cache enabled", "dir", cacheDir, "quota_bytes", cacheQuota)
	}

	// 启动 metrics HTTP 端点（/metrics + /healthz）。
	// fuseMetrics 是 gofuse 包内的全局 FUSEMetrics 实例，实现 MetricsRecorder。
	if metricsAddr != "" {
		if srv := gofuse.StartMetricsServer(metricsAddr); srv != nil {
			log.Info("metrics server started", "addr", metricsAddr)
		}
	}

	// 构造 ReliabilityWrapper：retry + breaker + pathlock。
	// 默认重试 3 次（指数退避 500ms→5s），熔断阈值 5 次失败 / 30s 恢复。
	recorder := gofuse.GlobalMetricsRecorder()
	reliability := gofuse.NewReliabilityWrapper(
		recorder,
		retry.Config{
			MaxAttempts: 4,
			BaseDelay:   500 * time.Millisecond,
			MaxDelay:    5 * time.Second,
		},
		breaker.Config{
			Threshold: 5,
			Timeout:   30 * time.Second,
		},
	)

	server, err := gofuse.Mount(mountpoint, meta, chunkStore, chunkCache, recorder, reliability, nil)
	if err != nil {
		log.Error("failed to mount", "error", err)
		os.Exit(1)
	}
	defer server.Unmount()

	log.Info("mounted", "mountpoint", mountpoint, "backend", "nufs")

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh

	log.Info("received signal, unmounting", "signal", sig)
}

// runS3 mounts an external S3 bucket via FUSE.
func runS3(log *slog.Logger, args []string, cacheDir string, scanTTL time.Duration, readOnly bool, cacheQuota int64, metricsAddr string, insecure bool, debug bool, uid, gid uint) {
	if len(args) < 2 {
		fmt.Fprintf(os.Stderr, "error: s3 backend requires <endpoint/bucket/prefix> <mountpoint>\n\n")
		flag.Usage()
		os.Exit(1)
	}

	target := args[0]
	mountpoint := args[1]

	u, bucket, basePath, err := s3fs.ParseTarget(target)
	if err != nil {
		log.Error("invalid target", "target", target, "error", err)
		os.Exit(1)
	}

	cfg := &s3fs.Config{
		Bucket:      bucket,
		BasePath:    basePath,
		Target:      u,
		CacheDir:    cacheDir,
		ScanTTL:     scanTTL,
		MetricsAddr: metricsAddr,
		ReadOnly:    readOnly,
		CacheQuota:  cacheQuota,
		UID:         uint32(uid),
		GID:         uint32(gid),
		Insecure:    insecure,
		Debug:       debug,
	}
	if cfg.CacheDir == "" {
		cfg.CacheDir = "/var/lib/s3fs/cache"
	}

	log.Info("mounting", "target", target, "mountpoint", mountpoint, "backend", "s3")
	fs, err := s3fs.New(cfg)
	if err != nil {
		log.Error("failed to create filesystem", "error", err)
		os.Exit(1)
	}

	if err := fs.Serve(mountpoint); err != nil {
		log.Error("failed to serve", "error", err)
		os.Exit(1)
	}
}
