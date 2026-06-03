//go:build linux

// nufs-fuse is the unified FUSE gateway daemon. It mounts either the DFS
// distributed filesystem (default) or an external S3 bucket as a local
// directory.
//
// Usage:
//
//	nufs-fuse --backend=dfs [flags] <mountpoint>
//	nufs-fuse --backend=s3 [flags] <s3-endpoint/bucket/prefix> <mountpoint>
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
	flag.Parse()
	logging.Init(logging.Config{Level: "info", AddSource: true})
	log := logging.Named("nufs-fuse")

	switch *backend {
	case "dfs":
		mountpoint := mountpointFromArgs(flag.Args())
		runDFS(log, mountpoint, *metaDir, *metaAddr, *cacheDir)
	case "s3":
		runS3(log, flag.Args(), *cacheDir, *scanTTL, *readOnly, *cacheQuota, *metricsAddr, *insecure, *debug, *uid, *gid)
	default:
		fmt.Fprintf(os.Stderr, "unknown backend: %q (use dfs or s3)\n", *backend)
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

// runDFS mounts the DFS distributed filesystem via FUSE.
func runDFS(log *slog.Logger, mountpoint, metaDir, metaAddr, cacheDir string) {
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

	chunkStore := s3.NewDatanodeChunkStore()

	var chunkCache *gofuse.ChunkCache
	if cacheDir != "" {
		var err error
		chunkCache, err = gofuse.NewChunkCache(cacheDir)
		if err != nil {
			log.Error("failed to create chunk cache", "error", err)
			os.Exit(1)
		}
		log.Info("chunk cache enabled", "dir", cacheDir)
	}

	server, err := gofuse.Mount(mountpoint, meta, chunkStore, chunkCache, nil)
	if err != nil {
		log.Error("failed to mount", "error", err)
		os.Exit(1)
	}
	defer server.Unmount()

	log.Info("mounted", "mountpoint", mountpoint, "backend", "dfs")

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
