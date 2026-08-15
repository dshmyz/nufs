//go:build linux

// nufs-fuse is the unified FUSE gateway daemon. It mounts either the DFS
// distributed filesystem (default) or an external S3 bucket as a local
// directory.
//
// Usage:
//
//	nufs-fuse --backend=dfs --bucket=<bucket> [flags] <mountpoint>
//	nufs-fuse --backend=s3 [flags] <s3-endpoint/bucket/prefix> <mountpoint>
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	// DFS backend
	"github.com/dshmyz/nufs/nufs-core/chunkstore"
	gofuse "github.com/dshmyz/nufs/nufs-core/gateway/fuse"
	"github.com/dshmyz/nufs/nufs-core/internal/tlsutil"
	"github.com/dshmyz/nufs/nufs-core/metadata"

	// S3 backend
	"github.com/dshmyz/nufs/nufs-core/gateway/s3fs"

	"github.com/dshmyz/nufs/nufs-core/internal/config"
	"github.com/dshmyz/nufs/nufs-core/internal/httputil"
	"github.com/dshmyz/nufs/nufs-core/internal/logging"
	"github.com/dshmyz/nufs/nufs-core/internal/resilience/breaker"
	"github.com/dshmyz/nufs/nufs-core/internal/resilience/retry"

	"github.com/hanwen/go-fuse/v2/fuse"
)

func main() {
	var (
		backend = flag.String("backend", "nufs", "Backend: dfs (distributed filesystem) or s3 (external S3 bucket)")

		// DFS backend flags
		metaDir  = flag.String("meta-dir", "/var/lib/dfs/metadata", "DFS: Pebble metadata directory (local mode)")
		metaAddr = flag.String("meta-addr", "", "DFS: Remote metadata address (host:port)")

		// S3 backend flags
		scanTTL     = flag.Duration("scan-ttl", 60*time.Second, "S3: Directory scan cache TTL")
		metricsAddr = flag.String("metrics-addr", ":9901", "Metrics/health HTTP address (DFS and S3; empty = disabled)")
		insecure    = flag.Bool("insecure", false, "S3: Skip TLS verification")

		// Shared flags
		cacheDir      = flag.String("cache-dir", "", "Cache directory (empty=memory only)")
		logLevel      = flag.String("log-level", "info", "Log level (debug/info/warn/error)")
		logJSON       = flag.Bool("log-json", false, "JSON log output")
		logFile       = flag.String("log-file", "", "Log file path (default: nufs-{mountpoint}.log; empty = that default)")
		logMaxSize    = flag.Int("log-max-size", 100, "Max log file size in MB before rotation")
		logMaxBackups = flag.Int("log-max-backups", 7, "Max number of rotated log files to keep")

		// Shared FUSE flags
		uid        = flag.Uint("uid", 0, "File owner UID (0 = caller's uid)")
		gid        = flag.Uint("gid", 0, "File owner GID (0 = caller's gid)")
		allowOther = flag.Bool("allow-other", false, "Allow other users to access the mount")
		readOnly   = flag.Bool("read-only", false, "Read-only mount (deny all writes with EROFS)")
		debug      = flag.Bool("debug", false, "Debug logging (verbose FUSE + metadata traces)")

		// DFS read/write cache memory limits — bound the fuse daemon's
		// resident memory used by its two caches. readCacheMax limits the
		// chunk read-cache (ChunkCache); writeCacheMax limits the per-file
		// dirty write buffers (cross-file, throttles the writer with ENOSPC
		// rather than evicting — dirty data must not be lost).
		readCacheMax  = flag.Int64("read-cache-max", 1<<30, "DFS: read cache memory limit in bytes (0=disabled, default 1GiB)")
		writeCacheMax = flag.Int64("write-cache-max", 2<<30, "DFS: dirty write buffer memory limit in bytes (0=disabled, default 2GiB)")

		// DFS DirectIO flag
		directIO = flag.Bool("direct-io", false, "DFS: Bypass kernel page cache (DirectIO)")

		// DFS bucket flag
		bucket = flag.String("bucket", "", "DFS: bucket to mount (required)")

		// DFS credential flags — exchange accessKey/secretKey for a signed
		// principal-bound token at mount time.
		accessKey = flag.String("access-key", "", "DFS: access key to authenticate the mount with metad")
		secretKey = flag.String("secret-key", "", "DFS: secret key to authenticate the mount with metad")

		// DFS TLS flags — mirror metad's TLS configuration so the FUSE
		// client can connect to a TLS-enabled metadata and datanode.
		// --insecure (shared with S3) skips server certificate verification;
		// --tls-cert/key/ca enable mutual TLS for full mTLS.
		tlsCert              = flag.String("tls-cert", "", "DFS: TLS certificate file (enables HTTPS for metadata + datanode)")
		tlsKey               = flag.String("tls-key", "", "DFS: TLS private key file")
		tlsCA                = flag.String("tls-ca", "", "DFS: CA certificate for mutual TLS (client verification)")
		tlsRequireClientCert = flag.Bool("tls-require-client-cert", false, "DFS: Require client certificate signed by --tls-ca")
	)
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage:\n")
		fmt.Fprintf(os.Stderr, "  %s --config nufs-fuse.yaml <mountpoint>\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s --backend=dfs --bucket=<bucket> [flags] <mountpoint>\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s --backend=s3 [flags] <s3-endpoint/bucket/prefix> <mountpoint>\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Backends:\n")
		fmt.Fprintf(os.Stderr, "  dfs  Mount the DFS distributed filesystem (default)\n")
		fmt.Fprintf(os.Stderr, "  s3   Mount an external S3 bucket\n\n")
		fmt.Fprintf(os.Stderr, "Configuration:\n")
		fmt.Fprintf(os.Stderr, "  --config loads a YAML/JSON/TOML file; CLI flags override file values.\n")
		fmt.Fprintf(os.Stderr, "  Credential precedence: --access-key > --config file > META_* env.\n")
		fmt.Fprintf(os.Stderr, "  Secrets should NOT be passed via CLI (visible in ps(1)); use env or --config.\n\n")
		fmt.Fprintf(os.Stderr, "Flags:\n")
		flag.PrintDefaults()
	}
	config.Preload()
	flag.Parse()

	// Derive default log-file from mountpoint when --log-file is unset.
	logFileVal := *logFile
	if logFileVal == "" && len(flag.Args()) > 0 {
		logFileVal = "nufs-" + sanitizeForFilename(flag.Args()[0]) + ".log"
	}
	logging.Init(logging.Config{
		Level:      *logLevel,
		JSON:       *logJSON,
		AddSource:  true,
		LogFile:    logFileVal,
		MaxSize:    int64(*logMaxSize) * 1024 * 1024, // flag is MB, Config takes bytes
		MaxBackups: *logMaxBackups,
	})
	log := logging.Named("nufs-fuse")

	// Warn when secrets are passed via CLI (visible in ps(1)).
	for _, arg := range os.Args[1:] {
		if strings.HasPrefix(arg, "--secret-key=") {
			log.Warn("secret passed via CLI flag — visible in ps(1); prefer env or --config file in production")
			break
		}
	}

	switch *backend {
	case "nufs":
		mountpoint := mountpointFromArgs(flag.Args())
		runNUFS(log, &dfsMountArgs{
			mountpoint:    mountpoint,
			metaDir:       *metaDir,
			metaAddr:      *metaAddr,
			cacheDir:      *cacheDir,
			readCacheMax:  *readCacheMax,
			writeCacheMax: *writeCacheMax,
			metricsAddr:   *metricsAddr,
			directIO:      *directIO,
			bucket:        *bucket,
			accessKey:     *accessKey,
			secretKey:     *secretKey,
			tls: tlsutil.Config{
				CertFile:          *tlsCert,
				KeyFile:           *tlsKey,
				CAFile:            *tlsCA,
				SkipVerify:        *insecure,
				RequireClientCert: *tlsRequireClientCert,
			},
			uid:        uint32(*uid),
			gid:        uint32(*gid),
			allowOther: *allowOther,
			readOnly:   *readOnly,
			debug:      *debug,
		})
	case "s3":
		s3Args := flag.Args()
		runS3(log, &s3MountArgs{
			target:        mountpointFromArgs(s3Args),
			mountpoint:    s3Args[1],
			cacheDir:      *cacheDir,
			readCacheMax:  *readCacheMax,
			writeCacheMax: *writeCacheMax,
			scanTTL:       *scanTTL,
			accessKey:     *accessKey,
			secretKey:     *secretKey,
			metricsAddr:   *metricsAddr,
			allowOther:    *allowOther,
			readOnly:      *readOnly,
			insecure:      *insecure,
			debug:         *debug,
			uid:           uint32(*uid),
			gid:           uint32(*gid),
		})
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

// sanitizeForFilename converts a mountpoint path into a safe log-file
// suffix: takes the last path component, replaces non-alphanumerics with
// underscores, and strips leading underscores (from leading slashes).
//
//	/mnt/mybucket   → mybucket
//	/mnt/my bucket  → my_bucket
//	/mnt/data       → data
func sanitizeForFilename(mountpoint string) string {
	base := filepath.Base(mountpoint)
	var b strings.Builder
	for _, r := range base {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' {
			b.WriteRune(r)
		} else {
			b.WriteByte('_')
		}
	}
	s := strings.TrimLeft(b.String(), "_")
	if s == "" {
		s = "default"
	}
	return s
}

// dfsMountArgs are the DFS-backend mount parameters, assembled from CLI flags
// in main() and passed to runNUFS as a single value instead of a long flat
// argument list.
type dfsMountArgs struct {
	mountpoint    string
	metaDir       string
	metaAddr      string
	cacheDir      string
	readCacheMax  int64
	writeCacheMax int64
	metricsAddr   string
	directIO      bool
	bucket        string
	accessKey     string
	secretKey     string
	tls           tlsutil.Config
	uid, gid      uint32
	allowOther    bool
	readOnly      bool
	debug         bool
}

// runNUFS mounts the DFS distributed filesystem via FUSE.
func runNUFS(log *slog.Logger, a *dfsMountArgs) {
	if a.bucket == "" {
		log.Error("DFS backend requires --bucket=<name>")
		os.Exit(1)
	}
	if _, err := os.Stat(a.mountpoint); os.IsNotExist(err) {
		if err := os.MkdirAll(a.mountpoint, 0755); err != nil {
			log.Error("failed to create mountpoint", "mountpoint", a.mountpoint, "error", err)
			os.Exit(1)
		}
	}

	// Credential source: explicit flags/config are primary; environment
	// variables serve as a fallback for k8s Secret injection.
	ak, sk := a.accessKey, a.secretKey
	if ak == "" {
		ak = os.Getenv("META_ACCESS_KEY")
	}
	if sk == "" {
		sk = os.Getenv("META_SECRET_KEY")
	}

	// mountState holds the mutable mount state for remount support.
	state := &nufsMountState{
		cfg: mountConfig{
			log:           log,
			mountpoint:    a.mountpoint,
			metaDir:       a.metaDir,
			cacheDir:      a.cacheDir,
			readCacheMax:  a.readCacheMax,
			writeCacheMax: a.writeCacheMax,
			directIO:      a.directIO,
			bucket:        a.bucket,
			mountUID:      a.uid,
			mountGID:      a.gid,
			tls:           a.tls,
			allowOther:    a.allowOther,
			readOnly:      a.readOnly,
			debug:         a.debug,
		},
		metaAddr:  a.metaAddr,
		accessKey: ak,
		secretKey: sk,
	}

	state.mu.Lock()
	err := state.mount()
	state.mu.Unlock()
	if err != nil {
		log.Error("failed to initial mount", "error", err)
		os.Exit(1)
	}

	// Start control HTTP server for remount + status.
	if a.metricsAddr != "" {
		startControlServer(log, a.metricsAddr, state)
	}

	log.Info("mounted", "mountpoint", a.mountpoint, "bucket", a.bucket, "backend", "nufs")

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh

	log.Info("received signal, unmounting", "signal", sig)
	state.mu.Lock()
	state.unmount()
	state.mu.Unlock()
}

// mountConfig holds the immutable mount parameters, separate from the
// per-mount runtime state below. It never changes after construction.
type mountConfig struct {
	log           *slog.Logger
	mountpoint    string
	metaDir       string
	cacheDir      string
	readCacheMax  int64
	writeCacheMax int64
	directIO      bool
	bucket        string
	mountUID      uint32
	mountGID      uint32
	tls           tlsutil.Config
	allowOther    bool
	readOnly      bool
	debug         bool
}

// nufsMountState holds the mutable mount state for remount support. Config
// is split out into mountConfig so a field here means "the mount is live or
// its secrets are in flux", not "another static parameter".
type nufsMountState struct {
	cfg mountConfig
	mu  sync.Mutex // protects all mutable fields below

	// metaAddr is mutable: remount() hot-swaps the authoritative endpoint.
	metaAddr string
	// credential inputs driving re-authentication on remount.
	accessKey string
	secretKey string
	// The live signed bearer, replaced by exchangeAndStoreToken on mount
	// and remount. Used only for /status reporting.
	bearerToken    string
	mountedAt      time.Time
	tokenExpiresAt time.Time

	// tokenRefreshCancel stops the background token-refresh goroutine.
	// nil when no refresh is running (local mode or no credentials).
	tokenRefreshCancel context.CancelFunc

	server   *fuse.Server
	fsys     *gofuse.DFSFileSystem
	meta     metadata.MetadataService
	cache    *gofuse.ChunkCache
	recorder gofuse.MetricsRecorder
}

// mount creates a new FUSE mount. Caller must hold mu.
func (s *nufsMountState) mount() error {
	s.mountedAt = time.Now()
	var meta metadata.MetadataService
	var ownerForMount string
	if s.metaAddr != "" {
		scheme := "http"
		if s.cfg.tls.CertFile != "" || s.cfg.tls.SkipVerify {
			scheme = "https"
		}
		client := metadata.NewHTTPClient(scheme+"://"+s.metaAddr, 30*time.Second)

		if s.cfg.tls.CertFile != "" || s.cfg.tls.SkipVerify {
			if err := client.EnableTLS(s.cfg.tls); err != nil {
				return fmt.Errorf("enable TLS for metadata client: %w", err)
			}
		}

		// Exchange accessKey/secretKey for a signed, principal-bound token.
		// The verified principal drives RBAC; an empty owner means no RBAC
		// boundary (local/dev mode without credentials).
		if s.accessKey != "" && s.secretKey != "" {
			principal, err := s.exchangeAndStoreToken(client)
			if err != nil {
				return err
			}
			ownerForMount = principal
			s.startTokenRefresh()
		}

		meta = client
		s.cfg.log.Info("remote mode", "meta_addr", s.metaAddr, "principal", ownerForMount)
	} else {
		var err error
		meta, err = metadata.NewPebbleStore(metadata.PebbleStoreConfig{Dir: s.cfg.metaDir})
		if err != nil {
			return fmt.Errorf("metadata store: %w", err)
		}
		s.cfg.log.Info("local mode (development)", "dir", s.cfg.metaDir)
	}
	s.meta = meta

	chunkStore := chunkstore.NewDatanodeChunkStore()
	if s.cfg.tls.CertFile != "" || s.cfg.tls.SkipVerify {
		chunkStore.SetTLS(s.cfg.tls)
	}

	if s.cfg.cacheDir != "" {
		var err error
		s.cache, err = gofuse.NewChunkCacheWithQuota(s.cfg.cacheDir, 0, s.cfg.readCacheMax, gofuse.GlobalMetricsRecorder())
		if err != nil {
			meta.Close()
			return fmt.Errorf("chunk cache: %w", err)
		}
		s.cfg.log.Info("chunk cache enabled", "dir", s.cfg.cacheDir, "quota_bytes", s.cfg.readCacheMax)
	}

	s.recorder = gofuse.GlobalMetricsRecorder()
	reliability := gofuse.NewReliabilityWrapper(
		s.recorder,
		retry.Config{MaxAttempts: 4, BaseDelay: 500 * time.Millisecond, MaxDelay: 5 * time.Second},
		breaker.Config{Threshold: 5, Timeout: 30 * time.Second},
		s.cfg.log,
	)

	if s.cfg.directIO {
		gofuse.SetDirectIO(true)
		s.cfg.log.Info("DirectIO enabled")
	}

	server, fsys, err := gofuse.Mount(gofuse.MountOptions{
		Mountpoint:        s.cfg.mountpoint,
		Meta:              meta,
		ChunkStore:        chunkStore,
		Cache:             s.cache,
		Recorder:          s.recorder,
		Reliability:       reliability,
		FUSEOpts:          &fuse.MountOptions{AllowOther: s.cfg.allowOther, Name: "dfs", FsName: "dfs"},
		BucketName:        s.cfg.bucket,
		Owner:             ownerForMount,
		MountUID:          s.cfg.mountUID,
		MountGID:          s.cfg.mountGID,
		ReadOnly:          s.cfg.readOnly,
		GlobalDirtyBudget: s.cfg.writeCacheMax,
	})
	if err != nil {
		meta.Close()
		return fmt.Errorf("fuse mount: %w", err)
	}
	s.server = server
	s.fsys = fsys
	return nil
}

// exchangeAndStoreToken authenticates the client's credentials against metad,
// stores the returned bearer on the client, and returns the verified
// principal. The endpoint is pinned to the metad ops origin so the token
// request reaches the auth authority the fuse will actually talk to.
func (s *nufsMountState) exchangeAndStoreToken(client *metadata.HTTPClient) (string, error) {
	scheme := "http"
	if s.cfg.tls.CertFile != "" || s.cfg.tls.SkipVerify {
		scheme = "https"
	}
	authClient := metadata.NewHTTPClient(scheme+"://"+s.metaAddr, 30*time.Second)
	if s.cfg.tls.CertFile != "" || s.cfg.tls.SkipVerify {
		_ = authClient.EnableTLS(s.cfg.tls)
	}
	result, err := authClient.ExchangeCredential(context.Background(), s.accessKey, s.secretKey, s.cfg.bucket)
	if err != nil {
		return "", fmt.Errorf("authenticate with metad: %w", err)
	}
	if result.Token == "" || result.Principal == "" {
		return "", fmt.Errorf("metad returned an invalid token response")
	}
	client.SetAuthToken(result.Token)
	s.bearerToken = result.Token
	// Record expiry for /status and refresh scheduling. The FUSE does not
	// hold the signing key so we estimate from the TTL returned by metad.
	if result.TTLSeconds > 0 {
		s.tokenExpiresAt = time.Now().Add(time.Duration(result.TTLSeconds) * time.Second)
	}
	s.cfg.log.Info("authenticated with metad", "principal", result.Principal, "ttl_seconds", result.TTLSeconds)
	return result.Principal, nil
}

// tokenRefreshRetryDelay is how long to wait before retrying a failed
// proactive refresh. Short relative to the token TTL so a transient metad blip
// costs a retry rather than the whole refresh chain: giving up would let the
// token expire and turn a recoverable hiccup into a hard mount failure.
const tokenRefreshRetryDelay = 30 * time.Second

// startTokenRefresh starts a background goroutine that re-exchanges
// credentials before the current token expires. The refresh fires at 50%
// of the token TTL, giving ample margin. Caller must hold mu.
func (s *nufsMountState) startTokenRefresh() {
	// Cancel any prior refresh (e.g. after remount).
	if s.tokenRefreshCancel != nil {
		s.tokenRefreshCancel()
	}
	if s.accessKey == "" || s.secretKey == "" {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.tokenRefreshCancel = cancel

	// Read the expiry here, under the caller's lock, rather than in the
	// goroutine: exchangeAndStoreToken writes it from other goroutines.
	ttl := time.Until(s.tokenExpiresAt)
	if ttl <= 0 {
		ttl = metadata.DefaultTokenTTL()
	}
	refreshIn := ttl / 2
	log := s.cfg.log

	go func() {
		log.Info("token refresh scheduled", "in", refreshIn.Round(time.Second))
		for delay := refreshIn; ; delay = tokenRefreshRetryDelay {
			select {
			case <-ctx.Done():
				return
			case <-time.After(delay):
			}

			s.mu.Lock()
			// Refresh on the serving client itself, not a throwaway: the token
			// must land on the *metadata.HTTPClient wired into the mount
			// (s.fsys.Meta()), or the renewed token never reaches the data path
			// and the mount starts 401ing once the original token's TTL lapses.
			serving, ok := s.fsys.Meta().(*metadata.HTTPClient)
			if !ok {
				s.mu.Unlock()
				log.Error("token refresh: metadata client is not an HTTP client; giving up")
				return
			}
			_, err := s.exchangeAndStoreToken(serving)
			if err == nil {
				// Chain the next refresh off the new expiry, then hand the
				// lock back — startTokenRefresh cancels ctx, ending this loop.
				s.startTokenRefresh()
				s.mu.Unlock()
				log.Info("token refreshed proactively")
				return
			}
			s.mu.Unlock()
			log.Error("token refresh failed, will retry", "error", err, "retry_in", tokenRefreshRetryDelay)
		}
	}()
}

// unmount releases the current mount. Caller must hold mu.
func (s *nufsMountState) unmount() {
	if s.tokenRefreshCancel != nil {
		s.tokenRefreshCancel()
		s.tokenRefreshCancel = nil
	}
	if s.server != nil {
		s.server.Unmount()
		s.server = nil
	}
	if s.meta != nil {
		s.meta.Close()
		s.meta = nil
	}
	s.cache = nil
}

// remount 热切换：原子替换 metadata 客户端，不 unmount，不产生 EBADF。
// Caller must hold mu.
func (s *nufsMountState) remount(newMetaAddr string) error {
	if s.fsys == nil {
		return fmt.Errorf("filesystem not mounted")
	}

	if newMetaAddr == "" {
		newMetaAddr = s.metaAddr
	}

	s.metaAddr = newMetaAddr
	scheme := "http"
	if s.cfg.tls.CertFile != "" || s.cfg.tls.SkipVerify {
		scheme = "https"
	}
	newMeta := metadata.NewHTTPClient(scheme+"://"+newMetaAddr, 30*time.Second)
	if s.cfg.tls.CertFile != "" || s.cfg.tls.SkipVerify {
		_ = newMeta.EnableTLS(s.cfg.tls)
	}
	if s.accessKey != "" && s.secretKey != "" {
		// Stop any running refresh goroutine before re-authenticating.
		if s.tokenRefreshCancel != nil {
			s.tokenRefreshCancel()
			s.tokenRefreshCancel = nil
		}
		// Re-authenticate against the new authority; the old token was scoped
		// to its signer and the new node may hold a different signing key.
		if _, err := s.exchangeAndStoreToken(newMeta); err != nil {
			return err
		}
		s.startTokenRefresh()
	}

	s.fsys.SwapMetadata(newMeta)
	s.cfg.log.Info("metadata hot-swapped", "addr", newMetaAddr)
	return nil
}

// controlAuthed reports whether a control-plane mutating request is authorized.
//
// The control server listens on the metrics address (default 127.0.0.1:9900) and
// exposes /remount and /control/log-level, which can redirect the mount to an
// attacker-controlled metadata authority (and re-mint a token there) or flip
// logging off as an evasion. Those routes must not be reachable by an unrelated
// local process, so they require the operator credential: the mount's
// secretKey, compared in constant time. A mount with no credential (local-mode
// DFS without remote auth) has nothing to redirect and denies mutating control
// calls by default. Read-only /status and /healthz never call this.
func controlAuthed(r *http.Request, state *nufsMountState) bool {
	secret := state.secretKey
	if secret == "" {
		return false
	}
	return httputil.BearerTokenOK(r.Header.Get("Authorization"), secret)
}

// startControlServer starts the HTTP control server with /remount, /status and /healthz.
func startControlServer(log *slog.Logger, addr string, state *nufsMountState) {
	mux := http.NewServeMux()

	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		state.mu.Lock()
		defer state.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"mountpoint":    state.cfg.mountpoint,
			"meta_addr":     state.metaAddr,
			"mounted":       state.server != nil,
			"direct_io":     state.cfg.directIO,
			"mounted_at":    state.mountedAt.Format(time.RFC3339),
			"token_expires": state.tokenExpiresAt.Format(time.RFC3339),
		})
	})

	mux.HandleFunc("/control/log-level", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]interface{}{
				"level": logging.CurrentLevel(),
			})
		case http.MethodPost:
			// Mutating a running daemon's log level requires the operator
			// credential (the mount secretKey) so an unrelated local process
			// that can reach the control port cannot flip logging off as an
			// evasion. Read-only GET stays open.
			if !controlAuthed(r, state) {
				http.Error(w, "Unauthorized", http.StatusUnauthorized)
				return
			}
			var req struct {
				Level string `json:"level"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			valid := map[string]bool{"debug": true, "info": true, "warn": true, "error": true}
			if !valid[req.Level] {
				http.Error(w, "level must be one of: debug, info, warn, error", http.StatusBadRequest)
				return
			}
			logging.SetLevel(req.Level)
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]interface{}{
				"level": req.Level,
			})
			log.Info("log level changed", "new_level", req.Level)
		default:
			http.Error(w, "GET or POST required", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/remount", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST required", http.StatusMethodNotAllowed)
			return
		}
		// /remount redirects the mount to another metadata authority and re-mints
		// a token for that destination. Whoever can reach this port can exfiltrate
		// the mount to an attacker-controlled metad and capture its credentials, so
		// it must require the operator credential even on loopback.
		if !controlAuthed(r, state) {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		var req struct {
			MetaAddr string `json:"meta_addr,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		state.mu.Lock()
		defer state.mu.Unlock()

		log.Info("remounting", "new_meta_addr", req.MetaAddr)
		if err := state.remount(req.MetaAddr); err != nil {
			log.Error("remount failed", "error", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":     "remounted",
			"mountpoint": state.cfg.mountpoint,
			"meta_addr":  state.metaAddr,
		})
		log.Info("remount succeeded", "meta_addr", state.metaAddr)
	})

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		// Snapshot state under the lock, then release it: the metad probe below
		// can block for up to 3s, and a liveness scrape must not stall a
		// concurrent token refresh or remount.
		state.mu.Lock()
		mounted := state.server != nil
		metaAddr := state.metaAddr
		tokenExpires := state.tokenExpiresAt
		mountedAt := state.mountedAt
		state.mu.Unlock()

		healthy := true
		reasons := []string{}

		// Check 1: mount is alive.
		if !mounted {
			healthy = false
			reasons = append(reasons, "not mounted")
		}

		// Check 2: metadata service is reachable (quick GET /version).
		if metaAddr != "" {
			ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
			defer cancel()
			req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+metaAddr+"/version", nil)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				healthy = false
				reasons = append(reasons, "metadata unreachable: "+err.Error())
			} else {
				resp.Body.Close()
			}
		}

		// Check 3: token is not expired (if using credential flow).
		if !tokenExpires.IsZero() && time.Now().After(tokenExpires) {
			healthy = false
			reasons = append(reasons, "token expired")
		}

		w.Header().Set("Content-Type", "application/json")
		if !healthy {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			"healthy":       healthy,
			"reasons":       reasons,
			"token_expires": tokenExpires.Format(time.RFC3339),
			"mounted_at":    mountedAt.Format(time.RFC3339),
		})
	})

	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		log.Info("control server started", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("control server error", "error", err)
		}
	}()
}

// s3MountArgs are the S3-backend mount parameters, assembled from CLI flags
// in main() and passed to runS3 as a single value instead of a long flat
// argument list.
type s3MountArgs struct {
	target        string // <endpoint/bucket/prefix> position arg
	mountpoint    string
	cacheDir      string
	readCacheMax  int64
	writeCacheMax int64
	scanTTL       time.Duration
	accessKey     string
	secretKey     string
	metricsAddr   string
	allowOther    bool
	readOnly      bool
	insecure      bool
	debug         bool
	uid, gid      uint32
}

// runS3 mounts an external S3 bucket via FUSE.
func runS3(log *slog.Logger, args *s3MountArgs) {
	if args.target == "" {
		fmt.Fprintf(os.Stderr, "error: s3 backend requires <endpoint/bucket/prefix> <mountpoint>\n\n")
		flag.Usage()
		os.Exit(1)
	}

	target := args.target
	mountpoint := args.mountpoint

	u, bucket, basePath, err := s3fs.ParseTarget(target)
	if err != nil {
		log.Error("invalid target", "target", target, "error", err)
		os.Exit(1)
	}

	cfg := &s3fs.Config{
		Bucket:      bucket,
		BasePath:    basePath,
		Target:      u,
		CacheDir:    args.cacheDir,
		ScanTTL:     args.scanTTL,
		MetricsAddr: args.metricsAddr,
		ReadOnly:    args.readOnly,
		AccessKey:   args.accessKey,
		SecretKey:   args.secretKey,
		UID:         args.uid,
		GID:         args.gid,
		Insecure:    args.insecure,
		Debug:       args.debug,
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
