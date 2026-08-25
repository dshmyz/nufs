// nufs-s3 is the S3-compatible gateway daemon for DFS.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	gos3 "github.com/dshmyz/nufs/nufs-core/gateway/s3"
	"github.com/dshmyz/nufs/nufs-core/internal/config"
	"github.com/dshmyz/nufs/nufs-core/internal/logging"
	"github.com/dshmyz/nufs/nufs-core/internal/tlsutil"
	"github.com/dshmyz/nufs/nufs-core/metadata"
)

func main() {
	var (
		configPath          = flag.String("config", "", "Path to YAML config file")
		listenAddr          = flag.String("listen", ":8080", "HTTP listen address")
		metaAddr            = flag.String("meta-addr", "localhost:8091", "Metadata service address (host:port)")
		metaAuthToken       = flag.String("meta-auth-token", "", "Operator bearer token for the metad credential sync (--auth-token of metad). When set, the gateway pulls its credentials from the metad registry instead of local files/flags.")
		metaTLSCA           = flag.String("meta-tls-ca", "", "CA certificate file to trust the metad's TLS server cert (enables HTTPS to metad)")
		metaTLSSkipVerify   = flag.Bool("meta-tls-skip-verify", false, "Skip TLS server cert verification when connecting to metad (test only; prefer --meta-tls-ca in production)")
		credentialSyncInt   = flag.Duration("credential-sync-interval", 60*time.Second, "How often to refresh credentials from the metad registry (0 = startup pull only)")
		accessKey           = flag.String("access-key", "", "DEPRECATED: local access key fallback when --meta-auth-token is unset (empty = anonymous)")
		secretKey           = flag.String("secret-key", "", "DEPRECATED: local secret key fallback when --meta-auth-token is unset")
		credentialsFile     = flag.String("credentials-file", "", "DEPRECATED: YAML credentials file fallback (hot-reloadable) when --meta-auth-token is unset")
		partDir             = flag.String("part-dir", "/var/lib/nufs-s3/parts", "Multipart upload temp directory (empty=in-memory)")
		maxObjectSize       = flag.Int64("max-object-size", gos3.DefaultMaxObjectSize, "Maximum single-shot PUT body size in bytes (5 GiB by default)")
		gracefulTimeout     = flag.Duration("graceful-timeout", 30*time.Second, "Max time to wait for in-flight requests on shutdown")
		tlsCert             = flag.String("tls-cert", "", "TLS certificate file (enables HTTPS)")
		tlsKey              = flag.String("tls-key", "", "TLS private key file")
		rateLimit           = flag.Float64("rate-limit", 0, "Max requests/second per client IP (0 = unlimited)")
		rateLimitBurst      = flag.Int("rate-limit-burst", 0, "Rate limiter burst size (0 = same as rate-limit)")
		writeWorkers        = flag.Bool("write-workers", true, "Enable object write recovery and GC workers")
		writeWorkerInterval = flag.Duration("write-worker-interval", time.Minute, "Object write recovery/GC worker interval")
		writeWorkerLease    = flag.Duration("write-worker-lease", 30*time.Second, "Object write recovery/GC task lease duration")
		writeRecoveryLimit  = flag.Int("write-recovery-limit", 100, "Max write attempts to recover per worker tick")
		writeGCLimit        = flag.Int("write-gc-limit", 100, "Max write attempts to garbage collect per worker tick")
		writeGCAbandonAge   = flag.Duration("write-gc-abandon-age", time.Hour, "Age after which pending/allocated write attempts are considered abandoned")
		logLevel            = flag.String("log-level", "info", "Log level (debug/info/warn/error)")
		logJSON             = flag.Bool("log-json", false, "JSON log output")
	)
	_ = configPath
	config.Preload()
	flag.Parse()
	logging.Init(logging.Config{Level: *logLevel, JSON: *logJSON, AddSource: true})
	log := logging.Named("nufs-s3")

	log.Info("starting S3 gateway", "meta", *metaAddr, "listen", *listenAddr, "max_object_size", *maxObjectSize)

	metaScheme := "http"
	if *metaTLSCA != "" || *metaTLSSkipVerify {
		metaScheme = "https"
	}
	meta := metadata.NewHTTPClient(metaScheme+"://"+*metaAddr, 30*time.Second)
	defer meta.Close()
	if *metaTLSCA != "" || *metaTLSSkipVerify {
		if err := meta.EnableTLS(tlsutil.Config{CAFile: *metaTLSCA, SkipVerify: *metaTLSSkipVerify}); err != nil {
			log.Error("metad TLS config", "error", err)
			os.Exit(1)
		}
	}

	// When an operator token is configured, every metad call (data plane and
	// the credential sync) carries it: metad's BearerAuth gates non-public
	// routes (including the mutating bucket/ACL routes the gateway needs) with
	// that exact operator credential. The gateway already holds it to pull the
	// credential registry, so reusing it for the data plane is consistent with
	// the trust model — the S3 gateway is an operator-side service.
	if *metaAuthToken != "" {
		meta.SetAuthToken(*metaAuthToken)
	}

	creds := gos3.NewCredentialStore()
	var credSync *gos3.CredentialSyncer
	syncOK := false
	if *metaAuthToken != "" {
		// Primary credential source: the metad registry, pulled on start and
		// refreshed on --credential-sync-interval. The initial pull decides
		// whether sync is authoritative; on failure we fall back to the
		// legacy local sources below (never start anonymous when creds exist
		// in the registry but the sync hiccuped at boot).
		fetch := func(ctx context.Context) ([]metadata.GatewayCredential, error) {
			return meta.ListGatewayCredentials(ctx, *metaAuthToken)
		}
		credSync = gos3.NewCredentialSyncer(creds, fetch, *credentialSyncInt)
		if err := credSync.SyncOnce(context.Background()); err != nil {
			log.Warn("metad credential sync initial pull failed; falling back to local credentials", "error", err)
		} else {
			// Classify the boot-time posture explicitly: a successful sync
			// with ZERO credentials means auth is pinned on and every request
			// will be rejected (403) — never report that as "anonymous mode".
			posture := gos3.StartupAuthPosture(true, creds.Count())
			if posture == gos3.AuthPostureSyncedEmpty {
				log.Warn("metad credential sync returned zero credentials — auth is pinned on and EVERY request will be rejected (403); check `nufs-cli auth list` and that --credential-secret-key matches the metad deployment's key",
					"count", creds.Count(), "interval", *credentialSyncInt)
			} else {
				log.Info("auth enabled (metad registry sync)", "count", creds.Count(), "interval", *credentialSyncInt)
			}
			syncOK = true
			// Pin auth mode: once the registry is authoritative, revoking the
			// last credential must reject requests, not flip the gateway to
			// anonymous.
			creds.SetAuthMode(true)
		}
	}
	if !syncOK {
		// Deprecated local credential sources, only consulted when the metad
		// registry sync is not configured or failed at boot.
		if *accessKey != "" && *secretKey != "" {
			log.Warn("--access-key/--secret-key are deprecated; use `nufs-cli auth add` + --meta-auth-token to manage credentials in the metad registry")
			creds.AddCredential(*accessKey, *secretKey)
			log.Info("auth enabled (local CLI credentials, deprecated)")
		}
		if *credentialsFile != "" {
			log.Warn("--credentials-file is deprecated; use the metad registry credential sync (--meta-auth-token)")
			if err := creds.LoadCredentials(*credentialsFile); err != nil {
				log.Error("failed to load credentials file", "error", err)
				os.Exit(1)
			}
			log.Info("auth enabled (hot-reloadable credentials, deprecated)", "file", *credentialsFile)
		}
	}
	if !creds.HasCredentials() {
		// With the registry authoritative, an empty credential set is a locked
		// gate (auth pinned, 403 on every request), not an open one.
		if syncOK {
			log.Warn("auth is pinned on (metad registry sync) but zero credentials are loaded — every request will be rejected (403)")
		} else {
			log.Warn("running in anonymous mode (no auth)")
		}
	}

	health := func(ctx context.Context) error {
		cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		_, err := meta.ListBuckets(cctx)
		return err
	}

	gw := gos3.NewGateway(gos3.GatewayConfig{
		MetaService:         meta,
		Creds:               creds,
		PartDir:             *partDir,
		MaxObjectSize:       *maxObjectSize,
		RejectEmptyReplicas: true,
		HealthCheck:         health,
		ReadyCheck:          health,
		RateLimit:           *rateLimit,
		RateLimitBurst:      *rateLimitBurst,
		BackgroundWorkers: gos3.ObjectWriteBackgroundWorkerConfig{
			Enabled:       *writeWorkers,
			Interval:      *writeWorkerInterval,
			Lease:         *writeWorkerLease,
			RecoveryLimit: *writeRecoveryLimit,
			GCLimit:       *writeGCLimit,
			GCAbandonAge:  *writeGCAbandonAge,
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Preload bucket policies from the metad registry so authorization works
	// from the first request (not just after this process created buckets).
	gw.LoadPolicies(ctx)

	// Metad credential registry sync (when authoritative): refresh on
	// --credential-sync-interval so registry-side adds/revocations reach the
	// gateway within one interval. Run() re-pulls immediately, then ticks.
	if syncOK && credSync != nil {
		go func() {
			if err := credSync.Run(ctx); err != nil && err != context.Canceled {
				log.Error("credential sync stopped", "error", err)
			}
		}()
	}

	// Hot-reload: rate limit via main config file.
	if *configPath != "" {
		go func() {
			if err := config.Watch(ctx, *configPath, func() {
				rl := flag.Lookup("rate-limit")
				rb := flag.Lookup("rate-limit-burst")
				if rl != nil && rb != nil {
					rps, burst := parseRateLimitFlags(rl.Value.String(), rb.Value.String())
					gw.SetRateLimit(rps, burst)
					log.Info("rate limit updated from config", "rate", rps, "burst", burst)
				}
			}); err != nil && err != context.Canceled {
				log.Error("config watch error", "error", err)
			}
		}()
	}

	// Hot-reload: credentials file.
	if *credentialsFile != "" {
		go func() {
			if err := config.Watch(ctx, *credentialsFile, func() {
				if err := creds.LoadCredentials(*credentialsFile); err != nil {
					log.Error("credentials reload failed", "error", err)
					return
				}
				log.Info("credentials reloaded", "file", *credentialsFile, "count", creds.Count())
			}); err != nil && err != context.Canceled {
				log.Error("credentials watch error", "error", err)
			}
		}()
	}

	if err := gw.Run(ctx, gos3.ServerConfig{
		Addr:            *listenAddr,
		GracefulTimeout: *gracefulTimeout,
		TLSCertFile:     *tlsCert,
		TLSKeyFile:      *tlsKey,
	}); err != nil {
		log.Error("gateway exited with error", "error", err)
		os.Exit(1)
	}
}

func parseRateLimitFlags(rpsStr, burstStr string) (float64, int) {
	rps := 0.0
	burst := 0
	if rpsStr != "" {
		_, _ = fmt.Sscanf(rpsStr, "%f", &rps)
	}
	if burstStr != "" {
		_, _ = fmt.Sscanf(burstStr, "%d", &burst)
	}
	return rps, burst
}
