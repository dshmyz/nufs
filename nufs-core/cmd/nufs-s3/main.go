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
	"github.com/dshmyz/nufs/nufs-core/metadata"
)

func main() {
	var (
		configPath          = flag.String("config", "", "Path to YAML config file")
		listenAddr          = flag.String("listen", ":8080", "HTTP listen address")
		metaAddr            = flag.String("meta-addr", "localhost:8091", "Metadata service address (host:port)")
		accessKey           = flag.String("access-key", "", "Access key for auth (empty = anonymous)")
		secretKey           = flag.String("secret-key", "", "Secret key for auth")
		credentialsFile     = flag.String("credentials-file", "", "Path to YAML credentials file (hot-reloadable)")
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

	meta := metadata.NewHTTPClient("http://"+*metaAddr, 30*time.Second)
	defer meta.Close()

	creds := gos3.NewCredentialStore()
	if *accessKey != "" && *secretKey != "" {
		creds.AddCredential(*accessKey, *secretKey)
		log.Info("auth enabled (CLI credentials)")
	}
	if *credentialsFile != "" {
		if err := creds.LoadCredentials(*credentialsFile); err != nil {
			log.Error("failed to load credentials file", "error", err)
			os.Exit(1)
		}
		log.Info("auth enabled (hot-reloadable credentials)", "file", *credentialsFile)
	}
	if !creds.HasCredentials() {
		log.Warn("running in anonymous mode (no auth)")
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
