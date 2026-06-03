// nufs-s3 is the S3-compatible gateway daemon for DFS.
package main

import (
	"context"
	"flag"
	"os"
	"time"

	gos3 "github.com/example/dfs/gateway/s3"
	"github.com/example/dfs/internal/config"
	"github.com/example/dfs/internal/logging"
	"github.com/example/dfs/metadata"
)

func main() {
	var (
		configPath      = flag.String("config", "", "Path to YAML config file")
		listenAddr      = flag.String("listen", ":8080", "HTTP listen address")
		metaAddr        = flag.String("meta-addr", "localhost:8091", "Metadata service address (host:port)")
		accessKey       = flag.String("access-key", "", "Access key for auth (empty = anonymous)")
		secretKey       = flag.String("secret-key", "", "Secret key for auth")
		partDir         = flag.String("part-dir", "/var/lib/nufs-s3/parts", "Multipart upload temp directory (empty=in-memory)")
		maxObjectSize   = flag.Int64("max-object-size", gos3.DefaultMaxObjectSize, "Maximum single-shot PUT body size in bytes (5 GiB by default)")
		gracefulTimeout = flag.Duration("graceful-timeout", 30*time.Second, "Max time to wait for in-flight requests on shutdown")
		tlsCert         = flag.String("tls-cert", "", "TLS certificate file (enables HTTPS)")
		tlsKey          = flag.String("tls-key", "", "TLS private key file")
		rateLimit       = flag.Float64("rate-limit", 0, "Max requests/second per client IP (0 = unlimited)")
		rateLimitBurst  = flag.Int("rate-limit-burst", 0, "Rate limiter burst size (0 = same as rate-limit)")
		logLevel        = flag.String("log-level", "info", "Log level (debug/info/warn/error)")
		logJSON         = flag.Bool("log-json", false, "JSON log output")
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
		log.Info("auth enabled")
	} else {
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
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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
