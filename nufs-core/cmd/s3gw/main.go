// s3gw is the S3-compatible gateway daemon for DFS.
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"time"

	gos3 "github.com/example/dfs/gateway/s3"
	"github.com/example/dfs/metadata"
)

func main() {
	var (
		listenAddr    = flag.String("listen", ":8080", "HTTP listen address")
		metaAddr      = flag.String("meta-addr", "localhost:8091", "Metadata service address (host:port)")
		accessKey     = flag.String("access-key", "", "Access key for auth (empty = anonymous)")
		secretKey     = flag.String("secret-key", "", "Secret key for auth")
		maxObjectSize = flag.Int64("max-object-size", gos3.DefaultMaxObjectSize,
			"Maximum single-shot PUT body size in bytes (5 GiB by default)")
		gracefulTimeout = flag.Duration("graceful-timeout", 30*time.Second,
			"Max time to wait for in-flight requests on shutdown")
	)
	flag.Parse()

	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Lshortfile)
	log.Printf("s3gw: starting S3 gateway (meta=%s, listen=%s, max-object-size=%d)",
		*metaAddr, *listenAddr, *maxObjectSize)

	// Connect to remote metadata service.
	meta := metadata.NewHTTPClient("http://"+*metaAddr, 30*time.Second)
	defer meta.Close()

	// Setup credentials.
	creds := gos3.NewCredentialStore()
	if *accessKey != "" && *secretKey != "" {
		creds.AddCredential(*accessKey, *secretKey)
		log.Println("s3gw: auth enabled")
	} else {
		log.Println("s3gw: running in anonymous mode (no auth)")
	}

	// Create gateway. A healthz check is wired that fails the probe if
	// the metadata service cannot answer a ListBuckets call within 5s;
	// readyz reuses the same check (the gateway is only "ready" once
	// the metadata tier is reachable).
	health := func(ctx context.Context) error {
		cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		_, err := meta.ListBuckets(cctx)
		return err
	}

	gw := gos3.NewGateway(gos3.GatewayConfig{
		MetaService:         meta,
		Creds:               creds,
		MaxObjectSize:       *maxObjectSize,
		RejectEmptyReplicas: true,
		HealthCheck:         health,
		ReadyCheck:          health,
	})

	// Run blocks until SIGINT/SIGTERM (or context cancellation) and
	// performs a graceful shutdown bounded by -graceful-timeout.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := gw.Run(ctx, gos3.ServerConfig{
		Addr:            *listenAddr,
		GracefulTimeout: *gracefulTimeout,
	}); err != nil {
		log.Printf("s3gw: %v", err)
		os.Exit(1)
	}
}
