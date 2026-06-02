// s3gw is the S3-compatible gateway daemon for DFS.
package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	gos3 "github.com/example/dfs/gateway/s3"
	"github.com/example/dfs/metadata"
)

func main() {
	var (
		listenAddr = flag.String("listen", ":8080", "HTTP listen address")
		metaAddr   = flag.String("meta-addr", "localhost:8091", "Metadata service address (host:port)")
		accessKey  = flag.String("access-key", "", "Access key for auth (empty = anonymous)")
		secretKey  = flag.String("secret-key", "", "Secret key for auth")
	)
	flag.Parse()

	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Lshortfile)
	log.Printf("s3gw: starting S3 gateway (meta=%s)", *metaAddr)

	// Connect to remote metadata service
	meta := metadata.NewHTTPClient("http://"+*metaAddr, 30*time.Second)

	// Setup credentials
	creds := gos3.NewCredentialStore()
	if *accessKey != "" && *secretKey != "" {
		creds.AddCredential(*accessKey, *secretKey)
		log.Println("s3gw: auth enabled")
	} else {
		log.Println("s3gw: running in anonymous mode (no auth)")
	}

	defer meta.Close()

	// Create gateway
	gw := gos3.NewGateway(gos3.GatewayConfig{
		MetaService: meta,
		Creds:       creds,
	})

	// Create HTTP server
	srv := &http.Server{
		Addr:         *listenAddr,
		Handler:      gw.Handler(),
		ReadTimeout:  60 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	// Start server in background
	errCh := make(chan error, 1)
	go func() {
		log.Printf("s3gw: listening on %s", *listenAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	// Wait for interrupt signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		log.Printf("s3gw: received signal %v, shutting down...", sig)
	case err := <-errCh:
		log.Printf("s3gw: server error: %v", err)
	}

	// Graceful shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("s3gw: shutdown error: %v", err)
	}
	log.Println("s3gw: stopped")
}
