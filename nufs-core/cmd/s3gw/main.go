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
		metaDir    = flag.String("meta-dir", "/var/lib/dfs/metadata", "Pebble metadata directory")
		accessKey  = flag.String("access-key", "", "Access key for auth (empty = anonymous)")
		secretKey  = flag.String("secret-key", "", "Secret key for auth")
	)
	flag.Parse()

	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Lshortfile)
	log.Println("s3gw: starting S3 gateway...")

	// Create metadata store (PebbleStore)
	store, err := metadata.NewPebbleStore(metadata.PebbleStoreConfig{
		Dir: *metaDir,
	})
	if err != nil {
		log.Fatalf("s3gw: failed to create metadata store: %v", err)
	}
	defer store.Close()

	// Setup credentials
	creds := gos3.NewCredentialStore()
	if *accessKey != "" && *secretKey != "" {
		creds.AddCredential(*accessKey, *secretKey)
		log.Println("s3gw: auth enabled")
	} else {
		log.Println("s3gw: running in anonymous mode (no auth)")
	}

	// Create gateway
	gw := gos3.NewGateway(gos3.GatewayConfig{
		MetaService: store,
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
