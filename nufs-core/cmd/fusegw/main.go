//go:build linux

// fusegw is the FUSE filesystem gateway daemon for DFS.
package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	gofuse "github.com/example/dfs/gateway/fuse"
	"github.com/example/dfs/gateway/s3"
	"github.com/example/dfs/metadata"
)

func main() {
	var (
		mountpoint = flag.String("mount", "/mnt/dfs", "FUSE mount point")
		metaDir    = flag.String("meta-dir", "/var/lib/dfs/metadata", "Pebble metadata directory")
		cacheDir   = flag.String("cache-dir", "", "Chunk cache directory (empty=memory only)")
	)
	flag.Parse()

	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Lshortfile)
	log.Println("fusegw: starting FUSE gateway...")

	// Validate mountpoint
	if _, err := os.Stat(*mountpoint); os.IsNotExist(err) {
		if err := os.MkdirAll(*mountpoint, 0755); err != nil {
			log.Fatalf("fusegw: failed to create mountpoint %s: %v", *mountpoint, err)
		}
	}

	// Create metadata store (PebbleStore)
	store, err := metadata.NewPebbleStore(metadata.PebbleStoreConfig{
		Dir: *metaDir,
	})
	if err != nil {
		log.Fatalf("fusegw: failed to create metadata store: %v", err)
	}
	defer store.Close()

	// Chunk store: commit 1.1 uses an in-process memory store so
	// fusegw is self-contained and easy to exercise from a single
	// VM. A later commit wires it up to a local datanode daemon
	// (or a remote DatanodeChunkStore) the same way the S3
	// gateway does it.
	chunkStore := s3.NewMemoryChunkStore()

	// Create chunk cache (optional)
	var chunkCache *gofuse.ChunkCache
	if *cacheDir != "" {
		chunkCache, err = gofuse.NewChunkCache(*cacheDir)
		if err != nil {
			log.Fatalf("fusegw: failed to create chunk cache: %v", err)
		}
		log.Printf("fusegw: chunk cache enabled at %s", *cacheDir)
	}

	// Mount FUSE filesystem
	server, err := gofuse.Mount(*mountpoint, store, chunkStore, chunkCache, nil)
	if err != nil {
		log.Fatalf("fusegw: failed to mount: %v", err)
	}
	defer server.Unmount()

	log.Printf("fusegw: mounted at %s", *mountpoint)

	// Wait for interrupt
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh

	log.Printf("fusegw: received signal %v, unmounting...", sig)
	if err := server.Unmount(); err != nil {
		log.Printf("fusegw: unmount error: %v", err)
	}
	log.Println("fusegw: stopped")
}
