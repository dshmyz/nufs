//go:build linux

// fusegw is the FUSE filesystem gateway daemon for DFS.
package main

import (
	"flag"
	"os"
	"os/signal"
	"syscall"

	gofuse "github.com/example/dfs/gateway/fuse"
	"github.com/example/dfs/gateway/s3"
	"github.com/example/dfs/internal/logging"
	"github.com/example/dfs/metadata"
)

func main() {
	var (
		mountpoint = flag.String("mount", "/mnt/dfs", "FUSE mount point")
		metaDir    = flag.String("meta-dir", "/var/lib/dfs/metadata", "Pebble metadata directory")
		cacheDir   = flag.String("cache-dir", "", "Chunk cache directory (empty=memory only)")
	)
	flag.Parse()
	logging.Init(logging.Config{Level: "info", AddSource: true})
	log := logging.Named("fusegw")

	log.Info("starting FUSE gateway")

	if _, err := os.Stat(*mountpoint); os.IsNotExist(err) {
		if err := os.MkdirAll(*mountpoint, 0755); err != nil {
			log.Error("failed to create mountpoint", "mountpoint", *mountpoint, "error", err)
			os.Exit(1)
		}
	}

	store, err := metadata.NewPebbleStore(metadata.PebbleStoreConfig{Dir: *metaDir})
	if err != nil {
		log.Error("failed to create metadata store", "error", err)
		os.Exit(1)
	}
	defer store.Close()

	chunkStore := s3.NewMemoryChunkStore()

	var chunkCache *gofuse.ChunkCache
	if *cacheDir != "" {
		chunkCache, err = gofuse.NewChunkCache(*cacheDir)
		if err != nil {
			log.Error("failed to create chunk cache", "error", err)
			os.Exit(1)
		}
		log.Info("chunk cache enabled", "dir", *cacheDir)
	}

	server, err := gofuse.Mount(*mountpoint, store, chunkStore, chunkCache, nil)
	if err != nil {
		log.Error("failed to mount", "error", err)
		os.Exit(1)
	}
	defer server.Unmount()

	log.Info("mounted", "mountpoint", *mountpoint)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh

	log.Info("received signal, unmounting", "signal", sig)
	if err := server.Unmount(); err != nil {
		log.Warn("unmount error", "error", err)
	}
	log.Info("stopped")
}
