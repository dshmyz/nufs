//go:build linux

// fusegw is the FUSE filesystem gateway daemon for DFS.
package main

import (
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	gofuse "github.com/example/dfs/gateway/fuse"
	"github.com/example/dfs/gateway/s3"
	"github.com/example/dfs/internal/logging"
	"github.com/example/dfs/metadata"
)

func main() {
	var (
		mountpoint = flag.String("mount", "/mnt/dfs", "FUSE mount point")
		metaDir    = flag.String("meta-dir", "/var/lib/dfs/metadata", "Pebble metadata directory (local mode)")
		metaAddr   = flag.String("meta-addr", "", "Remote metadata address (host:port, enables remote+DatanodeChunkStore)")
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

	var meta metadata.MetadataService

	if *metaAddr != "" {
		meta = metadata.NewHTTPClient("http://"+*metaAddr, 30*time.Second)
		log.Info("remote mode", "meta_addr", *metaAddr)
	} else {
		var err error
		meta, err = metadata.NewPebbleStore(metadata.PebbleStoreConfig{Dir: *metaDir})
		if err != nil {
			log.Error("failed to create metadata store", "error", err)
			os.Exit(1)
		}
		log.Info("local mode (development)", "dir", *metaDir)
	}
	defer meta.Close()

	chunkStore := s3.NewDatanodeChunkStore()

	var chunkCache *gofuse.ChunkCache
	if *cacheDir != "" {
		var err error
		chunkCache, err = gofuse.NewChunkCache(*cacheDir)
		if err != nil {
			log.Error("failed to create chunk cache", "error", err)
			os.Exit(1)
		}
		log.Info("chunk cache enabled", "dir", *cacheDir)
	}

	server, err := gofuse.Mount(*mountpoint, meta, chunkStore, chunkCache, nil)
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
