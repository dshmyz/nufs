// datanode is the chunk storage daemon for the distributed storage system.
// It manages local disk storage, handles read/write/replicate requests,
// and reports status to the metadata service via heartbeat.
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/example/dfs/datanode"
	"github.com/example/dfs/metadata"
)

func main() {
	var (
		nodeID     = flag.Uint64("node-id", 1, "Unique data node ID")
		listenAddr = flag.String("listen", "0.0.0.0:9100", "TCP listen address")
		dataDir    = flag.String("data-dir", "/var/lib/dfs/data", "Chunk storage root directory")
		metaAddr   = flag.String("metadata", "localhost:8091", "Metadata service HTTP address")
		rack       = flag.String("rack", "rack-1", "Rack identifier for topology placement")
		zone       = flag.String("zone", "zone-1", "Availability zone identifier")
		capacityGB = flag.Int64("capacity", 1000, "Node storage capacity in GB")
	)
	flag.Parse()

	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Lshortfile)

	cfg := datanode.Config{
		NodeID:              metadata.NodeID(*nodeID),
		ListenAddr:          *listenAddr,
		DataDir:             *dataDir,
		MetadataAddr:        *metaAddr,
		MetadataCacheDir:    "",
		HeartbeatInterval:   10 * time.Second,
		MaxConcurrentWrites: 64,
		MaxConcurrentReads:  256,
		Rack:                *rack,
		Zone:                *zone,
		Tier:                metadata.TierHot,
		CapacityGB:          *capacityGB,
	}

	log.Printf("datanode: starting (node_id=%d, addr=%s, data=%s)", cfg.NodeID, cfg.ListenAddr, cfg.DataDir)

	// --- 1. Initialize WAL (crash recovery) ---
	wal, err := datanode.NewWriteAheadLog(filepath.Join(cfg.DataDir, "wal"))
	if err != nil {
		log.Fatalf("datanode: failed to init WAL: %v", err)
	}
	defer wal.Close()

	// --- 2. Initialize chunk store ---
	chunkStore, err := datanode.NewChunkStore(cfg.DataDir, cfg.MaxConcurrentWrites, cfg.MaxConcurrentReads, wal)
	if err != nil {
		log.Fatalf("datanode: failed to init chunk store: %v", err)
	}
	totalBytes, chunkCount := chunkStore.Stats()
	log.Printf("datanode: chunk store ready (chunks=%d, bytes=%d)", chunkCount, totalBytes)

	// --- 3. Initialize disk manager ---
	diskManager, err := datanode.NewDiskManager(cfg.DataDir, chunkStore, cfg.CapacityGB, wal)
	if err != nil {
		log.Fatalf("datanode: failed to init disk manager: %v", err)
	}
	diskManager.Start()
	defer diskManager.Stop()

	// --- 4. Connect to metadata service (PebbleStore) ---
	// In production, the datanode connects to the metad service via HTTP/gRPC
	// For now, we use a direct PebbleStore connection for simplicity
	metaStore, err := metadata.NewPebbleStore(metadata.PebbleStoreConfig{
		Dir:    cfg.MetadataCacheDir,
		NodeID: uint64(cfg.NodeID),
	})
	if err != nil {
		log.Fatalf("datanode: failed to connect to metadata service: %v", err)
	}
	defer metaStore.Close()

	// Register this node with the metadata service
	err = metaStore.RegisterNode(context.Background(), &metadata.NodeInfo{
		ID:         cfg.NodeID,
		Addr:       cfg.ListenAddr,
		DataDir:    cfg.DataDir,
		Rack:       cfg.Rack,
		Zone:       cfg.Zone,
		Tier:       cfg.Tier,
		CapacityGB: cfg.CapacityGB,
	})
	if err != nil && err != metadata.ErrNodeAlreadyExists {
		log.Fatalf("datanode: failed to register node: %v", err)
	}
	log.Printf("datanode: registered with metadata service")

	// --- 5. Start TCP server ---
	server := datanode.NewServer(cfg, chunkStore)
	if err := server.Start(); err != nil {
		log.Fatalf("datanode: failed to start server: %v", err)
	}
	defer server.Stop()

	// --- 6. Start replicator ---
	replicator := datanode.NewReplicator(cfg.ListenAddr, 4)
	replicator.Start()
	defer replicator.Stop()

	// --- 7. Start chain replicator ---
	chainRepl := datanode.NewChainReplicator(cfg.ListenAddr, cfg.NodeID, 5*time.Second)

	// --- 8. Start anti-entropy engine ---
	antiEntropy := datanode.NewAntiEntropy(chunkStore, metaStore, cfg.NodeID)
	antiEntropy.Start(30 * time.Minute)
	defer antiEntropy.Stop()

	// --- 9. Start heartbeat reporter ---
	heartbeat := datanode.NewHeartbeatReporter(cfg, metaStore, chunkStore)
	heartbeat.Start()
	defer heartbeat.Stop()

	// --- 10. Start ops API ---
	opsServer := datanode.NewOpsServer(cfg, chunkStore, metaStore, diskManager, chainRepl, antiEntropy)
	opsServer.Start()
	defer opsServer.Stop()

	log.Printf("datanode: all components started successfully")

	// --- 11. Wait for shutdown signal ---
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	log.Printf("datanode: received signal %v, shutting down", sig)

	// Graceful shutdown (reverse order) is handled by defers
	log.Printf("datanode: shutdown complete")
}
