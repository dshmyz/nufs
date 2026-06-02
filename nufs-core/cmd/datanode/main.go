package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/example/dfs/datanode"
	"github.com/example/dfs/metadata"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Lshortfile)

	if len(os.Args) >= 2 {
		switch os.Args[1] {
		case "status", "adopt", "retire":
			runManagementCommand(os.Args[1], os.Args[2:])
			return
		}
	}

	var (
		nodeID     = flag.Uint64("node-id", 1, "Unique data node ID")
		listenAddr = flag.String("listen", "0.0.0.0:9100", "TCP listen address")
		dataDir    = flag.String("data-dir", "/var/lib/dfs/data", "Chunk storage root directory")
		dataDirs   = flag.String("data-dirs", "", "Comma-separated data directories (enables supervisor mode)")
		basePort   = flag.Int("base-port", 9100, "Base port for supervisor mode children")
		machineID  = flag.String("machine-id", "", "Machine identifier for topology placement")
		metaAddr   = flag.String("metadata", "localhost:8091", "Metadata service HTTP address")
		rack       = flag.String("rack", "rack-1", "Rack identifier for topology placement")
		zone       = flag.String("zone", "zone-1", "Availability zone identifier")
		capacityGB = flag.Int64("capacity", 1000, "Node storage capacity in GB")
	)
	flag.Parse()

	// Supervisor mode
	if *dataDirs != "" {
		dirs := splitAndClean(*dataDirs)
		if len(dirs) == 0 {
			log.Fatalf("datanode: --data-dirs is empty")
		}
		mid := *machineID
		if mid == "" {
			mid = readMachineID()
		}
		runSupervisor(dirs, *basePort, mid, *metaAddr, *rack, *zone, *capacityGB)
		return
	}

	// Single-process mode (backward compatible)
	if *dataDir == "" {
		log.Fatalf("datanode: either --data-dir or --data-dirs must be set")
	}

	mid := *machineID
	if mid == "" {
		mid = readMachineID()
	}

	runDataNode(datanode.Config{
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
		MachineID:           mid,
		Tier:                metadata.TierHot,
		CapacityGB:          *capacityGB,
	})
}

func splitAndClean(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func readMachineID() string {
	b, err := os.ReadFile("/etc/machine-id")
	if err == nil {
		return strings.TrimSpace(string(b))
	}
	b, err = os.ReadFile("/sys/class/dmi/id/product_uuid")
	if err == nil {
		return strings.TrimSpace(string(b))
	}
	log.Printf("datanode: WARNING could not read machine-id, using hostname")
	h, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return h
}

func runDataNode(cfg datanode.Config) {
	log.Printf("datanode: starting (node_id=%d, addr=%s, data=%s, machine=%s)", cfg.NodeID, cfg.ListenAddr, cfg.DataDir, cfg.MachineID)

	wal, err := datanode.NewWriteAheadLog(filepath.Join(cfg.DataDir, "wal"))
	if err != nil {
		log.Fatalf("datanode: failed to init WAL: %v", err)
	}
	defer wal.Close()

	chunkStore, err := datanode.NewChunkStore(cfg.DataDir, cfg.MaxConcurrentWrites, cfg.MaxConcurrentReads, wal)
	if err != nil {
		log.Fatalf("datanode: failed to init chunk store: %v", err)
	}
	totalBytes, chunkCount := chunkStore.Stats()
	log.Printf("datanode: chunk store ready (chunks=%d, bytes=%d)", chunkCount, totalBytes)

	diskManager, err := datanode.NewDiskManager(cfg.DataDir, chunkStore, cfg.CapacityGB, wal)
	if err != nil {
		log.Fatalf("datanode: failed to init disk manager: %v", err)
	}
	diskManager.Start()
	defer diskManager.Stop()

	metaSvcAddr := fmt.Sprintf("http://%s", cfg.MetadataAddr)
	metaStore := metadata.NewHTTPClient(metaSvcAddr, 30*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	err = metaStore.RegisterNode(ctx, &metadata.NodeInfo{
		ID:         cfg.NodeID,
		Addr:       cfg.ListenAddr,
		DataDir:    cfg.DataDir,
		Rack:       cfg.Rack,
		Zone:       cfg.Zone,
		MachineID:  cfg.MachineID,
		Tier:       cfg.Tier,
		CapacityGB: cfg.CapacityGB,
	})
	cancel()
	if err != nil && err != metadata.ErrNodeAlreadyExists {
		log.Fatalf("datanode: failed to register node: %v", err)
	}
	log.Printf("datanode: registered with metadata service at %s", cfg.MetadataAddr)

	server := datanode.NewServer(cfg, chunkStore)
	if err := server.Start(); err != nil {
		log.Fatalf("datanode: failed to start server: %v", err)
	}
	defer server.Stop()

	replicator := datanode.NewReplicator(cfg.ListenAddr, 4)
	replicator.Start()
	defer replicator.Stop()

	chainRepl := datanode.NewChainReplicator(cfg.ListenAddr, cfg.NodeID, 5*time.Second)

	repairWorker := datanode.NewRepairWorker(datanode.RepairConfig{
		Meta:       metaStore,
		NodeID:     cfg.NodeID,
		Interval:   30 * time.Second,
		Replicator: replicator,
		LocalAddr:  cfg.ListenAddr,
	})
	repairWorker.Start(context.Background())
	defer repairWorker.Stop()

	antiEntropy := datanode.NewAntiEntropy(chunkStore, metaStore, cfg.NodeID)
	antiEntropy.Start(30 * time.Minute)
	defer antiEntropy.Stop()

	heartbeat := datanode.NewHeartbeatReporter(cfg, metaStore, chunkStore)
	heartbeat.Start()
	defer heartbeat.Stop()

	opsServer := datanode.NewOpsServerWithRepair(cfg, chunkStore, metaStore, diskManager, chainRepl, antiEntropy, repairWorker)
	opsServer.Start()
	defer opsServer.Stop()

	log.Printf("datanode: all components started successfully")

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	log.Printf("datanode: received signal %v, shutting down", sig)
	log.Printf("datanode: shutdown complete")
}
