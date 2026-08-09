package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// nodeIDFile is the filename within a data dir that persists the node ID.
const nodeIDFile = "node_id"

// nodeIDEnv is the environment variable for an explicit node ID path
// that is independent of data directory order. When set, the node ID
// is persisted to this path instead of dirs[0]/node_id.
const nodeIDEnv = "NUFS_NODE_ID_FILE"

// defaultNodeIDPath is the fallback path for persisting the node ID
// when no data dir is available and no env var is set.
const defaultNodeIDPath = "/var/lib/nufs/node_id"

// resolveNodeIDPath returns the path where the node ID should be persisted.
// Priority: NUFS_NODE_ID_FILE env > first data dir > /var/lib/nufs/node_id.
func resolveNodeIDPath(dataDir string) string {
	if p := os.Getenv(nodeIDEnv); p != "" {
		return p
	}
	if dataDir != "" {
		return filepath.Join(dataDir, nodeIDFile)
	}
	return defaultNodeIDPath
}

// loadOrAllocateNodeID returns the node ID persisted at the given path,
// allocating and persisting fallback there if none exists yet.
func loadOrAllocateNodeID(path string, fallback metadata.NodeID) metadata.NodeID {
	b, err := os.ReadFile(path)
	if err == nil {
		id, err := strconv.ParseUint(strings.TrimSpace(string(b)), 10, 64)
		if err == nil && id > 0 {
			return metadata.NodeID(id)
		}
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		log.Printf("datanode: warning: failed to create dir for node ID %s: %v", dir, err)
		return fallback
	}
	if err := os.WriteFile(path, []byte(fmt.Sprintf("%d\n", fallback)), 0644); err != nil {
		log.Printf("datanode: warning: failed to persist node ID at %s: %v", path, err)
	}
	return fallback
}
