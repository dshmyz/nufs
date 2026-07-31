package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/example/dfs/metadata"
)

// nodeIDFile is the filename within a data dir that persists the node ID.
const nodeIDFile = "node_id"

// loadOrAllocateNodeID returns the node ID persisted in dir, allocating and
// persisting fallback there if none exists yet.
func loadOrAllocateNodeID(dir string, fallback metadata.NodeID) metadata.NodeID {
	path := filepath.Join(dir, nodeIDFile)
	b, err := os.ReadFile(path)
	if err == nil {
		id, err := strconv.ParseUint(strings.TrimSpace(string(b)), 10, 64)
		if err == nil && id > 0 {
			return metadata.NodeID(id)
		}
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		log.Printf("datanode: warning: failed to create data dir for node ID %s: %v", dir, err)
		return fallback
	}
	if err := os.WriteFile(path, []byte(fmt.Sprintf("%d\n", fallback)), 0644); err != nil {
		log.Printf("datanode: warning: failed to persist node ID for %s: %v", dir, err)
	}
	return fallback
}
