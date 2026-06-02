// metad is the metadata service daemon for the distributed storage system.
// It uses Pebble as the storage engine with optional Raft consensus.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/example/dfs/metadata"
)

func main() {
	var (
		dataDir       = flag.String("data-dir", "/var/lib/dfs/metadata", "Pebble data directory")
		cacheDir      = flag.String("cache-dir", "", "Pebble read cache directory (optional)")
		nodeID        = flag.Uint64("node-id", 1, "Metadata node ID (for chunk ID generation)")
		memTableSize  = flag.Uint64("memtable-size", 256<<20, "Pebble memtable size in bytes")
		enableRaft    = flag.Bool("raft", true, "Enable Raft consensus")
		raftAddr      = flag.String("raft-addr", "0.0.0.0:7000", "Raft bind address")
		raftDir       = flag.String("raft-dir", "/var/lib/dfs/raft", "Raft data directory")
		raftBootstrap = flag.Bool("raft-bootstrap", false, "Bootstrap a new Raft cluster")
		opsAddr       = flag.String("ops-addr", "0.0.0.0:8091", "Operations HTTP API address")
		leaseTTL      = flag.Duration("lease-ttl", 30*time.Second, "Node lease TTL")
		gcInterval    = flag.Duration("gc-interval", 10*time.Minute, "GC scan interval")
		gcDryRun      = flag.Bool("gc-dry-run", false, "GC dry-run mode (no deletes)")
		scrubInterval = flag.Duration("scrub-interval", 1*time.Hour, "Scrub interval")
	)
	flag.Parse()

	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Lshortfile)
	log.Printf("metad: starting metadata service (node_id=%d, data=%s)", *nodeID, *dataDir)
	log.Printf("metad: Go %s %s/%s", runtime.Version(), runtime.GOOS, runtime.GOARCH)

	// --- 1. Create PebbleStore (single instance) ---
	pebbleCfg := metadata.PebbleStoreConfig{
		Dir:          *dataDir,
		CacheDir:     *cacheDir,
		NodeID:       *nodeID,
		MemTableSize: *memTableSize,
	}

	store, err := metadata.NewPebbleStore(pebbleCfg)
	if err != nil {
		log.Fatalf("metad: failed to create PebbleStore: %v", err)
	}
	log.Printf("metad: PebbleStore initialized (dir=%s)", *dataDir)

	// --- 2. Configure Raft (uses the same PebbleStore) ---
	var raftNode *metadata.RaftNode

	if *enableRaft {
		raftCfg := metadata.RaftNodeConfig{
			NodeID:             fmt.Sprintf("meta-%d", *nodeID),
			BindAddr:           *raftAddr,
			RaftDir:            *raftDir,
			Bootstrap:          *raftBootstrap,
			SnapshotThreshold:  8192,
			SnapshotInterval:   2 * time.Minute,
			TrailingLogs:       10240,
		}

		raftNode, err = metadata.NewRaftNode(store, raftCfg)
		if err != nil {
			log.Fatalf("metad: failed to create Raft node: %v", err)
		}
		store.SetRaftNode(raftNode)
		log.Printf("metad: Raft node started (addr=%s, bootstrap=%v)", *raftAddr, *raftBootstrap)

		// Wait for leadership
		for i := 0; i < 30; i++ {
			if store.IsLeader() {
				log.Printf("metad: this node is the Raft leader")
				break
			}
			time.Sleep(time.Second)
		}
	} else {
		log.Printf("metad: running in single-node mode (Raft disabled)")
	}

	// --- 3. Create production service bundle (wraps the single PebbleStore) ---
	opts := []metadata.ServiceOption{
		metadata.WithLeaseTTL(*leaseTTL),
		metadata.WithGCInterval(*gcInterval),
		metadata.WithGCDryRun(*gcDryRun),
		metadata.WithScrubInterval(*scrubInterval),
	}

	bundle, err := metadata.NewPebbleServiceBundle(store, opts...)
	if err != nil {
		log.Fatalf("metad: failed to create service bundle: %v", err)
	}
	defer bundle.Close()

	// Set Raft reference on bundle (needed for health checks)
	bundle.Raft = raftNode

	log.Printf("metad: service bundle initialized")

	// --- 4. Start operations HTTP API + Admin dashboard ---
	mux := http.NewServeMux()
	registerOpsHandlers(mux, store, bundle)

	admin := newAdminServer(store, bundle)
	admin.RegisterRoutes(mux)
	log.Printf("metad: admin dashboard available at http://%s/admin/", *opsAddr)

	opsServer := &http.Server{
		Addr:         *opsAddr,
		Handler:      mux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	go func() {
		log.Printf("metad: ops API listening on %s", *opsAddr)
		if err := opsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("metad: ops server error: %v", err)
		}
	}()

	log.Printf("metad: metadata service ready")

	// --- 5. Wait for shutdown ---
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	log.Printf("metad: received signal %v, shutting down", sig)

	// Trigger Raft snapshot before shutdown
	if raftNode != nil {
		if err := raftNode.TriggerSnapshot(); err != nil {
			log.Printf("metad: snapshot warning: %v", err)
		}
	}

	// Graceful shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutdownCancel()
	if err := opsServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("metad: ops shutdown error: %v", err)
	}

	log.Printf("metad: shutdown complete")
}

// registerOpsHandlers sets up the operations API endpoints.
func registerOpsHandlers(mux *http.ServeMux, store *metadata.PebbleStore, bundle *metadata.ServiceBundle) {
	s := &opsHandlers{store: store, bundle: bundle}

	// Health endpoints
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/ready", s.handleReady)

	// Bucket operations
	mux.HandleFunc("/api/v1/buckets", s.handleBuckets)
	mux.HandleFunc("/api/v1/buckets/", s.handleBucketByID)

	// Chunk operations (migration)
	mux.HandleFunc("/api/v1/chunks/migrate-replica", s.handleMigrateReplica)

	// Node operations
	mux.HandleFunc("/api/v1/nodes", s.handleNodes)
	mux.HandleFunc("/api/v1/nodes/", s.handleNodesByID)

	// Chunk operations
	mux.HandleFunc("/api/v1/chunks", s.handleChunks)
	mux.HandleFunc("/api/v1/chunks/", s.handleChunksByID)

	// Namespace operations
	mux.HandleFunc("/api/v1/namespace/mkdir", s.handleMkDir)
	mux.HandleFunc("/api/v1/namespace/rmdir", s.handleRmDir)
	mux.HandleFunc("/api/v1/namespace/readdir", s.handleReadDir)
	mux.HandleFunc("/api/v1/namespace/createfile", s.handleCreateFile)
	mux.HandleFunc("/api/v1/namespace/unlink", s.handleUnlink)
	mux.HandleFunc("/api/v1/namespace/lookup", s.handleLookup)
	mux.HandleFunc("/api/v1/namespace/rename", s.handleRename)
	mux.HandleFunc("/api/v1/namespace/symlink", s.handleSymlink)
	mux.HandleFunc("/api/v1/namespace/readlink", s.handleReadlink)
	mux.HandleFunc("/api/v1/namespace/link", s.handleLink)

	// Inode operations
	mux.HandleFunc("/api/v1/inodes/", s.handleInodesByID)

	// Repair operations
	mux.HandleFunc("/api/v1/repair/queue", s.handleRepairQueue)
	mux.HandleFunc("/api/v1/repair/trigger", s.handleTriggerRepair)
	mux.HandleFunc("/api/v1/repair/", s.handleRepairByID)

	// Rebalance operations
	mux.HandleFunc("/api/v1/rebalance/trigger", s.handleTriggerRebalance)

	// Cluster operations
	mux.HandleFunc("/api/v1/cluster/status", s.handleClusterStatus)
	mux.HandleFunc("/api/v1/metrics", s.handleMetrics)
}

type opsHandlers struct {
	store  *metadata.PebbleStore
	bundle *metadata.ServiceBundle
}

func (h *opsHandlers) handleHealth(w http.ResponseWriter, r *http.Request) {
	if h.bundle.IsReady() {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"healthy"}`))
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte(`{"status":"initializing"}`))
	}
}

func (h *opsHandlers) handleReady(w http.ResponseWriter, r *http.Request) {
	if h.bundle.IsReady() {
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
}

func (h *opsHandlers) handleClusterStatus(w http.ResponseWriter, r *http.Request) {
	status := map[string]interface{}{
		"is_leader":  h.store.IsLeader(),
		"version":    "0.2.0",
		"leader_uri": h.store.LeaderAddr(),
	}
	writeJSON(w, status)
}

func (h *opsHandlers) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if h.bundle.Metrics != nil {
		writeJSON(w, h.bundle.Metrics.Snapshot())
	} else {
		writeJSON(w, map[string]string{"status": "no metrics"})
	}
}

// --- Bucket handlers ---

func (h *opsHandlers) handleBuckets(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		buckets, err := h.store.ListBuckets(r.Context())
		if err != nil {
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, buckets)
	case http.MethodPost:
		var req struct {
			Name   string              `json:"name"`
			Policy metadata.PlacementPolicy `json:"policy"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid request body")
			return
		}
		if err := h.store.CreateBucket(r.Context(), req.Name, req.Policy); err != nil {
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
		w.WriteHeader(http.StatusCreated)
		writeJSON(w, map[string]string{"status": "created"})
	default:
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (h *opsHandlers) handleBucketByID(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Path[len("/api/v1/buckets/"):]
	if name == "" {
		writeJSONError(w, http.StatusBadRequest, "bucket name required")
		return
	}
	switch r.Method {
	case http.MethodGet:
		bucket, err := h.store.GetBucket(r.Context(), name)
		if err != nil {
			writeJSONError(w, http.StatusNotFound, err.Error())
			return
		}
		writeJSON(w, bucket)
	case http.MethodDelete:
		if err := h.store.DeleteBucket(r.Context(), name); err != nil {
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, map[string]string{"status": "deleted"})
	default:
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

// --- Node handlers ---

func (h *opsHandlers) handleNodes(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		nodes, err := h.store.ListNodes(r.Context())
		if err != nil {
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, nodes)
	case http.MethodPost:
		var info metadata.NodeInfo
		if err := json.NewDecoder(r.Body).Decode(&info); err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid request body")
			return
		}
		if err := h.store.RegisterNode(r.Context(), &info); err != nil {
			if errors.Is(err, metadata.ErrNodeAlreadyExists) {
				writeJSONError(w, http.StatusConflict, err.Error())
			} else {
				writeJSONError(w, http.StatusInternalServerError, err.Error())
			}
			return
		}
		w.WriteHeader(http.StatusCreated)
		writeJSON(w, map[string]string{"status": "registered"})
	default:
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (h *opsHandlers) handleNodesByID(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path[len("/api/v1/nodes/"):]
	var nodeID metadata.NodeID
	if _, err := fmt.Sscanf(path, "%d", &nodeID); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid node ID")
		return
	}

	// Check for sub-paths
	rest := path
	var sub string
	if _, err := fmt.Sscanf(path, "%d/%s", &nodeID, &sub); err == nil {
		rest = sub
	} else {
		rest = ""
	}

	switch {
	case rest == "":
		switch r.Method {
		case http.MethodGet:
			node, err := h.store.GetNode(r.Context(), nodeID)
			if err != nil {
				writeJSONError(w, http.StatusNotFound, err.Error())
				return
			}
			writeJSON(w, node)
		default:
			writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	case rest == "heartbeat":
		if r.Method != http.MethodPost {
			writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		var report metadata.NodeReport
		if err := json.NewDecoder(r.Body).Decode(&report); err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid request body")
			return
		}
		if err := h.store.Heartbeat(r.Context(), nodeID, &report); err != nil {
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, map[string]string{"status": "ok"})
	case rest == "decommission":
		if r.Method != http.MethodPost {
			writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		if err := h.store.DecommissionNode(r.Context(), nodeID); err != nil {
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, map[string]string{"status": "decommissioned"})
	case rest == "chunks":
		if r.Method != http.MethodGet {
			writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		chunks, err := h.store.ChunksByNode(r.Context(), nodeID)
		if err != nil {
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, chunks)
	default:
		writeJSONError(w, http.StatusNotFound, "unknown node sub-path")
	}
}

// --- Chunk handlers ---

func (h *opsHandlers) handleChunks(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		inodeIDStr := r.URL.Query().Get("inode_id")
		if inodeIDStr != "" {
			var inodeID metadata.InodeID
			if _, err := fmt.Sscanf(inodeIDStr, "%d", &inodeID); err != nil {
				writeJSONError(w, http.StatusBadRequest, "invalid inode_id")
				return
			}
			refs, err := h.store.ListChunks(r.Context(), inodeID)
			if err != nil {
				writeJSONError(w, http.StatusInternalServerError, err.Error())
				return
			}
			writeJSON(w, refs)
			return
		}
		writeJSONError(w, http.StatusBadRequest, "inode_id required")
	case http.MethodPost:
		// Allocate chunk
		var req struct {
			InodeID metadata.InodeID       `json:"inode_id"`
			Offset  int64                  `json:"offset"`
			Policy  metadata.PlacementPolicy `json:"policy"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid request body")
			return
		}
		chunk, err := h.store.AllocateChunk(r.Context(), req.InodeID, req.Offset, req.Policy)
		if err != nil {
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
		w.WriteHeader(http.StatusCreated)
		writeJSON(w, chunk)
	default:
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (h *opsHandlers) handleChunksByID(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path[len("/api/v1/chunks/"):]

	// Special case: /api/v1/chunks/report-state
	if path == "report-state" {
		h.handleReportChunkState(w, r)
		return
	}

	var chunkID metadata.ChunkID
	if _, err := fmt.Sscanf(path, "%d", &chunkID); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid chunk ID")
		return
	}

	// Check for sub-paths (e.g., /api/v1/chunks/123/commit)
	var rest string
	if _, err := fmt.Sscanf(path, "%d/%s", &chunkID, &rest); err != nil {
		rest = ""
	}

	switch {
	case rest == "":
		switch r.Method {
		case http.MethodGet:
			chunk, err := h.store.GetChunk(r.Context(), chunkID)
			if err != nil {
				writeJSONError(w, http.StatusNotFound, err.Error())
				return
			}
			writeJSON(w, chunk)
		case http.MethodPut:
			var chunk metadata.ChunkMeta
			if err := json.NewDecoder(r.Body).Decode(&chunk); err != nil {
				writeJSONError(w, http.StatusBadRequest, "invalid request body")
				return
			}
			chunk.ID = chunkID
			if err := h.store.UpdateChunk(r.Context(), &chunk); err != nil {
				writeJSONError(w, http.StatusInternalServerError, err.Error())
				return
			}
			writeJSON(w, map[string]string{"status": "updated"})
		case http.MethodDelete:
			if err := h.store.DeleteChunk(r.Context(), chunkID); err != nil {
				writeJSONError(w, http.StatusInternalServerError, err.Error())
				return
			}
			writeJSON(w, map[string]string{"status": "deleted"})
		default:
			writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	case rest == "commit":
		if r.Method != http.MethodPost {
			writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		var req struct {
			Checksum uint32 `json:"checksum"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid request body")
			return
		}
		if err := h.store.CommitChunk(r.Context(), chunkID, req.Checksum); err != nil {
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, map[string]string{"status": "committed"})
	case rest == "seal":
		if r.Method != http.MethodPost {
			writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		if err := h.store.SealChunk(r.Context(), chunkID); err != nil {
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, map[string]string{"status": "sealed"})
	default:
		writeJSONError(w, http.StatusNotFound, "unknown chunk sub-path")
	}
}

func (h *opsHandlers) handleMigrateReplica(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req struct {
		ChunkID  metadata.ChunkID `json:"chunk_id"`
		FromNode metadata.NodeID  `json:"from_node"`
		ToNode   metadata.NodeID  `json:"to_node"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := h.store.MigrateChunkReplica(r.Context(), req.ChunkID, req.FromNode, req.ToNode); err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, map[string]string{"status": "migrated"})
}

func (h *opsHandlers) handleReportChunkState(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req struct {
		NodeID metadata.NodeID                          `json:"node_id"`
		States map[metadata.ChunkID]metadata.ReplicaState `json:"states"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := h.store.ReportChunkState(r.Context(), req.NodeID, req.States); err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, map[string]string{"status": "reported"})
}

// --- Namespace handlers ---

func (h *opsHandlers) handleMkDir(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req struct {
		Parent metadata.InodeID `json:"parent"`
		Name   string           `json:"name"`
		Mode   uint32           `json:"mode"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	meta, err := h.store.MkDir(r.Context(), req.Parent, req.Name, req.Mode)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusCreated)
	writeJSON(w, meta)
}

func (h *opsHandlers) handleRmDir(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req struct {
		Parent metadata.InodeID `json:"parent"`
		Name   string           `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := h.store.RmDir(r.Context(), req.Parent, req.Name); err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, map[string]string{"status": "removed"})
}

func (h *opsHandlers) handleReadDir(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var parent metadata.InodeID
	offset := 0
	limit := 1000
	fmt.Sscanf(r.URL.Query().Get("parent"), "%d", &parent)
	fmt.Sscanf(r.URL.Query().Get("offset"), "%d", &offset)
	fmt.Sscanf(r.URL.Query().Get("limit"), "%d", &limit)
	entries, err := h.store.ReadDir(r.Context(), parent, offset, limit)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, entries)
}

func (h *opsHandlers) handleCreateFile(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req struct {
		Parent metadata.InodeID `json:"parent"`
		Name   string           `json:"name"`
		Mode   uint32           `json:"mode"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	meta, err := h.store.CreateFile(r.Context(), req.Parent, req.Name, req.Mode)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusCreated)
	writeJSON(w, meta)
}

func (h *opsHandlers) handleUnlink(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req struct {
		Parent metadata.InodeID `json:"parent"`
		Name   string           `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := h.store.Unlink(r.Context(), req.Parent, req.Name); err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, map[string]string{"status": "unlinked"})
}

func (h *opsHandlers) handleLookup(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var parent metadata.InodeID
	name := r.URL.Query().Get("name")
	fmt.Sscanf(r.URL.Query().Get("parent"), "%d", &parent)
	meta, err := h.store.Lookup(r.Context(), parent, name)
	if err != nil {
		writeJSONError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, meta)
}

func (h *opsHandlers) handleRename(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req struct {
		OldParent metadata.InodeID `json:"old_parent"`
		OldName   string           `json:"old_name"`
		NewParent metadata.InodeID `json:"new_parent"`
		NewName   string           `json:"new_name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := h.store.Rename(r.Context(), req.OldParent, req.OldName, req.NewParent, req.NewName); err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, map[string]string{"status": "renamed"})
}

func (h *opsHandlers) handleSymlink(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req struct {
		Parent metadata.InodeID `json:"parent"`
		Name   string           `json:"name"`
		Target string           `json:"target"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	meta, err := h.store.Symlink(r.Context(), req.Parent, req.Name, req.Target)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusCreated)
	writeJSON(w, meta)
}

func (h *opsHandlers) handleReadlink(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var id metadata.InodeID
	fmt.Sscanf(r.URL.Query().Get("id"), "%d", &id)
	target, err := h.store.Readlink(r.Context(), id)
	if err != nil {
		writeJSONError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, map[string]string{"target": target})
}

func (h *opsHandlers) handleLink(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req struct {
		Parent metadata.InodeID `json:"parent"`
		Name   string           `json:"name"`
		Target metadata.InodeID `json:"target"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	meta, err := h.store.Link(r.Context(), req.Parent, req.Name, req.Target)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusCreated)
	writeJSON(w, meta)
}

// --- Inode handlers ---

func (h *opsHandlers) handleInodesByID(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path[len("/api/v1/inodes/"):]
	var inodeID metadata.InodeID
	if _, err := fmt.Sscanf(path, "%d", &inodeID); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid inode ID")
		return
	}
	switch r.Method {
	case http.MethodGet:
		meta, err := h.store.GetInode(r.Context(), inodeID)
		if err != nil {
			writeJSONError(w, http.StatusNotFound, err.Error())
			return
		}
		writeJSON(w, meta)
	case http.MethodPut:
		var meta metadata.InodeMeta
		if err := json.NewDecoder(r.Body).Decode(&meta); err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid request body")
			return
		}
		meta.ID = inodeID
		if err := h.store.UpdateInode(r.Context(), &meta); err != nil {
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, map[string]string{"status": "updated"})
	default:
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

// --- Repair handlers ---

func (h *opsHandlers) handleRepairQueue(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	tasks, err := h.store.GetRepairQueue(r.Context())
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, tasks)
}

func (h *opsHandlers) handleTriggerRepair(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req struct {
		ChunkID metadata.ChunkID `json:"chunk_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := h.store.TriggerRepair(r.Context(), req.ChunkID); err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, map[string]string{"status": "triggered"})
}

func (h *opsHandlers) handleRepairByID(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path[len("/api/v1/repair/"):]
	var chunkID metadata.ChunkID
	if _, err := fmt.Sscanf(path, "%d", &chunkID); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid chunk ID")
		return
	}
	switch r.Method {
	case http.MethodDelete:
		if err := h.store.RemoveRepairTask(r.Context(), chunkID); err != nil {
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, map[string]string{"status": "removed"})
	default:
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

// --- Rebalance handlers ---

func (h *opsHandlers) handleTriggerRebalance(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if err := h.store.TriggerRebalance(r.Context()); err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, map[string]string{"status": "triggered"})
}

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.Encode(v)
}

func writeJSONError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
