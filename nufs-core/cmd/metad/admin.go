package main

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"time"

	"github.com/example/dfs/metadata"
)

//go:embed static/*
var adminStaticRaw embed.FS

var adminStaticFS fs.FS

func init() {
	var err error
	adminStaticFS, err = fs.Sub(adminStaticRaw, "static")
	if err != nil {
		panic(err)
	}
}

// adminServer serves the NUFS admin console as a Vue single-page app.
//
// The page is entirely static (a shell that mounts a Vue app via a hash
// router); all data comes from the /api/v1/* JSON endpoints that
// registerOpsHandlers mounts on the same mux/port. This type only owns the
// static shell, the demo-seed action, and the SSE metrics stream that feeds
// the Overview's live chart.
type adminServer struct {
	store  *metadata.PebbleStore
	bundle *metadata.ServiceBundle
	shell  []byte // static/index.html bytes, read once at startup
}

func newAdminServer(store *metadata.PebbleStore, bundle *metadata.ServiceBundle) *adminServer {
	shell, err := fs.ReadFile(adminStaticFS, "index.html")
	if err != nil {
		log.Fatalf("admin: cannot read embedded shell index.html: %v", err)
	}
	return &adminServer{store: store, bundle: bundle, shell: shell}
}

func (a *adminServer) RegisterRoutes(mux *http.ServeMux) {
	mux.Handle("/admin/static/", http.StripPrefix("/admin/static/", http.FileServer(http.FS(adminStaticFS))))
	// The SPA uses a hash router, so every path under /admin/ serves the same
	// shell; the JavaScript reads location.hash to pick the page component.
	// /admin/seed and /admin/metrics/stream are exact patterns and therefore
	// win over this subtree in the new ServeMux.
	mux.HandleFunc("/admin/", a.serveShell)
	mux.HandleFunc("/admin/seed", a.handleSeedDemo)
	mux.HandleFunc("/admin/metrics/stream", a.handleMetricsStream)
}

func (a *adminServer) serveShell(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if _, err := w.Write(a.shell); err != nil {
		log.Printf("admin: write shell: %v", err)
	}
}

// --- Pure data helpers (also exercised by admin_topology_test.go) ---

// stateClass maps a node state to a CSS badge class (success/warning/danger/
// secondary).
func stateClass(s metadata.NodeState) string {
	switch s {
	case metadata.NodeOnline:
		return "success"
	case metadata.NodeDraining:
		return "warning"
	case metadata.NodeOffline:
		return "secondary"
	case metadata.NodeFailed:
		return "danger"
	case metadata.NodeDecommissioned:
		return "secondary"
	default:
		return "secondary"
	}
}

// stateLabel maps a node state to a human-readable name.
func stateLabel(s metadata.NodeState) string {
	switch s {
	case metadata.NodeOnline:
		return "Online"
	case metadata.NodeDraining:
		return "Draining"
	case metadata.NodeOffline:
		return "Offline"
	case metadata.NodeFailed:
		return "Failed"
	case metadata.NodeDecommissioned:
		return "Decommissioned"
	default:
		return "Unknown"
	}
}

// humanBytes renders a byte count as a compact human-readable string
// ("211 KB", "1.4 GB").
func humanBytes(b int64) string {
	switch {
	case b < 0:
		return "-"
	case b < 1024:
		return fmt.Sprintf("%d B", b)
	case b < 1024*1024:
		return fmt.Sprintf("%.1f KB", float64(b)/1024)
	case b < 1024*1024*1024:
		return fmt.Sprintf("%.1f MB", float64(b)/(1024*1024))
	case b < 1024*1024*1024*1024:
		return fmt.Sprintf("%.1f GB", float64(b)/(1024*1024*1024))
	default:
		return fmt.Sprintf("%.2f TB", float64(b)/(1024*1024*1024*1024))
	}
}

// topoNode is a rendered node card in the Overview topology. CapacityGB is
// the physical capacity reported via heartbeat; UsagePct is used/Capacity
// (0 when capacity is unknown), so the console never draws a misleading ring
// for a node we have no capacity for.
type topoNode struct {
	ID        metadata.NodeID
	Addr      string
	StateName string // Online / Draining / Offline / Failed / Unknown
	StateCls  string // success / warning / danger / secondary (CSS)
	Capacity  int64  // GB
	Used      int64  // GB (logical live bytes)
	OnDisk    int64  // GB (physical on-disk footprint)
	// Byte-accurate footprints for rendering logical-vs-physical precisely.
	UsedBytes     int64
	OnDiskBytes   int64
	CapacityBytes int64
	UsagePct      float64
	DashOffset    float64 // capacity-ring stroke offset (precomputed)
	Chunks        int64
	Machine       string
	Zone          string
	Rack          string
}

// topoGroup groups rendered node cards by fault domain (rack/zone).
type topoGroup struct {
	Name  string
	Nodes []topoNode
}

func buildTopology(nodes []metadata.NodeInfo) []topoGroup {
	groups := make([]topoGroup, 0, 4)
	byName := make(map[string]int)
	for _, n := range nodes {
		domain := n.Rack
		if domain == "" {
			domain = "unassigned"
			if n.Zone != "" {
				domain = "zone / " + n.Zone
			}
		} else if n.Zone != "" {
			domain = n.Rack + " / " + n.Zone
		}
		tn := topoNode{
			ID:            n.ID,
			Addr:          n.Addr,
			StateName:     stateLabel(n.State),
			StateCls:      stateClass(n.State),
			Capacity:      n.CapacityGB,
			Used:          n.UsedGB,
			OnDisk:        n.OnDiskGB,
			UsedBytes:     n.UsedBytes,
			OnDiskBytes:   n.OnDiskBytes,
			CapacityBytes: n.CapacityBytes,
			Chunks:        n.ChunkCount,
			Machine:       n.MachineID,
			Zone:          n.Zone,
			Rack:          n.Rack,
		}
		if n.CapacityGB > 0 {
			tn.UsagePct = float64(n.UsedGB) / float64(n.CapacityGB) * 100
			// Ring circumference is 2*pi*r for r=19 (119.4); dashoffset
			// reveals the used fraction.
			const ringC = 119.4
			used := tn.UsagePct / 100
			if used > 1 {
				used = 1
			}
			tn.DashOffset = ringC - ringC*used
		}
		idx, ok := byName[domain]
		if !ok {
			idx = len(groups)
			byName[domain] = idx
			groups = append(groups, topoGroup{Name: domain})
		}
		groups[idx].Nodes = append(groups[idx].Nodes, tn)
	}
	return groups
}

// --- Seed Demo Data ---

func (a *adminServer) handleSeedDemo(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "POST required")
		return
	}
	if err := a.seedDemoData(r.Context()); err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, map[string]string{"status": "ok"})
}

// --- SSE Metrics Stream ---

type metricsStreamEvent struct {
	Timestamp     int64   `json:"ts"`
	NodesTotal    int     `json:"nodes_total"`
	NodesOnline   int     `json:"nodes_online"`
	NodesOffline  int     `json:"nodes_offline"`
	NodesDraining int     `json:"nodes_draining"`
	BucketsTotal  int     `json:"buckets_total"`
	ChunksTotal   int64   `json:"chunks_total"`
	RepairCount   int     `json:"repair_count"`
	UsedGB        int64   `json:"used_gb"`
	CapacityGB    int64   `json:"capacity_gb"`
	CapacityPct   float64 `json:"capacity_pct"`
	OpsTotal      int64   `json:"ops_total"`
	OpsRate       float64 `json:"ops_rate"`
	ReadRate      float64 `json:"read_rate"`
	WriteRate     float64 `json:"write_rate"`
	ErrorRate     float64 `json:"error_rate"`
	ReadOps       int64   `json:"read_ops"`
	WriteOps      int64   `json:"write_ops"`
	ErrorsTotal   int64   `json:"errors_total"`
	ReadP50us     int64   `json:"read_p50_us"`
	ReadP99us     int64   `json:"read_p99_us"`
	WriteP50us    int64   `json:"write_p50_us"`
	WriteP99us    int64   `json:"write_p99_us"`
	IsLeader      bool    `json:"is_leader"`
	UptimeSeconds int64   `json:"uptime_seconds"`
}

func (a *adminServer) handleMetricsStream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	ctx := r.Context()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	var prevSnap metadata.MetricsSnapshot
	var prevTime time.Time
	var havePrev bool

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := time.Now()

			nodes, _ := a.store.ListNodes(ctx)
			buckets, _ := a.store.ListBuckets(ctx)
			tasks, _ := a.store.GetRepairQueue(ctx)

			var chunkCount, totalCap, usedCap int64
			online, offline, draining := 0, 0, 0
			for _, n := range nodes {
				chunkCount += n.ChunkCount
				totalCap += n.CapacityGB
				usedCap += n.UsedGB
				switch n.State {
				case metadata.NodeOnline:
					online++
				case metadata.NodeOffline:
					offline++
				case metadata.NodeDraining:
					draining++
				}
			}
			capPct := 0.0
			if totalCap > 0 {
				capPct = float64(usedCap) / float64(totalCap) * 100
			}

			evt := metricsStreamEvent{
				Timestamp:     now.UnixMilli(),
				NodesTotal:    len(nodes),
				NodesOnline:   online,
				NodesOffline:  offline,
				NodesDraining: draining,
				BucketsTotal:  len(buckets),
				ChunksTotal:   chunkCount,
				RepairCount:   len(tasks),
				UsedGB:        usedCap,
				CapacityGB:    totalCap,
				CapacityPct:   capPct,
				IsLeader:      a.store.IsLeader(),
			}

			if a.bundle.Metrics != nil {
				snap := a.bundle.Metrics.Snapshot()
				evt.OpsTotal = snap.OpsTotal
				evt.ReadOps = snap.ReadOps
				evt.WriteOps = snap.WriteOps
				evt.ErrorsTotal = snap.ErrorsTotal
				evt.ReadP50us = snap.ReadP50us
				evt.ReadP99us = snap.ReadP99us
				evt.WriteP50us = snap.WriteP50us
				evt.WriteP99us = snap.WriteP99us
				evt.UptimeSeconds = snap.UptimeSeconds

				if havePrev {
					elapsed := now.Sub(prevTime).Seconds()
					if elapsed > 0 {
						evt.OpsRate = float64(snap.OpsTotal-prevSnap.OpsTotal) / elapsed
						evt.ReadRate = float64(snap.ReadOps-prevSnap.ReadOps) / elapsed
						evt.WriteRate = float64(snap.WriteOps-prevSnap.WriteOps) / elapsed
						evt.ErrorRate = float64(snap.ErrorsTotal-prevSnap.ErrorsTotal) / elapsed
					}
				}
				prevSnap = snap
				prevTime = now
				havePrev = true
			}

			var buf bytes.Buffer
			json.NewEncoder(&buf).Encode(evt)
			fmt.Fprintf(w, "data: %s\n\n", buf.String())
			flusher.Flush()
		}
	}
}

func (a *adminServer) seedDemoData(ctx context.Context) error {
	nodes := []metadata.NodeInfo{
		{ID: 1, Addr: "10.0.0.1:9100", DataDir: "/data/dfs/node1", Rack: "rack-a", Zone: "us-east-1a", CapacityGB: 2000, UsedGB: 720, ChunkCount: 48, State: metadata.NodeOnline, LastSeen: time.Now().UnixNano()},
		{ID: 2, Addr: "10.0.0.2:9100", DataDir: "/data/dfs/node2", Rack: "rack-b", Zone: "us-east-1b", CapacityGB: 2000, UsedGB: 580, ChunkCount: 35, State: metadata.NodeOnline, LastSeen: time.Now().UnixNano()},
		{ID: 3, Addr: "10.0.0.3:9100", DataDir: "/data/dfs/node3", Rack: "rack-a", Zone: "us-east-1a", CapacityGB: 4000, UsedGB: 1200, ChunkCount: 72, State: metadata.NodeOnline, LastSeen: time.Now().UnixNano()},
	}
	for _, n := range nodes {
		if err := a.store.RegisterNode(ctx, &n); err != nil {
			return fmt.Errorf("register node %d: %w", n.ID, err)
		}
	}

	buckets := []struct {
		name string
		pol  metadata.PlacementPolicy
	}{
		{"user-uploads", metadata.PlacementPolicy{ID: "default", ReplicationFactor: 3}},
		{"logs-archive", metadata.PlacementPolicy{ID: "cold", ReplicationFactor: 2}},
	}
	for _, b := range buckets {
		if err := a.store.CreateBucket(ctx, b.name, b.pol); err != nil {
			return fmt.Errorf("create bucket %s: %w", b.name, err)
		}
	}

	// Create root inodes for demo files
	rootID := metadata.InodeID(1)
	for _, name := range []string{"demo-file-1.txt", "demo-file-2.txt", "notes.txt", "data.bin", "readme.md", "config.json"} {
		if _, err := a.store.CreateFile(ctx, rootID, name, 0644); err != nil {
			return fmt.Errorf("create file %s: %w", name, err)
		}
	}

	// List the created inodes to get their IDs
	entries, err := a.store.ReadDir(ctx, rootID, 0, 100)
	if err != nil {
		return fmt.Errorf("readdir: %w", err)
	}

	// Create chunks via the store's chunk ID generator
	for i, entry := range entries {
		if i >= 6 {
			break
		}
		chunk, err := a.store.AllocateChunk(ctx, entry.InodeID, 0, metadata.PlacementPolicy{ID: "default", ReplicationFactor: 3})
		if err != nil {
			return fmt.Errorf("allocate chunk %d: %w", i, err)
		}
		// Assign replicas to nodes
		nodeIDs := []metadata.NodeID{1, 2, 3}
		for j, nid := range nodeIDs {
			chunk.Replicas = append(chunk.Replicas, metadata.ReplicaInfo{
				NodeID: nid,
				Addr:   nodes[nid-1].Addr,
				State:  metadata.ReplicaReady,
			})
			if j >= 2 {
				break
			}
		}
		chunk.Size = 33554432
		if err := a.store.UpdateChunk(ctx, chunk); err != nil {
			return fmt.Errorf("update chunk %d: %w", chunk.ID, err)
		}
	}
	return nil
}
