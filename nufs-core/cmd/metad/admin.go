package main

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"html/template"
	"io/fs"
	"log"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/example/dfs/metadata"
)

//go:embed templates/*.html
var adminTemplateFS embed.FS

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

type adminServer struct {
	store  *metadata.PebbleStore
	bundle *metadata.ServiceBundle
	tmpls  map[string]*template.Template
}

func newAdminServer(store *metadata.PebbleStore, bundle *metadata.ServiceBundle) *adminServer {
	funcMap := template.FuncMap{
		"hasPrefix": strings.HasPrefix,
		"stateClass": func(s metadata.NodeState) string {
			switch s {
			case metadata.NodeOnline:
				return "success"
			case metadata.NodeDraining:
				return "warning"
			case metadata.NodeOffline:
				return "secondary"
			case metadata.NodeFailed:
				return "danger"
			default:
				return "secondary"
			}
		},
		"stateLabel": func(s metadata.NodeState) string {
			switch s {
			case metadata.NodeOnline:
				return "Online"
			case metadata.NodeDraining:
				return "Draining"
			case metadata.NodeOffline:
				return "Offline"
			case metadata.NodeFailed:
				return "Failed"
			default:
				return "Unknown"
			}
		},
		"formatTime": func(t time.Time) string {
			if t.IsZero() {
				return "-"
			}
			return t.Format("2006-01-02 15:04:05")
		},
		"formatUnix": func(ts int64) string {
			if ts == 0 {
				return "-"
			}
			return time.Unix(0, ts).Format("2006-01-02 15:04:05")
		},
		"basename": func(path string) string {
			return filepath.Base(path)
		},
		"tierLabel": func(t metadata.StorageTier) string {
			switch t {
			case metadata.StorageTierAny:
				return "Any"
			case metadata.TierHot:
				return "Hot (NVMe)"
			case metadata.TierWarm:
				return "Warm (SSD)"
			case metadata.TierCold:
				return "Cold (HDD)"
			case metadata.TierArchive:
				return "Archive"
			default:
				return fmt.Sprintf("Tier %d", t)
			}
		},
		"chunkStateClass": func(s metadata.ChunkState) string {
			switch s {
			case metadata.ChunkCreated:
				return "info"
			case metadata.ChunkSealed:
				return "primary"
			case metadata.ChunkReady:
				return "success"
			case metadata.ChunkDegraded:
				return "warning"
			case metadata.ChunkOrphan:
				return "secondary"
			default:
				return "secondary"
			}
		},
		"chunkStateLabel": func(s metadata.ChunkState) string {
			switch s {
			case metadata.ChunkCreated:
				return "Created"
			case metadata.ChunkSealed:
				return "Sealed"
			case metadata.ChunkReady:
				return "Ready"
			case metadata.ChunkDegraded:
				return "Degraded"
			case metadata.ChunkOrphan:
				return "Orphan"
			default:
				return "Unknown"
			}
		},
	}

	pages := []string{"overview", "nodes", "node_detail", "buckets", "chunks", "repair", "rebalance"}
	tmpls := make(map[string]*template.Template, len(pages))
	for _, p := range pages {
		tmpls[p] = template.Must(
			template.New("").Funcs(funcMap).ParseFS(adminTemplateFS,
				"templates/layout.html",
				"templates/"+p+".html",
			),
		)
	}
	return &adminServer{store: store, bundle: bundle, tmpls: tmpls}
}

func (a *adminServer) RegisterRoutes(mux *http.ServeMux) {
	mux.Handle("/admin/static/", http.StripPrefix("/admin/static/", http.FileServer(http.FS(adminStaticFS))))
	mux.HandleFunc("/admin/", a.handleOverview)
	mux.HandleFunc("/admin/nodes", a.handleNodes)
	mux.HandleFunc("/admin/nodes/", a.handleNodeDetail)
	mux.HandleFunc("/admin/buckets", a.handleBuckets)
	mux.HandleFunc("/admin/chunks", a.handleChunks)
	mux.HandleFunc("/admin/repair", a.handleRepair)
	mux.HandleFunc("/admin/rebalance", a.handleRebalance)
	mux.HandleFunc("/admin/seed", a.handleSeedDemo)
	mux.HandleFunc("/admin/metrics/stream", a.handleMetricsStream)
}

type pageData struct {
	Title      string
	IsLeader   bool
	LeaderAddr string
	Version    string
}

func (a *adminServer) baseData(title string) pageData {
	return pageData{
		Title:      title,
		IsLeader:   a.store.IsLeader(),
		LeaderAddr: a.store.LeaderAddr(),
		Version:    "0.2.0",
	}
}

func (a *adminServer) render(w http.ResponseWriter, page string, data interface{}) {
	tmpl, ok := a.tmpls[page]
	if !ok {
		http.Error(w, "template not found", http.StatusInternalServerError)
		return
	}
	var buf bytes.Buffer
	if err := tmpl.ExecuteTemplate(&buf, page, data); err != nil {
		log.Printf("admin: template error (%s): %v", page, err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write(buf.Bytes())
}

// --- Overview ---

type overviewData struct {
	pageData
	Nodes          []metadata.NodeInfo
	Buckets        []metadata.BucketInfo
	RepairLen      int
	ChunkCount     int
	OnlineNodes    int
	OfflineNodes   int
	DrainingNodes  int
	TotalCapacity  int64
	UsedCapacity   int64
	CapacityPct    float64
	IsEmpty        bool
}

func (a *adminServer) handleOverview(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/admin/" {
		http.NotFound(w, r)
		return
	}
	ctx := r.Context()
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

	a.render(w, "overview", overviewData{
		pageData:       a.baseData("Cluster Overview"),
		Nodes:          nodes,
		Buckets:        buckets,
		RepairLen:      len(tasks),
		ChunkCount:     int(chunkCount),
		OnlineNodes:    online,
		OfflineNodes:   offline,
		DrainingNodes:  draining,
		TotalCapacity:  totalCap,
		UsedCapacity:   usedCap,
		CapacityPct:    capPct,
		IsEmpty:        len(nodes) == 0 && len(buckets) == 0,
	})
}

// --- Nodes ---

type nodesData struct {
	pageData
	Nodes []metadata.NodeInfo
}

func (a *adminServer) handleNodes(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/admin/nodes" {
		http.NotFound(w, r)
		return
	}
	nodes, err := a.store.ListNodes(r.Context())
	if err != nil {
		nodes = nil
	}
	a.render(w, "nodes", nodesData{
		pageData: a.baseData("Nodes"),
		Nodes:    nodes,
	})
}

// --- Node Detail ---

type nodeDetailData struct {
	pageData
	Node   *metadata.NodeInfo
	Chunks []metadata.ChunkMeta
	Found  bool
}

func (a *adminServer) handleNodeDetail(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/admin/nodes/")
	id, err := strconv.ParseUint(path, 10, 64)
	if err != nil {
		http.NotFound(w, r)
		return
	}

	ctx := r.Context()
	node, err := a.store.GetNode(ctx, metadata.NodeID(id))
	if err != nil {
		a.render(w, "node_detail", nodeDetailData{pageData: a.baseData("Node Detail"), Found: false})
		return
	}

	chunks, _ := a.store.ChunksByNode(ctx, metadata.NodeID(id))
	a.render(w, "node_detail", nodeDetailData{
		pageData: a.baseData(fmt.Sprintf("Node %d", id)),
		Node:     node,
		Chunks:   chunks,
		Found:    true,
	})
}

// --- Buckets ---

type bucketsData struct {
	pageData
	Buckets []metadata.BucketInfo
}

func (a *adminServer) handleBuckets(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/admin/buckets" {
		http.NotFound(w, r)
		return
	}
	buckets, err := a.store.ListBuckets(r.Context())
	if err != nil {
		buckets = nil
	}
	a.render(w, "buckets", bucketsData{
		pageData: a.baseData("Buckets"),
		Buckets:  buckets,
	})
}

// --- Chunks ---

func (a *adminServer) handleChunks(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/admin/chunks" {
		http.NotFound(w, r)
		return
	}
	a.render(w, "chunks", struct{ pageData }{pageData: a.baseData("Chunks")})
}

// --- Repair ---

type repairData struct {
	pageData
	Tasks []metadata.RepairTask
}

func (a *adminServer) handleRepair(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/admin/repair" {
		http.NotFound(w, r)
		return
	}
	tasks, err := a.store.GetRepairQueue(r.Context())
	if err != nil {
		tasks = nil
	}
	a.render(w, "repair", repairData{
		pageData: a.baseData("Repair Queue"),
		Tasks:    tasks,
	})
}

// --- Rebalance ---

func (a *adminServer) handleRebalance(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/admin/rebalance" {
		http.NotFound(w, r)
		return
	}
	a.render(w, "rebalance", struct{ pageData }{pageData: a.baseData("Rebalance")})
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
