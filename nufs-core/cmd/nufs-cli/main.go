// nufs-cli is the unified DFS cluster administration CLI tool.
// It supports both local mode (direct Pebble access) and remote mode
// (metad HTTP API).
//
// Usage:
//
//	nufs-cli [flags] <command> [args]
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"os"
	"text/tabwriter"
	"time"

	"github.com/example/dfs/internal/tools/backup"
	"github.com/example/dfs/internal/tools/doctor"
	"github.com/example/dfs/internal/tools/restore"
	"github.com/example/dfs/metadata"
)

func main() {
	var (
		metaDir  = flag.String("meta-dir", "/var/lib/dfs/metadata", "Pebble metadata directory (local mode)")
		metaAddr = flag.String("meta-addr", "localhost:8091", "Metadata HTTP address (remote mode)")
		mode     = flag.String("mode", "auto", "Connection mode: auto, local, remote")
	)
	flag.Usage = func() {
		fmt.Fprint(os.Stderr, `NUFS Cluster Administration Tool

Usage:
  nufs-cli [flags] <command> [args]

Flags:
`)
		flag.PrintDefaults()
		fmt.Fprint(os.Stderr, `
Commands (local/remote):
  nodes                    List all data nodes
  buckets                  List all buckets
  decommission <id>        Decommission a data node
  repair-queue             Show pending repair tasks
  trigger-rebalance        Trigger cluster-wide rebalance
  leader [id]            Show/transfer Raft leadership

Commands (remote only):
  cluster info             Show cluster status
  bucket create <name>     Create a new bucket
  gc scan                  Trigger orphan chunk scan
  metrics                  Show node metrics
  health                   Check node health
  rebalance                Show rebalance plan
  scrub                    Check chunk replica consistency

Tools (dispatched to subcommands):
  backup [flags]           Metadata backup (was nufs-backup)
  restore [flags]          Metadata restore (was nufs-restore)
  doctor [flags]           Cluster diagnostics (was nufs-doctor)

Disk management (remote, --node=<addr>):
  disk status              Show disk status on a datanode
  disk adopt <dir>         Add a disk to a running datanode
  disk retire <dir>        Emergency remove (no migration)
  disk decommission <dir>  Planned remove (migrate first)
  disk drain               Stop accepting new writes
  disk verify <dir>        Verify checksums on a disk

Cluster:
  balance                  Show capacity balance across nodes
`)
	}
	flag.Parse()

	// Auto-detect mode: if meta-dir exists, use local; otherwise remote.
	useLocal := *mode == "local"
	if *mode == "auto" {
		if _, err := os.Stat(*metaDir); err == nil {
			useLocal = true
		}
	}

	args := flag.Args()
	if len(args) == 0 {
		flag.Usage()
		os.Exit(1)
	}

	// Check for tools that are dispatched to subcommands before
	// the normal local/remote mode handling.
	if len(args) > 0 {
		switch args[0] {
		case "backup":
			os.Exit(backup.RunBackupCommand(context.Background(), args[1:], os.Stdout, os.Stderr))
		case "restore":
			os.Exit(restore.RunRestoreCommand(context.Background(), args[1:], os.Stdout, os.Stderr))
		case "doctor":
			os.Exit(doctor.RunDoctor(context.Background(), args[1:], os.Stdout, os.Stderr))
		}
	}

	if useLocal {
		runLocal(args, *metaDir)
	} else {
		runRemote(args, *metaAddr)
	}
}

// ============================================================
// Local mode — direct Pebble access
// ============================================================

func runLocal(args []string, metaDir string) {
	store, err := metadata.NewPebbleStore(metadata.PebbleStoreConfig{Dir: metaDir})
	if err != nil {
		log.Fatalf("open metadata store: %v", err)
	}
	defer store.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cmd, cmdArgs := parseSubcommand(args)
	switch cmd {
	case "nodes":
		cmdNodes(ctx, store)
	case "buckets":
		cmdBuckets(ctx, store)
	case "decommission":
		if len(cmdArgs) < 1 {
			fmt.Println("Usage: nufs-cli decommission <node_id>")
			os.Exit(1)
		}
		cmdDecommission(ctx, store, cmdArgs)
	case "repair", "repair-queue":
		cmdRepairQueue(ctx, store)
	case "trigger-rebalance":
		cmdTriggerRebalance(ctx, store)
	case "leader":
		cmdLeader(store, cmdArgs)
	case "rebalance":
		cmdRebalancePlan(ctx, store)
	case "scrub":
		cmdScrub(ctx, store)
	default:
		fmt.Fprintf(os.Stderr, "unknown command (local mode): %s\n", cmd)
		os.Exit(1)
	}
}

func parseSubcommand(args []string) (string, []string) {
	if len(args) == 0 {
		return "", nil
	}
	// Handle subcommands like "node list", "bucket list"
	if len(args) >= 2 {
		switch args[0] {
		case "node", "nodes":
			return "nodes", args[1:]
		case "bucket", "buckets":
			return "buckets", args[1:]
		case "repair":
			return "repair", args[1:]
		case "cluster":
			return args[1], nil
		}
	}
	return args[0], args[1:]
}

func cmdNodes(ctx context.Context, store *metadata.PebbleStore) {
	nodes, err := store.ListNodes(ctx)
	if err != nil {
		log.Fatalf("list nodes: %v", err)
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tADDR\tRACK\tZONE\tTIER\tSTATE\tCHUNKS\tUSED/CAP(GB)")
	for _, n := range nodes {
		state := "unknown"
		switch n.State {
		case metadata.NodeOnline:
			state = "online"
		case metadata.NodeDraining:
			state = "draining"
		case metadata.NodeOffline:
			state = "offline"
		case metadata.NodeFailed:
			state = "failed"
		}
		tier := "any"
		switch n.Tier {
		case metadata.TierHot:
			tier = "hot"
		case metadata.TierWarm:
			tier = "warm"
		case metadata.TierCold:
			tier = "cold"
		case metadata.TierArchive:
			tier = "archive"
		}
		fmt.Fprintf(w, "%d\t%s\t%s\t%s\t%s\t%s\t%d\t%d/%d\n",
			n.ID, n.Addr, n.Rack, n.Zone, tier, state,
			n.ChunkCount, n.UsedGB, n.CapacityGB)
	}
	w.Flush()
	fmt.Printf("\nTotal: %d nodes\n", len(nodes))
}

func cmdBuckets(ctx context.Context, store *metadata.PebbleStore) {
	buckets, err := store.ListBuckets(ctx)
	if err != nil {
		log.Fatalf("list buckets: %v", err)
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "NAME\tROOT_INODE\tPOLICY\tREPLICATION\tCREATED")
	for _, b := range buckets {
		fmt.Fprintf(w, "%s\t%d\t%s\t%d\t%s\n",
			b.Name, b.RootInode, b.Policy.ID, b.Policy.ReplicationFactor,
			b.CreationDate.Format(time.RFC3339))
	}
	w.Flush()
	fmt.Printf("\nTotal: %d buckets\n", len(buckets))
}

func cmdDecommission(ctx context.Context, store *metadata.PebbleStore, args []string) {
	var nodeID metadata.NodeID
	if _, err := fmt.Sscanf(args[0], "%d", &nodeID); err != nil {
		log.Fatalf("invalid node ID: %s", args[0])
	}
	fmt.Printf("Decommissioning node %d...\n", nodeID)
	if err := store.DecommissionNode(ctx, nodeID); err != nil {
		log.Fatalf("decommission: %v", err)
	}
	fmt.Println("Node marked for decommission. Chunks will be migrated.")
}

func cmdRepairQueue(ctx context.Context, store *metadata.PebbleStore) {
	tasks, err := store.GetRepairQueue(ctx)
	if err != nil {
		log.Fatalf("get repair queue: %v", err)
	}
	if len(tasks) == 0 {
		fmt.Println("No pending repair tasks.")
		return
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "CHUNK_ID\tREASON\tPRIORITY\tCREATED")
	for _, task := range tasks {
		fmt.Fprintf(w, "%d\t%s\t%d\t%s\n",
			task.ChunkID, task.Reason, task.Priority,
			task.CreatedAt.Format(time.RFC3339))
	}
	w.Flush()
	fmt.Printf("\nTotal: %d pending repairs\n", len(tasks))
}

func cmdTriggerRebalance(ctx context.Context, store *metadata.PebbleStore) {
	fmt.Println("Triggering cluster rebalance...")
	if err := store.TriggerRebalance(ctx); err != nil {
		log.Fatalf("trigger rebalance: %v", err)
	}
	fmt.Println("Rebalance triggered.")
}

func cmdLeader(store *metadata.PebbleStore, args []string) {
	if len(args) > 0 {
		fmt.Println("Leader transfer not available in local mode (use --mode=remote)")
		return
	}
	if store.IsLeader() {
		fmt.Println("This node IS the Raft leader")
	} else {
		fmt.Printf("Raft leader is at: %s\n", store.LeaderAddr())
	}
}

func cmdRebalancePlan(ctx context.Context, store *metadata.PebbleStore) {
	nodes, err := store.ListNodes(ctx)
	if err != nil {
		log.Fatalf("list nodes: %v", err)
	}

	planner := &metadata.RebalancePlanner{}
	result := planner.PlanRebalance(nodes, 0.1)

	fmt.Printf("Cluster Status:\n")
	fmt.Printf("  Balanced:  %v\n", result.Balanced)
	fmt.Printf("  Imbalance: %.4f\n", result.Imbalance)
	fmt.Printf("  Plans:     %d\n\n", len(result.Plans))

	if len(result.Plans) > 0 {
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "CHUNK\tSOURCE\tTARGET\tREASON")
		for i, plan := range result.Plans {
			if i >= 20 {
				fmt.Fprintf(w, "... and %d more\n", len(result.Plans)-20)
				break
			}
			fmt.Fprintf(w, "%d\t%d\t%d\t%s\n",
				plan.ChunkID, plan.SourceNode, plan.TargetNode, plan.Reason)
		}
		w.Flush()
	}
}

func cmdScrub(ctx context.Context, store *metadata.PebbleStore) {
	fmt.Println("Scrubbing all chunks for consistency...")
	start := time.Now()

	// Scan all chunk keys and check replica health
	var scanned, healthy, unhealthy int
	err := store.ScrubAllChunks(func(chunkID metadata.ChunkID, replicaCount, healthyCount int) {
		scanned++
		if healthyCount == 0 {
			unhealthy++
			fmt.Printf("  UNHEALTHY: chunk %d has %d replicas, 0 healthy\n", chunkID, replicaCount)
		} else {
			healthy++
		}
	})
	if err != nil {
		log.Fatalf("scrub: %v", err)
	}

	fmt.Printf("\nScrub completed in %s\n", time.Since(start).Round(time.Millisecond))
	fmt.Printf("  Scanned:   %d\n", scanned)
	fmt.Printf("  Healthy:   %d\n", healthy)
	fmt.Printf("  Unhealthy: %d\n", unhealthy)
	if unhealthy > 0 {
		fmt.Println("\nRun 'nufs-cli repair-queue' to see pending repairs.")
		os.Exit(1)
	}
}

// ============================================================
// Remote mode — metad HTTP API
// ============================================================

func runRemote(args []string, metaAddr string) {
	baseURL := "http://" + metaAddr
	api := &remoteAPI{base: baseURL}

	cmd, cmdArgs := parseSubcommand(args)
	switch cmd {
	case "nodes":
		api.cmdNodes()
	case "buckets":
		api.cmdBuckets(cmdArgs)
	case "decommission":
		if len(cmdArgs) < 1 {
			fmt.Println("Usage: nufs-cli decommission <node_id>")
			os.Exit(1)
		}
		api.cmdDecommission(cmdArgs[0])
	case "repair", "repair-queue":
		api.cmdRepairQueue()
	case "trigger-rebalance":
		api.cmdTriggerRebalance()
	case "leader":
		if len(cmdArgs) > 0 {
			api.cmdTransferLeader(cmdArgs[0])
		} else {
			api.cmdClusterInfo()
		}
	case "rebalance":
		api.cmdClusterInfo()
	case "cluster":
		api.cmdClusterInfo()
	case "gc":
		api.cmdGCScan()
	case "metrics":
		api.cmdMetrics()
	case "health":
		api.cmdHealth()
	case "info":
		api.cmdClusterInfo()
	case "scrub":
		api.cmdScrub()
	case "balance":
		api.cmdBalance()
	case "disk":
		api.cmdDisk(cmdArgs)
	default:
		fmt.Fprintf(os.Stderr, "unknown command (remote mode): %s\n", cmd)
		os.Exit(1)
	}
}

type remoteAPI struct {
	base string
}

func (a *remoteAPI) get(path string) []byte {
	url := a.base + path
	resp, err := http.Get(url)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		fmt.Fprintf(os.Stderr, "Error: GET %s failed: %s: %s\n", path, resp.Status, string(body))
		os.Exit(1)
	}
	return body
}

func (a *remoteAPI) post(path string, body io.Reader) []byte {
	url := a.base + path
	resp, err := http.Post(url, "application/json", body)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		fmt.Fprintf(os.Stderr, "Error: POST %s failed: %s: %s\n", path, resp.Status, string(b))
		os.Exit(1)
	}
	return b
}

func (a *remoteAPI) prettyJSON(data []byte) {
	var v interface{}
	json.Unmarshal(data, &v)
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	enc.Encode(v)
}

func (a *remoteAPI) cmdNodes() {
	resp := a.get("/api/v1/nodes")
	var nodes []map[string]interface{}
	json.Unmarshal(resp, &nodes)

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "NODE ID\tSTATE\tADDRESS\tRACK\tZONE")
	for _, n := range nodes {
		fmt.Fprintf(w, "%v\t%v\t%v\t%v\t%v\n",
			n["id"], n["state"], n["addr"], n["rack"], n["zone"])
	}
	w.Flush()
}

func (a *remoteAPI) cmdBuckets(args []string) {
	if len(args) > 0 && args[0] == "create" {
		if len(args) < 2 {
			fmt.Println("Usage: nufs-cli bucket create <name>")
			os.Exit(1)
		}
		req := struct {
			Name   string                   `json:"name"`
			Policy metadata.PlacementPolicy `json:"policy"`
		}{
			Name: args[1],
			Policy: metadata.PlacementPolicy{
				ID:                "default",
				ReplicationFactor: 3,
				TopologySpread:    metadata.SpreadNode,
			},
		}
		body, err := json.Marshal(req)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: marshal bucket request: %v\n", err)
			os.Exit(1)
		}
		a.post("/api/v1/buckets", bytes.NewReader(body))
		fmt.Printf("Bucket '%s' created\n", args[1])
		return
	}
	resp := a.get("/api/v1/buckets")
	var buckets []string
	json.Unmarshal(resp, &buckets)

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "BUCKET")
	for _, b := range buckets {
		fmt.Fprintf(w, "%s\n", b)
	}
	w.Flush()
}

func (a *remoteAPI) cmdDecommission(nodeID string) {
	resp := a.post("/api/v1/nodes/"+nodeID+"/decommission", nil)
	a.prettyJSON(resp)
}

func (a *remoteAPI) cmdRepairQueue() {
	resp := a.get("/api/v1/repair/queue")
	var tasks []map[string]interface{}
	json.Unmarshal(resp, &tasks)

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "CHUNK ID\tSTATE\tRETRIES\tCREATED")
	for _, t := range tasks {
		fmt.Fprintf(w, "%v\t%v\t%v\t%v\n",
			t["chunk_id"], t["state"], t["retries"], t["created_at"])
	}
	w.Flush()
}

func (a *remoteAPI) cmdTriggerRebalance() {
	resp := a.post("/api/v1/rebalance/trigger", nil)
	a.prettyJSON(resp)
}

func (a *remoteAPI) cmdClusterInfo() {
	resp := a.get("/api/v1/cluster/status")
	a.prettyJSON(resp)
}

func (a *remoteAPI) cmdGCScan() {
	resp := a.post("/api/v1/gc/scan", nil)
	a.prettyJSON(resp)
}

func (a *remoteAPI) cmdMetrics() {
	resp := a.get("/api/v1/metrics")
	a.prettyJSON(resp)
}

func (a *remoteAPI) cmdHealth() {
	resp := a.get("/api/v1/health")
	a.prettyJSON(resp)
}

func (a *remoteAPI) cmdScrub() {
	fmt.Println("Scrubbing all chunks (remote)...")
	resp := a.get("/api/v1/scrub")
	a.prettyJSON(resp)
}

// ============================================================
// Cluster balance
// ============================================================

func (a *remoteAPI) cmdBalance() {
	resp := a.get("/api/v1/cluster/balance")
	var bal struct {
		Nodes []struct {
			ID      int     `json:"id"`
			Addr    string  `json:"addr"`
			CapGB   int64   `json:"capacity_gb"`
			UsedGB  int64   `json:"used_gb"`
			UsedPct float64 `json:"used_pct"`
			Tier    string  `json:"tier"`
			Online  bool    `json:"online"`
		} `json:"nodes"`
		TotalUsedGB   int64   `json:"total_used_gb"`
		TotalCapGB    int64   `json:"total_cap_gb"`
		TotalUsedPct  float64 `json:"total_used_pct"`
		Imbalance     float64 `json:"imbalance"`
		MinUsedPct    float64 `json:"min_used_pct"`
		MaxUsedPct    float64 `json:"max_used_pct"`
		Recommendation string `json:"recommendation"`
	}
	json.Unmarshal(resp, &bal)

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(w, "NODE\tADDR\tCAP(GB)\tUSED(GB)\tUSED%%\tTIER\tSTATUS\n")
	for _, n := range bal.Nodes {
		status := "online"
		if !n.Online {
			status = "offline"
		}
		fmt.Fprintf(w, "%d\t%s\t%d\t%d\t%.1f%%\t%s\t%s\n",
			n.ID, n.Addr, n.CapGB, n.UsedGB, n.UsedPct*100, n.Tier, status)
	}
	w.Flush()
	fmt.Printf("\nTotal: %d/%d GB (%.1f%%)  Imbalance: %.1f%%  %s\n",
		bal.TotalUsedGB, bal.TotalCapGB, bal.TotalUsedPct*100,
		bal.Imbalance*100, bal.Recommendation)
}

// ============================================================
// Transfer Raft leadership
// ============================================================

func (a *remoteAPI) cmdTransferLeader(targetID string) {
	leaderResp := a.get("/api/v1/cluster/status")
	var status struct {
		IsLeader  bool   `json:"is_leader"`
		LeaderURI string `json:"leader_uri"`
	}
	json.Unmarshal(leaderResp, &status)
	fmt.Printf("Current leader: %s\n", status.LeaderURI)

	path := "/api/v1/cluster/leader"
	if targetID != "" {
		path += "?node_id=" + url.QueryEscape(targetID)
		fmt.Printf("Transferring leadership to %s...\n", targetID)
	} else {
		fmt.Println("Transferring leadership (auto-select)...")
	}
	resp := a.post(path, nil)
	fmt.Printf("Result: %s\n", string(resp))

	time.Sleep(2 * time.Second)
	leaderResp2 := a.get("/api/v1/cluster/status")
	json.Unmarshal(leaderResp2, &status)
	fmt.Printf("New leader: %s\n", status.LeaderURI)
}

// ============================================================
// Disk management (remote datanode HTTP API)
// ============================================================

func (a *remoteAPI) cmdDisk(args []string) {
	if len(args) < 1 {
		fmt.Println(`Usage: nufs-cli disk <subcommand> --node=<addr>

Subcommands:
  status [--node=addr]              Show disk status on a datanode
  adopt <dir> --node=addr           Add a disk to a running datanode
  retire <dir> --node=addr          Emergency remove a disk (no migration)
  decommission <dir> --node=addr    Planned remove (migrate first)
  drain --node=addr                 Stop accepting new writes
  verify <dir> --node=addr          Verify checksums on a disk

Examples:
  nufs-cli disk status --node=10.0.0.1:8091
  nufs-cli disk adopt /new-disk --node=10.0.0.1:8091
  nufs-cli disk decommission /old-disk --node=10.0.0.1:8091`)
		os.Exit(1)
	}

	subcmd := args[0]
	rest := args[1:]

	// Parse --node flag
	var nodeAddr string
	var positional []string
	for _, a := range rest {
		if strings.HasPrefix(a, "--node=") {
			nodeAddr = strings.TrimPrefix(a, "--node=")
		} else if a == "--node" {
			// handled below
		} else {
			positional = append(positional, a)
		}
	}
	// Also handle --node addr (space separated)
	for i, a := range rest {
		if a == "--node" && i+1 < len(rest) {
			nodeAddr = rest[i+1]
		}
	}

	if nodeAddr == "" {
		// Try to find a datanode address from the cluster
		nodes := a.get("/api/v1/nodes")
		var nodeList []struct {
			ID   int    `json:"id"`
			Addr string `json:"addr"`
		}
		json.Unmarshal(nodes, &nodeList)
		if len(nodeList) == 0 {
			fmt.Fprintln(os.Stderr, "Error: --node not specified and no nodes found in cluster")
			os.Exit(1)
		}
		// Use first node's data addr, replace port with ops port (8091)
		// In production, datanode ops addr is typically :8091
		nodeAddr = strings.Split(nodeList[0].Addr, ":")[0] + ":8091"
		fmt.Fprintf(os.Stderr, "Using first node: %s (use --node to specify)\n", nodeAddr)
	}

	diskAPI := &diskRemoteAPI{base: "http://" + nodeAddr}

	switch subcmd {
	case "status":
		diskAPI.status()
	case "adopt":
		if len(positional) < 1 {
			fmt.Fprintln(os.Stderr, "Error: adopt requires a directory path")
			os.Exit(1)
		}
		diskAPI.adopt(positional[0])
	case "retire":
		if len(positional) < 1 {
			fmt.Fprintln(os.Stderr, "Error: retire requires a directory path")
			os.Exit(1)
		}
		diskAPI.retire(positional[0])
	case "decommission":
		if len(positional) < 1 {
			fmt.Fprintln(os.Stderr, "Error: decommission requires a directory path")
			os.Exit(1)
		}
		diskAPI.decommission(positional[0])
	case "drain":
		diskAPI.drain()
	case "verify":
		if len(positional) < 1 {
			fmt.Fprintln(os.Stderr, "Error: verify requires a directory path")
			os.Exit(1)
		}
		diskAPI.verify(positional[0])
	default:
		fmt.Fprintf(os.Stderr, "Unknown disk subcommand: %s\n", subcmd)
		os.Exit(1)
	}
}

type diskRemoteAPI struct {
	base string
}

func (d *diskRemoteAPI) get(path string) []byte {
	resp, err := http.Get(d.base + path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: cannot connect to datanode at %s: %v\n", d.base, err)
		fmt.Fprintf(os.Stderr, "Hint: use --node=<addr> to specify the datanode ops address (default port 8091)\n")
		os.Exit(1)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		fmt.Fprintf(os.Stderr, "Error (HTTP %d): %s\n", resp.StatusCode, string(data))
		os.Exit(1)
	}
	return data
}

func (d *diskRemoteAPI) post(path string) []byte {
	resp, err := http.Post(d.base+path, "application/json", nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: cannot connect to datanode at %s: %v\n", d.base, err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		fmt.Fprintf(os.Stderr, "Error (HTTP %d): %s\n", resp.StatusCode, string(data))
		os.Exit(1)
	}
	return data
}

func (d *diskRemoteAPI) status() {
	resp := d.get("/api/v1/disks")
	var disks struct {
		Disks []struct {
			Index  int    `json:"index"`
			Dir    string `json:"dir"`
			Failed bool   `json:"failed"`
			Chunks int64  `json:"chunks"`
			Bytes  int64  `json:"bytes"`
		} `json:"disks"`
		TotalChunks int64 `json:"total_chunks"`
		TotalBytes  int64 `json:"total_bytes"`
	}
	json.Unmarshal(resp, &disks)

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(w, "INDEX\tDIR\tSTATUS\tCHUNKS\tSIZE\n")
	for _, d := range disks.Disks {
		status := "online"
		if d.Failed {
			status = "FAILED"
		}
		fmt.Fprintf(w, "%d\t%s\t%s\t%d\t%s\n",
			d.Index, d.Dir, status, d.Chunks, humanBytes(d.Bytes))
	}
	w.Flush()
	fmt.Printf("\nTotal: %d chunks, %s\n", disks.TotalChunks, humanBytes(disks.TotalBytes))
}

func (d *diskRemoteAPI) adopt(dir string) {
	resp := d.post("/api/v1/disks/adopt?dir=" + url.QueryEscape(dir))
	fmt.Printf("Adopted: %s\n", string(resp))
}

func (d *diskRemoteAPI) retire(dir string) {
	resp := d.post("/api/v1/disks/retire?dir=" + url.QueryEscape(dir))
	fmt.Printf("Retired: %s\n", string(resp))
}

func (d *diskRemoteAPI) decommission(dir string) {
	resp := d.post("/api/v1/disks/decommission?dir=" + url.QueryEscape(dir))
	fmt.Printf("Decommissioned: %s\n", string(resp))
}

func (d *diskRemoteAPI) drain() {
	resp := d.post("/api/v1/disks/drain")
	fmt.Printf("Drain: %s\n", string(resp))
}

func (d *diskRemoteAPI) verify(dir string) {
	resp := d.post("/api/v1/disks/verify?dir=" + url.QueryEscape(dir))
	var result struct {
		Dir       string `json:"dir"`
		Total     int    `json:"total"`
		Verified  int    `json:"verified"`
		Corrupted int    `json:"corrupted"`
		Failed    int    `json:"failed"`
	}
	json.Unmarshal(resp, &result)
	fmt.Printf("Verify %s: %d total, %d verified, %d corrupted, %d failed\n",
		result.Dir, result.Total, result.Verified, result.Corrupted, result.Failed)
	if result.Corrupted > 0 || result.Failed > 0 {
		os.Exit(1)
	}
}

func humanBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(b)/float64(div), "KMGTPE"[exp])
}
