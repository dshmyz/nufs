// nufs-cli is the unified DFS cluster administration CLI tool.
// It supports both local mode (direct Pebble access) and remote mode
// (metad HTTP API).
//
// Usage:
//
//	nufs-cli [flags] <command> [args]
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"text/tabwriter"
	"time"

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
  leader                   Show Raft leader info

Commands (remote only):
  cluster info             Show cluster status
  bucket create <name>     Create a new bucket
  gc scan                  Trigger orphan chunk scan
  metrics                  Show node metrics
  health                   Check node health
  rebalance                Show rebalance plan
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
		cmdLeader(store)
	case "rebalance":
		cmdRebalancePlan(ctx, store)
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

func cmdLeader(store *metadata.PebbleStore) {
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
		api.cmdClusterInfo()
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
