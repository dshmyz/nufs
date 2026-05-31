// ctl is the DFS cluster administration CLI tool.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"text/tabwriter"
	"time"

	"github.com/example/dfs/metadata"
)

func main() {
	var metaDir = flag.String("meta-dir", "/var/lib/dfs/metadata", "Pebble metadata directory")
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		printUsage()
		os.Exit(1)
	}

	// Create metadata store (PebbleStore)
	store, err := metadata.NewPebbleStore(metadata.PebbleStoreConfig{
		Dir: *metaDir,
	})
	if err != nil {
		log.Fatalf("ctl: failed to open metadata store: %v", err)
	}
	defer store.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cmd := args[0]
	cmdArgs := args[1:]

	switch cmd {
	case "nodes":
		cmdNodes(ctx, store)
	case "buckets":
		cmdBuckets(ctx, store)
	case "rebalance":
		cmdRebalance(ctx, store)
	case "decommission":
		if len(cmdArgs) < 1 {
			fmt.Println("Usage: ctl decommission <node_id>")
			os.Exit(1)
		}
		cmdDecommission(ctx, store, cmdArgs)
	case "repair-queue":
		cmdRepairQueue(ctx, store)
	case "trigger-rebalance":
		cmdTriggerRebalance(ctx, store)
	case "leader":
		cmdLeader(store)
	default:
		fmt.Printf("Unknown command: %s\n", cmd)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println(`DFS Cluster Administration Tool

Usage: ctl [flags] <command> [args]

Commands:
  nodes              List all data nodes
  buckets            List all buckets
  rebalance          Show rebalance plan
  decommission <id>  Decommission a data node
  repair-queue       Show pending repair tasks
  trigger-rebalance  Trigger cluster-wide rebalance
  leader             Show Raft leader info

Flags:`)
	flag.PrintDefaults()
}

func cmdNodes(ctx context.Context, store *metadata.PebbleStore) {
	nodes, err := store.ListNodes(ctx)
	if err != nil {
		log.Fatalf("Failed to list nodes: %v", err)
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
		log.Fatalf("Failed to list buckets: %v", err)
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

func cmdRebalance(ctx context.Context, store *metadata.PebbleStore) {
	nodes, err := store.ListNodes(ctx)
	if err != nil {
		log.Fatalf("Failed to list nodes: %v", err)
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

func cmdDecommission(ctx context.Context, store *metadata.PebbleStore, args []string) {
	var nodeID metadata.NodeID
	if _, err := fmt.Sscanf(args[0], "%d", &nodeID); err != nil {
		log.Fatalf("Invalid node ID: %s", args[0])
	}

	fmt.Printf("Decommissioning node %d...\n", nodeID)
	if err := store.DecommissionNode(ctx, nodeID); err != nil {
		log.Fatalf("Failed to decommission: %v", err)
	}
	fmt.Println("Node marked for decommission. Chunks will be migrated.")
}

func cmdRepairQueue(ctx context.Context, store *metadata.PebbleStore) {
	tasks, err := store.GetRepairQueue(ctx)
	if err != nil {
		log.Fatalf("Failed to get repair queue: %v", err)
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
		log.Fatalf("Failed to trigger rebalance: %v", err)
	}
	fmt.Println("Rebalance triggered. Monitor progress with 'ctl rebalance'.")
}

func cmdLeader(store *metadata.PebbleStore) {
	if store.IsLeader() {
		fmt.Println("This node IS the Raft leader")
	} else {
		addr := store.LeaderAddr()
		fmt.Printf("Raft leader is at: %s\n", addr)
	}
}

// PrettyPrintJSON outputs a value as formatted JSON.
func PrettyPrintJSON(v interface{}) {
	data, _ := json.MarshalIndent(v, "", "  ")
	fmt.Println(string(data))
}
