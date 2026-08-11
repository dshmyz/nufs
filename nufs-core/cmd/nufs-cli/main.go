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
	"os"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/dshmyz/nufs/nufs-core/internal/tools/backup"
	"github.com/dshmyz/nufs/nufs-core/internal/tools/doctor"
	"github.com/dshmyz/nufs/nufs-core/internal/tools/restore"
	"github.com/dshmyz/nufs/nufs-core/metadata"
)

func main() {
	var (
		metaDir   = flag.String("meta-dir", "/var/lib/dfs/metadata", "Pebble metadata directory (local mode)")
		metaAddr  = flag.String("meta-addr", "localhost:8091", "Metadata HTTP address (remote mode)")
		mode      = flag.String("mode", "auto", "Connection mode: auto, local, remote")
		authToken = flag.String("auth-token", "", "Bearer token for metad ops API (remote mode)")
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
  stat <bucket>/<path>     Show metadata (inode/xattrs/chunks) for a file or directory
  ns <bucket>/<dir>        List directory entries under a bucket path
  kv get <key>             Read a raw metadata KV value
  kv scan <prefix>         Scan raw metadata KV entries under a catalog prefix
  inode <id>               Show inode metadata + xattrs by id
  chunks --inode <id>      List chunk references for an inode
  chunk <id>               Show full chunk/replica/EC detail
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
  audit [--limit N]        Show audit trail tail (remote only)
  locks --inode <id>       List advisory locks for an inode (remote only)
  backups [status]         Show metadata backup status/list (remote only)
  write-attempts [--state S]  Show object write recovery state (remote only)

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
		runRemote(args, *metaAddr, *authToken)
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
		cmdBuckets(ctx, store, cmdArgs)
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
	case "stat":
		cmdStat(&localNS{ctx: ctx, store: store}, cmdArgs)
	case "ns":
		cmdNS(&localNS{ctx: ctx, store: store}, cmdArgs)
	case "kv":
		cmdKVLocal(ctx, store, cmdArgs)
	case "inode":
		if len(cmdArgs) < 1 {
			fmt.Fprintln(os.Stderr, "Usage: nufs-cli inode <id>")
			os.Exit(1)
		}
		cmdInodeLocal(ctx, store, cmdArgs[0])
	case "chunks":
		cmdChunksLocal(ctx, store, cmdArgs)
	case "chunk":
		if len(cmdArgs) < 1 {
			fmt.Fprintln(os.Stderr, "Usage: nufs-cli chunk <id>")
			os.Exit(1)
		}
		cmdChunkLocal(ctx, store, cmdArgs[0])
	case "audit", "locks", "backups", "write-attempts":
		fmt.Fprintf(os.Stderr, "%s is remote-only (metad ops API); run with --mode=remote\n", cmd)
		os.Exit(1)
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

func cmdBuckets(ctx context.Context, store *metadata.PebbleStore, args []string) {
	if len(args) == 0 {
		cmdBucketList(ctx, store)
		return
	}
	switch args[0] {
	case "info":
		cmdBucketInfoLocal(ctx, store, args[1:])
	case "delete":
		cmdBucketDeleteLocal(ctx, store, args[1:])
	default:
		cmdBucketList(ctx, store)
	}
}

func cmdBucketList(ctx context.Context, store *metadata.PebbleStore) {
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

func cmdBucketInfoLocal(ctx context.Context, store *metadata.PebbleStore, args []string) {
	if len(args) < 1 {
		fmt.Println("Usage: nufs-cli bucket info <name>")
		os.Exit(1)
	}
	bucket, err := store.GetBucket(ctx, args[0])
	if err != nil {
		log.Fatalf("get bucket: %v", err)
	}
	fmt.Printf("Name:         %s\n", bucket.Name)
	fmt.Printf("Root Inode:   %d\n", bucket.RootInode)
	fmt.Printf("Policy ID:    %s\n", bucket.Policy.ID)
	if bucket.Policy.ECConfig != nil && bucket.Policy.ECConfig.DataShards > 0 {
		fmt.Printf("EC:           %d+%d (tolerates %d failures)\n",
			bucket.Policy.ECConfig.DataShards, bucket.Policy.ECConfig.ParityShards,
			bucket.Policy.ECConfig.MaxFailures())
	} else {
		fmt.Printf("Replication:  %d\n", bucket.Policy.ReplicationFactor)
	}
	fmt.Printf("Storage Tier: %d\n", bucket.Policy.StorageTier)
	fmt.Printf("Created:      %s\n", bucket.CreationDate.Format(time.RFC3339))
}

func cmdBucketDeleteLocal(ctx context.Context, store *metadata.PebbleStore, args []string) {
	if len(args) < 1 {
		fmt.Println("Usage: nufs-cli bucket delete <name>")
		os.Exit(1)
	}
	name := args[0]
	fmt.Printf("Delete bucket '%s'? This only works if the bucket is empty. [y/N] ", name)
	var confirm string
	fmt.Scanln(&confirm)
	if confirm != "y" && confirm != "Y" {
		fmt.Println("Aborted.")
		return
	}
	if err := store.DeleteBucket(ctx, name); err != nil {
		log.Fatalf("delete bucket: %v", err)
	}
	fmt.Printf("Bucket '%s' deleted\n", name)
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

func runRemote(args []string, metaAddr string, authToken string) {
	baseURL := "http://" + metaAddr
	api := newRemoteAPI(baseURL, authToken)

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
	case "stat":
		cmdStat(&remoteNS{api: api}, cmdArgs)
	case "ns":
		cmdNS(&remoteNS{api: api}, cmdArgs)
	case "kv":
		kvRemote(api, cmdArgs)
	case "inode":
		if len(cmdArgs) < 1 {
			fmt.Fprintln(os.Stderr, "Usage: nufs-cli inode <id>")
			os.Exit(1)
		}
		cmdInodeRemote(api, cmdArgs[0])
	case "chunks":
		cmdChunksRemote(api, cmdArgs)
	case "chunk":
		if len(cmdArgs) < 1 {
			fmt.Fprintln(os.Stderr, "Usage: nufs-cli chunk <id>")
			os.Exit(1)
		}
		cmdChunkRemote(api, cmdArgs[0])
	case "audit":
		cmdAuditRemote(api, cmdArgs)
	case "locks":
		cmdLocksRemote(api, cmdArgs)
	case "backups":
		cmdBackupsRemote(api, cmdArgs)
	case "write-attempts":
		cmdWriteAttemptsRemote(api, cmdArgs)
	case "acl":
		api.cmdACL(cmdArgs)
	case "auth":
		api.cmdAuth(cmdArgs)
	case "disk":
		api.cmdDisk(cmdArgs)
	default:
		fmt.Fprintf(os.Stderr, "unknown command (remote mode): %s\n", cmd)
		os.Exit(1)
	}
}

type remoteAPI struct {
	base      string
	authToken string
	client    *http.Client
}

// newRemoteAPI builds a remoteAPI whose HTTP client preserves the bearer token
// across cross-host redirects. http.DefaultClient strips Authorization when a
// 307 leader redirect changes hosts, so ops/auth calls that land on a Raft
// follower (which 307s to the leader) would otherwise arrive without their
// token and 401.
func newRemoteAPI(base, authToken string) *remoteAPI {
	return &remoteAPI{
		base:      base,
		authToken: authToken,
		client: &http.Client{
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				if len(via) >= 10 {
					return fmt.Errorf("stopped after 10 redirects")
				}
				// Re-apply the bearer token on the redirected request, including
				// cross-host leader redirects Go's default would strip it from.
				if authToken != "" {
					req.Header.Set("Authorization", "Bearer "+authToken)
				}
				return nil
			},
		},
	}
}

// do performs an HTTP request with an optional bearer token. If the upstream
// returns an auth error and the caller did not supply a token, it surfaces a
// hint so operators know to pass --auth-token.
func (a *remoteAPI) do(method, path string, body io.Reader) (*http.Response, []byte) {
	req, err := http.NewRequest(method, a.base+path, body)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	if a.authToken != "" {
		req.Header.Set("Authorization", "Bearer "+a.authToken)
	}
	resp, err := a.client.Do(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	return resp, data
}

func (a *remoteAPI) get(path string) []byte {
	resp, body := a.do(http.MethodGet, path, nil)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		if resp.StatusCode == http.StatusUnauthorized && a.authToken == "" {
			fmt.Fprintf(os.Stderr, "Error: GET %s: %s (401). Pass --auth-token to authenticate.\n", path, resp.Status)
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "Error: GET %s failed: %s: %s\n", path, resp.Status, string(body))
		os.Exit(1)
	}
	return body
}

func (a *remoteAPI) post(path string, body io.Reader) []byte {
	resp, b := a.do(http.MethodPost, path, body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		if resp.StatusCode == http.StatusUnauthorized && a.authToken == "" {
			fmt.Fprintf(os.Stderr, "Error: POST %s: %s (401). Pass --auth-token to authenticate.\n", path, resp.Status)
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "Error: POST %s failed: %s: %s\n", path, resp.Status, string(b))
		os.Exit(1)
	}
	return b
}

func (a *remoteAPI) put(path string, body io.Reader) []byte {
	resp, b := a.do(http.MethodPut, path, body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		if resp.StatusCode == http.StatusUnauthorized && a.authToken == "" {
			fmt.Fprintf(os.Stderr, "Error: PUT %s: %s (401). Pass --auth-token to authenticate.\n", path, resp.Status)
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "Error: PUT %s failed: %s: %s\n", path, resp.Status, string(b))
		os.Exit(1)
	}
	return b
}

func (a *remoteAPI) delete(path string) []byte {
	resp, b := a.do(http.MethodDelete, path, nil)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		if resp.StatusCode == http.StatusUnauthorized && a.authToken == "" {
			fmt.Fprintf(os.Stderr, "Error: DELETE %s: %s (401). Pass --auth-token to authenticate.\n", path, resp.Status)
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "Error: DELETE %s failed: %s: %s\n", path, resp.Status, string(b))
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
	if len(args) == 0 {
		a.cmdBucketList()
		return
	}
	switch args[0] {
	case "create":
		a.cmdBucketCreate(args[1:])
	case "info":
		a.cmdBucketInfo(args[1:])
	case "delete":
		a.cmdBucketDelete(args[1:])
	case "quota":
		a.cmdBucketQuota(args[1:])
	case "usage":
		a.cmdBucketUsage()
	default:
		fmt.Fprintf(os.Stderr, "Usage: nufs-cli bucket [create|info|delete|quota|usage] ...\n")
		os.Exit(1)
	}
}

func (a *remoteAPI) cmdBucketList() {
	resp := a.get("/api/v1/buckets")
	var buckets []metadata.BucketInfo
	json.Unmarshal(resp, &buckets)

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "NAME\tROOT_INODE\tREPLICATION\tCREATED")
	for _, b := range buckets {
		fmt.Fprintf(w, "%s\t%d\t%d\t%s\n",
			b.Name, b.RootInode, b.Policy.ReplicationFactor,
			b.CreationDate.Format(time.RFC3339))
	}
	w.Flush()
	fmt.Printf("\nTotal: %d buckets\n", len(buckets))
}

func (a *remoteAPI) cmdBucketCreate(args []string) {
	if len(args) < 1 {
		fmt.Println("Usage: nufs-cli bucket create <name> [--ec-data N --ec-parity N]")
		fmt.Println("  Default: replication factor 3 (no erasure coding)")
		fmt.Println("  EC mode: --ec-data 4 --ec-parity 2 → 4 data + 2 parity shards")
		os.Exit(1)
	}
	name := args[0]
	rf := 3
	var ec *metadata.ECConfig
	haveData, haveParity := false, false

	for i := 1; i < len(args); i++ {
		parseShard := func() int {
			if i+1 >= len(args) {
				fmt.Fprintf(os.Stderr, "Error: %s requires an integer argument\n", args[i])
				os.Exit(1)
			}
			n, err := strconv.Atoi(args[i+1])
			if err != nil || n < 1 {
				fmt.Fprintf(os.Stderr, "Error: %s requires a positive integer, got %q\n", args[i], args[i+1])
				os.Exit(1)
			}
			return n
		}
		switch args[i] {
		case "--ec-data":
			n := parseShard()
			if ec == nil {
				ec = &metadata.ECConfig{}
			}
			ec.DataShards = n
			haveData = true
			i++
		case "--ec-parity":
			n := parseShard()
			if ec == nil {
				ec = &metadata.ECConfig{}
			}
			ec.ParityShards = n
			haveParity = true
			i++
		}
	}

	// EC shard counts are structural: data and parity must be supplied together
	// and both positive, or we would silently mint a replication factor with no
	// parity (e.g. --ec-data 4 alone -> rf=4) that offers no redundancy.
	if haveData != haveParity {
		fmt.Fprintln(os.Stderr, "Error: --ec-data and --ec-parity must be specified together")
		os.Exit(1)
	}
	if haveData {
		rf = ec.DataShards + ec.ParityShards
	}

	req := struct {
		Name   string                   `json:"name"`
		Policy metadata.PlacementPolicy `json:"policy"`
	}{
		Name: name,
		Policy: metadata.PlacementPolicy{
			ID:                "default",
			ReplicationFactor: rf,
			ECConfig:          ec,
			TopologySpread:    metadata.SpreadNode,
		},
	}
	body, err := json.Marshal(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: marshal bucket request: %v\n", err)
		os.Exit(1)
	}
	a.post("/api/v1/buckets", bytes.NewReader(body))
	if ec != nil && ec.DataShards > 0 {
		fmt.Printf("Bucket '%s' created (EC %d+%d)\n", name, ec.DataShards, ec.ParityShards)
	} else {
		fmt.Printf("Bucket '%s' created (RF=%d)\n", name, rf)
	}
}

func (a *remoteAPI) cmdBucketInfo(args []string) {
	if len(args) < 1 {
		fmt.Println("Usage: nufs-cli bucket info <name>")
		os.Exit(1)
	}
	name := args[0]
	resp := a.get("/api/v1/buckets/" + name)
	var bucket metadata.BucketInfo
	json.Unmarshal(resp, &bucket)

	fmt.Printf("Name:         %s\n", bucket.Name)
	fmt.Printf("Root Inode:   %d\n", bucket.RootInode)
	fmt.Printf("Policy ID:    %s\n", bucket.Policy.ID)
	if bucket.Policy.ECConfig != nil && bucket.Policy.ECConfig.DataShards > 0 {
		fmt.Printf("EC:           %d+%d (tolerates %d failures)\n",
			bucket.Policy.ECConfig.DataShards, bucket.Policy.ECConfig.ParityShards,
			bucket.Policy.ECConfig.MaxFailures())
	} else {
		fmt.Printf("Replication:  %d\n", bucket.Policy.ReplicationFactor)
	}
	fmt.Printf("Storage Tier: %d\n", bucket.Policy.StorageTier)
	fmt.Printf("Created:      %s\n", bucket.CreationDate.Format(time.RFC3339))
}

func (a *remoteAPI) cmdBucketDelete(args []string) {
	if len(args) < 1 {
		fmt.Println("Usage: nufs-cli bucket delete <name>")
		os.Exit(1)
	}
	name := args[0]
	fmt.Printf("Delete bucket '%s'? This only works if the bucket is empty. [y/N] ", name)
	var confirm string
	fmt.Scanln(&confirm)
	if confirm != "y" && confirm != "Y" {
		fmt.Println("Aborted.")
		return
	}
	a.delete("/api/v1/buckets/" + name)
	fmt.Printf("Bucket '%s' deleted\n", name)
}

func (a *remoteAPI) cmdBucketQuota(args []string) {
	if len(args) < 2 {
		fmt.Println("Usage: nufs-cli bucket quota <get|set|delete> <bucket> [--max-bytes N] [--max-objects N]")
		os.Exit(1)
	}
	action, name := args[0], args[1]
	switch action {
	case "get":
		resp := a.get("/api/v1/buckets/" + name + "/quota")
		var status struct {
			Bucket string `json:"bucket"`
			Quota  *struct {
				MaxSizeBytes int64 `json:"max_bytes"`
				MaxObjects   int64 `json:"max_objects"`
			} `json:"quota"`
			Usage *struct {
				UsedBytes int64 `json:"used_bytes"`
				Objects   int   `json:"objects"`
			} `json:"usage"`
		}
		json.Unmarshal(resp, &status)

		fmt.Printf("Bucket: %s\n", name)
		if status.Quota != nil {
			fmt.Printf("Max Bytes:   %s\n", humanBytes(status.Quota.MaxSizeBytes))
			fmt.Printf("Max Objects: %s\n", formatCount(status.Quota.MaxObjects))
		} else {
			fmt.Println("Quota:       (none)")
		}
		if status.Usage != nil {
			fmt.Printf("Used Bytes:  %s\n", humanBytes(status.Usage.UsedBytes))
			fmt.Printf("Objects:     %s\n", formatCount(int64(status.Usage.Objects)))
		}

	case "set":
		var maxBytes, maxObjects int64
		for i := 2; i < len(args); i++ {
			switch args[i] {
			case "--max-bytes":
				if i+1 < len(args) {
					fmt.Sscanf(args[i+1], "%d", &maxBytes)
					i++
				}
			case "--max-objects":
				if i+1 < len(args) {
					fmt.Sscanf(args[i+1], "%d", &maxObjects)
					i++
				}
			}
		}
		req := struct {
			MaxSizeBytes int64 `json:"max_bytes"`
			MaxObjects   int64 `json:"max_objects"`
		}{MaxSizeBytes: maxBytes, MaxObjects: maxObjects}
		body, _ := json.Marshal(req)
		a.put("/api/v1/buckets/"+name+"/quota", bytes.NewReader(body))
		fmt.Printf("Quota set for bucket '%s'\n", name)

	case "delete":
		a.delete("/api/v1/buckets/" + name + "/quota")
		fmt.Printf("Quota removed for bucket '%s'\n", name)

	default:
		fmt.Fprintf(os.Stderr, "Usage: nufs-cli bucket quota <get|set|delete> <bucket>\n")
		os.Exit(1)
	}
}

func (a *remoteAPI) cmdBucketUsage() {
	resp := a.get("/api/v1/admin/bucket-usage")
	var usage []struct {
		Name      string `json:"name"`
		UsedBytes int64  `json:"used_bytes"`
		Objects   int    `json:"objects"`
	}
	json.Unmarshal(resp, &usage)

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "BUCKET\tUSED\tOBJECTS")
	for _, u := range usage {
		fmt.Fprintf(w, "%s\t%s\t%s\n", u.Name, humanBytes(u.UsedBytes), formatCount(int64(u.Objects)))
	}
	w.Flush()
	fmt.Printf("\nTotal: %d buckets\n", len(usage))
}

func (a *remoteAPI) cmdACL(args []string) {
	if len(args) < 1 {
		fmt.Println("Usage: nufs-cli acl <get|set|delete> <bucket> [flags]")
		os.Exit(1)
	}
	switch args[0] {
	case "get":
		a.cmdACLGet(args[1:])
	case "set":
		a.cmdACLSet(args[1:])
	case "delete":
		a.cmdACLDelete(args[1:])
	default:
		fmt.Fprintf(os.Stderr, "Usage: nufs-cli acl <get|set|delete> <bucket>\n")
		os.Exit(1)
	}
}

func (a *remoteAPI) cmdACLGet(args []string) {
	if len(args) < 1 {
		fmt.Println("Usage: nufs-cli acl get <bucket>")
		os.Exit(1)
	}
	resp := a.get("/api/v1/acl/" + args[0])
	var policy metadata.BucketPolicy
	json.Unmarshal(resp, &policy)

	fmt.Printf("Bucket:        %s\n", policy.Bucket)
	fmt.Printf("Owner:         %s\n", policy.Owner)
	fmt.Printf("Default Access: %s\n", policy.DefaultAccess)
	if len(policy.Statements) > 0 {
		fmt.Println("Statements:")
		for _, s := range policy.Statements {
			perms := make([]string, len(s.Permissions))
			for i, p := range s.Permissions {
				perms[i] = string(p)
			}
			fmt.Printf("  %s  %s  [%s]  %s\n", s.Effect, s.Principal, strings.Join(perms, ","), s.Resource)
		}
	} else {
		fmt.Println("Statements:    (none)")
	}
}

func (a *remoteAPI) cmdACLSet(args []string) {
	if len(args) < 2 {
		fmt.Println("Usage: nufs-cli acl set <bucket> --default <allow|deny> [--owner <name>] [--allow <principal> <perms>] [--deny <principal> <perms>]")
		fmt.Println("  perms: comma-separated list of read,write,admin,list")
		os.Exit(1)
	}
	bucket := args[0]
	policy := metadata.BucketPolicy{
		Bucket:        bucket,
		DefaultAccess: "deny",
	}

	for i := 1; i < len(args); i++ {
		switch args[i] {
		case "--default":
			if i+1 < len(args) {
				policy.DefaultAccess = args[i+1]
				i++
			}
		case "--owner":
			if i+1 < len(args) {
				policy.Owner = args[i+1]
				i++
			}
		case "--allow":
			if i+2 < len(args) {
				stmt := metadata.Statement{
					Effect:    "allow",
					Principal: metadata.Principal(args[i+1]),
					Resource:  bucket,
				}
				for _, p := range strings.Split(args[i+2], ",") {
					stmt.Permissions = append(stmt.Permissions, metadata.Permission(strings.TrimSpace(p)))
				}
				policy.Statements = append(policy.Statements, stmt)
				i += 2
			}
		case "--deny":
			if i+2 < len(args) {
				stmt := metadata.Statement{
					Effect:    "deny",
					Principal: metadata.Principal(args[i+1]),
					Resource:  bucket,
				}
				for _, p := range strings.Split(args[i+2], ",") {
					stmt.Permissions = append(stmt.Permissions, metadata.Permission(strings.TrimSpace(p)))
				}
				policy.Statements = append(policy.Statements, stmt)
				i += 2
			}
		}
	}

	body, _ := json.Marshal(policy)
	a.put("/api/v1/acl/"+bucket, bytes.NewReader(body))
	fmt.Printf("Policy set for bucket '%s'\n", bucket)
}

func (a *remoteAPI) cmdACLDelete(args []string) {
	if len(args) < 1 {
		fmt.Println("Usage: nufs-cli acl delete <bucket>")
		os.Exit(1)
	}
	a.delete("/api/v1/acl/" + args[0])
	fmt.Printf("Policy deleted for bucket '%s'\n", args[0])
}

// cmdAuth manages the metad credential registry (the authentication
// authority for mounts). Credentials map an accessKey -> {secretHash,
// boundPrincipal}; a mount exchanges them for a signed token at startup.
func (a *remoteAPI) cmdAuth(args []string) {
	if len(args) < 1 {
		fmt.Println("Usage: nufs-cli auth <add|del|list> [access-key] [flags]")
		os.Exit(1)
	}
	switch args[0] {
	case "add":
		a.cmdCredAdd(args[1:])
	case "del", "delete":
		a.cmdCredDelete(args[1:])
	case "list", "ls":
		a.cmdCredList()
	default:
		fmt.Fprintf(os.Stderr, "Usage: nufs-cli auth <add|del|list> [access-key]\n")
		os.Exit(1)
	}
}

func (a *remoteAPI) cmdCredAdd(args []string) {
	if len(args) < 1 {
		fmt.Println("Usage: nufs-cli auth add <access-key> --secret <secret-key> [--principal <name>]")
		os.Exit(1)
	}
	ak := args[0]
	var secret, principal string
	for i := 1; i < len(args); i++ {
		switch args[i] {
		case "--secret":
			if i+1 < len(args) {
				secret = args[i+1]
				i++
			}
		case "--principal":
			if i+1 < len(args) {
				principal = args[i+1]
				i++
			}
		}
	}
	if secret == "" {
		fmt.Fprintln(os.Stderr, "Error: --secret <secret-key> is required")
		os.Exit(1)
	}
	body, _ := json.Marshal(map[string]string{
		"secret_key": secret,
		"principal":  principal,
	})
	a.put("/api/v1/auth/creds/"+ak, bytes.NewReader(body))
	fmt.Printf("Credential '%s' added\n", ak)
}

func (a *remoteAPI) cmdCredDelete(args []string) {
	if len(args) < 1 {
		fmt.Println("Usage: nufs-cli auth del <access-key>")
		os.Exit(1)
	}
	a.delete("/api/v1/auth/creds/" + args[0])
	fmt.Printf("Credential '%s' deleted\n", args[0])
}

func (a *remoteAPI) cmdCredList() {
	resp := a.get("/api/v1/auth/creds/")
	a.prettyJSON(resp)
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
		TotalUsedGB    int64   `json:"total_used_gb"`
		TotalCapGB     int64   `json:"total_cap_gb"`
		TotalUsedPct   float64 `json:"total_used_pct"`
		Imbalance      float64 `json:"imbalance"`
		MinUsedPct     float64 `json:"min_used_pct"`
		MaxUsedPct     float64 `json:"max_used_pct"`
		Recommendation string  `json:"recommendation"`
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

func formatCount(n int64) string {
	if n < 1000 {
		return fmt.Sprintf("%d", n)
	}
	return fmt.Sprintf("%d", n) // simple for now
}
