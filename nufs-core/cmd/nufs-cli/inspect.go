package main

// ============================================================
// inspect — wrap the remaining read-only ops queries into the CLI.
//
//   nufs-cli inode <id>                 # inode metadata + xattrs by id
//   nufs-cli chunks [--inode <id>]      # chunk references for an inode
//   nufs-cli chunk <id>                 # full chunk/replica/EC detail
//   nufs-cli audit [--limit N]          # audit trail tail (remote)
//   nufs-cli locks [--inode <id>]       # advisory locks (remote)
//   nufs-cli backups [status]           # metadata backup status/list (remote)
//   nufs-cli write-attempts             # object write recovery attempts (remote)
//
// inode/chunks/chunk work in both local and remote mode; the rest are
// remote-only (they read metad-side operator state with no portable
// Pebble-only equivalent).
// ============================================================

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"text/tabwriter"

	"github.com/example/dfs/metadata"
)

// ---- inode (local + remote) ----

func cmdInodeLocal(ctx context.Context, store *metadata.PebbleStore, idStr string) {
	id, err := strconv.ParseUint(idStr, 10, 64)
	if err != nil || id == 0 {
		fmt.Fprintf(os.Stderr, "invalid inode ID: %s\n", idStr)
		os.Exit(1)
	}
	meta, err := store.GetInode(ctx, metadata.InodeID(id))
	if err != nil {
		fmt.Fprintf(os.Stderr, "inode: %v\n", err)
		os.Exit(1)
	}
	printInodeMeta(meta)
}

func cmdInodeRemote(api *remoteAPI, idStr string) {
	id, err := strconv.ParseUint(idStr, 10, 64)
	if err != nil || id == 0 {
		fmt.Fprintf(os.Stderr, "invalid inode ID: %s\n", idStr)
		os.Exit(1)
	}
	resp := api.get(fmt.Sprintf("/api/v1/inodes/%d", id))
	var meta metadata.InodeMeta
	if err := json.Unmarshal(resp, &meta); err != nil {
		fmt.Fprintf(os.Stderr, "inode: decode: %v\n", err)
		os.Exit(1)
	}
	printInodeMeta(&meta)
}

// ---- chunks / chunk (local + remote) ----

func cmdChunksLocal(ctx context.Context, store *metadata.PebbleStore, args []string) {
	idStr, err := inodeFlag(args)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Usage: nufs-cli chunks --inode <id>")
		os.Exit(1)
	}
	id, err := strconv.ParseUint(idStr, 10, 64)
	if err != nil || id == 0 {
		fmt.Fprintf(os.Stderr, "invalid inode ID: %s\n", idStr)
		os.Exit(1)
	}
	refs, err := store.ListChunks(ctx, metadata.InodeID(id))
	if err != nil {
		fmt.Fprintf(os.Stderr, "chunks: %v\n", err)
		os.Exit(1)
	}
	printChunkRefs(refs)
}

func cmdChunksRemote(api *remoteAPI, args []string) {
	idStr, err := inodeFlag(args)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Usage: nufs-cli chunks --inode <id>")
		os.Exit(1)
	}
	resp := api.get("/api/v1/chunks?inode_id=" + url.QueryEscape(idStr))
	var refs []metadata.ChunkRef
	if err := json.Unmarshal(resp, &refs); err != nil {
		fmt.Fprintf(os.Stderr, "chunks: decode: %v\n", err)
		os.Exit(1)
	}
	printChunkRefs(refs)
}

func printChunkRefs(refs []metadata.ChunkRef) {
	if len(refs) == 0 {
		fmt.Println("No chunks.")
		return
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "CHUNK_ID\tOFFSET\tLENGTH\tVERSION")
	for _, c := range refs {
		fmt.Fprintf(w, "%d\t%d\t%d\t%d\n", c.ID, c.Offset, c.Length, c.Version)
	}
	w.Flush()
	fmt.Printf("\nTotal: %d chunks\n", len(refs))
}

func cmdChunkLocal(ctx context.Context, store *metadata.PebbleStore, idStr string) {
	id, err := strconv.ParseUint(idStr, 10, 64)
	if err != nil || id == 0 {
		fmt.Fprintf(os.Stderr, "invalid chunk ID: %s\n", idStr)
		os.Exit(1)
	}
	chunk, err := store.GetChunk(ctx, metadata.ChunkID(id))
	if err != nil {
		fmt.Fprintf(os.Stderr, "chunk: %v\n", err)
		os.Exit(1)
	}
	prettyJSONEncode(chunk)
}

func cmdChunkRemote(api *remoteAPI, idStr string) {
	if _, err := strconv.ParseUint(idStr, 10, 64); err != nil {
		fmt.Fprintf(os.Stderr, "invalid chunk ID: %s\n", idStr)
		os.Exit(1)
	}
	api.prettyJSON(api.get("/api/v1/chunks/" + url.PathEscape(idStr)))
}

// ---- remote-only wrappers ----

func cmdAuditRemote(api *remoteAPI, args []string) {
	limit := 1000
	for i := 0; i < len(args); i++ {
		if args[i] == "--limit" && i+1 < len(args) {
			fmt.Sscanf(args[i+1], "%d", &limit)
			i++
		}
	}
	api.prettyJSON(api.get(fmt.Sprintf("/api/v1/audit?limit=%d", limit)))
}

// cmdLocksRemote lists advisory locks. The metad endpoint requires an inode
// id (it 400s without one), so --inode is mandatory here.
func cmdLocksRemote(api *remoteAPI, args []string) {
	inode, err := inodeFlag(args)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Usage: nufs-cli locks --inode <id>")
		os.Exit(1)
	}
	q := url.Values{}
	q.Set("inode", inode)
	api.prettyJSON(api.get("/api/v1/locks?" + q.Encode()))
}

func cmdBackupsRemote(api *remoteAPI, args []string) {
	if len(args) > 0 && args[0] == "status" {
		api.prettyJSON(api.get("/api/v1/backups/status"))
		return
	}
	api.prettyJSON(api.get("/api/v1/backups"))
}

// cmdWriteAttemptsRemote reports object-write recovery state. With no args it
// hits the unauthenticated-agnostic per-state rollup (write-ops/status); with
// --state <S> it lists the individual write-attempt records in that state.
// Valid states: pending | chunks_allocated | chunks_durable | committed |
// failed | recovery_needed.
func cmdWriteAttemptsRemote(api *remoteAPI, args []string) {
	state := ""
	for i := 0; i < len(args); i++ {
		if args[i] == "--state" && i+1 < len(args) {
			state = args[i+1]
			i++
		}
	}
	if state == "" {
		api.prettyJSON(api.get("/api/v1/write-ops/status"))
		return
	}
	q := url.Values{}
	q.Set("limit", "1000")
	api.prettyJSON(api.get("/api/v1/write-attempts?state=" + url.QueryEscape(state) + "&" + q.Encode()))
}

// ---- helpers ----

// inodeFlag extracts --inode <id> (or --inode=<id>) from args.
func inodeFlag(args []string) (string, error) {
	for i := 0; i < len(args); i++ {
		switch {
		case args[i] == "--inode" && i+1 < len(args):
			return args[i+1], nil
		case len(args[i]) > len("--inode=") && args[i][:len("--inode=")] == "--inode=":
			return args[i][len("--inode="):], nil
		}
	}
	return "", fmt.Errorf("no --inode flag")
}

func prettyJSONEncode(v interface{}) {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	enc.Encode(v)
}
