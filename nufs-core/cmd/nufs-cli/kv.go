package main

// ============================================================
// kv — raw metadata KV inspection for operators.
//
//   nufs-cli kv get <key>                 # single raw key/value
//   nufs-cli kv scan <prefix> [--limit N] [--cursor KEY]
//
// Local mode reads the metad Pebble directory directly (read-only).
// Remote mode calls the metad ops API GET /api/v1/kv, which is
// requireLeader-gated and bearer-protected (pass --auth-token).
// Scans are restricted to the documented catalog prefixes.
// ============================================================

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/dshmyz/nufs/nufs-core/metadata"
)

func cmdKVLocal(ctx context.Context, store *metadata.PebbleStore, args []string) {
	sub := "scan"
	if len(args) > 0 {
		sub = args[0]
		args = args[1:]
	}
	switch sub {
	case "get":
		if len(args) < 1 {
			fmt.Fprintln(os.Stderr, "Usage: nufs-cli kv get <key>")
			os.Exit(1)
		}
		kvLocalGet(store, args[0])
	case "scan":
		kvLocalScan(ctx, store, args)
	default:
		fmt.Fprintf(os.Stderr, "unknown kv subcommand (local): %s\n", sub)
		os.Exit(1)
	}
}

func kvLocalGet(store *metadata.PebbleStore, key string) {
	found, val, err := store.KVGet(key)
	if err != nil {
		fmt.Fprintf(os.Stderr, "kv get: %v\n", err)
		os.Exit(1)
	}
	if !found {
		fmt.Printf("key %q: not found\n", key)
		return
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(w, "Key:\t%s\n", key)
	fmt.Fprintf(w, "Size:\t%d\n", len(val))
	fmt.Fprintf(w, "Value:\t%s\n", printableBytes(val))
	fmt.Fprintf(w, "Value(b64):\t%s\n", base64.StdEncoding.EncodeToString(val))
	w.Flush()
}

func kvLocalScan(ctx context.Context, store *metadata.PebbleStore, args []string) {
	var prefix string
	limit := 100
	var cursor string
	for i := 0; i < len(args); i++ {
		switch {
		case args[i] == "--limit" && i+1 < len(args):
			fmt.Sscanf(args[i+1], "%d", &limit)
			i++
		case args[i] == "--cursor" && i+1 < len(args):
			cursor = args[i+1]
			i++
		case prefix == "":
			prefix = args[i]
		default:
			fmt.Fprintf(os.Stderr, "unknown kv scan arg: %s\n", args[i])
			os.Exit(1)
		}
	}
	if prefix == "" {
		fmt.Fprintln(os.Stderr, "Usage: nufs-cli kv scan <prefix> [--limit N] [--cursor KEY]")
		os.Exit(1)
	}
	if !kvPrefixAllowed(prefix) {
		fmt.Fprintf(os.Stderr, "prefix %q not allowed; allowed: %s\n",
			prefix, strings.Join(metadata.KVCatalogPrefixes(), " "))
		os.Exit(1)
	}
	page, err := store.KVScan(prefix, []byte(cursor), limit)
	if err != nil {
		fmt.Fprintf(os.Stderr, "kv scan: %v\n", err)
		os.Exit(1)
	}
	printKVPage(prefix, page.Keys, page.Values, page.HasMore, page.NextKey)
}

func kvRemote(api *remoteAPI, args []string) {
	sub := "scan"
	if len(args) > 0 {
		sub = args[0]
		args = args[1:]
	}
	switch sub {
	case "get":
		if len(args) < 1 {
			fmt.Fprintln(os.Stderr, "Usage: nufs-cli kv get <key>")
			os.Exit(1)
		}
		kvRemoteGet(api, args[0])
	case "scan":
		var prefix string
		limit := 100
		var cursor string
		for i := 0; i < len(args); i++ {
			switch {
			case args[i] == "--limit" && i+1 < len(args):
				fmt.Sscanf(args[i+1], "%d", &limit)
				i++
			case args[i] == "--cursor" && i+1 < len(args):
				cursor = args[i+1]
				i++
			case prefix == "":
				prefix = args[i]
			default:
				fmt.Fprintf(os.Stderr, "unknown kv scan arg: %s\n", args[i])
				os.Exit(1)
			}
		}
		if prefix == "" {
			fmt.Fprintln(os.Stderr, "Usage: nufs-cli kv scan <prefix> [--limit N] [--cursor KEY]")
			os.Exit(1)
		}
		kvRemoteScan(api, prefix, limit, cursor)
	default:
		fmt.Fprintf(os.Stderr, "unknown kv subcommand (remote): %s\n", sub)
		os.Exit(1)
	}
}

func kvRemoteGet(api *remoteAPI, key string) {
	resp := api.get("/api/v1/kv?get=" + url.QueryEscape(key))
	printKVRemoteGet(resp)
}

func kvRemoteScan(api *remoteAPI, prefix string, limit int, cursor string) {
	q := url.Values{}
	q.Set("scan", prefix)
	q.Set("limit", fmt.Sprintf("%d", limit))
	if cursor != "" {
		q.Set("cursor", cursor)
	}
	resp := api.get("/api/v1/kv?" + q.Encode())
	printKVRemoteScan(resp)
}

func printKVRemoteGet(data []byte) {
	var r struct {
		Key       string `json:"key"`
		Found     bool   `json:"found"`
		Value     string `json:"value"`
		ValueB64  string `json:"value_b64"`
		ValueSize int    `json:"value_size"`
	}
	json.Unmarshal(data, &r)
	if !r.Found {
		fmt.Printf("key %q: not found\n", r.Key)
		return
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(w, "Key:\t%s\n", r.Key)
	fmt.Fprintf(w, "Size:\t%d\n", r.ValueSize)
	fmt.Fprintf(w, "Value:\t%s\n", r.Value)
	fmt.Fprintf(w, "Value(b64):\t%s\n", r.ValueB64)
	w.Flush()
}

func printKVRemoteScan(data []byte) {
	var r struct {
		Prefix  string `json:"prefix"`
		Count   int    `json:"count"`
		HasMore bool   `json:"has_more"`
		NextKey string `json:"next_key"`
		Entries []struct {
			Key       string `json:"key"`
			ValueB64  string `json:"value_b64"`
			ValueSize int    `json:"value_size"`
		} `json:"entries"`
	}
	json.Unmarshal(data, &r)
	keys := make([][]byte, 0, len(r.Entries))
	vals := make([][]byte, 0, len(r.Entries))
	for _, e := range r.Entries {
		keys = append(keys, []byte(e.Key))
		raw, _ := base64.StdEncoding.DecodeString(e.ValueB64)
		vals = append(vals, raw)
	}
	printKVPage(r.Prefix, keys, vals, r.HasMore, []byte(r.NextKey))
}

func printKVPage(prefix string, keys, vals [][]byte, hasMore bool, nextKey []byte) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "KEY\tVALUE")
	for i, k := range keys {
		fmt.Fprintf(w, "%s\t%s\n", string(k), printableBytes(vals[i]))
	}
	w.Flush()
	fmt.Printf("\nTotal: %d entries (prefix %q)\n", len(keys), prefix)
	if hasMore {
		fmt.Printf("Has more pages; resume with --cursor %q\n", string(nextKey))
	}
}

// kvPrefixAllowed mirrors the metad ops KV scan allowlist so the local CLI
// rejects disallowed prefixes before touching the store.
func kvPrefixAllowed(prefix string) bool {
	for _, p := range metadata.KVCatalogPrefixes() {
		if prefix == p {
			return true
		}
	}
	return false
}
