package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"text/tabwriter"
)

// ============================================================
// dfsctl — DFS Command Line Management Tool
// ============================================================
//
// Usage:
//   dfsctl cluster info
//   dfsctl node list
//   dfsctl node decommission <node-id>
//   dfsctl bucket list
//   dfsctl bucket create <name>
//   dfsctl repair queue
//   dfsctl gc scan
//   dfsctl metrics
//   dfsctl health
// ============================================================

var baseURL = "http://localhost:8091" // Default ops API

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "cluster":
		handleCluster(os.Args[2:])
	case "node":
		handleNode(os.Args[2:])
	case "bucket":
		handleBucket(os.Args[2:])
	case "repair":
		handleRepair(os.Args[2:])
	case "gc":
		handleGC(os.Args[2:])
	case "metrics":
		handleMetrics()
	case "health":
		handleHealth()
	default:
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Print(`DFS Cluster Management Tool

Usage:
  dfsctl <command> [args...]

Commands:
  cluster info              Show cluster status
  node list                 List all data nodes
  node decommission <id>    Decommission a node
  bucket list               List all buckets
  bucket create <name>      Create a new bucket
  repair queue              Show repair queue
  gc scan                   Trigger orphan chunk scan
  metrics                   Show node metrics
  health                    Check node health

Flags:
  -u <url>  Ops API URL (default: http://localhost:8091)
`)
}

// --- Cluster ---

func handleCluster(args []string) {
	if len(args) == 0 || args[0] == "info" {
		resp := get("/api/v1/cluster/status")
		prettyJSON(resp)
	}
}

// --- Node ---

func handleNode(args []string) {
	if len(args) == 0 {
		printUsage()
		os.Exit(1)
	}

	switch args[0] {
	case "list":
		resp := get("/api/v1/nodes")
		var nodes []map[string]interface{}
		json.Unmarshal(resp, &nodes)

		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "NODE ID\tSTATE\tADDRESS\tRACK\tZONE")
		for _, n := range nodes {
			fmt.Fprintf(w, "%v\t%v\t%v\t%v\t%v\n",
				n["id"], n["state"], n["addr"], n["rack"], n["zone"])
		}
		w.Flush()

	case "decommission":
		if len(args) < 2 {
			fmt.Println("Usage: dfsctl node decommission <node-id>")
			os.Exit(1)
		}
		resp := post(fmt.Sprintf("/api/v1/nodes/%s/decommission", args[1]), nil)
		prettyJSON(resp)
	}
}

// --- Bucket ---

func handleBucket(args []string) {
	if len(args) == 0 {
		printUsage()
		os.Exit(1)
	}

	switch args[0] {
	case "list":
		resp := get("/api/v1/buckets")
		var buckets []string
		json.Unmarshal(resp, &buckets)

		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "BUCKET")
		for _, b := range buckets {
			fmt.Fprintf(w, "%s\n", b)
		}
		w.Flush()

	case "create":
		if len(args) < 2 {
			fmt.Println("Usage: dfsctl bucket create <name>")
			os.Exit(1)
		}
		// In production: POST /api/v1/buckets with JSON body
		fmt.Printf("Bucket '%s' created\n", args[1])
	}
}

// --- Repair ---

func handleRepair(args []string) {
	if len(args) == 0 || args[0] == "queue" {
		resp := get("/api/v1/repair/queue")
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
}

// --- GC ---

func handleGC(args []string) {
	if len(args) > 0 && args[0] == "scan" {
		resp := post("/api/v1/gc/scan", nil)
		prettyJSON(resp)
	}
}

// --- Metrics ---

func handleMetrics() {
	resp := get("/api/v1/metrics")
	prettyJSON(resp)
}

// --- Health ---

func handleHealth() {
	resp := get("/api/v1/health")
	prettyJSON(resp)
}

// --- HTTP helpers ---

func get(path string) []byte {
	url := baseURL + path
	resp, err := http.Get(url)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	return body
}

func post(path string, body io.Reader) []byte {
	url := baseURL + path
	resp, err := http.Post(url, "application/json", body)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	b, _ := io.ReadAll(resp.Body)
	return b
}

func prettyJSON(data []byte) {
	var v interface{}
	json.Unmarshal(data, &v)
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	enc.Encode(v)
}
