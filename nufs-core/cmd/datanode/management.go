package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"

	"github.com/example/dfs/datanode"
)

// ============================================================
// Management socket protocol — identical to the supervisor
// implementation so existing tooling keeps working.
// ============================================================

type sockMsg struct {
	Cmd  string `json:"cmd"`
	Path string `json:"path,omitempty"`
}

type sockResp struct {
	Status string      `json:"status"`
	Error  string      `json:"error,omitempty"`
	Data   interface{} `json:"data,omitempty"`
}

// ============================================================
// Management server — in-process JBOD management socket
// ============================================================

// managementServer runs a unix-domain-socket listener that accepts
// status/adopt/retire commands from the CLI.
type managementServer struct {
	sockPath string
	store    *datanode.ChunkStore
	disk     *datanode.DiskManager
	listener net.Listener
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

func newManagementServer(sockPath string, store *datanode.ChunkStore, dm *datanode.DiskManager) *managementServer {
	return &managementServer{
		sockPath: sockPath,
		store:    store,
		disk:     dm,
		stopCh:   make(chan struct{}),
	}
}

func (ms *managementServer) Start() error {
	os.Remove(ms.sockPath) // clean stale socket
	ln, err := net.Listen("unix", ms.sockPath)
	if err != nil {
		return fmt.Errorf("datanode: management socket listen: %w", err)
	}
	ms.listener = ln
	ms.wg.Add(1)
	go ms.acceptLoop()
	return nil
}

func (ms *managementServer) Stop() {
	close(ms.stopCh)
	if ms.listener != nil {
		ms.listener.Close()
	}
	ms.wg.Wait()
	os.Remove(ms.sockPath)
}

func (ms *managementServer) acceptLoop() {
	defer ms.wg.Done()
	for {
		conn, err := ms.listener.Accept()
		if err != nil {
			select {
			case <-ms.stopCh:
				return
			default:
				continue
			}
		}
		go ms.handleConn(conn)
	}
}

func (ms *managementServer) handleConn(conn net.Conn) {
	defer conn.Close()

	var msg sockMsg
	if err := json.NewDecoder(conn).Decode(&msg); err != nil {
		json.NewEncoder(conn).Encode(sockResp{Status: "error", Error: "parse error"})
		return
	}

	switch msg.Cmd {
	case "status":
		ms.handleStatus(conn)
	case "adopt":
		ms.handleAdopt(conn, msg.Path)
	case "retire":
		ms.handleRetire(conn, msg.Path)
	default:
		json.NewEncoder(conn).Encode(sockResp{Status: "error", Error: "unknown command"})
	}
}

func (ms *managementServer) handleStatus(conn net.Conn) {
	totalBytes, chunkCount := ms.store.Stats()
	infos := ms.store.DiskInfos()

	type diskStatus struct {
		Index  int    `json:"index"`
		Dir    string `json:"dir"`
		Failed bool   `json:"failed"`
		Chunks int64  `json:"chunks"`
		Bytes  int64  `json:"bytes"`
	}

	// Count chunks per disk from ListChunks.
	chunksPerDisk := make(map[int]struct{ count, bytes int64 })
	for _, info := range ms.store.ListChunks() {
		v := chunksPerDisk[info.DiskIndex]
		v.count++
		v.bytes += info.Size
		chunksPerDisk[info.DiskIndex] = v
	}

	disks := make([]diskStatus, 0, len(infos))
	for _, di := range infos {
		v := chunksPerDisk[di.Index]
		disks = append(disks, diskStatus{
			Index:  di.Index,
			Dir:    di.Dir,
			Failed: di.Failed,
			Chunks: v.count,
			Bytes:  v.bytes,
		})
	}

	data := map[string]interface{}{
		"disks":        disks,
		"total_chunks": chunkCount,
		"total_bytes":  totalBytes,
	}
	json.NewEncoder(conn).Encode(sockResp{Status: "ok", Data: data})
}

func (ms *managementServer) handleAdopt(conn net.Conn, dir string) {
	if dir == "" {
		json.NewEncoder(conn).Encode(sockResp{Status: "error", Error: "path required"})
		return
	}
	idx, err := ms.store.AddDisk(dir, 8, 8, nil)
	if err != nil {
		json.NewEncoder(conn).Encode(sockResp{Status: "error", Error: err.Error()})
		return
	}
	json.NewEncoder(conn).Encode(sockResp{Status: "ok", Data: map[string]interface{}{
		"dir": dir, "index": idx,
	}})
}

func (ms *managementServer) handleRetire(conn net.Conn, dir string) {
	if dir == "" {
		json.NewEncoder(conn).Encode(sockResp{Status: "error", Error: "path required"})
		return
	}
	for _, di := range ms.store.DiskInfos() {
		if di.Dir == dir {
			if err := ms.store.RemoveDisk(di.Index); err != nil {
				json.NewEncoder(conn).Encode(sockResp{Status: "error", Error: err.Error()})
				return
			}
			json.NewEncoder(conn).Encode(sockResp{Status: "ok"})
			return
		}
	}
	json.NewEncoder(conn).Encode(sockResp{Status: "error", Error: "dir not found"})
}

// ============================================================
// CLI client — runManagementCommand + findSockPath
// ============================================================

func runManagementCommand(cmd string, args []string) {
	sockPath := ""
	if cmd == "status" {
		sockPath = findSockPath(args)
	} else if len(args) > 0 {
		sockPath = findSockPath([]string{args[0]})
	}

	if sockPath == "" {
		log.Fatalf("datanode: cannot find management socket; run from data dir or specify path")
	}

	conn, err := net.Dial("unix", sockPath)
	if err != nil {
		log.Fatalf("datanode: connect to management socket: %v", err)
	}
	defer conn.Close()

	msg := sockMsg{Cmd: cmd}
	if (cmd == "adopt" || cmd == "retire") && len(args) > 0 {
		msg.Path = args[0]
	}

	json.NewEncoder(conn).Encode(msg)

	var resp sockResp
	json.NewDecoder(conn).Decode(&resp)
	if resp.Status != "ok" {
		log.Fatalf("datanode: %s", resp.Error)
	}

	if cmd == "status" {
		data, _ := json.MarshalIndent(resp.Data, "", "  ")
		fmt.Println(string(data))
	} else {
		fmt.Println("ok")
	}
}

func findSockPath(args []string) string {
	for _, a := range args {
		if a != "" {
			candidate := filepath.Join(a, ".datanode.sock")
			if _, err := os.Stat(candidate); err == nil {
				return candidate
			}
		}
	}
	for _, env := range os.Environ() {
		if len(env) > 10 && env[:10] == "DATA_DIRS=" {
			parts := splitAndClean(env[10:])
			if len(parts) > 0 {
				candidate := filepath.Join(parts[0], ".datanode.sock")
				if _, err := os.Stat(candidate); err == nil {
					return candidate
				}
			}
		}
	}
	// Check common default locations
	for _, candidate := range []string{
		"/var/lib/dfs/data/.datanode.sock",
		filepath.Join(os.TempDir(), ".datanode.sock"),
	} {
		if _, err := os.Stat(candidate); err == nil {
			return candidate
		}
	}
	return ""
}

func startManagementServer(store *datanode.ChunkStore, dm *datanode.DiskManager, dataDirs []string) (func(), error) {
	sockPath := filepath.Join(dataDirs[0], ".datanode.sock")
	ms := newManagementServer(sockPath, store, dm)
	if err := ms.Start(); err != nil {
		return nil, err
	}
	log.Printf("datanode: management socket ready at %s", sockPath)
	return ms.Stop, nil
}
