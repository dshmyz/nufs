package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/example/dfs/metadata"
)

const nodeIDFile = ".dfs-node-id"

type childState string

const (
	childRunning  childState = "running"
	childStarting childState = "starting"
	childStopped  childState = "stopped"
	childCrashed  childState = "crashed"
)

type childInfo struct {
	Dir     string
	Port    int
	NodeID  metadata.NodeID
	Pid     int
	State   childState
	Started time.Time
	Cmd     *exec.Cmd
}

type supervisor struct {
	mu           sync.Mutex
	dataDirs     []string
	basePort     int
	machineID    string
	externalHost string
	metaAddr     string
	rack         string
	zone         string
	capacityGB   int64
	children     map[string]*childInfo // key = dataDir
	sockPath     string
	stopCh       chan struct{}
	wg           sync.WaitGroup
	nextNodeID   metadata.NodeID
}

func runSupervisor(dataDirs []string, basePort int, machineID, externalHost, metaAddr, rack, zone string, capacityGB int64) {
	log.Printf("datanode: supervisor mode (%d disks, machine=%s)", len(dataDirs), machineID)

	sv := &supervisor{
		dataDirs:     dataDirs,
		basePort:     basePort,
		machineID:    machineID,
		externalHost: externalHost,
		metaAddr:     metaAddr,
		rack:         rack,
		zone:         zone,
		capacityGB:   capacityGB,
		children:     make(map[string]*childInfo),
		stopCh:       make(chan struct{}),
		nextNodeID:   1,
	}

	sockPath := filepath.Join(dataDirs[0], ".datanode.sock")
	sv.sockPath = sockPath

	for _, dir := range dataDirs {
		port := basePort + len(sv.children)
		sv.startChild(dir, port)
	}

	sv.wg.Add(1)
	go sv.monitorLoop()

	sv.wg.Add(1)
	go sv.socketListener(sockPath)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	log.Printf("datanode: supervisor shutting down")
	close(sv.stopCh)
	sv.stopAll()
	os.Remove(sockPath)
	sv.wg.Wait()
	log.Printf("datanode: supervisor shutdown complete")
}

func (sv *supervisor) startChild(dir string, port int) {
	sv.mu.Lock()
	nextID := sv.nextNodeID
	sv.nextNodeID++
	sv.mu.Unlock()

	nid := sv.loadOrAllocateNodeID(dir, nextID)
	host := sv.externalHost
	if host == "" {
		host = "127.0.0.1"
	}
	addr := fmt.Sprintf("%s:%d", host, port)

	args := []string{
		"--node-id", fmt.Sprintf("%d", nid),
		"--listen", addr,
		"--data-dir", dir,
		"--machine-id", sv.machineID,
		"--metadata", sv.metaAddr,
		"--rack", sv.rack,
		"--zone", sv.zone,
		"--capacity", fmt.Sprintf("%d", sv.capacityGB),
	}

	cmd := exec.Command(os.Args[0], args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		log.Printf("datanode: failed to start child for %s: %v", dir, err)
		return
	}

	ci := &childInfo{
		Dir:     dir,
		Port:    port,
		NodeID:  nid,
		Pid:     cmd.Process.Pid,
		State:   childRunning,
		Started: time.Now(),
		Cmd:     cmd,
	}

	sv.mu.Lock()
	sv.children[dir] = ci
	sv.mu.Unlock()

	log.Printf("datanode: started child node_id=%d pid=%d dir=%s port=%d", nid, cmd.Process.Pid, dir, port)

	go func() {
		err := cmd.Wait()
		sv.mu.Lock()
		if ci.State != childStopped {
			if err != nil {
				ci.State = childCrashed
				log.Printf("datanode: child node_id=%d dir=%s crashed: %v", nid, dir, err)
			} else {
				ci.State = childStopped
				log.Printf("datanode: child node_id=%d dir=%s exited", nid, dir)
			}
		}
		sv.mu.Unlock()
	}()
}

func (sv *supervisor) loadOrAllocateNodeID(dir string, fallback metadata.NodeID) metadata.NodeID {
	path := filepath.Join(dir, nodeIDFile)
	b, err := os.ReadFile(path)
	if err == nil {
		id, err := strconv.ParseUint(strings.TrimSpace(string(b)), 10, 64)
		if err == nil && id > 0 {
			return metadata.NodeID(id)
		}
	}
	if err := os.WriteFile(path, []byte(fmt.Sprintf("%d\n", fallback)), 0644); err != nil {
		log.Printf("datanode: warning: failed to persist node ID for %s: %v", dir, err)
	}
	return fallback
}

func (sv *supervisor) stopAll() {
	sv.mu.Lock()
	for _, ci := range sv.children {
		if ci.Cmd != nil && ci.Cmd.Process != nil {
			log.Printf("datanode: stopping child pid=%d dir=%s", ci.Pid, ci.Dir)
			ci.Cmd.Process.Signal(syscall.SIGTERM)
		}
	}
	sv.mu.Unlock()

	done := make(chan struct{}, 1)
	go func() {
		sv.mu.Lock()
		for _, ci := range sv.children {
			if ci.Cmd != nil {
				ci.Cmd.Wait()
			}
			ci.State = childStopped
		}
		sv.mu.Unlock()
		done <- struct{}{}
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		sv.mu.Lock()
		for _, ci := range sv.children {
			if ci.Cmd != nil && ci.Cmd.Process != nil {
				ci.Cmd.Process.Kill()
			}
		}
		sv.mu.Unlock()
	}
}

func (sv *supervisor) monitorLoop() {
	defer sv.wg.Done()
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	maxRestarts := 3
	restartCount := make(map[string]int)

	for {
		select {
		case <-sv.stopCh:
			return
		case <-ticker.C:
			sv.mu.Lock()
			for _, ci := range sv.children {
				if ci.State == childCrashed {
					if restartCount[ci.Dir] >= maxRestarts {
						log.Printf("datanode: child dir=%s max restarts reached, not restarting", ci.Dir)
						continue
					}
					backoff := time.Duration(1<<min(restartCount[ci.Dir], 3)) * time.Second
					log.Printf("datanode: restarting child dir=%s (attempt %d/%d, backoff %v)",
						ci.Dir, restartCount[ci.Dir]+1, maxRestarts, backoff)
					restartCount[ci.Dir]++
					go func(info *childInfo) {
						time.Sleep(backoff)
						sv.startChild(info.Dir, info.Port)
					}(ci)
				}
			}
			sv.mu.Unlock()
		}
	}
}

type sockMsg struct {
	Cmd   string `json:"cmd"`
	Path  string `json:"path,omitempty"`
}

type sockResp struct {
	Status string      `json:"status"`
	Error  string      `json:"error,omitempty"`
	Data   interface{} `json:"data,omitempty"`
}

func (sv *supervisor) socketListener(sockPath string) {
	os.Remove(sockPath)
	listener, err := net.Listen("unix", sockPath)
	if err != nil {
		log.Printf("datanode: supervisor socket listen error: %v", err)
		return
	}

	go func() {
		<-sv.stopCh
		listener.Close()
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		go sv.handleSocketConn(conn)
	}
}

func (sv *supervisor) handleSocketConn(conn net.Conn) {
	defer conn.Close()

	var msg sockMsg
	if err := json.NewDecoder(conn).Decode(&msg); err != nil {
		json.NewEncoder(conn).Encode(sockResp{Status: "error", Error: "parse error"})
		return
	}

	switch msg.Cmd {
	case "status":
		sv.mu.Lock()
		type childStatus struct {
			Dir      string     `json:"dir"`
			Port     int        `json:"port"`
			NodeID   uint64     `json:"node_id"`
			Pid      int        `json:"pid"`
			State    childState `json:"state"`
			Uptime   string     `json:"uptime"`
		}
		var statuses []childStatus
		for _, ci := range sv.children {
			uptime := time.Since(ci.Started).Truncate(time.Second).String()
			statuses = append(statuses, childStatus{
				Dir: ci.Dir, Port: ci.Port, NodeID: uint64(ci.NodeID),
				Pid: ci.Pid, State: ci.State, Uptime: uptime,
			})
		}
		sv.mu.Unlock()
		json.NewEncoder(conn).Encode(sockResp{Status: "ok", Data: statuses})

	case "adopt":
		dir := msg.Path
		if dir == "" {
			json.NewEncoder(conn).Encode(sockResp{Status: "error", Error: "path required"})
			return
		}
		sv.mu.Lock()
		if _, exists := sv.children[dir]; exists {
			sv.mu.Unlock()
			json.NewEncoder(conn).Encode(sockResp{Status: "error", Error: "dir already managed"})
			return
		}
		port := sv.basePort + len(sv.children)
		sv.mu.Unlock()
		sv.startChild(dir, port)
		json.NewEncoder(conn).Encode(sockResp{Status: "ok", Data: map[string]interface{}{
			"dir": dir, "port": port,
		}})

	case "retire":
		dir := msg.Path
		if dir == "" {
			json.NewEncoder(conn).Encode(sockResp{Status: "error", Error: "path required"})
			return
		}
		sv.mu.Lock()
		ci, exists := sv.children[dir]
		if !exists {
			sv.mu.Unlock()
			json.NewEncoder(conn).Encode(sockResp{Status: "error", Error: "dir not found"})
			return
		}
		sv.mu.Unlock()

		if ci.Cmd != nil && ci.Cmd.Process != nil {
			ci.Cmd.Process.Signal(syscall.SIGTERM)
		}

		done := make(chan struct{}, 1)
		go func() {
			if ci.Cmd != nil {
				ci.Cmd.Wait()
			}
			done <- struct{}{}
		}()
		select {
		case <-done:
		case <-time.After(15 * time.Second):
			if ci.Cmd != nil && ci.Cmd.Process != nil {
				ci.Cmd.Process.Kill()
			}
		}

		sv.mu.Lock()
		delete(sv.children, dir)
		sv.mu.Unlock()
		json.NewEncoder(conn).Encode(sockResp{Status: "ok"})

	default:
		json.NewEncoder(conn).Encode(sockResp{Status: "error", Error: "unknown command"})
	}
}

func runManagementCommand(cmd string, args []string) {
	sockPath := ""
	if cmd == "status" {
		sockPath = findSockPath(args)
	} else if len(args) > 0 {
		sockPath = findSockPath([]string{args[0]})
	}

	if sockPath == "" {
		log.Fatalf("datanode: cannot find supervisor socket; run from data dir or specify path")
	}

	conn, err := net.Dial("unix", sockPath)
	if err != nil {
		log.Fatalf("datanode: connect to supervisor: %v", err)
	}
	defer conn.Close()

	msg := sockMsg{Cmd: cmd}
	if cmd == "adopt" || cmd == "retire" {
		if len(args) == 0 {
			log.Fatalf("datanode: %s requires a path argument", cmd)
		}
		msg.Path = args[0]
		if cmd == "adopt" {
			sockPath = findSockPath(args)
		}
	}

	json.NewEncoder(conn).Encode(msg)

	var resp sockResp
	json.NewDecoder(conn).Decode(&resp)
	if resp.Status != "ok" {
		log.Fatalf("datanode: supervisor: %s", resp.Error)
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
		if strings.HasPrefix(env, "DATANODE_DATA_DIR=") {
			parts := strings.SplitN(env, "=", 2)
			if len(parts) == 2 {
				candidate := filepath.Join(parts[1], ".datanode.sock")
				if _, err := os.Stat(candidate); err == nil {
					return candidate
				}
			}
		}
	}
	return ""
}
