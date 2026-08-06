// Command loadgen drives a running datanode's TCP chunk protocol with a
// sustained write+read loop so the ops dashboard's telemetry (read/write IOPS,
// throughput, fsync, chunk count, capacity) actually move. Dev/demo tool only.
//
// Usage:
//
//	loadgen -addr 127.0.0.1:19100 -chunks 2048 -wps 40 -size 65536
package main

import (
	"flag"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/example/dfs/datanode"
	"github.com/example/dfs/metadata"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:19100", "datanode chunk TCP listen addr")
	chunks := flag.Int("chunks", 2048, "ring size of distinct chunk IDs to cycle")
	base := flag.Int("base", 1, "first chunk ID")
	wps := flag.Float64("wps", 40, "writes per second")
	rps := flag.Float64("rps", 120, "reads per second")
	size := flag.Int("size", 65536, "payload size per chunk (bytes)")
	flag.Parse()

	c := datanode.NewClient(*addr)
	if err := c.Connect(); err != nil {
		log.Fatalf("connect %s: %v", *addr, err)
	}
	defer c.Close()

	rng := rand.New(rand.NewSource(42))
	var (
		seq int64
		nw  int64
		nr  int64
	)
	writeTick := time.NewTicker(time.Duration(float64(time.Second) / *wps))
	readTick := time.NewTicker(time.Duration(float64(time.Second) / *rps))
	defer writeTick.Stop()
	defer readTick.Stop()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	log.Printf("loadgen: %s chunks=%d wps=%.0f rps=%.0f size=%d", *addr, *chunks, *wps, *rps, *size)
	t0 := time.Now()
	for {
		select {
		case <-sig:
			log.Printf("loadgen: %d writes, %d reads in %s", nw, nr, time.Since(t0).Round(time.Second))
			return
		case <-writeTick.C:
			seq++
			id := metadata.ChunkID(*base + int(seq)%*chunks)
			data := make([]byte, *size)
			rng.Read(data)
			resp, err := c.WriteChunk(id, data)
			nw++
			if err != nil || resp.Status != datanode.StatusOK {
				log.Printf("write chunk %d err=%v status=%d %s", id, err, resp.Status, resp.Error)
			}
		case <-readTick.C:
			// Only read chunks already written (seq counts writes; cap at ring).
			lo := int(seq)
			if lo > *chunks {
				lo = *chunks
			}
			if lo <= 0 {
				continue
			}
			id := metadata.ChunkID(*base + rng.Intn(lo))
			resp, err := c.ReadChunk(id, 0, int32(*size))
			nr++
			if err != nil || resp.Status != datanode.StatusOK {
				log.Printf("read chunk %d err=%v status=%d %s", id, err, resp.Status, resp.Error)
			}
		}
	}
}