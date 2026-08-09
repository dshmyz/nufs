// Command storage-crash-helper drives a real V2.1 segment store in a
// separate process so a test can SIGKILL it mid-flight.
//
// It exists because a clean Store.Close() proves nothing about crash
// recovery: Close flushes the overlay into Pebble, so a test that closes
// the store is only testing the graceful path. This helper deliberately
// never closes the store and installs no signal handler, so SIGKILL
// leaves exactly the on-disk state an abrupt process death produces.
//
// Every acknowledged mutation is reported on stdout as one flushed JSON
// line before the helper moves on, so the parent knows precisely which
// operations were acknowledged at the moment it pulled the trigger:
//
//	{"op":"put","extent_id":42,"generation":1,"checksum":1234,"ack":true}
//
// After the requested work completes the helper prints a "ready" line
// and blocks forever, waiting to be killed.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sync"

	"github.com/dshmyz/nufs/nufs-core/datanode/storage"
	"github.com/dshmyz/nufs/nufs-core/datanode/storage/segment"
)

// ackLine is one acknowledged mutation, reported to the parent.
type ackLine struct {
	Op         string `json:"op"`
	ExtentID   uint64 `json:"extent_id"`
	Generation uint64 `json:"generation"`
	Checksum   uint32 `json:"checksum"`
	Ack        bool   `json:"ack"`
}

func main() {
	var (
		dir         = flag.String("dir", "", "store directory (required)")
		writers     = flag.Int("writers", 16, "concurrent writers")
		writes      = flag.Int("writes", 4096, "total writes across all writers")
		deleteEvery = flag.Int("delete-every", 17, "delete every Nth acknowledged put (0 = never)")
		payloadSize = flag.Int("payload", 512, "payload bytes per write")
		segmentSize = flag.Int64("segment-size", 256<<20, "segment size in bytes")
	)
	flag.Parse()
	if *dir == "" {
		fmt.Fprintln(os.Stderr, "storage-crash-helper: --dir is required")
		os.Exit(2)
	}

	s, err := segment.New(segment.Config{
		Dir:         *dir,
		SegmentSize: *segmentSize,
		StreamID:    1,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "storage-crash-helper: open store: %v\n", err)
		os.Exit(1)
	}
	// Deliberately NO defer s.Close(): this process is meant to die
	// abruptly, and closing on the way out would make recovery trivial.

	out := bufio.NewWriter(os.Stdout)
	var mu sync.Mutex // serialises stdout across writers
	report := func(l ackLine) {
		mu.Lock()
		defer mu.Unlock()
		b, err := json.Marshal(l)
		if err != nil {
			fmt.Fprintf(os.Stderr, "storage-crash-helper: marshal: %v\n", err)
			os.Exit(1)
		}
		out.Write(b)
		out.WriteByte('\n')
		// Flush per line: an unflushed ack the parent never saw would be
		// indistinguishable from a lost write during verification.
		if err := out.Flush(); err != nil {
			fmt.Fprintf(os.Stderr, "storage-crash-helper: flush: %v\n", err)
			os.Exit(1)
		}
	}

	ctx := context.Background()
	perWriter := *writes / *writers
	if perWriter == 0 {
		perWriter = 1
	}

	var wg sync.WaitGroup
	for w := 0; w < *writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				n := w*perWriter + i
				extentID := storage.ExtentID(n + 1)
				payload := payloadFor(n, *payloadSize)
				if _, err := s.Write(ctx, &storage.WriteRequest{
					ExtentID:   extentID,
					Generation: 1,
					Data:       payload,
				}); err != nil {
					// A rejected write was never acknowledged, so it is not
					// reported and the parent will not require it to survive.
					continue
				}
				report(ackLine{
					Op: "put", ExtentID: uint64(extentID), Generation: 1,
					Checksum: storage.CRC32C(payload), Ack: true,
				})

				if *deleteEvery > 0 && n%*deleteEvery == 0 {
					if err := s.Delete(ctx, &storage.DeleteRequest{
						ExtentID: extentID, Generation: 1,
					}); err != nil {
						continue
					}
					report(ackLine{
						Op: "delete", ExtentID: uint64(extentID), Generation: 1, Ack: true,
					})
				}
			}
		}(w)
	}
	wg.Wait()

	// Signal that all requested work is acknowledged and durable, then
	// block. The parent kills us from here.
	mu.Lock()
	out.WriteString("{\"op\":\"ready\"}\n")
	out.Flush()
	mu.Unlock()

	select {} // wait for SIGKILL
}

// payloadFor builds a deterministic payload the parent can recompute, so
// recovery can be checked byte-exact without shipping the data across.
func payloadFor(n, size int) []byte {
	if size <= 0 {
		size = 512
	}
	seed := fmt.Sprintf("extent-%d:", n)
	b := make([]byte, size)
	for i := range b {
		b[i] = seed[i%len(seed)]
	}
	return b
}
