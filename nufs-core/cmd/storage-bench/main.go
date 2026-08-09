// Command storage-bench runs the V2.1 §19 DataNode performance
// acceptance benchmarks against the real storage engine (real fsync
// durability). Exit code is non-zero if any target is not met.
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/dshmyz/nufs/nufs-core/datanode/storage/benchmark"
)

func main() {
	dir := flag.String("dir", "/tmp/nufs-storage-bench", "benchmark data directory")
	flag.Parse()

	if err := os.MkdirAll(*dir, 0755); err != nil {
		fmt.Printf("mkdir: %v\n", err)
		os.Exit(1)
	}
	os.Exit(benchmark.RunAll(*dir))
}
