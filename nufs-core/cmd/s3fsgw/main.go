// Command s3fsgw mounts an S3 bucket as a FUSE filesystem.
//
// Usage:
//
//	s3fsgw [flags] <s3-endpoint/bucket/prefix> <mountpoint>
//
// Examples:
//
//	s3fsgw https://s3.amazonaws.com/my-bucket /mnt/s3
//	s3fsgw -cache-dir /tmp/s3cache -scan-ttl 30s http://localhost:9000/mybucket /mnt/s3
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/example/dfs/gateway/s3fs"
)

func main() {
	cacheDir := flag.String("cache-dir", "/var/lib/s3fs/cache", "Cache directory")
	scanTTL := flag.Duration("scan-ttl", 60*time.Second, "Directory scan cache TTL")
	readOnly := flag.Bool("read-only", false, "Read-only mode")
	cacheQuota := flag.Int64("cache-quota", 0, "Cache disk quota in bytes (0=unlimited)")
	metricsAddr := flag.String("metrics-addr", ":9900", "Metrics/health HTTP address")
	insecure := flag.Bool("insecure", false, "Skip TLS verification")
	debug := flag.Bool("debug", false, "Debug logging")
	uid := flag.Uint("uid", 0, "File owner UID")
	gid := flag.Uint("gid", 0, "File owner GID")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [flags] <s3-endpoint/bucket/prefix> <mountpoint>\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Mount an S3 bucket as a local FUSE filesystem.\n\n")
		fmt.Fprintf(os.Stderr, "Examples:\n")
		fmt.Fprintf(os.Stderr, "  %s https://s3.amazonaws.com/my-bucket /mnt/s3\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s http://localhost:9000/mybucket /mnt/s3\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Flags:\n")
		flag.PrintDefaults()
	}
	flag.Parse()

	if flag.NArg() < 2 {
		flag.Usage()
		os.Exit(1)
	}

	target := flag.Arg(0)
	mountpoint := flag.Arg(1)

	// Parse S3 target URL.
	u, bucket, basePath, err := s3fs.ParseTarget(target)
	if err != nil {
		log.Fatalf("Invalid target: %v", err)
	}

	// Build config.
	cfg := &s3fs.Config{
		Bucket:      bucket,
		BasePath:    basePath,
		Target:      u,
		CacheDir:    *cacheDir,
		ScanTTL:     *scanTTL,
		MetricsAddr: *metricsAddr,
		ReadOnly:    *readOnly,
		CacheQuota:  *cacheQuota,
		UID:         uint32(*uid),
		GID:         uint32(*gid),
		Insecure:    *insecure,
		Debug:       *debug,
	}

	// Create filesystem.
	log.Printf("s3fsgw: mounting %s -> %s", target, mountpoint)
	fs, err := s3fs.New(cfg)
	if err != nil {
		log.Fatalf("Failed to create filesystem: %v", err)
	}

	// Serve FUSE requests.
	if err := fs.Serve(mountpoint); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
