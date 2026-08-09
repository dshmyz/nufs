// nufs-import imports data from external storage systems into NUFS.
//
// Usage:
//
//	nufs-import s3 [flags] <s3-bucket> <dfs-bucket>
//	nufs-import nfs [flags] <nfs-path> <dfs-bucket>
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/dshmyz/nufs/nufs-core/metadata"
)

func main() {
	var (
		metaAddr  = flag.String("meta-addr", "localhost:8091", "Metadata HTTP address")
		metaDir   = flag.String("meta-dir", "", "Local Pebble metadata directory (auto-detect)")
		workers   = flag.Int("workers", 4, "Number of parallel import workers")
		dryRun    = flag.Bool("dry-run", false, "Print what would be imported without actually importing")
		overwrite = flag.Bool("overwrite", false, "Overwrite existing files")
		prefix    = flag.String("prefix", "", "Only import files with this prefix")
		exclude   = flag.String("exclude", "", "Exclude files matching this pattern")
	)
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, `NUFS Data Import Tool

Import data from external storage systems into NUFS.

Usage:
  nufs-import [flags] <source-type> <source-path> <dfs-bucket>

Source types:
  s3   Import from an S3-compatible bucket
  nfs  Import from a local/NFS filesystem path

Flags:
`)
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, `
Examples:
  nufs-import s3 my-s3-bucket my-dfs-bucket
  nfs-import nfs /mnt/nfs/data my-dfs-bucket
`)
	}
	flag.Parse()

	args := flag.Args()
	if len(args) < 3 {
		flag.Usage()
		os.Exit(1)
	}

	sourceType, sourcePath, bucket := args[0], args[1], args[2]

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var importer Importer
	switch sourceType {
	case "s3":
		importer = newS3Importer(sourcePath, bucket, *metaAddr, *workers, *dryRun)
	case "nfs":
		if *metaDir != "" {
			// Local mode
			store, err := metadata.NewPebbleStore(metadata.PebbleStoreConfig{Dir: *metaDir})
			if err != nil {
				log.Fatalf("open local metadata: %v", err)
			}
			defer store.Close()
			importer = newNFSLocalImporter(sourcePath, bucket, store, *workers, *dryRun)
		} else {
			// Remote mode
			importer = newNFSRemoteImporter(sourcePath, bucket, *metaAddr, *workers, *dryRun)
		}
	default:
		fmt.Fprintf(os.Stderr, "unsupported source type: %s (use s3 or nfs)\n", sourceType)
		os.Exit(1)
	}

	fmt.Printf("Importing from %s:%s into DFS bucket %s\n", sourceType, sourcePath, bucket)
	fmt.Printf("Workers: %d, Dry-run: %v, Overwrite: %v\n", *workers, *dryRun, *overwrite)

	start := time.Now()
	if err := importer.Import(ctx, *prefix, *exclude, *overwrite); err != nil {
		log.Fatalf("import failed: %v", err)
	}
	fmt.Printf("\nImport completed in %s\n", time.Since(start).Round(time.Millisecond))
}

// Importer defines the interface for import sources.
type Importer interface {
	Import(ctx context.Context, prefix, exclude string, overwrite bool) error
}

// ============================================================
// S3 Importer
// ============================================================

type s3Importer struct {
	bucket    string
	dfsBucket string
	metaAddr  string
	workers   int
	dryRun    bool
}

func newS3Importer(bucket, dfsBucket, metaAddr string, workers int, dryRun bool) *s3Importer {
	return &s3Importer{
		bucket:    bucket,
		dfsBucket: dfsBucket,
		metaAddr:  metaAddr,
		workers:   workers,
		dryRun:    dryRun,
	}
}

func (i *s3Importer) Import(ctx context.Context, prefix, exclude string, overwrite bool) error {
	return fmt.Errorf("s3 import is not implemented: no S3 object reader or NUFS data writer is wired yet")
}

// ============================================================
// NFS Importer (Remote mode)
// ============================================================

type nfsRemoteImporter struct {
	sourcePath string
	dfsBucket  string
	metaAddr   string
	workers    int
	dryRun     bool
}

func newNFSRemoteImporter(sourcePath, dfsBucket, metaAddr string, workers int, dryRun bool) *nfsRemoteImporter {
	return &nfsRemoteImporter{
		sourcePath: sourcePath,
		dfsBucket:  dfsBucket,
		metaAddr:   metaAddr,
		workers:    workers,
		dryRun:     dryRun,
	}
}

func (i *nfsRemoteImporter) Import(ctx context.Context, prefix, exclude string, overwrite bool) error {
	return importFromFS(ctx, i.sourcePath, i.dfsBucket, i.metaAddr, i.workers, i.dryRun, prefix, exclude, overwrite, nil)
}

// ============================================================
// NFS Importer (Local mode)
// ============================================================

type nfsLocalImporter struct {
	sourcePath string
	dfsBucket  string
	store      *metadata.PebbleStore
	workers    int
	dryRun     bool
}

func newNFSLocalImporter(sourcePath, dfsBucket string, store *metadata.PebbleStore, workers int, dryRun bool) *nfsLocalImporter {
	return &nfsLocalImporter{
		sourcePath: sourcePath,
		dfsBucket:  dfsBucket,
		store:      store,
		workers:    workers,
		dryRun:     dryRun,
	}
}

func (i *nfsLocalImporter) Import(ctx context.Context, prefix, exclude string, overwrite bool) error {
	return importFromFS(ctx, i.sourcePath, i.dfsBucket, "", i.workers, i.dryRun, prefix, exclude, overwrite, i.store)
}

// importFromFS performs the actual filesystem import.
func importFromFS(ctx context.Context, sourcePath, dfsBucket, metaAddr string, workers int, dryRun bool, prefix, exclude string, overwrite bool, store *metadata.PebbleStore) error {
	// Ensure source path exists
	info, err := os.Stat(sourcePath)
	if err != nil {
		return fmt.Errorf("source path: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("source path must be a directory: %s", sourcePath)
	}

	// Collect all files
	var files []string
	err = filepath.Walk(sourcePath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}

		// Apply prefix filter
		relPath, _ := filepath.Rel(sourcePath, path)
		if prefix != "" && !strings.HasPrefix(relPath, prefix) {
			return nil
		}

		// Apply exclude filter
		if exclude != "" {
			matched, _ := filepath.Match(exclude, filepath.Base(path))
			if matched {
				return nil
			}
		}

		files = append(files, path)
		return nil
	})
	if err != nil {
		return fmt.Errorf("walk source: %w", err)
	}

	fmt.Printf("Found %d files to import\n", len(files))

	if dryRun {
		for _, f := range files {
			relPath, _ := filepath.Rel(sourcePath, f)
			fmt.Printf("  [DRY-RUN] would import: %s -> %s/%s\n", f, dfsBucket, relPath)
		}
		return nil
	}

	return fmt.Errorf("filesystem import is not implemented: refusing to create metadata-only files without writing chunk data")
}
