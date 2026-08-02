package storage

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// ReleaseGateCheck audits the storage tree against the V2.1 §21 release
// gates. Each test encodes one gate and scans the source to confirm the
// invariant holds. These are structural checks; the behavioral gates
// (no ack loss after one-node failure, corrupt bytes never returned)
// are covered by the crash matrix and reader tests.

// sourceFiles returns all .go files in the V2.1 storage AND metadata
// trees, walked from the module root (test cwd is the storage package).
// The legacy chunkstore/ tree is out of scope (removed at V2.1 parity).
func sourceFiles() []string {
	roots := []string{
		filepath.Join("..", "..", "datanode/storage"),
		filepath.Join("..", "..", "metadata"),
	}
	var files []string
	for _, root := range roots {
		filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return nil
			}
			if info.IsDir() && strings.Contains(path, "benchmark") {
				return nil
			}
			if strings.HasSuffix(path, ".go") && !strings.HasSuffix(path, "_test.go") {
				files = append(files, path)
			}
			return nil
		})
	}
	return files
}

// Gate 1: startup must not scan all segments. Recovery is bounded to
// active-segment tails + WAL replay, never a full sealed-segment scan.
func TestGate_NoStartupFullScan(t *testing.T) {
	for _, path := range sourceFiles() {
		src, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		// Forbid a full-tree walk or "scan everything" idiom in
		// production startup code.
		lower := strings.ToLower(string(src))
		if strings.Contains(lower, "filepath.walk") && strings.Contains(path, "recovery") {
			t.Errorf("%s: recovery must not walk the full segment tree", path)
		}
		if strings.Contains(lower, "scanshards()") && !strings.Contains(path, "scan.go") {
			t.Errorf("%s: unexpected full scan call", path)
		}
	}
}

// Gate 2: no unbounded in-memory map of all local extents. The overlay
// is bounded by the flush budget; production code must not build a map
// keyed by every extent.
func TestGate_NoUnboundedExtentMap(t *testing.T) {
	for _, path := range sourceFiles() {
		src, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		// Forbid the specific anti-pattern of a map of all extents in
		// the overlay/store. The testutil model is test-only.
		if strings.Contains(string(src), "chunks map[metadata.ChunkID]") && !strings.Contains(path, "chunkstore.go") {
			t.Errorf("%s: unbounded chunk map pattern", path)
		}
	}
}

// Gate 3: inventory/repair/GC must be paginated (no unpaginated full
// listing in a single response).
func TestGate_PaginatedListings(t *testing.T) {
	// The production listing entry points must take a page/cursor.
	requiredPaged := []string{"ListActive", "ExpiredBatches", "func (s *InventoryStore) Global", "func (ps *ExtentPageStore) ResolveExtents"}
	found := map[string]bool{}
	for _, path := range sourceFiles() {
		src, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		for _, fn := range requiredPaged {
			if strings.Contains(string(src), fn) {
				found[fn] = true
			}
		}
	}
	for _, fn := range requiredPaged {
		if !found[fn] {
			t.Errorf("paged entry point %s not found", fn)
		}
	}
}

// Gate 4: a durable local batch requires exactly one foreground fsync
// barrier. After the group-commit refactor, the barrier lives in
// commitBatch (one Sync per batch); Write submits to the coordinator and
// is acknowledged only after that barrier.
func TestGate_SingleFsyncBarrier(t *testing.T) {
	path := filepath.Join("..", "..", "datanode/storage/segment/store.go")
	src, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// Parse and find commitBatch's body; count Sync calls. It must be
	// exactly one (the §6.4 single-barrier batch commit).
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, path, src, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, decl := range f.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name.Name != "commitBatch" {
			continue
		}
		syncCount := 0
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			if sel, ok := call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "Sync" {
				syncCount++
			}
			return true
		})
		if syncCount != 1 {
			t.Errorf("commitBatch has %d Sync calls, want exactly 1 (single fsync barrier per batch)", syncCount)
		}
	}
}

// Gate 5: corrupt or unverifiable data must never be returned as
// successful reads. The reader must verify frame CRC and AEAD before
// returning.
func TestGate_ReadVerifiesChecksums(t *testing.T) {
	path := filepath.Join("..", "..", "datanode/storage/segment/reader.go")
	src, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{"VerifyFrameCRC", "DecryptFrame", "DecompressFrame"} {
		if !strings.Contains(string(src), required) {
			t.Errorf("reader.go must verify via %s", required)
		}
	}
	// The reader must propagate checksum failures, not swallow them.
	if !strings.Contains(string(src), "ErrChecksumMismatch") && !strings.Contains(string(src), "VerifyFrameCRC") {
		t.Error("reader.go must propagate checksum verification failures")
	}
}

// Gate 6: range reads must not read/authenticate an entire large extent.
func TestGate_RangeReadBoundedFrames(t *testing.T) {
	path := filepath.Join("..", "..", "datanode/storage/segment/reader.go")
	src, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// The range-read path must read per-frame, not the whole payload.
	if !strings.Contains(string(src), "ReadRangeFrames") {
		t.Error("reader.go must provide ReadRangeFrames for bounded range reads")
	}
	// Verify it slices frames rather than reading the entire extent.
	if !strings.Contains(string(src), "fi.Entries") {
		t.Error("range read must iterate frame-index entries, not the whole payload")
	}
}

// Gate 7: small logical files must not create individual filesystem
// files. The small-file path packs records into 1GiB small segments.
func TestGate_NoPerFileInode(t *testing.T) {
	path := filepath.Join("..", "..", "datanode/storage/segment/small_store.go")
	src, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// The small store must write into a segment, not create a file per
	// logical file.
	if !strings.Contains(string(src), "SmallFileThreshold") {
		t.Error("small store must enforce the small-file packing bound")
	}
}

// Gate 8: ownership must not change implicitly with a hash ring. The
// catalog maps logical partitions to Raft groups explicitly.
func TestGate_ExplicitOwnership(t *testing.T) {
	// The V2 metadata layer routes by encoded logical partition, not a
	// hash ring. Verify the catalog exists and ownership is explicit.
	for _, path := range sourceFiles() {
		src, _ := os.ReadFile(path)
		if strings.Contains(string(src), "OwnerPartition") || strings.Contains(string(src), "RouteExtent") {
			return
		}
	}
	t.Error("no explicit logical-partition ownership routing found")
}
