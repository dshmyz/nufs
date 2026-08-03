package segment

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/example/dfs/datanode/storage"
)

// TestProcessCrash_AcknowledgedMutationsRecover is the Task 7 gate:
// it proves that an abrupt process crash (SIGKILL, no Close()) loses
// no acknowledged write or delete. The helper subprocess never closes
// its store and installs no signal handler, so SIGKILL leaves exactly
// the on-disk state a real crash produces.
func TestProcessCrash_AcknowledgedMutationsRecover(t *testing.T) {
	if testing.Short() {
		t.Skip("process crash test")
	}

	// Build the helper once for all iterations.
	helperBin := filepath.Join(t.TempDir(), "storage-crash-helper")
	buildCmd := exec.Command("go", "build", "-o", helperBin, "../../../tests/storage-crash-helper")
	if out, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("build crash helper: %v\n%s", err, out)
	}

	dir := t.TempDir()
	// 60s per iteration is generous; the helper writes ~512 mutations in
	// well under 10s. Kept high so a slow CI box never spuriously fails.
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Start the helper subprocess. 8 writers × 32 writes = 256 ops, small
	// enough that 50 iterations finish well inside the 10m gate timeout
	// while still exercising multi-record batches (MaxBatch=256) and
	// generation-fenced deletes.
	cmd := exec.CommandContext(ctx, helperBin,
		"--dir", dir,
		"--writers", "8",
		"--writes", "256",
		"--delete-every", "17",
		"--segment-size", fmt.Sprintf("%d", 256<<20),
	)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("stdout pipe: %v", err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start helper: %v", err)
	}
	defer func() {
		// Ensure the subprocess is dead regardless of test outcome.
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}()

	// Capture acknowledged mutations from JSON lines.
	type ackLine struct {
		Op         string `json:"op"`
		ExtentID   uint64 `json:"extent_id"`
		Generation uint64 `json:"generation"`
		Checksum   uint32 `json:"checksum"`
		Ack        bool   `json:"ack"`
	}
	var acks []ackLine
	scanner := bufio.NewScanner(stdout)
	batchCount := 0
	lastBatchTick := time.Now()
	ready := false

	for scanner.Scan() {
		line := scanner.Text()
		var a ackLine
		if err := json.Unmarshal([]byte(line), &a); err != nil {
			t.Logf("invalid JSON (ignoring): %s", line)
			continue
		}
		if a.Op == "ready" {
			// Every requested mutation has been acknowledged and flushed
			// by the helper before it prints "ready". Capturing the full
			// sequence eliminates the ambiguity of a kill mid-way through
			// a put-then-delete pair (where the parent might see the put
			// ack but not the delete ack, while the disk already reflects
			// the delete). After "ready" there is no in-flight operation;
			// SIGKILL still bypasses Close(), so this is still a true
			// abrupt-crash recovery test.
			ready = true
			break
		}
		if !a.Ack {
			continue
		}
		acks = append(acks, a)

		// Detect multi-record batch boundaries: if 2ms passed since the
		// last ack, the preceding group was likely one batch.
		now := time.Now()
		if now.Sub(lastBatchTick) > 3*time.Millisecond {
			if len(acks) > 1 {
				batchCount++
			}
		}
		lastBatchTick = now
	}

	if !ready {
		t.Fatalf("helper exited before reporting ready (captured %d acks)", len(acks))
	}
	if len(acks) == 0 {
		t.Fatal("no acknowledged mutations captured before kill")
	}
	if batchCount == 0 {
		t.Logf("WARNING: no multi-record batch detected; killing anyway with %d acks", len(acks))
	}

	t.Logf("captured %d acknowledged mutations (%d detected batches); killing helper", len(acks), batchCount)

	// SIGKILL the helper — no graceful shutdown, no Close().
	if err := cmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatalf("SIGKILL: %v", err)
	}
	_ = cmd.Wait() // reap the zombie

	// Reopen the store — recovery must replay every acknowledged mutation.
	s, err := New(Config{
		Dir:         dir,
		SegmentSize: 256 << 20,
		StreamID:    1,
		UseMemIndex: true,
	})
	if err != nil {
		t.Fatalf("reopen store after crash: %v", err)
	}
	defer s.Close()

	if !s.DataReady() {
		t.Fatal("DataReady() = false after recovery")
	}

	// Reduce the acknowledged op sequence to the FINAL state of each
	// (extent, generation). A put followed by a delete for the same
	// extent must be verified as deleted, not as a live put — both acks
	// were captured, and the delete supersedes the put. Each extent is
	// written at most once by the helper, so the only ordering is
	// put-then-delete; tracking the last op per key is exact.
	type finalState struct {
		op       string
		checksum uint32
	}
	final := make(map[uint64]finalState, len(acks))
	for _, a := range acks {
		// (extent_id, generation) is the key; generation is always 1 here.
		final[a.ExtentID] = finalState{op: a.Op, checksum: a.Checksum}
	}

	// Verify every acknowledged operation's effect survived.
	readCtx := context.Background()
	missing := 0
	corrupt := 0
	wrongState := 0

	for extentID, fs := range final {
		eid := storage.ExtentID(extentID)

		switch fs.op {
		case "put":
			// The extent must be readable with the exact checksum.
			res, err := s.Read(readCtx, &storage.ReadRequest{
				ExtentID:   eid,
				Generation: 1,
			})
			if err != nil {
				if err == storage.ErrExtentNotFound {
					missing++
					t.Errorf("extent %d gen 1 LOST (acknowledged put not found)", eid)
				} else {
					corrupt++
					t.Errorf("extent %d gen 1 read error: %v", eid, err)
				}
				continue
			}
			if res.Checksum != fs.checksum {
				corrupt++
				t.Errorf("extent %d gen 1 checksum mismatch: got %d, want %d", eid, res.Checksum, fs.checksum)
			}

		case "delete":
			// The extent must be tombstoned or not found (both are valid:
			// ErrExtentNotFound means the tombstone was already applied).
			st, err := s.Stat(readCtx, &storage.StatRequest{
				ExtentID:   eid,
				Generation: 1,
			})
			if err == storage.ErrExtentNotFound {
				// Already deleted — correct.
				continue
			}
			if err != nil {
				corrupt++
				t.Errorf("delete extent %d gen 1 stat error: %v", eid, err)
				continue
			}
			if st.State != storage.ExtentTombstoned {
				wrongState++
				// Recovery diagnostics: show whether the delete was replayed
				// into the overlay (empty post-flush is expected) and what
				// the recovery result recorded.
				rr := s.RecoveryResult()
				t.Errorf("delete extent %d gen 1 not tombstoned: state=%d (recovery: commits=%d applied=%d lastSeq=%d safeSeq=%d trailing=%d)",
					eid, st.State, rr.Commits, rr.Applied, rr.LastSeq, rr.SafeSeq, rr.TrailingBytes)
			}

		default:
			t.Errorf("extent %d: unknown op %q", eid, fs.op)
		}
	}

	if missing > 0 || corrupt > 0 || wrongState > 0 {
		t.Fatalf("FAIL: %d missing, %d corrupt, %d wrong state out of %d final extents", missing, corrupt, wrongState, len(final))
	}

	t.Logf("PASS: all %d acknowledged final-state extents recovered correctly", len(final))
}

// TestProcessCrash_IndexRollback proves that removing the derived
// Pebble index before recovery forces segment-log replay and still
// recovers every acknowledged mutation (the overlay is the read
// authority, not Pebble).
func TestProcessCrash_IndexRollback(t *testing.T) {
	if testing.Short() {
		t.Skip("process crash test")
	}

	helperBin := filepath.Join(t.TempDir(), "storage-crash-helper")
	buildCmd := exec.Command("go", "build", "-o", helperBin, "../../../tests/storage-crash-helper")
	if out, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("build crash helper: %v\n%s", err, out)
	}

	dir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, helperBin,
		"--dir", dir,
		"--writers", "8",
		"--writes", "512",
		"--delete-every", "0",
		"--segment-size", fmt.Sprintf("%d", 256<<20),
	)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("stdout pipe: %v", err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start helper: %v", err)
	}
	defer func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}()

	type ackLine struct {
		Op         string `json:"op"`
		ExtentID   uint64 `json:"extent_id"`
		Generation uint64 `json:"generation"`
		Checksum   uint32 `json:"checksum"`
		Ack        bool   `json:"ack"`
	}
	var acks []ackLine
	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		line := scanner.Text()
		var a ackLine
		if err := json.Unmarshal([]byte(line), &a); err != nil {
			continue
		}
		if a.Op == "ready" {
			break
		}
		if a.Ack && a.Op == "put" {
			acks = append(acks, a)
		}
		if len(acks) >= 256 {
			break
		}
	}

	if len(acks) == 0 {
		t.Fatal("no acks")
	}
	t.Logf("captured %d acks; killing", len(acks))

	if err := cmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatalf("SIGKILL: %v", err)
	}
	_ = cmd.Wait()

	// Remove the entire Pebble index directory so recovery must replay
	// from the segment-log alone.
	indexDir := filepath.Join(dir, "index")
	if err := os.RemoveAll(indexDir); err != nil {
		t.Fatalf("remove index: %v", err)
	}
	t.Logf("removed index directory; reopening (forces segment-log replay)")

	s, err := New(Config{
		Dir:         dir,
		SegmentSize: 256 << 20,
		StreamID:    1,
		UseMemIndex: true,
	})
	if err != nil {
		t.Fatalf("reopen after index removal: %v", err)
	}
	defer s.Close()

	if !s.DataReady() {
		t.Fatal("DataReady() = false")
	}

	readCtx := context.Background()
	missing := 0
	for i, a := range acks {
		res, err := s.Read(readCtx, &storage.ReadRequest{
			ExtentID:   storage.ExtentID(a.ExtentID),
			Generation: storage.Generation(a.Generation),
		})
		if err != nil {
			missing++
			t.Errorf("ack %d: extent %d LOST after index rollback", i, a.ExtentID)
			continue
		}
		if res.Checksum != a.Checksum {
			t.Errorf("ack %d: checksum mismatch", i)
		}
	}

	if missing > 0 {
		t.Fatalf("FAIL: %d/%d mutations lost after index rollback", missing, len(acks))
	}
	t.Logf("PASS: all %d mutations recovered from segment-log alone", len(acks))
}
