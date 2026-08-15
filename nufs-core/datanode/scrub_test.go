package datanode

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dshmyz/nufs/nufs-core/datanode/storage"
	"github.com/dshmyz/nufs/nufs-core/datanode/storage/journal"
	"github.com/dshmyz/nufs/nufs-core/datanode/storage/segment"
	"github.com/dshmyz/nufs/nufs-core/metadata"
)

func TestScrubWorker_CleanChunks(t *testing.T) {
	v, _ := newTestMultiStore(t, 1)
	// Write a few chunks so the scrub has something to scan.
	for i := metadata.ChunkID(1); i <= 5; i++ {
		if err := v.Write(i, []byte("scrub-clean-data-payload")); err != nil {
			t.Fatalf("write chunk %d: %v", i, err)
		}
	}

	sw := NewScrubWorker(v, WithScrubInterval(time.Hour))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sw.Start(ctx)

	// Wait for the initial immediate scan to complete.
	deadline := time.After(5 * time.Second)
	for {
		st := sw.Stats()
		if st.Scanned >= 5 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for scan: %+v", st)
		case <-time.After(50 * time.Millisecond):
		}
	}

	st := sw.Stats()
	if st.Corrupt != 0 {
		t.Errorf("expected 0 corrupt, got %d", st.Corrupt)
	}
	if st.Failed != 0 {
		t.Errorf("expected 0 failed, got %d", st.Failed)
	}
	sw.Stop()
}

func TestScrubWorker_StartStopIdempotent(t *testing.T) {
	v, _ := newTestMultiStore(t, 1)
	sw := NewScrubWorker(v, WithScrubInterval(time.Hour))
	ctx := context.Background()

	// Multiple starts should be safe.
	sw.Start(ctx)
	sw.Start(ctx)
	sw.Start(ctx)

	// Multiple stops should be safe.
	sw.Stop()
	sw.Stop()
}

func TestScrubWorker_FindsCorruption(t *testing.T) {
	// Build a V2Store with a known data dir so we can corrupt the file.
	dir := t.TempDir()
	store, err := segment.New(segment.Config{
		Dir:         dir,
		UseMemIndex: true,
		StreamID:    1,
	})
	if err != nil {
		t.Fatalf("segment.New: %v", err)
	}
	v := NewMultiV2Store([]storage.Store{store}, dir)

	chunkID := metadata.ChunkID(42)
	payload := []byte("scrub-corruption-test-payload-unique-bytes")
	if err := v.Write(chunkID, payload); err != nil {
		t.Fatalf("write: %v", err)
	}

	// Verify the chunk is clean before corruption.
	ok, _, err := v.VerifyChunkData(chunkID)
	if err != nil {
		t.Fatalf("pre-corruption verify: %v", err)
	}
	if !ok {
		t.Fatal("chunk already corrupt before corruption")
	}

	// Corrupt the data: flip a byte in the segment data file. The segment
	// store uses files named <id>.seg in subdirectories (data/active/).
	corrupted := false
	filepath.Walk(dir, func(fpath string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || filepath.Ext(fpath) != ".seg" {
			return nil
		}
		data, readErr := os.ReadFile(fpath)
		if readErr != nil || len(data) < 32 {
			return nil
		}
		// Flip a byte near the middle of the file (likely in a data record).
		mid := len(data) / 2
		data[mid] ^= 0xFF
		if writeErr := os.WriteFile(fpath, data, 0644); writeErr != nil {
			t.Fatalf("corrupt write: %v", writeErr)
		}
		corrupted = true
		return filepath.SkipAll
	})
	if !corrupted {
		t.Skip("no .seg file found to corrupt; segment format may differ")
	}

	// Open a change journal to capture scrub findings.
	jDir := t.TempDir()
	j, err := journal.OpenChangeJournal(journal.JournalOptions{
		Dir:      jDir,
		MaxBytes: 1 << 20,
	})
	if err != nil {
		t.Fatalf("open journal: %v", err)
	}
	defer j.Close()

	sw := NewScrubWorker(v, WithScrubInterval(time.Hour), WithScrubBatchSize(1))
	sw.SetChangeJournal(j)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sw.Start(ctx)

	// Wait for the initial scan to detect the corruption (or I/O error).
	deadline := time.After(10 * time.Second)
	for {
		st := sw.Stats()
		// The scrub may classify a corrupted segment read as "failed" (I/O
		// error from the frame-level CRC) or as "corrupt" (logical CRC
		// mismatch). Either way, the data integrity issue is detected.
		if st.Corrupt > 0 || st.Failed > 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("scrub did not detect corruption: %+v", st)
		case <-time.After(100 * time.Millisecond):
		}
	}

	// If the scrub found a logical CRC mismatch (not just an I/O error),
	// verify the journal received an EventScrubFinding.
	st := sw.Stats()
	if st.Corrupt > 0 {
		events, _ := j.Pending(100, 1<<20)
		found := false
		for _, ev := range events {
			if ev.Kind == journal.EventScrubFinding {
				found = true
				break
			}
		}
		if !found {
			t.Error("corrupt count > 0 but no EventScrubFinding in journal")
		}
	}

	sw.Stop()
}
