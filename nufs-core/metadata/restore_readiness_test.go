package metadata

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/cockroachdb/pebble"
)

func TestRestoreReadinessReportsMissingDegradedAndProbeErrors(t *testing.T) {
	store := newBackupCatalogTestStore(t, t.TempDir())
	defer store.Close()
	ctx := context.Background()
	if err := store.PutRestorePendingMarker(ctx, &RestorePendingMarker{
		BackupID:        "backup-restore",
		SourceClusterID: "source-cluster",
		AppliedIndex:    42,
		RestoredAt:      testBackupTime(),
	}); err != nil {
		t.Fatal(err)
	}
	writeRestoreReadinessChunk(t, store, ChunkMeta{ID: 7, Size: 64, State: ChunkReady})
	writeRestoreReadinessChunk(t, store, ChunkMeta{ID: 8, Size: 64, State: ChunkReady})
	writeRestoreReadinessChunk(t, store, ChunkMeta{ID: 9, Size: 64, State: ChunkReady})
	probe := restoreReadinessProbe{
		reachable: map[ChunkID]int{7: 0, 8: 1},
		errors:    map[ChunkID]error{9: errors.New("probe timed out")},
	}

	report, err := VerifyRestoredChunkAvailability(ctx, store, probe, 2)
	if err == nil {
		t.Fatal("VerifyRestoredChunkAvailability accepted unavailable chunks")
	}
	if report == nil || report.TotalChunks != 3 || report.UnavailableChunks != 3 || report.Ready {
		t.Fatalf("readiness report = %+v", report)
	}
	want := []RestoreChunkAvailabilityIssue{
		{ChunkID: 7, RequiredMinimum: 2, Reachable: 0, Status: RestoreChunkMissing},
		{ChunkID: 8, RequiredMinimum: 2, Reachable: 1, Status: RestoreChunkDegraded},
		{ChunkID: 9, RequiredMinimum: 2, Reachable: 0, Status: RestoreChunkProbeError, Error: "probe timed out"},
	}
	if len(report.Issues) != len(want) {
		t.Fatalf("issues = %+v, want %+v", report.Issues, want)
	}
	for i := range want {
		if report.Issues[i] != want[i] {
			t.Fatalf("issue[%d] = %+v, want %+v", i, report.Issues[i], want[i])
		}
	}
	marker, err := store.GetRestorePendingMarker(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if marker == nil {
		t.Fatal("readiness verifier mutated restore pending marker")
	}
}

func TestRestoreReadinessLeavesMarkerWhenChunksAvailable(t *testing.T) {
	store := newBackupCatalogTestStore(t, t.TempDir())
	defer store.Close()
	ctx := context.Background()
	if err := store.PutRestorePendingMarker(ctx, &RestorePendingMarker{
		BackupID:        "backup-restore",
		SourceClusterID: "source-cluster",
		AppliedIndex:    42,
		RestoredAt:      testBackupTime(),
	}); err != nil {
		t.Fatal(err)
	}
	writeRestoreReadinessChunk(t, store, ChunkMeta{ID: 7, Size: 64, State: ChunkReady})
	probe := restoreReadinessProbe{reachable: map[ChunkID]int{7: 2}}

	report, err := VerifyRestoredChunkAvailability(ctx, store, probe, 2)
	if err != nil {
		t.Fatal(err)
	}
	if !report.Ready || report.TotalChunks != 1 || report.UnavailableChunks != 0 {
		t.Fatalf("readiness report = %+v", report)
	}
	marker, err := store.GetRestorePendingMarker(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if marker == nil || marker.BackupID != "backup-restore" {
		t.Fatalf("restore pending marker was mutated: %+v", marker)
	}
}

type restoreReadinessProbe struct {
	reachable map[ChunkID]int
	errors    map[ChunkID]error
	err       error
}

func (p restoreReadinessProbe) ReachableReplicas(_ context.Context, chunk *ChunkMeta) (int, error) {
	if p.err != nil {
		return 0, p.err
	}
	if chunk == nil {
		return 0, errors.New("nil chunk")
	}
	if err := p.errors[chunk.ID]; err != nil {
		return 0, err
	}
	return p.reachable[chunk.ID], nil
}

func writeRestoreReadinessChunk(t *testing.T, store *PebbleStore, chunk ChunkMeta) {
	t.Helper()
	data, err := marshalValue(&chunk, codecMsgpack)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.db.Set([]byte(prefixChunk+strconv.FormatUint(uint64(chunk.ID), 10)), data, pebble.Sync); err != nil {
		t.Fatal(err)
	}
}
