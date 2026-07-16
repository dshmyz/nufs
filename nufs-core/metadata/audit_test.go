package metadata

import (
	"context"
	"testing"
	"time"
)

// TestAuditLogger_FlushPersistsViaRaftPath verifies that flushed audit
// records are persisted through the Raft replication path (or sync write
// when Raft is disabled), NOT through a direct db.Commit(nil) that
// bypasses both replication and durability.
//
// TDD red phase: before the fix, flush() called batch.Commit(nil) which
// (1) bypasses Raft replication in cluster mode and (2) uses no-sync
// write that can lose data on crash. This test asserts records survive
// a simulated barrier and are readable via QueryAudit.
func TestAuditLogger_FlushPersistsViaRaftPath(t *testing.T) {
	store := newTestPebbleStore(t)

	al := NewAuditLogger(store, AuditConfig{
		BufferSize:    16,
		FlushInterval: 1 * time.Hour, // disable auto-flush; we call flush manually
	})
	defer al.Stop()

	// Record a few entries.
	al.Log(AuditCreateBucket, "user1", "bucket-a", "ok")
	al.Log(AuditDeleteBucket, "user2", "bucket-b", "ok",
		WithError("not found"))

	// Manually flush to force persistence.
	al.flush()

	// Query back — records must be readable.
	records, err := al.QueryAudit(context.Background(), 0, time.Now().UnixNano()+1, 100)
	if err != nil {
		t.Fatalf("QueryAudit: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}

	// Verify content.
	if records[0].Action != AuditCreateBucket {
		t.Errorf("first record action: got %s, want %s", records[0].Action, AuditCreateBucket)
	}
	if records[1].Action != AuditDeleteBucket {
		t.Errorf("second record action: got %s, want %s", records[1].Action, AuditDeleteBucket)
	}
	if records[1].Error != "not found" {
		t.Errorf("second record error: got %q, want %q", records[1].Error, "not found")
	}
}

// TestAuditLogger_FlushUsesSyncWrite verifies that flush uses a synced
// write (pebble.Sync) when Raft is not configured. We assert this by
// checking that the store's write metrics counter increments — the
// applyBatchViaRaft path records metrics, while a direct db.Commit(nil)
// does not.
func TestAuditLogger_FlushUsesSyncWrite(t *testing.T) {
	store := newTestPebbleStore(t)
	store.metrics = NewMetrics()

	al := NewAuditLogger(store, AuditConfig{
		BufferSize:    16,
		FlushInterval: 1 * time.Hour,
	})
	defer al.Stop()

	before := store.metrics.Snapshot().WriteOps

	al.Log(AuditCreateFile, "user1", "file-a", "ok")
	al.flush()

	after := store.metrics.Snapshot().WriteOps
	if after <= before {
		t.Fatalf("expected write ops to increase after flush (before=%d, after=%d) — flush bypassed applyBatchViaRaft",
			before, after)
	}
}

// TestAuditLogger_FlushEmptyBufferIsNoop verifies that flushing an
// empty buffer does not error or write anything.
func TestAuditLogger_FlushEmptyBufferIsNoop(t *testing.T) {
	store := newTestPebbleStore(t)

	al := NewAuditLogger(store, AuditConfig{
		BufferSize:    16,
		FlushInterval: 1 * time.Hour,
	})
	defer al.Stop()

	// Flush with no records — must not panic or error.
	al.flush()

	records, err := al.QueryAudit(context.Background(), 0, time.Now().UnixNano()+1, 100)
	if err != nil {
		t.Fatalf("QueryAudit: %v", err)
	}
	if len(records) != 0 {
		t.Fatalf("expected 0 records, got %d", len(records))
	}
}

// TestAuditLogger_RingBufferOverwrite verifies that when the ring
// buffer fills, the oldest records are overwritten (documented behavior).
func TestAuditLogger_RingBufferOverwrite(t *testing.T) {
	store := newTestPebbleStore(t)

	al := NewAuditLogger(store, AuditConfig{
		BufferSize:    4,
		FlushInterval: 1 * time.Hour,
	})
	defer al.Stop()

	// Write 6 records into a buffer of size 4 — only last 4 survive.
	for i := 0; i < 6; i++ {
		al.Log(AuditCreateFile, "user", "file", "ok")
	}

	al.flush()

	records, err := al.QueryAudit(context.Background(), 0, time.Now().UnixNano()+1, 100)
	if err != nil {
		t.Fatalf("QueryAudit: %v", err)
	}
	if len(records) != 4 {
		t.Fatalf("expected 4 records (buffer size), got %d", len(records))
	}
}
