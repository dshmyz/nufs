package metadata

import (
	"testing"
)

func TestCrossShardRename_Lifecycle(t *testing.T) {
	store := newV2TestPebbleStore(t)
	txns := NewCrossShardTxnStore(store)

	txn, err := txns.Begin(42, 1, 2, 100, "src.txt", 200, "dst.txt", 300)
	if err != nil {
		t.Fatal(err)
	}
	if txn.State != TxnPreparing || txn.Prepared {
		t.Fatalf("begin state: %+v", txn)
	}

	// Commit before prepare must fail.
	if _, err := txns.Commit(42); err == nil {
		t.Fatal("commit before prepare should fail")
	}

	// Prepare then commit.
	if _, err := txns.MarkPrepared(42); err != nil {
		t.Fatal(err)
	}
	if _, err := txns.Commit(42); err != nil {
		t.Fatal(err)
	}
	// Commit decision is durable and authoritative.
	got, err := txns.ResolveDecision(42)
	if err != nil {
		t.Fatal(err)
	}
	if !got.CommitDecision || got.State != TxnCommitted {
		t.Fatalf("committed decision: %+v", got)
	}
	if got.ChildInode != 300 {
		t.Fatalf("child inode = %d, want 300", got.ChildInode)
	}

	// Apply → idempotent terminal state.
	if _, err := txns.MarkApplied(42); err != nil {
		t.Fatal(err)
	}
	got2, _ := txns.ResolveDecision(42)
	if got2.State != TxnApplied {
		t.Fatalf("applied state: %+v", got2)
	}
}

func TestCrossShardRename_PrepareOnlyNotCommitted(t *testing.T) {
	store := newV2TestPebbleStore(t)
	txns := NewCrossShardTxnStore(store)

	if _, err := txns.Begin(7, 1, 2, 100, "a", 200, "b", 300); err != nil {
		t.Fatal(err)
	}
	if _, err := txns.MarkPrepared(7); err != nil {
		t.Fatal(err)
	}
	// A crash here (prepared but not committed) leaves the txn
	// Preparing; the recovery worker sees no durable decision and
	// abandons it — never rolls back independently (§11.6).
	got, err := txns.ResolveDecision(7)
	if err != nil {
		t.Fatal(err)
	}
	if got.CommitDecision {
		t.Fatal("no commit decision should exist before Commit")
	}
	if got.State != TxnPreparing {
		t.Fatalf("state = %s, want preparing", got.State)
	}
}
