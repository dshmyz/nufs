package metadata

import (
	"context"
	"testing"
	"time"
)

// This file is Program 6 Phase F4: tests for stripe-orphan discovery (§14).
// A failed conversion (RollbackConversion) leaves partial shards that no live
// chunk references; ListOrphanStripes and IsChunkShardsOrphaned gate whether
// those shards are reclaimable, deferring on age and never touching Completed
// stripes.

func TestECListStripes_AndOrphanFilter(t *testing.T) {
	store := newV2TestPebbleStore(t)
	ec := NewECStore(store)

	st, _ := ec.BeginConversion("stripe-o1", 100, 1, 0x1)
	if err := ec.RollbackConversion(st, "failed"); err != nil {
		t.Fatal(err)
	}

	// A freshly-rolled-back stripe is not yet an orphan (age gate).
	orphans, err := ec.ListOrphanStripes(24 * time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if len(orphans) != 0 {
		t.Fatalf("fresh rollback should not be orphaned, got %d", len(orphans))
	}
	// Backdate the rollback so it is older than the gate.
	st.RolledBackAt = time.Now().Add(-48 * time.Hour).UnixNano()
	if err := ec.PutStripe(st); err != nil {
		t.Fatal(err)
	}
	orphans, err = ec.ListOrphanStripes(24 * time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if len(orphans) != 1 || orphans[0].StripeID != "stripe-o1" {
		t.Fatalf("wanted stripe-o1 orphan, got %+v", orphans)
	}
	// A completed stripe is never an orphan no matter its age.
	st2, _ := ec.BeginConversion("stripe-o2", 101, 1, 0x2)
	if err := ec.CompleteConversion(st2, time.Now().Add(-48*time.Hour)); err != nil {
		t.Fatal(err)
	}
	orphans, _ = ec.ListOrphanStripes(24 * time.Hour)
	if len(orphans) != 1 {
		t.Fatalf("completed stripe leaked into orphans: %+v", orphans)
	}
}

func TestECIsChunkShardsOrphaned(t *testing.T) {
	store := newV2TestPebbleStore(t)
	ec := NewECStore(store)
	ctx := context.Background()
	older := time.Hour

	// No stripe, no chunk → shards are leaked orphans.
	orph, err := ec.IsChunkShardsOrphaned(ctx, 900, older)
	if err != nil {
		t.Fatal(err)
	}
	if !orph {
		t.Fatal("chunk with no stripe and no chunk meta should be orphaned")
	}

	// Completed stripe → chunk served from its shards, not orphans.
	st, _ := ec.BeginConversion("stripe-c", 901, 1, 0x1)
	if err := ec.CompleteConversion(st, time.Now()); err != nil {
		t.Fatal(err)
	}
	orph, err = ec.IsChunkShardsOrphaned(ctx, 901, older)
	if err != nil {
		t.Fatal(err)
	}
	if orph {
		t.Fatal("complete stripe shards must not be orphaned")
	}

	// Rolled-back and aged → orphans.
	st2, _ := ec.BeginConversion("stripe-r", 902, 1, 0x2)
	if err := ec.RollbackConversion(st2, "failed"); err != nil {
		t.Fatal(err)
	}
	st2.RolledBackAt = time.Now().Add(-2 * time.Hour).UnixNano()
	if err := ec.PutStripe(st2); err != nil {
		t.Fatal(err)
	}
	orph, err = ec.IsChunkShardsOrphaned(ctx, 902, older)
	if err != nil {
		t.Fatal(err)
	}
	if !orph {
		t.Fatal("aged rolled-back stripe shards should be orphaned")
	}

	// Rolled-back but young → not yet reclaimable.
	st3, _ := ec.BeginConversion("stripe-y", 903, 1, 0x3)
	if err := ec.RollbackConversion(st3, "failed"); err != nil {
		t.Fatal(err)
	}
	orph, err = ec.IsChunkShardsOrphaned(ctx, 903, older)
	if err != nil {
		t.Fatal(err)
	}
	if orph {
		t.Fatal("young rolled-back stripe must not be orphaned")
	}

	// In-flight (Preparing) → not orphaned.
	st4, _ := ec.BeginConversion("stripe-p", 904, 1, 0x4)
	orph, err = ec.IsChunkShardsOrphaned(ctx, 904, older)
	if err != nil {
		t.Fatal(err)
	}
	if orph {
		t.Fatal("in-flight stripe must not be orphaned")
	}
	_ = st4
}
