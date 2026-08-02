package metadata

import (
	"testing"
)

func TestLogicalPartition_RouteAndMigrate(t *testing.T) {
	store := newV2TestPebbleStore(t)
	catalog := NewLogicalPartitionCatalog(store)

	if _, err := catalog.Create(0x0001, 1); err != nil {
		t.Fatal(err)
	}
	primary, secondary, err := catalog.Route(0x0001)
	if err != nil {
		t.Fatal(err)
	}
	if primary != 1 || len(secondary) != 0 {
		t.Fatalf("route stable: primary=%d secondary=%v", primary, secondary)
	}

	// Begin migration to group 2.
	if _, err := catalog.BeginMigration(0x0001, 2); err != nil {
		t.Fatal(err)
	}
	primary, secondary, err = catalog.Route(0x0001)
	if err != nil {
		t.Fatal(err)
	}
	if primary != 2 || len(secondary) != 1 || secondary[0] != 1 {
		t.Fatalf("route migrating: primary=%d secondary=%v", primary, secondary)
	}
	// Complete: single group again.
	if err := catalog.CompleteMigration(0x0001); err != nil {
		t.Fatal(err)
	}
	primary, secondary, _ = catalog.Route(0x0001)
	if primary != 2 || len(secondary) != 0 {
		t.Fatalf("route after complete: primary=%d secondary=%v", primary, secondary)
	}
}

func TestLogicalPartition_RouteExtent(t *testing.T) {
	store := newV2TestPebbleStore(t)
	catalog := NewLogicalPartitionCatalog(store)
	if _, err := catalog.Create(0x1234, 7); err != nil {
		t.Fatal(err)
	}
	id := EncodeExtentIDV2(0x1234, 0xABCDEF)
	group, err := catalog.RouteExtent(id)
	if err != nil {
		t.Fatal(err)
	}
	if group != 7 {
		t.Fatalf("extent routed to group %d, want 7", group)
	}
}

func TestDirectoryPartition_SplitAndLookup(t *testing.T) {
	store := newV2TestPebbleStore(t)
	dps := NewDirectoryPartitionStore(store)

	// Not partitioned: lookup returns colocated.
	_, _, partitioned, err := dps.Lookup(10, "foo")
	if err != nil || partitioned {
		t.Fatalf("unpartitioned lookup: partitioned=%v err=%v", partitioned, err)
	}

	// Split the whole namespace into two ranges at "m".
	dm, err := dps.SplitRange(10, 0, "m", []uint32{1, 2})
	if err != nil {
		t.Fatal(err)
	}
	if !dm.Partitioned || len(dm.Ranges) != 2 {
		t.Fatalf("split result: %+v", dm.Ranges)
	}
	// Lookup "a" → shard 1, "z" → shard 2.
	shardA, _, partA, _ := dps.Lookup(10, "a")
	if !partA || shardA != 1 {
		t.Fatalf("lookup a: shard=%d partitioned=%v", shardA, partA)
	}
	shardZ, _, _, _ := dps.Lookup(10, "z")
	if shardZ != 2 {
		t.Fatalf("lookup z: shard=%d want 2", shardZ)
	}
	// Version increments across splits.
	if dm.Version != 2 {
		t.Fatalf("version = %d, want 2", dm.Version)
	}
}

func TestShouldPartition_Thresholds(t *testing.T) {
	proactive, hard := ShouldPartition(500000, 0, 0, 0)
	if !proactive || hard {
		t.Fatalf("500K entries: proactive=%v hard=%v", proactive, hard)
	}
	proactive, hard = ShouldPartition(1000000, 0, 0, 0)
	if !proactive || !hard {
		t.Fatalf("1M entries: proactive=%v hard=%v", proactive, hard)
	}
	proactive, _ = ShouldPartition(100, 0, 0, 65)
	if !proactive {
		t.Fatal("65% CPU should trigger proactive")
	}
}
