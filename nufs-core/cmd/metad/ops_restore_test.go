package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// TestOpsRestoreNode exercises the /api/v1/nodes/{id}/restore route end-to-end:
// an operator decommissions a node, then brings it back online through the
// control plane so placement picks it again — the reversible half of the
// sticky-decommission feature (commit 346c884). It drives the real HTTP route
// through the HTTPClient, matching the opsHandler registration.
func TestOpsRestoreNode(t *testing.T) {
	ctx := context.Background()
	store, err := metadata.NewPebbleStore(metadata.PebbleStoreConfig{
		Dir:         t.TempDir(),
		UseInMemory: true,
		NodeID:      1,
	})
	if err != nil {
		t.Fatalf("NewPebbleStore: %v", err)
	}
	defer store.Close()

	bundle, err := metadata.NewPebbleServiceBundle(
		store,
		metadata.WithLeaseTTL(0),
		metadata.WithGCInterval(0),
		metadata.WithScrubInterval(0),
	)
	if err != nil {
		t.Fatalf("NewPebbleServiceBundle: %v", err)
	}
	defer bundle.Close()

	mux := http.NewServeMux()
	registerOpsHandlers(mux, store, store, bundle, "")
	server := httptest.NewServer(mux)
	defer server.Close()

	client := metadata.NewHTTPClient(server.URL, 0)

	const id = metadata.NodeID(42)
	if err := store.RegisterNode(ctx, &metadata.NodeInfo{ID: id, Addr: "n42:9100", CapacityGB: 100}); err != nil {
		t.Fatalf("RegisterNode: %v", err)
	}
	if err := store.RegisterNode(ctx, &metadata.NodeInfo{ID: 43, Addr: "n43:9100", CapacityGB: 100}); err != nil {
		t.Fatalf("RegisterNode 43: %v", err)
	}

	// Decommission via the route → draining, excluded from placement.
	if err := client.DecommissionNode(ctx, id); err != nil {
		t.Fatalf("DecommissionNode via route: %v", err)
	}
	n, _ := store.GetNode(ctx, id)
	if n.State != metadata.NodeDraining {
		t.Fatalf("after decommission state=%v, want NodeDraining", n.State)
	}

	// Restore via the route → online again.
	if err := client.RestoreNode(ctx, id); err != nil {
		t.Fatalf("RestoreNode via route: %v", err)
	}
	n, _ = store.GetNode(ctx, id)
	if n.State != metadata.NodeOnline {
		t.Fatalf("after restore state=%v, want NodeOnline", n.State)
	}

	// Restoring an unknown node via the route is a 500 (HTTPClient surfaces it).
	if err := client.RestoreNode(ctx, metadata.NodeID(999)); err == nil {
		t.Fatalf("RestoreNode unknown id expected error, got nil")
	}
}
