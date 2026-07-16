package main

import "testing"

func TestResolveMetadataNodeID(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want uint64
	}{
		{name: "numeric", raw: "7", want: 7},
		{name: "statefulset pod name", raw: "nufs-metad-0", want: 1},
		{name: "multi digit ordinal", raw: "nufs-metad-12", want: 13},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolveMetadataNodeID(tt.raw)
			if err != nil {
				t.Fatalf("resolveMetadataNodeID: %v", err)
			}
			if got != tt.want {
				t.Fatalf("expected %d, got %d", tt.want, got)
			}
		})
	}
}

func TestResolveMetadataNodeIDRejectsInvalidValues(t *testing.T) {
	for _, raw := range []string{"", "0", "nufs-metad-x"} {
		if _, err := resolveMetadataNodeID(raw); err == nil {
			t.Fatalf("expected %q to fail", raw)
		}
	}
}

func TestParseRaftBootstrapPeers(t *testing.T) {
	peers, err := parseRaftBootstrapPeers("meta-0=metad-0:7000, meta-1=metad-1:7000")
	if err != nil {
		t.Fatalf("parseRaftBootstrapPeers: %v", err)
	}
	if len(peers) != 2 {
		t.Fatalf("expected 2 peers, got %d", len(peers))
	}
	if peers[0].ID != "meta-0" || peers[0].Address != "metad-0:7000" {
		t.Fatalf("unexpected first peer: %+v", peers[0])
	}
	if peers[1].ID != "meta-1" || peers[1].Address != "metad-1:7000" {
		t.Fatalf("unexpected second peer: %+v", peers[1])
	}
}

func TestParseRaftPeerOpsURLs(t *testing.T) {
	peerOps, err := parseRaftPeerOpsURLs("meta-0=http://metad-0:8091,meta-1=http://metad-1:8091")
	if err != nil {
		t.Fatalf("parseRaftPeerOpsURLs: %v", err)
	}
	if peerOps["meta-0"] != "http://metad-0:8091" {
		t.Fatalf("unexpected meta-0 ops URL: %q", peerOps["meta-0"])
	}
	if peerOps["meta-1"] != "http://metad-1:8091" {
		t.Fatalf("unexpected meta-1 ops URL: %q", peerOps["meta-1"])
	}
}

func TestParseRaftPeerSpecsRejectsMalformedEntries(t *testing.T) {
	if _, err := parseRaftBootstrapPeers("meta-0"); err == nil {
		t.Fatal("expected malformed bootstrap peer to fail")
	}
	if _, err := parseRaftPeerOpsURLs("=http://metad-0:8091"); err == nil {
		t.Fatal("expected empty peer ID to fail")
	}
}
