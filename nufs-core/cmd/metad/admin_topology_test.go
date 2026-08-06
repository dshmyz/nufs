package main

import (
	"testing"

	"github.com/example/dfs/metadata"
)

func TestBuildTopologyGroupsByFaultDomain(t *testing.T) {
	nodes := []metadata.NodeInfo{
		{ID: 1, Rack: "rack-a", Zone: "us-east-1a", CapacityGB: 1000, UsedGB: 250, State: metadata.NodeOnline},
		{ID: 2, Rack: "rack-a", Zone: "us-east-1a", CapacityGB: 1000, UsedGB: 500, State: metadata.NodeDraining},
		{ID: 3, Rack: "rack-b", Zone: "us-east-1b", CapacityGB: 2000, UsedGB: 500, State: metadata.NodeOffline},
		{ID: 4, Addr: "10.0.0.4:9100"}, // no rack/zone -> unassigned
	}

	groups := buildTopology(nodes)
	if len(groups) != 3 {
		t.Fatalf("expected 3 fault domains, got %d: %+v", len(groups), groups)
	}

	byName := map[string]int{}
	for i, g := range groups {
		byName[g.Name] = i
	}
	if _, ok := byName["rack-a / us-east-1a"]; !ok {
		t.Fatalf("missing rack-a group: %+v", byName)
	}
	if _, ok := byName["rack-b / us-east-1b"]; !ok {
		t.Fatalf("missing rack-b group: %+v", byName)
	}
	if _, ok := byName["unassigned"]; !ok {
		t.Fatalf("missing unassigned group: %+v", byName)
	}

	// rack-a group has 2 nodes in order
	ga := groups[byName["rack-a / us-east-1a"]]
	if len(ga.Nodes) != 2 {
		t.Fatalf("rack-a expected 2 nodes, got %d", len(ga.Nodes))
	}
	if ga.Nodes[0].UsagePct != 25.0 {
		t.Errorf("node1 usage: got %v want 25.0", ga.Nodes[0].UsagePct)
	}
	if ga.Nodes[0].StateCls != "success" || ga.Nodes[0].StateName != "Online" {
		t.Errorf("node1 state: got %s/%s", ga.Nodes[0].StateCls, ga.Nodes[0].StateName)
	}
	if ga.Nodes[1].StateCls != "warning" || ga.Nodes[1].StateName != "Draining" {
		t.Errorf("node2 state: got %s/%s", ga.Nodes[1].StateCls, ga.Nodes[1].StateName)
	}

	// offline node -> secondary (gray; matches the shared stateClass used by
	// the nodes table and node_detail pages — only NodeFailed is danger/red)
	gb := groups[byName["rack-b / us-east-1b"]]
	if len(gb.Nodes) != 1 || gb.Nodes[0].StateCls != "secondary" || gb.Nodes[0].StateName != "Offline" {
		t.Errorf("offline node: got len=%d cls=%s name=%s", len(gb.Nodes), gb.Nodes[0].StateCls, gb.Nodes[0].StateName)
	}

	// unassigned node has no capacity -> UsagePct stays 0, dashoffset 0
	gu := groups[byName["unassigned"]]
	if len(gu.Nodes) != 1 {
		t.Fatalf("unassigned expected 1 node")
	}
	if gu.Nodes[0].Capacity != 0 || gu.Nodes[0].UsagePct != 0 {
		t.Errorf("unassigned node capacity/pct: got %d/%v", gu.Nodes[0].Capacity, gu.Nodes[0].UsagePct)
	}
}

func TestBuildTopologyDashOffset(t *testing.T) {
	// 25% used on r=19 ring: dashoffset = 119.4 - 119.4*0.25 = 89.55
	nodes := []metadata.NodeInfo{
		{ID: 1, Rack: "rack-a", CapacityGB: 1000, UsedGB: 250, State: metadata.NodeOnline},
	}
	g := buildTopology(nodes)
	if len(g) != 1 || len(g[0].Nodes) != 1 {
		t.Fatalf("unexpected topology: %+v", g)
	}
	n := g[0].Nodes[0]
	if diff := n.DashOffset - 89.55; diff < -1e-6 || diff > 1e-6 {
		t.Errorf("dashoffset: got %v want 89.55", n.DashOffset)
	}

	// over-capacity is clamped to 100% (dashoffset 0)
	nodes[0].UsedGB = 5000
	g = buildTopology(nodes)
	if got := g[0].Nodes[0].DashOffset; got != 0 {
		t.Errorf("clamped dashoffset: got %v want 0", got)
	}
}