package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

// handleDatanodeOps proxies disk/GC/node lifecycle ops to a single datanode
// via the cluster client's DoDatanodeOps. The call is server-side (management
// network -> datanode ops port), so it works even when control-plane ports are
// not reachable from the operator browser.
//
// Path: /api/v1/clusters/{id}/datanode/{nodeId}/{opPath...}
//   GET  .../datanode/1/disks                      -> list disks
//   POST .../datanode/1/disks/adopt?dir=...        -> adopt disk
//   POST .../datanode/1/disks/verify|migrate|decommission|retire?dir=...
//   POST .../datanode/1/disks/drain
//   POST .../datanode/1/gc/scan
func (r *Router) handleDatanodeOps(w http.ResponseWriter, req *http.Request, clusterID string, subpath []string) {
	if len(subpath) < 2 {
		http.Error(w, "invalid path: /clusters/{id}/datanode/{nodeId}/{opPath...}", http.StatusBadRequest)
		return
	}
	nodeID := subpath[0]
	opPath := strings.Join(subpath[1:], "/")

	client, ok := r.registry.GetClient(clusterID)
	if !ok {
		http.Error(w, "cluster not found", http.StatusNotFound)
		return
	}

	// Resolve the node's chunk addr (host:port) from metad; the ops port is
	// the cluster's configured datanode_ops_port.
	var nodes []map[string]interface{}
	if err := r.proxy.Get(req.Context(), clusterID, "/api/v1/nodes", &nodes); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	var nodeAddr string
	for _, n := range nodes {
		var id string
		switch v := n["id"].(type) {
		case string:
			id = v
		case float64:
			id = fmt.Sprintf("%.0f", v)
		}
		if id == nodeID {
			nodeAddr, _ = n["addr"].(string)
			break
		}
	}
	if nodeAddr == "" {
		http.Error(w, "node not found: "+nodeID, http.StatusNotFound)
		return
	}

	var result json.RawMessage
	if err := client.DoDatanodeOps(req.Context(), nodeAddr, opPath, req.Method, req.Body, &result); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if len(result) > 0 {
		_, _ = w.Write(result)
	} else {
		w.WriteHeader(http.StatusOK)
	}
}
