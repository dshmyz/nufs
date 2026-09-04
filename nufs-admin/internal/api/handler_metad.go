package api

import (
	"encoding/json"
	"net/http"
	"strings"
)

// handleMetadPassthrough proxies a metad ops endpoint that the console surfaces
// without per-resource decoding: backups, cluster balance, background tasks and
// the EC conversion queue. GET returns raw JSON; POST forwards the body (e.g.
// triggering a backup). Auth token is attached server-side by the proxy.
//
// Path: /api/v1/clusters/{id}/{resource}/{subpath...} -> GET/POST {metadPath}
func (r *Router) handleMetadPassthrough(w http.ResponseWriter, req *http.Request, clusterID, metadPath string, subpath []string) {
	path := metadPath
	if len(subpath) > 0 {
		path += "/" + strings.Join(subpath, "/")
	}

	var raw json.RawMessage
	var err error
	switch req.Method {
	case http.MethodGet:
		err = r.proxy.GetUncached(req.Context(), clusterID, path, &raw)
	case http.MethodPost:
		err = r.proxy.Post(req.Context(), clusterID, path, req.Body, &raw)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if len(raw) > 0 {
		_, _ = w.Write(raw)
	} else {
		w.WriteHeader(http.StatusOK)
	}
}
