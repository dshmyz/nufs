package main

// ============================================================
// /api/v1/kv — read-only raw KV inspection for operators.
//
//   GET /api/v1/kv?get=<key>
//   GET /api/v1/kv?scan=<prefix>&limit=<N>&cursor=<key>
//
// This is a debug/ops tool, not a general-purpose data API:
//   - requires the Raft leader (consistent primary copy);
//   - sits outside the public path allowlist, so with --auth-token set it
//     is automatically bearer-protected;
//   - only reads — never writes or mutates;
//   - scan restricts results to the documented catalog prefixes (see
//     metadata.KVCatalogPrefixes) so operators cannot dump internal
//     system/raft state indiscriminately.
// ============================================================

import (
	"encoding/base64"
	"net/http"
	"strconv"
	"strings"

	"github.com/example/dfs/metadata"
)

func (h *opsHandlers) handleKV(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if !h.requireLeader(w, r) {
		return
	}

	q := r.URL.Query()
	if key := q.Get("get"); key != "" {
		h.kvGet(w, key)
		return
	}
	if prefix := q.Get("scan"); prefix != "" {
		h.kvScan(w, prefix, q.Get("limit"), q.Get("cursor"))
		return
	}
	writeJSONError(w, http.StatusBadRequest, "specify ?get=<key> or ?scan=<prefix>")
}

func (h *opsHandlers) kvGet(w http.ResponseWriter, key string) {
	found, value, err := h.store.KVGet(key)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	resp := map[string]interface{}{
		"key":        key,
		"found":      found,
		"value_b64":  base64.StdEncoding.EncodeToString(value),
		"value_size": len(value),
	}
	if found {
		resp["value"] = printableValue(value)
	}
	writeJSON(w, resp)
}

func (h *opsHandlers) kvScan(w http.ResponseWriter, prefix, limitStr, cursorStr string) {
	if !kvPrefixAllowed(prefix) {
		writeJSONError(w, http.StatusBadRequest,
			"scan prefix not allowed; allowed prefixes: "+strings.Join(metadata.KVCatalogPrefixes(), " "))
		return
	}
	limit := 100
	if limitStr != "" {
		n, err := strconv.Atoi(limitStr)
		if err != nil || n < 1 {
			writeJSONError(w, http.StatusBadRequest, "invalid limit")
			return
		}
		if n > 1000 {
			n = 1000
		}
		limit = n
	}
	var cursor []byte
	if cursorStr != "" {
		cursor = []byte(cursorStr)
	}
	page, err := h.store.KVScan(prefix, cursor, limit)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	entries := make([]map[string]interface{}, 0, len(page.Keys))
	for i, k := range page.Keys {
		entries = append(entries, map[string]interface{}{
			"key":        string(k),
			"value_b64":  base64.StdEncoding.EncodeToString(page.Values[i]),
			"value_size": len(page.Values[i]),
		})
	}
	resp := map[string]interface{}{
		"prefix":   prefix,
		"count":    len(entries),
		"has_more": page.HasMore,
		"entries":  entries,
	}
	if page.HasMore && len(page.NextKey) > 0 {
		resp["next_key"] = string(page.NextKey)
	}
	writeJSON(w, resp)
}

// kvPrefixAllowed reports whether a scan prefix is one of the documented
// catalog prefixes (exact-prefix match so `/inode/` is allowed but a raw
// prefix like `/` or `system/` is not).
func kvPrefixAllowed(prefix string) bool {
	for _, p := range metadata.KVCatalogPrefixes() {
		if prefix == p {
			return true
		}
	}
	return false
}

// printableValue renders a value for human display: UTF-8 text if it is
// printable, otherwise a compact base64 marker. Never returns binary that
// could garble a terminal.
func printableValue(b []byte) string {
	if !isPrintableUTF8(b) {
		return "<" + strconv.Itoa(len(b)) + " bytes: " + base64.StdEncoding.EncodeToString(b) + ">"
	}
	return string(b)
}

func isPrintableUTF8(b []byte) bool {
	if len(b) == 0 {
		return true
	}
	for _, c := range b {
		if c == '\n' || c == '\t' || c == '\r' {
			continue
		}
		if c < 0x20 || c >= 0x7f {
			return false
		}
	}
	return true
}
