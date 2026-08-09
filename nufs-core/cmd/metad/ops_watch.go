package main

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// --- Watch / SSE-style event stream ---

// watchEvent is the JSON object emitted to clients on every metadata change.
// Type is "set" or "delete"; Key is `inode:<id>`, `chunk:<id>`, `bucket:<name>`,
// or a server-internal prefix like `node:<id>` used by placement.
//
// Clients (gateway / datanode) subscribe with `?prefix=inode:` to only receive
// inode-related events, reducing network traffic.
type watchEvent struct {
	Type  string    `json:"type"`
	Key   string    `json:"key"`
	Time  time.Time `json:"time"`
	Value []byte    `json:"value,omitempty"`
}

// handleWatch streams metadata change events in newline-delimited JSON.
//
// Query parameters:
//
//	prefix – optional key prefix filter. Clients usually pass
//	         "prefix=inode:" or "prefix=chunk:" to reduce traffic.
//
//	timeout – idle timeout in seconds. If no event arrives for this long,
//	          the server closes the stream so clients can reconnect.
//	          0 disables the timeout. Default: 60.
//
// Output format (ndjson):
//
//	{"type":"set","key":"inode:42","time":"2025-06-19T10:00:00Z"}
//	{"type":"delete","key":"chunk:123","time":"2025-06-19T10:00:01Z"}
//
// Clients should reconnect on EOF; events are best-effort and the stream
// can be terminated at any time by either side.
func (h *opsHandlers) handleWatch(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	prefix := q.Get("prefix")

	// idle timeout – hard-close the stream if no events flow for this long.
	// Default 60s; 0 disables.
	idleTimeout := 60 * time.Second
	if s := q.Get("timeout"); s != "" {
		if secs, err := strconv.ParseInt(s, 10, 32); err == nil && secs >= 0 {
			if secs == 0 {
				idleTimeout = 0
			} else {
				idleTimeout = time.Duration(secs) * time.Second
			}
		}
	}

	// Subscribe to the EventBus via PebbleStore.Events().
	bus := h.store.Events()
	if bus == nil {
		// No EventBus available: server is not configured with watch
		// support (e.g. missing NewEventBus call in main).
		writeJSONError(w, http.StatusNotImplemented, "watch support is not enabled")
		return
	}

	watcher := bus.Watch(prefix)
	defer watcher.Close()

	// Set up SSE-style streaming headers.
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("Cache-Control", "no-cache, no-transform")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}

	ctx := r.Context()
	var timer *time.Timer
	var timerC <-chan time.Time
	if idleTimeout > 0 {
		timer = time.NewTimer(idleTimeout)
		defer timer.Stop()
		timerC = timer.C
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-timerC:
			// Send a keep-alive newline; resets the timer so a dead
			// TCP peer will be detected without waiting for OS-level
			// keepalives.
			if _, err := w.Write([]byte("\n")); err != nil {
				return
			}
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
			timer.Reset(idleTimeout)
		case ev, ok := <-watcher.Events():
			if !ok {
				return
			}
			if timer != nil {
				timer.Reset(idleTimeout)
			}
			we := watchEvent{
				Type:  "set",
				Key:   ev.Key,
				Time:  ev.Time,
				Value: ev.Value,
			}
			if ev.Type == metadata.EventDelete {
				we.Type = "delete"
			}
			if err := json.NewEncoder(w).Encode(we); err != nil {
				return
			}
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
		}
	}
}
