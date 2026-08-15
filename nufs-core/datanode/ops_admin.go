package datanode

import (
	"embed"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/dshmyz/nufs/nufs-core/internal/version"
)

// Capacity thresholds for the V2.1 engine (and the webhook/admin view). They
// mirror DiskManager's warn/critical defaults so both engines alert at the
// same usage levels.
const (
	capacityWarnPct     = 0.75
	capacityCriticalPct = 0.85
)

// capacityAlertEvent is one recorded capacity-alert transition, pushed to the
// optional webhook and retained in the admin ring buffer.
type capacityAlertEvent struct {
	NodeID     uint64    `json:"node_id"`
	Level      string    `json:"level"`
	UsagePct   float64   `json:"usage_pct"`
	UsedBytes  int64     `json:"used_bytes"`
	TotalBytes int64     `json:"total_bytes"`
	Ts         time.Time `json:"ts"`
}

// capacityOverview is a unified used/total/usage view across engines. V1
// derives it from the DiskManager; V2.1 derives it by summing each disk's
// UsedBytes and detecting each disk dir's filesystem total via Statfs.
type capacityOverview struct {
	UsedBytes  int64   `json:"used_bytes"`
	OnDiskBytes int64  `json:"on_disk_bytes,omitempty"`
	TotalBytes int64   `json:"total_bytes"`
	UsagePct   float64 `json:"usage_pct"`
}

// embed the single-page admin UI.
//
//go:embed admin/index.html
var adminFS embed.FS

// ---- Instance-scoped alert + admin state on OpsServer ----

// registerAdminRoutes mounts the datanode admin web view and the capacity-alert
// JSON feed on the ops mux. The page is a single self-contained HTML asset (no
// server-side templating): it fetches the existing JSON endpoints below for
// Overview/Repair/Metrics and the /api/v1/admin/alerts feed for the alert
// history. It serves only this node's local view; cluster-level aggregation is
// the metad admin's job.
func (s *OpsServer) registerAdminRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/admin/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/admin/" && r.URL.Path != "/admin" {
			http.NotFound(w, r)
			return
		}
		body, err := adminFS.ReadFile("admin/index.html")
		if err != nil {
			http.Error(w, "admin asset missing", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(body)
	})
	mux.HandleFunc("/api/v1/admin/alerts", s.handleAdminAlerts)
}

// handleAdminAlerts returns the recent capacity-alert ring buffer as JSON.
func (s *OpsServer) handleAdminAlerts(w http.ResponseWriter, r *http.Request) {
	s.writeJSON(w, map[string]interface{}{
		"node_id":  uint64(s.cfg.NodeID),
		"alerts":   s.alertRingSnapshot(),
		"level":    s.currentAlertLevel().String(),
		"overview": s.capacityOverview(),
		"version":  version.Info(),
	})
}

// SetAlertWebhook configures an optional URL that receives capacity-alert
// events as a JSON POST (async, non-blocking). Empty disables delivery.
func (s *OpsServer) SetAlertWebhook(url string) {
	s.alertWebhook = strings.TrimSpace(url)
}

// NotifyCapacityAlert records a capacity-alert transition, retains it in the
// admin ring buffer, logs it, and (when a webhook is configured) POSTs the JSON
// payload asynchronously. It de-duplicates: only a level change fires.
func (s *OpsServer) NotifyCapacityAlert(level AlertLevel, usagePct float64, usedBytes, totalBytes int64) {
	prev := AlertLevel(s.lastAlertLevel.Load())
	if level == prev {
		return
	}
	s.lastAlertLevel.Store(int64(level))

	ev := capacityAlertEvent{
		NodeID:     uint64(s.cfg.NodeID),
		Level:      level.String(),
		UsagePct:   usagePct,
		UsedBytes:  usedBytes,
		TotalBytes: totalBytes,
		Ts:         time.Now().UTC(),
	}
	s.appendAlert(ev)

	if level == AlertNone {
		if prev != AlertNone {
			slog.Info("datanode: capacity alert cleared", "node_id", s.cfg.NodeID, "usage", pctStr(usagePct))
		}
		return
	}

	// Async, non-blocking delivery — never stall the alert caller on the
	// webhook network round-trip. Events go through a single serialized worker
	// so POST order matches notification order (see deliverWebhook).
	s.enqueueAlertWebhook(ev)
	if level == AlertCritical {
		slog.Error("datanode: capacity CRITICAL", "node_id", s.cfg.NodeID, "usage", pctStr(usagePct))
	} else {
		slog.Warn("datanode: capacity warning", "node_id", s.cfg.NodeID, "usage", pctStr(usagePct))
	}
}

func pctStr(pct float64) string {
	return fmt.Sprintf("%.1f%%", pct*100)
}

// enqueueAlertWebhook hands an alert event to the single serialized webhook
// worker. The worker is lazy-started on first use and reads events in FIFO
// order, so concurrent NotifyCapacityAlert calls cannot reorder deliveries.
func (s *OpsServer) enqueueAlertWebhook(ev capacityAlertEvent) {
	s.alertDispatchOne.Do(func() {
		s.alertDispatch = make(chan capacityAlertEvent, 64)
		go s.webhookWorker()
	})
	s.alertDispatch <- ev
}

// webhookWorker drains the alert dispatch queue in order, delivering each
// event (when a webhook URL is configured) asynchronously of the alert caller
// but serially with respect to other events.
func (s *OpsServer) webhookWorker() {
	for ev := range s.alertDispatch {
		url := s.alertWebhook
		if url != "" {
			s.deliverWebhook(url, ev)
		}
	}
}

func (s *OpsServer) deliverWebhook(url string, ev capacityAlertEvent) {
	body, err := json.Marshal(ev)
	if err != nil {
		slog.Error("datanode: encode alert webhook", "error", err)
		return
	}
	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(string(body)))
	if err != nil {
		slog.Error("datanode: alert webhook request", "error", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		slog.Warn("datanode: alert webhook delivery failed", "error", err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		slog.Warn("datanode: alert webhook non-2xx", "status", resp.StatusCode)
	}
}

func (s *OpsServer) appendAlert(ev capacityAlertEvent) {
	s.alertMu.Lock()
	defer s.alertMu.Unlock()
	const max = 200
	s.alertRing = append(s.alertRing, ev)
	if len(s.alertRing) > max {
		s.alertRing = s.alertRing[len(s.alertRing)-max:]
	}
}

func (s *OpsServer) alertRingSnapshot() []capacityAlertEvent {
	s.alertMu.RLock()
	defer s.alertMu.RUnlock()
	out := make([]capacityAlertEvent, len(s.alertRing))
	copy(out, s.alertRing)
	return out
}

// capacityOverview computes the aggregated used/total/usage across all disks
// by summing each disk's UsedBytes and detecting each disk dir's filesystem
// total via Statfs.
func (s *OpsServer) capacityOverview() capacityOverview {
	// V2.1 path: aggregate per-disk DiskInfo usage + Statfs totals. The
	// physical OnDiskBytes (compressed .seg footprint) is summed alongside so
	// an admin can show logical live vs physical on-disk side by side.
	var used, total, onDisk int64
	for _, d := range s.store.DiskInfos() {
		used += d.UsedBytes
		onDisk += d.OnDiskBytes
		if cap := detectCapacityBytes(d.Dir); cap > 0 {
			total += cap
		}
	}
	ov := capacityOverview{UsedBytes: used, OnDiskBytes: onDisk, TotalBytes: total}
	if total > 0 {
		ov.UsagePct = float64(used) / float64(total)
	}
	return ov
}

// capacityLevelForUsage maps a usage fraction to the capacity alert level,
// mirroring DiskManager's warn/critical thresholds. Pure so tests exercise the
// threshold logic without a real (never-near-full) filesystem.
func capacityLevelForUsage(usagePct float64) AlertLevel {
	switch {
	case usagePct >= capacityCriticalPct:
		return AlertCritical
	case usagePct >= capacityWarnPct:
		return AlertWarn
	default:
		return AlertNone
	}
}

// currentAlertLevel derives the alert level from the live capacity overview
// using usage thresholds. It does not mutate ring/webhook state.
func (s *OpsServer) currentAlertLevel() AlertLevel {
	return capacityLevelForUsage(s.capacityOverview().UsagePct)
}

// CheckAndNotifyCapacity is the engine-agnostic alert poll used by the V2.1
// path (which has no DiskManager callback). It reads the live overview and
// pushes any level transition through NotifyCapacityAlert (which de-dups).
func (s *OpsServer) CheckAndNotifyCapacity() {
	ov := s.capacityOverview()
	s.NotifyCapacityAlert(capacityLevelForUsage(ov.UsagePct), ov.UsagePct, ov.UsedBytes, ov.TotalBytes)
}
