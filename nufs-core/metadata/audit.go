package metadata

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"
)

// ============================================================
// AuditLogger — Structured audit trail for all mutating operations
// ============================================================

// AuditAction enumerates the categories of auditable operations.
type AuditAction string

const (
	AuditCreateBucket   AuditAction = "create_bucket"
	AuditDeleteBucket   AuditAction = "delete_bucket"
	AuditCreateFile     AuditAction = "create_file"
	AuditUnlink         AuditAction = "unlink"
	AuditMkDir          AuditAction = "mkdir"
	AuditRmDir          AuditAction = "rmdir"
	AuditRename         AuditAction = "rename"
	AuditWriteChunk     AuditAction = "write_chunk"
	AuditDeleteChunk    AuditAction = "delete_chunk"
	AuditSealChunk      AuditAction = "seal_chunk"
	AuditRegisterNode   AuditAction = "register_node"
	AuditDecommission   AuditAction = "decommission_node"
	AuditTriggerRepair  AuditAction = "trigger_repair"
	AuditTriggerRebalance AuditAction = "trigger_rebalance"
	AuditMigrateReplica AuditAction = "migrate_replica"
	AuditSetPolicy      AuditAction = "set_policy"
	AuditSetQuota       AuditAction = "set_quota"
	AuditScrub          AuditAction = "scrub"
	AuditGC             AuditAction = "gc"
)

// AuditRecord is a single audit log entry.
type AuditRecord struct {
	ID        string      `json:"id"`
	Timestamp int64       `json:"ts"`           // Unix nanoseconds
	Action    AuditAction `json:"action"`
	Actor     string      `json:"actor"`        // Who performed the action (access key / node ID)
	Resource  string      `json:"resource"`     // What was acted upon (bucket / inode / chunk)
	Result    string      `json:"result"`       // "ok" or "error"
	Error     string      `json:"error,omitempty"`
	Details   any         `json:"details,omitempty"` // Arbitrary structured details
	RequestID string      `json:"request_id,omitempty"`
	ClientIP  string      `json:"client_ip,omitempty"`
}

// AuditLogger records audit events asynchronously. Records are buffered
// in a ring and flushed to the PebbleStore in batches.
type AuditLogger struct {
	mu     sync.Mutex
	buf    []AuditRecord
	bufIdx int
	bufCap int
	store  *PebbleStore
	logger *slog.Logger
	done   chan struct{}
	wg     sync.WaitGroup
}

// AuditConfig controls audit logger behaviour.
type AuditConfig struct {
	BufferSize   int           // Ring buffer capacity (default: 4096)
	FlushInterval time.Duration // How often to flush to Pebble (default: 5s)
}

func defaultAuditConfig() AuditConfig {
	return AuditConfig{
		BufferSize:    4096,
		FlushInterval: 5 * time.Second,
	}
}

// NewAuditLogger creates a new audit logger backed by the given PebbleStore.
func NewAuditLogger(store *PebbleStore, cfg AuditConfig) *AuditLogger {
	if cfg.BufferSize <= 0 {
		cfg.BufferSize = 4096
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 5 * time.Second
	}
	al := &AuditLogger{
		buf:    make([]AuditRecord, cfg.BufferSize),
		bufIdx: 0,
		bufCap: cfg.BufferSize,
		store:  store,
		logger: slog.Default().With("component", "audit"),
		done:   make(chan struct{}),
	}
	al.wg.Add(1)
	go al.flushLoop(cfg.FlushInterval)
	return al
}

// Record appends an audit record to the ring buffer.
// This method never blocks; if the buffer is full the oldest
// record is silently overwritten (the trade-off for zero-latency audit).
func (al *AuditLogger) Record(rec AuditRecord) {
	if rec.Timestamp == 0 {
		rec.Timestamp = time.Now().UnixNano()
	}
	al.mu.Lock()
	al.buf[al.bufIdx%al.bufCap] = rec
	al.bufIdx++
	al.mu.Unlock()
}

// Log is a convenience wrapper that builds and records an AuditRecord.
func (al *AuditLogger) Log(action AuditAction, actor, resource, result string, opts ...AuditOption) {
	rec := AuditRecord{
		Action:   action,
		Actor:    actor,
		Resource: resource,
		Result:   result,
	}
	for _, opt := range opts {
		opt(&rec)
	}
	al.Record(rec)
}

// AuditOption is a functional option for AuditRecord.
type AuditOption func(*AuditRecord)

// WithError sets the error field.
func WithError(err string) AuditOption {
	return func(r *AuditRecord) { r.Error = err }
}

// WithDetails sets the details field.
func WithDetails(details any) AuditOption {
	return func(r *AuditRecord) { r.Details = details }
}

// WithRequestID sets the request ID.
func WithRequestID(id string) AuditOption {
	return func(r *AuditRecord) { r.RequestID = id }
}

// WithClientIP sets the client IP.
func WithClientIP(ip string) AuditOption {
	return func(r *AuditRecord) { r.ClientIP = ip }
}

// Stop gracefully shuts down the audit logger, flushing any buffered records.
func (al *AuditLogger) Stop() {
	close(al.done)
	al.wg.Wait()
	al.flush()
}

func (al *AuditLogger) flushLoop(interval time.Duration) {
	defer al.wg.Done()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-al.done:
			return
		case <-ticker.C:
			al.flush()
		}
	}
}

func (al *AuditLogger) flush() {
	al.mu.Lock()
	// Snapshot the buffer
	n := al.bufIdx
	if n == 0 {
		al.mu.Unlock()
		return
	}
	start := 0
	if n > al.bufCap {
		start = n - al.bufCap
	}
	records := make([]AuditRecord, 0, n-start)
	for i := start; i < n; i++ {
		records = append(records, al.buf[i%al.bufCap])
	}
	al.bufIdx = 0
	al.mu.Unlock()

	if len(records) == 0 {
		return
	}

	// Write to Pebble via the Raft replication path (applyBatchViaRaft).
	// This ensures audit records are:
	//   1. Replicated to all Raft peers (cluster-wide durability)
	//   2. Persisted with pebble.Sync (crash-safe)
	// Previously flush() called db.NewBatch().Commit(nil) which bypassed
	// both replication and sync — audit data could be lost on crash or
	// exist only on a single node.
	raftOps := make([]BatchOp, 0, len(records))
	for i, rec := range records {
		// Generate a unique ID if not set, so records with the same
		// timestamp don't collide in the keyspace. We append a sequence
		// number to guarantee uniqueness within a single flush batch.
		if rec.ID == "" {
			rec.ID = formatAuditUniqueID(rec.Timestamp, i)
		}
		key := []byte(auditKey(rec.Timestamp, rec.ID))
		val, err := json.Marshal(rec)
		if err != nil {
			al.logger.Error("audit marshal failed", "error", err)
			continue
		}
		raftOps = append(raftOps, BatchOp{Key: key, Value: val})
	}
	if len(raftOps) == 0 {
		return
	}
	if err := al.store.applyBatchViaRaft(raftOps); err != nil {
		al.logger.Error("audit flush failed", "error", err, "records", len(raftOps))
	} else {
		al.logger.Debug("audit flushed", "records", len(raftOps))
	}
}

// QueryAudit retrieves audit records from Pebble within a time range.
// Returns records sorted by timestamp ascending.
func (al *AuditLogger) QueryAudit(ctx context.Context, startTs, endTs int64, limit int) ([]AuditRecord, error) {
	if limit <= 0 {
		limit = 1000
	}
	if limit > 10000 {
		limit = 10000
	}

	startKey := auditKey(startTs, "")
	endKey := auditKey(endTs, "")

	iter, err := al.store.db.NewIter(nil)
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var records []AuditRecord
	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		if string(key) >= endKey {
			break
		}
		if string(key) < startKey {
			continue
		}
		var rec AuditRecord
		if err := json.Unmarshal(iter.Value(), &rec); err != nil {
			continue
		}
		records = append(records, rec)
		if len(records) >= limit {
			break
		}
	}
	return records, nil
}

// auditKey builds a Pebble key for an audit record.
// Format: /audit/{timestamp_ns}/{id}
// The timestamp prefix enables efficient range scans.
func auditKey(ts int64, id string) string {
	if id == "" {
		return formatAuditKey(ts, "")
	}
	return formatAuditKey(ts, id)
}

func formatAuditKey(ts int64, id string) string {
	if id == "" {
		return prefixAudit + formatInt64(ts)
	}
	return prefixAudit + formatInt64(ts) + "/" + id
}

// formatAuditUniqueID generates a unique record ID from the timestamp
// and a sequence number. This prevents key collisions when multiple
// records share the same nanosecond timestamp (common when records are
// logged in a tight loop).
func formatAuditUniqueID(ts int64, seq int) string {
	return formatInt64(ts) + "-" + formatInt64(int64(seq))
}

func formatInt64(v int64) string {
	return formatUint64(uint64(v))
}

func formatUint64(v uint64) string {
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	if i == len(buf) {
		return "0"
	}
	return string(buf[i:])
}
