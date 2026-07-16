// Package store provides MySQL-backed cluster storage.
package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// ClusterSource indicates whether a cluster is static (YAML) or dynamic (DB).
type ClusterSource string

const (
	SourceStatic  ClusterSource = "static"
	SourceDynamic ClusterSource = "dynamic"
)

// ClusterRecord represents a cluster row in the database.
type ClusterRecord struct {
	ID          string        `json:"id"`
	Region      string        `json:"region"`
	MetadOpsURL string        `json:"metad_ops_url"`
	Description string        `json:"description"`
	Source      ClusterSource `json:"source"`
	CreatedAt   time.Time     `json:"created_at"`
	UpdatedAt   time.Time     `json:"updated_at"`
}

// AuditAction represents a cluster audit log action.
type AuditAction string

const (
	AuditAdd     AuditAction = "add"
	AuditRemove  AuditAction = "remove"
	AuditUpdate  AuditAction = "update"
)

// AuditLogEntry represents a cluster audit log row.
type AuditLogEntry struct {
	ID        int64     `json:"id"`
	ClusterID string    `json:"cluster_id"`
	Action    AuditAction `json:"action"`
	Operator  string    `json:"operator"`
	Detail    string    `json:"detail"`
	CreatedAt time.Time `json:"created_at"`
}

// Store manages clusters in MySQL.
type Store struct {
	db *sql.DB
}

// New creates a store with database connection.
func New(db *sql.DB) *Store {
	return &Store{db: db}
}

// ListAll returns all clusters from database.
func (s *Store) ListAll(ctx context.Context) ([]ClusterRecord, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, region, metad_ops_url, description, source, created_at, updated_at
		 FROM clusters
		 ORDER BY source DESC, region, id`)
	if err != nil {
		return nil, fmt.Errorf("query clusters: %w", err)
	}
	defer rows.Close()

	var result []ClusterRecord
	for rows.Next() {
		var c ClusterRecord
		if err := rows.Scan(&c.ID, &c.Region, &c.MetadOpsURL, &c.Description,
			&c.Source, &c.CreatedAt, &c.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan cluster: %w", err)
		}
		result = append(result, c)
	}
	return result, rows.Err()
}

// Get returns a cluster by ID.
func (s *Store) Get(ctx context.Context, id string) (*ClusterRecord, error) {
	var c ClusterRecord
	err := s.db.QueryRowContext(ctx,
		`SELECT id, region, metad_ops_url, description, source, created_at, updated_at
		 FROM clusters WHERE id = ?`, id).
		Scan(&c.ID, &c.Region, &c.MetadOpsURL, &c.Description,
			&c.Source, &c.CreatedAt, &c.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get cluster %s: %w", id, err)
	}
	return &c, nil
}

// Add inserts a new dynamic cluster.
func (s *Store) Add(ctx context.Context, c ClusterRecord) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO clusters (id, region, metad_ops_url, description, source)
		 VALUES (?, ?, ?, ?, ?)`,
		c.ID, c.Region, c.MetadOpsURL, c.Description, SourceDynamic)
	if err != nil {
		return fmt.Errorf("insert cluster %s: %w", c.ID, err)
	}
	return nil
}

// Remove deletes a dynamic cluster. Static clusters cannot be removed.
func (s *Store) Remove(ctx context.Context, id string) error {
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM clusters WHERE id = ? AND source = ?`, id, SourceDynamic)
	if err != nil {
		return fmt.Errorf("delete cluster %s: %w", id, err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("cluster %s not found or is static", id)
	}
	return nil
}

// Update modifies a dynamic cluster.
func (s *Store) Update(ctx context.Context, c ClusterRecord) error {
	res, err := s.db.ExecContext(ctx,
		`UPDATE clusters SET region = ?, metad_ops_url = ?, description = ?
		 WHERE id = ? AND source = ?`,
		c.Region, c.MetadOpsURL, c.Description, c.ID, SourceDynamic)
	if err != nil {
		return fmt.Errorf("update cluster %s: %w", c.ID, err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("cluster %s not found or is static", c.ID)
	}
	return nil
}

// SyncStatic replaces all static clusters. Called on config reload.
func (s *Store) SyncStatic(ctx context.Context, static []ClusterRecord) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	// Delete all static clusters
	if _, err := tx.ExecContext(ctx, `DELETE FROM clusters WHERE source = ?`, SourceStatic); err != nil {
		return fmt.Errorf("delete static: %w", err)
	}

	// Insert new static clusters
	for _, c := range static {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO clusters (id, region, metad_ops_url, description, source)
			 VALUES (?, ?, ?, ?, ?)`,
			c.ID, c.Region, c.MetadOpsURL, c.Description, SourceStatic); err != nil {
			return fmt.Errorf("insert static %s: %w", c.ID, err)
		}
	}

	return tx.Commit()
}

// AddAuditLog records a cluster change.
func (s *Store) AddAuditLog(ctx context.Context, entry AuditLogEntry) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO cluster_audit_log (cluster_id, action, operator, detail)
		 VALUES (?, ?, ?, ?)`,
		entry.ClusterID, entry.Action, entry.Operator, entry.Detail)
	if err != nil {
		return fmt.Errorf("insert audit log: %w", err)
	}
	return nil
}

// ListAuditLogs returns recent audit log entries.
func (s *Store) ListAuditLogs(ctx context.Context, limit, offset int) ([]AuditLogEntry, error) {
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, cluster_id, action, operator, detail, created_at
		 FROM cluster_audit_log
		 ORDER BY created_at DESC
		 LIMIT ? OFFSET ?`, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("query audit logs: %w", err)
	}
	defer rows.Close()

	var result []AuditLogEntry
	for rows.Next() {
		var e AuditLogEntry
		if err := rows.Scan(&e.ID, &e.ClusterID, &e.Action, &e.Operator, &e.Detail, &e.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan audit log: %w", err)
		}
		result = append(result, e)
	}
	return result, rows.Err()
}