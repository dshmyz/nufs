package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/example/dfs/metadata"
)

type backupOpsCoordinator interface {
	Trigger(context.Context) (*metadata.BackupRunResult, error)
	Status(context.Context) metadata.BackupCoordinatorStatus
}

type backupVerifierRepository interface {
	Fetch(context.Context, string, string) (*metadata.BackupManifest, error)
}

type backupOpsDependency struct {
	coordinator backupOpsCoordinator
	repository  backupVerifierRepository
}

type backupListResponse struct {
	Tasks   []metadata.BackupTask            `json:"tasks"`
	Catalog *metadata.BackupCatalogState     `json:"catalog,omitempty"`
	Status  metadata.BackupCoordinatorStatus `json:"status"`
}

type backupStatusResponse struct {
	Status  metadata.BackupCoordinatorStatus `json:"status"`
	Catalog *metadata.BackupCatalogState     `json:"catalog,omitempty"`
}

type backupRunResponse struct {
	Task     metadata.BackupTask      `json:"task"`
	Manifest *metadata.BackupManifest `json:"manifest,omitempty"`
}

type backupVerifyResponse struct {
	BackupID string                   `json:"backup_id"`
	Verified bool                     `json:"verified"`
	Manifest *metadata.BackupManifest `json:"manifest,omitempty"`
}

type backupPruneDryRunResponse struct {
	DryRun             bool                             `json:"dry_run"`
	Retention          int                              `json:"retention"`
	CommittedBackups   int                              `json:"committed_backups"`
	DeletionCandidates []metadata.CommittedBackup       `json:"deletion_candidates"`
	Status             metadata.BackupCoordinatorStatus `json:"status"`
}

type backupInProgressResponse struct {
	Error string               `json:"error"`
	Code  string               `json:"code"`
	Task  *metadata.BackupTask `json:"task,omitempty"`
}

func (h *opsHandlers) handleBackups(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		h.handleBackupList(w, r)
	case http.MethodPost:
		if !h.requireLeader(w, r) {
			return
		}
		h.handleBackupCreate(w, r)
	default:
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (h *opsHandlers) handleBackupStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if !h.backupEnabled() {
		writeBackupError(w, http.StatusServiceUnavailable, "backup_disabled", "backup coordinator is not configured")
		return
	}
	catalog, err := h.store.GetBackupCatalogState(r.Context())
	if err != nil {
		writeBackupError(w, http.StatusInternalServerError, "backup_failed", err.Error())
		return
	}
	writeJSON(w, backupStatusResponse{
		Status:  h.backupCoordinator.Status(r.Context()),
		Catalog: catalog,
	})
}

func (h *opsHandlers) handleBackupList(w http.ResponseWriter, r *http.Request) {
	if !h.backupEnabled() {
		writeBackupError(w, http.StatusServiceUnavailable, "backup_disabled", "backup coordinator is not configured")
		return
	}
	limit := queryInt(r, "limit", 100)
	if limit <= 0 {
		limit = 100
	}
	tasks, err := h.store.ListBackupTasks(r.Context(), limit)
	if err != nil {
		writeBackupError(w, http.StatusInternalServerError, "backup_failed", err.Error())
		return
	}
	catalog, err := h.store.GetBackupCatalogState(r.Context())
	if err != nil {
		writeBackupError(w, http.StatusInternalServerError, "backup_failed", err.Error())
		return
	}
	writeJSON(w, backupListResponse{
		Tasks:   tasks,
		Catalog: catalog,
		Status:  h.backupCoordinator.Status(r.Context()),
	})
}

func (h *opsHandlers) handleBackupCreate(w http.ResponseWriter, r *http.Request) {
	if !h.backupEnabled() {
		writeBackupError(w, http.StatusServiceUnavailable, "backup_disabled", "backup coordinator is not configured")
		return
	}
	if status := h.backupCoordinator.Status(r.Context()); status.Active {
		active, err := h.activeBackupTask(r.Context())
		if err != nil {
			writeBackupError(w, http.StatusInternalServerError, "backup_failed", err.Error())
			return
		}
		writeBackupInProgress(w, active)
		return
	}
	result, err := h.backupCoordinator.Trigger(r.Context())
	if err != nil {
		writeBackupError(w, backupHTTPStatus(err), "backup_failed", err.Error())
		return
	}
	if result == nil {
		writeBackupError(w, http.StatusInternalServerError, "backup_failed", "backup coordinator returned no result")
		return
	}
	writeJSON(w, backupRunResponse{Task: result.Task, Manifest: result.Manifest})
}

func (h *opsHandlers) handleBackupByID(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/backups/")
	parts := strings.SplitN(path, "/", 2)
	backupID, err := url.PathUnescape(parts[0])
	if err != nil || strings.TrimSpace(backupID) == "" {
		writeJSONError(w, http.StatusBadRequest, "invalid backup id")
		return
	}
	action := ""
	if len(parts) == 2 {
		action = parts[1]
	}
	if action != "verify" || r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	h.handleBackupVerify(w, r, backupID)
}

func (h *opsHandlers) handleBackupVerify(w http.ResponseWriter, r *http.Request, backupID string) {
	if !h.backupEnabled() || h.backupRepository == nil {
		writeBackupError(w, http.StatusServiceUnavailable, "backup_disabled", "backup coordinator is not configured")
		return
	}
	if status := h.backupCoordinator.Status(r.Context()); status.Active {
		writeBackupError(w, http.StatusConflict, "backup_in_progress", "backup is currently in progress")
		return
	}
	target, err := os.MkdirTemp("", "nufs-backup-verify-*")
	if err != nil {
		writeBackupError(w, http.StatusInternalServerError, "backup_failed", err.Error())
		return
	}
	defer os.RemoveAll(target)

	manifest, err := h.backupRepository.Fetch(r.Context(), backupID, filepath.Join(target, "artifact"))
	if err != nil {
		backupVerificationFailuresTotal.Add(1)
		writeBackupError(w, http.StatusInternalServerError, "backup_failed", err.Error())
		return
	}
	writeJSON(w, backupVerifyResponse{BackupID: backupID, Verified: true, Manifest: manifest})
}

func (h *opsHandlers) handleBackupPrune(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if !h.backupEnabled() {
		writeBackupError(w, http.StatusServiceUnavailable, "backup_disabled", "backup coordinator is not configured")
		return
	}
	if r.URL.Query().Get("dry_run") != "true" {
		writeBackupError(w, http.StatusBadRequest, "backup_invalid_request", "backup prune currently requires dry_run=true")
		return
	}
	status := h.backupCoordinator.Status(r.Context())
	if status.Retention < 1 {
		writeBackupError(w, http.StatusInternalServerError, "backup_failed", "backup retention is not available")
		return
	}
	catalog, err := h.store.GetBackupCatalogState(r.Context())
	if err != nil {
		writeBackupError(w, http.StatusInternalServerError, "backup_failed", err.Error())
		return
	}
	var backups []metadata.CommittedBackup
	if catalog != nil {
		backups = append(backups, catalog.Backups...)
	}
	sort.Slice(backups, func(i, j int) bool {
		if backups[i].CreatedAt.Equal(backups[j].CreatedAt) {
			return backups[i].ID < backups[j].ID
		}
		return backups[i].CreatedAt.After(backups[j].CreatedAt)
	})
	var candidates []metadata.CommittedBackup
	if len(backups) > status.Retention {
		candidates = append(candidates, backups[status.Retention:]...)
	}
	writeJSON(w, backupPruneDryRunResponse{
		DryRun:             true,
		Retention:          status.Retention,
		CommittedBackups:   len(backups),
		DeletionCandidates: candidates,
		Status:             status,
	})
}

func (h *opsHandlers) backupEnabled() bool {
	return h != nil && h.backupCoordinator != nil
}

func (h *opsHandlers) activeBackupTask(ctx context.Context) (*metadata.BackupTask, error) {
	tasks, err := h.store.ListBackupTasks(ctx, 1000)
	if err != nil {
		return nil, err
	}
	var active *metadata.BackupTask
	for i := range tasks {
		task := tasks[i]
		switch task.State {
		case metadata.BackupTaskCreating, metadata.BackupTaskUploading, metadata.BackupTaskVerifying:
			if active == nil || task.StartedAt.After(active.StartedAt) {
				copy := task
				active = &copy
			}
		}
	}
	return active, nil
}

func writeBackupInProgress(w http.ResponseWriter, task *metadata.BackupTask) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusConflict)
	_ = json.NewEncoder(w).Encode(backupInProgressResponse{
		Error: "backup is currently in progress",
		Code:  "backup_in_progress",
		Task:  task,
	})
}

func writeBackupError(w http.ResponseWriter, status int, code, msg string) {
	writeJSONErrorC(w, status, code, msg)
}

func backupHTTPStatus(err error) int {
	switch {
	case errors.Is(err, metadata.ErrBackupCoordinatorStopped):
		return http.StatusServiceUnavailable
	default:
		return http.StatusInternalServerError
	}
}
