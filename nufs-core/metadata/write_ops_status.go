package metadata

type WriteOpsStatus struct {
	Attempts     map[string]int64     `json:"attempts"`
	RecoveryTask BackgroundTaskStatus `json:"recovery_task"`
	GCTask       BackgroundTaskStatus `json:"gc_task"`
}

type BackgroundTaskStatus struct {
	ID           string              `json:"id"`
	Type         BackgroundTaskType  `json:"type"`
	State        BackgroundTaskState `json:"state"`
	Target       string              `json:"target"`
	AttemptCount int                 `json:"attempt_count"`
	LastError    string              `json:"last_error,omitempty"`
	UpdatedAt    int64               `json:"updated_at"`
}

func NewBackgroundTaskStatus(task *BackgroundTask) BackgroundTaskStatus {
	if task == nil {
		return BackgroundTaskStatus{}
	}
	return BackgroundTaskStatus{
		ID:           task.ID,
		Type:         task.Type,
		State:        task.State,
		Target:       task.Target,
		AttemptCount: task.AttemptCount,
		LastError:    task.LastError,
		UpdatedAt:    task.UpdatedAt,
	}
}
