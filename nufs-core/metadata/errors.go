package metadata

import (
	"errors"
	"fmt"
)

// ============================================================
// Error Types — Production-grade error handling
// ============================================================

// ErrorCode classifies the error for programmatic handling.
type ErrorCode string

const (
	ErrCodeNamespace   ErrorCode = "NAMESPACE_ERROR"
	ErrCodeBucket      ErrorCode = "BUCKET_ERROR"
	ErrCodeChunk       ErrorCode = "CHUNK_ERROR"
	ErrCodeNode        ErrorCode = "NODE_ERROR"
	ErrCodeConsistency ErrorCode = "CONSISTENCY_ERROR"
	ErrCodeSystem      ErrorCode = "SYSTEM_ERROR"
)

// Error is a structured error with an error code for API responses.
type Error struct {
	Code    ErrorCode
	Message string
	Cause   error
}

func (e *Error) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("%s: %s: %v", e.Code, e.Message, e.Cause)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func (e *Error) Unwrap() error { return e.Cause }

// NewError creates a structured error.
func NewError(code ErrorCode, message string, cause error) *Error {
	return &Error{Code: code, Message: message, Cause: cause}
}

// Namespace errors
var (
	ErrBucketExists      = errors.New("metadata: bucket already exists")
	ErrBucketNotFound    = errors.New("metadata: bucket not found")
	ErrBucketNotEmpty    = errors.New("metadata: bucket is not empty")
	ErrEntryExists       = errors.New("metadata: directory entry already exists")
	ErrEntryNotFound     = errors.New("metadata: directory entry not found")
	ErrInodeNotFound     = errors.New("metadata: inode not found")
	ErrDirNotEmpty       = errors.New("metadata: directory is not empty")
	ErrNotDirectory      = errors.New("metadata: not a directory")
	ErrNotFile           = errors.New("metadata: not a regular file")
	ErrNotSymlink        = errors.New("metadata: not a symbolic link")
	ErrNameTooLong       = errors.New("metadata: name exceeds maximum length")
	ErrCrossBucketRename = errors.New("metadata: rename across buckets is not allowed")
	ErrXAttrNotFound     = errors.New("metadata: extended attribute not found")
	ErrExtentNotInline   = errors.New("metadata: extent is not inline")
	// ErrExtentNotFound is returned when an extent's metadata row
	// (/extent-meta/{id}) does not exist.
	ErrExtentNotFound = errors.New("metadata: extent not found")
	// ErrInodeModelMismatch is returned when a V1 write path (UpdateInode)
	// is applied to an inode that carries a V2 layout (LayoutInlineExtent /
	// LayoutExtentPages). Both models share the same /inode/{id} row, and a
	// V1 overwrite would silently wipe the V2 layout fields — refuse loudly
	// instead (roadmap stage 1 / V2.1 inode wiring).
	ErrInodeModelMismatch = errors.New("metadata: inode has a V2 layout; V1 update refused")
	ErrDirTooLarge        = errors.New("metadata: directory exceeds maximum entries limit")
	ErrAccessDenied       = errors.New("metadata: access denied")
	ErrQuotaExceeded      = errors.New("metadata: bucket quota exceeded")
)

// Internal sentinel errors
var (
	errStopIteration = errors.New("metadata: stop iteration")
)

// Chunk errors
var (
	ErrChunkNotFound      = errors.New("metadata: chunk not found")
	ErrChunkNotSealed     = errors.New("metadata: chunk is not sealed")
	ErrChunkAlreadySealed = errors.New("metadata: chunk is already sealed")
	ErrChunkChecksum      = errors.New("metadata: chunk checksum mismatch")
)

// Node & Cluster errors
var (
	ErrNodeNotFound      = errors.New("metadata: node not found")
	ErrNodeAlreadyExists = errors.New("metadata: node already registered")
	ErrNodeOffline       = errors.New("metadata: node is offline")
	ErrInsufficientNodes = errors.New("metadata: insufficient healthy nodes for placement")
	ErrPlacementFailed   = errors.New("metadata: failed to satisfy placement constraints")
	ErrNodeDraining      = errors.New("metadata: node is being decommissioned")
	// ErrTooManyRequests is returned when a node management operation
	// (register / heartbeat) exceeds the configured rate limit.
	// Callers should wait RetryAfter seconds and retry.
	ErrTooManyRequests = errors.New("metadata: request rate exceeded")
)

// System errors
var (
	ErrServiceClosed   = errors.New("metadata: service is closed")
	ErrInvalidArgument = errors.New("metadata: invalid argument")
	ErrInternalError   = errors.New("metadata: internal error")
	ErrVersionConflict = errors.New("metadata: MVCC version conflict")
	ErrLeaseExpired    = errors.New("metadata: node lease expired")
	ErrScrubCorrupted  = errors.New("metadata: chunk data corrupted")
	ErrNotLeader       = errors.New("metadata: not the Raft leader")
	ErrRaftApply       = errors.New("metadata: Raft apply failed")
	ErrTimeout         = errors.New("metadata: operation timed out")
)

// Advisory lock errors
var (
	// ErrLockBusy is returned by AdvisoryLock / AdvisoryLockShared
	// when another holder already owns an incompatible lock on the
	// inode. Callers should treat this as a transient condition and
	// retry (with backoff) or surface it to the user as a permission
	// error. The same model as POSIX EAGAIN from flock(2).
	ErrLockBusy = errors.New("metadata: lock is held by another owner")
	// ErrInvalidOwner is returned when the owner string is empty or
	// malformed. An empty owner would make Unlock a no-op for
	// everybody, so we reject it up front.
	ErrInvalidOwner = errors.New("metadata: lock owner must be non-empty")
)
