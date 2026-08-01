package storage

import "context"

// Store is the foreground local interface (§15). All operations carry
// exact generations so generation fencing is possible at every layer.
//
// Implementations must honor the durability invariants:
//   - data is fdatasynced before the index mutation is WAL-fsynced
//   - a DurableReceipt is only returned after both barriers
//   - checksum/decrypt failures never return unverified bytes
type Store interface {
	// Write durably stores an extent at the given generation, returning
	// a receipt once both barriers pass. Repeating the same
	// (extent_id, generation, checksum) is idempotent; reusing the
	// generation with a different payload returns ErrStaleGeneration.
	Write(ctx context.Context, req *WriteRequest) (*DurableReceipt, error)

	// Read returns the payload of the extent at the exact generation.
	// If the generation is not the latest, a read of the latest is
	// requested by the caller (metadata tracks generations).
	Read(ctx context.Context, req *ReadRequest) (*ReadResult, error)

	// Delete generation-fenced deletes an extent. The tombstone is
	// durably recorded in the WAL before returning.
	Delete(ctx context.Context, req *DeleteRequest) error

	// Stat returns the location and state of an extent, or
	// ErrExtentNotFound.
	Stat(ctx context.Context, req *StatRequest) (*StatResult, error)
}

// WriteRequest is a durably-stored extent.
type WriteRequest struct {
	ExtentID   ExtentID
	Generation Generation
	Data       []byte
	// KeyID is the encryption key ID (0 = plaintext).
	KeyID uint64
}

// ReadRequest requests an extent's payload.
type ReadRequest struct {
	ExtentID   ExtentID
	Generation Generation
	// LogicalOffset/Length: 0/0 reads the whole logical payload.
	LogicalOffset int64
	Length        int32
}

// ReadResult is the verified payload (and its checksum).
type ReadResult struct {
	Data     []byte
	Checksum uint32
}

// DeleteRequest is a generation-fenced delete.
type DeleteRequest struct {
	ExtentID   ExtentID
	Generation Generation
}

// StatRequest requests extent location info.
type StatRequest struct {
	ExtentID   ExtentID
	Generation Generation
}

// StatResult reports where an extent lives and its state.
type StatResult struct {
	SegmentID  SegmentID
	Offset     int64
	StoredLen  uint32
	LogicalLen uint32
	State      ExtentState
	Checksum   uint32
}
