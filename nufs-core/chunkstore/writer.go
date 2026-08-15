// Chunk dispatch pipeline shared by the S3 gateway and FUSE flush.
//
// Both consumers previously hand-rolled the same per-chunk lifecycle:
// allocate → WriteChunk → CommitChunk (EC-skip) → SealChunk → ChunkRef.
// ChunkWriter collapses that into two stages — AllocateRanges then
// WriteAllocated — so the SDK owns the chunk semantics once and the
// gateways become thin consumers.
package chunkstore

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"log/slog"

	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// ChunkLifecycle is the metadata surface the dispatch pipeline needs.
// It is deliberately narrow: only the three chunk-command operations.
// *metadata.PebbleStore and *metadata.HTTPClient both satisfy it.
type ChunkLifecycle interface {
	// AllocateChunksBatch reserves chunks for the given file offsets.
	// Chunk IDs are derived internally by the metadata service (one per
	// offset, in order). A batch must be ≤ metadata.MaxChunkAllocationBatch.
	AllocateChunksBatch(ctx context.Context, inodeID metadata.InodeID, offsets []int64, policy metadata.PlacementPolicy) ([]*metadata.ChunkMeta, error)
	// CommitChunk lifts a chunk from ChunkCreated to ChunkSealed, recording
	// its CRC32C checksum. Skipped for chunks written via the direct-EC path,
	// which lift themselves to ChunkReady inside WriteChunk.
	CommitChunk(ctx context.Context, chunkID metadata.ChunkID, checksum uint32) error
	// SealChunk advances a chunk to ChunkReady (idempotent on ChunkReady).
	SealChunk(ctx context.Context, chunkID metadata.ChunkID) error
}

var (
	_ ChunkLifecycle = (*metadata.PebbleStore)(nil)
	_ ChunkLifecycle = (*metadata.HTTPClient)(nil)
)

// OpExecutor wraps a single metadata or chunk operation. FUSE passes its
// ReliabilityWrapper method values (retry + circuit breaker), which have the
// exact signature func(op string, fn func() error) error; a nil OpExecutor is
// a passthrough (direct call), which is what the S3 gateway uses to keep its
// existing reliability path unchanged.
//
// Note the name: the plural-named Executor in buffered.go bundles the three
// operations (DoMeta/DoChunk/LockInode) a BufferedFile needs; this is the
// leaf function type used by ChunkWriter options.
type OpExecutor func(op string, fn func() error) error

// WriterOption configures a ChunkWriter.
type WriterOption func(*ChunkWriter)

// WithMetaAccessor overrides how the ChunkWriter resolves the metadata
// service, deferring resolution to each call. FUSE must use this: its
// metadata service is hot-swapped (SwapMetadata), so the accessor returns
// f.fs.Meta() instead of binding one instance at construction.
func WithMetaAccessor(f func() ChunkLifecycle) WriterOption {
	return func(w *ChunkWriter) { w.meta = f }
}

// WithDoMeta wraps every metadata command (allocate / commit / seal).
func WithDoMeta(e OpExecutor) WriterOption {
	return func(w *ChunkWriter) { w.doMeta = e }
}

// WithDoChunk wraps the WriteChunk call.
func WithDoChunk(e OpExecutor) WriterOption {
	return func(w *ChunkWriter) { w.doChunk = e }
}

// ChunkWriter encapsulates the chunk dispatch lifecycle. It is safe for
// concurrent use: it holds no per-call state.
type ChunkWriter struct {
	meta  func() ChunkLifecycle
	store ChunkStore
	// doMeta/doChunk wrap the metadata and chunk commands respectively.
	// nil = passthrough.
	doMeta  OpExecutor
	doChunk OpExecutor
}

// NewChunkWriter creates a dispatch pipeline over the given metadata
// lifecycle and chunk store. Unless overridden by WithMetaAccessor, the
// metadata service is bound once at construction.
func NewChunkWriter(meta ChunkLifecycle, store ChunkStore, opts ...WriterOption) *ChunkWriter {
	w := &ChunkWriter{
		meta:  func() ChunkLifecycle { return meta },
		store: store,
	}
	for _, o := range opts {
		o(w)
	}
	return w
}

// Stage errors distinguish where a multi-step chunk write failed, so the S3
// gateway can map them to HTTP statuses (write → 503, commit → 500) exactly
// as it did when it hand-rolled the lifecycle.
var (
	ErrChunkWriteFailed  = errors.New("chunkstore: write chunk failed")
	ErrChunkCommitFailed = errors.New("chunkstore: commit chunk failed")
)

// AllocateRanges reserves chunks for the given file offsets, sub-batching by
// metadata.MaxChunkAllocationBatch. AllocateChunksBatch derives a chunk ID
// per offset internally; the SDK never touches ID derivation.
//
// On error it returns the chunks allocated so far plus the error, so callers
// can compensate (S3) or leave them for the crash-recovery worker (FUSE).
// Offsets may be sparse (any in-file positions, not just chunk-aligned);
// callers typically pass chunk base offsets via ChunkBase.
func (w *ChunkWriter) AllocateRanges(ctx context.Context, inodeID metadata.InodeID, policy metadata.PlacementPolicy, offsets []int64) ([]*metadata.ChunkMeta, error) {
	if len(offsets) == 0 {
		return nil, nil
	}
	allocated := make([]*metadata.ChunkMeta, 0, len(offsets))
	for start := 0; start < len(offsets); {
		end := start + metadata.MaxChunkAllocationBatch
		if end > len(offsets) {
			end = len(offsets)
		}
		var batch []*metadata.ChunkMeta
		err := w.runMeta("allocate", func() error {
			var gerr error
			batch, gerr = w.meta().AllocateChunksBatch(ctx, inodeID, offsets[start:end], policy)
			return gerr
		})
		if err != nil {
			return allocated, err
		}
		allocated = append(allocated, batch...)
		start = end
	}
	return allocated, nil
}

// WriteAllocated writes one already-allocated chunk: WriteChunk, then
// CommitChunk (unless the chunk was written via direct-EC, which lifts it to
// ChunkReady inside WriteChunk and would make CommitChunk 500), then a
// non-fatal SealChunk. Returns the ChunkRef to merge into the inode's
// ChunkMap. Errors wrap ErrChunkWriteFailed / ErrChunkCommitFailed for
// stage classification.
func (w *ChunkWriter) WriteAllocated(ctx context.Context, chunk *metadata.ChunkMeta, offset int64, data []byte) (metadata.ChunkRef, error) {
	if err := w.runChunk("write", func() error {
		return w.store.WriteChunk(ctx, chunk, data)
	}); err != nil {
		return metadata.ChunkRef{}, fmt.Errorf("%w: %v", ErrChunkWriteFailed, err)
	}

	if !w.skipCommit(chunk) {
		checksum := crc32.ChecksumIEEE(data)
		if err := w.runMeta("commit", func() error {
			return w.meta().CommitChunk(ctx, chunk.ID, checksum)
		}); err != nil {
			return metadata.ChunkRef{}, fmt.Errorf("%w: %v", ErrChunkCommitFailed, err)
		}
	}

	if err := w.runMeta("seal", func() error {
		return w.meta().SealChunk(ctx, chunk.ID)
	}); err != nil {
		// Non-fatal: the chunk is already durable; seal is idempotent.
		slog.Warn("chunkstore: seal chunk failed", "chunk_id", chunk.ID, "error", err)
	}

	return metadata.ChunkRef{
		ID:      chunk.ID,
		Offset:  offset,
		Length:  int32(len(data)),
		Version: 1,
	}, nil
}

// skipCommit reports whether the chunk was already lifted to ChunkReady by a
// direct-EC write (WriteChunk → writeECShardDirect), in which case the generic
// CommitChunk — which requires ChunkCreated — would 500. Two conditions must
// both hold: the chunk actually carries an EC placement (writeECShardDirect
// only runs for ECGroup chunks), and the store is wired to the direct-EC
// authority. A non-EC chunk on an authority-wired store still needs commit.
func (w *ChunkWriter) skipCommit(chunk *metadata.ChunkMeta) bool {
	if chunk.ECGroup == nil {
		return false
	}
	ecCap, ok := w.store.(interface{ ECWriteEnabled() bool })
	return ok && ecCap.ECWriteEnabled()
}

// runMeta invokes fn through the metadata executor (or directly when nil).
func (w *ChunkWriter) runMeta(op string, fn func() error) error {
	if w.doMeta != nil {
		return w.doMeta(op, fn)
	}
	return fn()
}

// runChunk invokes fn through the chunk executor (or directly when nil).
func (w *ChunkWriter) runChunk(op string, fn func() error) error {
	if w.doChunk != nil {
		return w.doChunk(op, fn)
	}
	return fn()
}

// ChunkBase returns the base offset of the 64-MiB chunk that contains off
// (the single authoritative chunk-size constant is metadata.MaxChunkSize).
func ChunkBase(off int64) int64 {
	return off &^ (metadata.MaxChunkSize - 1)
}
