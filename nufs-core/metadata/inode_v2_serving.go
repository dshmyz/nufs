package metadata

import (
	"context"
	"fmt"
)

// inode_v2_serving.go exposes the V2.1 extent-layout inode model
// (InodeStoreV2: LayoutEmpty → LayoutInlineExtent → LayoutExtentPages,
// roadmap stage 1 §1.3) on the same serving surface the gateway already
// uses for V1 InodeMeta, so both local (PebbleStore) and remote
// (HTTPClient/metad) modes can reach it.
//
// Model invariant (see ExtentInodeService in service.go): an inode row at
// /inode/{id} is written by exactly one model. These methods are the
// canonical writers for V2 layout; V1 UpdateInode refuses to clobber a
// V2-layout row (ErrInodeModelMismatch).

// Compile-time check: PebbleStore satisfies the V2 extent-inode surface.
var _ ExtentInodeService = (*PebbleStore)(nil)

// extentMetaKey formats the /extent-meta/{extent_id} key under which a
// V2 extent's ExtentMetaV2 (length, placement, storage class, EC stripe)
// is persisted. SetInlineExtent/AppendExtent write it; GetExtentMeta reads
// it, so page-mode extents (which hold only ExtentRefs) keep their metadata
// reachable.
func extentMetaKey(id ExtentIDV2) string {
	return fmt.Sprintf("%s%d", prefixExtentMeta, id)
}

// putExtentMeta durably records an extent's metadata row.
func (s *PebbleStore) putExtentMeta(ext *ExtentMetaV2) error {
	return s.putMsgpack(extentMetaKey(ext.ID), ext)
}

// ResolveExtents returns the file's flat V2 extent references (inline or
// extent pages). Empty / V1 (ChunkMap) inodes decode as LayoutEmpty and
// yield (nil, nil).
func (s *PebbleStore) ResolveExtents(ctx context.Context, id InodeID) ([]ExtentRef, error) {
	if s.closed.Load() {
		return nil, ErrServiceClosed
	}
	return NewInodeStoreV2(s).ResolveExtents(id)
}

// GetExtentMeta returns a V2 extent's metadata from /extent-meta/{id}.
func (s *PebbleStore) GetExtentMeta(ctx context.Context, extentID ExtentIDV2) (*ExtentMetaV2, error) {
	if s.closed.Load() {
		return nil, ErrServiceClosed
	}
	var m ExtentMetaV2
	exists, err := s.getValue(extentMetaKey(extentID), &m)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, ErrExtentNotFound
	}
	return &m, nil
}

// SetInlineExtent promotes an inode to a single inline extent (≤ 16 MiB).
// Persists the extent metadata first (so a subsequent PromoteToPages keeps
// it reachable), then the inode's inline layout. The V1 inode cache is
// invalidated so GetInode re-reads the updated scalars (e.g. Size).
func (s *PebbleStore) SetInlineExtent(ctx context.Context, id InodeID, extent *ExtentMetaV2, size int64) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}
	if extent == nil {
		return ErrInvalidArgument
	}
	// Not atomic with the inode row below (two Raft mutations); a failure
	// after this write leaves a harmless orphan /extent-meta row.
	if err := s.putExtentMeta(extent); err != nil {
		return err
	}
	if err := NewInodeStoreV2(s).SetInlineExtent(id, extent, size); err != nil {
		return err
	}
	s.inCache.del(id)
	s.publishEvent(Event{Type: EventSet, Key: fmt.Sprintf("inode:%d", id)})
	return nil
}

// PromoteToPages transitions an inline-extent inode to extent pages (COW).
func (s *PebbleStore) PromoteToPages(ctx context.Context, id InodeID) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}
	if err := NewInodeStoreV2(s).PromoteToPages(id); err != nil {
		return err
	}
	s.inCache.del(id)
	s.publishEvent(Event{Type: EventSet, Key: fmt.Sprintf("inode:%d", id)})
	return nil
}

// AppendExtent appends an extent reference to a pages-layout inode,
// persisting the extent metadata. Returns the new COW extent root.
func (s *PebbleStore) AppendExtent(ctx context.Context, id InodeID, extent *ExtentMetaV2, offset int64) (uint64, error) {
	if s.closed.Load() {
		return 0, ErrServiceClosed
	}
	if extent == nil {
		return 0, ErrInvalidArgument
	}
	if err := s.putExtentMeta(extent); err != nil {
		return 0, err
	}
	root, err := NewInodeStoreV2(s).AppendExtent(id, ExtentRef{ExtentID: extent.ID, LogicalOffset: offset}, extent.LogicalLen)
	if err != nil {
		return 0, err
	}
	s.inCache.del(id)
	s.publishEvent(Event{Type: EventSet, Key: fmt.Sprintf("inode:%d", id)})
	return root, nil
}

// ReplaceExtents rewrites the file's entire extent set as extent pages,
// replacing whatever model the row previously had (empty, inline, or
// earlier pages). This is the whole-set writer for gateway overwrites:
// unlike PromoteToPages it does NOT preserve an old inline extent, because
// the overwrite has already rewritten the old extent's data into the new
// chunk set. Persists each extent's /extent-meta row, then the inode's
// pages layout.
//
// Bucket byte usage IS accumulated here (see InodeStoreV2's
// putWithBucketStats): without it the bucket usage counter freezes once
// writes land V2 layout and byte-quota enforcement silently degrades.
// Roadmap §1.4 re-aggregates usage by extent; that redesign is deferred,
// not the counter maintenance.
func (s *PebbleStore) ReplaceExtents(ctx context.Context, id InodeID, writes []ExtentWrite, size int64) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}
	for _, w := range writes {
		if w.Extent == nil {
			return ErrInvalidArgument
		}
		// Not atomic with the inode row below (two Raft mutations); a
		// failure after a write leaves a harmless orphan /extent-meta row.
		if err := s.putExtentMeta(w.Extent); err != nil {
			return err
		}
	}
	if err := NewInodeStoreV2(s).ReplaceExtents(id, writes, size); err != nil {
		return err
	}
	s.inCache.del(id)
	s.publishEvent(Event{Type: EventSet, Key: fmt.Sprintf("inode:%d", id)})
	return nil
}

// ResolveFileChunks maps a file's inode to the flat list of chunk
// references holding its data, under either storage model (roadmap §1.3b
// read dual-model).
//
// Read discriminator: a V1 inode (ChunkMap non-empty) is returned verbatim;
// an empty ChunkMap is probed through the V2 extent serving surface
// (ResolveExtents + GetExtentMeta). V1 empty files and V2 empty inodes both
// resolve to nil, which read paths already treat as "no chunks". es may be
// nil (e.g. a MetadataService mock without the extent surface): the probe is
// skipped and the V1 verdict stands, so readers keep working unchanged for
// services that never produce V2 layouts.
//
// Each V2 extent's data lives in the chunk whose numeric ID equals the
// extent ID (see ECStore.SwitchChunkToEC), so a reference is the extent's
// ID, its file offset, and its authoritative logical length from
// /extent-meta. The probe is a per-call store read; for a hot path that
// already holds a resolved view, amortize by caching the result across
// calls rather than re-probing.
func ResolveFileChunks(ctx context.Context, es ExtentInodeService, inode *InodeMeta) ([]ChunkRef, error) {
	if inode == nil {
		return nil, nil
	}
	if len(inode.ChunkMap) > 0 {
		return inode.ChunkMap, nil
	}
	if es == nil {
		return nil, nil
	}
	refs, err := es.ResolveExtents(ctx, inode.ID)
	if err != nil {
		return nil, err
	}
	if len(refs) == 0 {
		return nil, nil
	}
	chunks := make([]ChunkRef, 0, len(refs))
	for _, ref := range refs {
		meta, err := es.GetExtentMeta(ctx, ref.ExtentID)
		if err != nil {
			return nil, err
		}
		chunks = append(chunks, ChunkRef{
			ID:     ChunkID(ref.ExtentID),
			Offset: ref.LogicalOffset,
			Length: int32(meta.LogicalLen),
		})
	}
	return chunks, nil
}

// extentForRef builds the V2 extent metadata for a chunk reference. The
// extent's data lives in the chunk whose numeric ID equals the extent ID
// (see ResolveFileChunks), so the extent mirrors the chunk's ID and
// length. class/ecStripe come from ecClassForRef: ECConfig-bucket writes
// mark the extent ColdEC with the chunk's stripe, everything else stays
// hot-replica (roadmap §1.3d).
func extentForRef(ref ChunkRef, class StorageClass, ecStripe string) *ExtentMetaV2 {
	return &ExtentMetaV2{
		ID:           ExtentIDV2(ref.ID),
		Generation:   1,
		LogicalLen:   int64(ref.Length),
		Lifecycle:    LifecycleReady,
		StorageClass: class,
		ECStripeID:   ecStripe,
	}
}

// ecClassForRef resolves whether the chunk backing a committed ref is an EC
// chunk, and the extent's storage class + stripe reference if so (roadmap
// §1.3d). The chunk is the truth: direct-EC writes land via
// writeECShardDirect, which lifts the chunk to durable EC
// (RecordDirectEC sets ECStripeID = ECGroup.GroupID) before the ref is ever
// committed, so by commit time an ECConfig-bucket chunk carries both fields.
//
// Degrades to hot-replica defaults when the chunk cannot be read or is not
// EC — the extent's data is reachable through the chunk path regardless of
// class, so a lookup failure (e.g. a fake ref in a unit test, or a chunk
// raced by a tombstone) must never fail the write.
func ecClassForRef(ctx context.Context, meta commitModelAwareService, ref ChunkRef) (StorageClass, string) {
	ch, err := meta.GetChunk(ctx, ref.ID)
	if err != nil || ch == nil || ch.ECGroup == nil {
		return StorageClassHotReplica, ""
	}
	stripe := ch.ECStripeID
	if stripe == "" {
		// Fall back to the group ID for the pre/post-RecordDirect window —
		// both spell the same "ec-<chunk-id>" value.
		stripe = ch.ECGroup.GroupID
	}
	return StorageClassColdEC, stripe
}

// commitModelAwareService is the narrow surface CommitChunkRefsModelAware
// needs: UpdateInode for the V1 fallback and GetChunk to resolve each ref's
// EC class at commit time. The V2 extent surface is probed by type
// assertion, so services that only compose sub-interfaces (e.g. the
// write-attempt recovery worker's writeRecoveryMeta) can use the shared
// commit decision without implementing the full MetadataService.
type commitModelAwareService interface {
	UpdateInode(ctx context.Context, meta *InodeMeta) error
	GetChunk(ctx context.Context, chunkID ChunkID) (*ChunkMeta, error)
}

// CommitChunkRefsModelAware lands newChunkRefs as the file's data set
// under whichever storage model the serving surface supports (roadmap
// §1.3c write dual-model). It is the single commit decision shared by the
// gateway write paths and the write-attempt recovery worker, so the
// inline-vs-pages policy cannot drift between callers.
//
// With a V2 extent surface present, the commit produces V2 layout: a
// single chunk holding ≤ MaxInlineExtentSize becomes an inline extent;
// otherwise the whole ref set is rewritten as extent pages
// (ReplaceExtents). A V2 row that already exists (detected by probing the
// empty ChunkMap) is rewritten even when newChunkRefs is empty, so a
// zero-byte overwrite empties it rather than leaving stale extents.
// Without the extent surface (or for a brand-new zero-ref file) it falls
// back to the V1 ChunkMap update.
//
// CTime/MTime are set by the serving writer on the V2 path (parity with
// UpdateInode); the V1 fallback sets MTime here and lets UpdateInode bump
// CTime. inode must be the full read-modify-write projection (callers
// obtain it via GetInode before allocating).
func CommitChunkRefsModelAware(ctx context.Context, meta commitModelAwareService, inode *InodeMeta, newChunkRefs []ChunkRef, size int64) error {
	es, _ := meta.(ExtentInodeService)
	if es != nil {
		isV2 := len(inode.ChunkMap) == 0
		if isV2 {
			refs, err := es.ResolveExtents(ctx, inode.ID)
			if err != nil {
				return err
			}
			isV2 = len(refs) > 0
		}
		if isV2 || len(newChunkRefs) > 0 {
			if len(newChunkRefs) == 1 && size <= MaxInlineExtentSize {
				ref := newChunkRefs[0]
				class, stripe := ecClassForRef(ctx, meta, ref)
				return es.SetInlineExtent(ctx, inode.ID, extentForRef(ref, class, stripe), size)
			}
			writes := make([]ExtentWrite, 0, len(newChunkRefs))
			for _, ref := range newChunkRefs {
				class, stripe := ecClassForRef(ctx, meta, ref)
				writes = append(writes, ExtentWrite{Extent: extentForRef(ref, class, stripe), Offset: ref.Offset})
			}
			return es.ReplaceExtents(ctx, inode.ID, writes, size)
		}
	}
	inode.Size = size
	inode.ChunkMap = newChunkRefs
	inode.MTime = nowUnixNano()
	return meta.UpdateInode(ctx, inode)
}
