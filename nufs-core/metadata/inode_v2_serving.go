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
