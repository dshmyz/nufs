package metadata

import (
	"fmt"
)

// InodeStoreV2 provides the V2.1 fixed-attribute inode operations
// (§11.1). It manages layout transitions (Empty → InlineExtent →
// ExtentPages) and COW extent-page root switching, which happen under
// one atomic Raft mutation on the inode.
type InodeStoreV2 struct {
	store *PebbleStore
	pages *ExtentPageStore
}

// NewInodeStoreV2 creates the V2 inode store.
func NewInodeStoreV2(store *PebbleStore) *InodeStoreV2 {
	return &InodeStoreV2{store: store, pages: NewExtentPageStore(store)}
}

// inodeV2Key formats the inode key.
func inodeV2Key(id InodeID) string {
	return fmt.Sprintf("%s%d", prefixInode, id)
}

// Get reads an inode. Returns (nil, nil) if absent.
func (s *InodeStoreV2) Get(id InodeID) (*InodeMetaV2, error) {
	var in InodeMetaV2
	exists, err := s.store.getValue(inodeV2Key(id), &in)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, nil
	}
	return &in, nil
}

// Put writes an inode (atomically through Raft).
func (s *InodeStoreV2) Put(in *InodeMetaV2) error {
	return s.store.putMsgpack(inodeV2Key(in.ID), in)
}

// putWithBucketStats persists the inode row while keeping the bucket byte
// usage counter correct (roadmap §1.3c). UpdateInode accumulates usage via
// addBucketStatsOp for V1 rows; the V2 commit writers must do the same or
// the counter freezes and byte-quota enforcement silently degrades once
// writes start landing V2 layout. The stats row and the inode row share
// one atomic Raft batch, matching UpdateInode. AppendExtent/PromoteToPages
// use s.Put directly: PromoteToPages does not change Size, and
// AppendExtent's serving surface is not yet wired to a quota-enforced path.
func (s *InodeStoreV2) putWithBucketStats(in *InodeMetaV2, oldSize int64) error {
	if !s.store.cfg.UseBucketStats {
		return s.Put(in)
	}
	var ops []batchOp
	s.store.addBucketStatsOp(in.BucketRoot, in.Size-oldSize, 0, &ops)
	ops = append(ops, batchOp{Key: inodeV2Key(in.ID), Value: in})
	return s.store.applyBatchMsgpack(ops, nil)
}

// CreateEmpty creates an empty inode.
func (s *InodeStoreV2) CreateEmpty(id InodeID, typ FileType, bucketRoot InodeID, uid, gid, mode uint32) (*InodeMetaV2, error) {
	now := nowUnixNano()
	in := &InodeMetaV2{
		ID:         id,
		Type:       typ,
		BucketRoot: bucketRoot,
		UID:        uid,
		GID:        gid,
		Mode:       mode,
		CTime:      now,
		MTime:      now,
		ATime:      now,
		Layout:     LayoutEmpty,
	}
	if err := s.Put(in); err != nil {
		return nil, err
	}
	return in, nil
}

// SetInlineExtent promotes an empty inode to a single inline extent
// (files with one extent, ≤ 16 MiB). Layout → InlineExtent.
func (s *InodeStoreV2) SetInlineExtent(id InodeID, extent *ExtentMetaV2, size int64) error {
	in, err := s.Get(id)
	if err != nil {
		return err
	}
	if in == nil {
		return ErrInodeNotFound
	}
	oldRoot := in.ExtentRoot
	oldCount := in.ExtentPageCount
	oldSize := in.Size
	in.Layout = LayoutInlineExtent
	in.InlineExtent = extent
	in.Size = size
	in.MTime = nowUnixNano()
	in.CTime = nowUnixNano()
	if err := s.putWithBucketStats(in, oldSize); err != nil {
		return err
	}
	// Old extent-pages root enters delayed GC once the inline row is durable
	// (parity with PromoteToPages/AppendExtent/ReplaceExtents). Without this a
	// pages→inline shrink (e.g. truncate-down then rewrite to ≤ MaxInlineExtentSize)
	// would orphan the old root's pages and leave dangling chunk refs behind.
	if oldRoot != 0 {
		_ = s.pages.DeleteRoot(id, oldRoot, oldCount)
	}
	return nil
}

// PromoteToPages transitions an inline-extent inode to extent pages
// (multi-extent files). The inline extent becomes page 0 under root 1.
func (s *InodeStoreV2) PromoteToPages(id InodeID) error {
	in, err := s.Get(id)
	if err != nil {
		return err
	}
	if in == nil {
		return ErrInodeNotFound
	}
	newRoot := in.ExtentRoot + 1
	page := &ExtentPage{InodeID: id, PageNo: 0}
	if in.InlineExtent != nil {
		page.Extents = []ExtentRef{{ExtentID: in.InlineExtent.ID}}
	}
	if err := s.pages.writePage(page, newRoot); err != nil {
		return err
	}
	oldRoot := in.ExtentRoot
	oldCount := in.ExtentPageCount
	in.Layout = LayoutExtentPages
	in.InlineExtent = nil
	in.ExtentRoot = newRoot
	in.ExtentRootVersion++
	in.ExtentPageCount = 1
	in.CTime = nowUnixNano()
	if err := s.Put(in); err != nil {
		return err
	}
	// Old root enters delayed GC once the new root is durable.
	if oldRoot != 0 {
		_ = s.pages.DeleteRoot(id, oldRoot, oldCount)
	}
	return nil
}

// AppendExtent appends an extent reference to the file's COW page set
// and switches the inode root atomically. Returns the new root.
func (s *InodeStoreV2) AppendExtent(id InodeID, ref ExtentRef, extentSize int64) (uint64, error) {
	in, err := s.Get(id)
	if err != nil {
		return 0, err
	}
	if in == nil {
		return 0, ErrInodeNotFound
	}
	// Ensure we are in page mode.
	if in.Layout != LayoutExtentPages {
		if err := s.PromoteToPages(id); err != nil {
			return 0, err
		}
		in, err = s.Get(id)
		if err != nil {
			return 0, err
		}
	}
	newRoot := in.ExtentRoot + 1
	lastPage := in.ExtentPageCount
	var newPage uint32
	var created bool
	if lastPage == 0 {
		newPage, created, err = s.pages.AppendExtent(id, in.ExtentRoot, 0, ref)
	} else {
		newPage, created, err = s.pages.AppendExtent(id, in.ExtentRoot, lastPage-1, ref)
	}
	if err != nil {
		return 0, err
	}
	if created {
		in.ExtentPageCount++
	}
	in.ExtentRoot = newRoot
	in.ExtentRootVersion++
	oldSize := in.Size
	in.Size += extentSize
	in.MTime = nowUnixNano()
	in.CTime = nowUnixNano()
	if err := s.putWithBucketStats(in, oldSize); err != nil {
		return 0, err
	}
	_ = newPage
	return newRoot, nil
}

// ReplaceExtents rewrites the file's entire extent set as extent pages
// under a fresh COW root, replacing whatever model the row previously had
// (empty, inline, or earlier pages). Unlike PromoteToPages it does NOT
// preserve an old inline extent as page 0 — the gateway overwrite has
// already rewritten the old extent's data into the new chunk set, so the
// old reference would dangle. Empty writes (a zero-byte overwrite) leave a
// valid pages-layout inode with no extents.
func (s *InodeStoreV2) ReplaceExtents(id InodeID, writes []ExtentWrite, size int64) error {
	in, err := s.Get(id)
	if err != nil {
		return err
	}
	if in == nil {
		return ErrInodeNotFound
	}
	// The new extent set is written in full under the fresh root — no COW
	// carry-over from older roots, so the old root can be deleted as soon
	// as the switch is durable.
	newRoot := in.ExtentRoot + 1
	oldRoot := in.ExtentRoot
	oldCount := in.ExtentPageCount
	pageCount := uint32((len(writes) + MaxExtentsPerPage - 1) / MaxExtentsPerPage)
	for p := uint32(0); p < pageCount; p++ {
		lo := int(p) * MaxExtentsPerPage
		hi := lo + MaxExtentsPerPage
		if hi > len(writes) {
			hi = len(writes)
		}
		page := &ExtentPage{InodeID: id, PageNo: p}
		for _, w := range writes[lo:hi] {
			page.Extents = append(page.Extents, ExtentRef{ExtentID: w.Extent.ID, LogicalOffset: w.Offset})
		}
		if err := s.pages.writePage(page, newRoot); err != nil {
			return err
		}
	}
	now := nowUnixNano()
	in.Layout = LayoutExtentPages
	in.InlineExtent = nil
	in.ExtentRoot = newRoot
	in.ExtentRootVersion++
	in.ExtentPageCount = pageCount
	oldSize := in.Size
	in.Size = size
	in.MTime = now
	in.CTime = now
	if err := s.putWithBucketStats(in, oldSize); err != nil {
		return err
	}
	// Old root enters delayed GC once the new root is durable.
	if oldRoot != 0 {
		_ = s.pages.DeleteRoot(id, oldRoot, oldCount)
	}
	return nil
}

// ResolveExtents returns the flat extent list for a file (inline or
// pages).
func (s *InodeStoreV2) ResolveExtents(id InodeID) ([]ExtentRef, error) {
	in, err := s.Get(id)
	if err != nil {
		return nil, err
	}
	if in == nil {
		return nil, ErrInodeNotFound
	}
	switch in.Layout {
	case LayoutEmpty:
		return nil, nil
	case LayoutInlineExtent:
		return []ExtentRef{{ExtentID: in.InlineExtent.ID}}, nil
	case LayoutExtentPages:
		return s.pages.ResolveExtents(in)
	default:
		return nil, fmt.Errorf("metadata: unknown layout %d", in.Layout)
	}
}
