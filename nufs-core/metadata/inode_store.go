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
	in.Layout = LayoutInlineExtent
	in.InlineExtent = extent
	in.Size = size
	in.MTime = nowUnixNano()
	return s.Put(in)
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
	in.Size += extentSize
	in.MTime = nowUnixNano()
	if err := s.Put(in); err != nil {
		return 0, err
	}
	_ = newPage
	return newRoot, nil
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
