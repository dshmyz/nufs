package metadata

import (
	"encoding/binary"
)

// ExtentPageStore manages copy-on-write extent pages (V2.1 §11.1).
//
// Multi-extent files store their extent references in pages under
//
//	/extent-page/{inode_id}/{extent_root}/{page_no}
//
// Each page holds at most MaxExtentsPerPage references (256 → 4 GiB at
// a 16 MiB extent). Updates use copy-on-write: a modified page is
// written under a new extent_root, then one atomic Raft mutation
// switches inode.extent_root. Old roots enter delayed GC.
type ExtentPageStore struct {
	store *PebbleStore
}

// NewExtentPageStore creates a page store bound to the Pebble store.
func NewExtentPageStore(store *PebbleStore) *ExtentPageStore {
	return &ExtentPageStore{store: store}
}

// extentPageKey formats the on-disk key for a page.
func extentPageKey(inodeID InodeID, root uint64, pageNo uint32) string {
	var buf [64]byte
	n := copy(buf[:], prefixExtentPage)
	n += binary.PutUvarint(buf[n:], uint64(inodeID))
	buf[n] = '/'
	n++
	n += binary.PutUvarint(buf[n:], root)
	buf[n] = '/'
	n++
	n += binary.PutUvarint(buf[n:], uint64(pageNo))
	return string(buf[:n])
}

// GetPage reads a page under a specific root.
func (ps *ExtentPageStore) GetPage(inodeID InodeID, root uint64, pageNo uint32) (*ExtentPage, error) {
	key := extentPageKey(inodeID, root, pageNo)
	exists, err := ps.store.getValue(key, &ExtentPage{})
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, nil
	}
	// getValue decodes into a throwaway struct; read directly to get the
	// populated page with its identity set.
	raw, closer, err := ps.store.db.Get([]byte(key))
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	data := make([]byte, len(raw))
	copy(data, raw)
	page := &ExtentPage{}
	if err := unmarshalValue(data, page); err != nil {
		return nil, err
	}
	page.InodeID = inodeID
	page.PageNo = pageNo
	return page, nil
}

// ResolvePage reads page pageNo across a COW root history: it returns
// the newest root that has this page. Because COW only rewrites the
// pages that changed under a new root, unmodified pages live under
// older roots; resolving walks back to find them.
func (ps *ExtentPageStore) ResolvePage(inodeID InodeID, currentRoot uint64, pageNo uint32) (*ExtentPage, error) {
	for root := currentRoot; root > 0; root-- {
		page, err := ps.GetPage(inodeID, root, pageNo)
		if err != nil {
			return nil, err
		}
		if page != nil {
			return page, nil
		}
	}
	return nil, nil
}

// writePage writes a page under a root (COW write).
func (ps *ExtentPageStore) writePage(page *ExtentPage, root uint64) error {
	return ps.store.putMsgpack(extentPageKey(page.InodeID, root, page.PageNo), page)
}

// UpdatePage performs a copy-on-write update of one page: it reads the
// current page under oldRoot, applies fn, writes the modified page under
// newRoot, and returns the new page (which the caller publishes by
// switching inode.extent_root atomically).
//
// If the page does not exist under oldRoot, an empty page is created.
func (ps *ExtentPageStore) UpdatePage(inodeID InodeID, oldRoot uint64, newRoot uint64, pageNo uint32, fn func(page *ExtentPage) error) (*ExtentPage, error) {
	page, err := ps.GetPage(inodeID, oldRoot, pageNo)
	if err != nil {
		return nil, err
	}
	if page == nil {
		page = &ExtentPage{InodeID: inodeID, PageNo: pageNo}
	}
	// COW: copy before mutating so the old root's page is untouched.
	copied := *page
	copied.Extents = append([]ExtentRef(nil), page.Extents...)
	if err := fn(&copied); err != nil {
		return nil, err
	}
	if err := ps.writePage(&copied, newRoot); err != nil {
		return nil, err
	}
	return &copied, nil
}

// AppendExtent appends an extent reference to the last page, creating a
// new page if needed. Returns the page number written and whether a new
// page was created. The changed page is written under newRoot (COW);
// unmodified pages remain under older roots and are resolved by walking
// back.
func (ps *ExtentPageStore) AppendExtent(inodeID InodeID, root uint64, pageNo uint32, ref ExtentRef) (uint32, bool, error) {
	newRoot := root + 1
	page, err := ps.ResolvePage(inodeID, root, pageNo)
	if err != nil {
		return 0, false, err
	}
	if page != nil && len(page.Extents) >= MaxExtentsPerPage {
		// Page full: start a new page. The caller must bump
		// inode.ExtentPageCount and switch root.
		np := &ExtentPage{InodeID: inodeID, PageNo: pageNo + 1, Extents: []ExtentRef{ref}}
		if err := ps.writePage(np, newRoot); err != nil {
			return 0, false, err
		}
		return pageNo + 1, true, nil
	}
	updated, err := ps.UpdatePage(inodeID, root, newRoot, pageNo, func(p *ExtentPage) error {
		p.Extents = append(p.Extents, ref)
		return nil
	})
	if err != nil {
		return 0, false, err
	}
	return updated.PageNo, false, nil
}

// ResolveExtents reads all pages under a root and returns the flat
// extent reference list in page order. Each page is resolved across the
// COW root history (unmodified pages live under older roots).
func (ps *ExtentPageStore) ResolveExtents(inode *InodeMetaV2) ([]ExtentRef, error) {
	var out []ExtentRef
	for p := uint32(0); p < inode.ExtentPageCount; p++ {
		page, err := ps.ResolvePage(inode.ID, inode.ExtentRoot, p)
		if err != nil {
			return nil, err
		}
		if page == nil {
			continue
		}
		out = append(out, page.Extents...)
	}
	return out, nil
}

// DeleteRoot removes all pages under a root (delayed GC of an old COW
// root). Called after the new root is durable and no reader references
// the old one.
func (ps *ExtentPageStore) DeleteRoot(inodeID InodeID, root uint64, pageCount uint32) error {
	for p := uint32(0); p < pageCount; p++ {
		if err := ps.store.db.Delete([]byte(extentPageKey(inodeID, root, p)), nil); err != nil {
			return err
		}
	}
	return nil
}
