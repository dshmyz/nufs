package s3fs

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
)

var (
	errCacheNotFound = errors.New("cache: not found")
	errCacheClosed   = errors.New("cache: closed")
)

// CacheInode is the on-disk inode structure for the local metadata cache.
type CacheInode struct {
	ID       uint64            `json:"id"`
	IsDir    bool              `json:"is_dir"`
	Name     string            `json:"name"`
	Size     uint64            `json:"size"`
	Mode     uint32            `json:"mode"`
	UID      uint32            `json:"uid"`
	GID      uint32            `json:"gid"`
	Mtime    int64             `json:"mtime"`
	Ctime    int64             `json:"ctime"`
	Atime    int64             `json:"atime"`
	ETag     string            `json:"etag,omitempty"`
	Children map[string]uint64 `json:"children,omitempty"` // name → child inode ID
	SymlinkTarget string       `json:"symlink_target,omitempty"`
}

// PendingUpload tracks a cache file that needs to be uploaded to S3.
type PendingUpload struct {
	CachePath  string `json:"cache_path"`
	RemotePath string `json:"remote_path"`
	Size       int64  `json:"size"`
}

// PebbleCache is a local metadata cache backed by Pebble.
type PebbleCache struct {
	db   *pebble.DB
	mu   sync.Mutex
	next uint64 // monotonic inode ID generator
}

// OpenCache opens or creates a Pebble cache at the given directory.
func OpenCache(dir string) (*PebbleCache, error) {
	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		return nil, fmt.Errorf("open cache db: %w", err)
	}
	c := &PebbleCache{db: db}
	// Find max existing inode ID to resume from.
	iter, err := db.NewIter(&pebble.IterOptions{LowerBound: []byte("inode:")})
	if err != nil {
		db.Close()
		return nil, err
	}
	if iter.First() {
		for iter.Valid() {
			var in CacheInode
			if json.Unmarshal(iter.Value(), &in) == nil && in.ID > c.next {
				c.next = in.ID
			}
			iter.Next()
		}
	}
	iter.Close()
	c.next++
	return c, nil
}

// NextID returns a new unique inode ID.
func (c *PebbleCache) NextID() uint64 {
	c.mu.Lock()
	id := c.next
	c.next++
	c.mu.Unlock()
	return id
}

func inodeKey(id uint64) []byte {
	return []byte(fmt.Sprintf("inode:%016x", id))
}

func dirEntryKey(parentID uint64, name string) []byte {
	return []byte(fmt.Sprintf("dir:%016x:%s", parentID, name))
}

func pendingKey(basename string) []byte {
	return []byte(fmt.Sprintf("pending:%s", basename))
}

// PutInode stores an inode in the cache.
func (c *PebbleCache) PutInode(in *CacheInode) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	data, err := json.Marshal(in)
	if err != nil {
		return err
	}
	return c.db.Set(inodeKey(in.ID), data, nil)
}

// GetInode retrieves an inode by ID.
func (c *PebbleCache) GetInode(id uint64) (*CacheInode, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	data, closer, err := c.db.Get(inodeKey(id))
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, errCacheNotFound
	}
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	var in CacheInode
	if err := json.Unmarshal(data, &in); err != nil {
		return nil, err
	}
	return &in, nil
}

// DeleteInode removes an inode from the cache.
func (c *PebbleCache) DeleteInode(id uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.db.Delete(inodeKey(id), nil)
}

// PutDirEntry stores a directory entry mapping (parent, name) → childID.
func (c *PebbleCache) PutDirEntry(parentID uint64, name string, childID uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	buf := make([]byte, 8)
	buf[0] = byte(childID)
	buf[1] = byte(childID >> 8)
	buf[2] = byte(childID >> 16)
	buf[3] = byte(childID >> 24)
	buf[4] = byte(childID >> 32)
	buf[5] = byte(childID >> 40)
	buf[6] = byte(childID >> 48)
	buf[7] = byte(childID >> 56)
	return c.db.Set(dirEntryKey(parentID, name), buf, nil)
}

// GetDirEntry looks up a child inode ID by parent and name.
func (c *PebbleCache) GetDirEntry(parentID uint64, name string) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	data, closer, err := c.db.Get(dirEntryKey(parentID, name))
	if errors.Is(err, pebble.ErrNotFound) {
		return 0, errCacheNotFound
	}
	if err != nil {
		return 0, err
	}
	defer closer.Close()
	if len(data) < 8 {
		return 0, errors.New("cache: corrupt dir entry")
	}
	id := uint64(data[0]) | uint64(data[1])<<8 | uint64(data[2])<<16 | uint64(data[3])<<24 |
		uint64(data[4])<<32 | uint64(data[5])<<40 | uint64(data[6])<<48 | uint64(data[7])<<56
	return id, nil
}

// DeleteDirEntry removes a directory entry.
func (c *PebbleCache) DeleteDirEntry(parentID uint64, name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.db.Delete(dirEntryKey(parentID, name), nil)
}

// ListDirEntries returns all child entries for a directory.
func (c *PebbleCache) ListDirEntries(parentID uint64) (map[string]uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	prefix := []byte(fmt.Sprintf("dir:%016x:", parentID))
	iter, err := c.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: append(prefix, 0xff),
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	entries := make(map[string]uint64)
	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		// Extract name from key: "dir:{parentID}:{name}"
		nameStart := len(prefix)
		if nameStart >= len(key) {
			continue
		}
		name := string(key[nameStart:])
		val := iter.Value()
		if len(val) < 8 {
			continue
		}
		id := uint64(val[0]) | uint64(val[1])<<8 | uint64(val[2])<<16 | uint64(val[3])<<24 |
			uint64(val[4])<<32 | uint64(val[5])<<40 | uint64(val[6])<<48 | uint64(val[7])<<56
		entries[name] = id
	}
	return entries, nil
}

// RecordPending stores a pending upload entry for crash recovery.
func (c *PebbleCache) RecordPending(pu *PendingUpload) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	data, err := json.Marshal(pu)
	if err != nil {
		return err
	}
	return c.db.Set(pendingKey(pu.CachePath), data, nil)
}

// ClearPending removes a pending upload entry.
func (c *PebbleCache) ClearPending(cachePath string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.db.Delete(pendingKey(cachePath), nil)
}

// ListPending returns all pending uploads.
func (c *PebbleCache) ListPending() ([]*PendingUpload, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	prefix := []byte("pending:")
	iter, err := c.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: append(prefix, 0xff),
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	var result []*PendingUpload
	for iter.First(); iter.Valid(); iter.Next() {
		var pu PendingUpload
		if err := json.Unmarshal(iter.Value(), &pu); err == nil {
			result = append(result, &pu)
		}
	}
	return result, nil
}

// Scan returns a snapshot time for the given directory inode.
// Returns zero time if never scanned.
func (c *PebbleCache) GetLastScan(id uint64) time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	in, err := c.getInodeUnlocked(id)
	if err != nil {
		return time.Time{}
	}
	return time.Unix(in.Ctime, 0)
}

// SetLastScan updates the ctime field as a scan timestamp.
func (c *PebbleCache) SetLastScan(id uint64, t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	in, err := c.getInodeUnlocked(id)
	if err != nil {
		return err
	}
	in.Ctime = t.UnixNano()
	return c.putInodeUnlocked(in)
}

func (c *PebbleCache) getInodeUnlocked(id uint64) (*CacheInode, error) {
	data, closer, err := c.db.Get(inodeKey(id))
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, errCacheNotFound
	}
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	var in CacheInode
	if err := json.Unmarshal(data, &in); err != nil {
		return nil, err
	}
	return &in, nil
}

func (c *PebbleCache) putInodeUnlocked(in *CacheInode) error {
	data, err := json.Marshal(in)
	if err != nil {
		return err
	}
	return c.db.Set(inodeKey(in.ID), data, nil)
}

// Close closes the underlying Pebble database.
func (c *PebbleCache) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}
