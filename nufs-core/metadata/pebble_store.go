package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

// PebbleStore implements MetadataService using Pebble (LSM-tree) as the
// primary storage engine. Designed for hundred-million to billion-scale metadata.
// When configured with Raft, all writes go through the Raft log for consistency.
type PebbleStore struct {
	db        *pebble.DB
	cache     *pebble.DB // Optional read cache (nil if disabled)
	placement *PlacementEngine
	chunkGen  *ChunkIDGenerator
	inodeSeq  atomic.Uint64
	closed    atomic.Bool
	mu        sync.RWMutex
	cfg       PebbleStoreConfig

	// Raft integration: when set, all mutating operations are applied via Raft
	raft *RaftNode

	// advisoryLocks tracks the in-memory advisory file lock table.
	// See lock.go for the model. State is dropped on Close / restart.
	advisoryLocks *advisoryLockManager
}

// PebbleStoreConfig configures a PebbleStore instance.
type PebbleStoreConfig struct {
	// Dir is the directory for Pebble data files.
	Dir string
	// CacheDir enables a secondary Pebble instance for read caching (optional).
	CacheDir string
	// NodeID is used for chunk ID generation.
	NodeID uint64
	// MemTableSize is the size of each memtable in bytes (default 256MB).
	MemTableSize uint64
	// MaxOpenFiles limits the number of open SST files (default 16384).
	MaxOpenFiles int
	// UseInMemory uses an in-memory VFS (for testing only).
	UseInMemory bool
}

// batchJSONOp represents a single key-value write (JSON-serialized) in an atomic batch.
type batchJSONOp struct {
	Key   string      `json:"key"`
	Value interface{} `json:"value"`
}

// NewPebbleStore creates a new Pebble-backed metadata store.
func NewPebbleStore(cfg PebbleStoreConfig) (*PebbleStore, error) {
	if cfg.Dir == "" {
		return nil, fmt.Errorf("pebble store: Dir is required")
	}

	pebbleOpts := &pebble.Options{
		MemTableSize:                256 << 20, // 256MB
		MemTableStopWritesThreshold: 8,
		MaxOpenFiles:                16384,
		FormatMajorVersion:          pebble.FormatNewest,
	}
	if cfg.MemTableSize > 0 {
		pebbleOpts.MemTableSize = cfg.MemTableSize
	}
	if cfg.MaxOpenFiles > 0 {
		pebbleOpts.MaxOpenFiles = cfg.MaxOpenFiles
	}
	if cfg.UseInMemory {
		pebbleOpts.FS = vfs.NewMem()
	}

	db, err := pebble.Open(cfg.Dir, pebbleOpts)
	if err != nil {
		return nil, fmt.Errorf("pebble store: open db: %w", err)
	}

	s := &PebbleStore{
		db:            db,
		placement:     NewPlacementEngine(),
		chunkGen:      NewChunkIDGenerator(cfg.NodeID),
		cfg:           cfg,
		advisoryLocks: newAdvisoryLockManager(),
	}
	s.inodeSeq.Store(uint64(RootInodeID))

	if err := s.initRootInode(); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

// Close shuts down the store.
func (s *PebbleStore) Close() error {
	if s.closed.Swap(true) {
		return ErrServiceClosed
	}
	var errs []error
	if err := s.db.Close(); err != nil {
		errs = append(errs, err)
	}
	if s.cache != nil {
		if err := s.cache.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("pebble store: close errors: %v", errs)
	}
	return nil
}

// AdvisoryLock acquires an exclusive lock on inode for owner.
// See lock.go for the full model.
func (s *PebbleStore) AdvisoryLock(ctx context.Context, inode InodeID, owner string) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}
	// Honour context cancellation before doing any work. The lock
	// itself is a short, in-memory operation, so cancellation can
	// only land at the entry point — once we are inside the mutex
	// the call completes in microseconds.
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.advisoryLocks.acquire(inode, owner, LockModeExclusive)
}

// AdvisoryLockShared is the read-side equivalent. See AdvisoryLock
// and lock.go.
func (s *PebbleStore) AdvisoryLockShared(ctx context.Context, inode InodeID, owner string) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.advisoryLocks.acquire(inode, owner, LockModeShared)
}

// AdvisoryUnlock releases one acquisition of (inode, owner). A
// no-op for owners that do not hold the lock, matching flock(2).
func (s *PebbleStore) AdvisoryUnlock(ctx context.Context, inode InodeID, owner string) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.advisoryLocks.release(inode, owner)
}

// AdvisoryListLocks returns a snapshot of every holder of inode.
// Used by `dfsctl locks <inode>` and the admin endpoint; the
// runtime path does not call it.
func (s *PebbleStore) AdvisoryListLocks(ctx context.Context, inode InodeID) ([]LockInfo, error) {
	if s.closed.Load() {
		return nil, ErrServiceClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return s.advisoryLocks.list(inode), nil
}

// ========== Extended Attributes (xattrs) ==========

// GetXAttr returns the value of the named xattr on the given inode.
// Returns ErrXAttrNotFound if the attribute does not exist.
func (s *PebbleStore) GetXAttr(ctx context.Context, id InodeID, name string) ([]byte, error) {
	if s.closed.Load() {
		return nil, ErrServiceClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	meta, err := s.GetInode(ctx, id)
	if err != nil {
		return nil, err
	}
	if meta.XAttrs == nil {
		return nil, ErrXAttrNotFound
	}
	val, ok := meta.XAttrs[name]
	if !ok {
		return nil, ErrXAttrNotFound
	}
	out := make([]byte, len(val))
	copy(out, val)
	return out, nil
}

// SetXAttr sets the named xattr on the given inode. If the attribute
// already exists it is overwritten. An empty value removes the key.
func (s *PebbleStore) SetXAttr(ctx context.Context, id InodeID, name string, value []byte) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	meta, err := s.GetInode(ctx, id)
	if err != nil {
		return err
	}
	if meta.XAttrs == nil {
		meta.XAttrs = make(map[string][]byte)
	}
	v := make([]byte, len(value))
	copy(v, value)
	meta.XAttrs[name] = v
	return s.UpdateInode(ctx, meta)
}

// ListXAttr returns all xattrs on the given inode. The returned map
// is a copy; callers may mutate it freely.
func (s *PebbleStore) ListXAttr(ctx context.Context, id InodeID) (map[string][]byte, error) {
	if s.closed.Load() {
		return nil, ErrServiceClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	meta, err := s.GetInode(ctx, id)
	if err != nil {
		return nil, err
	}
	if len(meta.XAttrs) == 0 {
		return nil, nil
	}
	out := make(map[string][]byte, len(meta.XAttrs))
	for k, v := range meta.XAttrs {
		cp := make([]byte, len(v))
		copy(cp, v)
		out[k] = cp
	}
	return out, nil
}

// RemoveXAttr deletes the named xattr from the given inode. Removing
// a non-existent attribute is a no-op (not an error).
func (s *PebbleStore) RemoveXAttr(ctx context.Context, id InodeID, name string) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	meta, err := s.GetInode(ctx, id)
	if err != nil {
		return err
	}
	if meta.XAttrs == nil {
		return nil
	}
	delete(meta.XAttrs, name)
	return s.UpdateInode(ctx, meta)
}

// ========== Internal Helpers ==========

func (s *PebbleStore) initRootInode() error {
	key := fmt.Sprintf("%s%d", prefixInode, RootInodeID)
	_, closer, err := s.db.Get([]byte(key))
	if err == nil {
		closer.Close()
		return nil // Already initialized
	}
	if err != pebble.ErrNotFound {
		return fmt.Errorf("pebble store: init root inode: %w", err)
	}

	now := time.Now().UnixNano()
	root := &InodeMeta{
		ID:    RootInodeID,
		Type:  FileDirectory,
		Mode:  0755,
		NLink: 2,
		CTime: now,
		MTime: now,
		ATime: now,
	}
	return s.putJSON(key, root)
}

func (s *PebbleStore) nextInodeID() InodeID {
	return InodeID(s.inodeSeq.Add(1))
}

func (s *PebbleStore) putJSON(key string, v interface{}) error {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("pebble store: marshal: %w", err)
	}
	return s.applyViaRaft(OpSet, key, data)
}
// putJSONBatch writes to a Pebble batch (for atomic multi-key writes).
func putJSONBatch(batch *pebble.Batch, key string, v interface{}) error {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("pebble store: marshal: %w", err)
	}
	return batch.Set([]byte(key), data, nil)
}

func (s *PebbleStore) getJSON(key string, v interface{}) (bool, error) {
	val, closer, err := s.db.Get([]byte(key))
	if err == pebble.ErrNotFound {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("pebble store: get %q: %w", key, err)
	}
	defer closer.Close()

	// Copy val before closer.Close()
	data := make([]byte, len(val))
	copy(data, val)

	if err := json.Unmarshal(data, v); err != nil {
		return false, fmt.Errorf("pebble store: unmarshal %q: %w", key, err)
	}
	return true, nil
}

func (s *PebbleStore) deleteKey(key string) error {
	return s.applyViaRaft(OpDelete, key, nil)
}

// scanPrefix calls fn for each key-value pair matching the given prefix.
func (s *PebbleStore) scanPrefix(prefix string, fn func(key []byte, value []byte) error) error {
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(prefix),
		UpperBound: prefixUpperBound(prefix),
	})
	if err != nil {
		return err
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		val, err := iter.ValueAndErr()
		if err != nil {
			return err
		}
		if err := fn(iter.Key(), val); err != nil {
			return err
		}
	}
	return nil
}

// scanPrefixWithLimit returns up to `limit` entries matching prefix.
func (s *PebbleStore) scanPrefixWithLimit(prefix string, limit int) (keys [][]byte, vals [][]byte, err error) {
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(prefix),
		UpperBound: prefixUpperBound(prefix),
	})
	if err != nil {
		return nil, nil, err
	}
	defer iter.Close()

	count := 0
	for iter.First(); iter.Valid() && count < limit; iter.Next() {
		k := make([]byte, len(iter.Key()))
		copy(k, iter.Key())
		v, err := iter.ValueAndErr()
		if err != nil {
			return nil, nil, err
		}
		vc := make([]byte, len(v))
		copy(vc, v)
		keys = append(keys, k)
		vals = append(vals, vc)
		count++
	}
	return keys, vals, nil
}

// prefixUpperBound returns the lexicographic upper bound for a prefix scan.
// e.g., "/ns/5/" → "/ns/50" (increment last byte)
func prefixUpperBound(prefix string) []byte {
	b := []byte(prefix)
	if len(b) == 0 {
		return nil
	}
	// Find last byte that can be incremented
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] < 0xFF {
			b = b[:i+1]
			b[i]++
			return b
		}
	}
	return nil // all 0xFF
}

// ========== BucketService Implementation ==========

func (s *PebbleStore) CreateBucket(ctx context.Context, name string, policy PlacementPolicy) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}
	if len(name) == 0 || len(name) > MaxNameLength {
		return ErrInvalidArgument
	}

	bucketKey := prefixBucket + name
	var existing BucketInfo
	exists, err := s.getJSON(bucketKey, &existing)
	if err != nil {
		return err
	}
	if exists {
		return ErrBucketExists
	}

	rootID := s.nextInodeID()
	now := time.Now().UnixNano()
	root := &InodeMeta{
		ID:    rootID,
		Type:  FileDirectory,
		Mode:  0755,
		NLink: 2,
		CTime: now,
		MTime: now,
		ATime: now,
	}
	info := &BucketInfo{
		Name:         name,
		RootInode:    rootID,
		Policy:       policy,
		CreationDate: time.Now(),
	}
	ops := []batchJSONOp{
		{Key: fmt.Sprintf("%s%d", prefixInode, rootID), Value: root},
		{Key: bucketKey, Value: info},
		{Key: fmt.Sprintf("%s%s", prefixPolicy, name), Value: &policy},
	}
	return s.applyBatchJSON(ops, nil)
}

func (s *PebbleStore) DeleteBucket(ctx context.Context, name string) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}

	bucketKey := prefixBucket + name
	var info BucketInfo
	exists, err := s.getJSON(bucketKey, &info)
	if err != nil {
		return err
	}
	if !exists {
		return ErrBucketNotFound
	}

	// Check if bucket root has children
	nsPrefix := fmt.Sprintf("%s%d/", prefixNS, info.RootInode)
	_, vals, err := s.scanPrefixWithLimit(nsPrefix, 1)
	if err != nil {
		return err
	}
	if len(vals) > 0 {
		return ErrBucketNotEmpty
	}

	deletes := []string{bucketKey, fmt.Sprintf("%s%d", prefixInode, info.RootInode), fmt.Sprintf("%s%s", prefixPolicy, name)}
	return s.applyBatchJSON(nil, deletes)
}

func (s *PebbleStore) ListBuckets(ctx context.Context) ([]BucketInfo, error) {
	if s.closed.Load() {
		return nil, ErrServiceClosed
	}

	var buckets []BucketInfo
	err := s.scanPrefix(prefixBucket, func(key, val []byte) error {
		var b BucketInfo
		if err := json.Unmarshal(val, &b); err == nil {
			buckets = append(buckets, b)
		}
		return nil
	})
	return buckets, err
}

func (s *PebbleStore) GetBucket(ctx context.Context, name string) (*BucketInfo, error) {
	if s.closed.Load() {
		return nil, ErrServiceClosed
	}
	var info BucketInfo
	exists, err := s.getJSON(prefixBucket+name, &info)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, ErrBucketNotFound
	}
	return &info, nil
}

// ========== NamespaceService Implementation ==========

func (s *PebbleStore) MkDir(ctx context.Context, parent InodeID, name string, mode uint32) (*InodeMeta, error) {
	if s.closed.Load() {
		return nil, ErrServiceClosed
	}
	if len(name) > MaxNameLength {
		return nil, ErrNameTooLong
	}

	nsKey := fmt.Sprintf("%s%d/%s", prefixNS, parent, name)
	var existing DirEntry
	exists, err := s.getJSON(nsKey, &existing)
	if err != nil {
		return nil, err
	}
	if exists {
		return nil, ErrEntryExists
	}

	inodeID := s.nextInodeID()
	now := time.Now().UnixNano()
	meta := &InodeMeta{
		ID:    inodeID,
		Type:  FileDirectory,
		Mode:  mode,
		NLink: 2,
		CTime: now,
		MTime: now,
		ATime: now,
	}
	entry := &DirEntry{InodeID: inodeID, Type: FileDirectory, Name: name}
	ops := []batchJSONOp{
		{Key: fmt.Sprintf("%s%d", prefixInode, inodeID), Value: meta},
		{Key: nsKey, Value: entry},
	}

	// Update parent
	var parentMeta InodeMeta
	parentKey := fmt.Sprintf("%s%d", prefixInode, parent)
	pExists, _ := s.getJSON(parentKey, &parentMeta)
	if pExists {
		parentMeta.NLink++
		parentMeta.MTime = now
		ops = append(ops, batchJSONOp{Key: parentKey, Value: &parentMeta})
	}

	if err := s.applyBatchJSON(ops, nil); err != nil {
		return nil, err
	}
	return meta, nil
}

func (s *PebbleStore) RmDir(ctx context.Context, parent InodeID, name string) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}

	nsKey := fmt.Sprintf("%s%d/%s", prefixNS, parent, name)
	var entry DirEntry
	exists, err := s.getJSON(nsKey, &entry)
	if err != nil {
		return err
	}
	if !exists {
		return ErrEntryNotFound
	}
	if entry.Type != FileDirectory {
		return ErrNotDirectory
	}

	// Check not empty
	childPrefix := fmt.Sprintf("%s%d/", prefixNS, entry.InodeID)
	_, vals, err := s.scanPrefixWithLimit(childPrefix, 1)
	if err != nil {
		return err
	}
	if len(vals) > 0 {
		return ErrDirNotEmpty
	}

	deletes := []string{
		fmt.Sprintf("%s%d", prefixInode, entry.InodeID),
		nsKey,
	}

	// Update parent nlink
	var parentMeta InodeMeta
	parentKey := fmt.Sprintf("%s%d", prefixInode, parent)
	pExists, _ := s.getJSON(parentKey, &parentMeta)
	if pExists {
		parentMeta.MTime = time.Now().UnixNano()
		if parentMeta.NLink > 0 {
			parentMeta.NLink--
		}
		ops := []batchJSONOp{
			{Key: parentKey, Value: &parentMeta},
		}
		return s.applyBatchJSON(ops, deletes)
	}
	return s.applyBatchJSON(nil, deletes)
}

func (s *PebbleStore) ReadDir(ctx context.Context, parent InodeID, offset int, limit int) ([]DirEntry, error) {
	if s.closed.Load() {
		return nil, ErrServiceClosed
	}

	prefix := fmt.Sprintf("%s%d/", prefixNS, parent)
	var entries []DirEntry
	err := s.scanPrefix(prefix, func(key, val []byte) error {
		name := strings.TrimPrefix(string(key), prefix)
		if name == "" {
			return nil
		}
		var entry DirEntry
		if err := json.Unmarshal(val, &entry); err != nil {
			return nil
		}
		entry.Name = name
		entries = append(entries, entry)
		return nil
	})
	if err != nil {
		return nil, err
	}

	if offset >= len(entries) {
		return nil, nil
	}
	end := offset + limit
	if end > len(entries) {
		end = len(entries)
	}
	return entries[offset:end], nil
}

func (s *PebbleStore) CreateFile(ctx context.Context, parent InodeID, name string, mode uint32) (*InodeMeta, error) {
	if s.closed.Load() {
		return nil, ErrServiceClosed
	}
	if len(name) > MaxNameLength {
		return nil, ErrNameTooLong
	}

	nsKey := fmt.Sprintf("%s%d/%s", prefixNS, parent, name)
	var existing DirEntry
	exists, err := s.getJSON(nsKey, &existing)
	if err != nil {
		return nil, err
	}
	if exists {
		return nil, ErrEntryExists
	}

	inodeID := s.nextInodeID()
	now := time.Now().UnixNano()
	meta := &InodeMeta{
		ID: inodeID, Type: FileRegular, Mode: mode, NLink: 1,
		CTime: now, MTime: now, ATime: now,
	}
	entry := &DirEntry{InodeID: inodeID, Type: FileRegular, Name: name}
	ops := []batchJSONOp{
		{Key: fmt.Sprintf("%s%d", prefixInode, inodeID), Value: meta},
		{Key: nsKey, Value: entry},
	}
	if err := s.applyBatchJSON(ops, nil); err != nil {
		return nil, err
	}
	return meta, nil
}

func (s *PebbleStore) Unlink(ctx context.Context, parent InodeID, name string) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}

	nsKey := fmt.Sprintf("%s%d/%s", prefixNS, parent, name)
	var entry DirEntry
	exists, err := s.getJSON(nsKey, &entry)
	if err != nil {
		return err
	}
	if !exists {
		return ErrEntryNotFound
	}
	if entry.Type == FileDirectory {
		return ErrNotFile
	}

	var meta InodeMeta
	inodeKey := fmt.Sprintf("%s%d", prefixInode, entry.InodeID)
	pExists, _ := s.getJSON(inodeKey, &meta)
	ops := []batchJSONOp{}
	deletes := []string{nsKey}

	if pExists {
		meta.NLink--
		meta.MTime = time.Now().UnixNano()
		if meta.NLink <= 0 {
			deletes = append(deletes, inodeKey)
		} else {
			ops = append(ops, batchJSONOp{Key: inodeKey, Value: &meta})
		}
	}
	return s.applyBatchJSON(ops, deletes)
}

func (s *PebbleStore) Lookup(ctx context.Context, parent InodeID, name string) (*InodeMeta, error) {
	if s.closed.Load() {
		return nil, ErrServiceClosed
	}

	nsKey := fmt.Sprintf("%s%d/%s", prefixNS, parent, name)
	var entry DirEntry
	exists, err := s.getJSON(nsKey, &entry)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, ErrEntryNotFound
	}

	var meta InodeMeta
	exists, err = s.getJSON(fmt.Sprintf("%s%d", prefixInode, entry.InodeID), &meta)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, ErrInodeNotFound
	}
	return &meta, nil
}

func (s *PebbleStore) GetInode(ctx context.Context, id InodeID) (*InodeMeta, error) {
	if s.closed.Load() {
		return nil, ErrServiceClosed
	}
	var meta InodeMeta
	exists, err := s.getJSON(fmt.Sprintf("%s%d", prefixInode, id), &meta)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, ErrInodeNotFound
	}
	return &meta, nil
}

func (s *PebbleStore) UpdateInode(ctx context.Context, meta *InodeMeta) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}
	if meta == nil {
		return ErrInvalidArgument
	}
	meta.CTime = time.Now().UnixNano()
	return s.putJSON(fmt.Sprintf("%s%d", prefixInode, meta.ID), meta)
}

func (s *PebbleStore) Rename(ctx context.Context, oldParent InodeID, oldName string, newParent InodeID, newName string) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}

	oldNSKey := fmt.Sprintf("%s%d/%s", prefixNS, oldParent, oldName)
	var entry DirEntry
	exists, err := s.getJSON(oldNSKey, &entry)
	if err != nil {
		return err
	}
	if !exists {
		return ErrEntryNotFound
	}

	// Check destination
	newNSKey := fmt.Sprintf("%s%d/%s", prefixNS, newParent, newName)
	var destEntry DirEntry
	destExists, _ := s.getJSON(newNSKey, &destEntry)
	if destExists {
		if destEntry.Type == FileDirectory {
			return ErrEntryExists
		}
		if err := s.Unlink(ctx, newParent, newName); err != nil {
			return err
		}
	}

	entry.Name = newName
	ops := []batchJSONOp{
		{Key: newNSKey, Value: &entry},
	}
	deletes := []string{oldNSKey}
	return s.applyBatchJSON(ops, deletes)
}

func (s *PebbleStore) Symlink(ctx context.Context, parent InodeID, name string, target string) (*InodeMeta, error) {
	if s.closed.Load() {
		return nil, ErrServiceClosed
	}

	nsKey := fmt.Sprintf("%s%d/%s", prefixNS, parent, name)
	var existing DirEntry
	exists, err := s.getJSON(nsKey, &existing)
	if err != nil {
		return nil, err
	}
	if exists {
		return nil, ErrEntryExists
	}

	inodeID := s.nextInodeID()
	now := time.Now().UnixNano()
	meta := &InodeMeta{
		ID: inodeID, Type: FileSymlink, Mode: 0777, NLink: 1, Symlink: target,
		CTime: now, MTime: now, ATime: now,
	}
	entry := &DirEntry{InodeID: inodeID, Type: FileSymlink, Name: name}
	ops := []batchJSONOp{
		{Key: fmt.Sprintf("%s%d", prefixInode, inodeID), Value: meta},
		{Key: nsKey, Value: entry},
	}
	if err := s.applyBatchJSON(ops, nil); err != nil {
		return nil, err
	}
	return meta, nil
}
func (s *PebbleStore) Readlink(ctx context.Context, id InodeID) (string, error) {
	if s.closed.Load() {
		return "", ErrServiceClosed
	}
	var meta InodeMeta
	exists, err := s.getJSON(fmt.Sprintf("%s%d", prefixInode, id), &meta)
	if err != nil {
		return "", err
	}
	if !exists {
		return "", ErrInodeNotFound
	}
	if meta.Type != FileSymlink {
		return "", ErrNotSymlink
	}
	return meta.Symlink, nil
}

func (s *PebbleStore) Link(ctx context.Context, parent InodeID, name string, target InodeID) (*InodeMeta, error) {
	if s.closed.Load() {
		return nil, ErrServiceClosed
	}

	nsKey := fmt.Sprintf("%s%d/%s", prefixNS, parent, name)
	var existing DirEntry
	exists, err := s.getJSON(nsKey, &existing)
	if err != nil {
		return nil, err
	}
	if exists {
		return nil, ErrEntryExists
	}

	var meta InodeMeta
	inodeKey := fmt.Sprintf("%s%d", prefixInode, target)
	exists, err = s.getJSON(inodeKey, &meta)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, ErrInodeNotFound
	}
	if meta.Type == FileDirectory {
		return nil, ErrInvalidArgument
	}

	meta.NLink++
	meta.CTime = time.Now().UnixNano()
	entry := &DirEntry{InodeID: target, Type: meta.Type, Name: name}
	ops := []batchJSONOp{
		{Key: inodeKey, Value: &meta},
		{Key: nsKey, Value: entry},
	}
	if err := s.applyBatchJSON(ops, nil); err != nil {
		return nil, err
	}
	return &meta, nil
}
// ========== ChunkService Implementation ==========

func (s *PebbleStore) AllocateChunk(ctx context.Context, inodeID InodeID, offset int64, policy PlacementPolicy) (*ChunkMeta, error) {
	if s.closed.Load() {
		return nil, ErrServiceClosed
	}

	chunkID := s.chunkGen.Next()
	nodeIDs, err := s.placement.PlaceChunk(policy, nil)
	if err != nil {
		return nil, err
	}

	replicas := make([]ReplicaInfo, 0, len(nodeIDs))
	for _, nid := range nodeIDs {
		n, err := s.GetNode(ctx, nid)
		if err != nil {
			return nil, fmt.Errorf("allocate chunk: node %d not found: %w", nid, err)
		}
		replicas = append(replicas, ReplicaInfo{NodeID: nid, Addr: n.Addr, State: ReplicaSyncing})
	}

	chunk := &ChunkMeta{
		ID:         chunkID,
		Size:       MaxChunkSize,
		State:      ChunkCreated,
		Replicas:   replicas,
		Tier:       policy.StorageTier,
		CreateTime: time.Now().UnixNano(),
	}

	// Append to inode's chunk map
	var meta InodeMeta
	inodeKey := fmt.Sprintf("%s%d", prefixInode, inodeID)
	exists, err := s.getJSON(inodeKey, &meta)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, ErrInodeNotFound
	}

	ref := ChunkRef{ID: chunkID, Offset: offset, Length: 0, Version: time.Now().UnixNano()}
	meta.ChunkMap = append(meta.ChunkMap, ref)
	meta.MTime = time.Now().UnixNano()
	ops := []batchJSONOp{
		{Key: fmt.Sprintf("%s%d", prefixChunk, chunkID), Value: chunk},
		{Key: inodeKey, Value: &meta},
	}
	if err := s.applyBatchJSON(ops, nil); err != nil {
		return nil, err
	}
	return chunk, nil
}

func (s *PebbleStore) CommitChunk(ctx context.Context, chunkID ChunkID, checksum uint32) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}
	key := fmt.Sprintf("%s%d", prefixChunk, chunkID)
	var chunk ChunkMeta
	exists, err := s.getJSON(key, &chunk)
	if err != nil {
		return err
	}
	if !exists {
		return ErrChunkNotFound
	}
	if chunk.State != ChunkCreated {
		return ErrChunkAlreadySealed
	}
	chunk.State = ChunkSealed
	chunk.Checksum = checksum
	return s.putJSON(key, &chunk)
}

func (s *PebbleStore) GetChunk(ctx context.Context, chunkID ChunkID) (*ChunkMeta, error) {
	if s.closed.Load() {
		return nil, ErrServiceClosed
	}
	var chunk ChunkMeta
	exists, err := s.getJSON(fmt.Sprintf("%s%d", prefixChunk, chunkID), &chunk)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, ErrChunkNotFound
	}
	return &chunk, nil
}

// UpdateChunk overwrites chunk metadata (e.g. to change tier or state).
func (s *PebbleStore) UpdateChunk(ctx context.Context, chunk *ChunkMeta) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}
	key := fmt.Sprintf("%s%d", prefixChunk, chunk.ID)
	// Verify chunk exists before update
	var existing ChunkMeta
	exists, err := s.getJSON(key, &existing)
	if err != nil {
		return err
	}
	if !exists {
		return ErrChunkNotFound
	}
	return s.putJSON(key, chunk)
}

func (s *PebbleStore) SealChunk(ctx context.Context, chunkID ChunkID) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}
	key := fmt.Sprintf("%s%d", prefixChunk, chunkID)
	var chunk ChunkMeta
	exists, err := s.getJSON(key, &chunk)
	if err != nil {
		return err
	}
	if !exists {
		return ErrChunkNotFound
	}
	if chunk.State == ChunkReady {
		return nil
	}
	if chunk.State != ChunkSealed {
		return ErrChunkNotSealed
	}
	chunk.State = ChunkReady
	return s.putJSON(key, &chunk)
}

func (s *PebbleStore) ListChunks(ctx context.Context, inodeID InodeID) ([]ChunkRef, error) {
	if s.closed.Load() {
		return nil, ErrServiceClosed
	}
	var meta InodeMeta
	exists, err := s.getJSON(fmt.Sprintf("%s%d", prefixInode, inodeID), &meta)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, ErrInodeNotFound
	}
	return meta.ChunkMap, nil
}

func (s *PebbleStore) DeleteChunk(ctx context.Context, chunkID ChunkID) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}
	return s.deleteKey(fmt.Sprintf("%s%d", prefixChunk, chunkID))
}

func (s *PebbleStore) ReportChunkState(ctx context.Context, nodeID NodeID, states map[ChunkID]ReplicaState) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}
	if len(states) == 0 {
		return nil
	}
	return s.batchUpdateChunkStates(nodeID, states)
}

const maxBatchOps = 1000

// batchUpdateChunkStates updates replica states for multiple chunks in a single batch.
func (s *PebbleStore) batchUpdateChunkStates(nodeID NodeID, states map[ChunkID]ReplicaState) error {
	ops := make([]batchJSONOp, 0, len(states))
	deletes := make([]string, 0)

	for chunkID, state := range states {
		key := fmt.Sprintf("%s%d", prefixChunk, chunkID)
		var chunk ChunkMeta
		exists, err := s.getJSON(key, &chunk)
		if err != nil || !exists {
			continue
		}
		updated := false
		for i := range chunk.Replicas {
			if chunk.Replicas[i].NodeID == nodeID {
				chunk.Replicas[i].State = state
				updated = true
				break
			}
		}
		if !updated {
			chunk.Replicas = append(chunk.Replicas, ReplicaInfo{
				NodeID: nodeID,
				State:  state,
			})
		}
		ops = append(ops, batchJSONOp{Key: key, Value: &chunk})

		// Flush in batches to avoid oversized Raft entries
		if len(ops) >= maxBatchOps {
			if err := s.applyBatchJSON(ops, deletes); err != nil {
				return err
			}
			ops = ops[:0]
		}
	}

	if len(ops) > 0 {
		return s.applyBatchJSON(ops, deletes)
	}
	return nil
}

// ========== ClusterService Implementation ==========

func (s *PebbleStore) RegisterNode(ctx context.Context, info *NodeInfo) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}
	key := prefixNode + fmt.Sprintf("%d", info.ID)
	var existing NodeInfo
	exists, err := s.getJSON(key, &existing)
	if err != nil {
		return err
	}
	if exists {
		return ErrNodeAlreadyExists
	}
	info.State = NodeOnline
	info.LastSeen = time.Now().UnixNano()
	if err := s.putJSON(key, info); err != nil {
		return err
	}
	s.placement.UpdateNode(info)
	return nil
}

func (s *PebbleStore) Heartbeat(ctx context.Context, nodeID NodeID, report *NodeReport) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}
	key := prefixNode + fmt.Sprintf("%d", nodeID)
	var info NodeInfo
	exists, err := s.getJSON(key, &info)
	if err != nil {
		return err
	}
	if !exists {
		return ErrNodeNotFound
	}
	info.LastSeen = time.Now().UnixNano()
	info.State = NodeOnline
	if report != nil {
		info.UsedGB = report.UsedGB
		info.ChunkCount = report.ChunkCount
		s.placement.UpdateLoad(nodeID, report.DiskIO)
	}
	if err := s.putJSON(key, &info); err != nil {
		return err
	}
	s.placement.UpdateNode(&info)
	if report != nil && len(report.ChunkStates) > 0 {
		return s.ReportChunkState(ctx, nodeID, report.ChunkStates)
	}
	return nil
}

func (s *PebbleStore) DecommissionNode(ctx context.Context, nodeID NodeID) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}
	key := prefixNode + fmt.Sprintf("%d", nodeID)
	var info NodeInfo
	exists, err := s.getJSON(key, &info)
	if err != nil {
		return err
	}
	if !exists {
		return ErrNodeNotFound
	}
	info.State = NodeDraining
	return s.putJSON(key, &info)
}

func (s *PebbleStore) ListNodes(ctx context.Context) ([]NodeInfo, error) {
	if s.closed.Load() {
		return nil, ErrServiceClosed
	}
	var nodes []NodeInfo
	err := s.scanPrefix(prefixNode, func(key, val []byte) error {
		var n NodeInfo
		if err := json.Unmarshal(val, &n); err == nil {
			nodes = append(nodes, n)
		}
		return nil
	})
	return nodes, err
}

func (s *PebbleStore) GetNode(ctx context.Context, nodeID NodeID) (*NodeInfo, error) {
	if s.closed.Load() {
		return nil, ErrServiceClosed
	}
	var info NodeInfo
	exists, err := s.getJSON(prefixNode+fmt.Sprintf("%d", nodeID), &info)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, ErrNodeNotFound
	}
	return &info, nil
}

// ========== Additional PebbleStore Methods ==========

// ScanAllChunks iterates over all chunk metadata (used by repair/rebalance).
func (s *PebbleStore) ScanAllChunks(ctx context.Context, fn func(*ChunkMeta) error) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}
	return s.scanPrefix(prefixChunk, func(key, val []byte) error {
		var chunk ChunkMeta
		if err := json.Unmarshal(val, &chunk); err != nil {
			// Log and skip corrupted entries
			log.Printf("pebble store: corrupted chunk entry at key %q: %v", key, err)
			return nil
		}
		return fn(&chunk)
	})
}

// DB returns the underlying Pebble instance (for Raft snapshot/restore).
func (s *PebbleStore) DB() *pebble.DB {
	return s.db
}

// ========== RepairService Implementation ==========

func (s *PebbleStore) GetRepairQueue(ctx context.Context) ([]RepairTask, error) {
	if s.closed.Load() {
		return nil, ErrServiceClosed
	}
	var tasks []RepairTask
	err := s.scanPrefix(prefixRepair, func(key, val []byte) error {
		var task RepairTask
		if err := json.Unmarshal(val, &task); err == nil {
			tasks = append(tasks, task)
		}
		return nil
	})
	return tasks, err
}

func (s *PebbleStore) TriggerRepair(ctx context.Context, chunkID ChunkID) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}
	key := fmt.Sprintf("%s%d", prefixRepair, chunkID)
	task := RepairTask{
		ChunkID:   chunkID,
		Reason:    "triggered",
		CreatedAt: time.Now(),
	}
	return s.putJSON(key, &task)
}

func (s *PebbleStore) RemoveRepairTask(ctx context.Context, chunkID ChunkID) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}
	key := fmt.Sprintf("%s%d", prefixRepair, chunkID)
	return s.deleteKey(key)
}

// ========== Raft-Aware Write Path ==========

// SetRaftNode configures the PebbleStore to use Raft for consensus.
// When set, all mutating operations are proposed to the Raft log first.
func (s *PebbleStore) SetRaftNode(node *RaftNode) {
	s.raft = node
}

// IsLeader returns true if this node is the Raft leader.
func (s *PebbleStore) IsLeader() bool {
	if s.raft == nil {
		return true // No Raft = always leader (single-node mode)
	}
	return s.raft.IsLeader()
}

// LeaderAddr returns the address of the current Raft leader.
func (s *PebbleStore) LeaderAddr() string {
	if s.raft == nil {
		return ""
	}
	return s.raft.LeaderAddr()
}



// applyBatchJSON commits multiple JSON-encoded key-value pairs atomically via Raft or directly.
func (s *PebbleStore) applyBatchJSON(ops []batchJSONOp, deletes []string) error {
	raftOps := make([]BatchOp, 0, len(ops)+len(deletes))
	for _, op := range ops {
		data, err := json.Marshal(op.Value)
		if err != nil {
			return fmt.Errorf("marshal batch value: %w", err)
		}
		raftOps = append(raftOps, BatchOp{Key: []byte(op.Key), Value: data})
	}
	for _, key := range deletes {
		raftOps = append(raftOps, BatchOp{Delete: true, Key: []byte(key)})
	}
	return s.applyBatchViaRaft(raftOps)
}

// applyViaRaft proposes a write operation through the Raft log.
// If Raft is not configured, the write is applied directly.
func (s *PebbleStore) applyViaRaft(op RaftLogOp, key string, value []byte) error {
	if s.raft == nil {
		if op == OpDelete {
			return s.db.Delete([]byte(key), pebble.Sync)
		}
		return s.db.Set([]byte(key), value, pebble.Sync)
	}

	entry := &RaftLogEntry{
		Op:    op,
		Key:   []byte(key),
		Value: value,
	}
	return s.raft.Apply(entry, 10*time.Second)
}

// applyBatchViaRaft proposes a batch of operations through the Raft log.
// When Raft is not configured, applies directly in a Pebble batch.
func (s *PebbleStore) applyBatchViaRaft(ops []BatchOp) error {
	if len(ops) == 0 {
		return nil
	}

	if s.raft == nil {
		batch := s.db.NewBatch()
		defer batch.Close()
		for _, op := range ops {
			if op.Delete {
				batch.Delete(op.Key, nil)
			} else {
				batch.Set(op.Key, op.Value, nil)
			}
		}
		return batch.Commit(pebble.Sync)
	}

	entry := &RaftLogEntry{
		Op:    OpBatch,
		Batch: ops,
	}
	return s.raft.Apply(entry, 10*time.Second)
}
// ========== RebalanceService Implementation ==========

func (s *PebbleStore) TriggerRebalance(ctx context.Context) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}
	nodes, err := s.ListNodes(ctx)
	if err != nil {
		return err
	}

	planner := &RebalancePlanner{}

	// Build node→chunks map for concrete chunk IDs
	chunkMap := make(map[NodeID][]ChunkID)
	s.scanPrefix(prefixChunk, func(key, val []byte) error {
		var chunk ChunkMeta
		if err := json.Unmarshal(val, &chunk); err != nil {
			return nil
		}
		for _, r := range chunk.Replicas {
			chunkMap[r.NodeID] = append(chunkMap[r.NodeID], chunk.ID)
		}
		return nil
	})

	result := planner.PlanRebalanceWithChunks(nodes, chunkMap, 0.1)
	if result.Balanced || len(result.Plans) == 0 {
		return nil
	}

	for _, plan := range result.Plans {
		if plan.ChunkID == 0 {
			continue
		}
		key := fmt.Sprintf("%s%d", prefixRepair, plan.ChunkID)
		task := RepairTask{
			ChunkID:   plan.ChunkID,
			Reason:    fmt.Sprintf("rebalance: node %d → %d", plan.SourceNode, plan.TargetNode),
			Priority:  2,
			CreatedAt: time.Now(),
		}
		if err := s.putJSON(key, &task); err != nil {
			return err
		}
	}
	return nil
}

// ChunksByNode scans all chunks and returns those with a replica on the given node.
func (s *PebbleStore) ChunksByNode(ctx context.Context, nodeID NodeID) ([]ChunkMeta, error) {
	if s.closed.Load() {
		return nil, ErrServiceClosed
	}
	var result []ChunkMeta
	err := s.scanPrefix(prefixChunk, func(key, val []byte) error {
		var chunk ChunkMeta
		if err := json.Unmarshal(val, &chunk); err != nil {
			return nil
		}
		for _, r := range chunk.Replicas {
			if r.NodeID == nodeID {
				result = append(result, chunk)
				break
			}
		}
		return nil
	})
	return result, err
}

// MigrateChunkReplica removes a replica from fromNode and adds one on toNode.
// This updates the metadata record only; actual data transfer happens via the repair queue.
func (s *PebbleStore) MigrateChunkReplica(ctx context.Context, chunkID ChunkID, fromNode, toNode NodeID) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}
	chunk, err := s.GetChunk(ctx, chunkID)
	if err != nil {
		return err
	}

	// Get target node address
	var toAddr string
	nodes, _ := s.ListNodes(ctx)
	for _, n := range nodes {
		if n.ID == toNode {
			toAddr = n.Addr
			break
		}
	}
	if toAddr == "" {
		return fmt.Errorf("target node %d not found", toNode)
	}

	// Remove old replica
	newReplicas := make([]ReplicaInfo, 0, len(chunk.Replicas))
	for _, r := range chunk.Replicas {
		if r.NodeID != fromNode {
			newReplicas = append(newReplicas, r)
		}
	}

	// Add new replica
	newReplicas = append(newReplicas, ReplicaInfo{
		NodeID: toNode,
		Addr:   toAddr,
		State:  ReplicaSyncing,
	})

	chunk.Replicas = newReplicas
	return s.UpdateChunk(ctx, chunk)
}

// ScanAllInodes iterates over all inode metadata (used by lifecycle engine).
func (s *PebbleStore) ScanAllInodes(ctx context.Context, fn func(*InodeMeta) error) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}
	return s.scanPrefix(prefixInode, func(key, val []byte) error {
		var inode InodeMeta
		if err := json.Unmarshal(val, &inode); err != nil {
			// Log and skip corrupted entries
			log.Printf("pebble store: corrupted inode entry at key %q: %v", key, err)
			return nil
		}
		return fn(&inode)
	})
}
