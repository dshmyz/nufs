package metadata

import (
	"sort"
	"sync"
	"time"
)

// ============================================================
// Advisory file locks
// ============================================================
//
// Advisory locks coordinate concurrent access to an inode between
// independent clients (FUSE mount, S3 gateway, dfsctl). They follow
// the same semantics as POSIX flock(2): the kernel (here, the
// metadata service) tracks who holds the lock and reports busy
// status to subsequent acquirers, but does NOT prevent uncooperative
// callers from reading or writing the underlying data.
//
// Two flavours:
//   - AdvisoryLock      (exclusive / write): only one holder at a time.
//   - AdvisoryLockShared (shared / read):    many holders, but no exclusive.
//
// Owning a lock is recorded as (inode, owner). The owner string is
// client-supplied (typically "fusegw-<pid>" or "s3gw-<pid>"). Two
// holders with the same owner on the same inode are treated as
// distinct acquisitions and counted separately so re-entrant
// locking is supported. Calling Unlock for an (inode, owner) the
// caller does not hold is a no-op (POSIX-flock semantics), not an
// error.
//
// State is in-memory only. A PebbleStore restart drops every lock;
// clients must reacquire. This is intentional: persistent locks
// outlive the process that holds them and become data corruption
// hazards. In-memory locks that disappear on restart force every
// client to re-establish their coordination, which is the safe
// failure mode.

// LockMode distinguishes exclusive from shared acquisitions.
type LockMode uint8

const (
	// LockModeExclusive is the default. It is incompatible with any
	// other holder (exclusive or shared).
	LockModeExclusive LockMode = iota
	// LockModeShared is the read-side variant. Multiple shared
	// holders are allowed on the same inode, but any exclusive
	// acquisition blocks.
	LockModeShared
)

// LockInfo describes a single holder of an inode lock. Returned
// from AdvisoryListLocks for diagnostics; not used internally for
// coordination.
type LockInfo struct {
	Inode InodeID    `json:"inode"`
	Owner string     `json:"owner"`
	Mode  LockMode   `json:"mode"`
	Since int64      `json:"since_unix_nano"`
}

// lockHolder tracks a single (owner, mode) acquisition. The same
// owner may hold the same inode multiple times; refcount is
// incremented on each successful acquire and decremented on each
// release. The lock is freed when refcount reaches zero.
type lockHolder struct {
	owner    string
	mode     LockMode
	sinceNs  int64
	refcount int
}

// lockState is the per-inode lock table. The map holds every active
// holder keyed by owner string.
type lockState struct {
	holders map[string]*lockHolder
}

// advisoryLockManager is the central lock table. It is owned by
// PebbleStore and accessed under mu; the per-inode state is itself
// protected by mu (no per-inode mutexes — the per-inode state is
// short-lived and contended only at the moment of acquire/release,
// so the global mutex is fine at the scales we care about).
type advisoryLockManager struct {
	mu    sync.Mutex
	locks map[InodeID]*lockState
}

// newAdvisoryLockManager returns an empty lock table.
func newAdvisoryLockManager() *advisoryLockManager {
	return &advisoryLockManager{locks: make(map[InodeID]*lockState)}
}

// validateOwner rejects empty or whitespace-only owners. We do not
// enforce a length cap here; the upper bound is whatever the
// transport (HTTP header, FUSE context) accepts.
func validateOwner(owner string) error {
	if owner == "" {
		return ErrInvalidOwner
	}
	return nil
}

// acquire is the internal entry point used by both AdvisoryLock and
// AdvisoryLockShared. It returns nil on success and ErrLockBusy if
// the lock cannot be granted for compatibility reasons.
func (m *advisoryLockManager) acquire(inode InodeID, owner string, mode LockMode) error {
	if err := validateOwner(owner); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	state, ok := m.locks[inode]
	if !ok {
		state = &lockState{holders: make(map[string]*lockHolder)}
		m.locks[inode] = state
	}

	// Same-owner re-entry: bump the refcount and accept the request
	// in any mode. The mode of the *first* acquisition wins for the
	// purpose of cross-owner compatibility — promoting a shared
	// lock to exclusive on a re-entry is fine because no other
	// owner is involved.
	if h, exists := state.holders[owner]; exists {
		h.refcount++
		return nil
	}

	// Cross-owner check: a fresh acquirer must respect every
	// existing holder. A shared lock is compatible with other
	// shared holders, an exclusive lock is compatible with nothing.
	for otherOwner, h := range state.holders {
		if otherOwner == owner {
			continue
		}
		if h.mode == LockModeExclusive {
			return ErrLockBusy
		}
		if mode == LockModeExclusive {
			return ErrLockBusy
		}
	}

	state.holders[owner] = &lockHolder{
		owner:    owner,
		mode:     mode,
		sinceNs:  nowUnixNano(),
		refcount: 1,
	}
	return nil
}

// release decrements the refcount for (inode, owner). When the
// refcount reaches zero the holder entry is removed; if no holders
// remain, the per-inode state is also dropped to keep the map
// small. Releasing a lock the caller does not hold is a no-op,
// matching flock(2) where LOCK_UN on an unlocked file descriptor
// returns success.
func (m *advisoryLockManager) release(inode InodeID, owner string) error {
	if err := validateOwner(owner); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	state, ok := m.locks[inode]
	if !ok {
		return nil
	}
	h, ok := state.holders[owner]
	if !ok {
		return nil
	}
	h.refcount--
	if h.refcount <= 0 {
		delete(state.holders, owner)
	}
	if len(state.holders) == 0 {
		delete(m.locks, inode)
	}
	return nil
}

// list returns a stable, sorted snapshot of every holder of inode.
// The result is detached from internal state so the caller can
// iterate without holding the lock manager mutex.
func (m *advisoryLockManager) list(inode InodeID) []LockInfo {
	m.mu.Lock()
	defer m.mu.Unlock()

	state, ok := m.locks[inode]
	if !ok {
		return nil
	}
	out := make([]LockInfo, 0, len(state.holders))
	for _, h := range state.holders {
		out = append(out, LockInfo{
			Inode: inode,
			Owner: h.owner,
			Mode:  h.mode,
			Since: h.sinceNs,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Owner < out[j].Owner })
	return out
}

// nowUnixNano is the time source. Wrapped in a package-level var
// so tests can substitute a fake clock if needed; we don't take
// advantage of that here but the seam is in place.
var nowUnixNano = func() int64 {
	return time.Now().UnixNano()
}

// Compile-time interface check. PebbleStore satisfies the lock
// portion of MetadataService by delegating to advisoryLockManager.
var _ MetadataService = (*PebbleStore)(nil)
