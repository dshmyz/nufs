package datanode

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dshmyz/nufs/nufs-core/datanode/storage"
	"github.com/dshmyz/nufs/nufs-core/datanode/storage/maintenance"
	"github.com/dshmyz/nufs/nufs-core/datanode/storage/segment"
	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// extentLister is implemented by the V2.1 segment.Store (ListExtents).
// V2Store type-asserts each backend to it to enumerate committed extents
// for ListChunks/Stats/ChunkStateSnapshot and to rebuild the per-disk
// location/generation map at startup.
type extentLister interface {
	ListExtents() ([]segment.LiveExtent, error)
}

// diskBackend wraps one storage.Store with the per-disk accounting the
// V2Store needs (used bytes, extent count, write/error counters). The
// lister is non-nil for V2.1 segment stores; legacy backends that lack
// enumeration simply report nothing to ListChunks/Stats.
type diskBackend struct {
	store  storage.Store
	lister extentLister
	index  int
	// dir is the disk root (segment store's Config.Dir) reported to the
	// management/ops channel's DiskInfos/DiskInfo. Empty when the backend was
	// constructed without a directory (single-disk/tests).
	dir string

	// Accounting updated on the serving path and reconstructed at startup
	// for V2.1 backends via ListExtents.
	usedByts  atomic.Int64
	extCount  atomic.Int64
	readByts  atomic.Int64
	writeByts atomic.Int64

	// Rolling write-error window, consumed (Swap'ed to 0) by WriteErrorRate
	// each heartbeat — mirrors the legacy ChunkStore's rolling semantics.
	writeOps atomic.Int64
	writeErr atomic.Int64

	// failCount is a PERSISTENT consecutive-write-failure counter (never
	// reset by WriteErrorRate) used by diskFailed to flag a wedged disk.
	// It is kept separate from the rolling writeOps/writeErr window so the
	// rolling rate can reset each cycle without erasing the health signal.
	failCount atomic.Int64
}

// chunkLoc records where a chunk lives and at what generation, so a
// rewrite/read/delete/stat routes to the owning disk and generation.
type chunkLoc struct {
	disk int
	gen  storage.Generation
	// small marks a chunk whose extent lives in the small-file stream
	// (StreamID 0, ≤ SmallFileThreshold) instead of the data stream.
	small bool
}

// V2Store implements the LocalChunkStore and HeartbeatStore interfaces
// over one or more V2.1 storage.Store backends, so the V2.1 engine can
// serve real TCP read/write traffic and heartbeats with multi-disk
// parity to the legacy ChunkStore.
//
// Chunk IDs map to extents. Overwrites increment the extent generation
// (proper local generation fencing: a new payload at the same generation
// is rejected by the segment store), so a re-write of an existing chunk
// chains to gen+1 and the latest generation resolves reads/stats/deletes.
//
// Multi-disk behavior mirrors the legacy ChunkStore:
//   - a new chunk is placed on the least-used disk (fewest used bytes),
//   - an existing chunk is written back to the disk it already lives on,
//
// so overwrites stay co-located and capacity spreads across disks.
type V2Store struct {
	disks []*diskBackend
	// shards holds the EC-shard stores, one per disk (StreamID 2, class dir
	// "ecshard"), physically disjoint from the data-stream stores in disks.
	// A shard store is a plain segment store too, so each shard is a durable,
	// checksummed extent that survives a restart exactly like a data extent —
	// it is just not accounted in disks/Stats/DiskStats nor reported as a
	// replica, because a shard is a fragment, not a whole-chunk replica.
	shards []*diskBackend

	// small holds the small-file commit streams, one per data disk (parallel
	// to disks: small[i] is the StreamID 0 store on the same physical disk
	// as disks[i]). Chunks ≤ SmallFileThreshold are written here as a single
	// record (chunkLoc.small); an overwrite that outgrows the threshold
	// migrates to a data-stream disk. Empty when the node has no small
	// streams attached — the V2Store then behaves exactly as before.
	small []*diskBackend

	// caps holds one capacity guard per physical disk root, parallel to
	// disks (index i guards the disk hosting data store i and shard store
	// i). A nil entry means capacity protection is disabled for that disk
	// (unknown filesystem root / legacy single-disk without a dir). Written
	// once at construction and read on the write path, so it needs no lock.
	caps []*maintenance.CapacityGuard

	// mu guards locOf and shardDiskOf. Reconstruction at startup and serving
	// updates both mutate them; reads take RLock.
	mu    sync.RWMutex
	locOf map[metadata.ChunkID]chunkLoc
	// drainMu is the write-drain barrier (V1-b, §4 parity). Every write
	// primitive takes drainMu.RLock for its duration; QuiesceWrites takes
	// drainMu.Lock() to quiesce in-flight writes and block new ones, then
	// returns a release func that Unlock()s to resume. It is a separate
	// mutex from mu so writes are drained WITHOUT blocking concurrent reads
	// (reads take only mu.RLock, never drainMu). A drained store can keep
	// serving reads; after release it accepts writes again (non-destructive,
	// unlike close which sets closing permanently).
	drainMu sync.RWMutex
	// shardDiskOf records, per EC chunk and per shard index, which shard store
	// disk hosts that shard extent, so ReadShard/DeleteShard route to the disk
	// that wrote it. E3 spreads the 6+3 shards across distinct shard disks (disk
	// fault isolation within a node), so the map is indexed by shard index
	// rather than a single stripe-home disk per chunk.
	shardDiskOf map[metadata.ChunkID]map[int]int

	// stateVersion increments on every durable state change (write or
	// delete), so the heartbeat's incremental delta diff can detect change.
	stateVersion atomic.Uint64

	// Proactive disk-health monitor (§4, V1-c). The loop periodically probes
	// each data backend's responsiveness (a non-intrusive Stat) and drives
	// failCount upward on probe failure, so an idle or read-wedged disk
	// escalates to degraded/failed even without write traffic. Recovery is
	// write-path only: probe success never lowers failCount. diskMu guards
	// the run/stop lifecycle (Start/Stop are idempotent); diskStop signals
	// the loop to exit; diskWG joins it on Stop.
	diskStop     chan struct{}
	diskWG       sync.WaitGroup
	diskMu       sync.Mutex
	diskRun      bool
	diskInterval time.Duration // probe cadence; <=0 falls back to diskHealthDefaultInterval

	// diskFactory, when set, builds the paired data-stream and EC-shard
	// segment stores for a newly adopted disk dir (streams 1 and 2, sharing
	// the dir and change journal — the AddDisk analogue of the startup store
	// construction in runDataNodeV21). It is injected by the datanode main
	// because segment store construction needs the engine config (change
	// journal, index dir, KMS, segment size, stream IDs) that lives there,
	// not on the V2Store. nil means DiskLifecycleOps.AddDisk is unsupported.
	diskFactory func(dir string) (data, shard storage.Store, err error)
}

// SetDiskFactory installs the disk-construction callback used by
// DiskLifecycleOps.AddDisk. The datanode main wires it after building the
// startup stores so a runtime-adopted disk is constructed exactly like its
// siblings.
func (v *V2Store) SetDiskFactory(fn func(dir string) (data, shard storage.Store, err error)) {
	v.diskFactory = fn
}

// NewV2Store wraps a single backend (legacy/plain single-disk use). An
// optional dir is the disk root reported to DiskInfos when non-empty.
func NewV2Store(store storage.Store, dir ...string) *V2Store {
	v := &V2Store{locOf: make(map[metadata.ChunkID]chunkLoc), shardDiskOf: make(map[metadata.ChunkID]map[int]int)}
	for _, s := range []storage.Store{store} {
		d := ""
		if len(dir) > 0 {
			d = dir[0]
		}
		b := &diskBackend{store: s, dir: d}
		if lister, ok := s.(extentLister); ok {
			b.lister = lister
		}
		v.disks = append(v.disks, b)
		v.caps = append(v.caps, capacityForDisk(d))
	}
	return v
}

// NewMultiV2Store wraps multiple backends for JBOD multi-disk mode. dirs, if
// provided, gives each backend's disk root in the same order as stores (used
// for the management/ops DiskInfos). For V2.1 segment backends it
// reconstructs the per-disk location/generation map and usage accounting by
// enumerating each store's committed extents, the equivalent of the legacy
// ChunkStore's startup disk scan.
func NewMultiV2Store(stores []storage.Store, dirs ...string) *V2Store {
	v := &V2Store{locOf: make(map[metadata.ChunkID]chunkLoc), shardDiskOf: make(map[metadata.ChunkID]map[int]int)}
	for i, s := range stores {
		d := ""
		if i < len(dirs) {
			d = dirs[i]
		}
		b := &diskBackend{store: s, index: i, dir: d}
		if lister, ok := s.(extentLister); ok {
			b.lister = lister
			if extents, err := lister.ListExtents(); err == nil {
				for _, e := range extents {
					id := metadata.ChunkID(e.ExtentID)
					b.usedByts.Add(int64(e.Value.LogicalLen))
					b.extCount.Add(1)
					v.locOf[id] = chunkLoc{disk: i, gen: e.Generation}
				}
			}
		}
		v.disks = append(v.disks, b)
		v.caps = append(v.caps, capacityForDisk(d))
	}
	return v
}

// DataStores returns the per-disk data-stream segment stores (the same
// backing storage.Store instances passed to NewV2Store/NewMultiV2Store),
// in disk order, plus the small-file streams when attached. EC-shard stores
// are excluded. The background compaction worker drives these to reclaim
// superseded record bytes.
func (v *V2Store) DataStores() []storage.Store {
	out := make([]storage.Store, 0, len(v.disks)+len(v.small))
	for _, b := range v.disks {
		out = append(out, b.store)
	}
	for _, b := range v.small {
		out = append(out, b.store)
	}
	return out
}

// Write implements LocalChunkStore.Write. Route an overwrite to its
// owning disk at the next generation, place a new chunk on the
// least-used disk at generation 1, then durably write and update
// accounting.
//
// Small-chunk routing: a new chunk ≤ SmallFileThreshold goes to the small
// stream (when attached); an overwrite of a small chunk stays in the small
// stream while it fits and migrates to a data-stream disk when it outgrows
// the threshold.
func (v *V2Store) Write(chunkID metadata.ChunkID, data []byte) error {
	loc, ok := v.currentLoc(chunkID)
	if ok && loc.small {
		gen := loc.gen + 1
		if len(data) <= storage.SmallFileThreshold {
			return v.writeSmallTo(chunkID, loc.disk, gen, data)
		}
		return v.migrateSmallToData(chunkID, loc, gen, data)
	}
	if !ok && len(v.small) > 0 && len(data) <= storage.SmallFileThreshold {
		return v.writeSmallTo(chunkID, v.leastUsedSmall(), storage.Generation(1), data)
	}
	disk, gen := v.nextLoc(chunkID)
	return v.writeTo(chunkID, disk, gen, data)
}

// WriteGen implements LocalChunkStore.WriteGen (Metadata V2 fencing). Unlike
// Write — which derives the next generation locally (gen+1) — WriteGen writes
// under the generation the metadata service issued for this chunk. The
// metadata service is the generation authority: it hands each overwrite a new
// generation, so a stale or duplicate replica write lands on that exact
// generation and the segment store's phase-0 fencing rejects any older write
// whose payload doesn't match. This keeps all replicas of a chunk on the same
// authoritative generation instead of each datanode bumping its own counter.
func (v *V2Store) WriteGen(chunkID metadata.ChunkID, generation uint64, data []byte) error {
	gen := storage.Generation(generation)
	loc, ok := v.currentLoc(chunkID)
	if ok && loc.small {
		if len(data) <= storage.SmallFileThreshold {
			return v.writeSmallTo(chunkID, loc.disk, gen, data)
		}
		return v.migrateSmallToData(chunkID, loc, gen, data)
	}
	if !ok && len(v.small) > 0 && len(data) <= storage.SmallFileThreshold {
		return v.writeSmallTo(chunkID, v.leastUsedSmall(), gen, data)
	}
	if ok {
		// Overwrite existing chunk: keep disk locality, honor metadata gen.
		return v.writeTo(chunkID, loc.disk, gen, data)
	}
	// New chunk: least-used disk at the metadata-issued generation.
	best, _ := v.nextLoc(chunkID)
	return v.writeTo(chunkID, best, gen, data)
}

// currentLoc returns the recorded location of a chunk (ok=false when the
// chunk is unknown to this store).
func (v *V2Store) currentLoc(chunkID metadata.ChunkID) (chunkLoc, bool) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	loc, ok := v.locOf[chunkID]
	return loc, ok
}

// leastUsedSmall picks the small stream with the fewest used bytes, skipping
// streams whose paired data disk is FAILED (same physical disk). Falls back
// to 0 when every disk is failed (the failure surfaces through the health
// admission in writeSmallTo).
func (v *V2Store) leastUsedSmall() int {
	best, bestUsed := -1, int64(1<<63-1)
	for i, b := range v.small {
		if v.diskFailed(i) {
			continue
		}
		if used := b.usedByts.Load(); used < bestUsed {
			bestUsed, best = used, i
		}
	}
	if best < 0 {
		best = 0
	}
	return best
}

// writeTo durably writes data for chunkID to a specific disk under a specific
// generation, updating location and accounting. newChunk (gen==1) is inferred
// from the generation.
func (v *V2Store) writeTo(chunkID metadata.ChunkID, disk int, gen storage.Generation, data []byte) error {
	// Write barrier: hold drainMu.RLock for the whole write so DrainWrites
	// (which takes drainMu.Lock) cannot race an in-flight write. Reads do not
	// take drainMu, so draining never blocks them.
	v.drainMu.RLock()
	defer v.drainMu.RUnlock()
	if disk < 0 || disk >= len(v.disks) {
		disk = 0
	}
	// Health admission: a FAILED disk (>=5 consecutive write failures, per
	// the disk health state machine) must not accept writes — it is treated
	// as read-only, mirroring V1's CanAdmitWrite rejection. Degraded disks
	// (1..4 failures) remain eligible so a transient error doesn't permanently
	// starve the disk of the success that would clear its streak.
	if v.diskFailed(disk) {
		return fmt.Errorf("datanode: disk %d is FAILED, refusing write", disk)
	}
	// Capacity admission: reject the write before the disk fills (avoids
	// ENOSPC). No-op when capacity protection is disabled for this disk.
	if err := v.admitDiskWrite(disk, int64(len(data))); err != nil {
		return err
	}
	newChunk := gen == 1
	b := v.disks[disk]
	b.writeOps.Add(1)
	if _, err := b.store.Write(context.Background(), &storage.WriteRequest{
		ExtentID:   storage.ExtentID(chunkID),
		Generation: gen,
		Data:       data,
	}); err != nil {
		b.writeErr.Add(1)
		b.failCount.Add(1)
		return err
	}
	// A successful write ends any consecutive-failure streak.
	b.failCount.Store(0)
	b.writeByts.Add(int64(len(data)))
	if newChunk {
		b.usedByts.Add(int64(len(data)))
		b.extCount.Add(1)
	} else {
		// Overwrite: the prior generation still occupies index space; drop
		// its size so used-bytes tracks the chunk's current live size rather
		// than the sum of all generations.
		if gen > 1 {
			if old, err := b.store.Stat(context.Background(), &storage.StatRequest{
				ExtentID: storage.ExtentID(chunkID), Generation: gen - 1,
			}); err == nil {
				b.usedByts.Add(-int64(old.LogicalLen))
			}
		}
		b.usedByts.Add(int64(len(data)))
	}
	v.mu.Lock()
	v.locOf[chunkID] = chunkLoc{disk: disk, gen: gen}
	v.mu.Unlock()
	v.stateVersion.Add(1)
	return nil
}

// writeSmallTo durably writes data for chunkID to the i-th small stream under
// gen, with the same health/capacity admissions as writeTo (the small stream
// shares the physical disk with data store i) and mirrored accounting. When
// no small stream is attached it falls back to the data stream, so a mixed
// topology (some nodes with small stores) stays correct.
func (v *V2Store) writeSmallTo(chunkID metadata.ChunkID, i int, gen storage.Generation, data []byte) error {
	if len(v.small) == 0 {
		disk, g := v.nextLoc(chunkID)
		return v.writeTo(chunkID, disk, g, data)
	}
	if i < 0 || i >= len(v.small) {
		i = 0
	}
	// Write barrier: hold drainMu.RLock for the whole write so DrainWrites
	// cannot race an in-flight write (mirror writeTo).
	v.drainMu.RLock()
	defer v.drainMu.RUnlock()
	if v.diskFailed(i) {
		return fmt.Errorf("datanode: disk %d is FAILED, refusing small write", i)
	}
	if err := v.admitDiskWrite(i, int64(len(data))); err != nil {
		return err
	}
	newChunk := gen == 1
	b := v.small[i]
	b.writeOps.Add(1)
	if _, err := b.store.Write(context.Background(), &storage.WriteRequest{
		ExtentID:   storage.ExtentID(chunkID),
		Generation: gen,
		Data:       data,
	}); err != nil {
		b.writeErr.Add(1)
		b.failCount.Add(1)
		return err
	}
	b.failCount.Store(0)
	b.writeByts.Add(int64(len(data)))
	if newChunk {
		b.usedByts.Add(int64(len(data)))
		b.extCount.Add(1)
	} else {
		if gen > 1 {
			if old, err := b.store.Stat(context.Background(), &storage.StatRequest{
				ExtentID: storage.ExtentID(chunkID), Generation: gen - 1,
			}); err == nil {
				b.usedByts.Add(-int64(old.LogicalLen))
			}
		}
		b.usedByts.Add(int64(len(data)))
	}
	v.mu.Lock()
	v.locOf[chunkID] = chunkLoc{disk: i, gen: gen, small: true}
	v.mu.Unlock()
	v.stateVersion.Add(1)
	return nil
}

// migrateSmallToData moves a small-stream chunk that outgrew
// SmallFileThreshold onto a data-stream disk: the new payload is written to a
// data disk first (reads resolve through locOf immediately), then the stale
// small extent is tombstoned best-effort. A failed tombstone leaves an
// unreferenced small record that compaction reclaims; correctness never
// depends on it.
func (v *V2Store) migrateSmallToData(chunkID metadata.ChunkID, loc chunkLoc, gen storage.Generation, data []byte) error {
	best, _ := v.nextLoc(chunkID) // least-used data disk, skips FAILED disks
	if err := v.writeTo(chunkID, best, gen, data); err != nil {
		return err
	}
	// writeTo already re-pointed locOf at (best, gen) — reads now serve the
	// grown payload. Tombstone the stale small record best-effort.
	if i := loc.disk; i >= 0 && i < len(v.small) {
		b := v.small[i]
		if old, err := b.store.Stat(context.Background(), &storage.StatRequest{
			ExtentID: storage.ExtentID(chunkID), Generation: loc.gen,
		}); err == nil {
			b.usedByts.Add(-int64(old.LogicalLen))
		}
		if err := b.store.Delete(context.Background(), &storage.DeleteRequest{
			ExtentID:   storage.ExtentID(chunkID),
			Generation: loc.gen,
		}); err == nil {
			b.extCount.Add(-1)
		} else {
			slog.Warn("datanode: small extent tombstone failed after growth migration",
				"chunkID", chunkID, "error", err)
		}
	}
	return nil
}

// ============ EC shard I/O (V2.1 EC 6+3 service path, Phase E1) ============
//
// Each EC shard is stored as an independent, opaque extent in a dedicated
// shard commit stream (segment StreamID 2, on-disk class dir "ecshard") that
// is physically disjoint from the data stream (StreamID 1) — see
// segment.streamClassDir. Disjointness therefore comes from namespace
// separation, not from any property of the chunk-ID bits (a data-stream chunk
// ID can be any 64-bit value, so no bit layout could ever guarantee it never
// collides — hence the separate stream is the only sound isolation).
//
// Within a shard store the extent ID is the chunk's raw ID and the GENERATION
// carries the shard index (gen = shardIndex+1). A whole-chunk data extent that
// happens to share the same numeric ID lives in a different store, so the
// collision is impossible; and every extent in a shard store is by definition
// a shard, so reconstruction needs no marker bit. This works for any 64-bit
// chunk ID and any shard count.
//
// A shard is otherwise a first-class extent: durable, CRC32C-checksummed, and
// recoverable on restart like a data extent. Shard stores are NOT part of
// disks/Stats/DiskStats and their extents are never reported as whole-chunk
// replicas — a shard is a fragment. All shards of one chunk land on a single
// shard disk (its "stripe home"), which WriteShard picks as the least-used
// shard disk on first write and records in shardDiskOf.

// shardGen maps a shard index to its generation within the shard store (gen 0
// is reserved as the "no extent" sentinel, so shard 0 maps to gen 1).
func shardGen(shardIndex int) storage.Generation {
	return storage.Generation(shardIndex + 1)
}

// AttachShardStores wires EC-shard stores onto the V2Store, one per disk (in
// the same order as the data stores passed to NewMultiV2Store/NewV2Store).
// It reconstructs shardDiskOf by enumerating each shard store's committed
// extents — the shard-stream analogue of the data-stream startup scan — so a
// shard stripe survives a restart with reads routing to the shard store that
// holds each shard. Because a shard extent's generation encodes its shard
// index (gen = index+1), the per-shard owning disk is recovered exactly.
// Returns an error if the count does not match the data disks.
func (v *V2Store) AttachShardStores(shardStores []storage.Store) error {
	if len(shardStores) != len(v.disks) {
		return fmt.Errorf("attach shard stores: got %d stores for %d disks", len(shardStores), len(v.disks))
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	for i, s := range shardStores {
		b := &diskBackend{store: s, index: i}
		if lister, ok := s.(extentLister); ok {
			b.lister = lister
			if extents, err := lister.ListExtents(); err == nil {
				// Every extent in a shard store is a shard: its generation brings
				// its shard index, and store i is its owning disk. No marker bit
				// is needed.
				for _, e := range extents {
					idx := int(e.Generation) - 1
					cid := metadata.ChunkID(e.ExtentID)
					if v.shardDiskOf[cid] == nil {
						v.shardDiskOf[cid] = make(map[int]int)
					}
					v.shardDiskOf[cid][idx] = i
					b.usedByts.Add(int64(e.Value.LogicalLen))
					b.extCount.Add(1)
				}
			}
		}
		v.shards = append(v.shards, b)
	}
	return nil
}

// AttachSmallStores attaches the per-disk small-file commit streams
// (StreamID 0, ≤ SmallFileThreshold records). stores[i] is the small stream
// on the same physical disk as the i-th data store in disks. After
// attachment, new chunks ≤ SmallFileThreshold are routed to the small stream
// and existing small extents are enumerated into the location map, so reads,
// stats, heartbeats and the orphan GC resolve them across restarts. Without
// attachment the V2Store keeps routing everything to the data stream. Returns
// an error if the count does not match the data disks.
func (v *V2Store) AttachSmallStores(stores []storage.Store) error {
	if len(stores) > 0 && len(stores) != len(v.disks) {
		return fmt.Errorf("attach small stores: got %d stores for %d disks", len(stores), len(v.disks))
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	for i, s := range stores {
		b := &diskBackend{store: s, index: i}
		if lister, ok := s.(extentLister); ok {
			b.lister = lister
			if extents, err := lister.ListExtents(); err == nil {
				for _, e := range extents {
					v.locOf[metadata.ChunkID(e.ExtentID)] = chunkLoc{disk: i, gen: e.Generation, small: true}
					b.usedByts.Add(int64(e.Value.LogicalLen))
					b.extCount.Add(1)
				}
			}
		}
		v.small = append(v.small, b)
	}
	return nil
}

// shardDisk returns the disk hosting shard idx of chunkID, or -1 if unknown.
//
// The per-index map is authoritative during a session (every WriteShard/
// WriteShardAtDisk records it). After a restart the map is only partially
// recovered (ListExtents coalesces to one live generation per extent ID, and
// all shards of a chunk share that ID, so it cannot enumerate every shard), so
// an unknown (cid, idx) is resolved by probing each shard store for the exact
// shard generation (gen = idx+1) — at most one store holds it — and caching
// the result. This keeps restart routing correct without a new enumeration
// primitive in the storage layer.
func (v *V2Store) shardDisk(chunkID metadata.ChunkID, idx int) int {
	v.mu.RLock()
	if m, ok := v.shardDiskOf[chunkID]; ok {
		if d, ok := m[idx]; ok {
			v.mu.RUnlock()
			return d
		}
	}
	v.mu.RUnlock()

	gen := shardGen(idx)
	for d := 0; d < len(v.shards); d++ {
		if _, err := v.shards[d].store.Stat(context.Background(), &storage.StatRequest{
			ExtentID: storage.ExtentID(chunkID), Generation: gen,
		}); err == nil {
			v.mu.Lock()
			if v.shardDiskOf[chunkID] == nil {
				v.shardDiskOf[chunkID] = make(map[int]int)
			}
			v.shardDiskOf[chunkID][idx] = d
			v.mu.Unlock()
			return d
		}
	}
	return -1
}

// WriteShard durably writes one EC shard as an independent extent under the
// generation that encodes the shard index. It routes to the chunk's recorded
// shard disk for idx, or the least-used shard disk when this is the first
// write of that shard. Per-shard checksum integrity is maintained by the
// underlying CRC32C extent framing.
func (v *V2Store) WriteShard(chunkID metadata.ChunkID, shardIndex int, data []byte) error {
	if disk := v.shardDisk(chunkID, shardIndex); disk >= 0 {
		return v.writeShardAt(chunkID, shardIndex, disk, data)
	}
	return v.writeShardAt(chunkID, shardIndex, v.leastUsedShardDisk(), data)
}

// WriteShardAtDisk durably writes one EC shard extent to a specific disk
// (E3 write path: the 6+3 planner picks the owning disk, so a stripe spreads
// across distinct shard disks for §14 disk-level fault isolation).
func (v *V2Store) WriteShardAtDisk(chunkID metadata.ChunkID, shardIndex int, disk int, data []byte) error {
	if disk < 0 || disk >= len(v.shards) {
		return fmt.Errorf("write shard at disk: no shard store %d (have %d)", disk, len(v.shards))
	}
	return v.writeShardAt(chunkID, shardIndex, disk, data)
}

// WriteShardAtDiskPref durably writes one EC shard extent preferring the given
// (authoritative landing) disk, but falls back to a healthy accepting shard
// disk when the preferred one is tombstoned for that shard's generation. This
// is the cross-node counterpart to cleanShardDiskPref/RepairChunkECWithLanding:
// a repair push must not re-write a shard onto a disk whose (extent, gen) was
// tombstoned (§14 generation fence would reject it), routing instead to a
// least-used healthy disk on this node. It is the server side of the
// self-healer's cross-node restore push (handleReplicateECShard).
func (v *V2Store) WriteShardAtDiskPref(chunkID metadata.ChunkID, shardIndex, prefer int, data []byte) error {
	gen := shardGen(shardIndex)
	// Prefer the authoritative landing disk when it accepts a fresh write.
	if prefer >= 0 && prefer < len(v.shards) && v.shardAccepting(prefer, chunkID, gen) {
		return v.writeShardAt(chunkID, shardIndex, prefer, data)
	}
	// Landing disk tombstoned (or out of range): fall back to the least-used
	// healthy shard disk that accepts this generation, mirroring node-local
	// repair fallback. -1 means no disk accepted, which writeShardAt rejects.
	disk := v.cleanShardDiskPref(chunkID, shardIndex, -1)
	return v.writeShardAt(chunkID, shardIndex, disk, data)
}

// writeShardAt is the shared impl: write a shard extent on a given disk,
// record the per-shard owning disk, and update the disk's usage accounting.
func (v *V2Store) writeShardAt(chunkID metadata.ChunkID, shardIndex, disk int, data []byte) error {
	// Write barrier (shared by WriteShard/WriteShardAtDisk/WriteChunkEC) so
	// DrainWrites quiesces EC-shard writes too.
	v.drainMu.RLock()
	defer v.drainMu.RUnlock()
	if disk < 0 || disk >= len(v.shards) {
		return fmt.Errorf("write shard: no shard store attached (disk %d, have %d)", disk, len(v.shards))
	}
	b := v.shards[disk]
	// Capacity admission for the shard's physical disk (shares the root with
	// disks[disk], so the guard at caps[disk] governs both data and shards).
	if err := v.admitDiskWrite(disk, int64(len(data))); err != nil {
		return err
	}
	if _, err := b.store.Write(context.Background(), &storage.WriteRequest{
		ExtentID:   storage.ExtentID(chunkID),
		Generation: shardGen(shardIndex),
		Data:       data,
	}); err != nil {
		return err
	}
	b.usedByts.Add(int64(len(data)))
	b.extCount.Add(1)
	v.mu.Lock()
	if v.shardDiskOf[chunkID] == nil {
		v.shardDiskOf[chunkID] = make(map[int]int)
	}
	v.shardDiskOf[chunkID][shardIndex] = disk
	v.mu.Unlock()
	return nil
}

// ReadShard reads one EC shard extent back from its owning disk, byte-exact.
func (v *V2Store) ReadShard(chunkID metadata.ChunkID, shardIndex int) ([]byte, uint32, error) {
	disk := v.shardDisk(chunkID, shardIndex)
	if disk < 0 || disk >= len(v.shards) {
		return nil, 0, storage.ErrExtentNotFound
	}
	res, err := v.shards[disk].store.Read(context.Background(), &storage.ReadRequest{
		ExtentID: storage.ExtentID(chunkID), Generation: shardGen(shardIndex),
		LogicalOffset: 0, Length: 0,
	})
	if err != nil {
		return nil, 0, err
	}
	return res.Data, res.Checksum, nil
}

// ReadShardRange reads a sub-range [offset, offset+length) of one EC shard
// extent. When offset=0 and length=0 the full shard is returned (equivalent
// to ReadShard). The underlying segment store uses ReadRangeFrames which
// authenticates only the intersecting frames, bounding read amplification.
func (v *V2Store) ReadShardRange(chunkID metadata.ChunkID, shardIndex int, offset int64, length int32) ([]byte, uint32, error) {
	if offset == 0 && length == 0 {
		return v.ReadShard(chunkID, shardIndex)
	}
	disk := v.shardDisk(chunkID, shardIndex)
	if disk < 0 || disk >= len(v.shards) {
		return nil, 0, storage.ErrExtentNotFound
	}
	res, err := v.shards[disk].store.Read(context.Background(), &storage.ReadRequest{
		ExtentID:      storage.ExtentID(chunkID),
		Generation:    shardGen(shardIndex),
		LogicalOffset: offset,
		Length:        length,
	})
	if err != nil {
		return nil, 0, err
	}
	return res.Data, res.Checksum, nil
}

// DeleteShard removes one EC shard extent from its owning disk.
func (v *V2Store) DeleteShard(chunkID metadata.ChunkID, shardIndex int) error {
	disk := v.shardDisk(chunkID, shardIndex)
	if disk < 0 || disk >= len(v.shards) {
		return storage.ErrExtentNotFound
	}
	if err := v.shards[disk].store.Delete(context.Background(), &storage.DeleteRequest{
		ExtentID: storage.ExtentID(chunkID), Generation: shardGen(shardIndex),
	}); err != nil {
		return err
	}
	// Clean up the shardDiskOf mapping so deleted shard entries don't leak.
	v.mu.Lock()
	if m, ok := v.shardDiskOf[chunkID]; ok {
		delete(m, shardIndex)
		if len(m) == 0 {
			delete(v.shardDiskOf, chunkID)
		}
	}
	v.mu.Unlock()
	return nil
}

// leastUsedShardDisk returns the shard disk with the fewest bytes across its
// shard store, or -1 when no shard stores are attached. Unlike nextLoc, it
// does not consult locOf — shard placement is governed purely by shard-store
// usage.
func (v *V2Store) leastUsedShardDisk() int {
	best, bestUsed := -1, int64(1<<63-1)
	for i, d := range v.shards {
		if used := d.usedByts.Load(); i == 0 || used < bestUsed {
			bestUsed, best = used, i
		}
	}
	return best
}

// EC 6+3 aggregate write/read (V2.1 §14, Task #74 Phase E3). A written chunk
// is encoded into six data shards and three parity shards and each shard is
// stored as an independent extent spread across the node's distinct shard
// disks (disk-level fault isolation); reads aggregate all nine shards and
// decode the original. This is the single-node service path — multi-machine
// diversity and cross-node placement ride on the same WriteShardAtDisk/
// ReadShard primitive and are exercised by PlanShards + E5.

// WriteChunkEC encodes data into 6+3 and durably writes each shard to the disk
// chosen by placement[] (len 9, one disk per shard index, already validated for
// §14 diversity by the caller, e.g. ECStore.PlanShards). It returns an error if
// fewer than 9 shard stores are attached or if any shard write fails (partial
// writes are reclaimable orphans, §14).
func (v *V2Store) WriteChunkEC(chunkID metadata.ChunkID, data []byte, placement []int) error {
	if len(placement) != ec63Shards {
		return fmt.Errorf("write chunk ec: need %d shard placements, got %d", ec63Shards, len(placement))
	}
	all, err := encodeEC63(data)
	if err != nil {
		return fmt.Errorf("write chunk ec: encode: %w", err)
	}
	for idx, shard := range all {
		if err := v.WriteShardAtDisk(chunkID, idx, placement[idx], shard); err != nil {
			return fmt.Errorf("write chunk ec: shard %d: %w", idx, err)
		}
	}
	return nil
}

// ReadChunkEC reads the nine shards, reconstructs and returns the original
// (unpadded) payload of length originalLen. It is a strict full-shard read —
// every one of the nine shards must be present; a missing shard is an error,
// not a tolerated loss. Degraded reads that reconstruct from ≥6 surviving
// shards are ReadChunkECDegraded (E5).
func (v *V2Store) ReadChunkEC(chunkID metadata.ChunkID, originalLen int) ([]byte, uint32, error) {
	shards, missing, err := v.readChunkECShards(chunkID)
	if err != nil {
		return nil, 0, fmt.Errorf("read chunk ec: %w", err)
	}
	if len(missing) != 0 {
		return nil, 0, fmt.Errorf("read chunk ec: missing %d shard(s): %v", len(missing), missing)
	}
	data, err := decodeEC63(shards, originalLen)
	if err != nil {
		return nil, 0, fmt.Errorf("read chunk ec: %w", err)
	}
	return data, storage.CRC32C(data), nil
}

// readChunkECShards reads all nine shards, returning each shard's bytes (nil
// for a shard that is absent) and the list of missing shard indices. An absent
// shard is not an error here — callers decide whether the loss is tolerable.
func (v *V2Store) readChunkECShards(chunkID metadata.ChunkID) ([][]byte, []int, error) {
	shards := make([][]byte, ec63Shards)
	var missing []int
	for idx := 0; idx < ec63Shards; idx++ {
		data, _, err := v.ReadShard(chunkID, idx)
		if err != nil {
			if errors.Is(err, storage.ErrExtentNotFound) {
				missing = append(missing, idx)
				continue
			}
			return nil, nil, fmt.Errorf("shard %d: %w", idx, err)
		}
		shards[idx] = data
	}
	return shards, missing, nil
}

// ReadChunkECDegraded reads a 6+3 stripe tolerating up to three lost shards:
// it reconstructs the original payload from any ≥6 surviving shards and
// returns it byte-exact with the recomputed checksum, plus the indices of the
// missing shards. This is §14's degraded read — a read must still succeed
// (verifying the original extent checksum) while the stripe is under repair.
// With fewer than six shards present it returns the reconstruct error.
func (v *V2Store) ReadChunkECDegraded(chunkID metadata.ChunkID, originalLen int) ([]byte, uint32, []int, error) {
	shards, missing, err := v.readChunkECShards(chunkID)
	if err != nil {
		return nil, 0, nil, fmt.Errorf("read chunk ec degraded: %w", err)
	}
	available := ec63Shards - len(missing)
	if available < ec63Data {
		return nil, 0, missing, fmt.Errorf("read chunk ec degraded: only %d/%d shards present, need %d", available, ec63Shards, ec63Data)
	}
	data, err := decodeEC63(shards, originalLen)
	if err != nil {
		return nil, 0, missing, fmt.Errorf("read chunk ec degraded: %w", err)
	}
	return data, storage.CRC32C(data), missing, nil
}

// RepairChunkEC rebuilds any lost shards of a 6+3 stripe from the surviving
// ones and writes them onto healthy shard disks, restoring the full nine
// shards (§14 repair). A lost shard cannot be re-written back onto the store
// that lost it: deleting a shard tombstoned it at its generation (gen = idx+1
// is fixed by the E1 shard-extent design), and the generation fence rejects
// re-writing a tombstoned (extent, gen) in place — so the rebuild lands on the
// least-used shard disk that does not already hold the shard's generation,
// i.e. a healthy replacement location. It returns the number of shards rebuilt.
// With fewer than six shards present, or if any rebuild write fails, it returns
// an error and leaves the stripe degraded (no partial commit past survivors).
// It is the least-used-fallback form of RepairChunkECWithLanding.
func (v *V2Store) RepairChunkEC(chunkID metadata.ChunkID, originalLen int) (int, error) {
	return v.RepairChunkECWithLanding(chunkID, originalLen, nil)
}

// RepairChunkECWithLanding rebuilds lost shards of a 6+3 stripe like
// RepairChunkEC, but prefers to land each restored shard back onto its
// authoritative landing disk from the durable ECStripe (Program 5/F3, §14):
// landing[i].DiskID records which disk shard i originally landed on, so a
// repair that rebuilds i onto that same disk keeps the placement intact rather
// than relocating it to a least-used disk. The authoritative disk is only
// preferred when it can still accept a fresh write at the shard's generation —
// if it holds a tombstone or live record there (e.g. the shard was lost by
// deleting it off that disk), the rebuild falls back to the least-used healthy
// target, exactly as RepairChunkEC would. landing nil preserves the plain
// least-used behavior (V1 / no authority). DiskID encodes the node-local shard
// store index as DiskID % 1000, matching the ECSingleton resolveDisk mapping.
func (v *V2Store) RepairChunkECWithLanding(chunkID metadata.ChunkID, originalLen int, landing []metadata.ECShard) (int, error) {
	shards, missing, err := v.readChunkECShards(chunkID)
	if err != nil {
		return 0, fmt.Errorf("repair chunk ec: %w", err)
	}
	if len(missing) == 0 {
		return 0, nil // nothing to repair
	}
	// originalLen is advisory for reconstruction: a directly-written EC chunk's
	// ChunkMeta.Size (MaxChunkSize) exceeds the true padded length, so clamp to
	// the shard-derived length — which also happens to be the byte-exact result
	// the rebuilt shards decode to (§14). A legitimate length (converted chunk,
	// tests) <= paddedLen is kept.
	if originalLen <= 0 || originalLen > ec63PaddedLen(shards) {
		originalLen = ec63PaddedLen(shards)
	}
	rebuilt, _, err := reconstructEC63(shards, originalLen)
	if err != nil {
		return 0, fmt.Errorf("repair chunk ec: %w", err)
	}
	for _, idx := range missing {
		prefer := -1
		if idx < len(landing) {
			// Prefer the authoritative landing disk for this shard index
			// (Program 5 durable ECStripe), translating the cluster DiskID to
			// this node's local shard-store index (DiskID % 1000).
			prefer = int(landing[idx].DiskID % 1000)
		}
		disk := v.cleanShardDiskPref(chunkID, idx, prefer)
		if disk < 0 {
			return 0, fmt.Errorf("repair chunk ec: no healthy shard disk for shard %d", idx)
		}
		if err := v.WriteShardAtDisk(chunkID, idx, disk, rebuilt[idx]); err != nil {
			return 0, fmt.Errorf("repair chunk ec: restore shard %d: %w", idx, err)
		}
	}
	return len(missing), nil
}

// cleanShardDisk returns the least-used shard disk whose store can accept a
// fresh write of shard idx of chunkID — one that holds no record (neither live
// nor tombstoned) at the shard's generation (gen = idx+1). A store already
// holding that generation would reject the write via the generation fence
// (ErrStaleGeneration for a live value, or a tombstone that hides the extent).
// Returns -1 when no shard disk is a healthy target (e.g. every disk already
// holds the shard, so there is nothing to repair onto).
func (v *V2Store) cleanShardDisk(chunkID metadata.ChunkID, idx int) int {
	return v.cleanShardDiskPref(chunkID, idx, -1)
}

// cleanShardDiskPref is cleanShardDisk with an authoritative-disk preference
// (F3, §14): when prefer is a valid local shard-store index that can accept a
// fresh write at the shard's generation, it is chosen outright — rebuilding
// the lost shard back onto its originally-landed disk keeps the placement
// intact. When prefer is out of range, already holds the generation (its shard
// was deleted off it, so it tombstoned the generation), or no landing was
// supplied (-1), it falls back to the least-used healthy target.
func (v *V2Store) cleanShardDiskPref(chunkID metadata.ChunkID, idx, prefer int) int {
	gen := shardGen(idx)
	if prefer >= 0 && prefer < len(v.shards) && v.shardAccepting(prefer, chunkID, gen) {
		return prefer
	}
	best, bestUsed := -1, int64(1<<63-1)
	for d := 0; d < len(v.shards); d++ {
		if !v.shardAccepting(d, chunkID, gen) {
			continue
		}
		if used := v.shards[d].usedByts.Load(); used < bestUsed {
			bestUsed, best = used, d
		}
	}
	return best
}

// shardAccepting reports whether shard disk d accepts a fresh write at gen:
// its store holds no record (neither live nor tombstoned) for that
// (extent, generation), so the write would not trip the generation fence.
func (v *V2Store) shardAccepting(d int, chunkID metadata.ChunkID, gen storage.Generation) bool {
	_, err := v.shards[d].store.Stat(context.Background(), &storage.StatRequest{
		ExtentID: storage.ExtentID(chunkID), Generation: gen,
	})
	return err == storage.ErrExtentNotFound
}

// ReheatChunkEC reconstructs a full 6+3 stripe onto a clean shard disk (a
// replacement node/disk joining the stripe, §14 reheat): it rebuilds the
// complete nine-shard set from whatever ≥6 survive and writes every shard to
// the replacement disk. The target must be a shard disk that holds no record
// of these shards — a disk that lost the stripe has its shards tombstoned at
// their generations, and the generation fence would reject re-writing them in
// place (§14 gen fencing) — so the caller points reheat at a fresh store.
// Returns the number of shards written to the replacement disk.
func (v *V2Store) ReheatChunkEC(chunkID metadata.ChunkID, originalLen int, newDisk int) (int, error) {
	if newDisk < 0 || newDisk >= len(v.shards) {
		return 0, fmt.Errorf("reheat chunk ec: no shard store %d (have %d)", newDisk, len(v.shards))
	}
	shards, _, err := v.readChunkECShards(chunkID)
	if err != nil {
		return 0, fmt.Errorf("reheat chunk ec: %w", err)
	}
	rebuilt, _, err := reconstructEC63(shards, originalLen)
	if err != nil {
		return 0, fmt.Errorf("reheat chunk ec: %w", err)
	}
	written := 0
	for idx, shard := range rebuilt {
		if err := v.WriteShardAtDisk(chunkID, idx, newDisk, shard); err != nil {
			return 0, fmt.Errorf("reheat chunk ec: write shard %d: %w", idx, err)
		}
		written++
	}
	return written, nil
}

// nextLoc returns the owning disk and next generation for a write: the
// existing location bumped to gen+1 for an overwrite, or (least-used
// disk, 1) for a new chunk.
func (v *V2Store) nextLoc(chunkID metadata.ChunkID) (int, storage.Generation) {
	v.mu.RLock()
	loc, ok := v.locOf[chunkID]
	v.mu.RUnlock()
	if ok {
		return loc.disk, loc.gen + 1
	}

	best, bestUsed := -1, int64(1<<63-1)
	for i, d := range v.disks {
		// Skip FAILED disks (>=5 consecutive write failures) so a wedged disk
		// never receives new chunks — closing the placement gap where nextLoc's
		// least-used loop (unlike leastUsedDisk) failed to skip bad disks.
		if v.diskFailed(i) {
			continue
		}
		if used := d.usedByts.Load(); used < bestUsed {
			bestUsed, best = used, i
		}
	}
	if best < 0 {
		// Every disk is failed; fall back to disk 0 so the write surfaces the
		// failure through writeTo's health admission rather than panicking.
		best = 0
	}
	return best, 1
}

// RebalanceOne moves a single chunk from fromDisk to toDisk at the same
// generation (PUT-at-target: the extent is written under its existing
// generation, so the segment store places it as a fresh extent on the
// target). It reads the full payload from the source store, writes it to
// the target store (a different backend instance, so the same
// extentID+generation is a distinct extent there), verifies, then
// atomically re-points locOf and adjusts both disks' usage accounting.
//
// Concurrency: the store read/write happen WITHOUT the lock so concurrent
// Reads are not blocked for the duration of a large move. The locOf
// re-point is a guarded CAS — it only flips if the chunk still lives at
// (fromDisk, gen); if a concurrent Write/WriteGen/Delete already moved or
// removed the chunk, the orphaned target extent (reclaimed by compaction)
// is left unreferenced and accounting is untouched, so the winner's
// authoritative location and generation are never clobbered.
//
// toDisk==fromDisk, an unknown chunk, a missing source, or a move onto a
// failed disk returns an error without changing state.
func (v *V2Store) RebalanceOne(chunkID metadata.ChunkID, fromDisk, toDisk int) error {
	return v.rebalanceOne(chunkID, fromDisk, toDisk, false)
}

// rebalanceOne is the shared move primitive behind RebalanceOne (live load
// balancing, keep the source copy) and MigrateDisk (decommission pre-step,
// remove the source copy). removeSource tombstones the source extent once the
// move is committed so migrate truly drains the disk instead of leaving a
// byte-for-byte duplicate behind.
func (v *V2Store) rebalanceOne(chunkID metadata.ChunkID, fromDisk, toDisk int, removeSource bool) error {
	// Rebalance issues a write to the target disk, so hold the write barrier
	// for the whole move: a DrainWrites running concurrently must not observe
	// a half-moved extent. Reads (which take only mu.RLock) are unaffected.
	v.drainMu.RLock()
	defer v.drainMu.RUnlock()
	if fromDisk == toDisk {
		return fmt.Errorf("rebalance: fromDisk == toDisk (%d)", fromDisk)
	}
	if toDisk < 0 || toDisk >= len(v.disks) || fromDisk < 0 || fromDisk >= len(v.disks) {
		return fmt.Errorf("rebalance: disk index out of range (from=%d to=%d)", fromDisk, toDisk)
	}
	if v.disks[toDisk].failCount.Load() >= 5 {
		return fmt.Errorf("rebalance: target disk %d is failed", toDisk)
	}
	v.mu.RLock()
	loc, ok := v.locOf[chunkID]
	v.mu.RUnlock()
	if !ok {
		return fmt.Errorf("%w: chunk %d", storage.ErrExtentNotFound, chunkID)
	}
	if loc.disk != fromDisk {
		return fmt.Errorf("rebalance: chunk %d lives on disk %d, not %d", chunkID, loc.disk, fromDisk)
	}
	gen := loc.gen

	src := v.disks[fromDisk]
	dst := v.disks[toDisk]

	res, err := src.store.Read(context.Background(), &storage.ReadRequest{
		ExtentID:   storage.ExtentID(chunkID),
		Generation: gen,
	})
	if err != nil {
		return err
	}

	dst.writeOps.Add(1)
	if _, err := dst.store.Write(context.Background(), &storage.WriteRequest{
		ExtentID:   storage.ExtentID(chunkID),
		Generation: gen,
		Data:       res.Data,
	}); err != nil {
		dst.writeErr.Add(1)
		dst.failCount.Add(1)
		return err
	}
	dst.failCount.Store(0)
	dst.writeByts.Add(int64(len(res.Data)))

	// CAS re-point: only if the chunk still lives at (fromDisk, gen). A
	// concurrent overwrite/delete that won the race leaves locOf pointing at
	// the authoritative location and generation.
	v.mu.Lock()
	cur, still := v.locOf[chunkID]
	if !still || cur.disk != fromDisk || cur.gen != gen {
		v.mu.Unlock()
		return fmt.Errorf("rebalance: chunk %d moved concurrently during rebalance", chunkID)
	}
	v.locOf[chunkID] = chunkLoc{disk: toDisk, gen: gen}
	v.mu.Unlock()

	// For a migrate (removeSource), tombstone the source extent now that locOf
	// is authoritative at the target, so the source disk is physically drained
	// (decommission pre-step). The delete is at the store level — not v.Delete,
	// which would follow locOf to the target — and defers to the accounting
	// below (src loses the extent, dst gains it) so the counters move, not
	// double-count. A failed tombstone is best-effort: the chunk already reads
	// authoritatively from the target, and compaction reclaims the residue.
	if removeSource {
		_ = src.store.Delete(context.Background(), &storage.DeleteRequest{
			ExtentID:   storage.ExtentID(chunkID),
			Generation: gen,
		})
	}

	src.usedByts.Add(-int64(len(res.Data)))
	src.extCount.Add(-1)
	dst.usedByts.Add(int64(len(res.Data)))
	dst.extCount.Add(1)
	v.stateVersion.Add(1)
	return nil
}

// RebalanceBalanced drains the fullest disk onto the least-used healthy disk
// until the used-byte gap between them is at most thresholdBytes. It returns
// the number of extents moved.
//
// Termination and convergence: a single pass fixes the drain target (the
// least-used healthy disk) and only moves an extent that fits strictly within
// the current gap (len < srcUsed - dstUsed), so a move never overshoots into a
// reversed imbalance that a later move would thrash back. The gap therefore
// shrinks monotonically each move (no oscillation), and a second call is
// idempotent (returns 0 once balanced). Rebooting on a failed disk never
// happens — failed disks are excluded from both source and target. The loop
// is additionally bounded so it always returns.
func (v *V2Store) RebalanceBalanced(thresholdBytes int64) (int, error) {
	moved := 0
	// Cap worst-case iterations well above the number of movable extents so
	// progress is guaranteed even under adversarial interleavings.
	cap := 0
	for _, b := range v.disks {
		if b.lister != nil {
			if extents, err := b.lister.ListExtents(); err == nil {
				cap += len(extents)
			}
		}
	}
	if cap == 0 {
		cap = 64
	}
	excluded := make(map[metadata.ChunkID]bool) // chunks that failed to move this pass
	for i := 0; i < cap; i++ {
		src, chunkID, ok := v.pickRebalanceChunk(thresholdBytes, excluded)
		if !ok {
			break
		}
		dst := v.leastUsedDisk(-1)
		if err := v.RebalanceOne(chunkID, src, dst); err != nil {
			// Not movable in this pass; exclude it so we make progress instead
			// of re-picking the same chunk forever.
			excluded[chunkID] = true
			continue
		}
		moved++
	}
	return moved, nil
}

// pickRebalanceChunk chooses the single chunk to move next: from the fullest
// healthy disk (≠ the least-used target), the largest extent that fits
// strictly within the current used-byte gap. This is the move that releases
// the most capacity per accounting bump without overshooting. Returns ok=false
// when no move would strictly improve balance.
func (v *V2Store) pickRebalanceChunk(thresholdBytes int64, excluded map[metadata.ChunkID]bool) (src int, chunkID metadata.ChunkID, ok bool) {
	dst := v.leastUsedDisk(-1)
	if dst < 0 {
		return 0, 0, false
	}
	dstUsed := v.disks[dst].usedByts.Load()

	// Pick the fullest healthy source disk that is beyond threshold and has a
	// movable extent; greedy on the fullest disk keeps the driver converging.
	bestSrc, bestGap := -1, int64(thresholdBytes)
	for i, b := range v.disks {
		if i == dst || b.failCount.Load() >= 5 {
			continue
		}
		if gap := b.usedByts.Load() - dstUsed; gap > bestGap {
			bestGap, bestSrc = gap, i
		}
	}
	if bestSrc < 0 {
		return 0, 0, false
	}

	// Largest extent on the source that fits strictly within its gap — never
	// overshooting into a reversed imbalance.
	bestChunk, bestLen := metadata.ChunkID(0), int64(-1)
	if l := v.disks[bestSrc].lister; l != nil {
		srcUsed := v.disks[bestSrc].usedByts.Load()
		if extents, err := l.ListExtents(); err == nil {
			for _, e := range extents {
				id := metadata.ChunkID(e.ExtentID)
				if e.Generation == 0 || excluded[id] {
					continue
				}
				n := int64(e.Value.LogicalLen)
				// Strict fit within the current gap (no overshoot).
				if n < srcUsed-dstUsed && n > bestLen {
					bestLen, bestChunk = n, id
				}
			}
		}
	}
	if bestLen < 0 {
		return 0, 0, false
	}
	return bestSrc, bestChunk, true
}

// leastUsedDisk returns the index of the healthy disk with the fewest used
// bytes, skipping failed disks and the excluded index (use exclude=-1 for
// none). Returns -1 when no healthy disk qualifies.
func (v *V2Store) leastUsedDisk(exclude int) int {
	best, bestUsed := -1, int64(1<<63-1)
	for i, d := range v.disks {
		if i == exclude || v.diskFailed(i) {
			continue
		}
		if used := d.usedByts.Load(); used < bestUsed {
			bestUsed, best = used, i
		}
	}
	return best
}

// ============ Disk lifecycle (DiskLifecycleOps — V2.1 parity) ============
//
// V2.1 advertising, retiring, decommissioning, and migrating a disk, the
// V2Store analogue of the legacy ChunkStore's DiskLifecycleOps. The V2Store
// builds its migration on the existing RebalanceOne primitive and marks a
// removed disk failed (preserving its index slot, like V1), so reads to a
// chunk that still claims the old slot keep routing and the disk is excluded
// from all further placement.

// MigrateDisk moves every chunk extent on disk srcIdx onto a healthy target
// disk at the same generation, removing the source copy (PUT-at-target +
// tombstone the source, via rebalanceOne with removeSource), returning the
// number of extents migrated. It is the decommission pre-step: drain the
// source disk's data before retiring it. Chunks that cannot move (e.g. the
// target is full/failed — guarded inside rebalanceOne) are skipped and
// counted nowhere; the caller decides whether to proceed with retire on top
// of the residual. On success the source disk is physically emptied.
func (v *V2Store) MigrateDisk(srcIdx int) (int, error) {
	if srcIdx < 0 || srcIdx >= len(v.disks) {
		return 0, fmt.Errorf("migrate disk: index %d out of range (have %d)", srcIdx, len(v.disks))
	}
	if len(v.disks) <= 1 {
		return 0, fmt.Errorf("migrate disk: cannot migrate the only disk")
	}
	// Snapshot the source disk's extents before moving; a concurrent write to a
	// chunk here is a non-issue — RebalanceOne CAS-guards the locOf re-point, so
	// whichever wins the race is authoritative.
	var ids []metadata.ChunkID
	if l := v.disks[srcIdx].lister; l != nil {
		extents, err := l.ListExtents()
		if err != nil {
			return 0, fmt.Errorf("migrate disk: enumerate disk %d: %w", srcIdx, err)
		}
		for _, e := range extents {
			if e.Generation == 0 {
				continue
			}
			ids = append(ids, metadata.ChunkID(e.ExtentID))
		}
	}
	migrated := 0
	for _, id := range ids {
		dst := v.leastUsedDisk(srcIdx)
		if dst < 0 {
			break // no healthy target remains; stop the drain
		}
		if err := v.rebalanceOne(id, srcIdx, dst, true); err == nil {
			migrated++
		}
	}
	return migrated, nil
}

// RemoveDisk retires disk idx: it marks the disk and its paired shard backend
// FAILED (consecutive-failure counter pinned at the failure threshold) while
// preserving its index slot, so no new chunk or shard is placed on it and the
// existing helper guards (diskFailed, leastUsedDisk, nextLoc, RebalanceOne)
// all skip it. The disk's already-stored data is left in place — call
// MigrateDisk first to drain it, or accept it as a lost replica (survivor
// repair / anti-entropy will rebuild from healthy peers).
//
// Parity with ChunkStore.RemoveDisk: the slot is never removed from the
// slices, so location maps and reads that reference it keep resolving.
func (v *V2Store) RemoveDisk(idx int) error {
	if idx < 0 || idx >= len(v.disks) {
		return fmt.Errorf("remove disk: index %d out of range (have %d)", idx, len(v.disks))
	}
	if len(v.disks) <= 1 {
		return fmt.Errorf("remove disk: cannot remove the only disk")
	}
	v.disks[idx].failCount.Store(diskHealthFailedThreshold)
	if idx < len(v.shards) {
		v.shards[idx].failCount.Store(diskHealthFailedThreshold)
	}
	v.stateVersion.Add(1)
	return nil
}

// AddDisk adopts a new data dir by asking the injected disk factory to build
// its paired data-stream and EC-shard segment stores, then appends both (plus
// a capacity guard) to the V2Store and enumerates the new data/shards into
// the location maps. Returns the new disk's index.
//
// Without a configured diskFactory (SetDiskFactory never called) AddDisk is
// unsupported and returns an error — the single-disk/test construction path
// has no way to build an engine backend for an arbitrary new dir.
func (v *V2Store) AddDisk(dir string, maxWrites, maxReads int) (int, error) {
	if v.diskFactory == nil {
		return 0, fmt.Errorf("add disk: not configured (no disk factory); disk lifecycle unsupported by this engine")
	}
	// Re-adopting the same dir a previous retire already claimed? The retired
	// backend is still holding its on-disk index lock (the slot is preserved),
	// so a fresh engine store for the same dir would fail to acquire it. Tear
	// that engine down first to release the lock (and to forget the tombstoned
	// extents), then rebuild below. This makes retire → re-adopt the same dir a
	// reversible round-trip.
	if idx := v.retiredDiskIndexFor(dir); idx >= 0 {
		closeStore(v.disks[idx].store)
		if idx < len(v.shards) {
			closeStore(v.shards[idx].store)
		}
	}
	data, shard, err := v.diskFactory(dir)
	if err != nil {
		return 0, fmt.Errorf("add disk: build engine store for %s: %w", dir, err)
	}
	// Enumerate the new stores off-lock so a partially-built disk never leaks
	// into the serving slices; append everything under one lock at the end.
	n := len(v.disks)
	dataB := &diskBackend{store: data, index: n, dir: dir}
	var newLocs map[metadata.ChunkID]chunkLoc
	if l, ok := data.(extentLister); ok {
		dataB.lister = l
		if extents, err := l.ListExtents(); err != nil {
			closeStore(data)
			closeStore(shard)
			return 0, fmt.Errorf("add disk: enumerate data store for %s: %w", dir, err)
		} else {
			newLocs = make(map[metadata.ChunkID]chunkLoc, len(extents))
			for _, e := range extents {
				id := metadata.ChunkID(e.ExtentID)
				dataB.usedByts.Add(int64(e.Value.LogicalLen))
				dataB.extCount.Add(1)
				newLocs[id] = chunkLoc{disk: n, gen: e.Generation}
			}
		}
	}
	shardB := &diskBackend{store: shard, index: n}
	var newShards map[metadata.ChunkID]map[int]int
	if l, ok := shard.(extentLister); ok {
		shardB.lister = l
		if extents, err := l.ListExtents(); err != nil {
			closeStore(data)
			closeStore(shard)
			return 0, fmt.Errorf("add disk: enumerate shard store for %s: %w", dir, err)
		} else {
			newShards = make(map[metadata.ChunkID]map[int]int, len(extents))
			for _, e := range extents {
				idx := int(e.Generation) - 1
				cid := metadata.ChunkID(e.ExtentID)
				if newShards[cid] == nil {
					newShards[cid] = make(map[int]int)
				}
				newShards[cid][idx] = n
				shardB.usedByts.Add(int64(e.Value.LogicalLen))
				shardB.extCount.Add(1)
			}
		}
	}

	v.mu.Lock()
	v.disks = append(v.disks, dataB)
	v.shards = append(v.shards, shardB)
	v.caps = append(v.caps, capacityForDisk(dir))
	for id, loc := range newLocs {
		v.locOf[id] = loc
	}
	for cid, m := range newShards {
		if v.shardDiskOf[cid] == nil {
			v.shardDiskOf[cid] = make(map[int]int)
		}
		for idx, d := range m {
			v.shardDiskOf[cid][idx] = d
		}
	}
	v.mu.Unlock()
	v.stateVersion.Add(1)
	return n, nil
}

// closeStore best-effort closes a newly-created engine store on AddDisk
// rollback. Most V2.1 stores expose Close.
func closeStore(s storage.Store) {
	if c, ok := s.(interface{ Close() error }); ok {
		_ = c.Close()
	}
}

// retiredDiskIndexFor returns the index of a FAILED (retired) data backend
// whose dir matches, or -1. Used by AddDisk to release a previously-retired
// disk's on-disk lock before re-adopting the same dir. Reads v.disks under the
// reader lock so a concurrent lifecycle operation can't race the slot scan.
func (v *V2Store) retiredDiskIndexFor(dir string) int {
	v.mu.RLock()
	defer v.mu.RUnlock()
	for i, b := range v.disks {
		if b.dir == dir && v.diskFailed(i) {
			return i
		}
	}
	return -1
}

// loc returns the location of a chunk, defaulting to the first disk at
// generation 1 when unknown (mirrors the legacy chunkstore.diskOf
// fallback).
func (v *V2Store) loc(chunkID metadata.ChunkID) chunkLoc {
	v.mu.RLock()
	loc, ok := v.locOf[chunkID]
	v.mu.RUnlock()
	if !ok {
		loc = chunkLoc{disk: 0, gen: 1}
	}
	if loc.disk < 0 || loc.disk >= len(v.disks) {
		loc.disk = 0
	}
	return loc
}

// Read implements LocalChunkStore.Read.
func (v *V2Store) Read(chunkID metadata.ChunkID, offset int64, length int32) ([]byte, uint32, error) {
	loc := v.loc(chunkID)
	b := v.backendAt(loc)
	res, err := b.store.Read(context.Background(), &storage.ReadRequest{
		ExtentID:      storage.ExtentID(chunkID),
		Generation:    loc.gen,
		LogicalOffset: offset,
		Length:        length,
	})
	if err != nil {
		return nil, 0, err
	}
	b.readByts.Add(int64(len(res.Data)))
	return res.Data, res.Checksum, nil
}

// backendAt returns the diskBackend owning a chunk's extent: the small
// stream when the chunk lives there, otherwise the data stream.
func (v *V2Store) backendAt(loc chunkLoc) *diskBackend {
	if loc.small && len(v.small) > 0 {
		if loc.disk >= 0 && loc.disk < len(v.small) {
			return v.small[loc.disk]
		}
	}
	if loc.disk < 0 || loc.disk >= len(v.disks) {
		loc.disk = 0
	}
	return v.disks[loc.disk]
}

// Delete implements LocalChunkStore.Delete.
func (v *V2Store) Delete(chunkID metadata.ChunkID) error {
	loc := v.loc(chunkID)
	b := v.backendAt(loc)
	// Capture the live size before the fenced delete so used-bytes reflects
	// the freed space (a post-delete stat would return the tombstone).
	if ext, err := b.store.Stat(context.Background(), &storage.StatRequest{
		ExtentID: storage.ExtentID(chunkID), Generation: loc.gen,
	}); err == nil {
		b.usedByts.Add(-int64(ext.LogicalLen))
	}
	if err := b.store.Delete(context.Background(), &storage.DeleteRequest{
		ExtentID:   storage.ExtentID(chunkID),
		Generation: loc.gen,
	}); err != nil {
		return err
	}
	b.extCount.Add(-1)
	v.mu.Lock()
	delete(v.locOf, chunkID)
	delete(v.shardDiskOf, chunkID)
	v.mu.Unlock()
	v.stateVersion.Add(1)
	return nil
}

// Seal is a no-op for V2 — extents are sealed atomically through the
// commit log. Returns 0, nil.
func (v *V2Store) Seal(chunkID metadata.ChunkID) (uint32, error) {
	// V2 extents are committed atomically; no separate seal needed.
	return 0, nil
}

// Info returns basic info for a chunk, resolving size/checksum from the
// owning backend's stat.
func (v *V2Store) Info(chunkID metadata.ChunkID) (*LocalChunkInfo, bool) {
	loc := v.loc(chunkID)
	res, err := v.backendAt(loc).store.Stat(context.Background(), &storage.StatRequest{
		ExtentID:   storage.ExtentID(chunkID),
		Generation: loc.gen,
	})
	if err != nil {
		return nil, false
	}
	state := LocalSealed
	if res.State == storage.ExtentCorrupt {
		state = LocalCorrupt
	}
	return &LocalChunkInfo{
		ChunkID:     chunkID,
		Size:        int64(res.LogicalLen),
		Checksum:    res.Checksum,
		State:       state,
		WrittenAt:   time.Now(),
		LastAccess:  time.Now(),
		AccessCount: 0,
		DiskIndex:   loc.disk,
	}, true
}

// ListChunks returns all locally stored chunk information by enumerating
// every backend's committed extents (data stream + small stream).
func (v *V2Store) ListChunks() []LocalChunkInfo {
	var out []LocalChunkInfo
	for _, b := range append(append([]*diskBackend{}, v.disks...), v.small...) {
		if b.lister == nil {
			continue
		}
		extents, err := b.lister.ListExtents()
		if err != nil {
			continue
		}
		for _, e := range extents {
			state := LocalSealed
			if e.Value.State == storage.ExtentCorrupt {
				state = LocalCorrupt
			}
			out = append(out, LocalChunkInfo{
				ChunkID:   metadata.ChunkID(e.ExtentID),
				Size:      int64(e.Value.LogicalLen),
				Checksum:  e.Value.Checksum,
				State:     state,
				WrittenAt: time.Now(),
				DiskIndex: b.index,
			})
		}
	}
	return out
}

// Stats returns aggregate storage statistics across all disks.
func (v *V2Store) Stats() (totalBytes int64, chunkCount int64) {
	for _, b := range append(append([]*diskBackend{}, v.disks...), v.small...) {
		totalBytes += b.usedByts.Load()
		chunkCount += b.extCount.Load()
	}
	return totalBytes, chunkCount
}

// StateVersion returns a counter incremented on every durable chunk state
// change, so heartbeat can skip rebuilding the replica-state set when it
// is unchanged.
func (v *V2Store) StateVersion() uint64 {
	return v.stateVersion.Load()
}

// ChunkStateSnapshot returns the current replica-state view of every
// locally stored chunk, mapped to the ReplicaState heartbeat reports.
func (v *V2Store) ChunkStateSnapshot() map[metadata.ChunkID]metadata.ReplicaState {
	out := make(map[metadata.ChunkID]metadata.ReplicaState)
	for _, b := range append(append([]*diskBackend{}, v.disks...), v.small...) {
		if b.lister == nil {
			continue
		}
		extents, err := b.lister.ListExtents()
		if err != nil {
			continue
		}
		for _, e := range extents {
			st := metadata.ReplicaReady
			if e.Value.State == storage.ExtentCorrupt {
				st = metadata.ReplicaFailed
			}
			out[metadata.ChunkID(e.ExtentID)] = st
		}
	}
	return out
}

// DiskStats returns per-disk usage and health, used by the heartbeat
// reporter and the management interface.
func (v *V2Store) DiskStats() []DiskStatsItem {
	out := make([]DiskStatsItem, len(v.disks))
	for i, b := range v.disks {
		out[i] = DiskStatsItem{
			Index:       i,
			UsedBytes:   b.usedByts.Load(),
			TotalBytes:  detectCapacityBytes(b.dir),
			OnDiskBytes: dataFilesOnDiskBytes(b.dir, ".seg"),
			ChunkCount:  b.extCount.Load(),
			Failed:      v.diskFailed(i),
			State:       v.diskState(i),
		}
	}
	return out
}

// diskState derives the 3-tier health of a backend from its consecutive
// write-failure count, upgrading the crude diskFailed boolean into the V1
// DiskState model (disk health state machine, Program 4 V1-c):
//
//	failCount == 0              -> DiskOnline  (healthy)
//	failCount 1..4              -> DiskDegraded (I/O errors below threshold)
//	failCount >= 5              -> DiskFailed   (read-only, excluded from placement)
//
// Uses failCount (a persistent consecutive-failure counter, cleared by any
// success), not the rolling write-error window, so WriteErrorRate's per-cycle
// reset does not erase the health signal.
func (v *V2Store) diskState(i int) DiskState {
	switch fc := v.disks[i].failCount.Load(); {
	case fc >= 5:
		return DiskFailed
	case fc >= 1:
		return DiskDegraded
	default:
		return DiskOnline
	}
}

// diskFailed reports whether a backend is in the DiskFailed tier. It is the
// boolean gate used by placement and write admission: only a FAILED disk is
// excluded, so a merely-degraded disk (which can still clear its streak on a
// success) stays eligible rather than being permanently starved of writes.
func (v *V2Store) diskFailed(i int) bool {
	return v.diskState(i) == DiskFailed
}

// diskHealthDefaultInterval is how often the proactive monitor probes each
// data backend. It is behind StartDiskMonitor, so an operator can leave it
// off; when on, a responsive disk is never penalized (see probeDisk).
const diskHealthDefaultInterval = 30 * time.Second

// diskHealthFailedThreshold is the consecutive write-failure count at which a
// backend is considered FAILED (read-only, excluded from placement) per the
// disk health state machine. RemoveDisk pins a retired disk's counter here.
// This is the same value the drive-failure state (probeDisk, diskState) uses;
// it is a named constant here so the retire path reads its intent.
const diskHealthFailedThreshold int64 = 5

// StartDiskMonitor begins the proactive per-disk I/O health probe loop
// (§4 V1-c). On each tick it probes every DATA backend (v.disks; EC shard
// stores have their own self-heal path and are intentionally not probed
// here) and escalates failCount on real I/O failure.
//
// This is the "monitor-only + write-path recovery" policy the user selected:
//   - probe failure -> failCount++ -> diskState pushes the disk to
//     degraded/failed, so a read-wedged or idle disk that never gets writes
//     still escalates;
//   - probe success -> NOTHING (failCount is never lowered); only a real
//     write via writeTo (failCount.Store(0)) recovers a disk.
//
// StartDiskMonitor is idempotent: a second call while running is a no-op. The
// loop exits on ctx.Done() or StopDiskMonitor. If v.diskInterval is zero the
// default 30s cadence is used (tests can shrink it to drive ticks quickly).
func (v *V2Store) StartDiskMonitor(ctx context.Context) {
	v.diskMu.Lock()
	defer v.diskMu.Unlock()
	if v.diskRun {
		return
	}
	v.diskRun = true
	stopCh := make(chan struct{})
	v.diskStop = stopCh

	interval := v.diskInterval
	if interval <= 0 {
		interval = diskHealthDefaultInterval
	}
	ticker := time.NewTicker(interval)
	v.diskWG.Add(1)
	go func(stop <-chan struct{}) {
		defer v.diskWG.Done()
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-stop:
				return
			case <-ticker.C:
				for i := range v.disks {
					v.probeDisk(i)
				}
			}
		}
	}(stopCh)
}

// StopDiskMonitor stops the proactive probe loop and joins it. Safe to call
// repeatedly; returns without blocking if the monitor was never started.
func (v *V2Store) StopDiskMonitor() {
	v.diskMu.Lock()
	if !v.diskRun {
		v.diskMu.Unlock()
		return
	}
	v.diskRun = false
	stopCh := v.diskStop
	v.diskStop = nil
	v.diskMu.Unlock()

	if stopCh != nil {
		close(stopCh)
	}
	v.diskWG.Wait()
}

// probeDisk performs a non-intrusive responsiveness check on one data
// backend. It treats err==nil and storage.ErrExtentNotFound as RESPONSIVE
// (the probe extent never exists, so a healthy store returns
// ErrExtentNotFound — a responsive, non-error signal); any other error is a
// real I/O failure and increments failCount, driving diskState toward
// degraded/failed. probeDisk never decrements failCount — recovery is
// write-path only (writeTo success -> Store(0)).
func (v *V2Store) probeDisk(i int) {
	b := v.disks[i]
	probe := &storage.StatRequest{ExtentID: storage.ExtentID(0x7fffffffffffffff), Generation: 1}
	_, err := b.store.Stat(context.Background(), probe)
	if err == nil {
		return
	}
	if errors.Is(err, storage.ErrExtentNotFound) {
		return
	}
	b.failCount.Add(1)
}

// WriteErrorRate returns the aggregate write error rate (0.0-1.0) across
// all disks, as a rolling window since the last call. Like the legacy
// ChunkStore, the per-disk write-op/error counters are reset (Swap'd to 0)
// so each heartbeat reports the rate within its own cycle rather than a
// lifetime cumulative ratio. Disk health (diskFailed) is unaffected — it
// uses its own persistent failCount.
func (v *V2Store) WriteErrorRate() float64 {
	var ops, errs int64
	for _, b := range v.disks {
		ops += b.writeOps.Swap(0)
		errs += b.writeErr.Swap(0)
	}
	if ops == 0 {
		return 0
	}
	return float64(errs) / float64(ops)
}

// DrainWrites is the V2.1 drain surface (DrainOps parity with the legacy
// ChunkStore). It delegates to the internal QuiesceWrites barrier: block
// until every in-flight write has completed, then block all new writes;
// the returned release func resumes writes. Reads are never blocked, so a
// drained V2Store keeps serving reads. The ops and management /drain
// channels acquire this barrier to quiesce writes before a rolling restart.
func (v *V2Store) DrainWrites(ctx context.Context) (func(), error) {
	return v.QuiesceWrites(ctx)
}

// QuiesceWrites is the V2.1-internal write-drain barrier: it blocks until
// every in-flight write (writeTo/writeShardAt/RebalanceOne — all hold
// drainMu.RLock for their duration) has completed, then blocks all new
// writes. The returned release func resumes writes; reads are never blocked
// (they do not take drainMu), so a drained store continues serving reads. A
// ctx timeout returns ctx.Err with no release func and no barrier held.
//
// This is the graceful-shutdown path called by runDataNodeV21 before closing
// the stores. DrainWrites exposes it to the management/ops channel (DrainOps
// parity); QuiesceWrites remains the internal name used by the shutdown path.
func (v *V2Store) QuiesceWrites(ctx context.Context) (func(), error) {
	acquired := make(chan struct{})
	// release is idempotent so the success path and the timeout self-heal can
	// both call it without risk of a double-unlock.
	var once sync.Once
	release := func() { once.Do(func() { v.drainMu.Unlock() }) }

	// Acquire the barrier in a goroutine: drainMu.Lock blocks until every
	// in-flight write (which holds drainMu.RLock) completes, then blocks new
	// writes until release is called.
	go func() {
		v.drainMu.Lock()
		close(acquired)
	}()

	select {
	case <-acquired:
		// Barrier held exclusively by this goroutine until the caller
		// invokes release.
		return release, nil
	case <-ctx.Done():
		// Deadline elapsed before we could hand the barrier back. Do NOT
		// block the caller on acquired — the caller may itself hold a write
		// (read lock) that the acquire goroutine is waiting on, so waiting
		// here would deadlock. Instead spawn a self-heal goroutine that
		// releases the barrier the moment it is eventually acquired, so the
		// store never stays permanently drained. The caller gets the error.
		go func() {
			<-acquired
			release()
		}()
		return nil, ctx.Err()
	}
}

// ReadWriteBytes returns the cumulative bytes read and written on the
// serving path since startup, across all disks. The heartbeat samples
// these to compute a live DiskIO utilization — something the legacy
// ChunkStore's DiskManager counters never feed (RecordRead/RecordWrite
// have no serving-path callers), so V2.1 produces a real metric.
func (v *V2Store) ReadWriteBytes() (read int64, write int64) {
	for _, b := range v.disks {
		read += b.readByts.Load()
		write += b.writeByts.Load()
	}
	return read, write
}

// DiskInfos returns per-disk metadata for the management/ops channel,
// mirroring the legacy ChunkStore.DiskInfos. Each V2.1 disk is one segment
// store; Index/Dir/UsedBytes/ChunkCount/Failed derive from its accounting and
// OnDiskBytes from the actual .seg physical footprint (so an admin can show
// logical live vs physical on-disk side by side).
func (v *V2Store) DiskInfos() []DiskInfo {
	out := make([]DiskInfo, len(v.disks))
	for i, b := range v.disks {
		out[i] = DiskInfo{
			Index:       i,
			Dir:         b.dir,
			UsedBytes:   b.usedByts.Load(),
			OnDiskBytes: dataFilesOnDiskBytes(b.dir, ".seg"),
			ChunkCount:  b.extCount.Load(),
			Failed:      v.diskFailed(i),
			State:       v.diskState(i),
		}
	}
	return out
}

// VerifyChunkData re-reads a chunk and reports whether its on-disk content
// still matches the recorded checksum, mirroring the legacy ha.go Verify
// semantics. Size is resolved from the owning backend's stat, so a full
// re-read verifies every byte.
func (v *V2Store) VerifyChunkData(chunkID metadata.ChunkID) (bool, uint32, error) {
	info, ok := v.Info(chunkID)
	if !ok {
		return false, 0, fmt.Errorf("chunk %d not found locally", chunkID)
	}
	data, _, err := v.Read(chunkID, 0, int32(info.Size))
	if err != nil {
		return false, 0, err
	}
	// The V2.1 segment store records extent payload checksums as CRC32C
	// (Castagnoli), so the integrity comparison must match that, not IEEE.
	got := storage.CRC32C(data)
	return got == info.Checksum, got, nil
}

// PerfSnapshot returns engine performance counters. The legacy V1 metrics
// (fsync count/timing, semaphore waits) are chunk-file concerns V2.1 does
// not have — its equivalents live in ReadWriteBytes/DiskStats — so this
// reports an honest zero snapshot.
func (v *V2Store) PerfSnapshot() ChunkStorePerfSnapshot {
	return ChunkStorePerfSnapshot{}
}

// Compile-time interface checks.
var _ LocalChunkStore = (*V2Store)(nil)
var _ HeartbeatStore = (*V2Store)(nil)
