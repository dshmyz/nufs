package testutil

import (
	"fmt"
	"hash/fnv"
	"math/rand"
	"time"

	"github.com/example/dfs/datanode/storage"
)

// Op is a single operation in the deterministic reference-model
// sequence (§18.1).
type Op struct {
	Kind   OpKind
	Extent storage.ExtentID
	Gen    storage.Generation
	Size   int
	Seed   uint32
}

// OpKind enumerates reference-model operations.
type OpKind uint8

const (
	OpWrite OpKind = iota
	OpRead
	OpRangeRead
	OpOverwrite
	OpDelete
	OpSeal
	OpCompact
	OpCheckpoint
	OpCrash
	OpRecover
)

// Model is the in-memory reference state against which the engine's
// behavior is checked. It tracks the latest acknowledged generation
// per extent and its bytes, mirroring what a correct engine must
// return for reads at the last durable generation.
type Model struct {
	// live maps extent -> latest acknowledged generation -> data.
	live map[storage.ExtentID]map[storage.Generation][]byte
	// acked records which (extent, generation) ack has been returned.
	acked map[storage.ExtentID]storage.Generation
}

// NewModel returns an empty reference model.
func NewModel() *Model {
	return &Model{live: make(map[storage.ExtentID]map[storage.Generation][]byte), acked: make(map[storage.ExtentID]storage.Generation)}
}

// RandomOps generates n deterministic operations. The same seed always
// yields the same sequence, so a crash at op k is reproducible.
func RandomOps(seed int64, n int) []Op {
	r := rand.New(rand.NewSource(seed))
	ops := make([]Op, n)
	for i := range ops {
		extent := storage.ExtentID(1 + r.Int63n(1000))
		gen := storage.Generation(r.Int63n(8) + 1)
		kind := OpKind(r.Intn(7)) // write, read, range, overwrite, delete, seal, checkpoint
		// Force overwrites to reuse the same extent at a higher gen.
		if kind == OpOverwrite {
			gen = storage.Generation(r.Int63n(4) + 1)
			kind = OpWrite
		}
		ops[i] = Op{
			Kind:   kind,
			Extent: extent,
			Gen:    gen,
			Size:   int(r.Intn(64<<10) + 1), // up to 64 KiB payloads
			Seed:   r.Uint32(),
		}
	}
	return ops
}

// Apply updates the model according to an acknowledged write. It is
// called by the reference test when the engine returns a DurableReceipt.
func (m *Model) ApplyWrite(extent storage.ExtentID, gen storage.Generation, data []byte) {
	byGen := m.live[extent]
	if byGen == nil {
		byGen = make(map[storage.Generation][]byte)
		m.live[extent] = byGen
	}
	byGen[gen] = data
	if m.acked[extent] < gen {
		m.acked[extent] = gen
	}
}

// LatestAck returns the latest acknowledged generation for an extent.
func (m *Model) LatestAck(extent storage.ExtentID) (storage.Generation, bool) {
	g, ok := m.acked[extent]
	return g, ok
}

// LatestData returns the data of the latest acknowledged generation.
func (m *Model) LatestData(extent storage.ExtentID) ([]byte, bool) {
	g, ok := m.acked[extent]
	if !ok {
		return nil, false
	}
	byGen := m.live[extent]
	if byGen == nil {
		return nil, false
	}
	d, ok := byGen[g]
	return d, ok
}

// DeterministicData derives deterministic payload bytes for an op, so
// reads can be verified byte-for-byte.
func DeterministicData(extent storage.ExtentID, gen storage.Generation, size int, seed uint32) []byte {
	h := fnv.New64a()
	fmt.Fprintf(h, "%d/%d/%d", extent, gen, seed)
	buf := make([]byte, size)
	// Fill from a seeded stream derived from the op.
	r := rand.New(rand.NewSource(int64(h.Sum64())))
	r.Read(buf)
	return buf
}

// NowUnix returns the current unix time for fixtures.
func NowUnix() int64 { return time.Now().UnixNano() }
