package index

import (
	"encoding/binary"
	"fmt"

	"github.com/dshmyz/nufs/nufs-core/datanode/storage"
)

// The local extent index is a Pebble database mapping an extent key to
// a fixed-size value describing where the record lives on disk (§5.4).
//
// Key:  extent_id (8) + generation (8)
// Value: segment_id (8) + offset (8) + stored_len (4) + logical_len (4)
//        + state (1) + checksum (4) + padding (1)  = 30 bytes
//
// The key carries the generation so reads and fencing are exact: a
// lookup of (extent_id, generation) returns only that generation's
// record, and an overwrite of an older generation writes a new key
// rather than mutating in place. Compaction and GC resolve the live
// generation via a latest-generation query.

// KeyLen is the encoded extent index key size.
const KeyLen = 16

// ValueLen is the encoded extent index value size.
const ValueLen = 30

// Key encodes an (extent_id, generation) lookup key.
func Key(extentID storage.ExtentID, generation storage.Generation) []byte {
	k := make([]byte, KeyLen)
	binary.BigEndian.PutUint64(k[0:8], uint64(extentID))
	binary.BigEndian.PutUint64(k[8:16], uint64(generation))
	return k
}

// ExtentFromKey extracts the extent ID from a key.
func ExtentFromKey(k []byte) storage.ExtentID {
	return storage.ExtentID(binary.BigEndian.Uint64(k[0:8]))
}

// GenerationFromKey extracts the generation from a key.
func GenerationFromKey(k []byte) storage.Generation {
	return storage.Generation(binary.BigEndian.Uint64(k[8:16]))
}

// Value is the fixed-size index value.
type Value struct {
	SegmentID  storage.SegmentID
	Offset     int64
	StoredLen  uint32
	LogicalLen uint32
	State      storage.ExtentState
	Checksum   uint32
}

// Encode writes the value bytes.
func (v *Value) Encode(dst []byte) error {
	if len(dst) < ValueLen {
		return fmt.Errorf("storage: index value buffer too small")
	}
	binary.BigEndian.PutUint64(dst[0:8], uint64(v.SegmentID))
	binary.BigEndian.PutUint64(dst[8:16], uint64(v.Offset))
	binary.BigEndian.PutUint32(dst[16:20], v.StoredLen)
	binary.BigEndian.PutUint32(dst[20:24], v.LogicalLen)
	dst[24] = byte(v.State)
	binary.BigEndian.PutUint32(dst[25:29], v.Checksum)
	dst[29] = 0
	return nil
}

// Decode parses value bytes.
func (v *Value) Decode(src []byte) error {
	if len(src) < ValueLen {
		return fmt.Errorf("storage: index value too short: %d < %d", len(src), ValueLen)
	}
	v.SegmentID = storage.SegmentID(binary.BigEndian.Uint64(src[0:8]))
	v.Offset = int64(binary.BigEndian.Uint64(src[8:16]))
	v.StoredLen = binary.BigEndian.Uint32(src[16:20])
	v.LogicalLen = binary.BigEndian.Uint32(src[20:24])
	v.State = storage.ExtentState(src[24])
	v.Checksum = binary.BigEndian.Uint32(src[25:29])
	return nil
}
