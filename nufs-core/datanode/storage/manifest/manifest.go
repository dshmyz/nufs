package manifest

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"

	"github.com/dshmyz/nufs/nufs-core/datanode/storage"
)

// Superblock is the first fixed-size container on a disk (§5.2). It
// carries disk and cluster identity so a disk with a mismatched
// cluster or unsupported format is rejected rather than adopted.
type Superblock struct {
	Magic         uint32 // SuperblockMagic
	FormatVersion uint8  // storage.FormatVersion
	ClusterID     uint64
	DiskID        uint64
	NodeID        uint64
	CreatedAtUnix int64
	CRC           uint32
}

const superblockSize = 4 + 1 + 8 + 8 + 8 + 8 + 4 // 41

// Encode writes the superblock bytes.
func (s *Superblock) Encode(dst []byte) error {
	if len(dst) < superblockSize {
		return fmt.Errorf("storage: superblock buffer too small")
	}
	binary.BigEndian.PutUint32(dst[0:4], s.Magic)
	dst[4] = s.FormatVersion
	binary.BigEndian.PutUint64(dst[5:13], s.ClusterID)
	binary.BigEndian.PutUint64(dst[13:21], s.DiskID)
	binary.BigEndian.PutUint64(dst[21:29], s.NodeID)
	binary.BigEndian.PutUint64(dst[29:37], uint64(s.CreatedAtUnix))
	crc := crc32.ChecksumIEEE(dst[0:37])
	binary.BigEndian.PutUint32(dst[37:41], crc)
	return nil
}

// Decode parses and validates a superblock.
func (s *Superblock) Decode(src []byte) error {
	if len(src) < superblockSize {
		return fmt.Errorf("storage: superblock too short")
	}
	s.Magic = binary.BigEndian.Uint32(src[0:4])
	s.FormatVersion = src[4]
	s.ClusterID = binary.BigEndian.Uint64(src[5:13])
	s.DiskID = binary.BigEndian.Uint64(src[13:21])
	s.NodeID = binary.BigEndian.Uint64(src[21:29])
	s.CreatedAtUnix = int64(binary.BigEndian.Uint64(src[29:37]))
	want := binary.BigEndian.Uint32(src[37:41])
	got := crc32.ChecksumIEEE(src[0:37])
	if s.Magic != storage.SuperblockMagic {
		return fmt.Errorf("storage: bad superblock magic 0x%x", s.Magic)
	}
	if got != want {
		return fmt.Errorf("storage: superblock crc mismatch")
	}
	return nil
}

// LoadSuperblock reads and decodes the superblock at dir/superblock.
func LoadSuperblock(dir string) (*Superblock, error) {
	data, err := os.ReadFile(filepath.Join(dir, "superblock"))
	if err != nil {
		return nil, err
	}
	var sb Superblock
	if err := sb.Decode(data); err != nil {
		return nil, err
	}
	return &sb, nil
}

// WriteSuperblock atomically writes a superblock to dir/superblock.
func WriteSuperblock(dir string, sb *Superblock) error {
	buf := make([]byte, superblockSize)
	if err := sb.Encode(buf); err != nil {
		return err
	}
	path := filepath.Join(dir, "superblock")
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, buf, 0644); err != nil {
		return err
	}
	f, err := os.Open(tmp)
	if err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	f.Close()
	return os.Rename(tmp, path)
}

// Validate returns an error if the loaded superblock is incompatible
// with the expected cluster/disk identity.
func (s *Superblock) Validate(expectedCluster, expectedNode uint64) error {
	if s.FormatVersion != storage.FormatVersion {
		return fmt.Errorf("storage: unsupported format version %d (want %d)", s.FormatVersion, storage.FormatVersion)
	}
	if expectedCluster != 0 && s.ClusterID != expectedCluster {
		return fmt.Errorf("storage: cluster id mismatch: disk=%d expected=%d", s.ClusterID, expectedCluster)
	}
	if expectedNode != 0 && s.NodeID != expectedNode {
		return fmt.Errorf("storage: node id mismatch: disk=%d expected=%d", s.NodeID, expectedNode)
	}
	return nil
}

// ErrNoManifest is returned when CURRENT points at no manifest yet
// (fresh disk before first seal).
var ErrNoManifest = errors.New("storage: no manifest published")

// ManifestVersion is the schema version of a manifest.
const ManifestVersion = 1

// SegmentRecord describes one sealed segment as recorded in a manifest.
// Active segments are not in the manifest; their safe offsets live in
// the checkpoint (§7.3).
type SegmentRecord struct {
	ID        storage.SegmentID
	Class     storage.SegmentClass
	Path      string // relative path under segments/
	SizeBytes int64
	SealedAt  int64
	RecordCount uint64
}

// Manifest is an immutable, complete snapshot of all sealed segments on
// a disk at a point in time.
type Manifest struct {
	Version    uint8 // ManifestVersion
	Generation uint64
	Segments   []SegmentRecord
	PrevGeneration uint64 // 0 if none
}

const manifestHeaderSize = 4 + 1 + 8 + 8 + 8 + 4 // 33

// ManifestMagic distinguishes manifest containers.
const ManifestMagic = 0x4D414E49 // "MANI"

// Encode serializes the manifest (header + segment records + checksum).
// The segment records are length-prefixed for forward-compatible reads.
func (m *Manifest) Encode(dst []byte) error {
	if len(dst) < manifestHeaderSize {
		return fmt.Errorf("storage: manifest buffer too small")
	}
	binary.BigEndian.PutUint32(dst[0:4], ManifestMagic)
	dst[4] = m.Version
	binary.BigEndian.PutUint64(dst[5:13], m.Generation)
	binary.BigEndian.PutUint64(dst[13:21], m.PrevGeneration)
	binary.BigEndian.PutUint64(dst[21:29], uint64(len(m.Segments)))
	// CRC over [0,29) is written last.
	off := manifestHeaderSize
	for _, seg := range m.Segments {
		if off+8+1+8+2+8+8 > len(dst) {
			return fmt.Errorf("storage: manifest buffer too small for segments")
		}
		// Layout per record: id(8) class(1) size(8) name_len(2) name sealed_at(8) record_count(8)
		binary.BigEndian.PutUint64(dst[off:off+8], uint64(seg.ID))
		dst[off+8] = byte(seg.Class)
		binary.BigEndian.PutUint64(dst[off+9:off+17], uint64(seg.SizeBytes))
		binary.BigEndian.PutUint16(dst[off+17:off+19], uint16(len(seg.Path)))
		copy(dst[off+19:off+19+len(seg.Path)], seg.Path)
		off += 19 + len(seg.Path)
		binary.BigEndian.PutUint64(dst[off:off+8], uint64(seg.SealedAt))
		off += 8
		binary.BigEndian.PutUint64(dst[off:off+8], seg.RecordCount)
		off += 8
	}
	crc := crc32.ChecksumIEEE(dst[0:off])
	if off+4 > len(dst) {
		return fmt.Errorf("storage: manifest buffer too small for crc")
	}
	binary.BigEndian.PutUint32(dst[off:off+4], crc)
	return nil
}

// EncodeAlloc returns the exact byte size Encode will write.
func (m *Manifest) EncodeAlloc() int {
	size := manifestHeaderSize
	for _, seg := range m.Segments {
		size += 19 + len(seg.Path) + 16
	}
	return size + 4
}

// Decode parses and validates a manifest.
func (m *Manifest) Decode(src []byte) error {
	if len(src) < manifestHeaderSize {
		return fmt.Errorf("storage: manifest too short")
	}
	if binary.BigEndian.Uint32(src[0:4]) != ManifestMagic {
		return fmt.Errorf("storage: bad manifest magic")
	}
	m.Version = src[4]
	if m.Version != ManifestVersion {
		return fmt.Errorf("storage: unsupported manifest version %d", m.Version)
	}
	m.Generation = binary.BigEndian.Uint64(src[5:13])
	m.PrevGeneration = binary.BigEndian.Uint64(src[13:21])
	count := binary.BigEndian.Uint64(src[21:29])
	off := manifestHeaderSize
	m.Segments = m.Segments[:0]
	for i := uint64(0); i < count; i++ {
		if off+19 > len(src) {
			return fmt.Errorf("storage: manifest truncated in record %d", i)
		}
		var seg SegmentRecord
		seg.ID = storage.SegmentID(binary.BigEndian.Uint64(src[off : off+8]))
		seg.Class = storage.SegmentClass(src[off+8])
		seg.SizeBytes = int64(binary.BigEndian.Uint64(src[off+9 : off+17]))
		nameLen := int(binary.BigEndian.Uint16(src[off+17 : off+19]))
		off += 19
		if off+nameLen+16 > len(src) {
			return fmt.Errorf("storage: manifest truncated in name of record %d", i)
		}
		seg.Path = string(src[off : off+nameLen])
		off += nameLen
		seg.SealedAt = int64(binary.BigEndian.Uint64(src[off : off+8]))
		off += 8
		seg.RecordCount = binary.BigEndian.Uint64(src[off : off+8])
		off += 8
		m.Segments = append(m.Segments, seg)
	}
	if off+4 > len(src) {
		return fmt.Errorf("storage: manifest missing crc")
	}
	want := binary.BigEndian.Uint32(src[off : off+4])
	got := crc32.ChecksumIEEE(src[0:off])
	if got != want {
		return fmt.Errorf("storage: manifest crc mismatch")
	}
	return nil
}

// Current is the pointer file that atomically selects the active
// manifest (§7.2). It contains the manifest generation number.
type Current struct {
	Generation uint64
}

const currentSize = 8

// Encode writes CURRENT bytes.
func (c *Current) Encode(dst []byte) error {
	if len(dst) < currentSize {
		return fmt.Errorf("storage: current buffer too small")
	}
	binary.BigEndian.PutUint64(dst[0:8], c.Generation)
	return nil
}

// Decode parses CURRENT bytes.
func (c *Current) Decode(src []byte) error {
	if len(src) < currentSize {
		return fmt.Errorf("storage: current file too short")
	}
	c.Generation = binary.BigEndian.Uint64(src[0:8])
	return nil
}

// manifestPath returns the on-disk path for a manifest generation.
func manifestPath(dir string, generation uint64) string {
	return filepath.Join(dir, "manifests", fmt.Sprintf("MANIFEST-%d", generation))
}

// currentPath returns the CURRENT pointer path.
func currentPath(dir string) string {
	return filepath.Join(dir, "manifests", "CURRENT")
}

// Publish atomically writes manifest + CURRENT (CURRENT rename last).
// Readers always see either the old generation or the new one, never a
// torn combination.
func Publish(dir string, m *Manifest) error {
	if err := os.MkdirAll(filepath.Join(dir, "manifests"), 0755); err != nil {
		return err
	}
	buf := make([]byte, m.EncodeAlloc())
	if err := m.Encode(buf); err != nil {
		return err
	}
	path := manifestPath(dir, m.Generation)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, buf, 0644); err != nil {
		return err
	}
	f, err := os.Open(tmp)
	if err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	f.Close()
	if err := os.Rename(tmp, path); err != nil {
		return err
	}

	var cur Current
	cur.Generation = m.Generation
	cbuf := make([]byte, currentSize)
	if err := cur.Encode(cbuf); err != nil {
		return err
	}
	cp := currentPath(dir)
	ctmp := cp + ".tmp"
	if err := os.WriteFile(ctmp, cbuf, 0644); err != nil {
		return err
	}
	if err := syncDir(dir); err != nil {
		return err
	}
	return os.Rename(ctmp, cp)
}

// Load reads the CURRENT pointer and the manifest it selects. Returns
// ErrNoManifest if CURRENT does not exist (fresh disk).
func Load(dir string) (*Manifest, *Current, error) {
	data, err := os.ReadFile(currentPath(dir))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil, ErrNoManifest
		}
		return nil, nil, err
	}
	var cur Current
	if err := cur.Decode(data); err != nil {
		return nil, nil, fmt.Errorf("storage: decode CURRENT: %w", err)
	}
	md, err := os.ReadFile(manifestPath(dir, cur.Generation))
	if err != nil {
		return nil, nil, err
	}
	var m Manifest
	if err := m.Decode(md); err != nil {
		return nil, nil, err
	}
	return &m, &cur, nil
}

// syncDir fsyncs a directory so the rename is durable.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
