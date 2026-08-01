package journal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/example/dfs/datanode/storage"
)

// ChangeEvent is an asynchronous/out-of-band state change (V2.1 §12).
// The normal write path does NOT emit EXTENT_DURABLE here — those are
// already durable receipts to metadata. This journal carries only
// async changes: corruption, disk/segment loss, relocation, async
// third-replica completion, repair-created replicas, scrub findings,
// and completed deletions.
type ChangeEvent struct {
	// Seq is the monotonically increasing durable sequence.
	Seq uint64
	// Kind identifies the event type.
	Kind ChangeEventKind
	// ExtentID/Generation identify the affected extent (0 for
	// disk/segment-level events).
	ExtentID   storage.ExtentID
	Generation storage.Generation
	// SegmentID is set for segment/disk-level events.
	SegmentID storage.SegmentID
	// Reason is a short machine-readable code (forbidden as metric
	// labels, §17).
	Reason string
	// AtUnix is the event time.
	AtUnix int64
}

// ChangeEventKind enumerates async change-journal event types (§12).
type ChangeEventKind uint8

const (
	EventCorrupt ChangeEventKind = iota
	EventDiskLost
	EventSegmentLost
	EventRelocated
	EventThirdReplicaComplete
	EventRepairCreated
	EventScrubFinding
	EventDeleteComplete
)

func (k ChangeEventKind) String() string {
	switch k {
	case EventCorrupt:
		return "corrupt"
	case EventDiskLost:
		return "disk_lost"
	case EventSegmentLost:
		return "segment_lost"
	case EventRelocated:
		return "relocated"
	case EventThirdReplicaComplete:
		return "third_replica_complete"
	case EventRepairCreated:
		return "repair_created"
	case EventScrubFinding:
		return "scrub_finding"
	case EventDeleteComplete:
		return "delete_complete"
	default:
		return "unknown"
	}
}

// ChangeJournal persists async change events (V2.1 §12). Heartbeats
// upload at most MaxEventsPerHeartbeat / MaxHeartbeatBytes, metadata
// acknowledges a sequence, and the node retains unacknowledged events
// (at least RetainMinDuration, bounded by MaxBytes).
type ChangeJournal struct {
	dir string
	mu  sync.Mutex

	seq uint64
	f   *os.File
	// fileGen is the rotation counter for log file names (independent
	// of the event sequence).
	fileGen uint64
	// ack is the highest sequence metadata has acknowledged.
	ack uint64
	// bytes tracks the current log size.
	bytes int64

	// Retention bounds.
	MaxBytes       int64
	RetainMinNs    int64
	MaxPerHeartbeat int
	MaxHeartbeatBytes int
}

// JournalOptions configures the change journal (§16 inventory section).
type JournalOptions struct {
	Dir               string
	MaxBytes          int64
	RetainMinDuration time.Duration
	MaxPerHeartbeat   int
	MaxHeartbeatBytes int
}

// OpenChangeJournal opens (creating if needed) the change journal.
func OpenChangeJournal(opts JournalOptions) (*ChangeJournal, error) {
	if err := os.MkdirAll(opts.Dir, 0755); err != nil {
		return nil, err
	}
	j := &ChangeJournal{
		dir:                opts.Dir,
		MaxBytes:           opts.MaxBytes,
		RetainMinNs:        opts.RetainMinDuration.Nanoseconds(),
		MaxPerHeartbeat:    opts.MaxPerHeartbeat,
		MaxHeartbeatBytes:  opts.MaxHeartbeatBytes,
	}
	if j.MaxBytes == 0 {
		j.MaxBytes = 8 << 30 // §16: journal_max_bytes 8GiB
	}
	if j.RetainMinNs == 0 {
		j.RetainMinNs = int64(24 * time.Hour)
	}
	if j.MaxPerHeartbeat == 0 {
		j.MaxPerHeartbeat = 10000
	}
	if j.MaxHeartbeatBytes == 0 {
		j.MaxHeartbeatBytes = 4 << 20
	}
	// Resume the sequence by scanning event contents across all files
	// (the filename carries only the rotation counter).
	files, err := listChangeFiles(opts.Dir)
	if err != nil {
		return nil, err
	}
	var maxSeq uint64
	for _, f := range files {
		for _, ev := range decodeFile(f) {
			if ev.Seq > maxSeq {
				maxSeq = ev.Seq
			}
		}
		if s := seqFromName(f); s > j.fileGen {
			j.fileGen = s
		}
	}
	j.seq = maxSeq
	if len(files) > 0 {
		// Append to the newest file.
		newest := files[len(files)-1]
		f, err := os.OpenFile(newest, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			return nil, err
		}
		j.f = f
		st, _ := f.Stat()
		if st != nil {
			j.bytes = st.Size()
		}
	} else {
		if err := j.newFile(); err != nil {
			return nil, err
		}
	}
	return j, nil
}

// Append records an async event with a new durable sequence.
func (j *ChangeJournal) Append(kind ChangeEventKind, extentID storage.ExtentID, gen storage.Generation, segID storage.SegmentID, reason string) (uint64, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.seq++
	ev := ChangeEvent{
		Seq:        j.seq,
		Kind:       kind,
		ExtentID:   extentID,
		Generation: gen,
		SegmentID:  segID,
		Reason:     reason,
		AtUnix:     time.Now().UnixNano(),
	}
	buf := encodeEvent(ev)
	if j.bytes+int64(len(buf)) > j.MaxBytes {
		// Hard bound: stop destructive async actions and force
		// reconciliation rather than unbounded disk growth (§12).
		return 0, fmt.Errorf("journal: max bytes reached (%d), force reconciliation", j.MaxBytes)
	}
	if _, err := j.f.Write(buf); err != nil {
		return 0, err
	}
	if err := j.f.Sync(); err != nil {
		return 0, err
	}
	j.bytes += int64(len(buf))
	return j.seq, nil
}

// Ack advances the acknowledged sequence.
func (j *ChangeJournal) Ack(seq uint64) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if seq > j.ack {
		j.ack = seq
	}
}

// Pending returns events with sequence > ack, bounded by the heartbeat
// limits (§12). It returns the events and the next ack to send.
func (j *ChangeJournal) Pending(maxEvents int, maxBytes int) ([]ChangeEvent, uint64) {
	j.mu.Lock()
	defer j.mu.Unlock()
	files, _ := listChangeFiles(j.dir)
	var out []ChangeEvent
	var sentBytes int
	for _, path := range files {
		evs := decodeFile(path)
		for _, ev := range evs {
			if ev.Seq <= j.ack {
				continue
			}
			if len(out) >= maxEvents || sentBytes >= maxBytes {
				return out, j.ack + uint64(len(out))
			}
			out = append(out, ev)
			sentBytes += eventSize(ev)
		}
	}
	return out, j.ack + uint64(len(out))
}

// Seq returns the current highest sequence.
func (j *ChangeJournal) Seq() uint64 {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.seq
}

// Close closes the journal.
func (j *ChangeJournal) Close() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.f != nil {
		return j.f.Close()
	}
	return nil
}

func (j *ChangeJournal) newFile() error {
	if j.f != nil {
		j.f.Close()
	}
	j.fileGen++
	path := filepath.Join(j.dir, fmt.Sprintf("change-%016d.log", j.fileGen))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	j.f = f
	j.bytes = 0
	return nil
}

// ========== encoding ==========

// encodeEvent serializes a ChangeEvent. Format:
// len(4) + kind(1) + seq(8) + extent(8) + gen(8) + seg(8) + at(8)
// + reason_len(2) + reason + crc(4)
func encodeEvent(ev ChangeEvent) []byte {
	reason := []byte(ev.Reason)
	total := 4 + 1 + 8 + 8 + 8 + 8 + 8 + 2 + len(reason) + 4
	buf := make([]byte, total)
	body := buf[:4+1+8+8+8+8+8+2+len(reason)]
	binary.BigEndian.PutUint32(body[0:4], uint32(len(body)))
	body[4] = byte(ev.Kind)
	binary.BigEndian.PutUint64(body[5:13], ev.Seq)
	binary.BigEndian.PutUint64(body[13:21], uint64(ev.ExtentID))
	binary.BigEndian.PutUint64(body[21:29], uint64(ev.Generation))
	binary.BigEndian.PutUint64(body[29:37], uint64(ev.SegmentID))
	binary.BigEndian.PutUint64(body[37:45], uint64(ev.AtUnix))
	binary.BigEndian.PutUint16(body[45:47], uint16(len(reason)))
	copy(body[47:], reason)
	crc := crc32.ChecksumIEEE(body)
	binary.BigEndian.PutUint32(buf[len(body):len(body)+4], crc)
	return buf
}

// decodeFile decodes all events in a log file. Each record is
// length-prefixed; the length value counts the body bytes INCLUDING
// the length field itself (matching encodeEvent), and the CRC covers
// that body.
func decodeFile(path string) []ChangeEvent {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var out []ChangeEvent
	off := 0
	for off+8 <= len(data) {
		bodyLen := int(binary.BigEndian.Uint32(data[off : off+4]))
		if bodyLen < 47 || off+bodyLen+4 > len(data) {
			break
		}
		// The body includes the length field.
		body := data[off : off+bodyLen]
		want := binary.BigEndian.Uint32(data[off+bodyLen : off+bodyLen+4])
		if crc32.ChecksumIEEE(body) != want {
			break
		}
		ev := ChangeEvent{
			Kind:       ChangeEventKind(body[4]),
			Seq:        binary.BigEndian.Uint64(body[5:13]),
			ExtentID:   storage.ExtentID(binary.BigEndian.Uint64(body[13:21])),
			Generation: storage.Generation(binary.BigEndian.Uint64(body[21:29])),
			SegmentID:  storage.SegmentID(binary.BigEndian.Uint64(body[29:37])),
			AtUnix:     int64(binary.BigEndian.Uint64(body[37:45])),
		}
		rl := int(binary.BigEndian.Uint16(body[45:47]))
		if 47+rl <= len(body) {
			ev.Reason = string(body[47 : 47+rl])
		}
		out = append(out, ev)
		off += bodyLen + 4
	}
	return out
}

func eventSize(ev ChangeEvent) int {
	return 4 + 1 + 8 + 8 + 8 + 8 + 8 + 2 + len(ev.Reason) + 4
}

func listChangeFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var files []string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "change-") && strings.HasSuffix(e.Name(), ".log") {
			files = append(files, filepath.Join(dir, e.Name()))
		}
	}
	sort.Strings(files)
	return files, nil
}

func seqFromName(path string) uint64 {
	base := filepath.Base(path)
	base = strings.TrimSuffix(strings.TrimPrefix(base, "change-"), ".log")
	var s uint64
	fmt.Sscanf(base, "%d", &s)
	return s
}
