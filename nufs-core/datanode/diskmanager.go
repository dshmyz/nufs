package datanode

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/example/dfs/metadata"
)

// ============================================================
// Disk Manager — Production disk lifecycle management
// ============================================================

// DiskManager monitors disk health, enforces capacity limits,
// and manages storage tiers for chunk data.
type DiskManager struct {
	dataDir    string
	store      *ChunkStore
	capacityGB int64
	mu         sync.RWMutex

	// Real-time disk stats
	stats DiskStats

	// Storage tier configuration
	tiers map[metadata.StorageTier]*TierConfig

	// Admission control
	admitCh   chan struct{} // semaphore for admission
	rejectPct float64       // reject writes when usage > this %

	// WAL for crash recovery
	wal *WriteAheadLog

	stopCh  chan struct{}
	running atomic.Bool
}

// DiskStats holds real-time disk utilization metrics.
type DiskStats struct {
	TotalBytes  int64        `json:"total_bytes"`
	UsedBytes   int64        `json:"used_bytes"`
	AvailBytes  int64        `json:"avail_bytes"`
	UsagePct    float64      `json:"usage_pct"`
	ChunkCount  int64        `json:"chunk_count"`
	ReadIOPS    atomic.Int64 `json:"-"`
	WriteIOPS   atomic.Int64 `json:"-"`
	ReadBytes   atomic.Int64 `json:"-"`
	WriteBytes  atomic.Int64 `json:"-"`
	LastUpdated time.Time    `json:"last_updated"`
}

// TierConfig configures a storage tier.
type TierConfig struct {
	Tier       metadata.StorageTier
	MaxPct     float64 // Max % of total capacity for this tier
	MinAgeDays int     // Data older than this can be migrated down
}

// NewDiskManager creates a disk manager with capacity enforcement.
// The caller must provide a WAL instance (use NewWriteAheadLog to create one).
func NewDiskManager(dataDir string, store *ChunkStore, capacityGB int64, wal *WriteAheadLog) (*DiskManager, error) {

	dm := &DiskManager{
		dataDir:    dataDir,
		store:      store,
		capacityGB: capacityGB,
		tiers:      defaultTierConfig(),
		admitCh:    make(chan struct{}, 64),
		rejectPct:  0.90, // reject at 90%
		wal:        wal,
		stopCh:     make(chan struct{}),
	}
	dm.stats.TotalBytes = capacityGB * 1024 * 1024 * 1024
	dm.refreshStats()
	return dm, nil
}

func defaultTierConfig() map[metadata.StorageTier]*TierConfig {
	return map[metadata.StorageTier]*TierConfig{
		metadata.TierHot:     {Tier: metadata.TierHot, MaxPct: 0.30, MinAgeDays: 0},
		metadata.TierWarm:    {Tier: metadata.TierWarm, MaxPct: 0.40, MinAgeDays: 7},
		metadata.TierCold:    {Tier: metadata.TierCold, MaxPct: 0.25, MinAgeDays: 30},
		metadata.TierArchive: {Tier: metadata.TierArchive, MaxPct: 0.05, MinAgeDays: 90},
	}
}

// Start begins periodic disk monitoring.
func (dm *DiskManager) Start() {
	if dm.running.Swap(true) {
		return
	}
	go dm.monitorLoop()
}

// Stop terminates the disk manager.
func (dm *DiskManager) Stop() {
	if dm.running.Swap(false) {
		close(dm.stopCh)
	}
	dm.wal.Close()
}

// DiskStatsSnapshot is a point-in-time copy of DiskStats without atomic fields (safe to copy).
type DiskStatsSnapshot struct {
	TotalBytes  int64     `json:"total_bytes"`
	UsedBytes   int64     `json:"used_bytes"`
	AvailBytes  int64     `json:"avail_bytes"`
	UsagePct    float64   `json:"usage_pct"`
	ChunkCount  int64     `json:"chunk_count"`
	ReadIOPS    int64     `json:"read_iops"`
	WriteIOPS   int64     `json:"write_iops"`
	ReadBytes   int64     `json:"read_bytes"`
	WriteBytes  int64     `json:"write_bytes"`
	LastUpdated time.Time `json:"last_updated"`
}

// Stats returns current disk statistics as a snapshot (safe to copy).
func (dm *DiskManager) Stats() DiskStatsSnapshot {
	dm.mu.RLock()
	defer dm.mu.RUnlock()
	return DiskStatsSnapshot{
		TotalBytes:  dm.stats.TotalBytes,
		UsedBytes:   dm.stats.UsedBytes,
		AvailBytes:  dm.stats.AvailBytes,
		UsagePct:    dm.stats.UsagePct,
		ChunkCount:  dm.stats.ChunkCount,
		ReadIOPS:    dm.stats.ReadIOPS.Load(),
		WriteIOPS:   dm.stats.WriteIOPS.Load(),
		ReadBytes:   dm.stats.ReadBytes.Load(),
		WriteBytes:  dm.stats.WriteBytes.Load(),
		LastUpdated: dm.stats.LastUpdated,
	}
}

// CanAdmitWrite checks if the disk can accept a new write.
func (dm *DiskManager) CanAdmitWrite(sizeBytes int64) error {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	if dm.stats.TotalBytes == 0 {
		return nil // No capacity limit
	}

	// Check capacity
	projected := float64(dm.stats.UsedBytes+sizeBytes) / float64(dm.stats.TotalBytes)
	if projected > dm.rejectPct {
		return fmt.Errorf("disk: capacity limit reached (%.1f%% used, limit %.0f%%)",
			dm.stats.UsagePct*100, dm.rejectPct*100)
	}

	return nil
}

// RecordRead updates read I/O counters.
func (dm *DiskManager) RecordRead(bytes int64) {
	dm.stats.ReadIOPS.Add(1)
	dm.stats.ReadBytes.Add(bytes)
}

// RecordWrite updates write I/O counters.
func (dm *DiskManager) RecordWrite(bytes int64) {
	dm.stats.WriteIOPS.Add(1)
	dm.stats.WriteBytes.Add(bytes)
}

// WAL returns the write-ahead log for crash recovery.
func (dm *DiskManager) WAL() *WriteAheadLog {
	return dm.wal
}

func (dm *DiskManager) monitorLoop() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			dm.refreshStats()
		case <-dm.stopCh:
			return
		}
	}
}

func (dm *DiskManager) refreshStats() {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	dm.stats.ChunkCount = dm.store.chunkCount.Load()
	dm.stats.UsedBytes = dm.store.totalBytes.Load()

	if dm.stats.TotalBytes > 0 {
		dm.stats.AvailBytes = dm.stats.TotalBytes - dm.stats.UsedBytes
		dm.stats.UsagePct = float64(dm.stats.UsedBytes) / float64(dm.stats.TotalBytes)
	}
	dm.stats.LastUpdated = time.Now()
}

// ============================================================
// Write-Ahead Log — Crash recovery for in-flight writes
// ============================================================

// WAL entry format:
//   [magic: 4B "WAL1"] [length: 4B] [chunk_id: 8B] [op: 1B] [crc: 4B] [data...]

const (
	walMagic    = "WAL1"
	walOpWrite  = 0x01
	walOpDelete = 0x02
	walOpCommit = 0x03 // marks write as complete
)

// WriteAheadLog provides crash recovery for chunk writes.
type WriteAheadLog struct {
	dir  string
	file *os.File
	mu   sync.Mutex
}

// NewWriteAheadLog creates or opens a WAL.
func NewWriteAheadLog(dir string) (*WriteAheadLog, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, "wal.log")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	return &WriteAheadLog{dir: dir, file: f}, nil
}

// LogWrite records a pending chunk write.
func (w *WriteAheadLog) LogWrite(chunkID metadata.ChunkID, dataLen int) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	header := make([]byte, 21) // 4+4+8+1+4
	copy(header[0:4], walMagic)
	binary.BigEndian.PutUint32(header[4:8], uint32(dataLen))
	binary.BigEndian.PutUint64(header[8:16], uint64(chunkID))
	header[16] = walOpWrite
	crc := crc32.ChecksumIEEE(header[:17])
	binary.BigEndian.PutUint32(header[17:21], crc)

	if _, err := w.file.Write(header); err != nil {
		return err
	}
	return w.file.Sync()
}

// LogCommit records that a chunk write completed successfully.
func (w *WriteAheadLog) LogCommit(chunkID metadata.ChunkID) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	header := make([]byte, 21)
	copy(header[0:4], walMagic)
	binary.BigEndian.PutUint32(header[4:8], 0) // no data
	binary.BigEndian.PutUint64(header[8:16], uint64(chunkID))
	header[16] = walOpCommit
	crc := crc32.ChecksumIEEE(header[:17])
	binary.BigEndian.PutUint32(header[17:21], crc)

	if _, err := w.file.Write(header); err != nil {
		return err
	}
	return w.file.Sync()
}

// Recover returns chunk IDs that were written but not committed (need cleanup).
func (w *WriteAheadLog) Recover() ([]metadata.ChunkID, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	written := make(map[metadata.ChunkID]bool)
	committed := make(map[metadata.ChunkID]bool)

	header := make([]byte, 21)
	for {
		_, err := io.ReadFull(w.file, header)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			break
		}

		if string(header[0:4]) != walMagic {
			break // corrupted entry
		}

		// Verify CRC
		expected := binary.BigEndian.Uint32(header[17:21])
		actual := crc32.ChecksumIEEE(header[:17])
		if expected != actual {
			break
		}

		chunkID := metadata.ChunkID(binary.BigEndian.Uint64(header[8:16]))
		op := header[16]
		dataLen := binary.BigEndian.Uint32(header[4:8])

		switch op {
		case walOpWrite:
			written[chunkID] = true
			// Skip data payload
			if dataLen > 0 {
				w.file.Seek(int64(dataLen), io.SeekCurrent)
			}
		case walOpCommit:
			committed[chunkID] = true
		}
	}

	// Find uncommitted writes
	var orphans []metadata.ChunkID
	for id := range written {
		if !committed[id] {
			orphans = append(orphans, id)
		}
	}
	return orphans, nil
}

// Truncate clears the WAL (call after successful recovery).
func (w *WriteAheadLog) Truncate() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Truncate(0)
}

// Close closes the WAL file.
func (w *WriteAheadLog) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}

// ============================================================
// Storage Tier Migration
// ============================================================

// TierMigrator moves chunks between storage tiers based on age and access patterns.
type TierMigrator struct {
	store   *ChunkStore
	disk    *DiskManager
	stopCh  chan struct{}
	running atomic.Bool
}

// NewTierMigrator creates a tier migration engine.
func NewTierMigrator(store *ChunkStore, disk *DiskManager) *TierMigrator {
	return &TierMigrator{
		store:  store,
		disk:   disk,
		stopCh: make(chan struct{}),
	}
}

// MigrationPlan describes chunks to migrate between tiers.
type MigrationPlan struct {
	HotToWarm  int
	WarmToCold int
	ColdToArch int
	TotalBytes int64
}

// PlanMigration analyzes chunks and creates a migration plan.
func (tm *TierMigrator) PlanMigration() (*MigrationPlan, error) {
	plan := &MigrationPlan{}
	now := time.Now()

	tm.store.mu.RLock()
	defer tm.store.mu.RUnlock()

	for _, chunk := range tm.store.chunks {
		age := now.Sub(chunk.WrittenAt)

		// Simple age-based tiering (production: use access frequency + age)
		switch {
		case chunk.Tier == metadata.TierHot && age > 7*24*time.Hour:
			plan.HotToWarm++
			plan.TotalBytes += chunk.Size
		case chunk.Tier == metadata.TierWarm && age > 30*24*time.Hour:
			plan.WarmToCold++
			plan.TotalBytes += chunk.Size
		case chunk.Tier == metadata.TierCold && age > 90*24*time.Hour:
			plan.ColdToArch++
			plan.TotalBytes += chunk.Size
		}
	}

	return plan, nil
}

// Start runs tier migration periodically.
func (tm *TierMigrator) Start(interval time.Duration) {
	if tm.running.Swap(true) {
		return
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				plan, err := tm.PlanMigration()
				if err != nil {
					log.Printf("tier: plan error: %v", err)
					continue
				}
				if plan.HotToWarm+plan.WarmToCold+plan.ColdToArch > 0 {
					log.Printf("tier: migration plan: hot→warm=%d warm→cold=%d cold→arch=%d (%d bytes)",
						plan.HotToWarm, plan.WarmToCold, plan.ColdToArch, plan.TotalBytes)
					tm.executeMigration()
				}
			case <-tm.stopCh:
				return
			}
		}
	}()
}

// Stop terminates tier migration.
func (tm *TierMigrator) Stop() {
	if tm.running.Swap(false) {
		close(tm.stopCh)
	}
}

// executeMigration moves chunks to their target storage tier based on age.
func (tm *TierMigrator) executeMigration() {
	migrated := 0
	tm.store.mu.Lock()
	for _, chunk := range tm.store.chunks {
		age := time.Since(chunk.WrittenAt)
		changed := false
		switch {
		case chunk.Tier == metadata.TierHot && age > 7*24*time.Hour:
			chunk.Tier = metadata.TierWarm
			changed = true
		case chunk.Tier == metadata.TierWarm && age > 30*24*time.Hour:
			chunk.Tier = metadata.TierCold
			changed = true
		case chunk.Tier == metadata.TierCold && age > 90*24*time.Hour:
			chunk.Tier = metadata.TierArchive
			changed = true
		}
		if changed {
			migrated++
			tm.store.writeMetaSidecar(chunk.ChunkID, chunk)
		}
	}
	tm.store.mu.Unlock()

	if migrated > 0 {
		log.Printf("tier: migrated %d chunks", migrated)
	}
}
