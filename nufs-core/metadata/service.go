package metadata

import (
	"context"
	"sync"
	"time"
)

// ============================================================
// MetadataService — Unified interface for all backends
// ============================================================

// BucketService defines bucket lifecycle operations.
type BucketService interface {
	CreateBucket(ctx context.Context, name string, policy PlacementPolicy) error
	DeleteBucket(ctx context.Context, name string) error
	ListBuckets(ctx context.Context) ([]BucketInfo, error)
	GetBucket(ctx context.Context, name string) (*BucketInfo, error)
	GetBucketByRoot(ctx context.Context, rootInode InodeID) (*BucketInfo, error)
}

// BucketQuotaService defines quota configuration and admission checks for buckets.
type BucketQuotaService interface {
	GetBucketQuota(ctx context.Context, bucket string) (*BucketQuota, error)
	SetBucketQuota(ctx context.Context, bucket string, quota *BucketQuota) error
	DeleteBucketQuota(ctx context.Context, bucket string) error
	CheckBucketQuota(ctx context.Context, bucket string, additionalBytes int64, additionalObjects int64) error
	GetBucketUsage(ctx context.Context, bucket string) (*BucketUsage, error)
}

// NamespaceService defines directory and file namespace operations.
type NamespaceService interface {
	MkDir(ctx context.Context, parent InodeID, name string, mode uint32) (*InodeMeta, error)
	RmDir(ctx context.Context, parent InodeID, name string) error
	ReadDir(ctx context.Context, parent InodeID, offset int, limit int) ([]DirEntry, error)
	ReadDirFrom(ctx context.Context, parent InodeID, afterName string, limit int) ([]DirEntry, error)
	CreateFile(ctx context.Context, parent InodeID, name string, mode uint32) (*InodeMeta, error)
	// CreateNode creates a special (non-regular) namespace entry: FIFO, char
	// device, block device or socket. rdev is the device number, used only by
	// the char/block device types.
	CreateNode(ctx context.Context, parent InodeID, name string, ftype FileType, mode uint32, rdev uint32) (*InodeMeta, error)
	Unlink(ctx context.Context, parent InodeID, name string) error
	Lookup(ctx context.Context, parent InodeID, name string) (*InodeMeta, error)
	Rename(ctx context.Context, oldParent InodeID, oldName string, newParent InodeID, newName string) error
	Symlink(ctx context.Context, parent InodeID, name string, target string) (*InodeMeta, error)
	Readlink(ctx context.Context, id InodeID) (string, error)
	Link(ctx context.Context, parent InodeID, name string, target InodeID) (*InodeMeta, error)
}

// InodeService defines inode read/write operations.
type InodeService interface {
	GetInode(ctx context.Context, id InodeID) (*InodeMeta, error)
	UpdateInode(ctx context.Context, meta *InodeMeta) error
}

// ExtentInodeService is the V2.1 extent-layout inode surface (§11.1). Unlike
// InodeService (which speaks the legacy ChunkMap InodeMeta), this interface
// exposes the InodeMetaV2 layout transitions (Empty → InlineExtent →
// ExtentPages) and extent resolution.
//
// It is deliberately NOT embedded in MetadataService: the gateway depends on
// the smallest interface it needs, and V2 layout support is discovered by a
// type assertion so V1-only test mocks keep working unchanged. Production
// backends (PebbleStore local, ShardedStore, HTTPClient remote) all implement
// it.
//
// Both InodeMeta (V1) and InodeMetaV2 (V2) persist under the same
// /inode/{id} key (field-name msgpack, disjoint layout fields). The model
// invariant enforced across this surface: an inode row is written by exactly
// one model. V1 UpdateInode refuses to overwrite a V2-layout row
// (ErrInodeModelMismatch); these methods are the only way to write V2 layout.
type ExtentInodeService interface {
	// ResolveExtents returns the file's flat extent references (inline or
	// extent pages). For an empty inode — or a V1 (ChunkMap) row, which
	// decodes as LayoutEmpty — it returns (nil, nil): callers probe this
	// only when the ChunkMap is empty, so nil means "no V2 extents".
	ResolveExtents(ctx context.Context, id InodeID) ([]ExtentRef, error)

	// GetExtentMeta returns a V2 extent's metadata (length, placement,
	// storage class, EC stripe). Extents are recorded under
	// /extent-meta/{extent_id} by SetInlineExtent / AppendExtent.
	GetExtentMeta(ctx context.Context, extentID ExtentIDV2) (*ExtentMetaV2, error)

	// SetInlineExtent promotes an empty inode to a single inline extent
	// (files with one extent, ≤ 16 MiB). Persists the extent metadata and
	// the inode's inline layout in one serving-surface call.
	SetInlineExtent(ctx context.Context, id InodeID, extent *ExtentMetaV2, size int64) error

	// PromoteToPages transitions an inline-extent inode to extent pages
	// (multi-extent files). The inline extent becomes page 0 under a COW root.
	PromoteToPages(ctx context.Context, id InodeID) error

	// AppendExtent appends an extent reference to a pages-layout inode,
	// persisting the extent metadata. Returns the new COW extent root.
	AppendExtent(ctx context.Context, id InodeID, extent *ExtentMetaV2, offset int64) (uint64, error)

	// ReplaceExtents rewrites the file's entire extent set as extent pages
	// under a fresh COW root, replacing whatever model the row previously
	// had (empty, inline, or earlier pages). This is the whole-set writer
	// for gateway overwrites: unlike PromoteToPages it does NOT preserve an
	// old inline extent as page 0, because the overwrite has already
	// rewritten the old extent's data into the new chunk set. Persists each
	// extent's /extent-meta row and the inode's pages layout in one
	// serving-surface call.
	ReplaceExtents(ctx context.Context, id InodeID, writes []ExtentWrite, size int64) error
}

// ChunkService defines chunk lifecycle operations.
type ChunkService interface {
	AllocateChunk(ctx context.Context, inodeID InodeID, offset int64, policy PlacementPolicy) (*ChunkMeta, error)
	// AllocateChunksBatch atomically allocates up to MaxChunkAllocationBatch
	// chunks and appends their references to the inode in one metadata commit.
	AllocateChunksBatch(ctx context.Context, inodeID InodeID, offsets []int64, policy PlacementPolicy) ([]*ChunkMeta, error)
	CommitChunk(ctx context.Context, chunkID ChunkID, checksum uint32) error
	GetChunk(ctx context.Context, chunkID ChunkID) (*ChunkMeta, error)
	UpdateChunk(ctx context.Context, chunk *ChunkMeta) error
	SealChunk(ctx context.Context, chunkID ChunkID) error
	ListChunks(ctx context.Context, inodeID InodeID) ([]ChunkRef, error)
	DeleteChunk(ctx context.Context, chunkID ChunkID) error
	ReportChunkState(ctx context.Context, nodeID NodeID, states map[ChunkID]ReplicaState) error
}

// NodeService defines cluster node management operations.
type NodeService interface {
	RegisterNode(ctx context.Context, info *NodeInfo) error
	Heartbeat(ctx context.Context, nodeID NodeID, report *NodeReport) error
	DecommissionNode(ctx context.Context, nodeID NodeID) error
	EnterMaintenance(ctx context.Context, nodeID NodeID) error
	ExitMaintenance(ctx context.Context, nodeID NodeID) error
	RollingUpgradePlan(ctx context.Context) ([]NodeID, error)
	ListNodes(ctx context.Context) ([]NodeInfo, error)
	GetNode(ctx context.Context, nodeID NodeID) (*NodeInfo, error)
}

// RepairService defines repair and rebalance operations.
type RepairService interface {
	GetRepairQueue(ctx context.Context) ([]RepairTask, error)
	TriggerRepair(ctx context.Context, chunkID ChunkID) error
	RemoveRepairTask(ctx context.Context, chunkID ChunkID) error
	TriggerRebalance(ctx context.Context) error
	ChunksByNode(ctx context.Context, nodeID NodeID) ([]ChunkMeta, error)
	MigrateChunkReplica(ctx context.Context, chunkID ChunkID, fromNode, toNode NodeID) error
}

// WriteAttemptService records object write progress so interrupted writes can
// be recovered or cleaned up by background workers.
type WriteAttemptService interface {
	PutWriteAttempt(ctx context.Context, attempt *ObjectWriteAttempt) error
	GetWriteAttempt(ctx context.Context, id string) (*ObjectWriteAttempt, error)
	ListWriteAttemptsByState(ctx context.Context, state WriteAttemptState, limit int) ([]ObjectWriteAttempt, error)
	DeleteWriteAttempt(ctx context.Context, id string) error
}

// BackgroundTaskService coordinates durable background work across workers.
type BackgroundTaskService interface {
	PutBackgroundTask(ctx context.Context, task *BackgroundTask) error
	GetBackgroundTask(ctx context.Context, id string) (*BackgroundTask, error)
	LeaseBackgroundTask(ctx context.Context, taskType BackgroundTaskType, owner string, lease time.Duration) (*BackgroundTask, error)
	CompleteBackgroundTask(ctx context.Context, id string) error
	FailBackgroundTask(ctx context.Context, id string, lastErr string, maxAttempts int) error
}

// BackupMetadataService is the narrow durable state boundary used by backup,
// restore-readiness, and tombstone workers.
type BackupMetadataService interface {
	PutBackupTask(context.Context, *BackupTask) error
	GetBackupTask(context.Context, string) (*BackupTask, error)
	ListBackupTasks(context.Context, int) ([]BackupTask, error)
	ScanActiveBackupTasks(context.Context, func(BackupTask) error) error
	ReplaceCommittedBackupCatalog(context.Context, []CommittedBackup, time.Time) error
	GetBackupCatalogState(context.Context) (*BackupCatalogState, error)
	PutRestorePendingMarker(context.Context, *RestorePendingMarker) error
	GetRestorePendingMarker(context.Context) (*RestorePendingMarker, error)
	ClearRestorePendingMarker(context.Context) error
}

// LockService defines advisory lock operations.
type LockService interface {
	AdvisoryLock(ctx context.Context, inode InodeID, owner string) error
	AdvisoryLockShared(ctx context.Context, inode InodeID, owner string) error
	AdvisoryUnlock(ctx context.Context, inode InodeID, owner string) error
	AdvisoryListLocks(ctx context.Context, inode InodeID) ([]LockInfo, error)
}

// XAttrService defines extended attribute operations.
type XAttrService interface {
	GetXAttr(ctx context.Context, id InodeID, name string) ([]byte, error)
	SetXAttr(ctx context.Context, id InodeID, name string, value []byte) error
	ListXAttr(ctx context.Context, id InodeID) (map[string][]byte, error)
	RemoveXAttr(ctx context.Context, id InodeID, name string) error
}

// MetadataService is the composition of all sub-service interfaces.
// PebbleStore implements this interface as the sole storage backend,
// with optional Raft consensus for distributed deployments.
//
// Consumers should depend on the smallest sub-interface they need
// (e.g., ChunkService instead of MetadataService) to improve
// testability and reduce coupling.
type MetadataService interface {
	BucketService
	BucketQuotaService
	NamespaceService
	InodeService
	ChunkService
	NodeService
	RepairService
	WriteAttemptService
	BackgroundTaskService
	LockService
	XAttrService
	AccessControlService

	// Admin
	ComputeAllBucketUsage(ctx context.Context) ([]BucketUsage, error)

	// Lifecycle
	Close() error

	// OptimizedWrite combines CreateFile + AllocateChunksBatch +
	// CommitChunk into a single atomic metadata operation, reducing
	// lock contention from 4 acquisitions to 1. Used by the S3 gateway
	// PutObject path for improved write latency.
	// Returns the created inode and allocated chunks.
	CreateObjectWithChunks(ctx context.Context, parent InodeID, name string,
		mode uint32, offsets []int64, policy PlacementPolicy) (*InodeMeta, []*ChunkMeta, error)
}

// Compile-time interface check
var _ MetadataService = (*PebbleStore)(nil)

// ============================================================
// ServiceBundle — Production deployment composition
// ============================================================

// ServiceBundle composes a MetadataService with all production subsystems.
type ServiceBundle struct {
	// Core metadata service (PebbleStore)
	Metadata MetadataService

	// Production subsystems
	Events       *EventBus
	Metrics      *Metrics
	Health       *HealthChecker
	Leases       *LeaseManager
	GC           *ChunkGC
	Scrub        *Scrubber
	ExtentScrub  *ExtentScrubber
	Raft         *RaftNode
	Lifecycle    *LifecycleEngine
	Audit        *AuditLogger
	AutoBalancer *AutoBalancer

	// Ready channel: closed when all subsystems are initialized
	Ready chan struct{}

	restoreMu              sync.RWMutex
	restoreReadinessClosed bool
	restoreReadinessReport *RestoreReadinessReport
}

// Close shuts down all subsystems and the core service.
func (sb *ServiceBundle) Close() error {
	// Stop background workers first (reverse order of Start)
	if sb.Audit != nil {
		sb.Audit.Stop()
	}
	if sb.Lifecycle != nil {
		sb.Lifecycle.Stop()
	}
	if sb.AutoBalancer != nil {
		sb.AutoBalancer.Stop()
	}
	if sb.Scrub != nil {
		sb.Scrub.Stop()
	}
	if sb.ExtentScrub != nil {
		sb.ExtentScrub.Stop()
	}
	if sb.GC != nil {
		sb.GC.Stop()
	}
	if sb.Leases != nil {
		sb.Leases.Stop()
	}
	if sb.Raft != nil {
		sb.Raft.Shutdown()
	}
	return sb.Metadata.Close()
}

// IsReady returns true if the service bundle has completed initialization.
func (sb *ServiceBundle) IsReady() bool {
	if sb.Ready == nil {
		return !sb.restoreReadinessPending()
	}
	select {
	case <-sb.Ready:
		return !sb.restoreReadinessPending()
	default:
		return false
	}
}

// SetRestoreReadinessPending keeps a restored cluster out of readiness until
// replica availability has been verified and the restore marker is cleared.
func (sb *ServiceBundle) SetRestoreReadinessPending(report *RestoreReadinessReport) {
	sb.restoreMu.Lock()
	defer sb.restoreMu.Unlock()
	sb.restoreReadinessClosed = false
	sb.restoreReadinessReport = cloneRestoreReadinessReport(report)
}

// UpdateRestoreReadinessReport publishes the latest restore verification state.
func (sb *ServiceBundle) UpdateRestoreReadinessReport(report *RestoreReadinessReport) {
	sb.restoreMu.Lock()
	defer sb.restoreMu.Unlock()
	sb.restoreReadinessReport = cloneRestoreReadinessReport(report)
}

// CompleteRestoreReadiness opens readiness after restored replica verification succeeds.
func (sb *ServiceBundle) CompleteRestoreReadiness(report *RestoreReadinessReport) {
	sb.restoreMu.Lock()
	defer sb.restoreMu.Unlock()
	sb.restoreReadinessClosed = true
	sb.restoreReadinessReport = cloneRestoreReadinessReport(report)
}

// RestoreReadinessReport returns the latest restored-cluster verification report.
func (sb *ServiceBundle) RestoreReadinessReport() *RestoreReadinessReport {
	sb.restoreMu.RLock()
	defer sb.restoreMu.RUnlock()
	return cloneRestoreReadinessReport(sb.restoreReadinessReport)
}

func (sb *ServiceBundle) restoreReadinessPending() bool {
	sb.restoreMu.RLock()
	defer sb.restoreMu.RUnlock()
	return sb.restoreReadinessReport != nil && !sb.restoreReadinessClosed
}

func cloneRestoreReadinessReport(report *RestoreReadinessReport) *RestoreReadinessReport {
	if report == nil {
		return nil
	}
	out := *report
	out.Issues = append([]RestoreChunkAvailabilityIssue(nil), report.Issues...)
	return &out
}

// WaitForReady blocks until the service bundle is fully initialized.
func (sb *ServiceBundle) WaitForReady() {
	if sb.Ready != nil {
		<-sb.Ready
	}
}

// NewPebbleServiceBundle wraps an existing PebbleStore with production subsystems.
// Unlike the old version, this does NOT create a second PebbleStore instance.
func NewPebbleServiceBundle(store *PebbleStore, opts ...ServiceOption) (*ServiceBundle, error) {
	sopts := defaultServiceOptions()
	for _, opt := range opts {
		opt(sopts)
	}

	bundle := &ServiceBundle{
		Metadata: store,
		Metrics:  NewMetrics(),
		Events:   NewEventBus(256),
		Ready:    make(chan struct{}),
	}

	// Health checker
	bundle.Health = NewHealthChecker(store, nil, bundle.Metrics, sopts.Version)
	store.health = bundle.Health

	// Lease manager
	if sopts.LeaseTTL > 0 {
		bundle.Leases = NewLeaseManager(store, bundle.Events, sopts.LeaseTTL)
		bundle.Leases.Start()
	}

	// Chunk GC
	if sopts.GCInterval > 0 {
		bundle.GC = NewChunkGC(store, bundle.Events, bundle.Metrics, sopts.GCDryRun)
		bundle.GC.Start(sopts.GCInterval)
	}

	// Scrubber
	if sopts.ScrubInterval > 0 {
		bundle.Scrub = NewScrubber(store, bundle.Events)
		bundle.Scrub.Start(sopts.ScrubInterval)
		bundle.ExtentScrub = NewExtentScrubber(store)
		bundle.ExtentScrub.Start(sopts.ScrubInterval)
	}

	// Lifecycle engine
	if len(sopts.LifecycleRules) > 0 {
		bundle.Lifecycle = NewLifecycleEngine(store)
		for _, rule := range sopts.LifecycleRules {
			bundle.Lifecycle.AddRule(rule)
		}
		bundle.Lifecycle.Start(1 * time.Hour)
	}

	// Audit logger
	bundle.Audit = NewAuditLogger(store, defaultAuditConfig())

	// Auto balancer
	if sopts.AutoBalanceInterval > 0 {
		bundle.AutoBalancer = NewAutoBalancer(AutoBalancerConfig{
			Threshold:               sopts.AutoBalanceThreshold,
			Interval:                sopts.AutoBalanceInterval,
			MaxConcurrentMigrations: sopts.AutoBalanceMaxConcurrentMigrations,
		})
		bundle.AutoBalancer.SetStore(store)
		bundle.AutoBalancer.SetExecutor(NewRebalanceExecutor(store))
		bundle.AutoBalancer.Start()
	}

	// Auto-sync PlacementEngine with node state changes via EventBus
	store.SetEventBus(bundle.Events)
	store.placement.SubscribeEvents(bundle.Events)

	// Signal ready
	close(bundle.Ready)

	return bundle, nil
}

// ServiceOption configures a ServiceBundle.
type ServiceOption func(*serviceOptions)

type serviceOptions struct {
	Version                            string
	LeaseTTL                           time.Duration
	GCInterval                         time.Duration
	GCDryRun                           bool
	ScrubInterval                      time.Duration
	LifecycleRules                     []LifecycleRule
	AutoBalanceInterval                time.Duration
	AutoBalanceThreshold               float64
	AutoBalanceMaxConcurrentMigrations int
}

func defaultServiceOptions() *serviceOptions {
	return &serviceOptions{
		Version:                            "0.2.0",
		LeaseTTL:                           30 * time.Second,
		GCInterval:                         10 * time.Minute,
		ScrubInterval:                      1 * time.Hour,
		AutoBalanceThreshold:               0.15,
		AutoBalanceMaxConcurrentMigrations: 10,
	}
}

// WithVersion sets the service version.
func WithVersion(v string) ServiceOption {
	return func(o *serviceOptions) { o.Version = v }
}

// WithLeaseTTL sets the node heartbeat TTL.
func WithLeaseTTL(d time.Duration) ServiceOption {
	return func(o *serviceOptions) { o.LeaseTTL = d }
}

// WithGCInterval sets the orphan chunk GC interval.
func WithGCInterval(d time.Duration) ServiceOption {
	return func(o *serviceOptions) { o.GCInterval = d }
}

// WithScrubInterval sets the data scrub interval.
func WithScrubInterval(d time.Duration) ServiceOption {
	return func(o *serviceOptions) { o.ScrubInterval = d }
}

// WithGCDryRun enables GC dry-run mode (report only, no delete).
func WithGCDryRun(enabled bool) ServiceOption {
	return func(o *serviceOptions) { o.GCDryRun = enabled }
}

// WithLifecycleRules sets lifecycle management rules.
func WithLifecycleRules(rules []LifecycleRule) ServiceOption {
	return func(o *serviceOptions) { o.LifecycleRules = rules }
}

// WithAutoBalanceInterval enables periodic automatic rebalance checks.
// A non-positive interval disables the periodic auto balancer.
func WithAutoBalanceInterval(d time.Duration) ServiceOption {
	return func(o *serviceOptions) { o.AutoBalanceInterval = d }
}

// WithAutoBalanceThreshold sets the imbalance threshold for periodic auto balance.
func WithAutoBalanceThreshold(threshold float64) ServiceOption {
	return func(o *serviceOptions) { o.AutoBalanceThreshold = threshold }
}

// WithAutoBalanceMaxConcurrentMigrations caps migration plans per auto-balance pass.
func WithAutoBalanceMaxConcurrentMigrations(n int) ServiceOption {
	return func(o *serviceOptions) { o.AutoBalanceMaxConcurrentMigrations = n }
}
