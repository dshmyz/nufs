package metadata

import (
	"context"
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
}

// NamespaceService defines directory and file namespace operations.
type NamespaceService interface {
	MkDir(ctx context.Context, parent InodeID, name string, mode uint32) (*InodeMeta, error)
	RmDir(ctx context.Context, parent InodeID, name string) error
	ReadDir(ctx context.Context, parent InodeID, offset int, limit int) ([]DirEntry, error)
	CreateFile(ctx context.Context, parent InodeID, name string, mode uint32) (*InodeMeta, error)
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

// ChunkService defines chunk lifecycle operations.
type ChunkService interface {
	AllocateChunk(ctx context.Context, inodeID InodeID, offset int64, policy PlacementPolicy) (*ChunkMeta, error)
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
	NamespaceService
	InodeService
	ChunkService
	NodeService
	RepairService
	LockService
	XAttrService
	AccessControlService

	// Admin
	ComputeAllBucketUsage(ctx context.Context) ([]BucketUsage, error)

	// Lifecycle
	Close() error
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
	Events    *EventBus
	Metrics   *Metrics
	Health    *HealthChecker
	Leases    *LeaseManager
	GC        *ChunkGC
	Scrub     *Scrubber
	Raft      *RaftNode
	Lifecycle *LifecycleEngine
	Audit     *AuditLogger

	// Ready channel: closed when all subsystems are initialized
	Ready chan struct{}
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
	if sb.Scrub != nil {
		sb.Scrub.Stop()
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
		return true
	}
	select {
	case <-sb.Ready:
		return true
	default:
		return false
	}
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

	// Lease manager
	if sopts.LeaseTTL > 0 {
		bundle.Leases = NewLeaseManager(store, bundle.Events, sopts.LeaseTTL)
		bundle.Leases.Start()
	}

	// Chunk GC
	if sopts.GCInterval > 0 {
		bundle.GC = NewChunkGC(store, bundle.Events, sopts.GCDryRun)
		bundle.GC.Start(sopts.GCInterval)
	}

	// Scrubber
	if sopts.ScrubInterval > 0 {
		bundle.Scrub = NewScrubber(store, bundle.Events)
		bundle.Scrub.Start(sopts.ScrubInterval)
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
	Version        string
	LeaseTTL       time.Duration
	GCInterval     time.Duration
	GCDryRun       bool
	ScrubInterval  time.Duration
	LifecycleRules []LifecycleRule
}

func defaultServiceOptions() *serviceOptions {
	return &serviceOptions{
		Version:       "0.2.0",
		LeaseTTL:      30 * time.Second,
		GCInterval:    10 * time.Minute,
		ScrubInterval: 1 * time.Hour,
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
