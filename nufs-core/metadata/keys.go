package metadata

// ============================================================
// Key Prefixes — PebbleStore key space layout
// ============================================================

const (
	// Key prefixes for PebbleStore KV layout.
	// Each prefix defines a separate namespace in the key-value store.
	prefixBucket            = "/bucket/"
	prefixBucketByRoot      = "/bucket-by-root/" // rootInode → bucket name (reverse index)
	prefixBucketStats       = "/bucket-stats/"
	prefixNS                = "/ns/"
	prefixInode             = "/inode/"
	prefixChunk             = "/chunk/"
	prefixExtentPage        = "/extent-page/"
	prefixExtentMeta        = "/extent-meta/"
	prefixPlacementGroup    = "/placement-group/"
	prefixPGRebalance       = "/pg-rebalance/"
	prefixLogicalPartition  = "/logical-partition/"
	prefixDirectoryMap      = "/directory-map/"
	prefixCrossShardTxn     = "/cross-shard-txn/"
	prefixGCBucket          = "/gc-bucket/"
	prefixChunkTombstone    = "chunk-tombstone/"
	prefixNode              = "/node/"
	prefixPolicy            = "/policy/"
	prefixRepair            = "/repair/"
	prefixAudit             = "/audit/"
	prefixACL               = "/acl/"
	prefixQuota             = "/quota/"
	prefixQuotaUsage        = "/quota-usage/"
	prefixFreeList          = "/freelist/" // Recycled inode IDs
	prefixWriteAttempt      = "/write-attempt/"
	prefixWriteAttemptState = "/write-attempt-state/"
	prefixBackgroundTask    = "/background-task/"
	prefixBackgroundTaskQ   = "/background-task-queue/"
	prefixBackupTask        = "backup/task/"
	prefixBackupCatalog     = "backup/catalog/"
	keyBackupCatalog        = "backup/catalog-state"
	keyClusterID            = "system/cluster-id"
	keyInodeReferenceEpoch  = "system/inode-reference-epoch"
	keyRestorePending       = "system/restore-pending"
)
