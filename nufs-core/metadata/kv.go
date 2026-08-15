package metadata

// ============================================================
// Raw KV read access (read-only) for operators.
//
// Exposes a controlled, read-only view of the underlying Pebble
// key-value store. Used by the nufs-cli `kv` command in local mode
// and by the metad `/api/v1/kv` ops endpoint in remote mode (which
// additionally enforces requireLeader + auth-token). Only reads are
// exposed — nothing here can mutate the store.
// ============================================================

// KVGet returns the raw value bytes for a single key. found is false when
// the key is absent (equivalent to pebble.ErrNotFound). The returned slice
// is a copy, safe to hold after the underlying reader closes.
func (s *PebbleStore) KVGet(key string) (found bool, value []byte, err error) {
	return s.getRaw(key)
}

// KVScan returns a page of raw key/value pairs whose keys are prefixed by
// prefix. cursor is an optional exclusive-start key for paging (as returned
// by ScanPageResult.NextKey); pageSize<=0 defaults to 1000.
func (s *PebbleStore) KVScan(prefix string, cursor []byte, pageSize int) (*ScanPageResult, error) {
	return s.scanPrefixPaged(prefix, cursor, pageSize)
}

// KVCatalogPrefixes returns the operator-facing catalog key prefixes that a
// KV scan is allowed to read. This keeps the scan allowlist in sync with the
// documented key space in keys.go rather than hardcoding it in the ops layer.
func KVCatalogPrefixes() []string {
	return []string{
		prefixBucket,
		prefixBucketByRoot,
		prefixBucketStats,
		prefixNS,
		prefixInode,
		prefixChunk,
		prefixExtentPage,
		prefixExtentMeta,
		prefixPlacementGroup,
		prefixLogicalPartition,
		prefixDirectoryMap,
		prefixNode,
		prefixPolicy,
		prefixRepair,
		prefixAudit,
		prefixACL,
		prefixCredential,
		prefixQuota,
		prefixQuotaUsage,
		prefixFreeList,
		prefixWriteAttempt,
		prefixWriteAttemptState,
		prefixBackgroundTask,
		prefixChunkTombstone,
		prefixBackupTask,
		prefixBackupCatalog,
		prefixGCBucket,
	}
}
