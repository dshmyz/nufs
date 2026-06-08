package metadata

// ============================================================
// Key Prefixes — PebbleStore key space layout
// ============================================================

const (
	// Key prefixes for PebbleStore KV layout.
	// Each prefix defines a separate namespace in the key-value store.
	prefixBucket      = "/bucket/"
	prefixBucketStats = "/bucket-stats/"
	prefixNS          = "/ns/"
	prefixInode       = "/inode/"
	prefixChunk       = "/chunk/"
	prefixNode        = "/node/"
	prefixPolicy      = "/policy/"
	prefixRepair      = "/repair/"
)
