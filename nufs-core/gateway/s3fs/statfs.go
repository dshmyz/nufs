package s3fs

import (
	"context"
	"log/slog"
	"sync"
	"time"

	madmin "github.com/minio/madmin-go/v3"
	"github.com/minio/minio-go/v7"
)

// statfsRefreshTimeout bounds one statfs refresh: a stalled-but-connected
// server must hang df for at most this long, never indefinitely (the
// FUSE statfs context carries no deadline of its own).
const statfsRefreshTimeout = 2 * time.Minute

// statfsFailureCooldown is the negative cache after a failed refresh: a
// monitoring loop polling df against a failing server must not amplify
// into a full-prefix listing on every call. Stale data is served instead.
const statfsFailureCooldown = 30 * time.Second

// statfsProbeRetry bounds how often the MinIO admin probe is retried: a
// transient outage at first df must not permanently pin the mount to
// full-prefix sweeps for its whole uptime.
const statfsProbeRetry = 5 * time.Minute

// statfsNominalFree is the free space reported when the server has no
// quota configured (capacity is genuinely unbounded). Reporting zero
// free would make statvfs say the filesystem is 100% full and every
// space-checking writer (installers, backup tools, JVM/Python runtimes)
// would refuse to write.
const statfsNominalFree = 1 << 40 // 1 TiB

// statfsUsage lazily caches the mounted prefix's usage (and quota, when
// the server has one configured) as observed from the SERVER, refreshed
// at most once per ttl. The bucket is shared state that other clients
// mutate, so no locally maintained counter can be correct - only the
// server knows.
type statfsUsage struct {
	mu       sync.RWMutex
	ttl      time.Duration
	cooldown time.Duration
	usage    uint64
	quota    uint64    // last observed quota; 0 = none/unbounded
	at       time.Time // last successful observation
	failAt   time.Time // last failed attempt (negative cache)

	// fetch queries the server for (usage, quota); prevQuota is the last
	// cached quota so a quota endpoint failure can keep the last known
	// value. ok=false means the observation is incomplete and must NOT
	// be cached. Injected to keep the caching rules unit-testable.
	fetch func(ctx context.Context, prevQuota uint64) (usage, quota uint64, ok bool)
}

// get returns (usage, quota), refreshing from the server on first call
// and whenever the cached observation is older than ttl. A failed
// refresh serves the stale values and suppresses retries for cooldown.
//
// Fast path (cache hit within TTL): RLock only — concurrent df callers
// are not blocked.  Slow path acquires the write lock for the network
// fetch so only one refresh runs at a time.
func (s *statfsUsage) get(ctx context.Context) (usage, quota uint64) {
	s.mu.RLock()
	if !s.at.IsZero() && time.Since(s.at) < s.ttl {
		u, q := s.usage, s.quota
		s.mu.RUnlock()
		return u, q
	}
	if !s.failAt.IsZero() && time.Since(s.failAt) < s.cooldown {
		u, q := s.usage, s.quota
		s.mu.RUnlock()
		return u, q
	}
	s.mu.RUnlock()

	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	if !s.at.IsZero() && now.Sub(s.at) < s.ttl {
		return s.usage, s.quota
	}
	if !s.failAt.IsZero() && now.Sub(s.failAt) < s.cooldown {
		return s.usage, s.quota
	}
	usage, quota, ok := s.fetch(ctx, s.quota)
	if !ok {
		s.failAt = now
		return s.usage, s.quota
	}
	s.usage, s.quota, s.at, s.failAt = usage, quota, now, time.Time{}
	return s.usage, s.quota
}

// statfsFetch is the production fetch: MinIO admin API first (server-side
// accounting, no listing, plus the bucket's hard quota); subtree mounts
// (BasePath != "", the admin API accounts whole buckets only) and
// non-MinIO servers fall back to a ListObjectsV2 sweep scoped to the
// mount's prefix.
func (fsys *S3FileSystem) statfsFetch(ctx context.Context, prevQuota uint64) (usage, quota uint64, ok bool) {
	ctx, cancel := context.WithTimeout(ctx, statfsRefreshTimeout)
	defer cancel()

	if fsys.config.BasePath == "" {
		if adm := fsys.adminClient(); adm != nil {
			if info, err := adm.DataUsageInfo(ctx); err == nil {
				if u, found := info.BucketsUsage[fsys.config.Bucket]; found {
					q := prevQuota // keep the last known quota on a failed query
					if bq, qerr := adm.GetBucketQuota(ctx, fsys.config.Bucket); qerr == nil {
						q = bq.Size
					} else {
						slog.Debug("s3fs: quota query failed, keeping last known", "error", qerr)
					}
					if ctx.Err() != nil {
						return 0, 0, false
					}
					return u.Size, q, true
				}
			} else {
				slog.Debug("s3fs: DataUsageInfo unavailable, falling back to object sweep", "error", err)
			}
		}
	}

	// Standard-S3 fallback: one sweep of the mounted prefix.
	prefix := fsys.config.BasePath
	if prefix != "" {
		prefix += "/"
	}
	metricsIncS3List()
	var total uint64
	ch := fsys.api.ListObjects(ctx, fsys.config.Bucket, minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: true,
	})
	for obj := range ch {
		if obj.Err != nil {
			return 0, 0, false // partial observation - must not be cached
		}
		total += uint64(obj.Size)
	}
	if ctx.Err() != nil {
		return 0, 0, false
	}
	return total, 0, true
}

// adminClient returns the MinIO admin client when the endpoint speaks the
// MinIO admin protocol AND the mount credentials carry admin rights, nil
// otherwise. Probed lazily on first statfs (not at mount) and retried at
// most once per statfsProbeRetry, so a transient outage degrades to the
// sweep only until the next re-probe.
func (fsys *S3FileSystem) adminClient() *madmin.AdminClient {
	fsys.admMu.Lock()
	defer fsys.admMu.Unlock()
	if fsys.adm == nil {
		return nil
	}
	if fsys.admOK {
		return fsys.adm
	}
	if !fsys.admProbeAt.IsZero() && time.Since(fsys.admProbeAt) < statfsProbeRetry {
		return nil
	}
	fsys.admProbeAt = time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Any successful response - including "no quota set" - proves the
	// admin API is reachable with these credentials.
	if _, err := fsys.adm.GetBucketQuota(ctx, fsys.config.Bucket); err != nil {
		slog.Debug("s3fs: MinIO admin API unavailable, statfs falls back to object sweep", "error", err)
		return nil
	}
	fsys.admOK = true
	return fsys.adm
}
