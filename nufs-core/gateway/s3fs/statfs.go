package s3fs

import (
	"context"
	"log/slog"
	"sync"
	"time"

	madmin "github.com/minio/madmin-go/v3"
	"github.com/minio/minio-go/v7"
)

// statfsUsage lazily caches the bucket's usage (and quota, when the server
// has one configured) as observed from the SERVER, refreshed at most once
// per ttl. df/statfs is low-frequency, so a blocking refresh on cache miss
// is acceptable and keeps the data truthful: the bucket is shared state
// that other clients mutate, so no locally maintained counter can be
// correct - only the server knows.
//
// Two refresh paths:
//   - MinIO with admin-capable credentials: server-side accounting via
//     DataUsageInfo (no object listing) plus the bucket's hard quota via
//     GetBucketQuota. df then shows total/used/free.
//   - Any other S3-compatible server: a one-shot ListObjectsV2 sweep of
//     the bucket - the industry default for S3-backed mounts. Capacity is
//     unbounded (Bfree=0), df shows used only.
type statfsUsage struct {
	mu    sync.Mutex
	ttl   time.Duration
	usage uint64 // total object bytes in the bucket
	quota uint64 // configured hard quota bytes; 0 = unbounded
	at    time.Time
}

// get returns (usage, quota), refreshing from the server on first call
// and whenever the cached observation is older than ttl.
func (s *statfsUsage) get(ctx context.Context, fsys *S3FileSystem) (usage, quota uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.at.IsZero() && time.Since(s.at) < s.ttl {
		return s.usage, s.quota
	}
	s.refreshLocked(ctx, fsys)
	return s.usage, s.quota
}

// refreshLocked queries the server for current usage and quota. Caller
// holds s.mu.
func (s *statfsUsage) refreshLocked(ctx context.Context, fsys *S3FileSystem) {
	if adm := fsys.adminClient(); adm != nil {
		if info, err := adm.DataUsageInfo(ctx); err == nil {
			if u, ok := info.BucketsUsage[fsys.config.Bucket]; ok {
				var q uint64
				if bq, qerr := adm.GetBucketQuota(ctx, fsys.config.Bucket); qerr == nil {
					q = bq.Size
				}
				s.usage = u.Size
				s.quota = q
				s.at = time.Now()
				return
			}
		} else {
			slog.Debug("s3fs: DataUsageInfo unavailable, falling back to object sweep", "error", err)
		}
	}

	// Standard-S3 fallback: one-shot full-bucket sweep.
	var total uint64
	ch := fsys.api.ListObjects(ctx, fsys.config.Bucket, minio.ListObjectsOptions{
		Recursive: true,
	})
	for obj := range ch {
		if obj.Err == nil {
			total += uint64(obj.Size)
		}
	}
	s.usage = total
	s.quota = 0
	s.at = time.Now()
}

// adminClient returns the MinIO admin client when the endpoint speaks the
// MinIO admin protocol AND the mount credentials carry admin rights, nil
// otherwise. The probe runs once on first statfs (not at mount, to keep
// mount fast): a server that is not MinIO, or credentials without admin
// rights, simply and permanently disables the admin path.
func (fsys *S3FileSystem) adminClient() *madmin.AdminClient {
	fsys.adminProbe.Do(func() {
		if fsys.adm == nil {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		// Any successful response - including "no quota set" - proves the
		// admin API is reachable with these credentials.
		if _, err := fsys.adm.GetBucketQuota(ctx, fsys.config.Bucket); err != nil {
			slog.Debug("s3fs: MinIO admin API unavailable, statfs falls back to object sweep", "error", err)
			fsys.adm = nil
		}
	})
	return fsys.adm
}
