package s3

import (
	"context"
	"errors"
	"log"

	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// LoadPolicies preloads the gateway's in-memory ACL cache from the metad
// registry so a fresh gateway (or one behind a restart) authorizes against the
// persisted bucket policies instead of an empty cache. Before this, a restart
// left gw.acl empty and authenticated users were let through by default (the
// "no policy = authenticated users allowed" fallback in route()).
//
// It is best-effort: a bucket whose policy fails to load keeps no cache entry
// (falling back to default-deny semantics in CheckAccess). Call it once after
// constructing the gateway; the credential syncer may re-invoke it on its tick
// to pick up externally-changed policies.
func (gw *Gateway) LoadPolicies(ctx context.Context) {
	buckets, err := gw.meta.ListBuckets(ctx)
	if err != nil {
		log.Printf("s3gw: preload policies: list buckets: %v", err)
		return
	}
	for _, b := range buckets {
		p, err := gw.meta.GetBucketPolicy(ctx, b.Name)
		if err != nil {
			if errors.Is(err, metadata.ErrAccessDenied) {
				continue // no policy on record — leave the cache empty (default deny)
			}
			log.Printf("s3gw: preload policy %s: %v", b.Name, err)
			continue
		}
		gw.acl.SetPolicy(b.Name, p)
	}
}
