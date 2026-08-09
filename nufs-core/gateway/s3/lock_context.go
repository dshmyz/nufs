package s3

import (
	"context"
	"time"

	"github.com/dshmyz/nufs/nufs-core/metadata"
)

const detachedMetadataTimeout = 5 * time.Second

type advisoryUnlocker interface {
	AdvisoryUnlock(context.Context, metadata.InodeID, string) error
}

func detachedMetadataContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithTimeout(context.WithoutCancel(ctx), detachedMetadataTimeout)
}

func releaseAdvisoryLock(ctx context.Context, meta advisoryUnlocker, inode metadata.InodeID, owner string) error {
	unlockCtx, cancel := detachedMetadataContext(ctx)
	defer cancel()
	return meta.AdvisoryUnlock(unlockCtx, inode, owner)
}
