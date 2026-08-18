package s3

import (
	"context"
	"log"
	"time"

	"github.com/dshmyz/nufs/nufs-core/metadata"
)

// CredentialSyncer keeps the gateway's in-memory credential table in sync
// with the metad credential registry (the single source of truth). It pulls
// the full registry snapshot — plaintext secrets unsealed by metad — on start
// and then every interval, applying it via CredentialStore.ReplaceAll. A
// failed pull leaves the previous set in place (never clears on error), so a
// metad blip does not bounce the gateway back to anonymous mode.
//
// The pull is a background op: the operator revokes a key with
// `nufs-cli auth del` and the gateway drops it at the next tick (revocation
// latency <= interval).
type CredentialSyncer struct {
	store    *CredentialStore
	fetch    func(ctx context.Context) ([]metadata.GatewayCredential, error)
	interval time.Duration
}

// NewCredentialSyncer builds a syncer that pulls via fetch on start and then
// every interval. interval <= 0 disables periodic refresh (startup pull only).
func NewCredentialSyncer(store *CredentialStore, fetch func(ctx context.Context) ([]metadata.GatewayCredential, error), interval time.Duration) *CredentialSyncer {
	return &CredentialSyncer{store: store, fetch: fetch, interval: interval}
}

// Run performs the initial pull and then refreshes on the interval ticker
// until ctx is cancelled. The initial pull error is returned so the caller can
// decide to fall back to another credential source; subsequent pull errors are
// logged and the previous table is kept.
func (s *CredentialSyncer) Run(ctx context.Context) error {
	if err := s.SyncOnce(ctx); err != nil {
		return err
	}
	if s.interval <= 0 {
		return nil
	}
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := s.SyncOnce(ctx); err != nil {
				// Keep serving with the previous credential set.
				log.Printf("s3gw: credential sync failed; keeping previous set: %v", err)
			}
		}
	}
}

// SyncOnce pulls the registry once and applies it. The caller (nufs-s3 main)
// uses it as the startup probe to decide whether the sync path or the legacy
// local credential source is authoritative.
func (s *CredentialSyncer) SyncOnce(ctx context.Context) error {
	creds, err := s.fetch(ctx)
	if err != nil {
		return err
	}
	s.store.ReplaceAll(creds)
	return nil
}
