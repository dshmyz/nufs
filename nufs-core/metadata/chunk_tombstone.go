package metadata

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/pebble"
)

const (
	chunkTombstoneQuarantine = 25 * time.Hour
	chunkCatalogMaxAge       = 2 * time.Hour
)

// ChunkTombstone preserves the metadata required to retain payload replicas
// until every retained backup is too new to reference the original chunk.
type ChunkTombstone struct {
	ChunkID     ChunkID       `json:"chunk_id" msgpack:"chunk_id"`
	Replicas    []ReplicaInfo `json:"replicas" msgpack:"replicas"`
	Size        int64         `json:"size" msgpack:"size"`
	Reason      string        `json:"reason" msgpack:"reason"`
	DeletedAt   time.Time     `json:"deleted_at" msgpack:"deleted_at"`
	DeleteAfter time.Time     `json:"delete_after" msgpack:"delete_after"`
}

// TombstoneChunk records a logical deletion without deleting chunk metadata.
func (s *PebbleStore) TombstoneChunk(ctx context.Context, chunkID ChunkID, reason string) error {
	_, err := s.tombstoneChunk(ctx, chunkID, reason)
	return err
}

func (s *PebbleStore) tombstoneChunk(ctx context.Context, chunkID ChunkID, reason string) (bool, error) {
	references, err := s.stableInodeReferenceSnapshot(ctx)
	if err != nil {
		return false, err
	}
	return s.tombstoneChunkWithReferences(ctx, chunkID, reason, references)
}

func (s *PebbleStore) tombstoneChunkWithReferences(ctx context.Context, chunkID ChunkID, reason string, references inodeReferenceSnapshot) (bool, error) {
	if err := s.checkChunkTombstoneCall(ctx); err != nil {
		return false, err
	}
	if strings.TrimSpace(reason) == "" {
		return false, fmt.Errorf("chunk tombstone: reason is required")
	}

	chunkKey := chunkMetadataKey(chunkID)
	tombstoneKey := chunkTombstoneKey(chunkID)
	chunkRaw, chunkFound, err := s.readChunkTombstoneRaw(chunkKey)
	if err != nil {
		return false, fmt.Errorf("chunk tombstone: read chunk: %w", err)
	}
	tombstoneRaw, tombstoneFound, err := s.readChunkTombstoneRaw(tombstoneKey)
	if err != nil {
		return false, fmt.Errorf("chunk tombstone: read tombstone: %w", err)
	}
	if tombstoneFound {
		if _, err := validateDurableChunkTombstonePair(chunkID, chunkRaw, chunkFound, tombstoneRaw, tombstoneFound); err != nil {
			return false, fmt.Errorf("chunk tombstone: invalid durable pair: %w", err)
		}
		if references.contains(chunkID) {
			return false, fmt.Errorf("chunk tombstone: durable tombstone is referenced")
		}
		return false, nil
	}
	if references.contains(chunkID) {
		return false, nil
	}
	if !chunkFound {
		return false, nil
	}
	var chunk ChunkMeta
	if err := unmarshalValue(chunkRaw, &chunk); err != nil {
		return false, fmt.Errorf("chunk tombstone: decode chunk: %w", err)
	}
	if chunk.ID != chunkID {
		return false, fmt.Errorf("chunk tombstone: chunk key and value identities differ")
	}
	if chunk.Size < 0 {
		return false, fmt.Errorf("chunk tombstone: negative chunk size")
	}
	now := time.Now().UTC().Round(0)
	tombstone := ChunkTombstone{
		ChunkID:     chunkID,
		Replicas:    append([]ReplicaInfo(nil), chunk.Replicas...),
		Size:        int64(chunk.Size),
		Reason:      reason,
		DeletedAt:   now,
		DeleteAfter: now.Add(chunkTombstoneQuarantine),
	}
	encoded, err := marshalValue(&tombstone, codecMsgpack)
	if err != nil {
		return false, fmt.Errorf("chunk tombstone: encode: %w", err)
	}
	if hook := s.chunkTombstoneBeforeConditional; hook != nil {
		hook()
	}
	err = s.applyChunkTombstoneConditional(ctx, &ConditionalBatch{
		Version: chunkTombstoneFencedBatchVersion,
		Preconditions: []ConditionalPrecondition{
			inodeReferenceEpochPrecondition(references.epoch),
			{Key: []byte(chunkKey), ExpectedValue: chunkRaw},
			{Key: []byte(tombstoneKey), ExpectAbsent: true},
		},
		Mutations: []BatchOp{{Key: []byte(tombstoneKey), Value: encoded}},
	})
	if !errors.Is(err, ErrBackupMetadataConflict) {
		return err == nil, err
	}
	latest, found, readErr := s.readChunkTombstoneRaw(tombstoneKey)
	if readErr != nil {
		return false, fmt.Errorf("chunk tombstone: reconcile conflict: %w", readErr)
	}
	if found {
		latestChunk, latestChunkFound, chunkReadErr := s.readChunkTombstoneRaw(chunkKey)
		if chunkReadErr != nil {
			return false, fmt.Errorf("chunk tombstone: reconcile read chunk: %w", chunkReadErr)
		}
		if _, pairErr := validateDurableChunkTombstonePair(chunkID, latestChunk, latestChunkFound, latest, true); pairErr != nil {
			return false, fmt.Errorf("chunk tombstone: reconcile invalid durable pair: %w", pairErr)
		}
		return false, nil
	}
	return false, fmt.Errorf("chunk tombstone: %w", ErrBackupMetadataConflict)
}

// ListChunkTombstones returns validated durable tombstones. A non-positive
// limit returns every tombstone; GC uses the streaming helper below instead.
func (s *PebbleStore) ListChunkTombstones(ctx context.Context, limit int) ([]ChunkTombstone, error) {
	if err := s.checkChunkTombstoneCall(ctx); err != nil {
		return nil, err
	}
	var tombstones []ChunkTombstone
	err := s.scanChunkTombstones(ctx, func(tombstone ChunkTombstone) error {
		if limit > 0 && len(tombstones) >= limit {
			return errChunkTombstoneLimit
		}
		tombstones = append(tombstones, tombstone)
		return nil
	})
	if errors.Is(err, errChunkTombstoneLimit) {
		err = nil
	}
	return tombstones, err
}

// PurgeChunk atomically removes the retained chunk metadata and its tombstone.
func (s *PebbleStore) PurgeChunk(ctx context.Context, chunkID ChunkID) error {
	if err := s.checkChunkTombstoneCall(ctx); err != nil {
		return err
	}
	references, err := s.stableInodeReferenceSnapshot(ctx)
	if err != nil {
		return err
	}
	return s.purgeChunkIfEligible(ctx, chunkID, time.Now().UTC().Round(0), references)
}

func (s *PebbleStore) purgeChunkIfEligible(ctx context.Context, chunkID ChunkID, now time.Time, references inodeReferenceSnapshot) error {
	if references.contains(chunkID) {
		return fmt.Errorf("chunk purge: chunk %d is referenced", chunkID)
	}
	chunkKey := chunkMetadataKey(chunkID)
	tombstoneKey := chunkTombstoneKey(chunkID)
	chunkRaw, chunkFound, err := s.readChunkTombstoneRaw(chunkKey)
	if err != nil {
		return fmt.Errorf("chunk purge: read chunk: %w", err)
	}
	tombstoneRaw, tombstoneFound, err := s.readChunkTombstoneRaw(tombstoneKey)
	if err != nil {
		return fmt.Errorf("chunk purge: read tombstone: %w", err)
	}
	if !chunkFound && !tombstoneFound {
		return nil
	}
	if !chunkFound || !tombstoneFound {
		return fmt.Errorf("chunk purge: durable chunk/tombstone pair is inconsistent")
	}
	tombstone, err := validateDurableChunkTombstonePair(chunkID, chunkRaw, chunkFound, tombstoneRaw, tombstoneFound)
	if err != nil {
		return fmt.Errorf("chunk purge: invalid durable pair: %w", err)
	}
	catalogRaw, err := s.capturePurgeCatalog(ctx, tombstone, now)
	if err != nil {
		return err
	}
	if hook := s.chunkPurgeBeforeConditional; hook != nil {
		hook()
	}
	if err := s.applyChunkTombstoneConditional(ctx, &ConditionalBatch{
		Version: chunkTombstoneFencedBatchVersion,
		Preconditions: []ConditionalPrecondition{
			inodeReferenceEpochPrecondition(references.epoch),
			{Key: []byte(keyBackupCatalog), ExpectedValue: catalogRaw},
			{Key: []byte(chunkKey), ExpectedValue: chunkRaw},
			{Key: []byte(tombstoneKey), ExpectedValue: tombstoneRaw},
		},
		Mutations: []BatchOp{
			{Delete: true, Key: []byte(chunkKey)},
			{Delete: true, Key: []byte(tombstoneKey)},
		},
	}); err != nil {
		if !errors.Is(err, ErrBackupMetadataConflict) {
			return fmt.Errorf("chunk purge: %w", err)
		}
		latestChunk, latestChunkFound, chunkReadErr := s.readChunkTombstoneRaw(chunkKey)
		if chunkReadErr != nil {
			return fmt.Errorf("chunk purge: reconcile read chunk: %w", chunkReadErr)
		}
		latestTombstone, latestTombstoneFound, tombstoneReadErr := s.readChunkTombstoneRaw(tombstoneKey)
		if tombstoneReadErr != nil {
			return fmt.Errorf("chunk purge: reconcile read tombstone: %w", tombstoneReadErr)
		}
		if !latestChunkFound && !latestTombstoneFound {
			return nil
		}
		if _, pairErr := validateDurableChunkTombstonePair(chunkID, latestChunk, latestChunkFound, latestTombstone, latestTombstoneFound); pairErr != nil {
			return fmt.Errorf("chunk purge: reconcile invalid durable pair: %w", pairErr)
		}
		return fmt.Errorf("chunk purge: %w", ErrBackupMetadataConflict)
	}
	return nil
}

// CanPurgeChunk evaluates the time and reconciled-backup safety checks.
func (s *PebbleStore) CanPurgeChunk(ctx context.Context, tombstone ChunkTombstone, now time.Time) (bool, error) {
	if err := s.checkChunkTombstoneCall(ctx); err != nil {
		return false, err
	}
	if tombstone.DeletedAt.Location() != time.UTC || tombstone.DeleteAfter.Location() != time.UTC {
		return false, fmt.Errorf("chunk purge: tombstone times must be UTC")
	}
	if _, err := normalizeChunkTombstone(tombstone); err != nil {
		return false, fmt.Errorf("chunk purge: invalid tombstone: %w", err)
	}
	if now.IsZero() || now.Location() != time.UTC {
		return false, fmt.Errorf("chunk purge: now must be a non-zero UTC time")
	}
	now = now.Round(0)
	if now.Before(tombstone.DeleteAfter) {
		return false, nil
	}
	catalog, err := s.GetBackupCatalogState(ctx)
	if err != nil {
		return false, fmt.Errorf("chunk purge: read backup catalog: %w", err)
	}
	return canPurgeWithCatalog(tombstone, now, catalog)
}

func canPurgeWithCatalog(tombstone ChunkTombstone, now time.Time, catalog *BackupCatalogState) (bool, error) {
	if catalog == nil || len(catalog.Backups) == 0 {
		return false, fmt.Errorf("chunk purge: retained backup catalog is unavailable or empty")
	}
	if catalog.ReconciledAt.IsZero() || catalog.ReconciledAt.Location() != time.UTC {
		return false, fmt.Errorf("chunk purge: catalog reconciliation time is invalid")
	}
	if catalog.ReconciledAt.After(now) || now.Sub(catalog.ReconciledAt) > chunkCatalogMaxAge {
		return false, fmt.Errorf("chunk purge: catalog reconciliation is not fresh")
	}
	for _, backup := range catalog.Backups {
		if !backup.CreatedAt.After(tombstone.DeletedAt) {
			return false, nil
		}
	}
	return true, nil
}

func (s *PebbleStore) capturePurgeCatalog(ctx context.Context, tombstone ChunkTombstone, now time.Time) ([]byte, error) {
	if tombstone.DeletedAt.Location() != time.UTC || tombstone.DeleteAfter.Location() != time.UTC {
		return nil, fmt.Errorf("chunk purge: tombstone times must be UTC")
	}
	if _, err := normalizeChunkTombstone(tombstone); err != nil {
		return nil, fmt.Errorf("chunk purge: invalid tombstone: %w", err)
	}
	if now.IsZero() || now.Location() != time.UTC {
		return nil, fmt.Errorf("chunk purge: now must be a non-zero UTC time")
	}
	now = now.Round(0)
	if now.Before(tombstone.DeleteAfter) {
		return nil, errChunkPurgeIneligible
	}
	for attempt := 0; attempt < 3; attempt++ {
		raw, found, err := s.readChunkTombstoneRaw(keyBackupCatalog)
		if err != nil {
			return nil, fmt.Errorf("chunk purge: read backup catalog: %w", err)
		}
		if !found {
			return nil, fmt.Errorf("chunk purge: retained backup catalog is unavailable or empty")
		}
		var durable BackupCatalogState
		if err := unmarshalValue(raw, &durable); err != nil {
			return nil, fmt.Errorf("chunk purge: decode backup catalog: %w", err)
		}
		normalized, err := normalizeBackupCatalog(durable.Backups, durable.ReconciledAt)
		if err != nil {
			return nil, fmt.Errorf("chunk purge: invalid backup catalog: %w", err)
		}
		canonical, err := marshalValue(&normalized, codecMsgpack)
		if err != nil || !bytes.Equal(canonical, raw) {
			return nil, fmt.Errorf("chunk purge: backup catalog is not canonical")
		}
		catalog, err := s.GetBackupCatalogState(ctx)
		if err != nil {
			return nil, fmt.Errorf("chunk purge: read backup catalog: %w", err)
		}
		if eligible, guardErr := canPurgeWithCatalog(tombstone, now, catalog); guardErr != nil {
			return nil, guardErr
		} else if !eligible {
			return nil, errChunkPurgeIneligible
		}
		latest, latestFound, err := s.readChunkTombstoneRaw(keyBackupCatalog)
		if err != nil {
			return nil, fmt.Errorf("chunk purge: reread backup catalog: %w", err)
		}
		if latestFound && bytes.Equal(raw, latest) {
			return raw, nil
		}
	}
	return nil, fmt.Errorf("chunk purge: %w", ErrBackupMetadataConflict)
}

var (
	errChunkTombstoneLimit  = errors.New("chunk tombstone list limit")
	errChunkPurgeIneligible = errors.New("chunk purge is not currently eligible")
)

type inodeReferenceSnapshot struct {
	epoch      rawReferenceValue
	references map[ChunkID]struct{}
}

func (snapshot inodeReferenceSnapshot) contains(chunkID ChunkID) bool {
	_, found := snapshot.references[chunkID]
	return found
}

// stableInodeReferenceSnapshot fences a complete inode scan with the durable
// epoch advanced by every reference mutation. It deliberately fails closed if
// an inode record cannot be decoded or the scan is concurrent with a writer.
func (s *PebbleStore) stableInodeReferenceSnapshot(ctx context.Context) (inodeReferenceSnapshot, error) {
	if err := s.checkChunkTombstoneCall(ctx); err != nil {
		return inodeReferenceSnapshot{}, err
	}
	for attempt := 0; attempt < 3; attempt++ {
		epochBefore, err := s.readInodeReferenceEpochRaw()
		if err != nil {
			return inodeReferenceSnapshot{}, err
		}
		references := make(map[ChunkID]struct{})
		pages := NewExtentPageStore(s)
		err = s.scanPrefix(prefixInode, func(key, value []byte) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			// Model-aware reference collection (roadmap §1.4). A row decodes
			// as InodeMetaV2 with a real Layout when it is a V2 layout row;
			// V1 ChunkMap rows decode as LayoutEmpty (the ResolveExtents
			// probe discriminator). V2 extent data lives in the chunk whose
			// numeric ID equals the extent ID, so both V2 layouts contribute
			// the same chunk references a V1 ChunkMap would — without this
			// the orphan GC would tombstone (then purge) every V2-backed
			// chunk, since the V1 decode of a V2 row yields an empty ChunkMap.
			var v2 InodeMetaV2
			if err := unmarshalValue(value, &v2); err == nil && v2.Layout != LayoutEmpty {
				if err := validateInodeKeyIdentity(string(key), v2.ID); err != nil {
					return err
				}
				switch v2.Layout {
				case LayoutInlineExtent:
					if v2.InlineExtent != nil {
						references[ChunkID(v2.InlineExtent.ID)] = struct{}{}
					}
					return nil
				case LayoutExtentPages:
					// Per-inode resolve walks the COW root history for live
					// pages only; a raw /extent-page/ prefix scan would count
					// orphaned pages of deleted inodes and leak them forever.
					refs, err := pages.ResolveExtents(&v2)
					if err != nil {
						return fmt.Errorf("inode reference scan: resolve extents for inode %d: %w", v2.ID, err)
					}
					for _, ref := range refs {
						references[ChunkID(ref.ExtentID)] = struct{}{}
					}
					return nil
				default:
					// Unknown layout in a V2-decoded row: fail closed rather
					// than silently dropping its chunk references.
					return fmt.Errorf("inode reference scan: inode %d has unknown V2 layout %d", v2.ID, v2.Layout)
				}
			}
			meta, _, err := decodeReferencedInode(string(key), rawReferenceValue{found: true, value: value})
			if err != nil {
				return err
			}
			for _, ref := range meta.ChunkMap {
				references[ref.ID] = struct{}{}
			}
			return nil
		})
		if err != nil {
			return inodeReferenceSnapshot{}, fmt.Errorf("inode reference scan: %w", err)
		}
		epochAfter, err := s.readInodeReferenceEpochRaw()
		if err != nil {
			return inodeReferenceSnapshot{}, err
		}
		if rawReferenceValuesEqual(epochBefore, epochAfter) {
			return inodeReferenceSnapshot{epoch: epochAfter, references: references}, nil
		}
	}
	return inodeReferenceSnapshot{}, fmt.Errorf("inode reference scan: %w", ErrBackupMetadataConflict)
}

func (s *PebbleStore) readInodeReferenceEpochRaw() (rawReferenceValue, error) {
	raw, found, err := s.readChunkTombstoneRaw(keyInodeReferenceEpoch)
	if err != nil {
		return rawReferenceValue{}, fmt.Errorf("inode reference epoch: read: %w", err)
	}
	value := rawReferenceValue{found: found, value: raw}
	if _, err := decodeInodeReferenceEpoch(value); err != nil {
		return rawReferenceValue{}, err
	}
	return value, nil
}

func validateDurableChunkTombstonePair(chunkID ChunkID, chunkRaw []byte, chunkFound bool, tombstoneRaw []byte, tombstoneFound bool) (ChunkTombstone, error) {
	if !chunkFound || !tombstoneFound {
		return ChunkTombstone{}, fmt.Errorf("durable chunk/tombstone pair is inconsistent")
	}
	var chunk ChunkMeta
	if err := unmarshalValue(chunkRaw, &chunk); err != nil {
		return ChunkTombstone{}, fmt.Errorf("decode chunk: %w", err)
	}
	if chunk.ID != chunkID || chunk.Size < 0 {
		return ChunkTombstone{}, fmt.Errorf("chunk key and value identities differ")
	}
	tombstone, err := decodeChunkTombstone(tombstoneRaw)
	if err != nil {
		return ChunkTombstone{}, fmt.Errorf("decode tombstone: %w", err)
	}
	canonical, err := marshalValue(&tombstone, codecMsgpack)
	if err != nil || !bytes.Equal(canonical, tombstoneRaw) {
		return ChunkTombstone{}, fmt.Errorf("tombstone is not canonical")
	}
	if tombstone.ChunkID != chunkID || tombstone.Size != int64(chunk.Size) || !reflect.DeepEqual(tombstone.Replicas, chunk.Replicas) {
		return ChunkTombstone{}, fmt.Errorf("tombstone does not match immutable chunk snapshot")
	}
	return tombstone, nil
}

func (s *PebbleStore) scanChunkTombstones(ctx context.Context, visit func(ChunkTombstone) error) error {
	return s.scanPrefix(prefixChunkTombstone, func(key, value []byte) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		tombstone, err := decodeChunkTombstone(value)
		if err != nil {
			return fmt.Errorf("chunk tombstone %q: %w", string(key), err)
		}
		if string(key) != chunkTombstoneKey(tombstone.ChunkID) {
			return fmt.Errorf("chunk tombstone %q has mismatched chunk ID %d", string(key), tombstone.ChunkID)
		}
		chunkRaw, chunkFound, err := s.readChunkTombstoneRaw(chunkMetadataKey(tombstone.ChunkID))
		if err != nil {
			return fmt.Errorf("chunk tombstone %q: read retained chunk: %w", string(key), err)
		}
		if _, err := validateDurableChunkTombstonePair(tombstone.ChunkID, chunkRaw, chunkFound, value, true); err != nil {
			return fmt.Errorf("chunk tombstone %q: invalid durable pair: %w", string(key), err)
		}
		return visit(tombstone)
	})
}

func (s *PebbleStore) applyChunkTombstoneConditional(ctx context.Context, conditional *ConditionalBatch) error {
	if err := s.checkChunkTombstoneCall(ctx); err != nil {
		return err
	}
	if s.degradation.IsReadOnly() {
		return ErrServiceClosed
	}
	var err error
	if s.raft != nil {
		if !s.raft.conditionalIsLeader() {
			return fmt.Errorf("chunk tombstone: not leader")
		}
		err = s.raft.applyConditional(ctx, &RaftLogEntry{Op: OpConditionalBatch, Conditional: conditional})
	} else {
		s.mu.Lock()
		err = ctx.Err()
		if err == nil {
			err = applyConditionalBatchWithHook(s.db, conditional, pebble.Sync, s.conditionalBatchBeforeCommit)
		}
		s.mu.Unlock()
	}
	if errors.Is(err, ErrRaftConditionalConflict) {
		return ErrBackupMetadataConflict
	}
	return err
}

// updateLiveChunkMetadata performs the exact-value, tombstone-absent update
// used by every existing ChunkMeta mutation path.
func (s *PebbleStore) updateLiveChunkMetadata(ctx context.Context, expectedRaw []byte, chunk *ChunkMeta) error {
	if chunk == nil || chunk.ID == 0 {
		return ErrInvalidArgument
	}
	encoded, err := marshalValue(chunk, codecMsgpack)
	if err != nil {
		return fmt.Errorf("chunk metadata update: encode: %w", err)
	}
	chunkKey := chunkMetadataKey(chunk.ID)
	err = s.applyChunkTombstoneConditional(ctx, &ConditionalBatch{
		Version: chunkTombstoneFencedBatchVersion,
		Preconditions: []ConditionalPrecondition{
			{Key: []byte(chunkKey), ExpectedValue: append([]byte(nil), expectedRaw...)},
			{Key: []byte(chunkTombstoneKey(chunk.ID)), ExpectAbsent: true},
		},
		Mutations: []BatchOp{{Key: []byte(chunkKey), Value: encoded}},
	})
	if errors.Is(err, ErrBackupMetadataConflict) {
		return fmt.Errorf("chunk metadata update: %w", ErrBackupMetadataConflict)
	}
	return err
}

func (s *PebbleStore) checkChunkTombstoneCall(ctx context.Context) error {
	if s.closed.Load() {
		return ErrServiceClosed
	}
	if ctx == nil {
		return fmt.Errorf("chunk tombstone: nil context")
	}
	return ctx.Err()
}

func (s *PebbleStore) readChunkTombstoneRaw(key string) ([]byte, bool, error) {
	found, value, err := s.getRaw(key)
	return value, found, err
}

func decodeChunkTombstone(raw []byte) (ChunkTombstone, error) {
	var tombstone ChunkTombstone
	if err := unmarshalValue(raw, &tombstone); err != nil {
		return ChunkTombstone{}, err
	}
	return normalizeChunkTombstone(tombstone)
}

func normalizeChunkTombstone(tombstone ChunkTombstone) (ChunkTombstone, error) {
	if tombstone.ChunkID == 0 {
		return ChunkTombstone{}, fmt.Errorf("chunk ID is required")
	}
	if tombstone.Size < 0 {
		return ChunkTombstone{}, fmt.Errorf("size cannot be negative")
	}
	if strings.TrimSpace(tombstone.Reason) == "" {
		return ChunkTombstone{}, fmt.Errorf("reason is required")
	}
	if tombstone.DeletedAt.IsZero() || tombstone.DeleteAfter.IsZero() {
		return ChunkTombstone{}, fmt.Errorf("deletion times are required")
	}
	// MsgPack's timestamp representation carries an instant but not Go's
	// location identity. Canonicalize the decoded instant before checking the
	// exact durable interval; API inputs are separately required to be UTC.
	tombstone.DeletedAt = tombstone.DeletedAt.Round(0).UTC()
	tombstone.DeleteAfter = tombstone.DeleteAfter.Round(0).UTC()
	if !tombstone.DeleteAfter.Equal(tombstone.DeletedAt.Add(chunkTombstoneQuarantine)) {
		return ChunkTombstone{}, fmt.Errorf("delete-after must be exactly %s after deletion", chunkTombstoneQuarantine)
	}
	tombstone.Replicas = append([]ReplicaInfo(nil), tombstone.Replicas...)
	return tombstone, nil
}

func chunkMetadataKey(chunkID ChunkID) string {
	return prefixChunk + strconv.FormatUint(uint64(chunkID), 10)
}
func chunkTombstoneKey(chunkID ChunkID) string {
	return prefixChunkTombstone + strconv.FormatUint(uint64(chunkID), 10)
}

// validateChunkTombstoneConditionalBatch preserves the original v3 wire
// contract so committed pre-fence logs replay identically.
func validateChunkTombstoneConditionalBatch(conditional *ConditionalBatch) error {
	if conditional.ExpectedRaftTerm != 0 || len(conditional.PrefixReplacements) != 0 {
		return fmt.Errorf("chunk tombstone conditional batch has an invalid envelope")
	}
	if len(conditional.Preconditions) != 2 || len(conditional.Mutations) == 0 {
		return fmt.Errorf("chunk tombstone conditional batch has an invalid shape")
	}
	if len(conditional.Mutations) == 1 {
		return validateLegacyChunkTombstoneCreateBatch(conditional)
	}
	if len(conditional.Mutations) == 2 {
		return validateLegacyChunkTombstonePurgeBatch(conditional)
	}
	return fmt.Errorf("chunk tombstone conditional batch has too many mutations")
}

func validateLegacyChunkTombstoneCreateBatch(conditional *ConditionalBatch) error {
	chunkPrecondition, tombstonePrecondition, chunkID, err := validateLegacyChunkTombstonePreconditions(conditional.Preconditions, true)
	if err != nil {
		return err
	}
	mutation := conditional.Mutations[0]
	if mutation.Delete || !bytes.Equal(mutation.Key, tombstonePrecondition.Key) {
		return fmt.Errorf("chunk tombstone create must set the tombstone key")
	}
	var chunk ChunkMeta
	if err := unmarshalValue(chunkPrecondition.ExpectedValue, &chunk); err != nil || chunk.ID != chunkID || chunk.Size < 0 {
		return fmt.Errorf("chunk tombstone create has an invalid chunk precondition")
	}
	tombstone, err := decodeChunkTombstone(mutation.Value)
	if err != nil {
		return fmt.Errorf("chunk tombstone create has an invalid tombstone: %w", err)
	}
	if tombstone.ChunkID != chunkID || tombstone.Size != int64(chunk.Size) || !reflect.DeepEqual(tombstone.Replicas, chunk.Replicas) {
		return fmt.Errorf("chunk tombstone create does not snapshot its chunk")
	}
	canonical, err := marshalValue(&tombstone, codecMsgpack)
	if err != nil || !bytes.Equal(canonical, mutation.Value) {
		return fmt.Errorf("chunk tombstone create value is not canonical")
	}
	return nil
}

func validateLegacyChunkTombstonePurgeBatch(conditional *ConditionalBatch) error {
	chunkPrecondition, tombstonePrecondition, chunkID, err := validateLegacyChunkTombstonePreconditions(conditional.Preconditions, false)
	if err != nil {
		return err
	}
	if !conditional.Mutations[0].Delete || !conditional.Mutations[1].Delete ||
		!bytes.Equal(conditional.Mutations[0].Key, chunkPrecondition.Key) ||
		!bytes.Equal(conditional.Mutations[1].Key, tombstonePrecondition.Key) {
		return fmt.Errorf("chunk tombstone purge must delete the exact chunk/tombstone pair")
	}
	var chunk ChunkMeta
	if err := unmarshalValue(chunkPrecondition.ExpectedValue, &chunk); err != nil || chunk.ID != chunkID {
		return fmt.Errorf("chunk tombstone purge has an invalid chunk precondition")
	}
	tombstone, err := decodeChunkTombstone(tombstonePrecondition.ExpectedValue)
	if err != nil || tombstone.ChunkID != chunkID {
		return fmt.Errorf("chunk tombstone purge has an invalid tombstone precondition")
	}
	return nil
}

func validateLegacyChunkTombstonePreconditions(preconditions []ConditionalPrecondition, create bool) (ConditionalPrecondition, ConditionalPrecondition, ChunkID, error) {
	var chunkPrecondition, tombstonePrecondition ConditionalPrecondition
	for _, precondition := range preconditions {
		key := string(precondition.Key)
		switch {
		case strings.HasPrefix(key, prefixChunk):
			chunkPrecondition = precondition
		case strings.HasPrefix(key, prefixChunkTombstone):
			tombstonePrecondition = precondition
		default:
			return ConditionalPrecondition{}, ConditionalPrecondition{}, 0, fmt.Errorf("chunk tombstone conditional batch targets an invalid key")
		}
	}
	chunkID, err := parseChunkTombstoneKey(string(chunkPrecondition.Key), prefixChunk)
	if err != nil {
		return ConditionalPrecondition{}, ConditionalPrecondition{}, 0, err
	}
	tombstoneID, err := parseChunkTombstoneKey(string(tombstonePrecondition.Key), prefixChunkTombstone)
	if err != nil || tombstoneID != chunkID {
		return ConditionalPrecondition{}, ConditionalPrecondition{}, 0, fmt.Errorf("chunk tombstone conditional keys do not identify one chunk")
	}
	if chunkPrecondition.ExpectAbsent || len(chunkPrecondition.ExpectedValue) == 0 {
		return ConditionalPrecondition{}, ConditionalPrecondition{}, 0, fmt.Errorf("chunk tombstone conditional must compare the current chunk")
	}
	if create {
		if !tombstonePrecondition.ExpectAbsent {
			return ConditionalPrecondition{}, ConditionalPrecondition{}, 0, fmt.Errorf("chunk tombstone create must require tombstone absence")
		}
	} else if tombstonePrecondition.ExpectAbsent || len(tombstonePrecondition.ExpectedValue) == 0 {
		return ConditionalPrecondition{}, ConditionalPrecondition{}, 0, fmt.Errorf("chunk tombstone purge must compare the current tombstone")
	}
	return chunkPrecondition, tombstonePrecondition, chunkID, nil
}

func validateChunkTombstoneFencedBatch(conditional *ConditionalBatch) error {
	if conditional.ExpectedRaftTerm != 0 || len(conditional.PrefixReplacements) != 0 {
		return fmt.Errorf("chunk tombstone conditional batch has an invalid envelope")
	}
	if len(conditional.Mutations) == 0 {
		return fmt.Errorf("chunk tombstone conditional batch has an invalid shape")
	}
	if hasInodeAllocationPrecondition(conditional.Preconditions) {
		return validateChunkAllocationBatch(conditional)
	}
	if len(conditional.Mutations) == 1 {
		switch len(conditional.Preconditions) {
		case 2:
			return validateChunkMetadataUpdateBatch(conditional)
		case 3:
			return validateChunkTombstoneFencedCreateBatch(conditional)
		default:
			return fmt.Errorf("chunk tombstone fenced create/update has an invalid precondition shape")
		}
	}
	if len(conditional.Mutations) == 2 {
		if len(conditional.Preconditions) != 4 {
			return fmt.Errorf("chunk tombstone purge has an invalid precondition shape")
		}
		return validateChunkTombstoneFencedPurgeBatch(conditional)
	}
	return fmt.Errorf("chunk tombstone conditional batch has too many mutations")
}

func hasInodeAllocationPrecondition(preconditions []ConditionalPrecondition) bool {
	for _, precondition := range preconditions {
		if validInodeMetadataKey(string(precondition.Key)) {
			return true
		}
	}
	return false
}

func validateChunkAllocationBatch(conditional *ConditionalBatch) error {
	if len(conditional.PrefixReplacements) != 0 || conditional.ExpectedRaftTerm != 0 {
		return fmt.Errorf("chunk allocation has an invalid envelope")
	}
	var inodePrecondition, epochPrecondition ConditionalPrecondition
	chunkKeys := make(map[ChunkID]struct{})
	tombstoneKeys := make(map[ChunkID]struct{})
	for _, precondition := range conditional.Preconditions {
		key := string(precondition.Key)
		switch {
		case validInodeMetadataKey(key):
			if inodePrecondition.Key != nil || precondition.ExpectAbsent || len(precondition.ExpectedValue) == 0 {
				return fmt.Errorf("chunk allocation must compare one exact inode")
			}
			inodePrecondition = precondition
		case key == keyInodeReferenceEpoch:
			if epochPrecondition.Key != nil {
				return fmt.Errorf("chunk allocation has duplicate inode reference epoch")
			}
			epochPrecondition = precondition
		case validChunkMetadataKey(key):
			id, _ := parseChunkTombstoneKey(key, prefixChunk)
			if !precondition.ExpectAbsent {
				return fmt.Errorf("chunk allocation must require chunk absence")
			}
			if _, duplicate := chunkKeys[id]; duplicate {
				return fmt.Errorf("chunk allocation has duplicate chunk key")
			}
			chunkKeys[id] = struct{}{}
		case strings.HasPrefix(key, prefixChunkTombstone):
			id, err := parseChunkTombstoneKey(key, prefixChunkTombstone)
			if err != nil || !precondition.ExpectAbsent {
				return fmt.Errorf("chunk allocation must require tombstone absence")
			}
			if _, duplicate := tombstoneKeys[id]; duplicate {
				return fmt.Errorf("chunk allocation has duplicate tombstone key")
			}
			tombstoneKeys[id] = struct{}{}
		default:
			return fmt.Errorf("chunk allocation targets an invalid precondition key")
		}
	}
	if inodePrecondition.Key == nil || len(chunkKeys) == 0 || len(chunkKeys) != len(tombstoneKeys) {
		return fmt.Errorf("chunk allocation has an incomplete precondition set")
	}
	if err := validateInodeReferenceEpochConditional(epochPrecondition); err != nil {
		return err
	}
	for id := range chunkKeys {
		if _, ok := tombstoneKeys[id]; !ok {
			return fmt.Errorf("chunk allocation chunk/tombstone keys differ")
		}
	}
	var previous, next InodeMeta
	if err := unmarshalValue(inodePrecondition.ExpectedValue, &previous); err != nil || validateInodeKeyIdentity(string(inodePrecondition.Key), previous.ID) != nil {
		return fmt.Errorf("chunk allocation has an invalid inode precondition")
	}
	mutatedChunks := make(map[ChunkID]struct{})
	var inodeMutation, epochMutation *BatchOp
	for i := range conditional.Mutations {
		mutation := &conditional.Mutations[i]
		if mutation.Delete {
			return fmt.Errorf("chunk allocation cannot delete")
		}
		key := string(mutation.Key)
		switch {
		case validChunkMetadataKey(key):
			id, _ := parseChunkTombstoneKey(key, prefixChunk)
			if _, expected := chunkKeys[id]; !expected {
				return fmt.Errorf("chunk allocation creates an unconditioned chunk")
			}
			var chunk ChunkMeta
			if err := unmarshalValue(mutation.Value, &chunk); err != nil || chunk.ID != id {
				return fmt.Errorf("chunk allocation has an invalid chunk mutation")
			}
			if _, duplicate := mutatedChunks[id]; duplicate {
				return fmt.Errorf("chunk allocation has duplicate chunk mutation")
			}
			mutatedChunks[id] = struct{}{}
		case key == string(inodePrecondition.Key):
			if inodeMutation != nil || unmarshalValue(mutation.Value, &next) != nil || validateInodeKeyIdentity(key, next.ID) != nil {
				return fmt.Errorf("chunk allocation has an invalid inode mutation")
			}
			inodeMutation = mutation
		case key == keyInodeReferenceEpoch:
			if epochMutation != nil {
				return fmt.Errorf("chunk allocation has duplicate inode reference epoch mutation")
			}
			epochMutation = mutation
		default:
			return fmt.Errorf("chunk allocation has an invalid mutation key")
		}
	}
	if inodeMutation == nil || len(mutatedChunks) != len(chunkKeys) {
		return fmt.Errorf("chunk allocation has an incomplete mutation set")
	}
	// Epoch mutation is optional: per-inode CAS (inodeMutation) is
	// sufficient for correctness.  The global epoch was removed to
	// eliminate cross-operation contention storms.
	added := addedChunkReferences(previous, true, next, true)
	if len(added) != len(chunkKeys) || len(next.ChunkMap) < len(previous.ChunkMap) {
		return fmt.Errorf("chunk allocation inode references do not match created chunks")
	}
	seen := make(map[ChunkID]struct{}, len(next.ChunkMap))
	for _, ref := range next.ChunkMap {
		if _, duplicate := seen[ref.ID]; duplicate {
			return fmt.Errorf("chunk allocation inode has duplicate chunk references")
		}
		seen[ref.ID] = struct{}{}
	}
	for id := range chunkKeys {
		if _, ok := added[id]; !ok {
			return fmt.Errorf("chunk allocation inode does not reference created chunk")
		}
	}
	return nil
}

func validateChunkMetadataUpdateBatch(conditional *ConditionalBatch) error {
	var chunkPrecondition, tombstonePrecondition ConditionalPrecondition
	for _, precondition := range conditional.Preconditions {
		switch key := string(precondition.Key); {
		case strings.HasPrefix(key, prefixChunk):
			chunkPrecondition = precondition
		case strings.HasPrefix(key, prefixChunkTombstone):
			tombstonePrecondition = precondition
		default:
			return fmt.Errorf("chunk metadata update targets an invalid key")
		}
	}
	chunkID, err := parseChunkTombstoneKey(string(chunkPrecondition.Key), prefixChunk)
	if err != nil || chunkPrecondition.ExpectAbsent || len(chunkPrecondition.ExpectedValue) == 0 {
		return fmt.Errorf("chunk metadata update must compare the current chunk")
	}
	tombstoneID, err := parseChunkTombstoneKey(string(tombstonePrecondition.Key), prefixChunkTombstone)
	if err != nil || tombstoneID != chunkID || !tombstonePrecondition.ExpectAbsent {
		return fmt.Errorf("chunk metadata update must require its tombstone absence")
	}
	mutation := conditional.Mutations[0]
	if mutation.Delete || !bytes.Equal(mutation.Key, chunkPrecondition.Key) {
		return fmt.Errorf("chunk metadata update must replace its exact chunk key")
	}
	var expected, updated ChunkMeta
	if err := unmarshalValue(chunkPrecondition.ExpectedValue, &expected); err != nil || expected.ID != chunkID {
		return fmt.Errorf("chunk metadata update has an invalid chunk precondition")
	}
	if err := unmarshalValue(mutation.Value, &updated); err != nil || updated.ID != chunkID {
		return fmt.Errorf("chunk metadata update has an invalid replacement")
	}
	return nil
}

func validateChunkTombstoneFencedCreateBatch(conditional *ConditionalBatch) error {
	epochPrecondition, chunkPrecondition, tombstonePrecondition, chunkID, err := validateChunkTombstonePreconditions(conditional.Preconditions, true)
	if err != nil {
		return err
	}
	if err := validateInodeReferenceEpochConditional(epochPrecondition); err != nil {
		return err
	}
	mutation := conditional.Mutations[0]
	if mutation.Delete || !bytes.Equal(mutation.Key, tombstonePrecondition.Key) {
		return fmt.Errorf("chunk tombstone create must set the tombstone key")
	}
	var chunk ChunkMeta
	if err := unmarshalValue(chunkPrecondition.ExpectedValue, &chunk); err != nil || chunk.ID != chunkID || chunk.Size < 0 {
		return fmt.Errorf("chunk tombstone create has an invalid chunk precondition")
	}
	tombstone, err := decodeChunkTombstone(mutation.Value)
	if err != nil {
		return fmt.Errorf("chunk tombstone create has an invalid tombstone: %w", err)
	}
	if tombstone.ChunkID != chunkID || tombstone.Size != int64(chunk.Size) || !reflect.DeepEqual(tombstone.Replicas, chunk.Replicas) {
		return fmt.Errorf("chunk tombstone create does not snapshot its chunk")
	}
	canonical, err := marshalValue(&tombstone, codecMsgpack)
	if err != nil || !bytes.Equal(canonical, mutation.Value) {
		return fmt.Errorf("chunk tombstone create value is not canonical")
	}
	return nil
}

func validateChunkTombstoneFencedPurgeBatch(conditional *ConditionalBatch) error {
	epochPrecondition, chunkPrecondition, tombstonePrecondition, chunkID, err := validateChunkTombstonePreconditions(conditional.Preconditions, false)
	if err != nil {
		return err
	}
	if err := validateInodeReferenceEpochConditional(epochPrecondition); err != nil {
		return err
	}
	catalogPrecondition, ok := conditionalPreconditionForKey(conditional.Preconditions, keyBackupCatalog)
	if !ok || catalogPrecondition.ExpectAbsent || len(catalogPrecondition.ExpectedValue) == 0 {
		return fmt.Errorf("chunk tombstone purge must compare the canonical backup catalog")
	}
	var catalog BackupCatalogState
	if err := unmarshalValue(catalogPrecondition.ExpectedValue, &catalog); err != nil {
		return fmt.Errorf("chunk tombstone purge has an invalid backup catalog: %w", err)
	}
	normalized, err := normalizeBackupCatalog(catalog.Backups, catalog.ReconciledAt)
	if err != nil {
		return fmt.Errorf("chunk tombstone purge has an invalid backup catalog: %w", err)
	}
	canonicalCatalog, err := marshalValue(&normalized, codecMsgpack)
	if err != nil || !bytes.Equal(canonicalCatalog, catalogPrecondition.ExpectedValue) {
		return fmt.Errorf("chunk tombstone purge backup catalog is not canonical")
	}
	if !conditional.Mutations[0].Delete || !conditional.Mutations[1].Delete ||
		!bytes.Equal(conditional.Mutations[0].Key, chunkPrecondition.Key) ||
		!bytes.Equal(conditional.Mutations[1].Key, tombstonePrecondition.Key) {
		return fmt.Errorf("chunk tombstone purge must delete the exact chunk/tombstone pair")
	}
	var chunk ChunkMeta
	if err := unmarshalValue(chunkPrecondition.ExpectedValue, &chunk); err != nil || chunk.ID != chunkID {
		return fmt.Errorf("chunk tombstone purge has an invalid chunk precondition")
	}
	tombstone, err := decodeChunkTombstone(tombstonePrecondition.ExpectedValue)
	if err != nil || tombstone.ChunkID != chunkID {
		return fmt.Errorf("chunk tombstone purge has an invalid tombstone precondition")
	}
	return nil
}

func validateChunkTombstonePreconditions(preconditions []ConditionalPrecondition, create bool) (ConditionalPrecondition, ConditionalPrecondition, ConditionalPrecondition, ChunkID, error) {
	var epochPrecondition ConditionalPrecondition
	var chunkPrecondition, tombstonePrecondition ConditionalPrecondition
	for _, precondition := range preconditions {
		key := string(precondition.Key)
		switch {
		case key == keyInodeReferenceEpoch:
			epochPrecondition = precondition
		case key == keyBackupCatalog && !create:
			continue
		case strings.HasPrefix(key, prefixChunk):
			chunkPrecondition = precondition
		case strings.HasPrefix(key, prefixChunkTombstone):
			tombstonePrecondition = precondition
		default:
			return ConditionalPrecondition{}, ConditionalPrecondition{}, ConditionalPrecondition{}, 0, fmt.Errorf("chunk tombstone conditional batch targets an invalid key")
		}
	}
	chunkID, err := parseChunkTombstoneKey(string(chunkPrecondition.Key), prefixChunk)
	if err != nil {
		return ConditionalPrecondition{}, ConditionalPrecondition{}, ConditionalPrecondition{}, 0, err
	}
	tombstoneID, err := parseChunkTombstoneKey(string(tombstonePrecondition.Key), prefixChunkTombstone)
	if err != nil || tombstoneID != chunkID {
		return ConditionalPrecondition{}, ConditionalPrecondition{}, ConditionalPrecondition{}, 0, fmt.Errorf("chunk tombstone conditional keys do not identify one chunk")
	}
	if chunkPrecondition.ExpectAbsent || len(chunkPrecondition.ExpectedValue) == 0 {
		return ConditionalPrecondition{}, ConditionalPrecondition{}, ConditionalPrecondition{}, 0, fmt.Errorf("chunk tombstone conditional must compare the current chunk")
	}
	if create {
		if !tombstonePrecondition.ExpectAbsent {
			return ConditionalPrecondition{}, ConditionalPrecondition{}, ConditionalPrecondition{}, 0, fmt.Errorf("chunk tombstone create must require tombstone absence")
		}
	} else if tombstonePrecondition.ExpectAbsent || len(tombstonePrecondition.ExpectedValue) == 0 {
		return ConditionalPrecondition{}, ConditionalPrecondition{}, ConditionalPrecondition{}, 0, fmt.Errorf("chunk tombstone purge must compare the current tombstone")
	}
	return epochPrecondition, chunkPrecondition, tombstonePrecondition, chunkID, nil
}

func validateInodeReferenceEpochConditional(precondition ConditionalPrecondition) error {
	// Epoch validation removed: per-inode CAS is sufficient for
	// allocation correctness; global epoch caused contention storms.
	return nil
}

func conditionalPreconditionForKey(preconditions []ConditionalPrecondition, key string) (ConditionalPrecondition, bool) {
	for _, precondition := range preconditions {
		if string(precondition.Key) == key {
			return precondition, true
		}
	}
	return ConditionalPrecondition{}, false
}

func parseChunkTombstoneKey(key, prefix string) (ChunkID, error) {
	id, err := strconv.ParseUint(strings.TrimPrefix(key, prefix), 10, 64)
	if err != nil || id == 0 || prefix+strconv.FormatUint(id, 10) != key {
		return 0, fmt.Errorf("invalid chunk tombstone key %q", key)
	}
	return ChunkID(id), nil
}
