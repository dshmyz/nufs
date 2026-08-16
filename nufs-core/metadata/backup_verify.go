package metadata

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/cockroachdb/pebble"
)

type BackupVerificationCheck struct {
	Name   string `json:"name"`
	Passed bool   `json:"passed"`
}

type BackupVerificationReport struct {
	ManifestValid bool                      `json:"manifest_valid"`
	FilesVerified int                       `json:"files_verified"`
	BytesVerified int64                     `json:"bytes_verified"`
	RecordCounts  BackupRecordCounts        `json:"record_counts"`
	Checks        []BackupVerificationCheck `json:"checks"`
}

func VerifyBackupArtifact(ctx context.Context, checkpointDir string, manifest *BackupManifest) (*BackupVerificationReport, error) {
	report := newBackupVerificationReport()
	if err := manifest.Validate(); err != nil {
		return report, err
	}
	report.ManifestValid = true
	report.setCheck("manifest", true)
	files, totalBytes, err := collectBackupFiles(ctx, checkpointDir)
	if err != nil {
		return report, err
	}
	if err := verifyBackupFiles(manifest.Files, files, manifest.TotalBytes, totalBytes); err != nil {
		return report, err
	}
	report.FilesVerified = len(files)
	report.BytesVerified = totalBytes
	report.setCheck("file_set", true)
	report.setCheck("checksums", true)

	counts, err := inspectBackupCheckpoint(ctx, checkpointDir)
	if err != nil {
		return report, err
	}
	report.RecordCounts = counts
	report.setCheck("pebble_read_only", true)
	report.setCheck("durable_values", true)
	report.setCheck("root_inode", true)
	report.setCheck("bucket_roots", true)
	report.setCheck("directory_entries", true)
	report.setCheck("inode_chunk_references", true)
	report.setCheck("extent_references", true)
	if err := compareBackupRecordCounts(manifest.RecordCounts, counts); err != nil {
		return report, err
	}
	report.setCheck("record_counts", true)
	return report, nil
}

func newBackupVerificationReport() *BackupVerificationReport {
	names := []string{"manifest", "file_set", "checksums", "pebble_read_only", "durable_values", "root_inode", "bucket_roots", "directory_entries", "inode_chunk_references", "extent_references", "record_counts"}
	report := &BackupVerificationReport{Checks: make([]BackupVerificationCheck, len(names))}
	for i, name := range names {
		report.Checks[i].Name = name
	}
	return report
}

func (r *BackupVerificationReport) setCheck(name string, passed bool) {
	for i := range r.Checks {
		if r.Checks[i].Name == name {
			r.Checks[i].Passed = passed
			return
		}
	}
}

func verifyBackupFiles(expected, actual []BackupFile, expectedTotal, actualTotal int64) error {
	if expectedTotal != actualTotal {
		return fmt.Errorf("backup artifact: total bytes mismatch: manifest=%d actual=%d", expectedTotal, actualTotal)
	}
	actualByPath := make(map[string]BackupFile, len(actual))
	for _, file := range actual {
		actualByPath[file.Path] = file
	}
	if len(expected) != len(actualByPath) {
		return fmt.Errorf("backup artifact: file set mismatch: manifest=%d actual=%d", len(expected), len(actualByPath))
	}
	for _, expectedFile := range expected {
		actualFile, ok := actualByPath[expectedFile.Path]
		if !ok {
			return fmt.Errorf("backup artifact: declared file %q missing", expectedFile.Path)
		}
		if actualFile.Size != expectedFile.Size || actualFile.SHA256 != expectedFile.SHA256 {
			return fmt.Errorf("backup artifact: checksum mismatch for %q", expectedFile.Path)
		}
	}
	return nil
}

func inspectBackupCheckpoint(ctx context.Context, checkpointDir string) (BackupRecordCounts, error) {
	db, err := pebble.Open(checkpointDir, &pebble.Options{ReadOnly: true})
	if err != nil {
		return BackupRecordCounts{}, fmt.Errorf("backup artifact: open read-only pebble checkpoint: %w", err)
	}
	defer db.Close()

	state := backupCheckpointState{
		inodes:        make(map[InodeID]struct{}),
		chunks:        make(map[ChunkID]struct{}),
		extentMetaIDs: make(map[ExtentIDV2]struct{}),
	}
	for _, spec := range backupPrefixSpecs(ctx, &state, db) {
		if err := scanBackupPrefix(ctx, db, spec.prefix, spec.decode); err != nil {
			return BackupRecordCounts{}, err
		}
	}
	if _, ok := state.inodes[RootInodeID]; !ok {
		return BackupRecordCounts{}, fmt.Errorf("backup artifact: root inode %d missing", RootInodeID)
	}
	for _, root := range state.bucketRoots {
		if _, ok := state.inodes[root]; !ok {
			return BackupRecordCounts{}, fmt.Errorf("backup artifact: bucket root inode %d missing", root)
		}
	}
	for _, entry := range state.directoryEntries {
		if _, ok := state.inodes[entry.InodeID]; !ok {
			return BackupRecordCounts{}, fmt.Errorf("backup artifact: directory entry references missing inode %d", entry.InodeID)
		}
	}
	for _, parent := range state.namespaceParents {
		if _, ok := state.inodes[parent]; !ok {
			return BackupRecordCounts{}, fmt.Errorf("backup artifact: namespace parent inode %d missing", parent)
		}
	}
	for _, ref := range state.chunkRefs {
		if _, ok := state.chunks[ref.ID]; !ok {
			return BackupRecordCounts{}, fmt.Errorf("backup artifact: inode chunk reference lacks chunk metadata %d", ref.ID)
		}
	}
	for _, ext := range state.extentReferencedIDs {
		if _, ok := state.chunks[ChunkID(ext)]; !ok {
			return BackupRecordCounts{}, fmt.Errorf("backup artifact: V2 extent reference lacks chunk metadata %d", ext)
		}
		if _, ok := state.extentMetaIDs[ext]; !ok {
			return BackupRecordCounts{}, fmt.Errorf("backup artifact: V2 extent reference lacks extent metadata %d", ext)
		}
	}
	return state.counts, nil
}

type backupCheckpointState struct {
	counts              BackupRecordCounts
	inodes              map[InodeID]struct{}
	chunks              map[ChunkID]struct{}
	bucketRoots         []InodeID
	directoryEntries    []DirEntry
	namespaceParents    []InodeID
	chunkRefs           []ChunkRef
	extentReferencedIDs []ExtentIDV2
	extentMetaIDs       map[ExtentIDV2]struct{}
}

type backupPrefixSpec struct {
	prefix string
	decode func(key, value []byte) error
}

func backupPrefixSpecs(ctx context.Context, state *backupCheckpointState, db *pebble.DB) []backupPrefixSpec {
	decode := func(value []byte, into any, count *int64) error {
		if err := unmarshalValue(value, into); err != nil {
			return err
		}
		*count = *count + 1
		return nil
	}
	return []backupPrefixSpec{
		{prefixBucket, func(_ []byte, value []byte) error {
			var v BucketInfo
			if err := decode(value, &v, &state.counts.Buckets); err != nil {
				return err
			}
			state.bucketRoots = append(state.bucketRoots, v.RootInode)
			return nil
		}},
		{prefixBucketByRoot, func(key, value []byte) error {
			var v string
			if err := decode(value, &v, &state.counts.BucketByRoot); err != nil {
				return err
			}
			id, err := keyID(key, prefixBucketByRoot)
			if err != nil {
				return fmt.Errorf("invalid bucket-by-root key: %w", err)
			}
			state.bucketRoots = append(state.bucketRoots, InodeID(id))
			return nil
		}},
		{prefixBucketStats, func(_ []byte, value []byte) error {
			var v BucketUsage
			return decode(value, &v, &state.counts.BucketStats)
		}},
		{prefixNS, func(key []byte, value []byte) error {
			parent, err := namespaceParentID(key)
			if err != nil {
				return err
			}
			var v DirEntry
			if err := decode(value, &v, &state.counts.DirectoryEntries); err != nil {
				return err
			}
			state.directoryEntries = append(state.directoryEntries, v)
			state.namespaceParents = append(state.namespaceParents, parent)
			return nil
		}},
		{prefixInode, func(key []byte, value []byte) error {
			id, err := keyID(key, prefixInode)
			if err != nil {
				return err
			}
			// V2-first decode: a V2-layout inode's data lives in extents
			// (InlineExtent or COW pages) whose backing chunk rows use
			// ID == ExtentID. Decoding only V1 InodeMeta would yield an
			// empty ChunkMap for these rows and silently skip them in the
			// inode_chunk_references cross-check — exactly the hole this
			// knife closes. V1 rows and empty V2 rows decode Layout==0 and
			// fall through to the legacy ChunkMap path.
			var v2 InodeMetaV2
			if err := decode(value, &v2, &state.counts.Inodes); err != nil {
				return err
			}
			if v2.ID != InodeID(id) {
				return fmt.Errorf("inode key ID %d does not match value ID %d", id, v2.ID)
			}
			state.inodes[InodeID(id)] = struct{}{}
			switch v2.Layout {
			case LayoutInlineExtent:
				if v2.InlineExtent == nil {
					return fmt.Errorf("V2 inline inode %d lacks inline extent", id)
				}
				state.extentReferencedIDs = append(state.extentReferencedIDs, v2.InlineExtent.ID)
			case LayoutExtentPages:
				refs, err := backupResolveExtents(ctx, db, &v2)
				if err != nil {
					return err
				}
				for _, r := range refs {
					state.extentReferencedIDs = append(state.extentReferencedIDs, r.ExtentID)
				}
			default:
				var v1 InodeMeta
				if err := unmarshalValue(value, &v1); err != nil {
					return err
				}
				state.chunkRefs = append(state.chunkRefs, v1.ChunkMap...)
			}
			return nil
		}},
		{prefixChunk, func(key []byte, value []byte) error {
			id, err := keyID(key, prefixChunk)
			if err != nil {
				return err
			}
			var v ChunkMeta
			if err := decode(value, &v, &state.counts.Chunks); err != nil {
				return err
			}
			if v.ID != ChunkID(id) {
				return fmt.Errorf("chunk key ID %d does not match value ID %d", id, v.ID)
			}
			state.chunks[ChunkID(id)] = struct{}{}
			return nil
		}},
		{prefixExtentMeta, func(key []byte, value []byte) error {
			id, err := keyID(key, prefixExtentMeta)
			if err != nil {
				return err
			}
			var v ExtentMetaV2
			if err := decode(value, &v, &state.counts.ExtentMeta); err != nil {
				return err
			}
			if v.ID != ExtentIDV2(id) {
				return fmt.Errorf("extent metadata key ID %d does not match value ID %d", id, v.ID)
			}
			state.extentMetaIDs[ExtentIDV2(id)] = struct{}{}
			return nil
		}},
		{prefixExtentPage, func(_ []byte, value []byte) error {
			// Keys are binary uvarint (extentPageKey), not decimal — no
			// keyID check. Count-only; references are gathered via the
			// inode's extent pages.
			var p ExtentPage
			return decode(value, &p, &state.counts.ExtentPages)
		}},
		{prefixNode, func(_ []byte, value []byte) error { var v NodeInfo; return decode(value, &v, &state.counts.Nodes) }},
		{prefixPolicy, func(_ []byte, value []byte) error {
			var v PlacementPolicy
			return decode(value, &v, &state.counts.Policies)
		}},
		{prefixRepair, func(_ []byte, value []byte) error {
			var v RepairTask
			return decode(value, &v, &state.counts.RepairTasks)
		}},
		{prefixAudit, func(_ []byte, value []byte) error {
			var v AuditRecord
			return decode(value, &v, &state.counts.AuditRecords)
		}},
		{prefixACL, func(_ []byte, value []byte) error {
			var v BucketPolicy
			return decode(value, &v, &state.counts.BucketPolicies)
		}},
		{prefixQuota, func(_ []byte, value []byte) error { var v BucketQuota; return decode(value, &v, &state.counts.Quotas) }},
		{prefixQuotaUsage, func(_ []byte, value []byte) error {
			var v BucketUsage
			return decode(value, &v, &state.counts.QuotaUsage)
		}},
		{prefixFreeList, func(_ []byte, value []byte) error { var v InodeID; return decode(value, &v, &state.counts.FreeInodes) }},
		{prefixWriteAttempt, func(_ []byte, value []byte) error {
			var v ObjectWriteAttempt
			return decode(value, &v, &state.counts.WriteAttempts)
		}},
		{prefixWriteAttemptState, func(_ []byte, value []byte) error {
			var v string
			return decode(value, &v, &state.counts.WriteAttemptStates)
		}},
		{prefixBackgroundTask, func(_ []byte, value []byte) error {
			var v BackgroundTask
			return decode(value, &v, &state.counts.BackgroundTasks)
		}},
		{prefixBackgroundTaskQ, func(_ []byte, value []byte) error {
			var v string
			return decode(value, &v, &state.counts.BackgroundTaskQueue)
		}},
		{metaNodeOpsPrefix, func(_ []byte, value []byte) error {
			if len(value) == 0 {
				return fmt.Errorf("empty raft node ops URL")
			}
			state.counts.RaftNodeOps++
			return nil
		}},
	}
}

func keyID(key []byte, prefix string) (uint64, error) {
	segment := string(key[len(prefix):])
	id, err := strconv.ParseUint(segment, 10, 64)
	if err != nil {
		return 0, err
	}
	if segment != strconv.FormatUint(id, 10) {
		return 0, fmt.Errorf("non-canonical numeric key %q", key)
	}
	return id, nil
}

func namespaceParentID(key []byte) (InodeID, error) {
	parts := strings.SplitN(string(key[len(prefixNS):]), "/", 2)
	if len(parts) != 2 || parts[1] == "" {
		return 0, fmt.Errorf("invalid namespace key %q", key)
	}
	id, err := keyID([]byte(prefixNS+parts[0]), prefixNS)
	return InodeID(id), err
}

// backupGetPage reads one extent page under an exact root from a
// read-only checkpoint db. Mirrors ExtentPageStore.GetPage over the raw
// *pebble.DB (the store binding is not usable on a checkpoint).
func backupGetPage(ctx context.Context, db *pebble.DB, inodeID InodeID, root uint64, pageNo uint32) (*ExtentPage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	key := extentPageKey(inodeID, root, pageNo)
	raw, closer, err := db.Get([]byte(key))
	if err == pebble.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	data := make([]byte, len(raw))
	copy(data, raw)
	page := &ExtentPage{}
	if err := unmarshalValue(data, page); err != nil {
		return nil, err
	}
	page.InodeID = inodeID
	page.PageNo = pageNo
	return page, nil
}

// backupResolvePage walks the COW root history down from currentRoot to
// find the newest root holding pageNo. Mirrors ExtentPageStore.ResolvePage.
func backupResolvePage(ctx context.Context, db *pebble.DB, inodeID InodeID, currentRoot uint64, pageNo uint32) (*ExtentPage, error) {
	for root := currentRoot; root > 0; root-- {
		page, err := backupGetPage(ctx, db, inodeID, root, pageNo)
		if err != nil {
			return nil, err
		}
		if page != nil {
			return page, nil
		}
	}
	return nil, nil
}

// backupResolveExtents returns the flat extent reference list for a V2
// pages-layout inode across the checkpoint. Missing pages are skipped
// (tolerant, matching ResolveExtents); references are then cross-checked
// against /extent-meta/ and /chunk/ rows by the caller.
func backupResolveExtents(ctx context.Context, db *pebble.DB, in *InodeMetaV2) ([]ExtentRef, error) {
	var out []ExtentRef
	for p := uint32(0); p < in.ExtentPageCount; p++ {
		page, err := backupResolvePage(ctx, db, in.ID, in.ExtentRoot, p)
		if err != nil {
			return nil, err
		}
		if page == nil {
			continue
		}
		out = append(out, page.Extents...)
	}
	return out, nil
}

func scanBackupPrefix(ctx context.Context, db *pebble.DB, prefix string, decode func(key, value []byte) error) error {
	iter, err := db.NewIter(&pebble.IterOptions{LowerBound: []byte(prefix), UpperBound: prefixUpperBound(prefix)})
	if err != nil {
		return fmt.Errorf("backup artifact: iterate %q: %w", prefix, err)
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := decode(iter.Key(), iter.Value()); err != nil {
			return fmt.Errorf("backup artifact: decode %q key %q: %w", prefix, iter.Key(), err)
		}
	}
	if err := iter.Error(); err != nil {
		return fmt.Errorf("backup artifact: iterate %q: %w", prefix, err)
	}
	return nil
}

func compareBackupRecordCounts(expected, actual BackupRecordCounts) error {
	if expected != actual {
		return fmt.Errorf("backup artifact: record counts mismatch: manifest=%+v actual=%+v", expected, actual)
	}
	return nil
}
