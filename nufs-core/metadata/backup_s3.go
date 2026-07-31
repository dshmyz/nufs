package metadata

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

// S3RemoteStorage implements RemoteStorage for AWS S3.
type S3RemoteStorage struct {
	bucket   string
	prefix   string
	s3Client *s3.Client
	region   string
}

// S3Config holds S3 remote storage configuration.
type S3Config struct {
	Bucket   string // S3 bucket name
	Prefix   string // Optional key prefix (e.g., "backups/")
	Region   string // AWS region (e.g., "us-east-1")
	Endpoint string // Custom endpoint for S3-compatible stores (MinIO, etc.)
	Insecure bool   // Use HTTP instead of HTTPS (dev only)
}

// NewS3RemoteStorage creates a new S3 remote storage backend.
func NewS3RemoteStorage(cfg S3Config) (*S3RemoteStorage, error) {
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("s3 remote storage: bucket is required")
	}

	ctx := context.Background()
	opts := []func(*config.LoadOptions) error{
		config.WithRegion(cfg.Region),
	}

	if cfg.Endpoint != "" {
		// For S3-compatible stores like MinIO
		opts = append(opts, config.WithBaseEndpoint(cfg.Endpoint))
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("s3 remote storage: load config: %w", err)
	}

	s3Client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.Endpoint != "" {
			o.UsePathStyle = true
		}
	})

	return &S3RemoteStorage{
		bucket:   cfg.Bucket,
		prefix:   cfg.Prefix,
		s3Client: s3Client,
		region:   cfg.Region,
	}, nil
}

// Upload copies the local backup directory to the S3 bucket.
func (s *S3RemoteStorage) Upload(ctx context.Context, key string, localDir string) error {
	return filepath.Walk(localDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}

		// Compute S3 key: prefix/backup-key/relative-path
		relPath, err := filepath.Rel(localDir, path)
		if err != nil {
			return fmt.Errorf("compute relative path: %w", err)
		}
		s3Key := s.buildKey(key, relPath)

		// Open file for upload
		f, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("open file %s: %w", path, err)
		}
		defer f.Close()

		// Upload using PutObject
		_, err = s.s3Client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(s.bucket),
			Key:    aws.String(s3Key),
			Body:   f,
		})
		if err != nil {
			return fmt.Errorf("upload %s to s3://%s/%s: %w", path, s.bucket, s3Key, err)
		}

		slog.Debug("s3: uploaded", "key", s3Key, "size", info.Size())
		return nil
	})
}

// buildKey constructs the full S3 key from the backup key and file path.
func (s *S3RemoteStorage) buildKey(backupKey, filePath string) string {
	parts := []string{}
	if s.prefix != "" {
		parts = append(parts, strings.TrimSuffix(s.prefix, "/"))
	}
	parts = append(parts, backupKey)
	parts = append(parts, filePath)
	return strings.Join(parts, "/")
}

// Delete removes a backup from S3.
func (s *S3RemoteStorage) Delete(ctx context.Context, key string) error {
	// List all objects with this prefix
	var objectsToDelete []types.ObjectIdentifier
	paginator := s3.NewListObjectsV2Paginator(s.s3Client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(s.buildKey(key, "")),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("list objects for deletion: %w", err)
		}
		for _, obj := range page.Contents {
			objectsToDelete = append(objectsToDelete, types.ObjectIdentifier{Key: obj.Key})
		}
	}

	if len(objectsToDelete) == 0 {
		return nil
	}

	for start := 0; start < len(objectsToDelete); start += 1000 {
		end := min(start+1000, len(objectsToDelete))
		output, err := s.s3Client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(s.bucket),
			Delete: &types.Delete{
				Objects: objectsToDelete[start:end],
				Quiet:   aws.Bool(true),
			},
		})
		if err != nil {
			return fmt.Errorf("delete objects: %w", err)
		}
		if len(output.Errors) > 0 {
			first := output.Errors[0]
			return fmt.Errorf("delete object %q: %s: %s",
				aws.ToString(first.Key), aws.ToString(first.Code), aws.ToString(first.Message))
		}
	}

	slog.Info("s3: deleted backup", "key", key, "objects", len(objectsToDelete))
	return nil
}

// List returns all backup keys in S3.
func (s *S3RemoteStorage) List(ctx context.Context) ([]string, error) {
	var keys []string
	prefix := s.prefix
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	paginator := s3.NewListObjectsV2Paginator(s.s3Client, &s3.ListObjectsV2Input{
		Bucket:    aws.String(s.bucket),
		Prefix:    aws.String(prefix),
		Delimiter: aws.String("/"),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("list backups: %w", err)
		}
		for _, p := range page.CommonPrefixes {
			// Extract backup key from prefix
			pStr := *p.Prefix
			pStr = strings.TrimPrefix(pStr, prefix)
			pStr = strings.TrimSuffix(pStr, "/")
			if pStr != "" {
				keys = append(keys, pStr)
			}
		}
	}

	return keys, nil
}

type backupS3API interface {
	PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	CopyObject(context.Context, *s3.CopyObjectInput, ...func(*s3.Options)) (*s3.CopyObjectOutput, error)
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	DeleteObjects(context.Context, *s3.DeleteObjectsInput, ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error)
	ListObjectsV2(context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
}

const backupClaimsDir = "claims"

type s3BackupClaim struct {
	BackupID       string    `json:"backup_id"`
	ManifestSHA256 string    `json:"manifest_sha256"`
	AttemptID      string    `json:"attempt_id"`
	CreatedAt      time.Time `json:"created_at"`
}

type s3CommitMarker struct {
	BackupID       string `json:"backup_id"`
	ManifestSHA256 string `json:"manifest_sha256"`
	AttemptID      string `json:"attempt_id"`
}

type s3CommitReconcileState int

const (
	s3CommitAbsent s3CommitReconcileState = iota
	s3CommitMatching
	s3CommitIndeterminate
)

type S3BackupRepository struct {
	client           backupS3API
	bucket           string
	prefix           string
	publishMu        sync.Mutex
	afterClaim       func(s3BackupClaim) error
	beforeCommit     func(s3BackupClaim) error
	beforeMarker     func(s3BackupClaim) error
	beforeOpenTarget func(string) error
}

func NewS3BackupRepository(cfg S3Config) (*S3BackupRepository, error) {
	storage, err := NewS3RemoteStorage(cfg)
	if err != nil {
		return nil, err
	}
	return newS3BackupRepository(storage.s3Client, storage.bucket, storage.prefix)
}

func newS3BackupRepository(client backupS3API, bucket, prefix string) (*S3BackupRepository, error) {
	if client == nil {
		return nil, fmt.Errorf("s3 backup repository: client is required")
	}
	if bucket == "" {
		return nil, fmt.Errorf("s3 backup repository: bucket is required")
	}
	normalizedPrefix, err := normalizeS3BackupPrefix(prefix)
	if err != nil {
		return nil, err
	}
	return &S3BackupRepository{client: client, bucket: bucket, prefix: normalizedPrefix}, nil
}

func (r *S3BackupRepository) Publish(ctx context.Context, checkpointDir string, manifest *BackupManifest) (retErr error) {
	if err := validateRepositoryManifest(manifest); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := VerifyBackupArtifact(ctx, checkpointDir, manifest); err != nil {
		return fmt.Errorf("s3 backup repository: verify source artifact: %w", err)
	}
	manifestData, err := json.Marshal(manifest)
	if err != nil {
		return fmt.Errorf("s3 backup repository: encode manifest: %w", err)
	}
	digest := sha256.Sum256(manifestData)
	r.publishMu.Lock()
	defer r.publishMu.Unlock()
	same, err := r.matchesCommitted(ctx, manifest.BackupID, manifestData)
	if err != nil {
		return err
	}
	if same {
		return nil
	}
	attemptID, err := newBackupAttemptID()
	if err != nil {
		return err
	}
	claim := s3BackupClaim{
		BackupID:       manifest.BackupID,
		ManifestSHA256: fmt.Sprintf("%x", digest[:]),
		AttemptID:      attemptID,
		CreatedAt:      time.Now().UTC(),
	}
	claimData, err := json.Marshal(claim)
	if err != nil {
		return fmt.Errorf("s3 backup repository: encode claim: %w", err)
	}
	marker := commitMarkerFromClaim(claim)
	markerData, err := json.Marshal(marker)
	if err != nil {
		return fmt.Errorf("s3 backup repository: encode commit marker: %w", err)
	}
	claimKey := r.key(backupClaimsDir, manifest.BackupID+".json")
	_, err = r.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(r.bucket),
		Key:         aws.String(claimKey),
		Body:        bytes.NewReader(claimData),
		IfNoneMatch: aws.String("*"),
	})
	if err != nil {
		if isS3PreconditionFailed(err) {
			same, committedErr := r.matchesCommitted(ctx, manifest.BackupID, manifestData)
			if committedErr != nil {
				return committedErr
			}
			if same {
				return nil
			}
			return fmt.Errorf("s3 backup repository: backup %q has an active or stale claim", manifest.BackupID)
		}
		return fmt.Errorf("s3 backup repository: create claim: %w", err)
	}
	claimOwned := true
	committed := false
	cleanupAllowed := true
	defer func() {
		if retErr == nil || !claimOwned || committed || !cleanupAllowed {
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		retErr = errors.Join(retErr, r.cleanupOwnedS3Attempt(cleanupCtx, manifest, claim))
	}()
	if r.afterClaim != nil {
		if err := r.afterClaim(claim); err != nil {
			return fmt.Errorf("s3 backup repository: after claim: %w", err)
		}
	}

	info, err := os.Lstat(checkpointDir)
	if err != nil {
		return fmt.Errorf("s3 backup repository: inspect checkpoint: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("s3 backup repository: checkpoint is not a directory")
	}
	sourceRoot, err := os.OpenRoot(checkpointDir)
	if err != nil {
		return fmt.Errorf("s3 backup repository: open checkpoint: %w", err)
	}
	defer sourceRoot.Close()

	for _, file := range manifest.Files {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := rejectRootSymlinks(sourceRoot, file.Path, false); err != nil {
			return fmt.Errorf("s3 backup repository: inspect %q: %w", file.Path, err)
		}
		source, err := sourceRoot.Open(file.Path)
		if err != nil {
			return fmt.Errorf("s3 backup repository: open %q: %w", file.Path, err)
		}
		sourceInfo, err := source.Stat()
		if err != nil {
			source.Close()
			return fmt.Errorf("s3 backup repository: stat %q: %w", file.Path, err)
		}
		if !sourceInfo.Mode().IsRegular() {
			source.Close()
			return fmt.Errorf("s3 backup repository: source %q is not a regular file", file.Path)
		}
		key := r.key(backupStagingDir, manifest.BackupID, attemptID, backupFilesDir, file.Path)
		_, putErr := r.client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(r.bucket),
			Key:    aws.String(key),
			Body:   source,
		})
		closeErr := source.Close()
		if putErr != nil {
			return fmt.Errorf("s3 backup repository: upload %q: %w", key, putErr)
		}
		if closeErr != nil {
			return fmt.Errorf("s3 backup repository: close %q: %w", file.Path, closeErr)
		}
	}
	stagingManifestKey := r.key(backupStagingDir, manifest.BackupID, attemptID, backupManifestFile)
	if err := r.putBytes(ctx, stagingManifestKey, manifestData); err != nil {
		return err
	}
	if err := r.verifyClaimOwner(ctx, claim); err != nil {
		return err
	}
	for _, file := range manifest.Files {
		sourceKey := r.key(backupStagingDir, manifest.BackupID, attemptID, backupFilesDir, file.Path)
		targetKey := r.key(backupCommittedDir, manifest.BackupID, backupFilesDir, file.Path)
		if err := r.copyObject(ctx, sourceKey, targetKey); err != nil {
			return err
		}
	}
	finalManifestKey := r.key(backupCommittedDir, manifest.BackupID, backupManifestFile)
	if err := r.copyObject(ctx, stagingManifestKey, finalManifestKey); err != nil {
		return err
	}
	if r.beforeCommit != nil {
		if err := r.beforeCommit(claim); err != nil {
			return fmt.Errorf("s3 backup repository: before commit: %w", err)
		}
	}
	if err := r.verifyClaimOwner(ctx, claim); err != nil {
		return err
	}
	if r.beforeMarker != nil {
		if err := r.beforeMarker(claim); err != nil {
			return fmt.Errorf("s3 backup repository: before marker: %w", err)
		}
	}
	markerKey := r.key(backupCommittedDir, manifest.BackupID, backupCommitMarker)
	if putErr := r.putCommitMarker(ctx, markerKey, markerData); putErr != nil {
		reconcileCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		state, reconcileErr := r.reconcileCommit(reconcileCtx, claim, manifestData)
		cancel()
		switch state {
		case s3CommitMatching:
			committed = true
			return nil
		case s3CommitAbsent:
			return putErr
		default:
			cleanupAllowed = false
			return errors.Join(
				fmt.Errorf("s3 backup repository: indeterminate commit for backup %q", manifest.BackupID),
				putErr,
				reconcileErr,
			)
		}
	}
	committed = true
	return nil
}

func (r *S3BackupRepository) ListCommitted(ctx context.Context) ([]BackupDescriptor, error) {
	markerPrefix := r.key(backupCommittedDir) + "/"
	objects, err := r.listAll(ctx, markerPrefix)
	if err != nil {
		return nil, err
	}
	seen := make(map[string]struct{})
	var descriptors []BackupDescriptor
	for _, object := range objects {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		key := aws.ToString(object.Key)
		backupID, ok := r.committedMarkerID(key)
		if !ok {
			continue
		}
		if _, duplicate := seen[backupID]; duplicate {
			continue
		}
		seen[backupID] = struct{}{}
		data, err := r.getBytes(ctx, r.key(backupCommittedDir, backupID, backupManifestFile))
		if err != nil {
			if isS3NotFound(err) {
				continue
			}
			return nil, fmt.Errorf("s3 backup repository: read manifest for %q: %w", backupID, err)
		}
		manifest, err := decodeBackupManifest(data)
		if err != nil || manifest.BackupID != backupID {
			continue
		}
		descriptors = append(descriptors, descriptorFromManifest(manifest))
	}
	sortBackupDescriptors(descriptors)
	return descriptors, nil
}

func (r *S3BackupRepository) Fetch(ctx context.Context, backupID, targetDir string) (_ *BackupManifest, retErr error) {
	if err := validateBackupID(backupID); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if _, err := r.getBytes(ctx, r.key(backupCommittedDir, backupID, backupCommitMarker)); err != nil {
		return nil, fmt.Errorf("s3 backup repository: backup %q is not committed: %w", backupID, err)
	}
	manifestData, err := r.getBytes(ctx, r.key(backupCommittedDir, backupID, backupManifestFile))
	if err != nil {
		return nil, err
	}
	manifest, err := decodeBackupManifest(manifestData)
	if err != nil {
		return nil, err
	}
	if manifest.BackupID != backupID {
		return nil, fmt.Errorf("s3 backup repository: manifest ID %q does not match backup %q", manifest.BackupID, backupID)
	}

	target, err := prepareRestoreTarget(targetDir, r.beforeOpenTarget)
	if err != nil {
		return nil, err
	}
	var createdFiles, createdDirectories []restorePathIdentity
	defer func() {
		removeTarget := retErr != nil && target.created
		if retErr != nil {
			retErr = errors.Join(
				retErr,
				cleanupRootEntries(target.root, createdFiles, createdDirectories),
			)
		}
		retErr = errors.Join(retErr, target.finish(removeTarget))
	}()

	for _, file := range manifest.Files {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		output, err := r.client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(r.bucket),
			Key:    aws.String(r.key(backupCommittedDir, backupID, backupFilesDir, file.Path)),
		})
		if err != nil {
			closeErr := closeS3OutputBody(output)
			if closeErr != nil {
				err = errors.Join(err, closeErr)
			}
			return nil, fmt.Errorf("s3 backup repository: download %q: %w", file.Path, err)
		}
		if output == nil || output.Body == nil {
			return nil, fmt.Errorf("s3 backup repository: download %q returned nil body", file.Path)
		}
		writeErr := writeS3BodyExclusive(
			target.root,
			file.Path,
			output.Body,
			&createdFiles,
			&createdDirectories,
		)
		closeErr := output.Body.Close()
		if writeErr != nil {
			return nil, fmt.Errorf("s3 backup repository: write %q: %w", file.Path, writeErr)
		}
		if closeErr != nil {
			_ = target.root.Remove(file.Path)
			return nil, fmt.Errorf("s3 backup repository: close %q body: %w", file.Path, closeErr)
		}
	}
	if _, err := VerifyBackupArtifact(ctx, target.absolute, manifest); err != nil {
		return nil, fmt.Errorf("s3 backup repository: verify fetched artifact: %w", err)
	}
	return manifest, nil
}

func (r *S3BackupRepository) Delete(ctx context.Context, backupID string) error {
	if err := validateBackupID(backupID); err != nil {
		return err
	}
	markerKey := r.key(backupCommittedDir, backupID, backupCommitMarker)
	if _, err := r.getBytes(ctx, markerKey); err != nil {
		if !isS3NotFound(err) {
			return fmt.Errorf("s3 backup repository: inspect marker before delete: %w", err)
		}
		if _, claimErr := r.getBytes(ctx, r.key(backupClaimsDir, backupID+".json")); claimErr == nil {
			return fmt.Errorf("s3 backup repository: backup %q has an in-progress or interrupted claim", backupID)
		} else if !isS3NotFound(claimErr) {
			return fmt.Errorf("s3 backup repository: inspect claim before delete: %w", claimErr)
		}
		return nil
	}
	finalObjects, err := r.listAll(ctx, r.key(backupCommittedDir, backupID)+"/")
	if err != nil {
		return err
	}
	stagingObjects, err := r.listAll(ctx, r.key(backupStagingDir, backupID)+"/")
	if err != nil {
		return err
	}
	var keys []string
	manifestKey := r.key(backupCommittedDir, backupID, backupManifestFile)
	for _, object := range append(finalObjects, stagingObjects...) {
		key := aws.ToString(object.Key)
		if key != "" && key != markerKey && key != manifestKey {
			keys = append(keys, key)
		}
	}
	if err := r.deleteKeys(ctx, keys); err != nil {
		return err
	}
	claimKey := r.key(backupClaimsDir, backupID+".json")
	if err := r.deleteKeys(ctx, []string{claimKey}); err != nil {
		return fmt.Errorf("s3 backup repository: remove claim before marker: %w", err)
	}
	if err := r.deleteKeys(ctx, []string{manifestKey}); err != nil {
		return fmt.Errorf("s3 backup repository: remove committed manifest before marker: %w", err)
	}
	if err := r.deleteKeys(ctx, []string{markerKey}); err != nil {
		return fmt.Errorf("s3 backup repository: remove visibility marker last: %w", err)
	}
	return nil
}

func (r *S3BackupRepository) DeleteStagingOlderThan(ctx context.Context, cutoff time.Time) error {
	stagingObjects, err := r.listAll(ctx, r.key(backupStagingDir)+"/")
	if err != nil {
		return err
	}
	type attemptGroup struct {
		backupID string
		keys     []string
		newest   time.Time
		valid    bool
	}
	groups := make(map[string]*attemptGroup)
	stagingPrefix := r.key(backupStagingDir) + "/"
	for _, object := range stagingObjects {
		key := aws.ToString(object.Key)
		remainder := strings.TrimPrefix(key, stagingPrefix)
		parts := strings.SplitN(remainder, "/", 3)
		if len(parts) != 3 || validateBackupID(parts[0]) != nil || validateAttemptID(parts[1]) != nil {
			continue
		}
		groupKey := parts[0] + "/" + parts[1]
		group := groups[groupKey]
		if group == nil {
			group = &attemptGroup{backupID: parts[0], valid: true}
			groups[groupKey] = group
		}
		group.keys = append(group.keys, key)
		if object.LastModified == nil {
			group.valid = false
		} else if object.LastModified.After(group.newest) {
			group.newest = *object.LastModified
		}
	}
	for _, group := range groups {
		if !group.valid || !group.newest.Before(cutoff) {
			continue
		}
		if _, err := r.getBytes(ctx, r.key(backupClaimsDir, group.backupID+".json")); err == nil {
			continue
		} else if !isS3NotFound(err) {
			return fmt.Errorf("s3 backup repository: inspect current claim before staging cleanup: %w", err)
		}
		if err := r.deleteKeys(ctx, group.keys); err != nil {
			return err
		}
	}
	return nil
}

func normalizeS3BackupPrefix(prefix string) (string, error) {
	if prefix == "" {
		return "", nil
	}
	normalized := strings.TrimSuffix(prefix, "/")
	if normalized == "" || strings.HasPrefix(normalized, "/") || strings.Contains(normalized, `\`) ||
		path.Clean(normalized) != normalized {
		return "", fmt.Errorf("s3 backup repository: invalid prefix %q", prefix)
	}
	for _, part := range strings.Split(normalized, "/") {
		if part == "" || part == "." || part == ".." {
			return "", fmt.Errorf("s3 backup repository: invalid prefix %q", prefix)
		}
	}
	return normalized, nil
}

func (r *S3BackupRepository) key(parts ...string) string {
	key := strings.Join(parts, "/")
	if r.prefix == "" {
		return key
	}
	return r.prefix + "/" + key
}

func (r *S3BackupRepository) putBytes(ctx context.Context, key string, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	_, err := r.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(r.bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	})
	if err != nil {
		return fmt.Errorf("s3 backup repository: put %q: %w", key, err)
	}
	return nil
}

func (r *S3BackupRepository) putCommitMarker(ctx context.Context, key string, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	_, err := r.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(r.bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(data),
		IfNoneMatch: aws.String("*"),
	})
	if err != nil {
		return fmt.Errorf("s3 backup repository: create commit marker %q: %w", key, err)
	}
	return nil
}

func (r *S3BackupRepository) copyObject(ctx context.Context, sourceKey, targetKey string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	copySource := url.PathEscape(r.bucket + "/" + sourceKey)
	_, err := r.client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(r.bucket),
		Key:        aws.String(targetKey),
		CopySource: aws.String(copySource),
	})
	if err != nil {
		return fmt.Errorf("s3 backup repository: copy %q to %q: %w", sourceKey, targetKey, err)
	}
	return nil
}

func (r *S3BackupRepository) getBytes(ctx context.Context, key string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	output, err := r.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(r.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		closeErr := closeS3OutputBody(output)
		return nil, errors.Join(err, closeErr)
	}
	if output == nil || output.Body == nil {
		return nil, fmt.Errorf("s3 backup repository: get %q returned nil body", key)
	}
	data, readErr := io.ReadAll(output.Body)
	closeErr := output.Body.Close()
	if readErr != nil {
		return nil, fmt.Errorf("s3 backup repository: read %q: %w", key, readErr)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("s3 backup repository: close %q body: %w", key, closeErr)
	}
	return data, nil
}

func closeS3OutputBody(output *s3.GetObjectOutput) error {
	if output == nil || output.Body == nil {
		return nil
	}
	if err := output.Body.Close(); err != nil {
		return fmt.Errorf("s3 backup repository: close response body: %w", err)
	}
	return nil
}

func (r *S3BackupRepository) matchesCommitted(ctx context.Context, backupID string, manifestData []byte) (bool, error) {
	markerKey := r.key(backupCommittedDir, backupID, backupCommitMarker)
	markerData, err := r.getBytes(ctx, markerKey)
	if err != nil {
		if isS3NotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("s3 backup repository: check committed marker: %w", err)
	}
	marker, err := decodeS3CommitMarker(markerData)
	if err != nil {
		return false, fmt.Errorf("s3 backup repository: indeterminate committed marker: %w", err)
	}
	if marker.BackupID != backupID {
		return false, fmt.Errorf("s3 backup repository: indeterminate committed marker ID %q", marker.BackupID)
	}
	claim, err := r.readS3Claim(ctx, backupID)
	if err != nil {
		return false, fmt.Errorf("s3 backup repository: indeterminate committed claim: %w", err)
	}
	if marker != commitMarkerFromClaim(claim) {
		return false, fmt.Errorf("s3 backup repository: indeterminate marker/claim mismatch")
	}
	existingData, err := r.getBytes(ctx, r.key(backupCommittedDir, backupID, backupManifestFile))
	if err != nil {
		return false, fmt.Errorf("s3 backup repository: read committed manifest: %w", err)
	}
	existing, err := decodeBackupManifest(existingData)
	if err != nil {
		return false, err
	}
	if existing.BackupID != backupID {
		return false, fmt.Errorf("s3 backup repository: committed manifest ID %q does not match backup %q", existing.BackupID, backupID)
	}
	canonical, err := json.Marshal(existing)
	if err != nil {
		return false, fmt.Errorf("s3 backup repository: encode committed manifest: %w", err)
	}
	digest := sha256.Sum256(canonical)
	if hex.EncodeToString(digest[:]) != marker.ManifestSHA256 {
		return false, fmt.Errorf("s3 backup repository: indeterminate committed manifest digest mismatch")
	}
	if !bytes.Equal(canonical, manifestData) {
		return false, fmt.Errorf("s3 backup repository: committed backup %q already exists with different contents", backupID)
	}
	return true, nil
}

func commitMarkerFromClaim(claim s3BackupClaim) s3CommitMarker {
	return s3CommitMarker{
		BackupID:       claim.BackupID,
		ManifestSHA256: claim.ManifestSHA256,
		AttemptID:      claim.AttemptID,
	}
}

func decodeS3CommitMarker(data []byte) (s3CommitMarker, error) {
	var marker s3CommitMarker
	decoder := json.NewDecoder(bytes.NewReader(data))
	if err := decoder.Decode(&marker); err != nil {
		return marker, fmt.Errorf("decode marker: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			err = fmt.Errorf("additional JSON value")
		}
		return marker, fmt.Errorf("decode marker: %w", err)
	}
	if err := validateBackupID(marker.BackupID); err != nil {
		return marker, err
	}
	if err := validateAttemptID(marker.AttemptID); err != nil {
		return marker, err
	}
	if len(marker.ManifestSHA256) != sha256.Size*2 {
		return marker, fmt.Errorf("invalid marker manifest digest")
	}
	if _, err := hex.DecodeString(marker.ManifestSHA256); err != nil ||
		strings.ToLower(marker.ManifestSHA256) != marker.ManifestSHA256 {
		return marker, fmt.Errorf("invalid marker manifest digest")
	}
	return marker, nil
}

func (r *S3BackupRepository) reconcileCommit(
	ctx context.Context,
	expectedClaim s3BackupClaim,
	manifestData []byte,
) (s3CommitReconcileState, error) {
	markerKey := r.key(backupCommittedDir, expectedClaim.BackupID, backupCommitMarker)
	markerData, err := r.getBytes(ctx, markerKey)
	if err != nil {
		if isS3NotFound(err) {
			return s3CommitAbsent, nil
		}
		return s3CommitIndeterminate, fmt.Errorf("read COMMITTED after Put error: %w", err)
	}
	marker, err := decodeS3CommitMarker(markerData)
	if err != nil {
		return s3CommitIndeterminate, fmt.Errorf("classify COMMITTED after Put error: %w", err)
	}
	if marker != commitMarkerFromClaim(expectedClaim) {
		return s3CommitIndeterminate, fmt.Errorf("COMMITTED identifies a different publication")
	}
	actualClaim, err := r.readS3Claim(ctx, expectedClaim.BackupID)
	if err != nil {
		return s3CommitIndeterminate, fmt.Errorf("re-read claim after Put error: %w", err)
	}
	if actualClaim != expectedClaim {
		return s3CommitIndeterminate, fmt.Errorf("claim changed after COMMITTED Put")
	}
	finalData, err := r.getBytes(
		ctx,
		r.key(backupCommittedDir, expectedClaim.BackupID, backupManifestFile),
	)
	if err != nil {
		return s3CommitIndeterminate, fmt.Errorf("re-read manifest after Put error: %w", err)
	}
	finalManifest, err := decodeBackupManifest(finalData)
	if err != nil {
		return s3CommitIndeterminate, fmt.Errorf("decode manifest after Put error: %w", err)
	}
	canonical, err := json.Marshal(finalManifest)
	if err != nil {
		return s3CommitIndeterminate, fmt.Errorf("encode manifest after Put error: %w", err)
	}
	digest := sha256.Sum256(canonical)
	if !bytes.Equal(canonical, manifestData) ||
		hex.EncodeToString(digest[:]) != expectedClaim.ManifestSHA256 {
		return s3CommitIndeterminate, fmt.Errorf("manifest does not match COMMITTED publication")
	}
	return s3CommitMatching, nil
}

func decodeS3Claim(data []byte) (s3BackupClaim, error) {
	var claim s3BackupClaim
	if err := json.Unmarshal(data, &claim); err != nil {
		return claim, fmt.Errorf("s3 backup repository: decode claim: %w", err)
	}
	if err := validateBackupID(claim.BackupID); err != nil {
		return claim, err
	}
	if err := validateAttemptID(claim.AttemptID); err != nil {
		return claim, err
	}
	if len(claim.ManifestSHA256) != sha256.Size*2 {
		return claim, fmt.Errorf("s3 backup repository: invalid claim manifest digest")
	}
	if _, err := hex.DecodeString(claim.ManifestSHA256); err != nil ||
		strings.ToLower(claim.ManifestSHA256) != claim.ManifestSHA256 {
		return claim, fmt.Errorf("s3 backup repository: invalid claim manifest digest")
	}
	if claim.CreatedAt.IsZero() {
		return claim, fmt.Errorf("s3 backup repository: claim creation time is required")
	}
	return claim, nil
}

func (r *S3BackupRepository) readS3Claim(ctx context.Context, backupID string) (s3BackupClaim, error) {
	data, err := r.getBytes(ctx, r.key(backupClaimsDir, backupID+".json"))
	if err != nil {
		return s3BackupClaim{}, err
	}
	claim, err := decodeS3Claim(data)
	if err != nil {
		return s3BackupClaim{}, err
	}
	if claim.BackupID != backupID {
		return s3BackupClaim{}, fmt.Errorf("s3 backup repository: claim ID does not match key")
	}
	return claim, nil
}

func (r *S3BackupRepository) verifyClaimOwner(ctx context.Context, expected s3BackupClaim) error {
	actual, err := r.readS3Claim(ctx, expected.BackupID)
	if err != nil {
		return fmt.Errorf("s3 backup repository: verify claim owner: %w", err)
	}
	if actual != expected {
		return fmt.Errorf("s3 backup repository: claim ownership changed")
	}
	return nil
}

func (r *S3BackupRepository) cleanupOwnedS3Attempt(ctx context.Context, manifest *BackupManifest, claim s3BackupClaim) error {
	if err := r.verifyClaimOwner(ctx, claim); err != nil {
		return err
	}
	keys := make([]string, 0, len(manifest.Files)*2+2)
	for _, file := range manifest.Files {
		keys = append(keys,
			r.key(backupCommittedDir, manifest.BackupID, backupFilesDir, file.Path),
			r.key(backupStagingDir, manifest.BackupID, claim.AttemptID, backupFilesDir, file.Path),
		)
	}
	keys = append(keys,
		r.key(backupCommittedDir, manifest.BackupID, backupManifestFile),
		r.key(backupStagingDir, manifest.BackupID, claim.AttemptID, backupManifestFile),
	)
	if err := r.deleteKeys(ctx, keys); err != nil {
		return fmt.Errorf("s3 backup repository: clean failed attempt objects: %w", err)
	}
	if err := r.verifyClaimOwner(ctx, claim); err != nil {
		return err
	}
	if err := r.deleteKeys(ctx, []string{r.key(backupClaimsDir, manifest.BackupID+".json")}); err != nil {
		return fmt.Errorf("s3 backup repository: clean failed attempt claim: %w", err)
	}
	return nil
}

func isS3NotFound(err error) bool {
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	switch apiErr.ErrorCode() {
	case "NoSuchKey", "NotFound", "404":
		return true
	default:
		return false
	}
}

func isS3PreconditionFailed(err error) bool {
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	switch apiErr.ErrorCode() {
	case "PreconditionFailed", "ConditionalRequestConflict", "412":
		return true
	default:
		return false
	}
}

func (r *S3BackupRepository) listAll(ctx context.Context, prefix string) ([]types.Object, error) {
	var objects []types.Object
	var continuationToken *string
	seenTokens := make(map[string]struct{})
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		output, err := r.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(r.bucket),
			Prefix:            aws.String(prefix),
			ContinuationToken: continuationToken,
		})
		if err != nil {
			return nil, fmt.Errorf("s3 backup repository: list %q: %w", prefix, err)
		}
		for _, object := range output.Contents {
			if object.Key == nil || !strings.HasPrefix(aws.ToString(object.Key), prefix) {
				return nil, fmt.Errorf("s3 backup repository: list %q returned key outside prefix", prefix)
			}
		}
		objects = append(objects, output.Contents...)
		if !aws.ToBool(output.IsTruncated) {
			return objects, nil
		}
		if output.NextContinuationToken == nil || aws.ToString(output.NextContinuationToken) == "" {
			return nil, fmt.Errorf("s3 backup repository: truncated list %q lacks continuation token", prefix)
		}
		next := aws.ToString(output.NextContinuationToken)
		if _, seen := seenTokens[next]; seen {
			return nil, fmt.Errorf("s3 backup repository: repeated continuation token %q", next)
		}
		seenTokens[next] = struct{}{}
		continuationToken = output.NextContinuationToken
	}
}

func (r *S3BackupRepository) committedMarkerID(key string) (string, bool) {
	prefix := r.key(backupCommittedDir) + "/"
	if !strings.HasPrefix(key, prefix) {
		return "", false
	}
	remainder := strings.TrimPrefix(key, prefix)
	parts := strings.Split(remainder, "/")
	if len(parts) != 2 || parts[1] != backupCommitMarker || validateBackupID(parts[0]) != nil {
		return "", false
	}
	return parts[0], true
}

func (r *S3BackupRepository) deleteKeys(ctx context.Context, keys []string) error {
	const maxDeleteObjects = 1000
	for start := 0; start < len(keys); start += maxDeleteObjects {
		if err := ctx.Err(); err != nil {
			return err
		}
		end := min(start+maxDeleteObjects, len(keys))
		objects := make([]types.ObjectIdentifier, 0, end-start)
		for _, key := range keys[start:end] {
			objects = append(objects, types.ObjectIdentifier{Key: aws.String(key)})
		}
		output, err := r.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(r.bucket),
			Delete: &types.Delete{Objects: objects, Quiet: aws.Bool(true)},
		})
		if err != nil {
			return fmt.Errorf("s3 backup repository: delete objects: %w", err)
		}
		if len(output.Errors) > 0 {
			first := output.Errors[0]
			return fmt.Errorf("s3 backup repository: delete %q: %s: %s",
				aws.ToString(first.Key), aws.ToString(first.Code), aws.ToString(first.Message))
		}
	}
	return nil
}

func writeS3BodyExclusive(
	targetRoot *os.Root,
	name string,
	body io.Reader,
	createdFiles *[]restorePathIdentity,
	createdDirectories *[]restorePathIdentity,
) error {
	if err := ensureRootParents(targetRoot, name, createdDirectories); err != nil {
		return err
	}
	target, err := targetRoot.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(target, body)
	syncErr := target.Sync()
	targetInfo, statErr := target.Stat()
	closeErr := target.Close()
	if copyErr != nil || syncErr != nil || statErr != nil || closeErr != nil {
		_ = targetRoot.Remove(name)
	}
	if copyErr != nil {
		return copyErr
	}
	if syncErr != nil {
		return syncErr
	}
	if statErr != nil {
		return statErr
	}
	if closeErr != nil {
		return closeErr
	}
	if createdFiles != nil {
		*createdFiles = append(*createdFiles, restorePathIdentity{name: name, info: targetInfo})
	}
	return nil
}
