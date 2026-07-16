package metadata

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
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

	_, err := s.s3Client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: aws.String(s.bucket),
		Delete: &types.Delete{
			Objects: objectsToDelete,
			Quiet:   aws.Bool(true),
		},
	})
	if err != nil {
		return fmt.Errorf("delete objects: %w", err)
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
