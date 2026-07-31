package s3

import (
	"context"
	"errors"
	"io"
)

var (
	ErrObjectBucketNotFound = errors.New("object bucket not found")
	ErrObjectLocked         = errors.New("object locked")
	ErrObjectBodyTooLarge   = errors.New("object body too large")
	ErrObjectQuotaExceeded  = errors.New("object quota exceeded")
	ErrObjectNoReplicas     = errors.New("object no replicas")
	ErrObjectWriteFailed    = errors.New("object write failed")
	ErrObjectCommitFailed   = errors.New("object commit failed")
	ErrObjectMetadataFailed = errors.New("object metadata failed")
)

// ObjectCommitter owns object write/read orchestration behind a small
// interface so HTTP handlers only translate S3 protocol details.
type ObjectCommitter interface {
	Put(ctx context.Context, req PutObjectRequest) (PutObjectResult, error)
	Get(ctx context.Context, req GetObjectRequest) (ObjectReader, error)
}

type PutObjectRequest struct {
	Bucket        string
	Key           string
	Body          io.Reader
	ContentLength int64
	MaxObjectSize int64
	RequestID     string
}

type PutObjectResult struct {
	ETag string
	Size int64
}

type GetObjectRequest struct {
	Bucket string
	Key    string
	Range  *ObjectRange
}

type ObjectRange struct {
	Start int64
	End   int64
}

type ObjectReader interface {
	io.ReadCloser
	Size() int64
	ETag() string
}
