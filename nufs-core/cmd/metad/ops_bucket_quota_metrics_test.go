package main

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/dshmyz/nufs/nufs-core/metadata"
)

func TestWritePrometheusBucketQuotaValuesAndFiltering(t *testing.T) {
	source := &fakeBucketQuotaMetricsSource{
		buckets: []metadata.BucketInfo{
			{Name: "photos"},
			{Name: "bytes-only"},
			{Name: "no-quota"},
			{Name: "quota-error"},
			{Name: "usage-error"},
		},
		quotas: map[string]*metadata.BucketQuota{
			"photos":     {MaxSizeBytes: 100, MaxObjects: 10},
			"bytes-only": {MaxSizeBytes: 200},
			"usage-error": {
				MaxSizeBytes: 50,
				MaxObjects:   5,
			},
		},
		usages: map[string]*metadata.BucketUsage{
			"photos":     {Name: "photos", UsedBytes: 25, Objects: 4},
			"bytes-only": {Name: "bytes-only", UsedBytes: 20, Objects: 99},
		},
		quotaErrors: map[string]error{"quota-error": errors.New("quota unavailable")},
		usageErrors: map[string]error{"usage-error": errors.New("usage unavailable")},
	}

	var output bytes.Buffer
	writePrometheusBucketQuota(context.Background(), &output, source)
	body := output.String()

	for _, want := range []string{
		`nufs_bucket_quota_limit{bucket="photos",resource="bytes"} 100`,
		`nufs_bucket_quota_usage{bucket="photos",resource="bytes"} 25`,
		`nufs_bucket_quota_used_ratio{bucket="photos",resource="bytes"} 0.25`,
		`nufs_bucket_quota_limit{bucket="photos",resource="objects"} 10`,
		`nufs_bucket_quota_usage{bucket="photos",resource="objects"} 4`,
		`nufs_bucket_quota_used_ratio{bucket="photos",resource="objects"} 0.4`,
		`nufs_bucket_quota_limit{bucket="bytes-only",resource="bytes"} 200`,
		`nufs_bucket_quota_usage{bucket="bytes-only",resource="bytes"} 20`,
		`nufs_bucket_quota_used_ratio{bucket="bytes-only",resource="bytes"} 0.1`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics missing %q:\n%s", want, body)
		}
	}

	for _, family := range []string{
		"nufs_bucket_quota_limit",
		"nufs_bucket_quota_usage",
		"nufs_bucket_quota_used_ratio",
	} {
		if got := strings.Count(body, "# HELP "+family+" "); got != 1 {
			t.Fatalf("%s HELP count = %d, want 1:\n%s", family, got, body)
		}
		if got := strings.Count(body, "# TYPE "+family+" gauge"); got != 1 {
			t.Fatalf("%s TYPE count = %d, want 1:\n%s", family, got, body)
		}
	}

	for _, unwanted := range []string{
		`bucket="bytes-only",resource="objects"`,
		`bucket="no-quota"`,
		`bucket="quota-error"`,
		`bucket="usage-error"`,
	} {
		if strings.Contains(body, unwanted) {
			t.Fatalf("metrics unexpectedly contain %q:\n%s", unwanted, body)
		}
	}
}

func TestWritePrometheusBucketQuotaEscapesBucketLabel(t *testing.T) {
	const bucket = "quoted\"\\line\nnext"
	source := &fakeBucketQuotaMetricsSource{
		buckets: []metadata.BucketInfo{{Name: bucket}},
		quotas: map[string]*metadata.BucketQuota{
			bucket: {MaxObjects: 2},
		},
		usages: map[string]*metadata.BucketUsage{
			bucket: {Name: bucket, Objects: 1},
		},
	}

	var output bytes.Buffer
	writePrometheusBucketQuota(context.Background(), &output, source)

	want := `nufs_bucket_quota_limit{bucket="quoted\"\\line\nnext",resource="objects"} 2`
	if !strings.Contains(output.String(), want) {
		t.Fatalf("escaped metric missing %q:\n%s", want, output.String())
	}
}

func TestPrometheusMetricsIncludesBucketQuotaAndObjectWriteOps(t *testing.T) {
	ctx := context.Background()
	store, bundle := newOpsTestStore(t)
	store.SetQuotaManager(metadata.NewQuotaManager())
	if err := store.CreateBucket(ctx, "photos", metadata.PlacementPolicy{ReplicationFactor: 1}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	if err := store.SetBucketQuota(ctx, "photos", &metadata.BucketQuota{MaxSizeBytes: 100, MaxObjects: 10}); err != nil {
		t.Fatalf("SetBucketQuota: %v", err)
	}
	if err := store.PutWriteAttempt(ctx, &metadata.ObjectWriteAttempt{ID: "failed-1", State: metadata.WriteAttemptFailed}); err != nil {
		t.Fatalf("PutWriteAttempt: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rr := httptest.NewRecorder()
	prometheusMetricsHandler(store, bundle.Metrics).ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status code = %d body=%s", rr.Code, rr.Body.String())
	}

	for _, want := range []string{
		`nufs_bucket_quota_limit{bucket="photos",resource="bytes"} 100`,
		`nufs_bucket_quota_usage{bucket="photos",resource="objects"} 0`,
		`nufs_bucket_quota_used_ratio{bucket="photos",resource="bytes"} 0`,
		`nufs_object_write_attempts{state="failed"} 1`,
	} {
		if !strings.Contains(rr.Body.String(), want) {
			t.Fatalf("prometheus metrics missing %q:\n%s", want, rr.Body.String())
		}
	}
}

type fakeBucketQuotaMetricsSource struct {
	buckets     []metadata.BucketInfo
	quotas      map[string]*metadata.BucketQuota
	usages      map[string]*metadata.BucketUsage
	listErr     error
	quotaErrors map[string]error
	usageErrors map[string]error
}

func (s *fakeBucketQuotaMetricsSource) ListBuckets(context.Context) ([]metadata.BucketInfo, error) {
	return s.buckets, s.listErr
}

func (s *fakeBucketQuotaMetricsSource) GetBucketQuota(_ context.Context, bucket string) (*metadata.BucketQuota, error) {
	if err := s.quotaErrors[bucket]; err != nil {
		return nil, err
	}
	return s.quotas[bucket], nil
}

func (s *fakeBucketQuotaMetricsSource) GetBucketUsage(_ context.Context, bucket string) (*metadata.BucketUsage, error) {
	if err := s.usageErrors[bucket]; err != nil {
		return nil, err
	}
	return s.usages[bucket], nil
}
