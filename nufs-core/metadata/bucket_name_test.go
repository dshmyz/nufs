package metadata

import (
	"context"
	"errors"
	"testing"
)

func TestPebbleStoreCreateBucketRejectsSlashInName(t *testing.T) {
	store := newTestPebbleStore(t)

	err := store.CreateBucket(context.Background(), "foo/quota", PlacementPolicy{})
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("CreateBucket(\"foo/quota\") error = %v, want ErrInvalidArgument", err)
	}
}
