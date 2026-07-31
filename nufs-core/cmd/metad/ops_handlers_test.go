package main

import (
	"net/url"
	"testing"
)

func TestLeaderRedirectTargetPreservesEscapedPathAndQuery(t *testing.T) {
	requestURL := &url.URL{
		Path:     "/api/v1/buckets/./quota",
		RawPath:  "/api/v1/buckets/%2E/quota",
		RawQuery: "bucket_path=dot&source=admin",
	}

	got := leaderRedirectTarget("http://leader:8091", requestURL)
	want := "http://leader:8091/api/v1/buckets/%2E/quota?bucket_path=dot&source=admin"
	if got != want {
		t.Fatalf("leader redirect target = %q, want %q", got, want)
	}
}
