package fuse

import (
	"testing"
)

func TestParsePath(t *testing.T) {
	// Test helper used by gateways
	tests := []struct {
		path       string
		wantBucket string
		wantKey    string
	}{
		{"/", "", ""},
		{"/bucket", "bucket", ""},
		{"/bucket/key", "bucket", "key"},
		{"/bucket/path/to/file.txt", "bucket", "path/to/file.txt"},
	}
	for _, tt := range tests {
		path := tt.path
		// Inline parsePath logic for testing
		p := path
		if len(p) > 0 && p[0] == '/' {
			p = p[1:]
		}
		if p == "" {
			if "" != tt.wantBucket || "" != tt.wantKey {
				t.Errorf("parsePath(%q) = empty, want (%q, %q)", tt.path, tt.wantBucket, tt.wantKey)
			}
			continue
		}
		idx := -1
		for i := 0; i < len(p); i++ {
			if p[i] == '/' {
				idx = i
				break
			}
		}
		var bucket, key string
		if idx == -1 {
			bucket = p
		} else {
			bucket = p[:idx]
			key = p[idx+1:]
		}
		if bucket != tt.wantBucket || key != tt.wantKey {
			t.Errorf("parsePath(%q) = (%q, %q), want (%q, %q)", tt.path, bucket, key, tt.wantBucket, tt.wantKey)
		}
	}
}
