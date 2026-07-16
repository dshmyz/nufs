package metadata

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestHTTPClientSendsAuthToken(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer secret" {
			t.Fatalf("expected Authorization header, got %q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[]`))
	}))
	defer srv.Close()

	c := NewHTTPClient(srv.URL, time.Second)
	c.SetAuthToken("secret")
	if _, err := c.ListBuckets(context.Background()); err != nil {
		t.Fatalf("ListBuckets: %v", err)
	}
}

func TestHTTPClientSendsAuthTokenOnRedirect(t *testing.T) {
	leader := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer secret" {
			t.Fatalf("expected redirected Authorization header, got %q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[]`))
	}))
	defer leader.Close()

	follower := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, leader.URL+r.URL.Path, http.StatusTemporaryRedirect)
	}))
	defer follower.Close()

	c := NewHTTPClient(follower.URL, time.Second)
	c.SetAuthToken("secret")
	if _, err := c.ListBuckets(context.Background()); err != nil {
		t.Fatalf("ListBuckets via redirect: %v", err)
	}
}

func TestHTTPClientXAttrRoundTripOverJSON(t *testing.T) {
	var stored []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPut && r.URL.Path == "/api/v1/inodes/42/xattrs/user.mime":
			var req struct {
				Value []byte `json:"value"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode set xattr request: %v", err)
			}
			stored = append([]byte(nil), req.Value...)
			_, _ = w.Write([]byte(`{"status":"updated"}`))
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/inodes/42/xattrs/user.mime":
			_ = json.NewEncoder(w).Encode(map[string][]byte{"value": stored})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/inodes/42/xattrs":
			_ = json.NewEncoder(w).Encode(map[string][]byte{"user.mime": stored})
		case r.Method == http.MethodDelete && r.URL.Path == "/api/v1/inodes/42/xattrs/user.mime":
			stored = nil
			_, _ = w.Write([]byte(`{"status":"removed"}`))
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer srv.Close()

	c := NewHTTPClient(srv.URL, time.Second)
	ctx := context.Background()
	value := []byte{0, 1, 2, 255, 'n', 'u'}
	if err := c.SetXAttr(ctx, 42, "user.mime", value); err != nil {
		t.Fatalf("SetXAttr: %v", err)
	}
	got, err := c.GetXAttr(ctx, 42, "user.mime")
	if err != nil {
		t.Fatalf("GetXAttr: %v", err)
	}
	if string(got) != string(value) {
		t.Fatalf("xattr value mismatch: got %v want %v", got, value)
	}
	listed, err := c.ListXAttr(ctx, 42)
	if err != nil {
		t.Fatalf("ListXAttr: %v", err)
	}
	if string(listed["user.mime"]) != string(value) {
		t.Fatalf("listed xattr mismatch: got %v want %v", listed["user.mime"], value)
	}
	if err := c.RemoveXAttr(ctx, 42, "user.mime"); err != nil {
		t.Fatalf("RemoveXAttr: %v", err)
	}
}

func TestHTTPClientGetXAttrMapsNotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":"missing","code":"xattr_not_found"}`))
	}))
	defer srv.Close()

	c := NewHTTPClient(srv.URL, time.Second)
	_, err := c.GetXAttr(context.Background(), 42, "user.missing")
	if !errors.Is(err, ErrXAttrNotFound) {
		t.Fatalf("expected ErrXAttrNotFound, got %v", err)
	}
}
