package api

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/your-org/nufs-admin/internal/auth"
	"github.com/your-org/nufs-admin/internal/cache"
	"github.com/your-org/nufs-admin/internal/cluster"
	"github.com/your-org/nufs-admin/internal/config"
	"github.com/your-org/nufs-admin/internal/proxy"
)

func TestBucketQuotaProxyMethodsAndEscapedPath(t *testing.T) {
	type seenRequest struct {
		method      string
		escapedPath string
		contentType string
		body        string
	}
	var (
		mu   sync.Mutex
		seen []seenRequest
	)

	metad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read request body: %v", err)
		}
		mu.Lock()
		seen = append(seen, seenRequest{
			method:      r.Method,
			escapedPath: r.URL.EscapedPath(),
			contentType: r.Header.Get("Content-Type"),
			body:        string(body),
		})
		mu.Unlock()

		switch r.Method {
		case http.MethodGet:
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"max_bytes":1024,"max_objects":7,"used_bytes":12,"used_objects":2}`))
		case http.MethodPut:
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"max_bytes":2048,"max_objects":9,"used_bytes":12,"used_objects":2}`))
		case http.MethodDelete:
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Fatalf("unexpected method %s", r.Method)
		}
	}))
	defer metad.Close()

	router, cleanup := newBucketQuotaTestRouter(t, metad.URL, time.Minute)
	defer cleanup()

	target := "/api/v1/clusters/prod/buckets/bucket%25%3F%23%20name/quota"
	tests := []struct {
		method     string
		body       string
		wantStatus int
		wantBody   string
	}{
		{method: http.MethodGet, wantStatus: http.StatusOK, wantBody: `"max_bytes":1024`},
		{method: http.MethodPut, body: `{"max_bytes":2048,"max_objects":9}`, wantStatus: http.StatusOK, wantBody: `"max_bytes":2048`},
		{method: http.MethodDelete, wantStatus: http.StatusNoContent},
	}
	for _, tt := range tests {
		t.Run(tt.method, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, target, strings.NewReader(tt.body))
			rr := httptest.NewRecorder()
			router.handleClusterRoutes(rr, req)

			if rr.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d; body=%s", rr.Code, tt.wantStatus, rr.Body.String())
			}
			if tt.wantBody != "" && !strings.Contains(rr.Body.String(), tt.wantBody) {
				t.Fatalf("body = %q, want substring %q", rr.Body.String(), tt.wantBody)
			}
		})
	}

	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 3 {
		t.Fatalf("upstream requests = %d, want 3", len(seen))
	}
	for _, got := range seen {
		if got.escapedPath != "/api/v1/buckets/bucket%25%3F%23%20name/quota" {
			t.Errorf("%s escaped path = %q", got.method, got.escapedPath)
		}
	}
	if seen[1].contentType != "application/json" {
		t.Errorf("PUT content type = %q, want application/json", seen[1].contentType)
	}
	if seen[1].body != `{"max_bytes":2048,"max_objects":9}` {
		t.Errorf("PUT body = %q", seen[1].body)
	}
}

func TestBucketQuotaDotSegmentsSurviveAdminAndMetadServeMuxes(t *testing.T) {
	tests := []struct {
		name            string
		method          string
		adminSegment    string
		adminQuery      string
		wantBucket      string
		wantEscapedPath string
		body            string
	}{
		{
			name:            "dot GET",
			method:          http.MethodGet,
			adminSegment:    "%252E",
			adminQuery:      "?bucket_path=dot",
			wantBucket:      ".",
			wantEscapedPath: "/api/v1/buckets/%2E/quota",
		},
		{
			name:            "dot-dot PUT",
			method:          http.MethodPut,
			adminSegment:    "%252E%252E",
			adminQuery:      "?bucket_path=dotdot",
			wantBucket:      "..",
			wantEscapedPath: "/api/v1/buckets/%2E%2E/quota",
			body:            `{"max_bytes":1024,"max_objects":1}`,
		},
		{
			name:            "literal percent-encoded dot GET",
			method:          http.MethodGet,
			adminSegment:    "%252E",
			wantBucket:      "%2E",
			wantEscapedPath: "/api/v1/buckets/%252E/quota",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var (
				metadRequests int
				gotBucket     string
				gotPath       string
			)
			metadMux := http.NewServeMux()
			metadMux.HandleFunc("/api/v1/buckets/", func(w http.ResponseWriter, r *http.Request) {
				metadRequests++
				gotPath = r.URL.EscapedPath()
				pathParts := strings.Split(r.URL.Path, "/")
				if len(pathParts) >= 5 {
					gotBucket = pathParts[4]
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"max_bytes":1024,"max_objects":1}`))
			})
			metad := httptest.NewServer(metadMux)
			defer metad.Close()

			router, cleanup := newBucketQuotaTestRouter(t, metad.URL, time.Minute)
			defer cleanup()
			adminMux := http.NewServeMux()
			router.Setup(adminMux)
			admin := httptest.NewServer(adminMux)
			defer admin.Close()

			token, err := router.jwt.GenerateToken("quota-test")
			if err != nil {
				t.Fatalf("GenerateToken: %v", err)
			}
			req, err := http.NewRequest(
				tt.method,
				admin.URL+"/api/v1/clusters/prod/buckets/"+tt.adminSegment+"/quota"+tt.adminQuery,
				strings.NewReader(tt.body),
			)
			if err != nil {
				t.Fatalf("NewRequest: %v", err)
			}
			req.Header.Set("Authorization", "Bearer "+token)
			adminClient := &http.Client{
				CheckRedirect: func(*http.Request, []*http.Request) error {
					return http.ErrUseLastResponse
				},
			}

			resp, err := adminClient.Do(req)
			if err != nil {
				t.Fatalf("Admin request: %v", err)
			}
			defer resp.Body.Close()
			responseBody, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("read Admin response: %v", err)
			}

			if resp.StatusCode != http.StatusOK {
				t.Fatalf("Admin status = %d, want 200; location=%q body=%s", resp.StatusCode, resp.Header.Get("Location"), responseBody)
			}
			if metadRequests != 1 {
				t.Fatalf("metad requests = %d, want 1", metadRequests)
			}
			if gotBucket != tt.wantBucket {
				t.Fatalf("metad bucket = %q, want %q", gotBucket, tt.wantBucket)
			}
			if gotPath != tt.wantEscapedPath {
				t.Fatalf("metad escaped path = %q, want %q", gotPath, tt.wantEscapedPath)
			}
		})
	}
}

func TestBucketDeleteDistinguishesDotFromLiteralPercentEncodedDot(t *testing.T) {
	tests := []struct {
		name            string
		adminTarget     string
		wantBucket      string
		wantEscapedPath string
	}{
		{
			name:            "dot marker",
			adminTarget:     "/api/v1/clusters/prod/buckets/%252E?bucket_path=dot",
			wantBucket:      ".",
			wantEscapedPath: "/api/v1/buckets/%2E",
		},
		{
			name:            "literal percent-encoded dot",
			adminTarget:     "/api/v1/clusters/prod/buckets/%252E",
			wantBucket:      "%2E",
			wantEscapedPath: "/api/v1/buckets/%252E",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var (
				metadRequests int
				gotBucket     string
				gotPath       string
			)
			metadMux := http.NewServeMux()
			metadMux.HandleFunc("/api/v1/buckets/", func(w http.ResponseWriter, r *http.Request) {
				metadRequests++
				gotPath = r.URL.EscapedPath()
				pathParts := strings.Split(r.URL.Path, "/")
				if len(pathParts) >= 5 {
					gotBucket = pathParts[4]
				}
				w.WriteHeader(http.StatusOK)
			})
			metad := httptest.NewServer(metadMux)
			defer metad.Close()

			router, cleanup := newBucketQuotaTestRouter(t, metad.URL, time.Minute)
			defer cleanup()
			adminMux := http.NewServeMux()
			router.Setup(adminMux)
			admin := httptest.NewServer(adminMux)
			defer admin.Close()

			token, err := router.jwt.GenerateToken("quota-test")
			if err != nil {
				t.Fatalf("GenerateToken: %v", err)
			}
			req, err := http.NewRequest(http.MethodDelete, admin.URL+tt.adminTarget, nil)
			if err != nil {
				t.Fatalf("NewRequest: %v", err)
			}
			req.Header.Set("Authorization", "Bearer "+token)

			resp, err := admin.Client().Do(req)
			if err != nil {
				t.Fatalf("Admin request: %v", err)
			}
			defer resp.Body.Close()
			responseBody, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("read Admin response: %v", err)
			}

			if resp.StatusCode != http.StatusOK {
				t.Fatalf("Admin status = %d, want 200; body=%s", resp.StatusCode, responseBody)
			}
			if metadRequests != 1 {
				t.Fatalf("metad requests = %d, want 1", metadRequests)
			}
			if gotBucket != tt.wantBucket {
				t.Fatalf("metad bucket = %q, want %q", gotBucket, tt.wantBucket)
			}
			if gotPath != tt.wantEscapedPath {
				t.Fatalf("metad escaped path = %q, want %q", gotPath, tt.wantEscapedPath)
			}
		})
	}
}

func TestBucketQuotaProxyPreservesUpstreamJSONErrors(t *testing.T) {
	for _, status := range []int{
		http.StatusBadRequest,
		http.StatusNotFound,
		http.StatusConflict,
		http.StatusInternalServerError,
	} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			metad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json; charset=utf-8")
				w.WriteHeader(status)
				_, _ = w.Write([]byte(`{"error":"quota request rejected"}`))
			}))
			defer metad.Close()

			router, cleanup := newBucketQuotaTestRouter(t, metad.URL, time.Minute)
			defer cleanup()

			req := httptest.NewRequest(http.MethodGet, "/api/v1/clusters/prod/buckets/test/quota", nil)
			rr := httptest.NewRecorder()
			router.handleClusterRoutes(rr, req)

			if rr.Code != status {
				t.Fatalf("status = %d, want %d; body=%s", rr.Code, status, rr.Body.String())
			}
			if got := rr.Header().Get("Content-Type"); got != "application/json; charset=utf-8" {
				t.Fatalf("content type = %q", got)
			}
			if strings.TrimSpace(rr.Body.String()) != `{"error":"quota request rejected"}` {
				t.Fatalf("body = %q", rr.Body.String())
			}
		})
	}
}

func TestBucketQuotaProxyDoesNotExposeNonJSONUpstreamError(t *testing.T) {
	metad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("database password=secret"))
	}))
	defer metad.Close()

	router, cleanup := newBucketQuotaTestRouter(t, metad.URL, time.Minute)
	defer cleanup()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/clusters/prod/buckets/test/quota", nil)
	rr := httptest.NewRecorder()
	router.handleClusterRoutes(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusInternalServerError)
	}
	if strings.Contains(rr.Body.String(), "password") || strings.Contains(rr.Body.String(), "secret") {
		t.Fatalf("response leaks upstream body: %q", rr.Body.String())
	}
}

func TestBucketQuotaProxyNetworkFailureReturnsServiceUnavailable(t *testing.T) {
	metad := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	metadURL := metad.URL
	metad.Close()

	router, cleanup := newBucketQuotaTestRouter(t, metadURL, time.Minute)
	defer cleanup()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/clusters/prod/buckets/test/quota", nil)
	rr := httptest.NewRecorder()
	router.handleClusterRoutes(rr, req)

	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d; body=%s", rr.Code, http.StatusServiceUnavailable, rr.Body.String())
	}
	if strings.Contains(rr.Body.String(), metadURL) {
		t.Fatalf("response leaks upstream address: %q", rr.Body.String())
	}
}

func TestBucketQuotaPutEmptyBodyPreservesMetadBadRequest(t *testing.T) {
	metad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		if len(body) != 0 {
			t.Fatalf("body = %q, want empty", body)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"request body is required"}`))
	}))
	defer metad.Close()

	router, cleanup := newBucketQuotaTestRouter(t, metad.URL, time.Minute)
	defer cleanup()

	req := httptest.NewRequest(http.MethodPut, "/api/v1/clusters/prod/buckets/test/quota", nil)
	rr := httptest.NewRecorder()
	router.handleClusterRoutes(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusBadRequest)
	}
	if strings.TrimSpace(rr.Body.String()) != `{"error":"request body is required"}` {
		t.Fatalf("body = %q", rr.Body.String())
	}
}

func TestBucketQuotaProxyAcceptsUpstreamSuccessStatuses(t *testing.T) {
	for _, tt := range []struct {
		name           string
		method         string
		upstreamStatus int
		responseBody   string
		wantStatus     int
	}{
		{
			name:           "PUT 201",
			method:         http.MethodPut,
			upstreamStatus: http.StatusCreated,
			responseBody:   `{"max_bytes":2,"max_objects":0}`,
			wantStatus:     http.StatusOK,
		},
		{
			name:           "DELETE 200",
			method:         http.MethodDelete,
			upstreamStatus: http.StatusOK,
			wantStatus:     http.StatusNoContent,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			metad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tt.upstreamStatus)
				_, _ = w.Write([]byte(tt.responseBody))
			}))
			defer metad.Close()

			router, cleanup := newBucketQuotaTestRouter(t, metad.URL, time.Minute)
			defer cleanup()

			req := httptest.NewRequest(tt.method, "/api/v1/clusters/prod/buckets/test/quota", strings.NewReader(`{"max_bytes":2}`))
			rr := httptest.NewRecorder()
			router.handleClusterRoutes(rr, req)
			if rr.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d; body=%s", rr.Code, tt.wantStatus, rr.Body.String())
			}
		})
	}
}

func TestBucketQuotaPutInvalidatesCachedGet(t *testing.T) {
	var (
		mu       sync.Mutex
		maxBytes int64 = 1
		gets     int
	)
	metad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodGet:
			gets++
		case http.MethodPut:
			var request struct {
				MaxBytes int64 `json:"max_bytes"`
			}
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Fatalf("decode PUT: %v", err)
			}
			maxBytes = request.MaxBytes
		default:
			t.Fatalf("unexpected method %s", r.Method)
		}
		_, _ = io.WriteString(w, `{"max_bytes":`+strconv.FormatInt(maxBytes, 10)+`,"max_objects":0}`)
	}))
	defer metad.Close()

	router, cleanup := newBucketQuotaTestRouter(t, metad.URL, time.Hour)
	defer cleanup()
	target := "/api/v1/clusters/prod/buckets/test/quota"

	assertQuotaMaxBytes(t, router, target, 1)

	req := httptest.NewRequest(http.MethodPut, target, strings.NewReader(`{"max_bytes":2,"max_objects":0}`))
	rr := httptest.NewRecorder()
	router.handleClusterRoutes(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("PUT status = %d body=%s", rr.Code, rr.Body.String())
	}

	assertQuotaMaxBytes(t, router, target, 2)

	mu.Lock()
	defer mu.Unlock()
	if gets != 2 {
		t.Fatalf("upstream GET count = %d, want 2", gets)
	}
}

func TestBucketQuotaPutBodyLimit(t *testing.T) {
	var upstreamRequests int
	metad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamRequests++
		w.WriteHeader(http.StatusOK)
	}))
	defer metad.Close()

	router, cleanup := newBucketQuotaTestRouter(t, metad.URL, time.Minute)
	defer cleanup()

	body := bytes.Repeat([]byte("x"), (64<<10)+1)
	req := httptest.NewRequest(http.MethodPut, "/api/v1/clusters/prod/buckets/test/quota", bytes.NewReader(body))
	rr := httptest.NewRecorder()
	router.handleClusterRoutes(rr, req)

	if rr.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want %d; body=%s", rr.Code, http.StatusRequestEntityTooLarge, rr.Body.String())
	}
	if upstreamRequests != 0 {
		t.Fatalf("upstream requests = %d, want 0", upstreamRequests)
	}
}

func TestBucketQuotaMethodAndPathValidation(t *testing.T) {
	router, cleanup := newBucketQuotaTestRouter(t, "http://127.0.0.1:1", time.Minute)
	defer cleanup()

	t.Run("method", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/clusters/prod/buckets/test/quota", nil)
		rr := httptest.NewRecorder()
		router.handleClusterRoutes(rr, req)
		if rr.Code != http.StatusMethodNotAllowed {
			t.Fatalf("status = %d, want %d", rr.Code, http.StatusMethodNotAllowed)
		}
		if got := rr.Header().Get("Allow"); got != "GET, PUT, DELETE" {
			t.Fatalf("Allow = %q", got)
		}
	})

	t.Run("unknown subresource", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/clusters/prod/buckets/test/not-quota", nil)
		rr := httptest.NewRecorder()
		router.handleClusterRoutes(rr, req)
		if rr.Code != http.StatusNotFound {
			t.Fatalf("status = %d, want %d", rr.Code, http.StatusNotFound)
		}
	})

	t.Run("extra segment", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/clusters/prod/buckets/test/quota/extra", nil)
		rr := httptest.NewRecorder()
		router.handleClusterRoutes(rr, req)
		if rr.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want %d", rr.Code, http.StatusBadRequest)
		}
	})
}

func newBucketQuotaTestRouter(t *testing.T, metadURL string, ttl time.Duration) (*Router, func()) {
	t.Helper()

	cfgPath := writeAdminTestConfig(t, metadURL)
	cfgMgr, err := config.NewManager(cfgPath, nil)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	registry, err := cluster.NewRegistry(cfgMgr, nil)
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	responseCache := cache.New(ttl)
	pr := proxy.NewProxy(registry, responseCache)
	router := NewRouter(pr, proxy.NewAggregator(pr), auth.NewJWTManager("secret"), &auth.UserStore{}, registry)
	return router, func() {
		responseCache.Close()
		registry.Close()
	}
}

func assertQuotaMaxBytes(t *testing.T, router *Router, target string, want int64) {
	t.Helper()

	req := httptest.NewRequest(http.MethodGet, target, nil)
	rr := httptest.NewRecorder()
	router.handleClusterRoutes(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("GET status = %d body=%s", rr.Code, rr.Body.String())
	}
	var response struct {
		MaxBytes int64 `json:"max_bytes"`
	}
	if err := json.NewDecoder(rr.Body).Decode(&response); err != nil {
		t.Fatalf("decode GET: %v", err)
	}
	if response.MaxBytes != want {
		t.Fatalf("max_bytes = %d, want %d", response.MaxBytes, want)
	}
}
