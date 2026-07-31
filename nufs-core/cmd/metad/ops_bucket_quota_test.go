package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/example/dfs/metadata"
)

func TestOpsHandlersBucketQuotaRoundTrip(t *testing.T) {
	ctx := context.Background()
	store, bundle := newOpsTestStore(t)
	store.SetQuotaManager(metadata.NewQuotaManager())
	if err := store.CreateBucket(ctx, "photos", metadata.PlacementPolicy{ReplicationFactor: 1}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	server := newOpsTestHTTPServer(t, store, bundle)
	client := metadata.NewHTTPClient(server.URL, 0)

	if err := client.SetBucketQuota(ctx, "photos", &metadata.BucketQuota{MaxSizeBytes: 1000, MaxObjects: 10, MaxChunkCount: 99}); err != nil {
		t.Fatalf("SetBucketQuota: %v", err)
	}
	got, err := client.GetBucketQuotaStatus(ctx, "photos")
	if err != nil {
		t.Fatalf("GetBucketQuotaStatus: %v", err)
	}
	if got.Bucket != "photos" || got.Quota == nil || got.Quota.MaxSizeBytes != 1000 || got.Quota.MaxObjects != 10 {
		t.Fatalf("quota status = %+v", got)
	}
	if got.Quota.MaxChunkCount != 0 {
		t.Fatalf("quota MaxChunkCount = %d, want omitted wire value", got.Quota.MaxChunkCount)
	}
	if got.Usage.Name != "photos" {
		t.Fatalf("usage = %+v", got.Usage)
	}
	quota, err := client.GetBucketQuota(ctx, "photos")
	if err != nil {
		t.Fatalf("GetBucketQuota: %v", err)
	}
	if quota == nil || quota.MaxSizeBytes != 1000 || quota.MaxObjects != 10 {
		t.Fatalf("quota = %+v", quota)
	}
	usage, err := client.GetBucketUsage(ctx, "photos")
	if err != nil {
		t.Fatalf("GetBucketUsage: %v", err)
	}
	if usage.Name != "photos" || usage.UsedBytes != 0 || usage.Objects != 0 {
		t.Fatalf("usage = %+v", usage)
	}
	if err := client.CheckBucketQuota(ctx, "photos", 999, 9); err != nil {
		t.Fatalf("CheckBucketQuota allow: %v", err)
	}
	err = client.CheckBucketQuota(ctx, "photos", 1001, 0)
	if !errors.Is(err, metadata.ErrQuotaExceeded) {
		t.Fatalf("CheckBucketQuota exceeded = %v", err)
	}
	if err := client.DeleteBucketQuota(ctx, "photos"); err != nil {
		t.Fatalf("DeleteBucketQuota: %v", err)
	}
	got, err = client.GetBucketQuotaStatus(ctx, "photos")
	if err != nil {
		t.Fatalf("GetBucketQuotaStatus after delete: %v", err)
	}
	if got.Quota != nil {
		t.Fatalf("quota after delete = %+v, want nil", got.Quota)
	}
}

func TestOpsHandlersBucketQuotaWireAndErrors(t *testing.T) {
	ctx := context.Background()
	store, bundle := newOpsTestStore(t)
	store.SetQuotaManager(metadata.NewQuotaManager())
	for _, name := range []string{"photos", "ordinary"} {
		if err := store.CreateBucket(ctx, name, metadata.PlacementPolicy{ReplicationFactor: 1}); err != nil {
			t.Fatalf("CreateBucket(%q): %v", name, err)
		}
	}
	server := newOpsTestHTTPServer(t, store, bundle)

	putBody := []byte(`{"max_bytes":1000,"max_objects":10}`)
	putResponse := doOpsQuotaRequest(t, server, http.MethodPut, "/api/v1/buckets/photos/quota", putBody)
	if putResponse.StatusCode != http.StatusOK {
		t.Fatalf("PUT quota status = %d, body=%s", putResponse.StatusCode, readOpsResponse(t, putResponse))
	}
	assertQuotaJSONWire(t, readOpsResponse(t, putResponse))

	getResponse := doOpsQuotaRequest(t, server, http.MethodGet, "/api/v1/buckets/photos/quota", nil)
	if getResponse.StatusCode != http.StatusOK {
		t.Fatalf("GET quota status = %d, body=%s", getResponse.StatusCode, readOpsResponse(t, getResponse))
	}
	assertQuotaJSONWire(t, readOpsResponse(t, getResponse))

	negativeResponse := doOpsQuotaRequest(t, server, http.MethodPut, "/api/v1/buckets/photos/quota", []byte(`{"max_bytes":-1}`))
	if negativeResponse.StatusCode != http.StatusBadRequest {
		t.Fatalf("PUT negative quota status = %d, body=%s", negativeResponse.StatusCode, readOpsResponse(t, negativeResponse))
	}
	_ = readOpsResponse(t, negativeResponse)

	internalFieldResponse := doOpsQuotaRequest(t, server, http.MethodPut, "/api/v1/buckets/photos/quota", []byte(`{"max_chunk_count":99}`))
	if internalFieldResponse.StatusCode != http.StatusBadRequest {
		t.Fatalf("PUT internal quota field status = %d, body=%s", internalFieldResponse.StatusCode, readOpsResponse(t, internalFieldResponse))
	}
	_ = readOpsResponse(t, internalFieldResponse)

	for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodDelete} {
		response := doOpsQuotaRequest(t, server, method, "/api/v1/buckets/missing/quota", []byte(`{"max_bytes":1}`))
		if response.StatusCode != http.StatusNotFound {
			t.Fatalf("%s missing quota status = %d, body=%s", method, response.StatusCode, readOpsResponse(t, response))
		}
		_ = readOpsResponse(t, response)
	}

	allowResponse := doOpsQuotaRequest(t, server, http.MethodPost, "/api/v1/buckets/photos/quota/check", []byte(`{"additional_bytes":1,"additional_objects":1}`))
	if allowResponse.StatusCode != http.StatusOK {
		t.Fatalf("POST quota check allow status = %d, body=%s", allowResponse.StatusCode, readOpsResponse(t, allowResponse))
	}
	_ = readOpsResponse(t, allowResponse)

	exceededResponse := doOpsQuotaRequest(t, server, http.MethodPost, "/api/v1/buckets/photos/quota/check", []byte(`{"additional_bytes":1001,"additional_objects":0}`))
	if exceededResponse.StatusCode != http.StatusConflict {
		t.Fatalf("POST quota check exceeded status = %d, body=%s", exceededResponse.StatusCode, readOpsResponse(t, exceededResponse))
	}
	exceededBody := readOpsResponse(t, exceededResponse)
	var exceededError map[string]string
	if err := json.Unmarshal([]byte(exceededBody), &exceededError); err != nil {
		t.Fatalf("decode quota exceeded response: %v", err)
	}
	if exceededError["code"] != "quota_exceeded" {
		t.Fatalf("quota exceeded response = %s", exceededBody)
	}

	malformedCheck := doOpsQuotaRequest(t, server, http.MethodPost, "/api/v1/buckets/photos/quota/check", []byte(`{"additional_bytes":"bad"}`))
	if malformedCheck.StatusCode != http.StatusBadRequest {
		t.Fatalf("POST malformed quota check status = %d, body=%s", malformedCheck.StatusCode, readOpsResponse(t, malformedCheck))
	}
	_ = readOpsResponse(t, malformedCheck)

	missingCheck := doOpsQuotaRequest(t, server, http.MethodPost, "/api/v1/buckets/missing/quota/check", []byte(`{"additional_bytes":1}`))
	if missingCheck.StatusCode != http.StatusNotFound {
		t.Fatalf("POST missing quota check status = %d, body=%s", missingCheck.StatusCode, readOpsResponse(t, missingCheck))
	}
	_ = readOpsResponse(t, missingCheck)

	emptyResponse := doOpsQuotaRequest(t, server, http.MethodGet, "/api/v1/buckets//quota", nil)
	if emptyResponse.StatusCode != http.StatusBadRequest {
		t.Fatalf("GET empty bucket quota status = %d, body=%s", emptyResponse.StatusCode, readOpsResponse(t, emptyResponse))
	}
	_ = readOpsResponse(t, emptyResponse)

	ordinaryGet := doOpsQuotaRequest(t, server, http.MethodGet, "/api/v1/buckets/ordinary", nil)
	if ordinaryGet.StatusCode != http.StatusOK {
		t.Fatalf("GET ordinary bucket status = %d, body=%s", ordinaryGet.StatusCode, readOpsResponse(t, ordinaryGet))
	}
	_ = readOpsResponse(t, ordinaryGet)
	ordinaryDelete := doOpsQuotaRequest(t, server, http.MethodDelete, "/api/v1/buckets/ordinary", nil)
	if ordinaryDelete.StatusCode != http.StatusOK {
		t.Fatalf("DELETE ordinary bucket status = %d, body=%s", ordinaryDelete.StatusCode, readOpsResponse(t, ordinaryDelete))
	}
	_ = readOpsResponse(t, ordinaryDelete)
}

func TestWriteBucketQuotaErrorMatchesWrappedBucketNotFound(t *testing.T) {
	rr := httptest.NewRecorder()
	writeBucketQuotaError(rr, fmt.Errorf("lookup bucket: %w", metadata.ErrBucketNotFound))
	if rr.Code != http.StatusNotFound {
		t.Fatalf("wrapped ErrBucketNotFound status = %d, body=%s", rr.Code, rr.Body.String())
	}
	if !errors.Is(fmt.Errorf("lookup bucket: %w", metadata.ErrBucketNotFound), metadata.ErrBucketNotFound) {
		t.Fatal("sanity check: expected wrapped error to match")
	}
}

func TestWriteBucketQuotaErrorClassifiesExceededAndBackendFailure(t *testing.T) {
	for _, tc := range []struct {
		name       string
		err        error
		statusCode int
		code       string
	}{
		{name: "quota exceeded", err: fmt.Errorf("check: %w", metadata.ErrQuotaExceeded), statusCode: http.StatusConflict, code: "quota_exceeded"},
		{name: "backend failure", err: errors.New("pebble unavailable"), statusCode: http.StatusInternalServerError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rr := httptest.NewRecorder()
			writeBucketQuotaError(rr, tc.err)
			if rr.Code != tc.statusCode {
				t.Fatalf("status = %d, want %d, body=%s", rr.Code, tc.statusCode, rr.Body.String())
			}
			var body map[string]string
			if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
				t.Fatalf("decode response: %v", err)
			}
			if body["code"] != tc.code {
				t.Fatalf("code = %q, want %q, body=%s", body["code"], tc.code, rr.Body.String())
			}
		})
	}
}

func newOpsTestHTTPServer(t *testing.T, store *metadata.PebbleStore, bundle *metadata.ServiceBundle) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	registerOpsHandlers(mux, store, bundle, "")
	server := httptest.NewServer(rejectEmptyBucketQuotaPath(mux))
	t.Cleanup(server.Close)
	return server
}

func doOpsQuotaRequest(t *testing.T, server *httptest.Server, method, path string, body []byte) *http.Response {
	t.Helper()
	request, err := http.NewRequest(method, server.URL+path, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatalf("Do(%s %s): %v", method, path, err)
	}
	return response
}

func readOpsResponse(t *testing.T, response *http.Response) string {
	t.Helper()
	defer response.Body.Close()
	var raw json.RawMessage
	if err := json.NewDecoder(response.Body).Decode(&raw); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return string(raw)
}

func assertQuotaJSONWire(t *testing.T, body string) {
	t.Helper()
	var status struct {
		Quota map[string]json.RawMessage `json:"quota"`
	}
	if err := json.Unmarshal([]byte(body), &status); err != nil {
		t.Fatalf("unmarshal quota response: %v", err)
	}
	if _, ok := status.Quota["max_bytes"]; !ok {
		t.Fatalf("quota wire missing max_bytes: %s", body)
	}
	if _, ok := status.Quota["max_objects"]; !ok {
		t.Fatalf("quota wire missing max_objects: %s", body)
	}
	if _, ok := status.Quota["max_chunk_count"]; ok {
		t.Fatalf("quota wire leaked max_chunk_count: %s", body)
	}
}
