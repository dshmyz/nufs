package main

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestMaxBodySizeMiddleware_RejectsOversizedBody(t *testing.T) {
	const limit = 1024 // 1 KiB for test
	handler := maxBodySizeMiddleware(limit, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusRequestEntityTooLarge)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write(body)
	}))

	// Body exactly at limit should succeed.
	small := bytes.Repeat([]byte("x"), limit)
	req := httptest.NewRequest(http.MethodPost, "/test", bytes.NewReader(small))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 for body at limit, got %d", rec.Code)
	}

	// Body exceeding limit should be rejected.
	big := bytes.Repeat([]byte("x"), limit+1)
	req = httptest.NewRequest(http.MethodPost, "/test", bytes.NewReader(big))
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413 for oversized body, got %d", rec.Code)
	}
}

func TestMaxBodySizeMiddleware_PassesGetUntouched(t *testing.T) {
	const limit = 10
	handler := maxBodySizeMiddleware(limit, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 for GET, got %d", rec.Code)
	}
}
