// Copyright (c) 2021 MinIO, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

//go:build linux
// +build linux

package minfs

import (
	"errors"
	"net"
	"testing"
)

func TestRetryWithBackoff_Success(t *testing.T) {
	calls := 0
	err := retryWithBackoff(func() error {
		calls++
		return nil
	})
	if err != nil {
		t.Fatalf("expected nil error, got: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected 1 call, got: %d", calls)
	}
}

func TestRetryWithBackoff_NonRetryableError(t *testing.T) {
	permErr := errors.New("permission denied")
	calls := 0
	err := retryWithBackoff(func() error {
		calls++
		return permErr
	})
	if err != permErr {
		t.Fatalf("expected permission denied, got: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected no retries for non-retryable error, calls: %d", calls)
	}
}

func TestRetryWithBackoff_EventualSuccess(t *testing.T) {
	calls := 0
	err := retryWithBackoff(func() error {
		calls++
		if calls < 3 {
			return &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("expected nil after retries, got: %v", err)
	}
	if calls != 3 {
		t.Fatalf("expected 3 calls, got: %d", calls)
	}
}

func TestRetryWithBackoff_MaxRetriesExhausted(t *testing.T) {
	calls := 0
	retryableErr := &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}
	err := retryWithBackoff(func() error {
		calls++
		return retryableErr
	})
	if err != retryableErr {
		t.Fatalf("expected retryable error after exhaustion, got: %v", err)
	}
	// 1 initial + maxRetries retries = maxRetries + 1 total
	expected := maxRetries + 1
	if calls != expected {
		t.Fatalf("expected %d calls, got: %d", expected, calls)
	}
}

func TestIsRetryableError_NilError(t *testing.T) {
	if isRetryableError(nil) {
		t.Fatal("nil error should not be retryable")
	}
}

func TestIsRetryableError_GenericError(t *testing.T) {
	if isRetryableError(errors.New("random error")) {
		t.Fatal("generic error should not be retryable")
	}
}

func TestIsRetryableError_NetError(t *testing.T) {
	netErr := &net.OpError{Op: "read", Net: "tcp", Err: errors.New("connection reset")}
	if !isRetryableError(netErr) {
		t.Fatal("net.OpError should be retryable")
	}
}

type mockHTTPStatusError struct {
	code int
}

func (e *mockHTTPStatusError) Error() string       { return "http error" }
func (e *mockHTTPStatusError) HTTPStatusCode() int { return e.code }

func TestIsRetryableError_HTTP5xx(t *testing.T) {
	for _, code := range []int{500, 502, 503, 504, 429} {
		err := &mockHTTPStatusError{code: code}
		if !isRetryableError(err) {
			t.Fatalf("HTTP %d should be retryable", code)
		}
	}
}

func TestIsRetryableError_HTTP4xx(t *testing.T) {
	for _, code := range []int{400, 401, 403, 404, 409} {
		err := &mockHTTPStatusError{code: code}
		if isRetryableError(err) {
			t.Fatalf("HTTP %d should NOT be retryable", code)
		}
	}
}
