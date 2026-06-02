package s3

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/example/dfs/metadata"
)

// --- MaxObjectSize ---

func TestPutObject_ExceedsMaxObjectSize(t *testing.T) {
	meta := newMockMetaService()
	meta.RegisterNode(context.Background(), &metadata.NodeInfo{
		ID: 1, Addr: "127.0.0.1:9001", State: metadata.NodeOnline,
	})
	// 16-byte cap so a 1 KiB body trips it.
	gw := NewGateway(GatewayConfig{
		MetaService:   meta,
		Creds:         NewCredentialStore(),
		ChunkStore:    NewMemoryChunkStore(),
		MaxObjectSize: 16,
	})
	ts := httptest.NewServer(gw.Handler())
	defer ts.Close()

	if r, _ := http.NewRequest(http.MethodPut, ts.URL+"/sizebucket", nil); r != nil {
		if resp, err := http.DefaultClient.Do(r); err == nil {
			resp.Body.Close()
		}
	}

	body := strings.NewReader(strings.Repeat("x", 1024))
	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/sizebucket/big.bin", body)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d", resp.StatusCode)
	}
	respBody, _ := io.ReadAll(resp.Body)
	if !bytes.Contains(respBody, []byte("EntityTooLarge")) {
		t.Errorf("expected EntityTooLarge code in body, got: %s", respBody)
	}
}

func TestPutObject_WithinMaxObjectSize(t *testing.T) {
	_, ts, _ := newTestGateway(t)
	defer ts.Close()

	if r, _ := http.NewRequest(http.MethodPut, ts.URL+"/sizeok", nil); r != nil {
		if resp, err := http.DefaultClient.Do(r); err == nil {
			resp.Body.Close()
		}
	}

	body := strings.NewReader("ok")
	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/sizeok/ok.bin", body)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}

// --- RejectEmptyReplicas ---

func TestPutObject_RejectsEmptyReplicas(t *testing.T) {
	meta := newMockMetaService()
	// Intentionally no node is registered; mock's AllocateChunk
	// returns a chunk with no replicas regardless of the node list.
	gw := NewGateway(GatewayConfig{
		MetaService:         meta,
		Creds:               NewCredentialStore(),
		ChunkStore:          NewMemoryChunkStore(),
		RejectEmptyReplicas: true,
	})
	ts := httptest.NewServer(gw.Handler())
	defer ts.Close()

	if r, _ := http.NewRequest(http.MethodPut, ts.URL+"/nobucket", nil); r != nil {
		resp, _ := http.DefaultClient.Do(r)
		resp.Body.Close()
	}

	body := strings.NewReader("hi")
	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/nobucket/k", body)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", resp.StatusCode)
	}
	respBody, _ := io.ReadAll(resp.Body)
	if !bytes.Contains(respBody, []byte("ServiceUnavailable")) {
		t.Errorf("expected ServiceUnavailable code, got: %s", respBody)
	}
}

// --- Healthz / readyz ---

func TestHealthz_DefaultOK(t *testing.T) {
	_, ts, _ := newTestGateway(t)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}

func TestHealthz_CustomCheckDown(t *testing.T) {
	meta := newMockMetaService()
	gw := NewGateway(GatewayConfig{
		MetaService: meta,
		Creds:       NewCredentialStore(),
		ChunkStore:  NewMemoryChunkStore(),
		HealthCheck: func(_ context.Context) error { return errors.New("datanode down") },
	})
	ts := httptest.NewServer(gw.Handler())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", resp.StatusCode)
	}
}

func TestReadyz_IndependentOfHealthz(t *testing.T) {
	meta := newMockMetaService()
	gw := NewGateway(GatewayConfig{
		MetaService: meta,
		Creds:       NewCredentialStore(),
		ChunkStore:  NewMemoryChunkStore(),
		HealthCheck: func(_ context.Context) error { return nil },
		ReadyCheck:  func(_ context.Context) error { return errors.New("metadata unreachable") },
	})
	ts := httptest.NewServer(gw.Handler())
	defer ts.Close()

	resp, _ := http.Get(ts.URL + "/healthz")
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("healthz: expected 200, got %d", resp.StatusCode)
	}

	resp, _ = http.Get(ts.URL + "/readyz")
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("readyz: expected 503, got %d", resp.StatusCode)
	}
}

// --- Graceful shutdown ---

func TestRun_GracefulShutdown(t *testing.T) {
	meta := newMockMetaService()
	gw := NewGateway(GatewayConfig{
		MetaService: meta,
		Creds:       NewCredentialStore(),
		ChunkStore:  NewMemoryChunkStore(),
	})

	// Pick a free port; Run will re-listen on it.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runErr := make(chan error, 1)
	go func() {
		runErr <- gw.Run(ctx, ServerConfig{
			Addr:            addr,
			GracefulTimeout: 2 * time.Second,
			// Trap is a no-op so we control shutdown via ctx.
			Trap: func(c chan<- os.Signal) {},
		})
	}()

	// Wait until the server is accepting.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			c.Close()
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	cancel()
	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return after context cancellation")
	}
}

func TestRun_RejectsMissingAddr(t *testing.T) {
	gw := NewGateway(GatewayConfig{
		MetaService: newMockMetaService(),
		Creds:       NewCredentialStore(),
		ChunkStore:  NewMemoryChunkStore(),
	})
	if err := gw.Run(context.Background(), ServerConfig{}); err == nil {
		t.Fatal("expected error for empty Addr")
	}
}
