package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/dshmyz/nufs/nufs-admin/internal/auth"
	"github.com/dshmyz/nufs/nufs-admin/internal/cache"
	"github.com/dshmyz/nufs/nufs-admin/internal/cluster"
	"github.com/dshmyz/nufs/nufs-admin/internal/config"
	"github.com/dshmyz/nufs/nufs-admin/internal/proxy"
)

func TestHandleWriteOpsStatusProxiesClusterStatus(t *testing.T) {
	metad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/write-ops/status" {
			t.Fatalf("unexpected metad path %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"attempts":{"failed":2},"recovery_task":{"state":"succeeded"},"gc_task":{"state":"queued"}}`))
	}))
	defer metad.Close()

	cfgPath := writeAdminTestConfig(t, metad.URL)
	cfgMgr, err := config.NewManager(cfgPath, nil)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	registry, err := cluster.NewRegistry(cfgMgr, nil)
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	defer registry.Close()
	c := cache.New(time.Second)
	defer c.Close()
	pr := proxy.NewProxy(registry, c)
	router := NewRouter(pr, proxy.NewAggregator(pr), auth.NewJWTManager("secret"), &auth.UserStore{}, registry)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/clusters/prod/write-ops/status", nil)
	rr := httptest.NewRecorder()
	router.handleClusterRoutes(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body.String())
	}
	var got struct {
		Cluster  string         `json:"cluster"`
		Attempts map[string]int `json:"attempts"`
	}
	if err := json.NewDecoder(rr.Body).Decode(&got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got.Cluster != "prod" {
		t.Fatalf("cluster = %q, want prod", got.Cluster)
	}
	if got.Attempts["failed"] != 2 {
		t.Fatalf("failed attempts = %d, want 2", got.Attempts["failed"])
	}
}

func writeAdminTestConfig(t *testing.T, metadURL string) string {
	t.Helper()

	path := t.TempDir() + "/config.yaml"
	data := []byte("clusters:\n  - name: prod\n    region: test\n    metad_ops_url: " + metadURL + "\n    description: test cluster\nserver:\n  jwt_secret: secret\n")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}
