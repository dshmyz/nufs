package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/example/dfs/metadata"
)

type restoreReadinessConfig struct {
	MinimumReadableReplicas int
	Probe                   metadata.RestoreReplicaProbe
	PollInterval            time.Duration
}

type restoreReadinessGate struct {
	cancel context.CancelFunc
	done   chan struct{}
	once   sync.Once
}

func startRestoreReadinessGate(
	ctx context.Context,
	store *metadata.PebbleStore,
	bundle *metadata.ServiceBundle,
	cfg restoreReadinessConfig,
) (*restoreReadinessGate, error) {
	if cfg.MinimumReadableReplicas < 1 {
		return nil, fmt.Errorf("restore readiness: minimum readable replicas must be at least 1")
	}
	if store == nil {
		return nil, fmt.Errorf("restore readiness: store is required")
	}
	if bundle == nil {
		return nil, fmt.Errorf("restore readiness: service bundle is required")
	}
	if cfg.Probe == nil {
		return nil, fmt.Errorf("restore readiness: probe is required")
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 5 * time.Second
	}

	marker, err := store.GetRestorePendingMarker(ctx)
	if err != nil {
		return nil, err
	}
	gateCtx, cancel := context.WithCancel(ctx)
	gate := &restoreReadinessGate{cancel: cancel, done: make(chan struct{})}
	if marker == nil {
		close(gate.done)
		return gate, nil
	}

	bundle.SetRestoreReadinessPending(&metadata.RestoreReadinessReport{
		Ready:           false,
		MinimumReplicas: cfg.MinimumReadableReplicas,
	})
	go gate.run(gateCtx, store, bundle, cfg)
	return gate, nil
}

func (g *restoreReadinessGate) Stop() {
	g.once.Do(func() {
		if g.cancel != nil {
			g.cancel()
		}
		<-g.done
	})
}

func (g *restoreReadinessGate) run(
	ctx context.Context,
	store *metadata.PebbleStore,
	bundle *metadata.ServiceBundle,
	cfg restoreReadinessConfig,
) {
	defer close(g.done)
	ticker := time.NewTicker(cfg.PollInterval)
	defer ticker.Stop()
	for {
		started := time.Now()
		report, verifyErr := metadata.VerifyRestoredChunkAvailability(ctx, store, cfg.Probe, cfg.MinimumReadableReplicas)
		restoreVerificationDurationMillis.Store(time.Since(started).Milliseconds())
		if verifyErr != nil {
			restoreVerificationFailuresTotal.Add(1)
		}
		if report != nil {
			bundle.UpdateRestoreReadinessReport(report)
		}
		if verifyErr == nil && report != nil && report.Ready {
			if err := store.ClearRestorePendingMarker(ctx); err == nil {
				bundle.CompleteRestoreReadiness(report)
				return
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

type datanodeRestoreReplicaProbe struct {
	Client  *http.Client
	Timeout time.Duration
}

func (p datanodeRestoreReplicaProbe) ReachableReplicas(ctx context.Context, chunk *metadata.ChunkMeta) (int, error) {
	if chunk == nil {
		return 0, fmt.Errorf("restore readiness: chunk is required")
	}
	client := p.Client
	if client == nil {
		timeout := p.Timeout
		if timeout <= 0 {
			timeout = 2 * time.Second
		}
		client = &http.Client{Timeout: timeout}
	}
	reachable := 0
	for _, replica := range chunk.Replicas {
		if replica.State != metadata.ReplicaReady || strings.TrimSpace(replica.Addr) == "" {
			continue
		}
		ok, err := probeDatanodeReplica(ctx, client, replica.Addr, chunk.ID)
		if err != nil {
			if ctx.Err() != nil {
				return reachable, ctx.Err()
			}
			continue
		}
		if ok {
			reachable++
		}
	}
	return reachable, nil
}

func probeDatanodeReplica(ctx context.Context, client *http.Client, addr string, chunkID metadata.ChunkID) (bool, error) {
	endpoint, err := restoreReplicaVerifyURL(addr, chunkID)
	if err != nil {
		return false, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return false, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return false, nil
	}
	var body struct {
		Valid bool `json:"valid"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return false, nil
	}
	return body.Valid, nil
}

func restoreReplicaVerifyURL(addr string, chunkID metadata.ChunkID) (string, error) {
	raw := strings.TrimSpace(addr)
	if !strings.HasPrefix(raw, "http://") && !strings.HasPrefix(raw, "https://") {
		raw = "http://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	u.Path = path.Join(u.Path, "api", "v1", "chunks", fmt.Sprintf("%d", chunkID), "verify")
	return u.String(), nil
}
