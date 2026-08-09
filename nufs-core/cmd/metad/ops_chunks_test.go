package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"
	"unsafe"

	"github.com/dshmyz/nufs/nufs-core/metadata"
	"github.com/hashicorp/raft"
)

func TestChunkAllocationUnknownOutcomeIsConflictWithMachineCode(t *testing.T) {
	store, bundle := newOpsTestStore(t)
	ctx := context.Background()
	if err := store.RegisterNode(ctx, &metadata.NodeInfo{ID: 1, Addr: "node:9001", CapacityGB: 10, Tier: metadata.TierHot}); err != nil {
		t.Fatalf("RegisterNode: %v", err)
	}
	if err := store.CreateBucket(ctx, "unknown", metadata.PlacementPolicy{ReplicationFactor: 1}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	bucket, err := store.GetBucket(ctx, "unknown")
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}
	inode, err := store.CreateFile(ctx, bucket.RootInode, "file", 0644)
	if err != nil {
		t.Fatalf("CreateFile: %v", err)
	}
	node := &metadata.RaftNode{}
	setUnexportedField(t, node, "conditionalLeaderHook", func() bool { return true })
	setUnexportedField(t, node, "conditionalApplyHook", func([]byte, time.Duration) raft.ApplyFuture {
		return neverCompletingApplyFuture{}
	})
	store.SetRaftNode(node)
	t.Cleanup(func() { setUnexportedField(t, store, "raft", (*metadata.RaftNode)(nil)) })

	reqBody, _ := json.Marshal(map[string]interface{}{
		"inode_id": inode.ID,
		"offset":   0,
		"policy":   metadata.PlacementPolicy{ReplicationFactor: 1},
	})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/chunks", bytes.NewReader(reqBody))
	rr := httptest.NewRecorder()
	(&opsHandlers{store: store, dataStore: store, bundle: bundle}).handleChunks(rr, req)
	if rr.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409, body=%s", rr.Code, rr.Body.String())
	}
	var body map[string]string
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body["code"] != "allocation_outcome_unknown" {
		t.Fatalf("code = %q, want allocation_outcome_unknown", body["code"])
	}
}

type neverCompletingApplyFuture struct{}

func (neverCompletingApplyFuture) Error() error {
	time.Sleep(24 * time.Hour)
	return errors.New("unreachable")
}
func (neverCompletingApplyFuture) Response() interface{} { return nil }
func (neverCompletingApplyFuture) Index() uint64         { return 0 }

func setUnexportedField(t *testing.T, target interface{}, name string, value interface{}) {
	t.Helper()
	field := reflect.ValueOf(target).Elem().FieldByName(name)
	if !field.IsValid() {
		t.Fatalf("missing field %s", name)
	}
	reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem().Set(reflect.ValueOf(value))
}
