package metadata

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/hashicorp/raft"
)

func TestConditionalRaftLogCodecCanonicalRoundTrip(t *testing.T) {
	entry := &RaftLogEntry{
		Op: OpConditionalBatch,
		Conditional: &ConditionalBatch{
			Version: conditionalBatchVersion,
			Preconditions: []ConditionalPrecondition{
				{Key: []byte("z"), ExpectedValue: []byte("old-z")},
				{Key: []byte("a"), ExpectAbsent: true},
			},
			Mutations: []BatchOp{
				{Key: []byte("z"), Value: []byte("new-z")},
				{Delete: true, Key: []byte("a")},
			},
			PrefixReplacements: []ConditionalPrefixReplacement{{
				Prefix: []byte(prefixBackupCatalog),
				Sets: []BatchOp{
					{Key: []byte(prefixBackupCatalog + "z"), Value: []byte("z")},
					{Key: []byte(prefixBackupCatalog + "a"), Value: []byte("a")},
				},
			}},
		},
	}

	encoded, err := entry.EncodeChecked()
	if err != nil {
		t.Fatalf("encode conditional entry: %v", err)
	}
	decoded, err := DecodeRaftLogEntry(encoded)
	if err != nil {
		t.Fatalf("decode conditional entry: %v", err)
	}
	if decoded.Op != OpConditionalBatch || decoded.Conditional == nil {
		t.Fatalf("decoded entry = %#v", decoded)
	}

	reencoded, err := decoded.EncodeChecked()
	if err != nil {
		t.Fatalf("re-encode conditional entry: %v", err)
	}
	if !bytes.Equal(encoded, reencoded) {
		t.Fatal("conditional encoding is not canonical")
	}

	reordered := &RaftLogEntry{
		Op: OpConditionalBatch,
		Conditional: &ConditionalBatch{
			Version:       conditionalBatchVersion,
			Preconditions: []ConditionalPrecondition{entry.Conditional.Preconditions[1], entry.Conditional.Preconditions[0]},
			Mutations:     []BatchOp{entry.Conditional.Mutations[1], entry.Conditional.Mutations[0]},
			PrefixReplacements: []ConditionalPrefixReplacement{{
				Prefix: []byte(prefixBackupCatalog),
				Sets: []BatchOp{
					entry.Conditional.PrefixReplacements[0].Sets[1],
					entry.Conditional.PrefixReplacements[0].Sets[0],
				},
			}},
		},
	}
	reorderedEncoded, err := reordered.EncodeChecked()
	if err != nil {
		t.Fatalf("encode reordered conditional entry: %v", err)
	}
	if !bytes.Equal(encoded, reorderedEncoded) {
		t.Fatal("equivalent conditional batches encoded differently")
	}
}

func TestConditionalRaftLogCodecRoundTripsExpectedRaftTerm(t *testing.T) {
	entry := &RaftLogEntry{
		Op:          OpConditionalBatch,
		Conditional: validTermFencedCreatingTaskBatch(t, "backup-fenced", 7),
	}

	encoded, err := entry.EncodeChecked()
	if err != nil {
		t.Fatalf("encode term-fenced conditional entry: %v", err)
	}
	decoded, err := DecodeRaftLogEntry(encoded)
	if err != nil {
		t.Fatalf("decode term-fenced conditional entry: %v", err)
	}
	if decoded.Conditional.ExpectedRaftTerm != 7 {
		t.Fatalf("expected Raft term = %d, want 7", decoded.Conditional.ExpectedRaftTerm)
	}
}

func TestConditionalRaftLogCodecRejectsMalformedAndUnsafeInputs(t *testing.T) {
	valid := &RaftLogEntry{
		Op: OpConditionalBatch,
		Conditional: &ConditionalBatch{
			Version: conditionalBatchVersion,
			Preconditions: []ConditionalPrecondition{{
				Key:          []byte("key"),
				ExpectAbsent: true,
			}},
			Mutations: []BatchOp{{
				Key:   []byte("key"),
				Value: []byte("value"),
			}},
		},
	}
	encoded, err := valid.EncodeChecked()
	if err != nil {
		t.Fatalf("encode valid entry: %v", err)
	}

	tooMany := []byte{byte(OpConditionalBatch), conditionalBatchVersion}
	var count [4]byte
	binary.BigEndian.PutUint32(count[:], maxConditionalPreconditions+1)
	tooMany = append(tooMany, count[:]...)

	decodeCases := map[string][]byte{
		"trailing bytes":          append(append([]byte(nil), encoded...), 0xff),
		"oversized log":           make([]byte, maxRaftLogEntryBytes+1),
		"excessive preconditions": tooMany,
		"truncated":               encoded[:len(encoded)-1],
	}
	for name, data := range decodeCases {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeRaftLogEntry(data); err == nil {
				t.Fatal("DecodeRaftLogEntry unexpectedly succeeded")
			}
		})
	}

	encodeCases := map[string]*ConditionalBatch{
		"unsupported version": {
			Version: conditionalBatchTermFencedVersion + 1,
		},
		"v1 with expected term": {
			Version:          conditionalBatchVersion,
			ExpectedRaftTerm: 7,
		},
		"term-fenced without expected term": {
			Version: conditionalBatchTermFencedVersion,
		},
		"duplicate precondition": {
			Version: conditionalBatchVersion,
			Preconditions: []ConditionalPrecondition{
				{Key: []byte("same"), ExpectAbsent: true},
				{Key: []byte("same"), ExpectedValue: []byte("value")},
			},
		},
		"absence with expected value": {
			Version: conditionalBatchVersion,
			Preconditions: []ConditionalPrecondition{{
				Key:           []byte("same"),
				ExpectedValue: []byte("value"),
				ExpectAbsent:  true,
			}},
		},
		"conflicting mutation": {
			Version: conditionalBatchVersion,
			Mutations: []BatchOp{
				{Key: []byte("same"), Value: []byte("one")},
				{Delete: true, Key: []byte("same")},
			},
		},
		"unsafe empty prefix": {
			Version: conditionalBatchVersion,
			PrefixReplacements: []ConditionalPrefixReplacement{{
				Prefix: []byte{},
			}},
		},
		"replacement outside prefix": {
			Version: conditionalBatchVersion,
			PrefixReplacements: []ConditionalPrefixReplacement{{
				Prefix: []byte("safe/"),
				Sets: []BatchOp{{
					Key:   []byte("other/key"),
					Value: []byte("value"),
				}},
			}},
		},
		"overlapping replacements": {
			Version: conditionalBatchVersion,
			PrefixReplacements: []ConditionalPrefixReplacement{
				{Prefix: []byte("safe/")},
				{Prefix: []byte("safe/nested/")},
			},
		},
		"broad root prefix": {
			Version: conditionalBatchVersion,
			PrefixReplacements: []ConditionalPrefixReplacement{{
				Prefix: []byte("/"),
				Sets:   []BatchOp{{Key: []byte("/replacement"), Value: []byte("value")}},
			}},
		},
		"broad backup prefix": {
			Version: conditionalBatchVersion,
			PrefixReplacements: []ConditionalPrefixReplacement{{
				Prefix: []byte("backup/"),
				Sets:   []BatchOp{{Key: []byte("backup/replacement"), Value: []byte("value")}},
			}},
		},
		"catalog super-prefix": {
			Version: conditionalBatchVersion,
			PrefixReplacements: []ConditionalPrefixReplacement{{
				Prefix: []byte("backup/catalog"),
				Sets:   []BatchOp{{Key: []byte("backup/catalog/replacement"), Value: []byte("value")}},
			}},
		},
		"catalog sub-prefix": {
			Version: conditionalBatchVersion,
			PrefixReplacements: []ConditionalPrefixReplacement{{
				Prefix: []byte(prefixBackupCatalog + "nested/"),
				Sets:   []BatchOp{{Key: []byte(prefixBackupCatalog + "nested/replacement"), Value: []byte("value")}},
			}},
		},
		"unregistered application prefix": {
			Version: conditionalBatchVersion,
			PrefixReplacements: []ConditionalPrefixReplacement{{
				Prefix: []byte("inode/"),
				Sets:   []BatchOp{{Key: []byte("inode/replacement"), Value: []byte("value")}},
			}},
		},
		"oversized key": {
			Version: conditionalBatchVersion,
			Mutations: []BatchOp{{
				Key:   bytes.Repeat([]byte{'k'}, maxConditionalKeyBytes+1),
				Value: []byte("value"),
			}},
		},
	}
	for name, conditional := range encodeCases {
		t.Run(name, func(t *testing.T) {
			entry := &RaftLogEntry{Op: OpConditionalBatch, Conditional: conditional}
			if _, err := entry.EncodeChecked(); err == nil {
				t.Fatal("EncodeChecked unexpectedly succeeded")
			}
		})
	}
}

func TestApplyConditionalRejectsCallerBuiltTermFencedPayloadsWithoutSideEffects(t *testing.T) {
	cases := map[string]*ConditionalBatch{
		"arbitrary key": {
			Version:          conditionalBatchTermFencedVersion,
			ExpectedRaftTerm: 7,
			Preconditions: []ConditionalPrecondition{{
				Key:          []byte("/arbitrary/key"),
				ExpectAbsent: true,
			}},
			Mutations: []BatchOp{{
				Key:   []byte("/arbitrary/key"),
				Value: []byte("injected"),
			}},
		},
		"cluster identity": {
			Version:          conditionalBatchTermFencedVersion,
			ExpectedRaftTerm: 7,
			Preconditions: []ConditionalPrecondition{{
				Key:          []byte(keyClusterID),
				ExpectAbsent: true,
			}},
			Mutations: []BatchOp{{
				Key:   []byte(keyClusterID),
				Value: []byte("injected"),
			}},
		},
		"restore marker": {
			Version:          conditionalBatchTermFencedVersion,
			ExpectedRaftTerm: 7,
			Preconditions: []ConditionalPrecondition{{
				Key:          []byte(keyRestorePending),
				ExpectAbsent: true,
			}},
			Mutations: []BatchOp{{
				Key:   []byte(keyRestorePending),
				Value: []byte("injected"),
			}},
		},
		"malformed backup task": {
			Version:          conditionalBatchTermFencedVersion,
			ExpectedRaftTerm: 7,
			Preconditions: []ConditionalPrecondition{{
				Key:          []byte(prefixBackupTask + "backup-injected"),
				ExpectAbsent: true,
			}},
			Mutations: []BatchOp{{
				Key:   []byte(prefixBackupTask + "backup-injected"),
				Value: []byte("not-a-backup-task"),
			}},
		},
		"malformed backup catalog": {
			Version:          conditionalBatchTermFencedVersion,
			ExpectedRaftTerm: 7,
			Mutations: []BatchOp{{
				Key:   []byte(keyBackupCatalog),
				Value: []byte("not-a-backup-catalog"),
			}},
			PrefixReplacements: []ConditionalPrefixReplacement{{
				Prefix: []byte(prefixBackupCatalog),
				Sets: []BatchOp{{
					Key:   []byte(prefixBackupCatalog + "backup-injected"),
					Value: []byte("not-a-committed-backup"),
				}},
			}},
		},
		"mixed task and arbitrary mutation": {
			Version:          conditionalBatchTermFencedVersion,
			ExpectedRaftTerm: 7,
			Preconditions: []ConditionalPrecondition{{
				Key:          []byte(prefixBackupTask + "backup-injected"),
				ExpectAbsent: true,
			}},
			Mutations: []BatchOp{
				{
					Key:   []byte(prefixBackupTask + "backup-injected"),
					Value: []byte("not-a-backup-task"),
				},
				{
					Key:   []byte("/arbitrary/mixed"),
					Value: []byte("injected"),
				},
			},
		},
	}

	for name, conditional := range cases {
		t.Run(name, func(t *testing.T) {
			store := newTestPebbleStore(t)
			defer store.Close()
			fsm := &PebbleFSM{store: store}
			var applyCalls atomic.Int32
			node := &RaftNode{
				conditionalLeaderHook: func() bool { return true },
				conditionalApplyHook: func(data []byte, _ time.Duration) raft.ApplyFuture {
					applyCalls.Add(1)
					future := newControlledConditionalFuture()
					future.Resolve(nil, fsm.Apply(&raft.Log{Index: 1, Term: 7, Data: data}))
					return future
				},
			}

			entry := &RaftLogEntry{
				Op:          OpConditionalBatch,
				Conditional: conditional,
			}
			if _, err := entry.EncodeChecked(); err == nil {
				t.Fatal("caller-built term-fenced payload unexpectedly encoded")
			}
			err := node.applyConditional(context.Background(), entry)
			if err == nil {
				t.Fatal("caller-built term-fenced payload unexpectedly succeeded")
			}
			if got := applyCalls.Load(); got != 0 {
				t.Fatalf("Raft apply calls = %d, want zero", got)
			}
			for _, key := range []string{
				"/arbitrary/key",
				"/arbitrary/mixed",
				keyClusterID,
				keyRestorePending,
				prefixBackupTask + "backup-injected",
				keyBackupCatalog,
				prefixBackupCatalog + "backup-injected",
			} {
				assertRawMissing(t, store, key)
			}
		})
	}
}

func TestChunkTombstoneConditionalRejectsCallerBuiltPayloads(t *testing.T) {
	store := newTestPebbleStore(t)
	defer store.Close()
	entry := &RaftLogEntry{
		Op: OpConditionalBatch,
		Conditional: &ConditionalBatch{
			Version: chunkTombstoneConditionalBatchVersion,
			Preconditions: []ConditionalPrecondition{{
				Key:          []byte("/arbitrary/key"),
				ExpectAbsent: true,
			}},
			Mutations: []BatchOp{{Key: []byte("/arbitrary/key"), Value: []byte("injected")}},
		},
	}
	if _, err := entry.EncodeChecked(); err == nil {
		t.Fatal("caller-built tombstone payload unexpectedly encoded")
	}
	assertRawMissing(t, store, "/arbitrary/key")
}

func TestPublicRaftSubmissionRejectsProtectedChunkOperationsWithoutWrites(t *testing.T) {
	store, node := newCheckpointRaftNode(t, true)
	defer node.Shutdown()
	if leader := waitForLeader([]*RaftNode{node}, 5*time.Second); leader == nil {
		t.Fatal("node did not become leader")
	}
	chunkID := ChunkID(9915)
	chunkKey := []byte(chunkMetadataKey(chunkID))
	tombstoneKey := []byte(chunkTombstoneKey(chunkID))
	chunkRaw, err := marshalValue(&ChunkMeta{ID: chunkID, Size: 64, State: ChunkReady}, codecMsgpack)
	if err != nil {
		t.Fatalf("encode chunk: %v", err)
	}
	entries := []*RaftLogEntry{
		{Op: OpSet, Key: chunkKey, Value: chunkRaw},
		{Op: OpDelete, Key: tombstoneKey},
		{Op: OpBatch, Batch: []BatchOp{{Key: chunkKey, Value: chunkRaw}}},
		{Op: OpCAS, Key: chunkKey, Value: []byte("invalid")},
		{Op: OpConditionalBatch, Conditional: &ConditionalBatch{Version: conditionalBatchVersion}},
	}
	for _, entry := range entries {
		if err := node.Apply(entry, time.Second); err == nil {
			t.Fatalf("public Apply accepted %#v", entry.Op)
		}
		if err := node.ApplyAutoForward(entry, time.Second); err == nil {
			t.Fatalf("public ApplyAutoForward accepted %#v", entry.Op)
		}
	}
	assertRawMissing(t, store, string(chunkKey))
	assertRawMissing(t, store, string(tombstoneKey))
}

func TestLegacyV3ChunkTombstoneReplayRemainsAccepted(t *testing.T) {
	store := newTestPebbleStore(t)
	defer store.Close()
	chunkID := ChunkID(9914)
	const legacyV3FixtureHex = "050300000002000000000b2f6368756e6b2f393931340000005b88a24944cf00000000000026baa453697a65d200000040a55374617465cc02a85265706c69636173c0a7454347726f7570c0a454696572cc00aa43726561746554696d65d30000000000000000a8436865636b73756dce0000000001000000146368756e6b2d746f6d6273746f6e652f393931340000000100000000146368756e6b2d746f6d6273746f6e652f393931340000005d86a86368756e6b5f6964cf00000000000026baa87265706c69636173c0a473697a65d30000000000000040a6726561736f6ea66c6567616379aa64656c657465645f6174d6ff6a6b3cc0ac64656c6574655f6166746572d6ff6a6c9c5000000000"
	legacy, err := hex.DecodeString(legacyV3FixtureHex)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeRaftLogEntry(legacy)
	if err != nil {
		t.Fatalf("decode legacy v3: %v", err)
	}
	if decoded.Conditional.Version != chunkTombstoneConditionalBatchVersion {
		t.Fatalf("decoded version = %d, want v3", decoded.Conditional.Version)
	}
	chunkRaw := decoded.Conditional.Preconditions[0].ExpectedValue
	tombstoneRaw := decoded.Conditional.Mutations[0].Value
	if err := store.db.Set([]byte(chunkMetadataKey(chunkID)), chunkRaw, pebble.Sync); err != nil {
		t.Fatalf("seed fixture chunk: %v", err)
	}
	reencoded, err := decoded.EncodeChecked()
	if err != nil {
		t.Fatalf("re-encode legacy v3: %v", err)
	}
	if !bytes.Equal(reencoded, legacy) {
		t.Fatal("legacy v3 changed during decode/re-encode")
	}
	fsm := &PebbleFSM{store: store}
	if response := fsm.Apply(&raft.Log{Index: 802, Term: 4, Data: legacy}); response != nil {
		t.Fatalf("legacy v3 replay: %#v", response)
	}
	assertRawValue(t, store, chunkTombstoneKey(chunkID), tombstoneRaw)
}

func TestFSMChunkTombstoneV4AcceptsOnlyFencedCreateShape(t *testing.T) {
	store := newTestPebbleStore(t)
	defer store.Close()
	chunkID := ChunkID(9901)
	chunk := &ChunkMeta{ID: chunkID, Size: 64, State: ChunkReady}
	chunkRaw, err := marshalValue(chunk, codecMsgpack)
	if err != nil {
		t.Fatalf("encode chunk: %v", err)
	}
	if err := store.db.Set([]byte(chunkMetadataKey(chunkID)), chunkRaw, pebble.Sync); err != nil {
		t.Fatalf("seed chunk: %v", err)
	}
	epochRaw, epochFound, err := store.readChunkTombstoneRaw(keyInodeReferenceEpoch)
	if err != nil {
		t.Fatalf("read inode reference epoch: %v", err)
	}
	epochPrecondition := ConditionalPrecondition{Key: []byte(keyInodeReferenceEpoch), ExpectedValue: epochRaw}
	if !epochFound {
		epochPrecondition = ConditionalPrecondition{Key: []byte(keyInodeReferenceEpoch), ExpectAbsent: true}
	}
	deletedAt := time.Now().UTC().Round(0)
	tombstone := ChunkTombstone{ChunkID: chunkID, Size: int64(chunk.Size), Reason: "test", DeletedAt: deletedAt, DeleteAfter: deletedAt.Add(chunkTombstoneQuarantine)}
	tombstoneRaw, err := marshalValue(&tombstone, codecMsgpack)
	if err != nil {
		t.Fatalf("encode tombstone: %v", err)
	}
	entry := &RaftLogEntry{Op: OpConditionalBatch, Conditional: &ConditionalBatch{
		Version: chunkTombstoneFencedBatchVersion,
		Preconditions: []ConditionalPrecondition{
			epochPrecondition,
			{Key: []byte(chunkMetadataKey(chunkID)), ExpectedValue: chunkRaw},
			{Key: []byte(chunkTombstoneKey(chunkID)), ExpectAbsent: true},
		},
		Mutations: []BatchOp{{Key: []byte(chunkTombstoneKey(chunkID)), Value: tombstoneRaw}},
	}}
	data, err := entry.EncodeChecked()
	if err != nil {
		t.Fatalf("encode fenced create: %v", err)
	}
	fsm := &PebbleFSM{store: store}
	if response := fsm.Apply(&raft.Log{Index: 91, Term: 7, Data: data}); response != nil {
		t.Fatalf("fsm v4 create response = %#v", response)
	}
	assertRawValue(t, store, chunkTombstoneKey(chunkID), tombstoneRaw)

	entry.Conditional.Preconditions = entry.Conditional.Preconditions[1:]
	if _, err := entry.EncodeChecked(); err == nil {
		t.Fatal("unfenced v4 create encoded successfully")
	}
}

func TestV4AllocationAllows1024ChunksDecodeAndFSMApply(t *testing.T) {
	build := func(count int) *ConditionalBatch {
		previous := &InodeMeta{ID: 8801}
		previousRaw, _ := marshalValue(previous, codecMsgpack)
		next := &InodeMeta{ID: 8801, ChunkMap: make([]ChunkRef, count)}
		preconditions := []ConditionalPrecondition{
			{Key: []byte("/inode/8801"), ExpectedValue: previousRaw},
			{Key: []byte(keyInodeReferenceEpoch), ExpectAbsent: true},
		}
		mutations := make([]BatchOp, 0, count+2)
		for i := 0; i < count; i++ {
			id := ChunkID(900000 + i)
			next.ChunkMap[i] = ChunkRef{ID: id, Offset: int64(i)}
			chunkRaw, _ := marshalValue(&ChunkMeta{ID: id, Size: 1, State: ChunkCreated}, codecMsgpack)
			preconditions = append(preconditions, ConditionalPrecondition{Key: []byte(chunkMetadataKey(id)), ExpectAbsent: true}, ConditionalPrecondition{Key: []byte(chunkTombstoneKey(id)), ExpectAbsent: true})
			mutations = append(mutations, BatchOp{Key: []byte(chunkMetadataKey(id)), Value: chunkRaw})
		}
		nextRaw, _ := marshalValue(next, codecMsgpack)
		mutations = append(mutations, BatchOp{Key: []byte("/inode/8801"), Value: nextRaw}, BatchOp{Key: []byte(keyInodeReferenceEpoch), Value: encodeInodeReferenceEpoch(1)})
		return &ConditionalBatch{Version: chunkTombstoneFencedBatchVersion, Preconditions: preconditions, Mutations: mutations}
	}
	data, err := (&RaftLogEntry{Op: OpConditionalBatch, Conditional: build(MaxChunkAllocationBatch)}).EncodeChecked()
	if err != nil || len(data) > maxRaftLogEntryBytes {
		t.Fatalf("1024 allocation encode = (%d, %v)", len(data), err)
	}
	decoded, err := DecodeRaftLogEntry(data)
	if err != nil {
		t.Fatalf("1024 allocation decode: %v", err)
	}
	db, err := pebble.Open(t.TempDir(), &pebble.Options{})
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	defer db.Close()
	previousRaw, _ := marshalValue(&InodeMeta{ID: 8801}, codecMsgpack)
	if err := db.Set([]byte("/inode/8801"), previousRaw, pebble.NoSync); err != nil {
		t.Fatalf("seed previous inode: %v", err)
	}
	if err := applyConditionalBatch(db, decoded.Conditional, pebble.NoSync); err != nil {
		t.Fatalf("1024 allocation FSM apply: %v", err)
	}
	raw, closer, err := db.Get([]byte("/inode/8801"))
	if err != nil {
		t.Fatalf("read applied inode: %v", err)
	}
	var applied InodeMeta
	if err := unmarshalValue(append([]byte(nil), raw...), &applied); err != nil {
		closer.Close()
		t.Fatalf("decode applied inode: %v", err)
	}
	closer.Close()
	if len(applied.ChunkMap) != MaxChunkAllocationBatch {
		t.Fatalf("applied chunk refs = %d, want %d", len(applied.ChunkMap), MaxChunkAllocationBatch)
	}
	if _, closer, err := db.Get([]byte(chunkMetadataKey(ChunkID(900000 + MaxChunkAllocationBatch - 1)))); err != nil {
		t.Fatalf("read last applied chunk: %v", err)
	} else {
		closer.Close()
	}
	if _, err := (&RaftLogEntry{Op: OpConditionalBatch, Conditional: build(MaxChunkAllocationBatch + 1)}).EncodeChecked(); err == nil {
		t.Fatal("oversized v4 allocation encoded")
	}
}

func TestConditionalPrefixReplacementRejectsUnregisteredDirectAndFSMApply(t *testing.T) {
	store := newTestPebbleStore(t)
	defer store.Close()
	for key, value := range map[string]string{
		"backup/task/survivor": "task",
		"backup/catalog/keep":  "catalog",
		"inode/survivor":       "inode",
	} {
		if err := store.db.Set([]byte(key), []byte(value), pebble.Sync); err != nil {
			t.Fatalf("seed %q: %v", key, err)
		}
	}

	for _, prefix := range []string{"/", "backup/", "backup/catalog", prefixBackupCatalog + "nested/", "inode/"} {
		t.Run("direct_"+strings.ReplaceAll(prefix, "/", "_"), func(t *testing.T) {
			err := applyConditionalBatch(store.db, &ConditionalBatch{
				Version: conditionalBatchVersion,
				PrefixReplacements: []ConditionalPrefixReplacement{{
					Prefix: []byte(prefix),
				}},
			}, pebble.Sync)
			if err == nil {
				t.Fatalf("direct apply accepted prefix %q", prefix)
			}
		})
	}
	assertRawValue(t, store, "backup/task/survivor", []byte("task"))
	assertRawValue(t, store, "backup/catalog/keep", []byte("catalog"))
	assertRawValue(t, store, "inode/survivor", []byte("inode"))

	fsm := &PebbleFSM{store: store}
	raw := encodeUncheckedConditionalPrefix([]byte("backup/"))
	response := fsm.Apply(&raft.Log{Index: 77, Term: 9, Data: raw})
	if response == nil {
		t.Fatal("FSM accepted broad unregistered prefix")
	}
	assertRawValue(t, store, "backup/task/survivor", []byte("task"))
	assertRawValue(t, store, "backup/catalog/keep", []byte("catalog"))
	if fsm.lastAppliedIndex != 0 || fsm.lastAppliedTerm != 0 {
		t.Fatalf("FSM position = %d/%d, want retained zeroes", fsm.lastAppliedIndex, fsm.lastAppliedTerm)
	}
}

func TestLegacyRaftBatchCodecPreservesLargeValidBatch(t *testing.T) {
	entry := &RaftLogEntry{
		Op:    OpBatch,
		Batch: make([]BatchOp, maxConditionalMutations+1),
	}
	for i := range entry.Batch {
		entry.Batch[i] = BatchOp{
			Delete: true,
			Key:    []byte{byte(i >> 8), byte(i)},
		}
	}
	encoded := entry.Encode()
	decoded, err := DecodeRaftLogEntry(encoded)
	if err != nil {
		t.Fatalf("decode legacy batch: %v", err)
	}
	if len(decoded.Batch) != len(entry.Batch) {
		t.Fatalf("decoded %d operations, want %d", len(decoded.Batch), len(entry.Batch))
	}
}

func TestRaftLogOpWireValuesRemainStable(t *testing.T) {
	got := []RaftLogOp{OpSet, OpDelete, OpBatch, OpCAS, OpConditionalBatch}
	want := []RaftLogOp{0x01, 0x02, 0x03, 0x04, 0x05}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("op %d wire value = 0x%02x, want 0x%02x", i, got[i], want[i])
		}
	}
}

func TestFSMConditionalBatchConflictHasNoPartialMutationOrCheckpointAdvance(t *testing.T) {
	store := newTestPebbleStore(t)
	defer store.Close()
	if err := store.db.Set([]byte("guard"), []byte("current"), nil); err != nil {
		t.Fatalf("seed guard: %v", err)
	}
	if err := store.db.Set([]byte(prefixBackupCatalog+"stale"), []byte("stale"), nil); err != nil {
		t.Fatalf("seed catalog: %v", err)
	}
	fsm := &PebbleFSM{store: store}
	entry := &RaftLogEntry{
		Op: OpConditionalBatch,
		Conditional: &ConditionalBatch{
			Version: conditionalBatchVersion,
			Preconditions: []ConditionalPrecondition{{
				Key:           []byte("guard"),
				ExpectedValue: []byte("wrong"),
			}},
			Mutations: []BatchOp{
				{Key: []byte("new"), Value: []byte("value")},
				{Delete: true, Key: []byte("guard")},
			},
			PrefixReplacements: []ConditionalPrefixReplacement{{
				Prefix: []byte(prefixBackupCatalog),
				Sets: []BatchOp{{
					Key:   []byte(prefixBackupCatalog + "new"),
					Value: []byte("new"),
				}},
			}},
		},
	}
	data, err := entry.EncodeChecked()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	response := fsm.Apply(&raft.Log{Index: 41, Term: 7, Data: data})
	responseErr, ok := response.(error)
	if !ok || !errors.Is(responseErr, ErrRaftConditionalConflict) {
		t.Fatalf("response = %#v, want conditional conflict", response)
	}
	assertRawValue(t, store, "guard", []byte("current"))
	assertRawMissing(t, store, "new")
	assertRawValue(t, store, prefixBackupCatalog+"stale", []byte("stale"))
	assertRawMissing(t, store, prefixBackupCatalog+"new")
	if fsm.lastAppliedIndex != 0 || fsm.lastAppliedTerm != 0 {
		t.Fatalf("FSM position = %d/%d, want retained zeroes", fsm.lastAppliedIndex, fsm.lastAppliedTerm)
	}
}

func TestFSMConditionalBatchExpectedAbsenceAndPrefixReplacement(t *testing.T) {
	store := newTestPebbleStore(t)
	defer store.Close()
	for key, value := range map[string]string{
		prefixBackupCatalog + "stale-a": "a",
		prefixBackupCatalog + "stale-b": "b",
		"outside":                       "keep",
	} {
		if err := store.db.Set([]byte(key), []byte(value), nil); err != nil {
			t.Fatalf("seed %s: %v", key, err)
		}
	}
	fsm := &PebbleFSM{store: store}
	entry := &RaftLogEntry{
		Op: OpConditionalBatch,
		Conditional: &ConditionalBatch{
			Version: conditionalBatchVersion,
			Preconditions: []ConditionalPrecondition{{
				Key:          []byte("created-once"),
				ExpectAbsent: true,
			}},
			Mutations: []BatchOp{{
				Key:   []byte("created-once"),
				Value: []byte("created"),
			}},
			PrefixReplacements: []ConditionalPrefixReplacement{{
				Prefix: []byte(prefixBackupCatalog),
				Sets: []BatchOp{
					{Key: []byte(prefixBackupCatalog + "new-a"), Value: []byte("new-a")},
					{Key: []byte(prefixBackupCatalog + "new-b"), Value: []byte("new-b")},
				},
			}},
		},
	}
	data, err := entry.EncodeChecked()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if response := fsm.Apply(&raft.Log{Index: 9, Term: 3, Data: data}); response != nil {
		t.Fatalf("response = %#v", response)
	}

	assertRawValue(t, store, "created-once", []byte("created"))
	assertRawMissing(t, store, prefixBackupCatalog+"stale-a")
	assertRawMissing(t, store, prefixBackupCatalog+"stale-b")
	assertRawValue(t, store, prefixBackupCatalog+"new-a", []byte("new-a"))
	assertRawValue(t, store, prefixBackupCatalog+"new-b", []byte("new-b"))
	assertRawValue(t, store, "outside", []byte("keep"))
}

func TestFSMConditionalBacklogPreservesEarlierTerminalAndClusterIdentity(t *testing.T) {
	store := newTestPebbleStore(t)
	defer store.Close()
	fsm := &PebbleFSM{store: store}

	terminal := validBackupTask("backup-backlog", testBackupTime())
	terminal.State = BackupTaskFailed
	terminal.CompletedAt = terminal.StartedAt.Add(2)
	terminal.UpdatedAt = terminal.CompletedAt
	terminal.LastError = "old leader failure"
	terminalBytes, err := marshalValue(&terminal, codecMsgpack)
	if err != nil {
		t.Fatalf("marshal terminal task: %v", err)
	}
	oldTaskLog := (&RaftLogEntry{
		Op:    OpSet,
		Key:   []byte(prefixBackupTask + terminal.ID),
		Value: terminalBytes,
	}).Encode()
	if response := fsm.Apply(&raft.Log{Index: 100, Term: 8, Data: oldTaskLog}); response != nil {
		t.Fatalf("apply old terminal task: %#v", response)
	}

	staleTask := terminal
	staleTask.State = BackupTaskVerifying
	staleTask.CompletedAt = timeZero()
	staleTask.LastError = ""
	staleTaskBytes, err := marshalValue(&staleTask, codecMsgpack)
	if err != nil {
		t.Fatalf("marshal stale task: %v", err)
	}
	staleTaskLog := mustEncodeConditional(t, &ConditionalBatch{
		Version: conditionalBatchVersion,
		Preconditions: []ConditionalPrecondition{{
			Key:          []byte(prefixBackupTask + terminal.ID),
			ExpectAbsent: true,
		}},
		Mutations: []BatchOp{{
			Key:   []byte(prefixBackupTask + terminal.ID),
			Value: staleTaskBytes,
		}},
	})
	if response := fsm.Apply(&raft.Log{Index: 101, Term: 9, Data: staleTaskLog}); !errors.Is(response.(error), ErrRaftConditionalConflict) {
		t.Fatalf("stale task response = %#v", response)
	}

	clusterBytes, err := marshalValue("cluster-old", codecMsgpack)
	if err != nil {
		t.Fatalf("marshal cluster ID: %v", err)
	}
	oldClusterLog := (&RaftLogEntry{Op: OpSet, Key: []byte(keyClusterID), Value: clusterBytes}).Encode()
	if response := fsm.Apply(&raft.Log{Index: 102, Term: 9, Data: oldClusterLog}); response != nil {
		t.Fatalf("apply old cluster ID: %#v", response)
	}
	newClusterBytes, err := marshalValue("cluster-new", codecMsgpack)
	if err != nil {
		t.Fatalf("marshal new cluster ID: %v", err)
	}
	staleClusterLog := mustEncodeConditional(t, &ConditionalBatch{
		Version: conditionalBatchVersion,
		Preconditions: []ConditionalPrecondition{{
			Key:          []byte(keyClusterID),
			ExpectAbsent: true,
		}},
		Mutations: []BatchOp{{
			Key:   []byte(keyClusterID),
			Value: newClusterBytes,
		}},
	})
	if response := fsm.Apply(&raft.Log{Index: 103, Term: 9, Data: staleClusterLog}); !errors.Is(response.(error), ErrRaftConditionalConflict) {
		t.Fatalf("stale cluster response = %#v", response)
	}

	var durableTask BackupTask
	found, err := store.getValue(prefixBackupTask+terminal.ID, &durableTask)
	if err != nil || !found {
		t.Fatalf("read durable task: found=%v err=%v", found, err)
	}
	if !terminalBackupTasksEqual(durableTask, terminal) {
		t.Fatalf("durable task = %#v, want terminal %#v", durableTask, terminal)
	}
	var durableCluster string
	found, err = store.getValue(keyClusterID, &durableCluster)
	if err != nil || !found || durableCluster != "cluster-old" {
		t.Fatalf("durable cluster = %q found=%v err=%v", durableCluster, found, err)
	}
}

func TestFSMConditionalExpectedRaftTermMismatchDoesNotWrite(t *testing.T) {
	store := newTestPebbleStore(t)
	defer store.Close()
	fsm := &PebbleFSM{store: store}
	key := prefixBackupTask + "backup-stale-generation"
	logData := mustEncodeConditional(
		t,
		validTermFencedCreatingTaskBatch(t, "backup-stale-generation", 7),
	)

	response := fsm.Apply(&raft.Log{Index: 100, Term: 8, Data: logData})
	responseErr, ok := response.(error)
	if !ok || !errors.Is(responseErr, ErrRaftConditionalConflict) {
		t.Fatalf("FSM response = %#v, want term-fence conflict", response)
	}
	assertRawMissing(t, store, key)
}

func TestGenericRaftApplyRejectsConditionalWithoutRaftOrHTTP(t *testing.T) {
	valid := &RaftLogEntry{
		Op: OpConditionalBatch,
		Conditional: &ConditionalBatch{
			Version: conditionalBatchVersion,
		},
	}
	invalid := &RaftLogEntry{Op: OpConditionalBatch}
	node := &RaftNode{}

	for name, call := range map[string]func() error{
		"apply valid":          func() error { return node.Apply(valid, time.Second) },
		"apply invalid":        func() error { return node.Apply(invalid, time.Second) },
		"auto-forward valid":   func() error { return node.ApplyAutoForward(valid, time.Second) },
		"auto-forward invalid": func() error { return node.ApplyAutoForward(invalid, time.Second) },
	} {
		t.Run(name, func(t *testing.T) {
			err := call()
			if err == nil || !strings.Contains(err.Error(), "restricted metadata submitter") {
				t.Fatalf("error = %v, want restricted submitter rejection", err)
			}
		})
	}
}

func TestApplyConditionalPropagatesFSMConflict(t *testing.T) {
	future := newControlledConditionalFuture()
	future.Resolve(nil, ErrRaftConditionalConflict)
	node := &RaftNode{
		conditionalLeaderHook: func() bool { return true },
		conditionalApplyHook: func([]byte, time.Duration) raft.ApplyFuture {
			return future
		},
	}
	err := node.applyConditional(context.Background(), &RaftLogEntry{
		Op: OpConditionalBatch,
		Conditional: &ConditionalBatch{
			Version: conditionalBatchVersion,
		},
	})
	if !errors.Is(err, ErrRaftConditionalConflict) {
		t.Fatalf("ApplyConditional error = %v, want FSM conflict", err)
	}
}

func TestApplyConditionalWaitersAreBoundedAndCancellationDoesNotLateSubmit(t *testing.T) {
	var applyCalls atomic.Int32
	accepted := make(chan *controlledConditionalFuture, conditionalFutureWaiterCapacity+2)
	node := &RaftNode{
		conditionalLeaderHook: func() bool { return true },
		conditionalApplyHook: func([]byte, time.Duration) raft.ApplyFuture {
			applyCalls.Add(1)
			future := newControlledConditionalFuture()
			accepted <- future
			return future
		},
	}
	entry := &RaftLogEntry{
		Op: OpConditionalBatch,
		Conditional: &ConditionalBatch{
			Version: conditionalBatchVersion,
		},
	}

	preCanceled, cancelPreCanceled := context.WithCancel(context.Background())
	cancelPreCanceled()
	if err := node.applyConditional(preCanceled, entry); !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-canceled ApplyConditional error = %v", err)
	}
	if got := applyCalls.Load(); got != 0 {
		t.Fatalf("pre-canceled call invoked raft.Apply: calls=%d", got)
	}

	futures := make([]*controlledConditionalFuture, 0, conditionalFutureWaiterCapacity)
	for i := 0; i < conditionalFutureWaiterCapacity; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		result := make(chan error, 1)
		go func() {
			result <- node.applyConditional(ctx, entry)
		}()
		future := <-accepted
		futures = append(futures, future)
		cancel()
		err := <-result
		if !errors.Is(err, context.Canceled) ||
			!errors.Is(err, ErrRaftConditionalOutcomeUnknown) {
			t.Fatalf("accepted cancellation %d error = %v", i, err)
		}
	}
	if got := applyCalls.Load(); got != int32(conditionalFutureWaiterCapacity) {
		t.Fatalf("apply calls = %d, want capacity %d", got, conditionalFutureWaiterCapacity)
	}

	blockedCtx, cancelBlocked := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancelBlocked()
	err := node.applyConditional(blockedCtx, entry)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("blocked ApplyConditional error = %v, want deadline", err)
	}
	if got := applyCalls.Load(); got != int32(conditionalFutureWaiterCapacity) {
		t.Fatalf("canceled-before-proposal invoked raft.Apply: calls=%d", got)
	}
	select {
	case <-accepted:
		t.Fatal("canceled-before-proposal produced a late submission")
	case <-time.After(25 * time.Millisecond):
	}

	futures[0].Resolve(nil, nil)
	result := make(chan error, 1)
	go func() {
		result <- node.applyConditional(context.Background(), entry)
	}()
	next := <-accepted
	next.Resolve(nil, ErrRaftConditionalConflict)
	if err := <-result; !errors.Is(err, ErrRaftConditionalConflict) {
		t.Fatalf("post-release ApplyConditional error = %v", err)
	}
	if got := applyCalls.Load(); got != int32(conditionalFutureWaiterCapacity+1) {
		t.Fatalf("slot release apply calls = %d", got)
	}

	for _, future := range futures[1:] {
		future.Resolve(nil, nil)
	}
}

func TestApplyBatchedOwnsSlicesAndRevalidatesFlush(t *testing.T) {
	var submitted *RaftLogEntry
	node := &RaftNode{
		publicLeaderHook: func() bool { return true },
		publicApplyHook: func(entry *RaftLogEntry, _ time.Duration) error {
			submitted = entry
			return nil
		},
	}
	bw := newRaftBatchWriter(node, 2, time.Hour)
	key := []byte("public-key-1")
	value := []byte("public-value-1")
	result := make(chan error, 1)
	go func() {
		result <- bw.ApplyBatched(BatchOp{Key: key, Value: value}, time.Second)
	}()
	waitForBatchPending(t, bw, 1)
	copy(key, []byte(prefixChunk))
	copy(value, []byte("mutated-value-"))
	if err := bw.ApplyBatched(BatchOp{Key: []byte("public-key-2"), Value: []byte("public-value-2")}, time.Second); err != nil {
		t.Fatalf("second ApplyBatched: %v", err)
	}
	if err := <-result; err != nil {
		t.Fatalf("first ApplyBatched: %v", err)
	}
	if submitted == nil || len(submitted.Batch) != 2 {
		t.Fatalf("submitted batch = %#v, want two ops", submitted)
	}
	if got := string(submitted.Batch[0].Key); got != "public-key-1" {
		t.Fatalf("submitted first key = %q, want copied original", got)
	}
	if got := string(submitted.Batch[0].Value); got != "public-value-1" {
		t.Fatalf("submitted first value = %q, want copied original", got)
	}
}

func TestApplyBatchedFlushRejectsProtectedBatchAndPropagatesToEveryWaiter(t *testing.T) {
	var applyCalls atomic.Int32
	node := &RaftNode{
		publicLeaderHook: func() bool { return true },
		publicApplyHook: func(*RaftLogEntry, time.Duration) error {
			applyCalls.Add(1)
			return nil
		},
	}
	bw := newRaftBatchWriter(node, 4, time.Hour)
	waiters := []chan error{make(chan error, 1), make(chan error, 1)}
	bw.pending = []BatchOp{
		{Key: []byte("public-key"), Value: []byte("public-value")},
		{Key: []byte(chunkMetadataKey(55)), Value: []byte("protected")},
	}
	bw.waiters = waiters
	bw.flush()
	for i, ch := range waiters {
		err := <-ch
		if err == nil || !strings.Contains(err.Error(), "protected metadata") {
			t.Fatalf("waiter %d error = %v, want protected rejection", i, err)
		}
	}
	if got := applyCalls.Load(); got != 0 {
		t.Fatalf("public apply hook called %d times for protected batch", got)
	}
}

func TestApplyBatchedFlushDeliversFSMErrorAndAcceptsSuccessfulResponse(t *testing.T) {
	fsmErr := errors.New("fsm rejected")
	node := &RaftNode{
		publicLeaderHook: func() bool { return true },
		publicApplyHook: func(*RaftLogEntry, time.Duration) error {
			return fsmErr
		},
	}
	bw := newRaftBatchWriter(node, 4, time.Hour)
	waiters := []chan error{make(chan error, 1), make(chan error, 1), make(chan error, 1)}
	bw.pending = []BatchOp{
		{Key: []byte("a"), Value: []byte("1")},
		{Key: []byte("b"), Value: []byte("2")},
		{Key: []byte("c"), Value: []byte("3")},
	}
	bw.waiters = waiters
	bw.flush()
	for i, ch := range waiters {
		if err := <-ch; !errors.Is(err, fsmErr) {
			t.Fatalf("waiter %d error = %v, want %v", i, err, fsmErr)
		}
	}

	node.publicApplyHook = func(*RaftLogEntry, time.Duration) error { return nil }
	if err := newRaftBatchWriter(node, 1, time.Hour).ApplyBatched(BatchOp{Key: []byte("ok"), Value: []byte("yes")}, time.Second); err != nil {
		t.Fatalf("successful non-error response ApplyBatched: %v", err)
	}
}

func TestApplyAutoForwardFollowerRejectsWithoutNetworkOrApply(t *testing.T) {
	var applyCalls atomic.Int32
	node := &RaftNode{
		publicLeaderHook: func() bool { return false },
		publicApplyHook: func(*RaftLogEntry, time.Duration) error {
			applyCalls.Add(1)
			return nil
		},
	}
	err := node.ApplyAutoForward(&RaftLogEntry{Op: OpSet, Key: []byte("public-key"), Value: []byte("value")}, time.Second)
	if err == nil || !strings.Contains(err.Error(), "not leader") {
		t.Fatalf("ApplyAutoForward follower error = %v, want not leader", err)
	}
	if got := applyCalls.Load(); got != 0 {
		t.Fatalf("follower ApplyAutoForward invoked apply/network hook %d times", got)
	}
}

func waitForBatchPending(t *testing.T, bw *raftBatchWriter, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		bw.mu.Lock()
		got := len(bw.pending)
		bw.mu.Unlock()
		if got == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	bw.mu.Lock()
	got := len(bw.pending)
	bw.mu.Unlock()
	t.Fatalf("pending batch length = %d, want %d", got, want)
}

func mustEncodeConditional(t *testing.T, conditional *ConditionalBatch) []byte {
	t.Helper()
	data, err := (&RaftLogEntry{Op: OpConditionalBatch, Conditional: conditional}).EncodeChecked()
	if err != nil {
		t.Fatalf("encode conditional entry: %v", err)
	}
	return data
}

func assertRawValue(t *testing.T, store *PebbleStore, key string, want []byte) {
	t.Helper()
	value, closer, err := store.db.Get([]byte(key))
	if err != nil {
		t.Fatalf("get %q: %v", key, err)
	}
	defer closer.Close()
	if !bytes.Equal(value, want) {
		t.Fatalf("value %q = %q, want %q", key, value, want)
	}
}

func assertRawMissing(t *testing.T, store *PebbleStore, key string) {
	t.Helper()
	_, closer, err := store.db.Get([]byte(key))
	if closer != nil {
		closer.Close()
	}
	if !errors.Is(err, pebble.ErrNotFound) {
		t.Fatalf("get %q error = %v, want not found", key, err)
	}
}

func testBackupTime() time.Time {
	return time.Date(2026, 7, 29, 12, 0, 0, 0, time.UTC)
}

func timeZero() time.Time {
	return time.Time{}
}

func validTermFencedCreatingTaskBatch(
	t *testing.T,
	id string,
	expectedRaftTerm uint64,
) *ConditionalBatch {
	t.Helper()
	task := validBackupTask(id, testBackupTime())
	normalized, err := normalizeBackupTask(&task)
	if err != nil {
		t.Fatalf("normalize term-fenced task: %v", err)
	}
	encoded, err := marshalValue(&normalized, codecMsgpack)
	if err != nil {
		t.Fatalf("encode term-fenced task: %v", err)
	}
	key := []byte(prefixBackupTask + id)
	return &ConditionalBatch{
		Version:          conditionalBatchTermFencedVersion,
		ExpectedRaftTerm: expectedRaftTerm,
		Preconditions: []ConditionalPrecondition{{
			Key:          key,
			ExpectAbsent: true,
		}},
		Mutations: []BatchOp{{
			Key:   key,
			Value: encoded,
		}},
	}
}

func encodeUncheckedConditionalPrefix(prefix []byte) []byte {
	var buf bytes.Buffer
	buf.WriteByte(byte(OpConditionalBatch))
	buf.WriteByte(conditionalBatchVersion)
	writeUint32(&buf, 0)
	writeUint32(&buf, 0)
	writeUint32(&buf, 1)
	writeBytes(&buf, prefix)
	writeUint32(&buf, 0)
	return buf.Bytes()
}

func encodeLegacyV3Conditional(conditional *ConditionalBatch) []byte {
	var buf bytes.Buffer
	buf.WriteByte(byte(OpConditionalBatch))
	buf.WriteByte(chunkTombstoneConditionalBatchVersion)
	writeUint32(&buf, uint32(len(conditional.Preconditions)))
	for _, precondition := range conditional.Preconditions {
		if precondition.ExpectAbsent {
			buf.WriteByte(1)
		} else {
			buf.WriteByte(0)
		}
		writeBytes(&buf, precondition.Key)
		if !precondition.ExpectAbsent {
			writeBytes(&buf, precondition.ExpectedValue)
		}
	}
	writeUint32(&buf, uint32(len(conditional.Mutations)))
	for _, mutation := range conditional.Mutations {
		writeBatchOp(&buf, mutation)
	}
	writeUint32(&buf, 0)
	return buf.Bytes()
}

type controlledConditionalFuture struct {
	done     chan struct{}
	once     sync.Once
	err      error
	response interface{}
}

func newControlledConditionalFuture() *controlledConditionalFuture {
	return &controlledConditionalFuture{done: make(chan struct{})}
}

func (f *controlledConditionalFuture) Resolve(err error, response interface{}) {
	f.once.Do(func() {
		f.err = err
		f.response = response
		close(f.done)
	})
}

func (f *controlledConditionalFuture) Error() error {
	<-f.done
	return f.err
}

func (f *controlledConditionalFuture) Response() interface{} {
	return f.response
}

func (f *controlledConditionalFuture) Index() uint64 {
	return 1
}
