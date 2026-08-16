package metadata

import (
	"encoding/json"
	"reflect"
	"testing"
)

// ============================================================
// Codec convergence regression (roadmap stage 4, knife-1)
// New writes are msgpack-only; reads auto-sniff JSON for the
// legacy rows written before convergence. These tests lock that:
//   1. msgpack round-trip survives unmarshalValue
//   2. legacy JSON rows (written as raw JSON bytes) still decode
//   3. msgpack first byte never collides with the JSON sniff
//      ('{' 0x7B / '[' 0x5B), so sniffing stays unambiguous
// ============================================================

func TestCodecMsgpackRoundTrip(t *testing.T) {
	cases := []interface{}{
		&ChunkMeta{ID: 42, Size: 512, State: ChunkReady},
		&InodeMeta{ID: 7, Type: FileDirectory, NLink: 2, Mode: 0755, ChunkMap: []ChunkRef{{ID: 42, Offset: 0, Length: 512}}},
		&RepairTask{ChunkID: 42, Reason: "triggered", Priority: 1},
	}
	for _, in := range cases {
		raw, err := marshalValue(in, codecMsgpack)
		if err != nil {
			t.Fatalf("msgpack marshal %T: %v", in, err)
		}
		out := reflect.New(reflect.TypeOf(in).Elem()).Interface()
		if err := unmarshalValue(raw, out); err != nil {
			t.Fatalf("msgpack unmarshal %T: %v", in, err)
		}
		if !reflect.DeepEqual(out, in) {
			t.Errorf("msgpack round-trip mismatch: got %#v, want %#v", out, in)
		}
	}
}

func TestCodecJSONReadCompat(t *testing.T) {
	// Legacy rows were JSON-encoded; encoding/json is the authentic
	// legacy producer. A JSON byte blob must still decode via the
	// format-sniffing read path (unmarshalValue).
	legacy := &ChunkMeta{ID: 41, Size: 1024, State: ChunkCreated}
	blob, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	var got ChunkMeta
	if err := unmarshalValue(blob, &got); err != nil {
		t.Fatalf("sniff JSON read failed: %v", err)
	}
	if !reflect.DeepEqual(&got, legacy) {
		t.Errorf("JSON compat mismatch: got %#v, want %#v", &got, legacy)
	}
}

func TestCodecMsgpackFirstByteNoSniffCollision(t *testing.T) {
	// The sniff treats '{' (0x7B) / '[' (0x5B) as JSON. msgpack rows
	// must never start with those bytes, else a msgpack row would be
	// misread as JSON and decode would fail.
	for _, v := range []interface{}{
		&ChunkMeta{ID: 1, Size: 2, State: ChunkReady},
		&InodeMeta{ID: 1, Type: FileDirectory},
		&BucketUsage{},
		&RepairTask{},
	} {
		raw, err := marshalValue(v, codecMsgpack)
		if err != nil {
			t.Fatalf("marshal %T: %v", v, err)
		}
		if len(raw) == 0 {
			t.Fatalf("empty msgpack blob for %T", v)
		}
		if raw[0] == '{' || raw[0] == '[' {
			t.Errorf("%T msgpack first byte 0x%02x collides with JSON sniff", v, raw[0])
		}
	}
}
