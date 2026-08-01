package index

import (
	"testing"

	"github.com/example/dfs/datanode/storage"
)

func TestKeyRoundtrip(t *testing.T) {
	id := storage.ExtentID(0x1122334455667788)
	gen := storage.Generation(0xabcdef)
	k := Key(id, gen)
	if len(k) != KeyLen {
		t.Fatalf("key len = %d, want %d", len(k), KeyLen)
	}
	if ExtentFromKey(k) != id {
		t.Fatalf("extent mismatch: %x", ExtentFromKey(k))
	}
	if GenerationFromKey(k) != gen {
		t.Fatalf("generation mismatch: %x", GenerationFromKey(k))
	}
	// Key ordering: same extent, higher generation sorts after.
	if string(Key(id, gen)) >= string(Key(id, gen+1)) {
		t.Fatal("expected generation to sort within an extent")
	}
}

func TestValueRoundtrip(t *testing.T) {
	v := Value{
		SegmentID:  42,
		Offset:     8192,
		StoredLen:  4096,
		LogicalLen: 4096,
		State:      storage.ExtentDurable,
		Checksum:   0x12345678,
	}
	buf := make([]byte, ValueLen)
	if err := v.Encode(buf); err != nil {
		t.Fatal(err)
	}
	var decoded Value
	if err := decoded.Decode(buf); err != nil {
		t.Fatal(err)
	}
	if decoded != v {
		t.Fatalf("roundtrip mismatch: got %+v want %+v", decoded, v)
	}
}
