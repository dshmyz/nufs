package metadata

import (
	"encoding/binary"
	"testing"
)

func TestBloomFilter_AddContains(t *testing.T) {
	bf := NewBloomFilter(1000, 0.01)

	// Add some elements
	for i := uint64(0); i < 100; i++ {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], i)
		bf.Add(buf[:])
	}

	// Check that added elements are found
	for i := uint64(0); i < 100; i++ {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], i)
		if !bf.Contains(buf[:]) {
			t.Errorf("element %d should be in filter", i)
		}
	}

	// Check that non-added elements are not found (mostly)
	falsePositives := 0
	for i := uint64(100); i < 200; i++ {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], i)
		if bf.Contains(buf[:]) {
			falsePositives++
		}
	}

	// With 1% FP rate and 100 elements, we should have very few false positives
	if falsePositives > 5 {
		t.Errorf("too many false positives: %d", falsePositives)
	}
}

func TestBloomFilter_Count(t *testing.T) {
	bf := NewBloomFilter(100, 0.01)

	if bf.Count() != 0 {
		t.Errorf("initial count should be 0, got %d", bf.Count())
	}

	for i := uint64(0); i < 50; i++ {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], i)
		bf.Add(buf[:])
	}

	if bf.Count() != 50 {
		t.Errorf("count should be 50, got %d", bf.Count())
	}
}

func TestBloomFilter_Merge(t *testing.T) {
	bf1 := NewBloomFilter(100, 0.01)
	bf2 := NewBloomFilter(100, 0.01)

	// Add different elements to each
	for i := uint64(0); i < 50; i++ {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], i)
		bf1.Add(buf[:])
	}
	for i := uint64(50); i < 100; i++ {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], i)
		bf2.Add(buf[:])
	}

	bf1.Merge(bf2)

	// Check that elements from both filters are found
	for i := uint64(0); i < 100; i++ {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], i)
		if !bf1.Contains(buf[:]) {
			t.Errorf("element %d should be in merged filter", i)
		}
	}
}
