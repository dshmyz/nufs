package metadata

import (
	"hash/fnv"
	"sync"
)

// BloomFilter is a space-efficient probabilistic data structure
// for testing set membership. False positives are possible, but
// false negatives are not.
type BloomFilter struct {
	mu      sync.Mutex
	bits    []uint64
	numBits uint64
	hashes  int
	count   uint64
}

// NewBloomFilter creates a Bloom filter with the given expected element
// count and false positive probability (0 < fp < 1).
func NewBloomFilter(expectedElements int, fpRate float64) *BloomFilter {
	// Optimal bit count: m = -n*ln(p) / (ln(2)^2)
	n := float64(expectedElements)
	p := fpRate
	lnP := ln(p)
	ln2 := ln(2.0)
	m := -n * lnP / (ln2 * ln2)
	numBits := uint64(m)
	if numBits < 64 {
		numBits = 64
	}

	// Optimal hash count: k = (m/n) * ln(2)
	k := int(float64(numBits)/n*ln2 + 0.5)
	if k < 1 {
		k = 1
	}
	if k > 30 {
		k = 30 // practical upper bound
	}

	return &BloomFilter{
		bits:    make([]uint64, (numBits+63)/64),
		numBits: numBits,
		hashes:  k,
	}
}

// ln computes natural logarithm using the series expansion for ln(1+x).
// For values outside (0.5, 2), we use the identity ln(x) = ln(x/2^k) + k*ln(2).
func ln(x float64) float64 {
	if x <= 0 {
		return -1e308 // negative infinity approximation
	}
	if x == 1 {
		return 0
	}

	// Normalize: find k such that x/2^k is in (0.5, 2)
	k := 0.0
	for x > 2.0 {
		x /= 2.0
		k++
	}
	for x < 0.5 {
		x *= 2.0
		k--
	}

	// Now x is in (0.5, 2). Use the series: ln(1+u) = u - u^2/2 + u^3/3 - ...
	// where u = (x-1)/(x+1)
	u := (x - 1) / (x + 1)
	u2 := u * u
	result := u
	term := u
	for i := 3; i < 50; i += 2 {
		term *= u2
		result += term / float64(i)
	}
	result *= 2

	// Add back k*ln(2)
	ln2 := 0.6931471805599453
	return result + k*ln2
}

// Add inserts an element into the Bloom filter.
func (bf *BloomFilter) Add(data []byte) {
	bf.mu.Lock()
	defer bf.mu.Unlock()

	h1, h2 := bf.doubleHash(data)
	for i := 0; i < bf.hashes; i++ {
		idx := (h1 + uint64(i)*h2) % bf.numBits
		bf.bits[idx/64] |= 1 << (idx % 64)
	}
	bf.count++
}

// Contains returns true if the element is probably in the set,
// or false if it is definitely not in the set.
func (bf *BloomFilter) Contains(data []byte) bool {
	bf.mu.Lock()
	defer bf.mu.Unlock()

	h1, h2 := bf.doubleHash(data)
	for i := 0; i < bf.hashes; i++ {
		idx := (h1 + uint64(i)*h2) % bf.numBits
		if bf.bits[idx/64]&(1<<(idx%64)) == 0 {
			return false
		}
	}
	return true
}

// Count returns the number of elements added.
func (bf *BloomFilter) Count() uint64 {
	bf.mu.Lock()
	defer bf.mu.Unlock()
	return bf.count
}

// doubleHash computes two independent hash values from data.
func (bf *BloomFilter) doubleHash(data []byte) (uint64, uint64) {
	h := fnv.New64a()
	h.Write(data)
	h1 := h.Sum64()

	h.Reset()
	h.Write([]byte{0xff})
	h.Write(data)
	h2 := h.Sum64()

	return h1, h2
}

// Merge combines two Bloom filters (must have same parameters).
func (bf *BloomFilter) Merge(other *BloomFilter) {
	bf.mu.Lock()
	defer bf.mu.Unlock()
	other.mu.Lock()
	defer other.mu.Unlock()

	for i := range bf.bits {
		bf.bits[i] |= other.bits[i]
	}
	bf.count += other.count
}
