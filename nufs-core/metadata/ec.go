package metadata

import (
	"fmt"
	"sync"
)

type ECEncoder struct {
	DataShards   int // K
	ParityShards int // M
}

type ECResult struct {
	DataShards   [][]byte
	ParityShards [][]byte
	TotalShards  int
}

func NewECEncoder(dataShards, parityShards int) *ECEncoder {
	return &ECEncoder{DataShards: dataShards, ParityShards: parityShards}
}

// Encode splits data into K data shards and computes M parity shards.
func (ec *ECEncoder) Encode(data []byte) (*ECResult, error) {
	k := ec.DataShards
	m := ec.ParityShards
	if k <= 0 || m <= 0 {
		return nil, fmt.Errorf("ec: invalid shard counts k=%d m=%d", k, m)
	}

	shardSize := (len(data) + k - 1) / k
	paddedLen := shardSize * k
	padded := make([]byte, paddedLen)
	copy(padded, data)

	result := &ECResult{
		DataShards:   make([][]byte, k),
		ParityShards: make([][]byte, m),
		TotalShards:  k + m,
	}
	for i := 0; i < k; i++ {
		shard := make([]byte, shardSize)
		copy(shard, padded[i*shardSize:(i+1)*shardSize])
		result.DataShards[i] = shard
	}

	for j := 0; j < m; j++ {
		parity := make([]byte, shardSize)
		for i := 0; i < k; i++ {
			coeff := gfPow(2, (j+1)*i)
			if coeff == 0 {
				continue
			}
			for b := 0; b < shardSize; b++ {
				parity[b] ^= gfMul(result.DataShards[i][b], coeff)
			}
		}
		result.ParityShards[j] = parity
	}
	return result, nil
}

// Decode reconstructs data from available shards.
// Strategy: build a KxK matrix from available equations, invert it via Gauss-Jordan,
// then combine source shards using the inverted coefficients.
func (ec *ECEncoder) Decode(shards [][]byte, shardPresent []bool, originalLen int) ([]byte, error) {
	k := ec.DataShards
	m := ec.ParityShards

	if len(shards) != k+m || len(shardPresent) != k+m {
		return nil, fmt.Errorf("ec: shard count mismatch")
	}

	available := 0
	for _, p := range shardPresent {
		if p {
			available++
		}
	}
	if available < k {
		return nil, fmt.Errorf("ec: insufficient shards (have %d, need %d)", available, k)
	}

	var shardSize int
	for i := 0; i < k+m; i++ {
		if shardPresent[i] && len(shards[i]) > 0 {
			shardSize = len(shards[i])
			break
		}
	}
	if shardSize == 0 {
		return nil, fmt.Errorf("ec: no available shards")
	}

	// Build K equations. eqSrc[i] = which shard equation i reads from.
	eqSrc := make([]int, k)
	eqCoeffs := make([][]byte, k)

	next := 0
	// Data shard equations first
	for i := 0; i < k; i++ {
		if shardPresent[i] {
			row := make([]byte, k)
			row[i] = 1
			eqCoeffs[next] = row
			eqSrc[next] = i
			next++
		}
	}
	// Then parity equations
	for j := 0; j < m && next < k; j++ {
		if shardPresent[k+j] {
			row := make([]byte, k)
			for i := 0; i < k; i++ {
				row[i] = gfPow(2, (j+1)*i)
			}
			eqCoeffs[next] = row
			eqSrc[next] = k + j
			next++
		}
	}

	// Augmented matrix: K x 2K
	// Left half: equation coefficients (will be eliminated to identity)
	// Right half: initially identity, will track how to combine source equations
	aug := make([][]byte, k)
	for i := 0; i < k; i++ {
		aug[i] = make([]byte, 2*k)
		copy(aug[i][:k], eqCoeffs[i])
		aug[i][k+i] = 1
	}

	// Gauss-Jordan — left half becomes identity
	for col := 0; col < k; col++ {
		pivot := -1
		for row := col; row < k; row++ {
			if aug[row][col] != 0 {
				pivot = row
				break
			}
		}
		if pivot == -1 {
			return nil, fmt.Errorf("ec: singular matrix")
		}
		aug[col], aug[pivot] = aug[pivot], aug[col]
		
		inv := gfInv(aug[col][col])
		for c := col; c < 2*k; c++ {
			aug[col][c] = gfMul(aug[col][c], inv)
		}
		for row := 0; row < k; row++ {
			if row == col {
				continue
			}
			factor := aug[row][col]
			if factor == 0 {
				continue
			}
			for c := col; c < 2*k; c++ {
				aug[row][c] ^= gfMul(factor, aug[col][c])
			}
		}
	}

	// After elimination, aug[i][k+j] = coefficient of equation j for data shard i.
	// Equation j reads from shard eqSrc[j].
	reconstructed := make([][]byte, k)
	for d := 0; d < k; d++ {
		shard := make([]byte, shardSize)
		for eq := 0; eq < k; eq++ {
			coeff := aug[d][k+eq]
			if coeff == 0 {
				continue
			}
			src := eqSrc[eq]
			srcData := shards[src]
			if srcData == nil || len(srcData) < shardSize {
				continue
			}
			for b := 0; b < shardSize; b++ {
				shard[b] ^= gfMul(srcData[b], coeff)
			}
		}
		reconstructed[d] = shard
	}

	result := make([]byte, 0, originalLen)
	for i := 0; i < k; i++ {
		result = append(result, reconstructed[i]...)
	}
	return result[:originalLen], nil
}

// Verify checks parity consistency.
func (ec *ECEncoder) Verify(result *ECResult) bool {
	k := ec.DataShards
	m := ec.ParityShards
	shardSize := len(result.DataShards[0])

	for j := 0; j < m; j++ {
		expected := make([]byte, shardSize)
		for i := 0; i < k; i++ {
			coeff := gfPow(2, (j+1)*i)
			if coeff == 0 {
				continue
			}
			for b := 0; b < shardSize; b++ {
				expected[b] ^= gfMul(result.DataShards[i][b], coeff)
			}
		}
		for b := 0; b < shardSize; b++ {
			if expected[b] != result.ParityShards[j][b] {
				return false
			}
		}
	}
	return true
}

// ========== GF(256) Arithmetic ==========

var (
	gfExpTable [512]byte
	gfLogTable [256]byte
	gfOnce     sync.Once
)

func initGFTables() {
	x := byte(1)
	for i := 0; i < 255; i++ {
		gfExpTable[i] = x
		gfExpTable[i+255] = x
		gfLogTable[x] = byte(i)
		x2 := int(x) << 1
		if x2 >= 256 {
			x2 ^= 0x11D
		}
		x = byte(x2)
	}
	gfLogTable[0] = 0
}

func gfMul(a, b byte) byte {
	gfOnce.Do(initGFTables)
	if a == 0 || b == 0 {
		return 0
	}
	return gfExpTable[int(gfLogTable[a])+int(gfLogTable[b])]
}

func gfInv(a byte) byte {
	gfOnce.Do(initGFTables)
	if a == 0 {
		return 0
	}
	return gfExpTable[255-int(gfLogTable[a])]
}

func gfPow(a byte, n int) byte {
	gfOnce.Do(initGFTables)
	if a == 0 {
		return 0
	}
	if n == 0 {
		return 1
	}
	logA := int(gfLogTable[a])
	result := (logA * n) % 255
	if result < 0 {
		result += 255
	}
	return gfExpTable[result]
}
