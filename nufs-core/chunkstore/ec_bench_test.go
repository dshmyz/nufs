//go:build linux

package chunkstore

import (
	"fmt"
	"runtime"
	"testing"
)

const (
	ecDataShards  = 6
	ecParityShards = 3
	ecTotalShards = ecDataShards + ecParityShards
)

// BenchmarkECStripeSizeCompare measures EC decode latency and memory for
// different stripe sizes. Run with:
//
//	go test ./chunkstore/ -run '^$' -bench BenchmarkECStripe -benchmem -memprofile /tmp/ec_mem.prof
//
// Compare outputs for stripeSize=1MiB vs stripeSize=64MiB to quantify
// the read amplification reduction.

func BenchmarkECStripe(b *testing.B) {
	for _, stripeSize := range []int{1 << 20, 4 << 20, 16 << 20, 64 << 20} {
		stripeMB := stripeSize >> 20
		b.Run(fmt.Sprintf("stripe_%dMiB", stripeMB), func(b *testing.B) {
			benchmarkECStripeDecode(b, stripeSize)
		})
	}
}

func benchmarkECStripeDecode(b *testing.B, stripeSize int) {
	encoder := GetECEncoder(ecDataShards, ecParityShards)

	// Build a fake stripe: stripeSize bytes of patterned data
	data := make([]byte, stripeSize)
	for i := range data {
		data[i] = byte(i)
	}

	// Encode into shards
	result, err := encoder.Encode(data)
	if err != nil {
		b.Fatal(err)
	}

	shards := make([][]byte, ecTotalShards)
	copy(shards[:ecDataShards], result.DataShards)
	copy(shards[ecDataShards:], result.ParityShards)
	present := make([]bool, ecTotalShards)
	for i := range present {
		present[i] = true
	}

	// Decode the full stripe (simulating what readECChunk does)
	b.SetBytes(int64(stripeSize))
	b.ReportAllocs()

	// Measure wall-clock decode time
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		decoded, err := encoder.Decode(shards, present, stripeSize)
		if err != nil {
			b.Fatal(err)
		}
		_ = decoded
	}
}

// BenchmarkECReadAmplification compares how much data needs to be fetched
// from the network (shards) vs what the caller actually needs, for a 4 KiB
// range read on different stripe sizes.
func BenchmarkECReadAmplification(b *testing.B) {
	for _, stripeSize := range []int{1 << 20, 4 << 20, 16 << 20, 64 << 20} {
		stripeMB := stripeSize >> 20
		b.Run(fmt.Sprintf("stripe_%dMiB", stripeMB), func(b *testing.B) {
			benchmarkECReadAmplification(b, stripeSize)
		})
	}
}

func benchmarkECReadAmplification(b *testing.B, stripeSize int) {
	encoder := GetECEncoder(ecDataShards, ecParityShards)

	data := make([]byte, stripeSize)
	for i := range data {
		data[i] = byte(i)
	}

	result, err := encoder.Encode(data)
	if err != nil {
		b.Fatal(err)
	}

	shards := make([][]byte, ecTotalShards)
	copy(shards[:ecDataShards], result.DataShards)
	copy(shards[ecDataShards:], result.ParityShards)
	present := make([]bool, ecTotalShards)
	for i := range present {
		present[i] = true
	}

	// Simulate reading 4 KiB from offset 0
	readWindow := 4096
	shardSizeBytes := (stripeSize + ecDataShards - 1) / ecDataShards

	// Compute how many shards are needed (K = DataShards for decode)
	needShards := ecDataShards
	fetchBytes := needShards * shardSizeBytes

	b.SetBytes(int64(readWindow))
	b.ReportAllocs()

	b.Run("fetch_and_decode", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			// Simulate: fetch K shards from network
			for s := 0; s < needShards; s++ {
				_ = shards[s] // network read
			}
			// Decode full stripe
			decoded, _ := encoder.Decode(shards, present, stripeSize)
			// Slice to range
			_ = decoded[:readWindow]
		}
	})

	// Report the amplification ratio
	amplification := float64(fetchBytes) / float64(readWindow)
	b.ReportMetric(amplification, "fetch-amplification")
	b.ReportMetric(float64(stripeSize), "stripe-bytes")
	b.ReportMetric(float64(shardSizeBytes), "shard-bytes")
}

// BenchmarkECDecodeMemory measures memory allocated during EC decode
// for different stripe sizes. Run with -memprofile to capture full profile.
func BenchmarkECDecodeMemory(b *testing.B) {
	for _, stripeSize := range []int{1 << 20, 4 << 20, 16 << 20, 64 << 20} {
		stripeMB := stripeSize >> 20
		b.Run(fmt.Sprintf("stripe_%dMiB", stripeMB), func(b *testing.B) {
			benchmarkECDecodeMemory(b, stripeSize)
		})
	}
}

func benchmarkECDecodeMemory(b *testing.B, stripeSize int) {
	encoder := GetECEncoder(ecDataShards, ecParityShards)

	data := make([]byte, stripeSize)
	for i := range data {
		data[i] = byte(i)
	}

	result, _ := encoder.Encode(data)

	shards := make([][]byte, ecTotalShards)
	copy(shards[:ecDataShards], result.DataShards)
	copy(shards[ecDataShards:], result.ParityShards)
	present := make([]bool, ecTotalShards)
	for i := range present {
		present[i] = true
	}

	// Warm up
	decoded, _ := encoder.Decode(shards, present, stripeSize)
	_ = decoded

	// Force GC before measurement
	runtime.GC()

	var memBefore, memAfter runtime.MemStats
	runtime.ReadMemStats(&memBefore)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d, _ := encoder.Decode(shards, present, stripeSize)
		_ = d
	}
	b.StopTimer()

	runtime.ReadMemStats(&memAfter)

	allocBytes := int64(memAfter.TotalAlloc) - int64(memBefore.TotalAlloc)
	if b.N > 0 {
		perOp := allocBytes / int64(b.N)
		b.ReportMetric(float64(perOp), "decode-bytes")
		b.ReportMetric(float64(stripeSize)/float64(perOp), "compression-ratio")
	}
	b.SetBytes(int64(stripeSize))
}
