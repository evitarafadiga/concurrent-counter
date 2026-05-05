package response_analyser

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

// ─── Fixtures ─────────────────────────────────────────────────────────────────

var smallPayload = []byte("AU_ieu13\n103956\nghgqb\n10002\na012ne")

var largePayload = func() []byte {
	codes := make([]string, 1000)
	for i := range codes {
		codes[i] = fmt.Sprintf("code-%d", i)
	}
	return []byte(strings.Join(codes, "\n"))
}()

func warmAnalyser(b *testing.B, bufSize int) DataExporter {
	b.Helper()
	a := New(context.Background(), 0, bufSize)
	a.Analyse(largePayload)
	time.Sleep(150 * time.Millisecond) // let consumer + snapshot settle
	b.ResetTimer()
	return a
}

func shutdownB(b *testing.B, a DataExporter) {
	b.Helper()
	b.StopTimer()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = a.Shutdown(ctx)
}

// ─── Analyse benchmarks ───────────────────────────────────────────────────────

// BenchmarkAnalyse_Small measures the hotpath for a small (5-code) payload.
// Expected: 0 allocs/op once pool is warmed up.
func BenchmarkAnalyse_Small(b *testing.B) {
	a := warmAnalyser(b, 1<<16)
	defer shutdownB(b, a)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		a.Analyse(smallPayload)
	}
}

// BenchmarkAnalyse_Large measures the hotpath for a 1000-code payload.
func BenchmarkAnalyse_Large(b *testing.B) {
	a := warmAnalyser(b, 1<<16)
	defer shutdownB(b, a)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		a.Analyse(largePayload)
	}
}

// BenchmarkAnalyse_Parallel models 100 concurrent goroutines on the hotpath.
func BenchmarkAnalyse_Parallel(b *testing.B) {
	a := warmAnalyser(b, 1<<16)
	defer shutdownB(b, a)
	b.ReportAllocs()
	b.SetParallelism(100)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			a.Analyse(smallPayload)
		}
	})
}

// BenchmarkAnalyse_ZeroAlloc verifies the 0-alloc contract on the hotpath.
func BenchmarkAnalyse_ZeroAlloc(b *testing.B) {
	a := warmAnalyser(b, 1<<16)
	defer shutdownB(b, a)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		a.Analyse(smallPayload)
	}
	result := testing.Benchmark(func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			a.Analyse(smallPayload)
		}
	})
	if result.AllocsPerOp() > 0 {
		b.Errorf("BenchmarkAnalyse_ZeroAlloc: got %d allocs/op, want 0", result.AllocsPerOp())
	}
}

// ─── GetCurrentCounts benchmarks ─────────────────────────────────────────────

// BenchmarkGetCurrentCounts_Small measures lock-free snapshot read (small map).
func BenchmarkGetCurrentCounts_Small(b *testing.B) {
	a := warmAnalyser(b, 1<<14)
	defer shutdownB(b, a)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = a.GetCurrentCounts()
	}
}

// BenchmarkGetCurrentCounts_Large measures snapshot read with 1000 keys.
func BenchmarkGetCurrentCounts_Large(b *testing.B) {
	a := New(context.Background(), 0, 1<<14)
	a.Analyse(largePayload)
	time.Sleep(150 * time.Millisecond)
	b.ResetTimer()
	defer shutdownB(b, a)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = a.GetCurrentCounts()
	}
}

// ─── Bloom filter benchmarks ──────────────────────────────────────────────────

// BenchmarkBloom_MayContain measures the bloom filter check path (no lock).
func BenchmarkBloom_MayContain(b *testing.B) {
	var f bloomFilter
	key := []byte("AU_ieu13")
	h1, h2 := hash128(key)
	f.add(h1, h2)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = f.mayContain(h1, h2)
	}
}

// BenchmarkBloom_Add measures the bloom filter insert path.
func BenchmarkBloom_Add(b *testing.B) {
	var f bloomFilter
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		h1, h2 := hash128(key)
		f.add(h1, h2)
	}
}

// ─── Hash benchmarks ──────────────────────────────────────────────────────────

func BenchmarkHash128(b *testing.B) {
	key := []byte("AU_ieu13")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = hash128(key)
	}
}

// ─── End-to-end throughput ────────────────────────────────────────────────────

// BenchmarkE2E_Throughput measures sustained parallel hotpath throughput.
func BenchmarkE2E_Throughput(b *testing.B) {
	a := warmAnalyser(b, 1<<16)
	defer shutdownB(b, a)
	b.ReportAllocs()
	b.SetParallelism(100)
	b.RunParallel(func(pb *testing.PB) {
		payload := make([]byte, len(smallPayload))
		copy(payload, smallPayload)
		for pb.Next() {
			a.Analyse(payload)
		}
	})
}
