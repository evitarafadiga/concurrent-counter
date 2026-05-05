package response_analyser

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

// ─── Benchmark fixtures ───────────────────────────────────────────────────────

// smallPayload is a realistic 5-code blob (~40 bytes).
var smallPayload = []byte("AU_ieu13\n103956\nghgqb\n10002\na012ne")

// largePayload is 1 000 unique codes joined by newlines.
var largePayload = func() []byte {
	codes := make([]string, 1000)
	for i := range codes {
		codes[i] = fmt.Sprintf("code-%d", i)
	}
	return []byte(strings.Join(codes, "\n"))
}()

// warmAnalyser returns an analyser pre-seeded with all keys so subsequent
// benchmarks exercise the fast (read-lock + atomic) path, not the slow path.
func warmAnalyser(b *testing.B, workers, bufSize int) DataExporter {
	b.Helper()
	a := New(context.Background(), workers, bufSize)
	a.Analyse(largePayload)
	time.Sleep(50 * time.Millisecond) // let workers process
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
// We expect 0 allocs/op once the pool is warmed up.
func BenchmarkAnalyse_Small(b *testing.B) {
	a := warmAnalyser(b, 4, 1<<16)
	defer shutdownB(b, a)

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		a.Analyse(smallPayload)
	}
}

// BenchmarkAnalyse_Large measures the hotpath for a large (1 000-code) payload.
func BenchmarkAnalyse_Large(b *testing.B) {
	a := warmAnalyser(b, 4, 1<<16)
	defer shutdownB(b, a)

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		a.Analyse(largePayload)
	}
}

// BenchmarkAnalyse_Parallel models 100 goroutines hammering Analyse concurrently.
// This is the closest simulation to production load.
func BenchmarkAnalyse_Parallel(b *testing.B) {
	a := warmAnalyser(b, 8, 1<<16)
	defer shutdownB(b, a)

	b.ReportAllocs()
	b.SetParallelism(100)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			a.Analyse(smallPayload)
		}
	})
}

// BenchmarkAnalyse_ZeroAlloc verifies the 0-alloc contract.
// Run with: go test -bench=BenchmarkAnalyse_ZeroAlloc -benchmem
// Expected: 0 allocs/op after pool warm-up.
func BenchmarkAnalyse_ZeroAlloc(b *testing.B) {
	a := warmAnalyser(b, 4, 1<<16)
	defer shutdownB(b, a)

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		a.Analyse(smallPayload)
	}

	// Fail the benchmark if allocations are detected.
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

// ─── Process benchmarks ───────────────────────────────────────────────────────

// BenchmarkProcess isolates the parser + counter update path (no channel overhead).
func BenchmarkProcess_Small(b *testing.B) {
	a := warmAnalyser(b, 1, 1).(*analyser)
	defer shutdownB(b, a)

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		a.process(smallPayload)
	}
}

func BenchmarkProcess_Large(b *testing.B) {
	a := warmAnalyser(b, 1, 1).(*analyser)
	defer shutdownB(b, a)

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		a.process(largePayload)
	}
}

// ─── GetCurrentCounts benchmarks ─────────────────────────────────────────────

func BenchmarkGetCurrentCounts_Small(b *testing.B) {
	a := warmAnalyser(b, 4, 1<<14)
	defer shutdownB(b, a)

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = a.GetCurrentCounts()
	}
}

func BenchmarkGetCurrentCounts_Large(b *testing.B) {
	a := New(context.Background(), 4, 1<<14)
	// Seed with 1 000 unique keys.
	a.Analyse(largePayload)
	time.Sleep(50 * time.Millisecond)
	b.ResetTimer()
	defer shutdownB(b, a)

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = a.GetCurrentCounts()
	}
}

// ─── Increment benchmarks ─────────────────────────────────────────────────────

// BenchmarkIncrement_Existing measures the fast path (key already present).
func BenchmarkIncrement_Existing(b *testing.B) {
	a := warmAnalyser(b, 1, 1).(*analyser)
	defer shutdownB(b, a)

	key := []byte("AU_ieu13")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		a.increment(key)
	}
}

// BenchmarkIncrement_New measures the slow path (first-time key insertion).
func BenchmarkIncrement_New(b *testing.B) {
	a := New(context.Background(), 1, 1).(*analyser)
	defer shutdownB(b, a)

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("newkey-%d", i))
		a.increment(key)
	}
}

// ─── FNV hash benchmark ───────────────────────────────────────────────────────

func BenchmarkFNV32(b *testing.B) {
	key := []byte("AU_ieu13")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = fnv32(key)
	}
}

// ─── End-to-end throughput ────────────────────────────────────────────────────

// BenchmarkE2E_Throughput measures the full pipeline: Analyse → worker → counter.
// It reports operations per second under sustained parallel load.
func BenchmarkE2E_Throughput(b *testing.B) {
	a := warmAnalyser(b, 8, 1<<16)
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
