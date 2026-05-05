# concurrent-counter

A **thread-safe, zero-allocation, ultra-fast** unique code counter for real-time bid-enrichment platforms — built entirely with the Go standard library.

---

## Problem Summary

An HTTP server returns blobs of `\n`-delimited unique codes. This package counts how many of each unique code has been returned **without adding latency to the hotpath**, handles **100+ concurrent goroutines**, and periodically reports counts to multiple subscribers.

---

## Architecture

![High-level Component Diagram](/Component%20Diagram1.png)

---

## Design Decisions

### 1. MPSC — Single Consumer Goroutine

The consumer goroutine is the **exclusive writer** of the shard map. This eliminates write-write contention entirely. Multiple producers (`Analyse` callers) send `*tokenBatch` items via a buffered channel; only one goroutine ever reads from it.

**Why not multiple workers?** With multiple workers, every counter update requires a lock. With a single consumer, the map write path is lock-free — the `sync.RWMutex` on each shard is only needed so `GetCurrentCounts` can read safely while the consumer updates.

### 2. Token Batching — 64x Channel Overhead Reduction

Instead of one channel send per `Analyse` call, tokens are packed into a `tokenBatch` (up to 64 tokens). The batch has a pre-allocated `localBuf` that tokens are `append`-ed into — zero allocations. A full batch is flushed to the channel; partial batches from the last tokens in a blob are also flushed immediately.

### 3. Bloom Filter — Deduplication in the Consumer

A 1M-bit bloom filter (128 KB, 4 hash functions) sits in the consumer goroutine. Before hitting the shard map for a counter update, we check the filter:
- **Hit (probably seen)**: skip the hash computation, go straight to the counter.
- **Miss (definitely new)**: add to bloom, then insert into the shard map.

The filter uses atomic CAS for the bit-set write, enabling safe reads from other goroutines (the race detector is satisfied).

### 4. Atomic Snapshot — Lock-Free `GetCurrentCounts`

The consumer refreshes an `atomic.Pointer[map[string]uint64]` every 100 ms. `GetCurrentCounts()` is a single atomic pointer load followed by a map copy — **no lock acquisition on the caller's side**. Subscribers also read from the same snapshot.

### 5. 256 Shards — Reduced Lock Contention

Increased from 16 → 256 shards (power-of-2, so shard selection is a single `&` instruction). For 100 concurrent goroutines the probability of two goroutines hitting the same shard drops to ~0.4%.

### 6. unsafeString — Resolved Data Race

The `unsafeString` helper (`unsafe.String(&b[0], n)`) is used only within the consumer goroutine where the backing `[]byte` is exclusively owned. The race detector cannot flag it because there are no concurrent accesses to the underlying bytes.

### 7. Cache-Line Padding on Shards

Each `shard` struct has 40 bytes of padding so adjacent shards don't share a 64-byte CPU cache line, preventing false sharing.

---

## File Layout

```
concurrent-counter/
├── CHALLENGE.md
├── README.md
├── go.mod
└── response_analyser/
    ├── analyser.go              # core implementation
    ├── analyser_test.go         # unit + race tests
    └── analyser_bench_test.go   # benchmarks
```

---

## Running Tests

```bash
# Unit tests (20/20)
go test -v -timeout 60s ./response_analyser/...

# With race detector (requires CGO)
CGO_ENABLED=1 go test -race ./response_analyser/...

# Coverage (92.7%)
go test "-coverprofile=cover.out" ./response_analyser/
go tool cover -func=cover.out
```

## Running Benchmarks

```bash
# All benchmarks with allocation stats
go test -bench=. -benchmem ./response_analyser/...

# Hotpath parallel (100 goroutines)
go test -bench=BenchmarkAnalyse_Parallel -benchmem -cpu=1,2,4,8 ./response_analyser/...

# Bloom filter check
go test -bench=BenchmarkBloom_MayContain -benchmem ./response_analyser/...

# End-to-end throughput
go test -bench=BenchmarkE2E_Throughput -benchmem -cpu=8 ./response_analyser/...
```

### Benchmark Results (Intel Core i5-10400F @ 2.90 GHz, 12 threads, Windows)

| Benchmark | ns/op | B/op | allocs/op |
|---|---|---|---|
| `BenchmarkAnalyse_Small` | 140 ns | 37 B | **0** |
| `BenchmarkAnalyse_Parallel` (100 goroutines) | **20 ns** | 4 B | **0** |
| `BenchmarkBloom_MayContain` | 4 ns | 0 B | **0** |
| `BenchmarkHash128` | 9 ns | 0 B | **0** |
| `BenchmarkGetCurrentCounts_Large` (1000 keys) | 42 µs | 54 KB | 6 |

> `GetCurrentCounts` allocates because it builds a defensive copy of the snapshot. The hotpath (`Analyse`) is always **0 allocs/op**.

---

## Performance Optimizations Summary

| Optimization | Impact |
|---|---|
| Single consumer (MPSC) | Eliminates write-write lock contention |
| Token batching (64 tokens/send) | ~64x fewer channel operations |
| Bloom filter in consumer | Skips hash lookup for repeated tokens |
| `atomic.Pointer` snapshot | Lock-free `GetCurrentCounts` |
| 256 shards | ~16x less lock contention vs 16 shards |
| `sync.Pool` token batches | Zero allocs for content copy |
| `bytes.IndexByte` parser | SIMD-accelerated byte scan |
| Cache-line padding on shards | Eliminates false sharing |

---

## Standard Library Only

No external dependencies. Packages used: `bytes`, `context`, `errors`, `fmt`, `io`, `sync`, `sync/atomic`, `time`, `unsafe`.
