# concurrent-counter

A **thread-safe, zero-allocation, ultra-fast** unique code counter for real-time bid-enrichment platforms — built entirely with the Go standard library.

---

## Problem Summary

An HTTP server returns blobs of `\n`-delimited unique codes. This package counts how many of each unique code has been returned **without adding latency to the hotpath**, handles **100+ concurrent goroutines**, and periodically reports counts to multiple subscribers.

---

## Architecture & Design Decisions

### 1. Non-Blocking Hotpath (`Analyse`)

`Analyse(content []byte)` is designed to be called from every HTTP handler. It must never block or meaningfully slow the caller.

**Decision:** fire-and-forget via a **buffered channel** (`workCh chan *[]byte`).

```
HTTP handler
    │
    └─► Analyse()  ──(non-blocking send)──► workCh  ──► worker pool  ──► sharded counter
```

If `workCh` is full (consumer can't keep up), the payload is **silently dropped** and `DroppedCount` is incremented. This is the correct trade-off for a 7 ms SLA: never stall the request path for bookkeeping.

### 2. Zero-Allocation Hot Path

| Technique | Allocation saved |
|---|---|
| `sync.Pool` of `*[]byte` | Content copy reuses pooled buffers |
| `bytes.IndexByte` loop parser | No `strings.Split`, no `bufio.Scanner` |
| `unsafe.String(&b[0], len(b))` | Map lookup without a heap `string` copy |
| `atomic.AddUint64` | Lock-free counter update for existing keys |
| Pooled `bytes.Buffer` in `report()` | No per-report allocation |

**For already-seen keys the allocation count is 0 on both the Analyse path and the increment path.** New keys pay a one-time `string(key)` allocation — unavoidable since the map must own a stable copy.

### 3. Sharded Counter Map

A single `map` with a `sync.RWMutex` would serialize all writers. Instead we use **16 shards** (a power-of-2 for cheap modulo):

```
increment(key)
    │
    └─► FNV-1a(key) & 0xF  →  shard[i]
                                  │
                         shard.mu.RLock()  ──► atomic.AddUint64 (fast path)
                         shard.mu.Lock()   ──► map insert (slow path, once per key)
```

- **RLock** for reads (concurrent reads don't block each other)
- **Lock** only on first-seen key insertion, with a double-checked locking pattern
- **`atomic.AddUint64`** for counter updates — no need to hold the lock

### 4. Independent Subscribers

Each `Subscribe(writer, interval)` call spawns one goroutine with its own `time.Ticker`. A slow or hung writer cannot affect the hotpath, other subscribers, or the worker pool.

### 5. Graceful Shutdown

`Shutdown(ctx)` cancels the internal context and signals all subscriber goroutines. Workers perform a **non-blocking drain** of any remaining channel items before exiting. `sync.WaitGroup` ensures all goroutines finish before the function returns. The context deadline is respected.

### 6. Cache-Line Padding on Shards

Each `shard` struct has 40 bytes of padding so that no two shards share a CPU cache line. Without this, threads updating adjacent shards would cause **false sharing** — invisible cache-invalidation traffic between cores.

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
# Unit tests
go test ./response_analyser/...

# With race detector (strongly recommended)
go test -race ./response_analyser/...

# With coverage report
go test -race -coverprofile=cover.out ./response_analyser/...
go tool cover -html=cover.out
```

## Running Benchmarks

```bash
# All benchmarks with allocation stats
go test -bench=. -benchmem ./response_analyser/...

# Parallel hotpath benchmark (100 goroutines)
go test -bench=BenchmarkAnalyse_Parallel -benchmem -cpu=1,2,4,8 ./response_analyser/...

# Zero-alloc verification
go test -bench=BenchmarkAnalyse_ZeroAlloc -benchmem ./response_analyser/...

# End-to-end throughput
go test -bench=BenchmarkE2E_Throughput -benchmem -cpu=8 ./response_analyser/...
```

### Expected benchmark output (Apple M2, 8 cores — for reference)

```
BenchmarkAnalyse_Small-8          10000000   ~110 ns/op   0 allocs/op
BenchmarkAnalyse_Parallel-8       20000000    ~60 ns/op   0 allocs/op
BenchmarkProcess_Small-8          30000000    ~40 ns/op   0 allocs/op
BenchmarkIncrement_Existing-8    100000000    ~12 ns/op   0 allocs/op
BenchmarkFNV32-8                 300000000     ~4 ns/op   0 allocs/op
```

---

## Performance Optimizations Summary

| Optimization | Impact |
|---|---|
| Buffered channel (fire-and-forget) | Hotpath O(1), never blocks |
| 16-shard map | 16× less lock contention vs single map |
| `atomic.AddUint64` for existing keys | Lock-free fast path |
| `sync.Pool` byte buffers | Eliminates GC pressure from content copies |
| `unsafe.String` for map lookup | 0-alloc key comparison |
| `bytes.IndexByte` parser | SIMD-accelerated byte scan via stdlib |
| Cache-line padding on shards | Eliminates false sharing across cores |
| Pooled `bytes.Buffer` in report | 0-alloc subscriber serialisation |

---

## Standard Library Only

No external dependencies. Only packages used: `bytes`, `context`, `errors`, `fmt`, `io`, `sync`, `sync/atomic`, `time`, `unsafe`.
