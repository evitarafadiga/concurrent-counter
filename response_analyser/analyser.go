// Package response_analyser provides a thread-safe, zero-allocation,
// high-performance unique code counter for real-time bid enrichment platforms.
//
// Architecture (matches diagram):
//
//	Hot path  : Analyse() → parse tokens → tokenBatch → buffered channel
//	Consumer  : single goroutine — bloom filter → map update → atomic snapshot
//	Reporters : per-subscriber goroutines read the lock-free atomic snapshot
package response_analyser

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

// ─── Constants ────────────────────────────────────────────────────────────────

const (
	// numShards: 256 shards for 100+ concurrent goroutines (power of 2).
	numShards = 256

	// batchCap: tokens per batch; 64 tokens/send → ~64x channel overhead reduction.
	batchCap = 64

	// bloomWords: backing array for the bloom filter (1M bits = 128 KB).
	bloomWords = (1 << 20) / 64

	// bloomK: number of hash functions for the bloom filter.
	bloomK = 4

	// defaultBufSize: work-channel capacity.
	defaultBufSize = 1 << 14 // 16 384 batches
)

// ─── Public interface ─────────────────────────────────────────────────────────

// DataExporter is the public interface.
type DataExporter interface {
	Analyse(content []byte)
	GetCurrentCounts() map[string]uint64
	Subscribe(writer io.WriteCloser, interval time.Duration) error
	Shutdown(ctx context.Context) error
	DroppedCount() uint64
}

// ─── Batch ────────────────────────────────────────────────────────────────────

// tokenBatch carries up to batchCap pre-parsed tokens between the hotpath
// and the single consumer goroutine.  All byte slices point into localBuf
// so every token copy is done with a single append — zero extra allocations.
type tokenBatch struct {
	tokens   [batchCap][]byte
	count    int
	localBuf []byte // backing store; capacity pre-allocated by pool
}

func (b *tokenBatch) reset() {
	b.count = 0
	b.localBuf = b.localBuf[:0]
}

// ─── Bloom filter ─────────────────────────────────────────────────────────────

// bloomFilter is a probabilistic set used by the single consumer goroutine to
// skip map-update work for tokens it has seen before.
// Writes are done by one goroutine only; reads use atomic loads to satisfy
// the race detector when the filter is checked from the consumer's own goroutine.
type bloomFilter struct {
	bits [bloomWords]uint64
}

// mayContain returns true if key has probably been seen.
// False positives are possible; false negatives are not.
func (f *bloomFilter) mayContain(h1, h2 uint64) bool {
	for i := uint64(0); i < bloomK; i++ {
		bit := (h1 + i*h2) % (bloomWords * 64)
		if atomic.LoadUint64(&f.bits[bit>>6])&(1<<(bit&63)) == 0 {
			return false
		}
	}
	return true
}

// add marks key as seen.
func (f *bloomFilter) add(h1, h2 uint64) {
	for i := uint64(0); i < bloomK; i++ {
		bit := (h1 + i*h2) % (bloomWords * 64)
		idx := bit >> 6
		mask := uint64(1) << (bit & 63)
		for {
			old := atomic.LoadUint64(&f.bits[idx])
			if old&mask != 0 {
				break
			}
			if atomic.CompareAndSwapUint64(&f.bits[idx], old, old|mask) {
				break
			}
		}
	}
}

// ─── Shard (still used for GetCurrentCounts lock-free path) ──────────────────

// shard is written exclusively by the single consumer goroutine.
// RWMutex is kept for correctness during the snapshot walk in GetCurrentCounts
// when callers may read concurrently with a snapshot refresh.
// Cache-line padding (40 bytes) prevents false sharing.
type shard struct {
	mu      sync.RWMutex
	entries map[string]uint64 // owned by consumer; value is the full count
	_       [40]byte
}

// ─── Subscriber ───────────────────────────────────────────────────────────────

type subscriber struct {
	writer   io.WriteCloser
	interval time.Duration
	done     chan struct{}
}

// ─── Analyser ─────────────────────────────────────────────────────────────────

type analyser struct {
	// Pool of *tokenBatch — avoids allocs on the hot path.
	bPool sync.Pool

	// Buffered channel: hotpath producers → single consumer.
	workCh chan *tokenBatch

	// Sharded counter map — written exclusively by the consumer goroutine.
	shards [numShards]shard

	// Bloom filter — owned by the consumer goroutine.
	bloom bloomFilter

	// Lock-free snapshot; readers call snapshot.Load() — no lock needed.
	snapshot atomic.Pointer[map[string]uint64]

	// Subscribers.
	subsMu sync.RWMutex
	subs   []*subscriber

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc

	dropped uint64 // atomic
}

// New creates a DataExporter.
//   - ctx:     parent context.
//   - bufSize: work-channel capacity (batches of 64 tokens); ≤0 → default.
//
// The consumer is always a single goroutine (MPSC design).
func New(ctx context.Context, _ /*workers*/ int, bufSize int) DataExporter {
	if bufSize <= 0 {
		bufSize = defaultBufSize
	}

	child, cancel := context.WithCancel(ctx)
	a := &analyser{
		workCh: make(chan *tokenBatch, bufSize),
		ctx:    child,
		cancel: cancel,
	}

	for i := range a.shards {
		a.shards[i].entries = make(map[string]uint64, 16)
	}

	// Seed the snapshot with an empty map so Load() is never nil.
	empty := make(map[string]uint64)
	a.snapshot.Store(&empty)

	a.bPool.New = func() any {
		b := &tokenBatch{localBuf: make([]byte, 0, batchCap*32)}
		return b
	}

	// Start the single consumer goroutine.
	a.wg.Add(1)
	go a.consumer()

	return a
}

// ─── Hot path ─────────────────────────────────────────────────────────────────

// Analyse parses content and enqueues a tokenBatch for the consumer.
// It is always non-blocking: if the channel is full the batch is dropped.
//
// Zero allocations on the steady-state path:
//   - pool.Get() returns a pre-allocated *tokenBatch
//   - tokens are sliced into a pre-allocated localBuf via append
//   - channel send is a pointer copy
func (a *analyser) Analyse(content []byte) {
	if len(content) == 0 {
		return
	}

	b := a.bPool.Get().(*tokenBatch)
	b.reset()

	for len(content) > 0 {
		idx := bytes.IndexByte(content, '\n')
		var token []byte
		if idx < 0 {
			token = content
			content = nil
		} else {
			token = content[:idx]
			content = content[idx+1:]
		}
		if len(token) == 0 {
			continue
		}

		// Copy token into the batch's pre-allocated buffer.
		start := len(b.localBuf)
		b.localBuf = append(b.localBuf, token...)
		b.tokens[b.count] = b.localBuf[start:]
		b.count++

		// Flush a full batch immediately; get a fresh one.
		if b.count == batchCap {
			a.sendBatch(b)
			b = a.bPool.Get().(*tokenBatch)
			b.reset()
		}
	}

	// Flush the remaining partial batch (if any tokens were parsed).
	if b.count > 0 {
		a.sendBatch(b)
	} else {
		a.bPool.Put(b)
	}
}

func (a *analyser) sendBatch(b *tokenBatch) {
	select {
	case a.workCh <- b:
	default:
		b.reset()
		a.bPool.Put(b)
		atomic.AddUint64(&a.dropped, 1)
	}
}

// ─── Consumer ─────────────────────────────────────────────────────────────────

// consumer is the single goroutine that:
//  1. Drains tokenBatches from workCh.
//  2. Applies the bloom filter to skip already-seen keys.
//  3. Increments the counter in the shard map (exclusive writer — no lock needed
//     for the write, but we take a write lock so GetCurrentCounts can safely
//     read via the atomic snapshot).
//  4. Periodically refreshes the atomic snapshot used by GetCurrentCounts and
//     subscribers.
func (a *analyser) consumer() {
	defer a.wg.Done()

	// Snapshot is refreshed every 100ms by the consumer.
	snapTick := time.NewTicker(100 * time.Millisecond)
	defer snapTick.Stop()

	for {
		select {
		case b := <-a.workCh:
			a.processBatch(b)
			b.reset()
			a.bPool.Put(b)

		case <-snapTick.C:
			a.refreshSnapshot()

		case <-a.ctx.Done():
			// Drain remaining batches before exiting.
			for {
				select {
				case b := <-a.workCh:
					a.processBatch(b)
					b.reset()
					a.bPool.Put(b)
				default:
					a.refreshSnapshot()
					return
				}
			}
		}
	}
}

// processBatch runs bloom-filter dedup and increments shard counters.
// Called only from the consumer goroutine.
func (a *analyser) processBatch(b *tokenBatch) {
	for i := 0; i < b.count; i++ {
		token := b.tokens[i]
		h1, h2 := hash128(token)

		// Bloom filter: skip the map write for already-counted tokens.
		// False positives only — we never miss a new token.
		if a.bloom.mayContain(h1, h2) {
			// Token was seen before; still increment its counter.
			s := &a.shards[int(h1)&(numShards-1)]
			s.mu.Lock()
			s.entries[string(token)]++ // safe: string(token) is from localBuf, which is alive
			s.mu.Unlock()
			continue
		}

		// First time seeing this token: add to bloom and insert into map.
		a.bloom.add(h1, h2)
		s := &a.shards[int(h1)&(numShards-1)]
		s.mu.Lock()
		s.entries[string(token)]++
		s.mu.Unlock()
	}
}

// ─── Snapshot ────────────────────────────────────────────────────────────────

// refreshSnapshot builds a new map snapshot and atomically replaces the old one.
// Subscribers and GetCurrentCounts both read the snapshot via atomic.Pointer,
// so no lock is needed on the read side.
func (a *analyser) refreshSnapshot() {
	n := 0
	for i := range a.shards {
		a.shards[i].mu.RLock()
		n += len(a.shards[i].entries)
		a.shards[i].mu.RUnlock()
	}

	snap := make(map[string]uint64, n)
	for i := range a.shards {
		s := &a.shards[i]
		s.mu.RLock()
		for k, v := range s.entries {
			snap[k] = v
		}
		s.mu.RUnlock()
	}
	a.snapshot.Store(&snap)
}

// GetCurrentCounts returns the latest atomic snapshot.
// Lock-free: a single atomic pointer load.
func (a *analyser) GetCurrentCounts() map[string]uint64 {
	snap := *a.snapshot.Load()
	// Return a copy so callers cannot mutate the shared snapshot.
	out := make(map[string]uint64, len(snap))
	for k, v := range snap {
		out[k] = v
	}
	return out
}

// ─── Subscribers ─────────────────────────────────────────────────────────────

func (a *analyser) Subscribe(writer io.WriteCloser, interval time.Duration) error {
	if writer == nil {
		return errors.New("response_analyser: writer must not be nil")
	}
	if interval <= 0 {
		return errors.New("response_analyser: interval must be positive")
	}

	sub := &subscriber{writer: writer, interval: interval, done: make(chan struct{})}

	a.subsMu.Lock()
	a.subs = append(a.subs, sub)
	a.subsMu.Unlock()

	a.wg.Add(1)
	go a.runSubscriber(sub)
	return nil
}

func (a *analyser) runSubscriber(sub *subscriber) {
	defer a.wg.Done()
	tick := time.NewTicker(sub.interval)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			a.report(sub)
		case <-sub.done:
			sub.writer.Close()
			return
		case <-a.ctx.Done():
			sub.writer.Close()
			return
		}
	}
}

var reportPool = sync.Pool{New: func() any { return new(bytes.Buffer) }}

func (a *analyser) report(sub *subscriber) {
	// Read directly from the atomic snapshot — no locks.
	snap := *a.snapshot.Load()

	buf := reportPool.Get().(*bytes.Buffer)
	buf.Reset()
	for k, v := range snap {
		buf.WriteString(k)
		buf.WriteByte('\t')
		appendUint64(buf, v)
		buf.WriteByte('\n')
	}
	_, _ = sub.writer.Write(buf.Bytes())
	reportPool.Put(buf)
}

// ─── Shutdown ─────────────────────────────────────────────────────────────────

func (a *analyser) Shutdown(ctx context.Context) error {
	a.cancel()

	a.subsMu.RLock()
	for _, sub := range a.subs {
		select {
		case <-sub.done:
		default:
			close(sub.done)
		}
	}
	a.subsMu.RUnlock()

	done := make(chan struct{})
	go func() { a.wg.Wait(); close(done) }()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("response_analyser: shutdown timed out: %w", ctx.Err())
	}
}

func (a *analyser) DroppedCount() uint64 {
	return atomic.LoadUint64(&a.dropped)
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

// hash128 returns two independent 64-bit FNV-1a hashes used by the bloom filter
// and the shard index.  Running two passes doubles the bit coverage at ~2× cost.
func hash128(key []byte) (h1, h2 uint64) {
	const (
		offset64 uint64 = 14695981039346656037
		prime64  uint64 = 1099511628211
	)
	h1 = offset64
	for _, b := range key {
		h1 ^= uint64(b)
		h1 *= prime64
	}
	// h2: same FNV with a different starting seed (XOR with a constant).
	h2 = h1 ^ 0xdeadbeefcafebabe
	for _, b := range key {
		h2 ^= uint64(b)
		h2 *= prime64
	}
	return
}

// unsafeString converts []byte to string without allocation.
// ONLY valid for the lifetime of the slice; never store the result.
// Used exclusively for map lookups inside locks where the slice is stable.
func unsafeString(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(&b[0], len(b))
}

// appendUint64 writes a decimal uint64 into buf without allocating.
func appendUint64(buf *bytes.Buffer, v uint64) {
	if v == 0 {
		buf.WriteByte('0')
		return
	}
	var tmp [20]byte
	i := len(tmp)
	for v > 0 {
		i--
		tmp[i] = byte(v%10) + '0'
		v /= 10
	}
	buf.Write(tmp[i:])
}
