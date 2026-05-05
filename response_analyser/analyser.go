// Package response_analyser provides a thread-safe, zero-allocation,
// high-performance unique code counter designed for real-time bid enrichment
// platforms requiring sub-millisecond processing under extreme concurrent load.
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

const (
	// numShards controls lock contention; must be a power of 2.
	numShards = 16

	// defaultWorkers is the number of goroutines draining the work channel.
	defaultWorkers = 8

	// defaultBufSize is the capacity of the buffered work channel.
	// At 1M rps with ~8µs avg processing time ≈ 8000 in-flight at peak.
	defaultBufSize = 1 << 14 // 16 384
)

// DataExporter is the public interface for the analyser.
type DataExporter interface {
	// Analyse is the hotpath entrypoint. Non-blocking; call from every HTTP handler.
	Analyse(content []byte)
	// GetCurrentCounts returns a point-in-time snapshot without resetting counters.
	GetCurrentCounts() map[string]uint64
	// Subscribe registers a writer that receives periodic count reports.
	Subscribe(writer io.WriteCloser, interval time.Duration) error
	// Shutdown gracefully stops all goroutines, respecting the context deadline.
	Shutdown(ctx context.Context) error
	// DroppedCount returns the number of payloads silently dropped under backpressure.
	DroppedCount() uint64
}

// shard holds a subset of the counter map behind its own mutex.
// The 40-byte padding prevents false sharing across 64-byte CPU cache lines.
type shard struct {
	mu      sync.RWMutex
	entries map[string]*uint64
	_       [40]byte // cache-line padding
}

// subscriber is a registered output consumer.
type subscriber struct {
	writer   io.WriteCloser
	interval time.Duration
	done     chan struct{}
}

// analyser implements DataExporter.
type analyser struct {
	shards  [numShards]shard
	pool    sync.Pool    // recycles *[]byte scratch buffers
	workCh  chan *[]byte // buffered channel: hotpath → workers

	subsMu sync.RWMutex
	subs   []*subscriber

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc

	dropped uint64 // atomic counter of payloads dropped under backpressure
}

// New creates a DataExporter.
//   - ctx:     parent context; cancelled on server shutdown.
//   - workers: number of goroutines draining the work channel (≥1).
//   - bufSize: work-channel capacity (≥1).
//
// Recommended production values: workers = runtime.NumCPU(), bufSize = 1<<14.
func New(ctx context.Context, workers, bufSize int) DataExporter {
	if workers <= 0 {
		workers = defaultWorkers
	}
	if bufSize <= 0 {
		bufSize = defaultBufSize
	}

	child, cancel := context.WithCancel(ctx)
	a := &analyser{
		workCh: make(chan *[]byte, bufSize),
		ctx:    child,
		cancel: cancel,
	}

	for i := range a.shards {
		a.shards[i].entries = make(map[string]*uint64, 64)
	}

	a.pool.New = func() any {
		b := make([]byte, 0, 512)
		return &b
	}

	a.wg.Add(workers)
	for i := 0; i < workers; i++ {
		go a.worker()
	}

	return a
}

// ─── Hotpath ─────────────────────────────────────────────────────────────────

// Analyse is the hotpath entrypoint. It copies content into a pooled buffer and
// enqueues it for async processing. The call is always non-blocking: if the
// channel is full the payload is dropped and DroppedCount is incremented.
func (a *analyser) Analyse(content []byte) {
	if len(content) == 0 {
		return
	}

	bp := a.pool.Get().(*[]byte)
	*bp = append((*bp)[:0], content...) // copy into pooled buffer

	select {
	case a.workCh <- bp:
	default:
		// Channel saturated: protect the hotpath by dropping this payload.
		*bp = (*bp)[:0]
		a.pool.Put(bp)
		atomic.AddUint64(&a.dropped, 1)
	}
}

// ─── Workers ─────────────────────────────────────────────────────────────────

// worker drains workCh and calls process for each payload.
// On context cancellation it performs a non-blocking drain before exiting.
func (a *analyser) worker() {
	defer a.wg.Done()
	for {
		select {
		case bp := <-a.workCh:
			a.process(*bp)
			*bp = (*bp)[:0]
			a.pool.Put(bp)

		case <-a.ctx.Done():
			// Drain any remaining buffered payloads before exiting.
			for {
				select {
				case bp := <-a.workCh:
					a.process(*bp)
					*bp = (*bp)[:0]
					a.pool.Put(bp)
				default:
					return
				}
			}
		}
	}
}

// process parses a newline-delimited blob and increments a counter per token.
// Uses bytes.IndexByte (no allocations) instead of strings.Split or bufio.Scanner.
func (a *analyser) process(data []byte) {
	for len(data) > 0 {
		idx := bytes.IndexByte(data, '\n')
		var token []byte
		if idx < 0 {
			token = data
			data = nil
		} else {
			token = data[:idx]
			data = data[idx+1:]
		}
		if len(token) == 0 {
			continue
		}
		a.increment(token)
	}
}

// increment is allocation-free for all previously-seen keys.
//
// The unsafe string conversion lets us do a map lookup with a []byte key
// without copying it to a heap-allocated string first. The resulting string
// is only valid for the duration of the lookup — we never store it.
//
// For new keys a single heap allocation occurs (string(key)) which is
// unavoidable: the map must own a stable copy of the key.
func (a *analyser) increment(key []byte) {
	s := &a.shards[shardIndex(key)]
	k := unsafeString(key) // zero-alloc conversion for the lookup

	// Fast path: key already exists — read-lock only.
	s.mu.RLock()
	ptr, ok := s.entries[k]
	s.mu.RUnlock()
	if ok {
		atomic.AddUint64(ptr, 1)
		return
	}

	// Slow path: new key — upgrade to write lock with double-check.
	s.mu.Lock()
	if ptr, ok = s.entries[k]; ok {
		s.mu.Unlock()
		atomic.AddUint64(ptr, 1)
		return
	}
	v := uint64(1)
	s.entries[string(key)] = &v // one-time allocation per unique key
	s.mu.Unlock()
}

// ─── Reporting ───────────────────────────────────────────────────────────────

// GetCurrentCounts returns a point-in-time snapshot of all counters.
// It acquires each shard's read-lock in sequence and uses atomic loads
// so it never races with concurrent increments.
func (a *analyser) GetCurrentCounts() map[string]uint64 {
	n := 0
	for i := range a.shards {
		a.shards[i].mu.RLock()
		n += len(a.shards[i].entries)
		a.shards[i].mu.RUnlock()
	}
	out := make(map[string]uint64, n)
	for i := range a.shards {
		s := &a.shards[i]
		s.mu.RLock()
		for k, v := range s.entries {
			out[k] = atomic.LoadUint64(v)
		}
		s.mu.RUnlock()
	}
	return out
}

// Subscribe registers a new subscriber. Each subscriber runs in its own
// goroutine so a slow writer cannot affect the hotpath or other subscribers.
func (a *analyser) Subscribe(writer io.WriteCloser, interval time.Duration) error {
	if writer == nil {
		return errors.New("response_analyser: writer must not be nil")
	}
	if interval <= 0 {
		return errors.New("response_analyser: interval must be positive")
	}

	sub := &subscriber{
		writer:   writer,
		interval: interval,
		done:     make(chan struct{}),
	}

	a.subsMu.Lock()
	a.subs = append(a.subs, sub)
	a.subsMu.Unlock()

	a.wg.Add(1)
	go a.runSubscriber(sub)
	return nil
}

// runSubscriber drives periodic reporting for a single subscriber.
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

// reportPool recycles bytes.Buffer instances used during report serialisation.
var reportPool = sync.Pool{New: func() any { return new(bytes.Buffer) }}

// report serialises current counts into the subscriber's writer.
// A pooled bytes.Buffer avoids per-report heap allocations.
func (a *analyser) report(sub *subscriber) {
	counts := a.GetCurrentCounts()
	buf := reportPool.Get().(*bytes.Buffer)
	buf.Reset()
	for k, v := range counts {
		buf.WriteString(k)
		buf.WriteByte('\t')
		appendUint64(buf, v)
		buf.WriteByte('\n')
	}
	if _, err := sub.writer.Write(buf.Bytes()); err != nil {
		// Writer error is non-fatal; the subscriber will be cleaned up on shutdown.
		_ = err
	}
	reportPool.Put(buf)
}

// ─── Shutdown ────────────────────────────────────────────────────────────────

// Shutdown cancels the internal context, signals all subscribers to stop, and
// waits for every goroutine to finish — or until ctx expires.
func (a *analyser) Shutdown(ctx context.Context) error {
	a.cancel()

	// Signal each subscriber to stop (idempotent: skip if already closed).
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

// DroppedCount returns the total number of Analyse payloads dropped under backpressure.
func (a *analyser) DroppedCount() uint64 {
	return atomic.LoadUint64(&a.dropped)
}

// ─── Internal helpers ────────────────────────────────────────────────────────

// shardIndex maps a key to one of the numShards shards using FNV-1a.
// numShards must be a power of 2 so the modulo collapses to a cheap AND.
func shardIndex(key []byte) int {
	return int(fnv32(key)) & (numShards - 1)
}

// fnv32 is an inlined FNV-1a 32-bit hash — small, fast, no imports.
func fnv32(key []byte) uint32 {
	const (
		offset32 uint32 = 2166136261
		prime32  uint32 = 16777619
	)
	h := offset32
	for _, b := range key {
		h ^= uint32(b)
		h *= prime32
	}
	return h
}

// unsafeString converts a []byte to string without a heap allocation.
// The result is only valid while the underlying slice is unmodified.
// Never store the result; use it only for immediate map lookups.
func unsafeString(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(&b[0], len(b))
}

// appendUint64 writes a decimal representation of v into buf without allocating.
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
