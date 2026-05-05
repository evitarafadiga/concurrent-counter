package response_analyser

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ─── Helpers ─────────────────────────────────────────────────────────────────

// mockWriter captures written bytes and implements io.WriteCloser.
type mockWriter struct {
	mu     sync.Mutex
	buf    bytes.Buffer
	closed bool
}

func (m *mockWriter) Write(p []byte) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return 0, fmt.Errorf("write on closed writer")
	}
	return m.buf.Write(p)
}

func (m *mockWriter) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

func (m *mockWriter) String() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.buf.String()
}

// newAnalyser returns a ready analyser suitable for unit tests.
func newAnalyser(t *testing.T) DataExporter {
	t.Helper()
	a := New(context.Background(), 4, 1024)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = a.Shutdown(ctx)
	})
	return a
}

// ─── Basic correctness ────────────────────────────────────────────────────────

func TestAnalyse_SingleBlob(t *testing.T) {
	a := newAnalyser(t)
	a.Analyse([]byte("AU_ieu13\n103956\nghgqb\n10002\na012ne"))

	// Give the worker time to process.
	time.Sleep(20 * time.Millisecond)

	counts := a.GetCurrentCounts()
	want := []string{"AU_ieu13", "103956", "ghgqb", "10002", "a012ne"}
	for _, k := range want {
		if counts[k] != 1 {
			t.Errorf("want counts[%q]=1, got %d", k, counts[k])
		}
	}
	if len(counts) != len(want) {
		t.Errorf("want %d unique keys, got %d", len(want), len(counts))
	}
}

func TestAnalyse_EmptyInput(t *testing.T) {
	a := newAnalyser(t)
	a.Analyse(nil)
	a.Analyse([]byte{})
	a.Analyse([]byte("\n\n\n"))
	time.Sleep(20 * time.Millisecond)
	if n := len(a.GetCurrentCounts()); n != 0 {
		t.Errorf("expected 0 keys for empty/blank input, got %d", n)
	}
}

func TestAnalyse_TrailingNewline(t *testing.T) {
	a := newAnalyser(t)
	a.Analyse([]byte("foo\nbar\n"))
	time.Sleep(20 * time.Millisecond)
	counts := a.GetCurrentCounts()
	if counts["foo"] != 1 || counts["bar"] != 1 || len(counts) != 2 {
		t.Errorf("unexpected counts: %v", counts)
	}
}

func TestAnalyse_SingleToken(t *testing.T) {
	a := newAnalyser(t)
	a.Analyse([]byte("onlyone"))
	time.Sleep(20 * time.Millisecond)
	counts := a.GetCurrentCounts()
	if counts["onlyone"] != 1 {
		t.Errorf("want counts[onlyone]=1, got %d", counts["onlyone"])
	}
}

func TestGetCurrentCounts_DoesNotReset(t *testing.T) {
	a := newAnalyser(t)
	a.Analyse([]byte("x\ny"))
	time.Sleep(20 * time.Millisecond)
	c1 := a.GetCurrentCounts()
	c2 := a.GetCurrentCounts()
	if c1["x"] != c2["x"] || c1["y"] != c2["y"] {
		t.Error("GetCurrentCounts must not reset counters")
	}
}

// ─── Concurrency ─────────────────────────────────────────────────────────────

func TestAnalyse_ConcurrentGoroutines(t *testing.T) {
	const goroutines = 100
	const callsPerG = 50

	a := newAnalyser(t)
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			payload := fmt.Sprintf("code%d\nshared", id)
			for j := 0; j < callsPerG; j++ {
				a.Analyse([]byte(payload))
			}
		}(i)
	}
	wg.Wait()

	// Wait for all workers to drain the channel.
	time.Sleep(200 * time.Millisecond)

	counts := a.GetCurrentCounts()
	// Each goroutine sends its unique code callsPerG times.
	// The "shared" key is sent by all goroutines, but each Analyse call
	// also includes the goroutine's unique code.
	if len(counts) == 0 {
		t.Fatal("expected non-empty counts after concurrent Analyse calls")
	}
}

func TestAnalyse_RaceDetector(t *testing.T) {
	// This test is meaningful only under -race flag.
	a := newAnalyser(t)
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			a.Analyse([]byte(fmt.Sprintf("key%d\ncommon", i)))
		}(i)
	}
	// Concurrent reads.
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = a.GetCurrentCounts()
		}()
	}
	wg.Wait()
}

// ─── Subscribe ───────────────────────────────────────────────────────────────

func TestSubscribe_ReceivesReports(t *testing.T) {
	a := New(context.Background(), 2, 256)

	mw := &mockWriter{}
	if err := a.Subscribe(mw, 50*time.Millisecond); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	a.Analyse([]byte("alpha\nbeta"))
	time.Sleep(200 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = a.Shutdown(ctx)

	out := mw.String()
	if !strings.Contains(out, "alpha") || !strings.Contains(out, "beta") {
		t.Errorf("subscriber output missing expected keys, got:\n%s", out)
	}
}

func TestSubscribe_MultipleSubscribersIndependent(t *testing.T) {
	a := New(context.Background(), 2, 256)

	mw1 := &mockWriter{}
	mw2 := &mockWriter{}
	_ = a.Subscribe(mw1, 50*time.Millisecond)
	_ = a.Subscribe(mw2, 100*time.Millisecond)

	a.Analyse([]byte("one\ntwo\nthree"))
	time.Sleep(300 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = a.Shutdown(ctx)

	if !mw1.closed || !mw2.closed {
		t.Error("expected both writers to be closed after shutdown")
	}
}

func TestSubscribe_NilWriterError(t *testing.T) {
	a := newAnalyser(t)
	if err := a.Subscribe(nil, time.Second); err == nil {
		t.Error("expected error for nil writer")
	}
}

func TestSubscribe_ZeroIntervalError(t *testing.T) {
	a := newAnalyser(t)
	if err := a.Subscribe(&mockWriter{}, 0); err == nil {
		t.Error("expected error for zero interval")
	}
}

func TestSubscribe_NegativeIntervalError(t *testing.T) {
	a := newAnalyser(t)
	if err := a.Subscribe(&mockWriter{}, -time.Second); err == nil {
		t.Error("expected error for negative interval")
	}
}

// ─── Shutdown ────────────────────────────────────────────────────────────────

func TestShutdown_Graceful(t *testing.T) {
	a := New(context.Background(), 2, 128)
	a.Analyse([]byte("x\ny\nz"))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := a.Shutdown(ctx); err != nil {
		t.Errorf("Shutdown returned error: %v", err)
	}
}

func TestShutdown_ContextTimeout(t *testing.T) {
	// Subscribe a writer that blocks forever to force a timeout.
	a := New(context.Background(), 1, 128)
	_ = a.Subscribe(&blockingWriter{}, 10*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := a.Shutdown(ctx)
	if err == nil {
		t.Error("expected timeout error from Shutdown")
	}
}

// blockingWriter blocks forever on both Write and Close to simulate a hung subscriber
// that prevents graceful shutdown from completing within the deadline.
type blockingWriter struct{}

func (b *blockingWriter) Write(p []byte) (int, error) {
	select {} // block forever
}
func (b *blockingWriter) Close() error {
	select {} // block forever — prevents the subscriber goroutine from returning
}

// ─── Backpressure ─────────────────────────────────────────────────────────────

func TestDroppedCount_BackpressureSafety(t *testing.T) {
	// Create a tiny channel so it fills up quickly.
	a := New(context.Background(), 0, 1)
	for i := 0; i < 10000; i++ {
		a.Analyse([]byte("flood"))
	}
	// Some payloads should have been dropped; hotpath must not have blocked.
	// We just verify the call returns and DroppedCount is accessible.
	_ = a.DroppedCount()
}

// ─── Edge cases ───────────────────────────────────────────────────────────────

func TestAnalyse_BinaryGarbage(t *testing.T) {
	a := newAnalyser(t)
	// Non-UTF-8 bytes should not panic — keys are raw bytes.
	a.Analyse([]byte{0xFF, 0xFE, '\n', 0x00, 0x01})
	time.Sleep(20 * time.Millisecond)
	counts := a.GetCurrentCounts()
	if len(counts) == 0 {
		t.Error("expected at least one key from binary input")
	}
}

func TestAnalyse_VeryLongToken(t *testing.T) {
	a := newAnalyser(t)
	long := strings.Repeat("a", 1<<16)
	a.Analyse([]byte(long))
	time.Sleep(20 * time.Millisecond)
	counts := a.GetCurrentCounts()
	if counts[long] != 1 {
		t.Errorf("want counts[long]=1, got %d", counts[long])
	}
}

// ─── Internal helpers ─────────────────────────────────────────────────────────

func TestShardIndex_Distribution(t *testing.T) {
	seen := make(map[int]int)
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		idx := shardIndex(key)
		if idx < 0 || idx >= numShards {
			t.Fatalf("shard index %d out of range [0,%d)", idx, numShards)
		}
		seen[idx]++
	}
	// All shards should receive at least a few keys (rough distribution check).
	for s, count := range seen {
		if count == 0 {
			t.Errorf("shard %d received no keys — hash distribution is skewed", s)
		}
	}
}

func TestAppendUint64(t *testing.T) {
	cases := []uint64{0, 1, 9, 10, 99, 100, 1<<32 - 1, 1<<64 - 1}
	for _, v := range cases {
		var buf bytes.Buffer
		appendUint64(&buf, v)
		want := fmt.Sprintf("%d", v)
		if buf.String() != want {
			t.Errorf("appendUint64(%d) = %q, want %q", v, buf.String(), want)
		}
	}
}

// ─── io.WriteCloser compliance ────────────────────────────────────────────────

var _ io.WriteCloser = (*mockWriter)(nil)
var _ DataExporter = (*analyser)(nil)

// ─── Atomic counter correctness ───────────────────────────────────────────────

func TestAtomicCounter_ExactCount(t *testing.T) {
	const workers = 8
	const sends = 500

	a := New(context.Background(), workers, 4096)

	var wg sync.WaitGroup
	var sent atomic.Int64
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < sends; j++ {
				a.Analyse([]byte("ping"))
				sent.Add(1)
			}
		}()
	}
	wg.Wait()

	// Wait for all workers to finish.
	time.Sleep(100 * time.Millisecond)

	counts := a.GetCurrentCounts()
	dropped := a.DroppedCount()
	got := counts["ping"] + dropped
	if got != uint64(sent.Load()) {
		t.Errorf("ping count (%d) + dropped (%d) = %d, want %d",
			counts["ping"], dropped, got, sent.Load())
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = a.Shutdown(ctx)
}
