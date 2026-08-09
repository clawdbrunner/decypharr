package reader

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"

	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/internal/config"
)

// TestMain points the process-wide config.Get() singleton at a throwaway
// directory before any test runs. usenetBufferPool() (invoked from
// NewSegmentCache) lazily calls config.Get(), which otherwise tries to
// read/create a config.json relative to the process's working directory and
// os.Exit(1)s on failure.
func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "reader-test-config-*")
	if err != nil {
		panic(err)
	}
	config.SetConfigPath(dir)
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

func newTestSegmentCache(t *testing.T, segCount int) *SegmentCache {
	t.Helper()
	segments := make([]SegmentMeta, segCount)
	for i := range segments {
		segments[i] = SegmentMeta{
			MessageID: "<test@example.com>",
			Number:    i + 1,
			Bytes:     1024,
		}
	}
	cache, err := NewSegmentCache(context.Background(), segments, DefaultConfig(), &ReaderStats{}, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewSegmentCache: %v", err)
	}
	t.Cleanup(func() { _ = cache.Close() })
	return cache
}

// TestMarkFailedThenResetFailedNetsToZero documents the mechanism prefetchOne's
// retry loop (via fetchWithRetry) now relies on: a transient failure recorded
// by MarkFailed must be fully undone by ResetFailed between retry attempts,
// leaving no trace in either the segment state or the failed-segment counter
// that feeds StreamingReader.checkFailedThreshold.
func TestMarkFailedThenResetFailedNetsToZero(t *testing.T) {
	cache := newTestSegmentCache(t, 4)
	segIdx := 2

	if got := cache.GetState(segIdx); got != StateEmpty {
		t.Fatalf("initial state = %v, want Empty", got)
	}
	if got := cache.FailedSegmentCount(); got != 0 {
		t.Fatalf("initial FailedSegmentCount = %d, want 0", got)
	}

	// Simulate a transient failure, as doFetch does on a non-cancellation error.
	transientErr := errors.New("connection reset by peer")
	cache.MarkFailed(segIdx, transientErr)

	if got := cache.GetState(segIdx); got != StateFailed {
		t.Fatalf("state after MarkFailed = %v, want Failed", got)
	}
	if got := cache.FailedSegmentCount(); got != 1 {
		t.Fatalf("FailedSegmentCount after MarkFailed = %d, want 1", got)
	}
	if err := cache.GetError(segIdx); !errors.Is(err, transientErr) {
		t.Fatalf("GetError after MarkFailed = %v, want %v", err, transientErr)
	}

	// fetchWithRetry calls ResetFailed before each retry attempt.
	cache.ResetFailed(segIdx)

	if got := cache.GetState(segIdx); got != StateEmpty {
		t.Fatalf("state after ResetFailed = %v, want Empty", got)
	}
	if got := cache.FailedSegmentCount(); got != 0 {
		t.Fatalf("FailedSegmentCount after ResetFailed = %d, want 0 (must net out, not leak into the broken-file threshold)", got)
	}
	if err := cache.GetError(segIdx); err != nil {
		t.Fatalf("GetError after ResetFailed = %v, want nil", err)
	}
}

// TestResetFailedIsNoOpWhenNotFailed guards the CAS semantics ResetFailed's
// doc comment describes: it must not touch (or double-decrement the counter
// for) a segment that isn't currently StateFailed, e.g. one a concurrent
// fetch already moved to OnDisk.
func TestResetFailedIsNoOpWhenNotFailed(t *testing.T) {
	cache := newTestSegmentCache(t, 4)
	segIdx := 1

	cache.SetState(segIdx, StateOnDisk)
	cache.ResetFailed(segIdx)

	if got := cache.GetState(segIdx); got != StateOnDisk {
		t.Fatalf("state after no-op ResetFailed = %v, want OnDisk", got)
	}
	if got := cache.FailedSegmentCount(); got != 0 {
		t.Fatalf("FailedSegmentCount after no-op ResetFailed = %d, want 0", got)
	}
}

// TestMarkFailedResetFailedMultipleSegmentsCounterAccuracy exercises the
// counter across several segments to make sure MarkFailed/ResetFailed keep
// failedSegmentCount accurate independently per segment, which is what
// StreamingReader.checkFailedThreshold compares against maxFailedSegments.
func TestMarkFailedResetFailedMultipleSegmentsCounterAccuracy(t *testing.T) {
	cache := newTestSegmentCache(t, 5)

	cache.MarkFailed(0, errors.New("boom"))
	cache.MarkFailed(1, errors.New("boom"))
	cache.MarkFailed(2, errors.New("boom"))
	if got := cache.FailedSegmentCount(); got != 3 {
		t.Fatalf("FailedSegmentCount after 3 MarkFailed = %d, want 3", got)
	}

	// Retry succeeds for segment 1: fetchWithRetry resets it before re-fetch.
	cache.ResetFailed(1)
	if got := cache.FailedSegmentCount(); got != 2 {
		t.Fatalf("FailedSegmentCount after 1 ResetFailed = %d, want 2", got)
	}
	if got := cache.GetState(1); got != StateEmpty {
		t.Fatalf("segment 1 state = %v, want Empty", got)
	}

	// Segments 0 and 2 remain failed (retries exhausted).
	if got := cache.GetState(0); got != StateFailed {
		t.Fatalf("segment 0 state = %v, want Failed", got)
	}
	if got := cache.GetState(2); got != StateFailed {
		t.Fatalf("segment 2 state = %v, want Failed", got)
	}
}

// TestEffectiveFailedSegmentCount exercises the retryingCount exclusion that
// checkFailedThreshold now relies on: a segment that is MarkFailed'd but also
// MarkRetrying'd (i.e. fetchWithRetry has it bracketed for an imminent retry)
// must not count toward the effective total, since it's expected to self-heal
// via the next iteration's ResetFailed before any concurrent reader's
// threshold check should treat it as permanently broken.
func TestEffectiveFailedSegmentCount(t *testing.T) {
	cache := newTestSegmentCache(t, 4)
	segIdx := 1

	// (i) No MarkRetrying calls: Effective equals raw.
	if got := cache.EffectiveFailedSegmentCount(); got != 0 {
		t.Fatalf("initial EffectiveFailedSegmentCount = %d, want 0", got)
	}
	cache.MarkFailed(segIdx, errors.New("boom"))
	if got, want := cache.EffectiveFailedSegmentCount(), cache.FailedSegmentCount(); got != want {
		t.Fatalf("EffectiveFailedSegmentCount = %d, want %d (== FailedSegmentCount, no retrying in flight)", got, want)
	}

	// (ii) MarkRetrying on the same segment excludes it from the effective count.
	cache.MarkRetrying(segIdx)
	if got := cache.EffectiveFailedSegmentCount(); got != 0 {
		t.Fatalf("EffectiveFailedSegmentCount after MarkRetrying = %d, want 0", got)
	}
	if got := cache.FailedSegmentCount(); got != 1 {
		t.Fatalf("raw FailedSegmentCount after MarkRetrying = %d, want 1 (unaffected)", got)
	}

	// (iii) ClearRetrying restores the effective count to match raw.
	cache.ClearRetrying(segIdx)
	if got, want := cache.EffectiveFailedSegmentCount(), cache.FailedSegmentCount(); got != want {
		t.Fatalf("EffectiveFailedSegmentCount after ClearRetrying = %d, want %d", got, want)
	}

	// (iv) Unbalanced Mark/ClearRetrying calls on one segment clamp that
	// segment's own retry state (any positive count excludes it), without
	// affecting any other segment's effective count.
	cache.MarkRetrying(segIdx)
	cache.MarkRetrying(segIdx)
	cache.MarkRetrying(segIdx)
	if got := cache.EffectiveFailedSegmentCount(); got != 0 {
		t.Fatalf("EffectiveFailedSegmentCount with retryingCount > 0 on the only failed segment = %d, want 0", got)
	}

	otherSeg := 2
	cache.MarkFailed(otherSeg, errors.New("genuinely broken"))
	if got := cache.EffectiveFailedSegmentCount(); got != 1 {
		t.Fatalf("EffectiveFailedSegmentCount = %d, want 1 (segment %d unaffected by segment %d's unbalanced retry marks)", got, otherSeg, segIdx)
	}
}

// TestEffectiveFailedSegmentCountPerSegmentIsolation is the direct regression
// test for the round-3 QA finding: retryingCount must be tracked per segment,
// not as a single global counter. A single genuinely-and-permanently-failed
// segment (MarkFailed, no retry in flight for it) must still be counted by
// EffectiveFailedSegmentCount even while several completely unrelated
// segments are concurrently MarkRetrying'd (never failed). With a global
// counter, those unrelated in-flight retries would push the shared
// retryingCount above zero and suppress the genuinely-failed segment's count
// too, defeating the broken-file threshold entirely.
func TestEffectiveFailedSegmentCountPerSegmentIsolation(t *testing.T) {
	cache := newTestSegmentCache(t, 8)
	const brokenSeg = 0

	// Segment 0 is genuinely, permanently failed. No retry is ever marked for it.
	cache.MarkFailed(brokenSeg, errors.New("permanently gone"))
	if got := cache.EffectiveFailedSegmentCount(); got != 1 {
		t.Fatalf("EffectiveFailedSegmentCount before unrelated retries = %d, want 1", got)
	}

	// Several unrelated, healthy segments have in-flight retries concurrently.
	// None of them are failed; this mirrors fetcher.go:428-431 marking
	// MarkRetrying before essentially every non-final fetch attempt.
	unrelated := []int{1, 2, 3, 4, 5, 6}
	var wg sync.WaitGroup
	start := make(chan struct{})
	for _, idx := range unrelated {
		idx := idx
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < 50; i++ {
				cache.MarkRetrying(idx)
				// The genuinely-failed segment's count must survive
				// concurrent unrelated retry churn at every point, not just
				// after everything settles.
				if got := cache.EffectiveFailedSegmentCount(); got != 1 {
					t.Errorf("EffectiveFailedSegmentCount during unrelated retry on segment %d = %d, want 1 (segment %d is unaffected)", idx, got, brokenSeg)
				}
				cache.ClearRetrying(idx)
			}
		}()
	}
	close(start)
	wg.Wait()

	if got := cache.EffectiveFailedSegmentCount(); got != 1 {
		t.Fatalf("EffectiveFailedSegmentCount after unrelated retries settle = %d, want 1", got)
	}

	// Sanity: the unrelated segments themselves never became Failed and
	// their retry counters netted back to zero.
	for _, idx := range unrelated {
		if got := cache.GetState(idx); got != StateEmpty {
			t.Fatalf("segment %d state = %v, want Empty (never failed)", idx, got)
		}
	}
}

// TestEffectiveFailedSegmentCountExcludesConcurrentRetries simulates the QA
// race scenario directly: N segments all fail near-simultaneously (as if N
// background prefetch workers hit the same transient provider blip together)
// and are all marked retrying before their next attempt. A concurrent
// checkFailedThreshold reading EffectiveFailedSegmentCount must see 0, not N,
// even though raw FailedSegmentCount is N and would have tripped a low
// threshold and permanently latched the file broken.
func TestEffectiveFailedSegmentCountExcludesConcurrentRetries(t *testing.T) {
	cache := newTestSegmentCache(t, 5)
	const threshold = 2
	const n = 3 // n > threshold: would falsely trip the old FailedSegmentCount check

	for i := 0; i < n; i++ {
		cache.MarkFailed(i, errors.New("transient blip"))
		cache.MarkRetrying(i)
	}

	if got := cache.FailedSegmentCount(); got != n {
		t.Fatalf("raw FailedSegmentCount = %d, want %d", got, n)
	}
	if got := cache.FailedSegmentCount(); int(got) <= threshold {
		t.Fatalf("test setup invalid: raw FailedSegmentCount %d must exceed threshold %d", got, threshold)
	}

	if got := cache.EffectiveFailedSegmentCount(); got != 0 {
		t.Fatalf("EffectiveFailedSegmentCount during concurrent retries = %d, want 0 (all mid-retry, none genuinely broken)", got)
	}
}
