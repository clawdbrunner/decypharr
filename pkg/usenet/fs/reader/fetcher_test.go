package reader

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/internal/nntp"
)

// newTestSegmentFetcher builds a SegmentFetcher wired to a fresh SegmentCache.
// The NNTP client is deliberately nil: every test overrides sf.attemptFetch,
// the seam introduced specifically so fetchWithRetry/doFetch's dedup, retry,
// and state-machine logic can be exercised without a live NNTP connection (no
// NNTP test double exists anywhere in this repo).
func newTestSegmentFetcher(t *testing.T, segCount int, cfg Config) (*SegmentFetcher, *SegmentCache) {
	t.Helper()
	cache := newTestSegmentCache(t, segCount)
	sf := NewSegmentFetcher(context.Background(), nil, cache, cfg, &ReaderStats{}, zerolog.Nop())
	t.Cleanup(sf.Close)
	return sf, cache
}

func retryConfig(maxRetries int, retryDelay time.Duration) Config {
	cfg := DefaultConfig()
	cfg.MaxRetries = maxRetries
	cfg.RetryDelay = retryDelay
	return cfg
}

// TestFetchWithRetry_MarkBeforeFetchOrdering proves fetchWithRetry marks a
// segment retrying BEFORE the attempt runs, per its own doc comment: "Mark
// that BEFORE calling Fetch ... so there'd be no window between MarkFailed
// and that mark where a concurrent reader could observe the transient bump."
// The window between doFetch's MarkFailed and the next loop iteration's
// ResetFailed/ClearRetrying is real but lasts only a few instructions (no
// sleep in between), so it can't reliably be caught by polling from another
// goroutine. Instead this probes it deterministically from inside the
// attempt itself: since MarkRetrying(segIdx) must already have run by the
// time attemptFetch is invoked, marking the segment Failed right there (a
// stand-in for the real MarkFailed doFetch is about to issue after we
// return) must already be excluded by EffectiveFailedSegmentCount.
func TestFetchWithRetry_MarkBeforeFetchOrdering(t *testing.T) {
	sf, cache := newTestSegmentFetcher(t, 4, retryConfig(2, 10*time.Millisecond))
	const segIdx = 1

	var calls atomic.Int32
	var probeEffective int32 = -1
	sf.attemptFetch = func(ctx context.Context, idx int, seg *SegmentMeta) error {
		if calls.Add(1) == 1 {
			cache.MarkFailed(idx, errors.New("probe"))
			probeEffective = cache.EffectiveFailedSegmentCount()
			cache.ResetFailed(idx) // undo the probe; doFetch issues the real MarkFailed itself
			return errors.New("transient blip")
		}
		return cache.Put(idx, []byte("ok"))
	}

	if err := sf.fetchWithRetry(context.Background(), segIdx); err != nil {
		t.Fatalf("fetchWithRetry returned %v, want nil (second attempt should succeed)", err)
	}
	if probeEffective != 0 {
		t.Fatalf("EffectiveFailedSegmentCount during first attempt (post-probe MarkFailed) = %d, want 0 (retryingCount must already be elevated before the attempt runs)", probeEffective)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("attemptFetch called %d times, want 2", got)
	}
	if got := cache.GetState(segIdx); got != StateOnDisk {
		t.Fatalf("final state = %v, want OnDisk", got)
	}
	if got := cache.EffectiveFailedSegmentCount(); got != 0 {
		t.Fatalf("EffectiveFailedSegmentCount after success = %d, want 0", got)
	}
}

// TestFetchWithRetry_CancellationExitPath verifies that a context cancellation
// observed mid-attempt short-circuits the retry loop, returns ctx.Err(), and
// leaves the segment's retry bookkeeping clean (no leaked retryingCount, no
// leftover Failed state — ReleaseFetching, not MarkFailed, handles
// cancellations).
func TestFetchWithRetry_CancellationExitPath(t *testing.T) {
	sf, cache := newTestSegmentFetcher(t, 4, retryConfig(3, 10*time.Millisecond))
	const segIdx = 2

	ctx, cancel := context.WithCancel(context.Background())
	attemptStarted := make(chan struct{})
	sf.attemptFetch = func(ctx context.Context, idx int, seg *SegmentMeta) error {
		close(attemptStarted)
		<-ctx.Done()
		return ctx.Err()
	}

	errCh := make(chan error, 1)
	go func() { errCh <- sf.fetchWithRetry(ctx, segIdx) }()

	<-attemptStarted
	cancel()

	err := <-errCh
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("fetchWithRetry returned %v, want context.Canceled", err)
	}
	if got := cache.GetState(segIdx); got != StateEmpty {
		t.Fatalf("state after cancellation = %v, want Empty (ReleaseFetching, not MarkFailed)", got)
	}
	if got := cache.EffectiveFailedSegmentCount(); got != 0 {
		t.Fatalf("EffectiveFailedSegmentCount after cancellation = %d, want 0", got)
	}

	// The retry counter must have netted back to exactly not-retrying: marking
	// this segment Failed afterward must make it count immediately.
	cache.MarkFailed(segIdx, errors.New("separately broken"))
	if got := cache.EffectiveFailedSegmentCount(); got != 1 {
		t.Fatalf("EffectiveFailedSegmentCount after a fresh MarkFailed = %d, want 1 (retryingCount must have been cleared, not leaked)", got)
	}
}

// TestFetchWithRetry_PermanentErrorShortCircuits verifies that
// nntp.IsArticleNotFoundError errors are not retried at all: fetchWithRetry
// must return on the first attempt, and MarkRetrying's bracket must have been
// cleared so the segment counts immediately toward EffectiveFailedSegmentCount.
func TestFetchWithRetry_PermanentErrorShortCircuits(t *testing.T) {
	sf, cache := newTestSegmentFetcher(t, 4, retryConfig(3, 10*time.Millisecond))
	const segIdx = 3

	var calls atomic.Int32
	permErr := &nntp.Error{Type: nntp.ErrorTypeArticleNotFound, Message: "no such article"}
	sf.attemptFetch = func(ctx context.Context, idx int, seg *SegmentMeta) error {
		calls.Add(1)
		return permErr
	}

	err := sf.fetchWithRetry(context.Background(), segIdx)
	if !nntp.IsArticleNotFoundError(err) {
		t.Fatalf("fetchWithRetry returned %v, want an article-not-found error", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("attemptFetch called %d times, want 1 (permanent error must not retry)", got)
	}
	if got := cache.GetState(segIdx); got != StateFailed {
		t.Fatalf("state = %v, want Failed", got)
	}
	if got := cache.EffectiveFailedSegmentCount(); got != 1 {
		t.Fatalf("EffectiveFailedSegmentCount = %d, want 1 (retrying bracket must be cleared on the permanent-error exit)", got)
	}
}

// TestFetchWithRetry_ConcurrentCallersSameSegment races many goroutines
// calling fetchWithRetry for the identical segment. Fetch's inFlight
// promise/dedup path must ensure exactly one physical attempt runs, and the
// per-segment retryingCount bookkeeping must not corrupt under concurrent
// access (run with -race).
func TestFetchWithRetry_ConcurrentCallersSameSegment(t *testing.T) {
	sf, cache := newTestSegmentFetcher(t, 4, retryConfig(3, 10*time.Millisecond))
	const segIdx = 0

	var calls atomic.Int32
	sf.attemptFetch = func(ctx context.Context, idx int, seg *SegmentMeta) error {
		calls.Add(1)
		time.Sleep(10 * time.Millisecond)
		return cache.Put(idx, []byte("data"))
	}

	const n = 20
	var wg sync.WaitGroup
	errs := make([]error, n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			errs[i] = sf.fetchWithRetry(context.Background(), segIdx)
		}()
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("caller %d: fetchWithRetry returned %v, want nil", i, err)
		}
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("attemptFetch called %d times, want 1 (Fetch's dedup should collapse concurrent callers)", got)
	}
	if got := cache.GetState(segIdx); got != StateOnDisk {
		t.Fatalf("final state = %v, want OnDisk", got)
	}
	if got := cache.EffectiveFailedSegmentCount(); got != 0 {
		t.Fatalf("EffectiveFailedSegmentCount = %d, want 0", got)
	}
}

// TestRetryBackoff_ExponentialWithCap documents the backoff fetchWithRetry
// already applies between retry attempts for the same segment: it doubles
// from RetryDelay (default 1s) and caps at 5s. This exists so a poison
// segment's retries within a single reader session don't hammer the NNTP
// provider back-to-back — see fetchWithRetry's time.After(retryBackoff(...))
// call. No jitter is added on top: this is a pure function of `attempt`, and
// per-provider connection-pool contention across concurrent readers already
// staggers actual send times in practice.
func TestRetryBackoff_ExponentialWithCap(t *testing.T) {
	sf, _ := newTestSegmentFetcher(t, 1, retryConfig(3, 200*time.Millisecond))

	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{1, 200 * time.Millisecond},
		{2, 400 * time.Millisecond},
		{3, 800 * time.Millisecond},
		{10, 5 * time.Second}, // capped
	}
	for _, c := range cases {
		if got := sf.retryBackoff(c.attempt); got != c.want {
			t.Errorf("retryBackoff(%d) = %v, want %v", c.attempt, got, c.want)
		}
	}
}

// TestRetryBackoff_DefaultsWhenRetryDelayUnset confirms the fallback base of
// 1s is used when Config.RetryDelay is zero (unset).
func TestRetryBackoff_DefaultsWhenRetryDelayUnset(t *testing.T) {
	sf, _ := newTestSegmentFetcher(t, 1, retryConfig(3, 0))

	if got, want := sf.retryBackoff(1), time.Second; got != want {
		t.Errorf("retryBackoff(1) with unset RetryDelay = %v, want %v", got, want)
	}
}

// TestFetchWithRetry_UnrelatedRetriesDontPerturbEffectiveCount is the
// fetcher-level counterpart of the cache-level regression test: one segment
// is driven to genuine permanent failure through the real fetchWithRetry path
// while several other segments are concurrently retrying (and eventually
// succeeding) through their own real fetchWithRetry calls. The permanently
// failed segment's contribution to EffectiveFailedSegmentCount must hold
// steady at 1 regardless of the unrelated concurrent retry activity.
func TestFetchWithRetry_UnrelatedRetriesDontPerturbEffectiveCount(t *testing.T) {
	sf, cache := newTestSegmentFetcher(t, 8, retryConfig(3, 15*time.Millisecond))
	const brokenSeg = 0
	unrelated := []int{1, 2, 3, 4, 5, 6, 7}

	permErr := &nntp.Error{Type: nntp.ErrorTypeArticleNotFound, Message: "gone"}
	var attemptCounts [8]atomic.Int32
	sf.attemptFetch = func(ctx context.Context, idx int, seg *SegmentMeta) error {
		n := attemptCounts[idx].Add(1)
		if idx == brokenSeg {
			return permErr
		}
		// Unrelated segments fail transiently once, then succeed.
		if n < 2 {
			return errors.New("transient")
		}
		return cache.Put(idx, []byte("ok"))
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := sf.fetchWithRetry(context.Background(), brokenSeg)
		if !nntp.IsArticleNotFoundError(err) {
			t.Errorf("broken segment fetchWithRetry returned %v, want article-not-found", err)
		}
	}()

	for _, idx := range unrelated {
		idx := idx
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := sf.fetchWithRetry(context.Background(), idx); err != nil {
				t.Errorf("segment %d fetchWithRetry returned %v, want nil", idx, err)
			}
		}()
	}
	wg.Wait()

	if got := cache.GetState(brokenSeg); got != StateFailed {
		t.Fatalf("broken segment state = %v, want Failed", got)
	}
	if got := cache.EffectiveFailedSegmentCount(); got != 1 {
		t.Fatalf("EffectiveFailedSegmentCount after unrelated concurrent retries = %d, want 1", got)
	}
	for _, idx := range unrelated {
		if got := cache.GetState(idx); got != StateOnDisk {
			t.Fatalf("unrelated segment %d state = %v, want OnDisk", idx, got)
		}
	}
}
