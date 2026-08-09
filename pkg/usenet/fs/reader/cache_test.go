package reader

import (
	"context"
	"errors"
	"os"
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
