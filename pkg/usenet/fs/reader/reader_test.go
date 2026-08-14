package reader

import (
	"context"
	"errors"
	"testing"

	"github.com/rs/zerolog"
)

// newTestStreamingReader builds a StreamingReader directly around a fresh
// SegmentCache, bypassing NewStreamingReader (which requires a live
// *nntp.Client). checkFailedThreshold and ReadAtContext's broken-latching
// only touch sr.cache/sr.maxFailedSegments/sr.broken/sr.logger, so no fetcher
// or NNTP client is needed for these tests.
func newTestStreamingReader(t *testing.T, segCount, maxFailedSegments int) (*StreamingReader, *SegmentCache) {
	t.Helper()
	cache := newTestSegmentCache(t, segCount)
	sr := &StreamingReader{
		cache:             cache,
		totalSize:         cache.TotalSize(),
		segCount:          cache.SegmentCount(),
		maxFailedSegments: maxFailedSegments,
		logger:            zerolog.Nop(),
		stats:             &ReaderStats{},
	}
	return sr, cache
}

// newTestStreamingReaderWithFetcher builds a StreamingReader wired the same
// way NewStreamingReader wires one — including a real SegmentFetcher with
// fetcher.brokenCheck pointed at sr.registryBrokenErr — but bypasses the
// *nntp.Client requirement by overriding attemptFetch, the same seam
// fetcher_test.go's newTestSegmentFetcher uses. Needed for tests that must
// exercise the full ReadAtContext -> readAtPlain -> EnsureSegments path
// rather than just the cache/threshold-only helper above.
func newTestStreamingReaderWithFetcher(t *testing.T, segCount, maxFailedSegments int) (*StreamingReader, *SegmentCache, *SegmentFetcher) {
	t.Helper()
	cache := newTestSegmentCache(t, segCount)
	cfg := DefaultConfig()
	cfg.MaxFailedSegments = maxFailedSegments
	stats := &ReaderStats{}
	fetcher := NewSegmentFetcher(context.Background(), nil, cache, cfg, stats, zerolog.Nop())
	fetcher.attemptFetch = func(ctx context.Context, segIdx int, seg *SegmentMeta) error {
		return cache.Put(segIdx, make([]byte, seg.Bytes))
	}
	t.Cleanup(fetcher.Close)

	sr := &StreamingReader{
		cache:             cache,
		fetcher:           fetcher,
		config:            cfg,
		totalSize:         cache.TotalSize(),
		segCount:          cache.SegmentCount(),
		maxFailedSegments: maxFailedSegments,
		logger:            zerolog.Nop(),
		stats:             stats,
	}
	fetcher.brokenCheck = sr.registryBrokenErr
	return sr, cache, fetcher
}

// TestCheckFailedThreshold_LatchesOnGenuinelyFailedSegments confirms that
// once enough segments are permanently failed (StateFailed, no retry in
// flight), checkFailedThreshold latches the reader broken so subsequent reads
// fail fast via ErrTooManyFailedSegments instead of retrying a doomed range
// forever.
func TestCheckFailedThreshold_LatchesOnGenuinelyFailedSegments(t *testing.T) {
	sr, cache := newTestStreamingReader(t, 6, 3)

	cache.MarkFailed(0, errors.New("gone"))
	cache.MarkFailed(1, errors.New("gone"))
	sr.checkFailedThreshold()
	if sr.broken.Load() {
		t.Fatal("broken latched at 2 failed segments, want threshold of 3 not yet crossed")
	}

	cache.MarkFailed(2, errors.New("gone"))
	sr.checkFailedThreshold()
	if !sr.broken.Load() {
		t.Fatal("broken not latched at 3 failed segments, want threshold of 3 crossed")
	}
}

// TestCheckFailedThreshold_FailedButRetryingDoesNotTrip is the corrected
// per-segment counterpart: segments that are Failed but currently bracketed
// by MarkRetrying (i.e. fetchWithRetry has them queued for an imminent
// retry) must not count toward the threshold, even when their raw count
// would exceed it.
func TestCheckFailedThreshold_FailedButRetryingDoesNotTrip(t *testing.T) {
	sr, cache := newTestStreamingReader(t, 6, 2)

	cache.MarkFailed(0, errors.New("blip"))
	cache.MarkRetrying(0)
	cache.MarkFailed(1, errors.New("blip"))
	cache.MarkRetrying(1)
	cache.MarkFailed(2, errors.New("blip"))
	cache.MarkRetrying(2)

	if got := cache.FailedSegmentCount(); got != 3 {
		t.Fatalf("raw FailedSegmentCount = %d, want 3 (test setup)", got)
	}

	sr.checkFailedThreshold()
	if sr.broken.Load() {
		t.Fatal("broken latched even though every failed segment is mid-retry (EffectiveFailedSegmentCount should be 0)")
	}

	// A genuinely failed, non-retrying segment among the retrying ones must
	// still trip the threshold on its own.
	cache.MarkFailed(3, errors.New("permanently gone"))
	cache.MarkFailed(4, errors.New("permanently gone"))
	sr.checkFailedThreshold()
	if !sr.broken.Load() {
		t.Fatal("broken not latched: 2 genuinely-failed-and-not-retrying segments should cross the threshold of 2, regardless of the 3 unrelated failed-but-retrying segments")
	}
}

// TestReadAtContext_ReturnsErrTooManyFailedSegmentsOnceBroken confirms the
// latch actually short-circuits reads once tripped.
func TestReadAtContext_ReturnsErrTooManyFailedSegmentsOnceBroken(t *testing.T) {
	sr, _ := newTestStreamingReader(t, 4, 1)
	sr.broken.Store(true)

	buf := make([]byte, 16)
	_, err := sr.ReadAtContext(context.Background(), buf, 0)
	if !errors.Is(err, ErrTooManyFailedSegments) {
		t.Fatalf("ReadAtContext returned %v, want ErrTooManyFailedSegments", err)
	}
}

// TestReadAtContext_ObservesSiblingBrokenViaRegistry is the Bug 2 regression
// test (TOCTOU): two readers share a brokenFileKey, simulating two
// concurrent FUSE opens of the same file during a reopen storm. Reader A
// crosses its own threshold and latches the registry broken. Reader B has
// its own threshold set impossibly high (so it could never trip locally on
// its own failure count) but must still observe the broken state on its very
// next ReadAtContext call, because ReadAtContext also consults the global
// registry, not just its own local sr.broken flag.
func TestReadAtContext_ObservesSiblingBrokenViaRegistry(t *testing.T) {
	const key = "Shared/concurrent-open.mkv"
	t.Cleanup(func() { ClearFileBroken(key) })

	srA, cacheA := newTestStreamingReader(t, 4, 2)
	srA.brokenFileKey = key

	srB, _ := newTestStreamingReader(t, 4, 1000)
	srB.brokenFileKey = key

	cacheA.MarkFailed(0, errors.New("gone"))
	cacheA.MarkFailed(1, errors.New("gone"))
	srA.checkFailedThreshold()
	if !srA.broken.Load() {
		t.Fatal("srA did not latch broken at its own threshold (test setup)")
	}
	if !isFileBroken(key) {
		t.Fatal("srA's latch did not record the key in the registry (test setup)")
	}

	if srB.broken.Load() {
		t.Fatal("srB already broken before its first ReadAtContext call (test setup)")
	}

	buf := make([]byte, 16)
	_, err := srB.ReadAtContext(context.Background(), buf, 0)
	if !errors.Is(err, ErrTooManyFailedSegments) {
		t.Fatalf("srB.ReadAtContext returned %v, want ErrTooManyFailedSegments (sibling latched the registry broken)", err)
	}
	if !srB.broken.Load() {
		t.Fatal("srB.ReadAtContext observed the registry broken but did not latch its own broken flag")
	}
}

// TestCheckFailedThreshold_NegativeThresholdNeverTrips confirms
// maxFailedSegments < 0 behaves identically to 0 (disabled), matching the
// "<= 0" check in checkFailedThreshold.
func TestCheckFailedThreshold_NegativeThresholdNeverTrips(t *testing.T) {
	sr, cache := newTestStreamingReader(t, 4, -1)

	cache.MarkFailed(0, errors.New("gone"))
	cache.MarkFailed(1, errors.New("gone"))
	cache.MarkFailed(2, errors.New("gone"))
	sr.checkFailedThreshold()

	if sr.broken.Load() {
		t.Fatal("broken latched with a negative (disabled) threshold, want never latched")
	}
}

// TestReadAtContext_DisabledThresholdIgnoresRegistry is the round-2 QA
// regression test for the disabled-threshold inconsistency: a reader built
// with MaxFailedSegments <= 0 (feature disabled, e.g. an operator forcing a
// retry of a file latched broken by an earlier reader instance) must ignore
// the registry entirely, exactly like NewStreamingReader's constructor gate
// already does. Before the fix, ReadAtContext's per-call registry check had
// no such gate, so the very first read on a "disabled" reader would still be
// rejected because of a stale registry entry — defeating the whole point of
// disabling the threshold.
func TestReadAtContext_DisabledThresholdIgnoresRegistry(t *testing.T) {
	const key = "disabled-threshold/retry-me.mkv"
	t.Cleanup(func() { ClearFileBroken(key) })
	markFileBroken(key)

	sr, _, _ := newTestStreamingReaderWithFetcher(t, 4, 0)
	sr.brokenFileKey = key

	buf := make([]byte, 16)
	n, err := sr.ReadAtContext(context.Background(), buf, 0)
	if errors.Is(err, ErrTooManyFailedSegments) {
		t.Fatalf("ReadAtContext returned ErrTooManyFailedSegments for a disabled-threshold reader, want the registry to be ignored entirely (as if it didn't exist)")
	}
	if err != nil {
		t.Fatalf("ReadAtContext returned unexpected error %v, want nil", err)
	}
	if n != len(buf) {
		t.Fatalf("ReadAtContext read %d bytes, want %d", n, len(buf))
	}
	if sr.broken.Load() {
		t.Fatal("sr.broken latched despite a disabled threshold")
	}
}
