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
