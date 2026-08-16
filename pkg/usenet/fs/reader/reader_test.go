package reader

import (
	"context"
	"errors"
	"testing"

	"github.com/rs/zerolog"

	"github.com/sirrobot01/decypharr/internal/nntp"
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

// latchViaThreshold drives key broken through the actual production latch
// path instead of the isFileBroken-style pre-latching
// TestRegistryBrokenErrFor_ConstructorAndReadPathAgree used to rely on: a
// source StreamingReader accumulates failedCount genuinely-failed segments
// and calls checkFailedThreshold itself, exactly as ReadAtContext's
// post-read hook does in production (reader.go:242). Once failedCount
// reaches maxFailedSegments, checkFailedThreshold calls markFileBroken,
// populating the same process-wide registry registryBrokenErrFor consults.
// A no-op (never latches) when failedCount stays below maxFailedSegments or
// maxFailedSegments <= 0, mirroring checkFailedThreshold's own guards.
func latchViaThreshold(t *testing.T, key string, maxFailedSegments, failedCount int) {
	t.Helper()
	src, cache := newTestStreamingReader(t, failedCount+4, maxFailedSegments)
	src.brokenFileKey = key
	for i := 0; i < failedCount; i++ {
		cache.MarkFailed(i, errors.New("gone"))
	}
	src.checkFailedThreshold()
}

// TestRegistryBrokenErrFor_ConstructorAndReadPathAgree is the round-3 QA
// drift-prevention test for finding 2: NewStreamingReader's constructor gate
// and StreamingReader.registryBrokenErr's per-read gate used to be two
// independent inline implementations of the same logic (isFileBroken +
// maxFailedSegments > 0), which happened to match but had no structural
// guarantee of staying that way. Both now delegate to the single
// registryBrokenErrFor helper.
//
// Round-4 QA widened this: the table used to pre-latch keys directly via
// markFileBroken, which only proves registryBrokenErrFor's own key-state
// branch is consistent — it never exercised checkFailedThreshold's
// below/at/over-threshold boundary decision, the actual mechanism that
// populates the registry in production. Every non-empty-key case here now
// latches (or doesn't) through latchViaThreshold instead, covering a
// minimal threshold (1), a general threshold (3), and for each the three
// boundary counts (max-1: just under, not latched; max: exactly at,
// latched; max+1: over, latched). The threshold-disabled + already-latched
// case is kept (latched via an independent threshold-3 source reader, since
// checkFailedThreshold no-ops when its own threshold is disabled) to keep
// proving the registry is strictly inert once MaxFailedSegments <= 0. Every
// case still drives both the real constructor and a bare instance's
// registryBrokenErr() (bypassing the constructor, mirroring
// newTestStreamingReader) and asserts the two outcomes always agree — a
// future edit that re-diverges one path from the other would fail this
// test.
func TestRegistryBrokenErrFor_ConstructorAndReadPathAgree(t *testing.T) {
	cases := []struct {
		name              string
		maxFailedSegments int
		// latch, if non-nil, is called with the case's key before the
		// constructor/read-path checks below. nil means the key is left
		// untouched (empty key, or a fresh key that never latches).
		latch func(t *testing.T, key string)
	}{
		{"threshold-disabled/empty-key", 0, nil},
		{"threshold-disabled/fresh-key", 0, nil},
		{
			"threshold-disabled/latched-key", 0,
			func(t *testing.T, key string) { latchViaThreshold(t, key, 3, 3) },
		},
		{"threshold-1/below", 1, func(t *testing.T, key string) { latchViaThreshold(t, key, 1, 0) }},
		{"threshold-1/at", 1, func(t *testing.T, key string) { latchViaThreshold(t, key, 1, 1) }},
		{"threshold-1/over", 1, func(t *testing.T, key string) { latchViaThreshold(t, key, 1, 2) }},
		{"threshold-3/below", 3, func(t *testing.T, key string) { latchViaThreshold(t, key, 3, 2) }},
		{"threshold-3/at", 3, func(t *testing.T, key string) { latchViaThreshold(t, key, 3, 3) }},
		{"threshold-3/over", 3, func(t *testing.T, key string) { latchViaThreshold(t, key, 3, 4) }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			key := "drift/" + tc.name
			if tc.name == "threshold-disabled/empty-key" {
				key = ""
			} else {
				t.Cleanup(func() { ClearFileBroken(key) })
			}

			if tc.latch != nil {
				tc.latch(t, key)
			}
			// Derived from the registry, not from whether tc.latch is set:
			// the "below" cases call latchViaThreshold too, but with a
			// failedCount that stays under the threshold, so it must be a
			// no-op.
			wantLatched := isFileBroken(key)

			diskPath := t.TempDir()
			client := &nntp.Client{}
			r, constructorErr := NewStreamingReader(context.Background(), client, testSegments(),
				WithMaxFailedSegments(tc.maxFailedSegments),
				WithBrokenFileKey(key),
				WithDiskPath(diskPath),
			)
			if r != nil {
				defer r.Close()
			}

			// Bare instance, bypassing the constructor, exercising exactly the
			// same registryBrokenErrFor call the real read path
			// (ReadAtContext -> registryBrokenErr) makes.
			bare := &StreamingReader{
				maxFailedSegments: tc.maxFailedSegments,
				brokenFileKey:     key,
			}
			readErr := bare.registryBrokenErr()

			constructorRejected := errors.Is(constructorErr, ErrTooManyFailedSegments)
			readRejected := errors.Is(readErr, ErrTooManyFailedSegments)
			if constructorRejected != readRejected {
				t.Fatalf("constructor rejected=%v (err=%v), read-path rejected=%v (err=%v): the two gates disagree", constructorRejected, constructorErr, readRejected, readErr)
			}

			wantRejected := tc.maxFailedSegments > 0 && wantLatched
			if constructorRejected != wantRejected {
				t.Fatalf("constructor rejected=%v, want %v", constructorRejected, wantRejected)
			}
		})
	}
}
