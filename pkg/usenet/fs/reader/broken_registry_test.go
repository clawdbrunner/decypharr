package reader

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/nntp"
)

// testSegments returns a minimal single-segment SegmentMeta slice, enough to
// pass NewStreamingReader's "no segments provided" guard.
func testSegments() []SegmentMeta {
	return []SegmentMeta{{MessageID: "<seg1@example.com>", Number: 1, Bytes: 1024}}
}

// TestNewStreamingReader_BrokenKeyFailsFastWithoutCacheAllocation is the
// cross-session-persistence regression test: once a file's key has latched
// broken (via checkFailedThreshold on some prior reader instance), a brand
// new NewStreamingReader call for that same key must fail immediately with
// ErrTooManyFailedSegments and must not allocate a SegmentCache. We verify
// "no cache allocated" concretely: NewSegmentCache always creates a
// "cache-*" temp subdirectory under DiskPath, so an empty DiskPath after the
// call proves the cache (and therefore no NNTP fetcher) was ever created.
func TestNewStreamingReader_BrokenKeyFailsFastWithoutCacheAllocation(t *testing.T) {
	const key = "Some.Release/broken.mkv"
	markFileBroken(key)
	t.Cleanup(func() { ClearFileBroken(key) })

	diskPath := t.TempDir()
	// A non-nil zero-value client is fine: the registry check must return
	// before the client is ever used.
	client := &nntp.Client{}

	r, err := NewStreamingReader(context.Background(), client, testSegments(),
		WithMaxFailedSegments(1),
		WithBrokenFileKey(key),
		WithDiskPath(diskPath),
	)
	if r != nil {
		_ = r.Close()
		t.Fatalf("NewStreamingReader returned a non-nil reader for a known-broken key")
	}
	if !errors.Is(err, ErrTooManyFailedSegments) {
		t.Fatalf("NewStreamingReader returned %v, want ErrTooManyFailedSegments", err)
	}

	entries, readErr := os.ReadDir(diskPath)
	if readErr != nil {
		t.Fatalf("ReadDir(diskPath): %v", readErr)
	}
	if len(entries) != 0 {
		t.Fatalf("DiskPath has %d entries after fail-fast, want 0 (no SegmentCache should have been allocated): %v", len(entries), entries)
	}
}

// TestCheckFailedThreshold_LatchingRecordsKeyInRegistry proves the other
// half of the loop: when a reader's checkFailedThreshold latches it broken,
// it must record that fact in the registry under its BrokenFileKey, so a
// subsequent NewStreamingReader call for the same key (simulating a fresh
// FUSE re-open of the same corrupt file) observes it broken.
func TestCheckFailedThreshold_LatchingRecordsKeyInRegistry(t *testing.T) {
	const key = "Another.Release/corrupt.mkv"
	t.Cleanup(func() { ClearFileBroken(key) })

	sr, cache := newTestStreamingReader(t, 4, 2)
	sr.brokenFileKey = key

	if isFileBroken(key) {
		t.Fatal("key reported broken before any reader latched it")
	}

	cache.MarkFailed(0, errors.New("gone"))
	cache.MarkFailed(1, errors.New("gone"))
	sr.checkFailedThreshold()

	if !sr.broken.Load() {
		t.Fatal("reader did not latch broken at threshold")
	}
	if !isFileBroken(key) {
		t.Fatal("checkFailedThreshold latched the reader but did not record the key in the broken-file registry")
	}

	// A brand new reader for the same key (the cross-session case) must now
	// fail fast, without ever touching NNTP or allocating a fresh cache.
	diskPath := t.TempDir()
	client := &nntp.Client{}
	r, err := NewStreamingReader(context.Background(), client, testSegments(),
		WithMaxFailedSegments(2),
		WithBrokenFileKey(key),
		WithDiskPath(diskPath),
	)
	if r != nil {
		_ = r.Close()
		t.Fatal("fresh NewStreamingReader for the now-broken key succeeded, want fail-fast")
	}
	if !errors.Is(err, ErrTooManyFailedSegments) {
		t.Fatalf("fresh NewStreamingReader returned %v, want ErrTooManyFailedSegments", err)
	}
}

// TestNewStreamingReader_DifferentKeyUnaffected proves the registry is keyed
// correctly: latching one file's key broken must not affect a different
// file's key. A fresh reader for key Y must construct successfully even
// though key X is broken.
func TestNewStreamingReader_DifferentKeyUnaffected(t *testing.T) {
	const brokenKey = "X/broken.mkv"
	const healthyKey = "Y/healthy.mkv"
	markFileBroken(brokenKey)
	t.Cleanup(func() { ClearFileBroken(brokenKey) })

	diskPath := t.TempDir()
	client := &nntp.Client{}

	r, err := NewStreamingReader(context.Background(), client, testSegments(),
		WithMaxFailedSegments(1),
		WithBrokenFileKey(healthyKey),
		WithDiskPath(diskPath),
	)
	if err != nil {
		t.Fatalf("NewStreamingReader for unrelated key returned %v, want nil", err)
	}
	if r == nil {
		t.Fatal("NewStreamingReader for unrelated key returned a nil reader with nil error")
	}
	defer r.Close()

	entries, readErr := os.ReadDir(diskPath)
	if readErr != nil {
		t.Fatalf("ReadDir(diskPath): %v", readErr)
	}
	if len(entries) == 0 {
		t.Fatal("expected a SegmentCache to have been allocated under DiskPath for the unaffected key")
	}
}

// TestNewStreamingReader_ThresholdDisabledIgnoresRegistry proves the
// no-op requirement: even if a key is (somehow) recorded broken, a reader
// constructed with MaxFailedSegments <= 0 (the threshold feature disabled)
// must not consult the registry at all and must construct normally. This
// preserves existing behavior for installs that never set
// max_failed_segments_threshold.
func TestNewStreamingReader_ThresholdDisabledIgnoresRegistry(t *testing.T) {
	const key = "Z/whatever.mkv"
	markFileBroken(key)
	t.Cleanup(func() { ClearFileBroken(key) })

	diskPath := t.TempDir()
	client := &nntp.Client{}

	r, err := NewStreamingReader(context.Background(), client, testSegments(),
		WithBrokenFileKey(key), // MaxFailedSegments left at 0 (disabled)
		WithDiskPath(diskPath),
	)
	if err != nil {
		t.Fatalf("NewStreamingReader with threshold disabled returned %v, want nil", err)
	}
	if r == nil {
		t.Fatal("NewStreamingReader with threshold disabled returned a nil reader")
	}
	defer r.Close()
}

// TestOnFileBrokenHook_FiresOnceUsage confirms the OnFileBroken hook fires
// exactly once per key even if markFileBroken is called multiple times for
// it (e.g. two concurrent readers both crossing the threshold around the
// same time).
func TestOnFileBrokenHook_FiresOnceUsage(t *testing.T) {
	const key = "Hook/fires-once.mkv"
	t.Cleanup(func() {
		ClearFileBroken(key)
		SetOnFileBroken(nil)
	})

	var calls atomic.Int32
	SetOnFileBroken(func(gotKey string) {
		calls.Add(1)
		if gotKey != key {
			t.Errorf("OnFileBroken called with %q, want %q", gotKey, key)
		}
	})

	markFileBroken(key)
	markFileBroken(key)
	markFileBroken(key)

	if got := calls.Load(); got != 1 {
		t.Fatalf("OnFileBroken fired %d times, want exactly 1", got)
	}
}

// TestSegmentCacheAllocated is a sanity check on the diskPath assertion
// technique used above: confirms NewSegmentCache really does create a
// "cache-*" subdirectory under DiskPath, so the "0 entries" assertions in
// the fail-fast tests are actually meaningful.
func TestSegmentCacheAllocated(t *testing.T) {
	diskPath := t.TempDir()
	cache, err := NewSegmentCache(context.Background(), testSegments(), Config{DiskPath: diskPath}, &ReaderStats{}, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewSegmentCache: %v", err)
	}
	defer cache.Close()

	matches, _ := filepath.Glob(filepath.Join(diskPath, "cache-*"))
	if len(matches) == 0 {
		t.Fatal("expected NewSegmentCache to create a cache-* subdirectory under DiskPath")
	}
}

// TestNewStreamingReader_NegativeThresholdDisablesRegistry confirms the
// NewStreamingReader fail-fast gate (config.MaxFailedSegments > 0) agrees
// with checkFailedThreshold's own "<= 0 disables" check for negative values:
// a negative MaxFailedSegments must behave exactly like the documented
// disabled (0) case, i.e. the registry must not be consulted at all, even
// for a key already recorded broken.
func TestNewStreamingReader_NegativeThresholdDisablesRegistry(t *testing.T) {
	const key = "NegativeThreshold/whatever.mkv"
	markFileBroken(key)
	t.Cleanup(func() { ClearFileBroken(key) })

	diskPath := t.TempDir()
	client := &nntp.Client{}

	r, err := NewStreamingReader(context.Background(), client, testSegments(),
		WithMaxFailedSegments(-1),
		WithBrokenFileKey(key),
		WithDiskPath(diskPath),
	)
	if err != nil {
		t.Fatalf("NewStreamingReader with negative (disabled) threshold returned %v, want nil", err)
	}
	if r == nil {
		t.Fatal("NewStreamingReader with negative (disabled) threshold returned a nil reader")
	}
	defer r.Close()
}

// TestMarkFileBroken_ConcurrentWithSetOnFileBroken_Race is the Bug 3
// regression test: concurrently calls markFileBroken for many distinct keys
// from multiple goroutines while a separate goroutine repeatedly re-sets the
// OnFileBroken hook via SetOnFileBroken. Run with -race, this catches the
// data race a bare package-level `var OnFileBroken func(string)` would have
// the moment something calls SetOnFileBroken concurrently with
// markFileBroken. It also asserts the "fires exactly once per key" contract
// still holds under that concurrency: the hook is set (to a
// functionally-stable recording closure) before the marking goroutines
// start, so it is never nil while marking is in flight, and every key must
// therefore fire exactly once regardless of how the repeated SetOnFileBroken
// calls interleave with markFileBroken's reads of it.
func TestMarkFileBroken_ConcurrentWithSetOnFileBroken_Race(t *testing.T) {
	const numKeys = 50
	keys := make([]string, numKeys)
	for i := range keys {
		keys[i] = fmt.Sprintf("Race/key-%d.mkv", i)
	}
	t.Cleanup(func() {
		for _, k := range keys {
			ClearFileBroken(k)
		}
		SetOnFileBroken(nil)
	})

	var counts sync.Map // map[string]*atomic.Int32
	recordingHook := func(key string) {
		v, _ := counts.LoadOrStore(key, new(atomic.Int32))
		v.(*atomic.Int32).Add(1)
	}
	SetOnFileBroken(recordingHook)

	stop := make(chan struct{})
	var setterWG sync.WaitGroup
	setterWG.Add(1)
	go func() {
		defer setterWG.Done()
		for {
			select {
			case <-stop:
				return
			default:
				SetOnFileBroken(recordingHook)
			}
		}
	}()

	const goroutines = 8
	var markWG sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		markWG.Add(1)
		go func() {
			defer markWG.Done()
			for _, k := range keys {
				markFileBroken(k)
				markFileBroken(k) // repeat call for the same key must not re-fire
			}
		}()
	}
	markWG.Wait()
	close(stop)
	setterWG.Wait()

	for _, k := range keys {
		if !isFileBroken(k) {
			t.Errorf("key %q not recorded broken after concurrent markFileBroken calls", k)
			continue
		}
		v, ok := counts.Load(k)
		if !ok {
			t.Errorf("hook never fired for key %q", k)
			continue
		}
		if got := v.(*atomic.Int32).Load(); got != 1 {
			t.Errorf("hook fired %d times for key %q, want exactly 1", got, k)
		}
	}
}
