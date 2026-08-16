package reader

import (
	"sync"
	"sync/atomic"
)

// brokenFiles is a process-lifetime record of files that have already
// latched broken (checkFailedThreshold crossed MaxFailedSegments) in some
// prior StreamingReader instance for the same key.
//
// It exists because NewStreamingReader is constructed fresh on every file
// open: FUSE/webdav re-open a corrupt file every 60-90s (Plex, rclone, etc.),
// and without this registry each new reader gets a brand-new SegmentCache
// with a zeroed failed-segment counter, so it has to re-accumulate failures
// from scratch before latching broken again — the file loops forever instead
// of failing fast. Keying on the reader's config.BrokenFileKey (the archive
// volume name, stable across reopens of the same file) fixes that: once any
// reader latches a key broken, every subsequent NewStreamingReader call for
// that key fails immediately, before any NNTP calls or cache allocation.
//
// In-memory only by design: it does not need to survive a container
// restart, since a restart also clears whatever was driving the poison-file
// reopen loop.
var brokenFiles sync.Map // map[string]struct{}

// onFileBroken, if set via SetOnFileBroken, is invoked exactly once per key
// the first time that key is recorded broken (not once per reader instance).
// It is left unset by default: this package only knows the caller-supplied
// BrokenFileKey, not the repair system's EntryName, so mapping one to the
// other and calling through to pkg/storage/repair.go's HealthBroken tracking
// has to happen at a layer that owns that mapping (the manager). Wiring this
// up is tracked as a follow-up; see SPEC-fail-after-n.md step 4.
//
// Stored behind atomic.Pointer rather than a bare func var: markFileBroken
// can be called concurrently with a future SetOnFileBroken call (e.g. the
// manager wiring the hook up during startup while readers are already
// latching files broken), and a bare package-level func var would be a
// go test -race violation the moment that happens.
var onFileBroken atomic.Pointer[func(string)]

// SetOnFileBroken registers fn as the callback invoked once per key the
// first time that key is recorded broken. Pass nil to unregister. Safe to
// call concurrently with markFileBroken.
func SetOnFileBroken(fn func(key string)) {
	if fn == nil {
		onFileBroken.Store(nil)
		return
	}
	onFileBroken.Store(&fn)
}

// isFileBroken reports whether key has already been latched broken by a
// prior reader.
func isFileBroken(key string) bool {
	if key == "" {
		return false
	}
	_, ok := brokenFiles.Load(key)
	return ok
}

// markFileBroken records key as broken. Safe to call repeatedly for the same
// key; only the first call for a given key fires OnFileBroken.
func markFileBroken(key string) {
	if key == "" {
		return
	}
	if _, loaded := brokenFiles.LoadOrStore(key, struct{}{}); !loaded {
		if fn := onFileBroken.Load(); fn != nil {
			(*fn)(key)
		}
	}
}

// ClearFileBroken removes key from the broken registry, e.g. once a repair
// sweep has re-acquired and verified the file. Exported for future
// repair-checker integration.
func ClearFileBroken(key string) {
	if key == "" {
		return
	}
	brokenFiles.Delete(key)
}
