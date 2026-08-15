package reader

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/nntp"
)

// SegmentFetcher handles downloading segments from NNTP with deduplication and retry.
//
// Key features:
//   - Request deduplication: Only one goroutine fetches a segment at a time
//   - Semaphore for connection limiting
//   - Background prefetch queue for read-ahead
//   - Streams directly to disk via cache's StreamWriter
type SegmentFetcher struct {
	client *nntp.Client
	cache  *SegmentCache
	config Config
	logger zerolog.Logger
	stats  *ReaderStats

	// Concurrency control
	semaphore chan struct{} // Limits concurrent downloads

	// Request deduplication
	inFlight   map[int]*fetchPromise
	inFlightMu sync.Mutex

	// Background prefetch
	prefetchCh     chan int
	prefetchQueued []atomic.Uint64 // one deduplication bit per segment
	prefetchWg     sync.WaitGroup

	// attemptFetch performs one physical fetch attempt for segIdx once
	// MarkFetching and the connection semaphore have both been acquired.
	// Defaults to defaultAttemptFetch (the real NNTP path via
	// client.ExecuteWithFailover). Tests override this to exercise doFetch's
	// and fetchWithRetry's dedup/retry/state-machine logic without a live
	// NNTP server.
	attemptFetch func(ctx context.Context, segIdx int, seg *SegmentMeta) error

	// brokenCheck, if set, is consulted by EnsureSegments before each segment
	// in the requested range. It lets the owning StreamingReader's
	// cross-session broken-file registry state (broken_registry.go) cut a
	// multi-segment read short the moment a sibling reader latches the file
	// broken mid-read, instead of only being checked once at the top of
	// ReadAtContext before any segment work starts. A read already in flight
	// for one segment when the latch flips is allowed to finish that segment
	// — this only stops the *remaining* not-yet-fetched segments in the same
	// read. Nil disables the check (e.g. a test harness fetcher with no
	// owning reader).
	brokenCheck func() error

	// Lifecycle
	ctx    context.Context
	cancel context.CancelFunc
}

// fetchPromise allows multiple goroutines to wait for the same segment download.
type fetchPromise struct {
	done chan struct{}
	err  error
}

// NewSegmentFetcher creates a new segment fetcher.
func NewSegmentFetcher(
	ctx context.Context,
	client *nntp.Client,
	cache *SegmentCache,
	config Config,
	stats *ReaderStats,
	logger zerolog.Logger,
) *SegmentFetcher {
	ctx, cancel := context.WithCancel(ctx)

	maxConns := config.MaxConnections
	if maxConns < 1 {
		maxConns = 8
	}

	sf := &SegmentFetcher{
		client:     client,
		cache:      cache,
		config:     config,
		logger:     logger.With().Str("component", "fetcher").Logger(),
		stats:      stats,
		semaphore:  make(chan struct{}, maxConns),
		inFlight:   make(map[int]*fetchPromise),
		prefetchCh: make(chan int, 256), // Buffer for prefetch hints
		// A packed atomic bitmap keeps duplicate suppression cheap even for
		// very large NZBs: 100k segments consume about 12 KiB, versus roughly
		// 400 KiB for one atomic.Bool per segment.
		prefetchQueued: make([]atomic.Uint64, (cache.SegmentCount()+63)/64),
		ctx:            ctx,
		cancel:         cancel,
	}
	sf.attemptFetch = sf.defaultAttemptFetch

	// Start fewer prefetch workers than foreground connection slots. Seeky
	// callers such as ffprobe can jump to the tail while head read-ahead is
	// still running; reserving at least one slot prevents background prefetch
	// from starving the blocking read that the caller is waiting on.
	numPrefetchWorkers := maxConns - 1
	if numPrefetchWorkers > 0 {
		for i := range numPrefetchWorkers {
			sf.prefetchWg.Add(1)
			go sf.prefetchWorker(i)
		}
	}

	return sf
}

// Fetch downloads a segment synchronously, with deduplication.
// Multiple goroutines calling Fetch for the same segment will share the download.
func (sf *SegmentFetcher) Fetch(ctx context.Context, segIdx int) error {
	// Fast path: already cached, or wait out an in-progress eviction so we
	// don't dedup/fetch against a segment whose disk range is mid-punch.
	for {
		state := sf.cache.GetState(segIdx)
		if state == StateEvicting {
			if err := sf.cache.WaitForEvictionRelease(ctx, segIdx); err != nil {
				return err
			}
			continue // slot is Empty now; re-evaluate
		}
		switch state {
		case StateOnDisk:
			return nil
		case StateFailed:
			return sf.cache.GetError(segIdx)
		}
		break
	}

	// Check if someone else is already fetching
	sf.inFlightMu.Lock()
	if promise, ok := sf.inFlight[segIdx]; ok {
		sf.inFlightMu.Unlock()
		// Wait for existing fetch
		select {
		case <-promise.done:
			return promise.err
		case <-ctx.Done():
			return ctx.Err()
		case <-sf.ctx.Done():
			return sf.ctx.Err()
		}
	}

	// We're the first - create promise
	promise := &fetchPromise{done: make(chan struct{})}
	sf.inFlight[segIdx] = promise
	sf.inFlightMu.Unlock()

	// Actually fetch
	err := sf.doFetch(ctx, segIdx)
	promise.err = err
	close(promise.done)

	// Cleanup
	sf.inFlightMu.Lock()
	delete(sf.inFlight, segIdx)
	sf.inFlightMu.Unlock()

	return err
}

// doFetch performs the actual NNTP download.
func (sf *SegmentFetcher) doFetch(ctx context.Context, segIdx int) error {
	seg := sf.cache.GetSegment(segIdx)
	if seg == nil {
		return ErrSegmentNotFound
	}

	// Try to mark as fetching (atomic transition Empty -> Fetching)
	if !sf.cache.MarkFetching(segIdx) {
		// Someone else is fetching or it's already cached
		state := sf.cache.GetState(segIdx)
		switch state {
		case StateOnDisk:
			return nil
		case StateFailed:
			return sf.cache.GetError(segIdx)
		case StateFetching:
			// Wait for the other fetcher
			return sf.cache.WaitForSegment(ctx, segIdx)
		case StateEvicting:
			// An evictor grabbed the slot between Fetch's check and here.
			// Wait for the punch to finish, then retry the fetch into the
			// released range.
			if err := sf.cache.WaitForEvictionRelease(ctx, segIdx); err != nil {
				return err
			}
			return sf.doFetch(ctx, segIdx)
		}
	}

	// Acquire connection slot
	select {
	case sf.semaphore <- struct{}{}:
		defer func() { <-sf.semaphore }()
	case <-ctx.Done():
		sf.cache.ReleaseFetching(segIdx)
		return ctx.Err()
	case <-sf.ctx.Done():
		sf.cache.ReleaseFetching(segIdx)
		return sf.ctx.Err()
	}

	attempt := sf.attemptFetch
	if attempt == nil {
		attempt = sf.defaultAttemptFetch
	}
	err := attempt(ctx, segIdx, seg)

	if err != nil {
		sf.stats.DownloadErrors.Add(1)
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			sf.cache.ReleaseFetching(segIdx)
			return err
		}
		sf.cache.MarkFailed(segIdx, err)
		return err
	}

	sf.stats.Downloads.Add(1)
	return nil
}

// defaultAttemptFetch performs the real NNTP download for one attempt: it
// wraps the call in a per-attempt timeout and streams the decoded article
// body straight into the cache's disk-backed writer for segIdx.
func (sf *SegmentFetcher) defaultAttemptFetch(ctx context.Context, segIdx int, seg *SegmentMeta) error {
	messageID := seg.MessageID
	timeout := sf.config.DownloadTimeout
	if timeout <= 0 {
		timeout = 60 * time.Second
	}

	downloadCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// ExecuteWithFailover already retries per provider and across providers —
	// a single call is sufficient.  An outer retry loop would multiply the
	// total attempts by retries×providers, leading to very long failure times.
	return sf.client.ExecuteWithFailover(downloadCtx, func(conn *nntp.Connection) error {
		stopCancel := context.AfterFunc(downloadCtx, func() {
			_ = conn.Close()
		})
		defer stopCancel()

		// Get the segment writer for the disk cache.
		writer := sf.cache.StreamWriter(segIdx)
		if writer == nil {
			return ErrCacheClosed
		}

		// Stream the decoded body into the chosen tier.
		n, err := conn.StreamBody(messageID, writer)
		if err != nil {
			writer.Discard()
			if ctxErr := downloadCtx.Err(); ctxErr != nil {
				return ctxErr
			}
			return err
		}
		if ctxErr := downloadCtx.Err(); ctxErr != nil {
			writer.Discard()
			return ctxErr
		}

		// Treat zero-byte articles as missing — the article exists on the
		// server but its body is empty/corrupted after yEnc decoding.
		if n == 0 {
			writer.Discard()
			return &nntp.Error{
				Type:    nntp.ErrorTypeArticleNotFound,
				Message: "article produced no data after decoding",
			}
		}

		// Commit (updates cache state to StateOnDisk).
		writer.Finalize()

		return nil
	})
}

func (sf *SegmentFetcher) markPrefetchQueued(segIdx int) bool {
	if segIdx < 0 || segIdx >= sf.cache.SegmentCount() {
		return false
	}
	word := &sf.prefetchQueued[segIdx>>6]
	mask := uint64(1) << uint(segIdx&63)
	for {
		old := word.Load()
		if old&mask != 0 {
			return false
		}
		if word.CompareAndSwap(old, old|mask) {
			return true
		}
	}
}

func (sf *SegmentFetcher) clearPrefetchQueued(segIdx int) {
	if segIdx < 0 || segIdx >= sf.cache.SegmentCount() {
		return
	}
	word := &sf.prefetchQueued[segIdx>>6]
	mask := uint64(1) << uint(segIdx&63)
	word.And(^mask)
}

// QueuePrefetch adds a segment to the background prefetch queue (non-blocking).
func (sf *SegmentFetcher) QueuePrefetch(segIdx int) {
	// Check if already cached
	state := sf.cache.GetState(segIdx)
	if state == StateOnDisk || state == StateFetching {
		return
	}
	// State remains Empty while a hint is waiting in prefetchCh. Track that
	// interval separately so frequent small ReadAt calls cannot enqueue the
	// same read-ahead window hundreds of times and crowd useful hints out.
	if !sf.markPrefetchQueued(segIdx) {
		return
	}

	select {
	case sf.prefetchCh <- segIdx:
		// Queued successfully
	default:
		sf.clearPrefetchQueued(segIdx)
		// Queue full, drop the hint
		sf.stats.PrefetchMisses.Add(1)
	}
}

// QueuePrefetchRange queues multiple segments for prefetch.
func (sf *SegmentFetcher) QueuePrefetchRange(startSeg, endSeg int) {
	for i := startSeg; i <= endSeg; i++ {
		sf.QueuePrefetch(i)
	}
}

// prefetchWorker processes segments from the prefetch queue.
func (sf *SegmentFetcher) prefetchWorker(id int) {
	defer sf.prefetchWg.Done()

	for {
		select {
		case <-sf.ctx.Done():
			return
		case segIdx, ok := <-sf.prefetchCh:
			if !ok {
				return
			}
			sf.prefetchOne(segIdx)
			sf.clearPrefetchQueued(segIdx)
		}
	}
}

// prefetchOne uses the deduplicated, failover-aware single-segment fetch path.
// It goes through fetchWithRetry so a transient failure hit during background
// prefetch gets retried exactly like a foreground read (EnsureSegments) does,
// instead of being marked permanently failed after a single attempt — see
// fetchWithRetry's doc comment for why that distinction matters for the
// broken-file threshold.
func (sf *SegmentFetcher) prefetchOne(segIdx int) {
	state := sf.cache.GetState(segIdx)
	if state == StateOnDisk {
		sf.stats.PrefetchHits.Add(1)
		return
	}

	// Consult the same registry gate EnsureSegments uses before doing any
	// foreground fetch work (see brokenCheck's doc comment). Without this, a
	// segment already queued in prefetchCh before the file latched broken
	// would still burn a full fetchWithRetry round — network calls and
	// backoff sleeps — even though every foreground read for the file is now
	// failing fast. Prefetch is best-effort background work, so a latch here
	// is silently dropped: no segment-failed mark, no error propagated. A
	// narrow check-to-fetch race (the latch flips between this check and the
	// fetchWithRetry call below) remains, same as EnsureSegments — accepted,
	// not worth closing.
	if sf.brokenCheck != nil {
		if err := sf.brokenCheck(); err != nil {
			sf.logger.Debug().Int("segment", segIdx).Msg("prefetch skipped: file latched broken")
			return
		}
	}

	// Deliberately no context.WithTimeout wrapper here. DownloadTimeout is a
	// per-attempt budget applied inside doFetch (see the downloadCtx wrap
	// around each ExecuteWithFailover call); fetchWithRetry makes multiple
	// attempts with backoff sleeps in between. Wrapping the whole retry loop
	// in a single DownloadTimeout would starve it down to effectively one
	// attempt, reintroducing the bug this fixes. The foreground path
	// (EnsureSegments, called from readAtPlain/readAtEncrypted) has the same
	// shape: it receives the caller's request context, not a fixed total
	// timeout, and relies on doFetch's per-attempt wrap for bounding. Here we
	// use sf.ctx, the fetcher's lifecycle context, since background prefetch
	// has no per-request caller context to inherit.
	err := sf.fetchWithRetry(sf.ctx, segIdx)

	if err != nil && err != context.Canceled && err != context.DeadlineExceeded {
		sf.logger.Debug().Err(err).Int("segment", segIdx).Msg("prefetch failed")
	}
}

// EnsureSegments fetches all segments in the range, returning when all are
// available. Segments are fetched in order; in steady-state playback the
// background prefetch workers have already downloaded them, so this loop
// usually just confirms cache presence. fetchWithRetry keeps a single
// transient segment failure from tearing down the whole stream.
func (sf *SegmentFetcher) EnsureSegments(ctx context.Context, startSeg, endSeg int) error {
	for i := startSeg; i <= endSeg; i++ {
		if sf.brokenCheck != nil {
			if err := sf.brokenCheck(); err != nil {
				return err
			}
		}
		state := sf.cache.GetState(i)
		if state != StateOnDisk {
			if err := sf.fetchWithRetry(ctx, i); err != nil {
				return err
			}
		}
	}
	return nil
}

// fetchWithRetry fetches a single segment, retrying transient failures so a
// momentary provider hiccup or stall does not tear down the whole stream.
// Permanent failures (article-not-found) and cancellations return immediately.
func (sf *SegmentFetcher) fetchWithRetry(ctx context.Context, segIdx int) error {
	maxAttempts := sf.config.MaxRetries
	if maxAttempts < 1 {
		maxAttempts = 3
	}

	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			// Clear the failed state so the segment can be re-fetched, then
			// back off briefly before retrying. ResetFailed is a CAS: if a
			// concurrent reader fetched the segment meanwhile it stays OnDisk.
			// ClearRetrying pairs with the MarkRetrying below that guarded the
			// previous attempt.
			sf.cache.ResetFailed(segIdx)
			sf.cache.ClearRetrying(segIdx)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-sf.ctx.Done():
				return sf.ctx.Err()
			case <-time.After(sf.retryBackoff(attempt)):
			}
		}

		// If this attempt fails, we'll retry on the next loop iteration
		// (unless it's the last attempt, a permanent error, or a
		// cancellation). Mark that BEFORE calling Fetch, not after it fails:
		// doFetch's MarkFailed makes the failure instantly visible to any
		// concurrent reader's checkFailedThreshold via the shared
		// failedSegmentCount counter. If we only marked "retrying" after the
		// failure, there'd be a window between MarkFailed and that mark
		// where a concurrent reader could observe the transient bump and
		// permanently latch the file broken over what would have been a
		// successful retry. Marking pre-emptively means retryingCount is
		// already elevated (excluding this segment from
		// EffectiveFailedSegmentCount) at the exact moment MarkFailed would
		// fire, closing that window entirely.
		willRetryOnFailure := attempt < maxAttempts-1
		if willRetryOnFailure {
			sf.cache.MarkRetrying(segIdx)
		}

		err := sf.Fetch(ctx, segIdx)
		if err == nil {
			if willRetryOnFailure {
				sf.cache.ClearRetrying(segIdx)
			}
			return nil
		}
		lastErr = err

		// Don't retry permanent errors or cancellations.
		if nntp.IsArticleNotFoundError(err) || ctx.Err() != nil || sf.ctx.Err() != nil {
			if willRetryOnFailure {
				sf.cache.ClearRetrying(segIdx)
			}
			return err
		}
		// Transient failure that will be retried: leave retryingCount
		// incremented. The next loop iteration's ResetFailed+ClearRetrying
		// above clears both the Failed state and the retrying mark together
		// before backing off and retrying.
	}
	return lastErr
}

// retryBackoff returns the delay before the given (1-indexed) retry attempt.
func (sf *SegmentFetcher) retryBackoff(attempt int) time.Duration {
	base := sf.config.RetryDelay
	if base <= 0 {
		base = time.Second
	}
	d := base << (attempt - 1)
	if maxDelay := 5 * time.Second; d > maxDelay {
		d = maxDelay
	}
	return d
}

// Close stops all workers and waits for them to finish.
//
// prefetchCh is deliberately never closed: QueuePrefetch can race Close (a
// ReadAtContext already past the reader's closed check), and a send on a
// closed channel panics even inside a select. Workers exit via sf.ctx instead,
// and the channel is garbage-collected with the fetcher.
func (sf *SegmentFetcher) Close() {
	sf.cancel()
	sf.prefetchWg.Wait()
}

// Error types
var (
	ErrSegmentNotFound = &segmentError{msg: "segment not found"}
	ErrCacheClosed     = &segmentError{msg: "cache closed"}
)

type segmentError struct {
	msg string
}

func (e *segmentError) Error() string {
	return e.msg
}
