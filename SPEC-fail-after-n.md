# Decypharr Fork: Fail-After-N-Failed-Segments Primitive

## Problem

When a Usenet release has corrupt/missing articles, the NNTP layer correctly
reports failures (yEnc decode errors, 430 ARTICLE_NOT_FOUND, 451 out of
space). However, the file entry stays "available" in the FUSE mount, so
webdav/rclone clients keep retrying the read forever. This causes readers
(ffprobe, Plex scanner) to D-state on FUSE reads, wedging downstream services.

## Root Cause Analysis

- NNTP layer is bounded: 60s timeouts, provider failover, failed-segment cache
- But files stay available even when segments are permanently failed
- After retries are exhausted (`MaxRetries`), `fetchWithRetry` returns the error
- The error propagates up to the FUSE read, but the entry is never marked broken
- Next read attempt starts fresh — the whole retry cycle repeats indefinitely

## Goal

Add a `max_failed_segments_threshold` config option that, when exceeded,
marks the file entry as broken and returns a permanent I/O error to FUSE
reads (instead of retrying forever).

## Implementation Plan

### 1. Config Option

Add to `internal/config/usenet.go` in the `Usenet` struct:

```go
// MaxFailedSegmentsThreshold is the maximum number of permanently failed
// segments per file before the file is marked broken and reads return
// EIO. 0 = disabled (current behavior). Default: 0 (disabled).
// Can also be expressed as a ratio (e.g., "10%" — 10% of total segments).
MaxFailedSegmentsThreshold string `json:"max_failed_segments_threshold,omitempty"`
```

Add a resolver method `MaxFailedSegmentsCount(totalSegments int) int` that
parses the config value (either absolute integer or "N%" ratio) and returns
the absolute threshold. Returns 0 (disabled) if unset or invalid.

### 2. Track Failed Segments in SegmentCache

In `pkg/usenet/fs/reader/cache.go`, add an atomic counter for permanently
failed segments:

```go
type SegmentCache struct {
    ...
    failedSegmentCount atomic.Int32
}
```

When `SetError` is called (marking a segment as `StateFailed`), increment
`failedSegmentCount`. When `ResetFailed` is called (re-fetch attempt),
decrement.

Add a method:
```go
func (sc *SegmentCache) FailedSegmentCount() int32 {
    return sc.failedSegmentCount.Load()
}
```

### 3. Threshold Check in StreamingReader

In `pkg/usenet/fs/reader/reader.go`, add a check in `ReadAtContext`:

After a segment fetch fails permanently (after retries), check if
`cache.FailedSegmentCount()` exceeds the threshold. If so:
- Log: "File marked broken: N/M segments failed (threshold: T)"
- Return `io.EOF` or a permanent error that causes FUSE to return EIO
- Set a flag on the reader so all subsequent reads fail immediately

### 4. Mark Entry Broken in Storage

When the threshold is hit, propagate to the manager/storage layer so the
entry is marked with `HealthBroken` status. This integrates with the existing
repair health checker (`pkg/storage/repair.go`).

The manager's `usenet.go` handler needs to:
1. Detect when a streaming reader has exceeded the failed-segment threshold
2. Call the storage layer to mark the entry health as broken
3. This makes the repair sweep pick it up in the next run

### 5. Integration Points

- `pkg/usenet/fs/reader/reader.go`: `ReadAtContext` — add threshold check
- `pkg/usenet/fs/reader/cache.go`: `SetError` — increment counter
- `pkg/usenet/fs/reader/types.go`: `Config` — add threshold field
- `internal/config/usenet.go`: `Usenet` struct — add config field
- `pkg/manager/usenet.go`: Wire the threshold from config to the reader config
- `pkg/storage/repair.go`: Entry is automatically picked up by health status

### 6. Default Behavior

- Default: `max_failed_segments_threshold: ""` (disabled)
- Recommended setting: `"5%"` — marks file broken when 5% of segments fail
- This catches corrupt releases (which typically have many bad segments)
  without false-flagging releases with 1-2 transiently bad articles

### 7. Testing

- Unit test: `cache_test.go` — verify failed counter increments/decrements
- Unit test: reader threshold check — verify EIO returned after threshold
- Integration: a release with corrupt segments should fail after threshold
  instead of looping forever

## Files to Modify

1. `internal/config/usenet.go` — add config field + resolver
2. `pkg/usenet/fs/reader/types.go` — add threshold to Config
3. `pkg/usenet/fs/reader/cache.go` — add failed counter
4. `pkg/usenet/fs/reader/reader.go` — add threshold check in ReadAtContext
5. `pkg/manager/usenet.go` — wire config to reader

## Codebase Reference

- Repo: ~/clawd/repos/decypharr (Go, fork of sirrobot01/decypharr)
- Upstream issue: https://github.com/sirrobot01/decypharr/issues/376
- Key files already read and understood:
  - pkg/usenet/fs/reader/fetcher.go (segment fetching + retry)
  - pkg/usenet/fs/reader/cache.go (segment state management)
  - pkg/usenet/fs/reader/types.go (Config, SegmentState, ReaderStats)
  - pkg/usenet/fs/reader/reader.go (StreamingReader)
  - internal/config/usenet.go (Usenet config struct)
  - internal/nntp/client.go (NNTP client, error types)
  - pkg/storage/repair.go (Health status, broken entries)

## Constraints

- Go 1.24+ (check go.mod)
- Must be backward compatible (default disabled)
- No new external dependencies
- Follow existing code patterns (zerolog logging, functional options, atomic ops)
- Must compile: `go build ./...`
