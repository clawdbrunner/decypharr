package manager

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet/parser"
)

// errJobOrphanBudgetExceeded is returned by processNZBJobWithTimeout when the
// number of already-detached (timed-out) NZB jobs has reached the configured
// budget. processJob treats it like isTooManyActiveDownloads: retry later,
// not a terminal failure.
var errJobOrphanBudgetExceeded = errors.New("nzb job orphan budget exceeded")

func isJobOrphanBudgetExceeded(err error) bool {
	return errors.Is(err, errJobOrphanBudgetExceeded)
}

// nzbOrphanRegistry tracks NZB jobs detached by processNZBJobWithTimeout: the
// hard timeout fired, the worker was freed, but the wedged goroutine is still
// running the actual I/O in the background. Each entry pins whatever
// NNTP connections/memory that goroutine holds until it finally returns, so
// the registry's count is used to backpressure new NZB jobs once too many
// pile up (see errJobOrphanBudgetExceeded) instead of letting them
// accumulate without bound.
type nzbOrphanRegistry struct {
	mu      sync.Mutex
	entries map[string]time.Time
}

func newNZBOrphanRegistry() *nzbOrphanRegistry {
	return &nzbOrphanRegistry{entries: make(map[string]time.Time)}
}

func (r *nzbOrphanRegistry) Register(infoHash string) {
	if infoHash == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.entries[infoHash] = time.Now()
}

func (r *nzbOrphanRegistry) Unregister(infoHash string) {
	if infoHash == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.entries, infoHash)
}

func (r *nzbOrphanRegistry) Count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.entries)
}

// AddNewNZB parses an NZB before entering the active-download queue.
func (m *Manager) AddNewNZB(ctx context.Context, req *ImportRequest) (string, error) {
	if m.usenet == nil {
		return "", fmt.Errorf("usenet not configured")
	}
	if req == nil || len(req.NZBContent) == 0 {
		return "", fmt.Errorf("NZB content is empty")
	}
	if req.Arr == nil {
		return "", fmt.Errorf("arr is required")
	}

	m.logger.Info().
		Str("name", req.Name).
		Str("category", req.Arr.Name).
		Msg("Adding new NZB to usenet")

	meta, groups, err := m.usenet.ParseWithID(ctx, req.Id, req.Name, req.NZBContent, req.Arr.Name)
	if err != nil {
		return "", fmt.Errorf("usenet parse failed: %w", err)
	}

	entry := &storage.Entry{
		InfoHash:         meta.ID,
		Name:             meta.Name,
		OriginalFilename: meta.Name,
		Size:             meta.TotalSize,
		Protocol:         config.ProtocolNZB,
		Bytes:            meta.TotalSize,
		Category:         req.Arr.Name,
		SavePath:         filepath.Join(req.DownloadFolder, req.Arr.Name),
		Status:           debridTypes.TorrentStatusDownloading,
		State:            storage.EntryStateDownloading,
		Progress:         0,
		Action:           req.Action,
		CallbackURL:      req.CallBackUrl,
		SkipMultiSeason:  req.SkipMultiSeason,
		CreatedAt:        time.Now(),
		UpdatedAt:        time.Now(),
		AddedOn:          time.Now(),
		Providers:        make(map[string]*storage.ProviderEntry),
		Files:            make(map[string]*storage.File),
		Tags:             []string{},
	}

	entry.ContentPath = entry.DownloadPath()
	entry.ActiveProvider = "usenet"
	_ = entry.AddUsenetProvider(meta)
	if err := m.queue.Add(entry); err != nil {
		return "", fmt.Errorf("failed to add nzb to queue: %w", err)
	}

	req.Status = "started"
	job := NewJob(JobTypeNZB, req)
	job.ID = entry.InfoHash
	job.Entry = entry
	job.NZBMeta = meta
	job.NZBGroups = groups
	if err := m.SubmitJob(job); err != nil {
		entry.MarkAsError(err)
		_ = m.queue.Update(entry)
		return "", fmt.Errorf("failed to queue NZB: %w", err)
	}
	return meta.ID, nil
}

// processNZBJobWithTimeout runs processNZBJob under the hard per-job timeout
// (usenet.job_timeout, default 15m).
//
// Why not just context.WithTimeout around the worker call? The inner
// processing_timeout (default 10m) is already a context deadline, but wedged
// NNTP I/O can ignore cancellation and never return. With a fixed worker pool
// (default 3), each such job pins its worker forever — and 3 stuck jobs
// silently stall the entire usenet pipeline while every healthcheck passes
// (observed in production 2026-08-15: 3 stuck entries starved the queue for
// days). So the job runs in its own goroutine: if the timeout fires first we
// mark the NZB failed (terminal — it must NOT be requeued as "Downloading"
// on every restart), return the error so processJob marks the entry terminal,
// and free the worker. The wedged goroutine is left detached until its I/O
// unblocks or the process exits; a leaked goroutine is strictly better than a
// permanently pinned worker.
//
// Known race: if the detached goroutine eventually completes after the
// timeout, it may attempt to update an entry we already failed. That result
// is discarded (see processNZB/processNewNzb's ctx.Err() guards) — the
// detached goroutine holding onto NNTP connections/memory until it does so
// is accepted, bounded by usenet.job_orphan_budget below, and the
// alternative (blocking the worker on a broken I/O path) is the outage this
// exists to prevent.
//
// Nothing job-specific is safely closable here to force an earlier release:
// the archive parser, availability checks and finalize all read/write
// through the shared *nntp.Client connection pool (m.usenet's), not a
// per-job connection or handle. Closing that pool on timeout would sever
// every other in-flight job sharing it, not just the wedged one — worse than
// the leak. So this relies on the orphan budget/backpressure below, not on
// forcibly reclaiming resources.
func (m *Manager) processNZBJobWithTimeout(ctx context.Context, job *Job) error {
	timeout := m.usenetJobTimeout
	if timeout <= 0 {
		// Timeout disabled/misconfigured: fall back to inline processing.
		return m.processNZBJobFn(ctx, job)
	}

	infoHash := ""
	if job.Entry != nil {
		infoHash = job.Entry.InfoHash
	}

	// Backpressure: refuse to start a new job while too many previously
	// timed-out jobs are still detached and wedged (each one pins NNTP
	// connections/memory indefinitely). Retried, not failed — the job is
	// simply not ready to run yet.
	if m.nzbOrphans != nil {
		if count := m.nzbOrphans.Count(); count >= m.usenetJobOrphanBudget {
			name := ""
			if job.Entry != nil {
				name = job.Entry.Name
			}
			m.logger.Error().
				Str("job_id", job.ID).
				Str("name", name).
				Int("orphan_count", count).
				Int("orphan_budget", m.usenetJobOrphanBudget).
				Msg("NZB_JOB_ORPHAN_BUDGET_EXCEEDED: too many detached timed-out jobs; backpressuring new job")
			return fmt.Errorf("%w: %d detached jobs >= budget %d", errJobOrphanBudgetExceeded, count, m.usenetJobOrphanBudget)
		}
	}

	jobCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- m.processNZBJobFn(jobCtx, job)
		if m.nzbOrphans != nil {
			m.nzbOrphans.Unregister(infoHash)
		}
	}()

	select {
	case err := <-done:
		return err
	case <-jobCtx.Done():
		if ctx.Err() != nil {
			// Parent cancelled (queue shutdown), not a job timeout.
			return ctx.Err()
		}
		stage := "unknown"
		name := ""
		if job.Entry != nil {
			name = job.Entry.Name
			if m.usenet != nil {
				stage = m.usenet.JobStage(job.Entry.InfoHash)
			}
		}
		timeoutErr := fmt.Errorf("nzb job timed out after %s (stage: %s)", timeout, stage)
		m.logger.Error().
			Str("job_id", job.ID).
			Str("name", name).
			Str("stage", stage).
			Str("timeout", timeout.String()).
			Msg("NZB job timed out; marking failed and freeing worker (wedged operation detached)")
		if m.nzbOrphans != nil {
			m.nzbOrphans.Register(infoHash)
		}
		// Terminal NZB-meta state so a restart does not requeue the entry as
		// "Downloading" (nzbNeedsReprocessing only requeues parsing/downloading
		// entries). The queue entry itself is marked terminal (EntryStateError)
		// by the processJob error path, which also surfaces the failure to arrs
		// via the sab history status/fail_message fields.
		if m.usenet != nil && job.Entry != nil {
			if err := m.usenet.MarkNZBFailed(job.Entry.InfoHash, timeoutErr); err != nil {
				m.logger.Warn().Err(err).
					Str("job_id", job.ID).
					Msg("Failed to mark timed-out NZB as failed in usenet storage")
			}
		}
		return timeoutErr
	}
}

func (m *Manager) processNZBJob(ctx context.Context, job *Job) error {
	if job == nil || job.Entry == nil {
		return fmt.Errorf("invalid NZB job")
	}
	if _, err := m.queue.GetTorrent(job.Entry.InfoHash); err != nil {
		return nil
	}
	if job.NZBMeta == nil {
		if job.Request == nil {
			m.waitForDownloadCompletion(ctx, job.Entry)
			return nil
		}
		return fmt.Errorf("parsed NZB metadata missing")
	}
	if job.Request != nil {
		job.Request.Status = "started"
	}
	return m.processNewNzb(ctx, job.Entry, job.NZBMeta, job.NZBGroups)
}

func (m *Manager) processNZB(ctx context.Context, entry *storage.Entry, metadata *storage.NZB) error {
	if ctx.Err() != nil {
		// Belt-and-braces: this ctx descends from processNZBJobWithTimeout's
		// jobCtx. If it's already done, the job either already timed out
		// (entry marked failed, terminal) or the queue is shutting down —
		// either way, a late/discarded result must never overwrite that with
		// a completed-write or fire post-processing.
		m.logger.Warn().
			Str("nzb_id", entry.InfoHash).
			Str("name", entry.Name).
			Err(ctx.Err()).
			Msg("late completion after timeout: discarding")
		return ctx.Err()
	}
	// Add files using logical streamable files
	for _, file := range metadata.Files {
		tFile := &storage.File{
			Name:     file.Name,
			Size:     file.Size,
			InfoHash: entry.InfoHash,
			AddedOn:  entry.AddedOn,
		}
		entry.Files[file.Name] = tFile
	}
	// Mark as complete
	if placement := entry.GetActiveProvider(); placement != nil {
		now := time.Now()
		placement.DownloadedAt = &now
		placement.Progress = 1.0
	}
	entry.Size = metadata.TotalSize
	entry.Progress = 1.0
	entry.UpdatedAt = time.Now()
	_ = m.queue.Update(entry)

	if len(entry.Files) == 0 {
		return fmt.Errorf("nzb has no files")
	}

	go m.processAction(entry)
	return nil
}

// processNewNzb processes a new NZB entry after it has been added to the usenet client
func (m *Manager) processNewNzb(parentCtx context.Context, entry *storage.Entry, metadata *storage.NZB, groups map[string]*parser.FileGroup) error {
	// Create context with timeout for processing
	ctx, cancel := context.WithTimeout(parentCtx, m.usenetTimeout)
	defer cancel()

	m.logger.Debug().
		Str("nzb_id", entry.InfoHash).
		Str("name", entry.Name).
		Str("processing_timeout", m.usenetTimeout.String()).
		Str("job_timeout", m.usenetJobTimeout.String()).
		Msg("Starting NZB processing (archive parse -> availability gate -> finalize)")

	updatedNZB, err := m.usenet.Process(ctx, metadata, groups)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return fmt.Errorf("usenet processing timed out after %s: %w", m.usenetTimeout, err)
		}
		return fmt.Errorf("failed to process nzb: %w", err)
	}
	if ctx.Err() != nil {
		// Belt-and-braces: even if usenet.Process wrongly reported success
		// post-cancellation, never hand a canceled/timed-out result to
		// processNZB for persistence.
		return fmt.Errorf("usenet processing canceled: %w", ctx.Err())
	}

	metadata = updatedNZB
	return m.processNZB(ctx, entry, metadata)
}

// HasUsenet returns true if usenet is configured
func (m *Manager) HasUsenet() bool {
	return m.usenet != nil
}

// UsenetStats returns usenet client statistics
func (m *Manager) UsenetStats() map[string]any {
	if m.usenet == nil {
		return nil
	}
	return m.usenet.Stats()
}

// SpeedTestRequest represents a speed test request payload
type SpeedTestRequest struct {
	Protocol string `json:"protocol"` // "nntp" or "debrid"
	Provider string `json:"provider"` // provider host/identifier
}

// SpeedTestResponse represents a speed test result
type SpeedTestResponse struct {
	Provider  string  `json:"provider"`
	Protocol  string  `json:"protocol"`
	SpeedMBps float64 `json:"speed_mbps"`
	LatencyMs int64   `json:"latency_ms"`
	BytesRead int64   `json:"bytes_read"`
	TestedAt  string  `json:"tested_at"`
	Error     string  `json:"error,omitempty"`
}

// SpeedTest runs a speed test for a specific provider based on protocol
func (m *Manager) SpeedTest(ctx context.Context, req SpeedTestRequest) SpeedTestResponse {
	switch req.Protocol {
	case "nntp":
		if m.usenet == nil {
			return SpeedTestResponse{
				Provider: req.Provider,
				Protocol: req.Protocol,
				Error:    "usenet not configured",
			}
		}
		result := m.usenet.SpeedTest(ctx, req.Provider)
		return SpeedTestResponse{
			Provider:  result.Provider,
			Protocol:  req.Protocol,
			SpeedMBps: result.SpeedMBps,
			LatencyMs: result.LatencyMs,
			BytesRead: result.BytesRead,
			TestedAt:  result.TestedAt.Format("2006-01-02T15:04:05Z07:00"),
			Error:     result.Error,
		}
	case "debrid":
		// Look up debrid client by provider name
		client, exists := m.clients.Load(req.Provider)
		if !exists {
			return SpeedTestResponse{
				Provider: req.Provider,
				Protocol: req.Protocol,
				Error:    "debrid provider not found: " + req.Provider,
			}
		}
		result := client.SpeedTest(ctx)

		// Store the result for persistence (so it shows up in stats)
		if result.Error == "" {
			m.debridSpeedTestResults.Store(req.Provider, result)
		}

		return SpeedTestResponse{
			Provider:  result.Provider,
			Protocol:  req.Protocol,
			SpeedMBps: result.SpeedMBps,
			LatencyMs: result.LatencyMs,
			BytesRead: result.BytesRead,
			TestedAt:  result.TestedAt.Format("2006-01-02T15:04:05Z07:00"),
			Error:     result.Error,
		}
	default:
		return SpeedTestResponse{
			Provider: req.Provider,
			Protocol: req.Protocol,
			Error:    "unknown protocol: " + req.Protocol,
		}
	}
}

func (m *Manager) syncNZBs(ctx context.Context) error {
	if m.usenet == nil {
		return nil
	}

	m.nzbSyncMu.Lock()
	defer m.nzbSyncMu.Unlock()

	pendingNZBs, err := m.usenet.ClaimNewNZBs()
	if err != nil {
		return fmt.Errorf("failed to claim new NZBs from usenet client: %w", err)
	}

	for _, pending := range pendingNZBs {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		req := NewNZBRequest(
			pending.Name,
			m.config.DownloadFolder,
			pending.Content,
			m.arr.GetOrCreate(""),
			config.DownloadActionNone,
			"",
			ImportTypeWatch,
			false,
		)
		if _, err := m.AddNewNZB(ctx, req); err != nil {
			m.logger.Error().Err(err).Str("name", pending.Name).Msg("Failed to queue watched NZB")
			continue
		}
		m.usenet.RemoveClaimedNZB(pending.Path)
	}
	return nil
}
