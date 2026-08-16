package manager

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/logger"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet"
)

// newTestManager builds a minimal Manager directly (bypassing New()/init(),
// which spin up debrid clients, schedulers, and a background restore that
// this test doesn't need). Real storage and a real (never-dialed) usenet
// client are used so MarkNZBFailed and the queue behave exactly as in
// production; only the network-facing NNTP provider is fake.
func newTestManager(t *testing.T, jobTimeout time.Duration, orphanBudget int, maxWorkers int) *Manager {
	t.Helper()

	config.Reset()
	config.SetConfigPath(t.TempDir())
	cfg := config.Get()
	cfg.Usenet.Providers = []config.UsenetProvider{
		{Host: "127.0.0.1", Port: 1, Username: "u", Password: "p", MaxConnections: 1},
	}

	strg, err := storage.NewStorage(filepath.Join(t.TempDir(), "db"))
	if err != nil {
		t.Fatalf("storage.NewStorage: %v", err)
	}

	usenetClient, err := usenet.New()
	if err != nil {
		t.Fatalf("usenet.New: %v", err)
	}

	m := &Manager{
		storage:               strg,
		logger:                logger.New("test-manager"),
		queue:                 newQueue(strg, ""),
		usenet:                usenetClient,
		usenetJobTimeout:      jobTimeout,
		usenetJobOrphanBudget: orphanBudget,
		nzbOrphans:            newNZBOrphanRegistry(),
		ctx:                   context.Background(),
	}
	m.processNZBJobFn = m.processNZBJob
	m.processNZBActionFn = m.processAction
	m.jobQueue = NewJobQueue(m.ctx, maxWorkers, m.processJob)

	t.Cleanup(func() {
		m.jobQueue.Close()
		_ = m.usenet.Close()
		_ = m.storage.Close()
	})

	return m
}

// newNZBJob builds a queued Entry + Job pair for an NZB import, registering
// the entry in the queue and a matching record in usenet storage (so
// MarkNZBFailed has something to act on), without going through the real
// AddNewNZB/parse path.
func newNZBJob(t *testing.T, m *Manager, infoHash, name string) *Job {
	t.Helper()

	entry := &storage.Entry{
		InfoHash:  infoHash,
		Name:      name,
		Protocol:  config.ProtocolNZB,
		Status:    debridTypes.TorrentStatusDownloading,
		State:     storage.EntryStateDownloading,
		Providers: make(map[string]*storage.ProviderEntry),
		Files:     make(map[string]*storage.File),
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	if err := m.queue.Add(entry); err != nil {
		t.Fatalf("queue.Add: %v", err)
	}

	nzbMeta := &storage.NZB{ID: infoHash, Name: name, Status: usenet.NZBStatusDownloading}
	if err := m.usenet.NZBStorage().AddNZB(nzbMeta); err != nil {
		t.Fatalf("AddNZB: %v", err)
	}

	job := NewJob(JobTypeNZB, nil)
	job.ID = infoHash
	job.Entry = entry
	job.NZBMeta = nzbMeta
	return job
}

// wedgeStub simulates processNZBJob for a single targeted job ID: it blocks
// until release is closed, then (mirroring what the real processNZB does)
// checks ctx.Err() before touching anything, and only mutates the entry /
// flips postProcessed if the context is still live. Every other job ID
// returns immediately, so the queue's worker throughput can be observed
// independently of the wedged job.
type wedgeStub struct {
	mu            sync.Mutex
	wedgeJobID    string
	release       chan struct{}
	calls         map[string]int
	postProcessed atomic.Bool
}

func newWedgeStub(wedgeJobID string) *wedgeStub {
	return &wedgeStub{
		wedgeJobID: wedgeJobID,
		release:    make(chan struct{}),
		calls:      make(map[string]int),
	}
}

func (s *wedgeStub) callCount(jobID string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls[jobID]
}

func (s *wedgeStub) run(ctx context.Context, job *Job) error {
	s.mu.Lock()
	s.calls[job.ID]++
	wedge := job.ID == s.wedgeJobID
	s.mu.Unlock()

	if !wedge {
		return nil
	}

	<-s.release
	if ctx.Err() != nil {
		// Mirrors processNZB's belt-and-braces guard: discard late results.
		return ctx.Err()
	}
	job.Entry.Progress = 1.0
	s.postProcessed.Store(true)
	return nil
}

func waitFor(t *testing.T, timeout time.Duration, msg string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	if !cond() {
		t.Fatalf("timed out waiting for: %s", msg)
	}
}

// TestProcessNZBJobWithTimeout_DetachedLateCompletion covers HIGH-1 and
// HIGH-2 from QA round 1 end to end through the real wrapper
// (processNZBJobWithTimeout / processJob), using an injected stub in place
// of processNZBJob so no real NNTP I/O is needed.
func TestProcessNZBJobWithTimeout_DetachedLateCompletion(t *testing.T) {
	m := newTestManager(t, 60*time.Millisecond, 5, 1)

	jobA := newNZBJob(t, m, "info-a", "Job.A")
	stub := newWedgeStub(jobA.ID)
	m.processNZBJobFn = stub.run

	if err := m.SubmitJob(jobA); err != nil {
		t.Fatalf("SubmitJob: %v", err)
	}

	// (a) Timeout fires: entry reaches EntryStateError, NZBMeta marked
	// failed, orphan registered, worker freed.
	waitFor(t, 2*time.Second, "entry A to reach EntryStateError", func() bool {
		e, err := m.queue.GetTorrent(jobA.ID)
		return err == nil && e.State == storage.EntryStateError
	})

	nzbHeader, err := m.usenet.GetNZBHeader(jobA.ID)
	if err != nil {
		t.Fatalf("GetNZBHeader: %v", err)
	}
	if nzbHeader.Status != usenet.NZBStatusFailed {
		t.Errorf("NZBMeta status = %q, want %q", nzbHeader.Status, usenet.NZBStatusFailed)
	}

	waitFor(t, 2*time.Second, "orphan to be registered", func() bool {
		return m.nzbOrphans.Count() == 1
	})

	// Worker freed: with a single-worker queue, job B must still get
	// processed while job A's goroutine is wedged.
	jobB := newNZBJob(t, m, "info-b", "Job.B")
	if err := m.SubmitJob(jobB); err != nil {
		t.Fatalf("SubmitJob jobB: %v", err)
	}
	waitFor(t, 2*time.Second, "job B to be processed by the freed worker", func() bool {
		return stub.callCount(jobB.ID) > 0
	})

	// (b) Release the wedge; stub returns success late.
	close(stub.release)

	waitFor(t, 2*time.Second, "orphan to be unregistered after late completion", func() bool {
		return m.nzbOrphans.Count() == 0
	})

	// State must remain failed/Error — no completed-overwrite, no
	// post-processing trigger.
	e, err := m.queue.GetTorrent(jobA.ID)
	if err != nil {
		t.Fatalf("GetTorrent: %v", err)
	}
	if e.State != storage.EntryStateError {
		t.Errorf("entry A state after late completion = %q, want %q", e.State, storage.EntryStateError)
	}
	if e.Progress != 0 {
		t.Errorf("entry A progress = %v, want 0 (no completed-overwrite)", e.Progress)
	}
	if stub.postProcessed.Load() {
		t.Error("post-processing was triggered by a late completion after timeout")
	}

	nzbHeader, err = m.usenet.GetNZBHeader(jobA.ID)
	if err != nil {
		t.Fatalf("GetNZBHeader after late completion: %v", err)
	}
	if nzbHeader.Status != usenet.NZBStatusFailed {
		t.Errorf("NZBMeta status after late completion = %q, want %q", nzbHeader.Status, usenet.NZBStatusFailed)
	}
}

// TestProcessNZBJobWithTimeout_OrphanBudgetBackpressure covers scenario (c):
// once the orphan budget is exhausted, new NZB jobs must be backpressured
// (retried, not failed) instead of started.
func TestProcessNZBJobWithTimeout_OrphanBudgetBackpressure(t *testing.T) {
	m := newTestManager(t, 50*time.Millisecond, 1, 1)
	if _, ok := m.nzbOrphans.RegisterIfUnderBudget("already-orphaned", 1); !ok {
		t.Fatal("failed to seed orphan")
	}

	stub := newWedgeStub("never-wedges")
	m.processNZBJobFn = stub.run

	job := newNZBJob(t, m, "info-c", "Job.C")

	err := m.processNZBJobWithTimeout(context.Background(), job)
	if err == nil {
		t.Fatal("expected orphan budget error, got nil")
	}
	if !isJobOrphanBudgetExceeded(err) {
		t.Errorf("error = %v, want errJobOrphanBudgetExceeded", err)
	}
	if got := stub.callCount(job.ID); got != 0 {
		t.Errorf("processNZBJobFn called %d times, want 0 (job must not start)", got)
	}

	// Through the full processJob path: retried, not terminal.
	m.processJob(context.Background(), job)
	e, err := m.queue.GetTorrent(job.ID)
	if err != nil {
		t.Fatalf("GetTorrent: %v", err)
	}
	if e.State == storage.EntryStateError {
		t.Error("entry marked terminal (EntryStateError) on orphan-budget backpressure, want retry/queued")
	}
	if e.Status != debridTypes.TorrentStatusQueued {
		t.Errorf("entry status = %q, want %q (queued for retry)", e.Status, debridTypes.TorrentStatusQueued)
	}
}

// TestProcessNZB_CancellationGuard is unit coverage (scenario d) for the
// manager-side cancellation-as-error gate added to processNZB: a
// canceled/timed-out context must be rejected before any state mutation or
// post-processing trigger, never silently accepted.
func TestProcessNZB_CancellationGuard(t *testing.T) {
	m := &Manager{logger: logger.New("test")}
	entry := &storage.Entry{
		InfoHash: "cancel-test",
		Files:    make(map[string]*storage.File),
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := m.processNZB(ctx, entry, &storage.NZB{})
	if err == nil {
		t.Fatal("expected error for canceled context, got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("error = %v, want wrapping context.Canceled", err)
	}
	if entry.Progress != 0 || len(entry.Files) != 0 {
		t.Error("processNZB mutated the entry despite a canceled context")
	}
}

func TestNZBOrphanRegistryAtomicAdmissionAndReaping(t *testing.T) {
	r := newNZBOrphanRegistry()
	const attempts, budget = 64, 64
	var successes atomic.Int64
	var wg sync.WaitGroup
	tokens := make(chan string, attempts)
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if token, ok := r.RegisterIfUnderBudget("same-job", budget); ok {
				successes.Add(1)
				tokens <- token
			}
		}()
	}
	wg.Wait()
	if got := successes.Load(); got != budget {
		t.Fatalf("successful admissions = %d, want %d", got, budget)
	}
	if got := r.Count(); got != budget {
		t.Fatalf("registry count = %d, want %d", got, budget)
	}

	if _, ok := r.RegisterIfUnderBudget("other", budget); ok {
		t.Fatal("different hash admitted while all same-hash instances consume budget")
	}
	for i := 0; i < attempts-1; i++ {
		r.Unregister(<-tokens)
	}
	if got := r.Count(); got != 1 {
		t.Fatalf("count before final same-hash unregister = %d, want 1", got)
	}
	otherToken, ok := r.RegisterIfUnderBudget("other", budget)
	if !ok {
		t.Fatal("other hash was not admitted into the one reopened slot")
	}
	r.Unregister(<-tokens)
	if got := r.Count(); got != 1 {
		t.Fatalf("interleaved other-hash count = %d, want 1", got)
	}
	r.Unregister(otherToken)
	r.Unregister("missing") // Missing cleanup is deliberately harmless.
}

func TestNZBOrphanRegistryReapRegisterUnregisterABA(t *testing.T) {
	r := newNZBOrphanRegistry()
	r.maxAge = 32 * time.Minute
	now := time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)
	r.nowFn = func() time.Time { return now }

	tokenA, ok := r.RegisterIfUnderBudget("same-job", 1)
	if !ok {
		t.Fatal("failed to register instance A")
	}
	now = now.Add(33 * time.Minute)
	if got := r.Count(); got != 0 {
		t.Fatalf("count after reaping A = %d, want 0", got)
	}
	tokenB, ok := r.RegisterIfUnderBudget("same-job", 1)
	if !ok {
		t.Fatal("failed to register instance B")
	}
	r.Unregister(tokenA)
	if got := r.Count(); got != 1 {
		t.Fatalf("old A cleanup removed B: count = %d, want 1", got)
	}
	if _, ok := r.RegisterIfUnderBudget("other-job", 1); ok {
		t.Fatal("budget reopened while instance B was still registered")
	}
	r.Unregister(tokenB)
	if _, ok := r.RegisterIfUnderBudget("other-job", 1); !ok {
		t.Fatal("budget did not reopen after instance B was unregistered")
	}
}

func TestNZBOrphanRegistryStaggeredSameHashReaping(t *testing.T) {
	r := newNZBOrphanRegistry()
	r.maxAge = 32 * time.Minute
	base := time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)
	now := base
	r.nowFn = func() time.Time { return now }
	tokenA, ok := r.RegisterIfUnderBudget("same-job", 2)
	if !ok {
		t.Fatal("failed to register instance A")
	}
	now = base.Add(31 * time.Minute)
	tokenB, ok := r.RegisterIfUnderBudget("same-job", 2)
	if !ok {
		t.Fatal("failed to register instance B")
	}
	now = base.Add(33 * time.Minute)
	if got := r.Count(); got != 1 {
		t.Fatalf("count after staggered reap = %d, want 1", got)
	}
	if _, exists := r.entries[tokenA]; exists {
		t.Fatal("33-minute-old instance A was not reaped")
	}
	if _, exists := r.entries[tokenB]; !exists {
		t.Fatal("2-minute-old instance B was reaped")
	}
}

func TestNZBOrphanRegistryManagerDerivedReaping(t *testing.T) {
	tests := []struct {
		name       string
		timeout    string
		orphanAges []time.Duration
		wantCounts []int
	}{
		{name: "four-times-timeout", timeout: "8m", orphanAges: []time.Duration{31 * time.Minute, 33 * time.Minute}, wantCounts: []int{1, 0}},
		{name: "thirty-minute-floor", timeout: "5m", orphanAges: []time.Duration{31 * time.Minute}, wantCounts: []int{0}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			config.Reset()
			config.SetConfigPath(t.TempDir())
			cfg := config.Get()
			cfg.Usenet.JobTimeout = tc.timeout
			if err := cfg.Save(); err != nil {
				t.Fatalf("save config: %v", err)
			}
			config.Reset()
			m := &Manager{}
			m.resetNZBJobConfig(config.Get())

			base := time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)
			for i, age := range tc.orphanAges {
				now := base
				m.nzbOrphans.nowFn = func() time.Time { return now }
				token, ok := m.nzbOrphans.RegisterIfUnderBudget(fmt.Sprintf("orphan-%d", i), 1)
				if !ok {
					t.Fatal("failed to register orphan")
				}
				now = base.Add(age)
				if got := m.nzbOrphans.Count(); got != tc.wantCounts[i] {
					t.Fatalf("timeout %s age %s count = %d, want %d (maxAge %s)", tc.timeout, age, got, tc.wantCounts[i], m.nzbOrphans.maxAge)
				}
				m.nzbOrphans.Unregister(token)
			}
		})
	}
}

func TestProcessNZBCommitGateOrdering(t *testing.T) {
	m := newTestManager(t, 40*time.Millisecond, 2, 1)
	job := newNZBJob(t, m, "race-timeout", "Race.Timeout")
	metadata := &storage.NZB{ID: job.ID, Name: job.Entry.Name, Files: []storage.NZBFile{{Name: "video.mkv", Size: 10}}}
	var actions atomic.Int64
	m.processNZBActionFn = func(entry *storage.Entry) {
		entry.MarkAsCompleted(entry.DownloadPath())
		_ = m.queue.Update(entry)
		actions.Add(1)
	}
	blocked, release := make(chan struct{}), make(chan struct{})
	m.beforeNZBCommitClaim = func() { close(blocked); <-release }
	m.processNZBJobFn = func(ctx context.Context, got *Job) error { return m.processNZB(ctx, got.Entry, metadata, got) }
	done := make(chan struct{})
	go func() { m.processJob(context.Background(), job); close(done) }()
	<-blocked
	<-done
	entry, err := m.queue.GetTorrent(job.ID)
	if err != nil || entry.State != storage.EntryStateError {
		t.Fatalf("timeout-first queue state = %q, err %v; want %q", entry.State, err, storage.EntryStateError)
	}
	header, err := m.usenet.GetNZBHeader(job.ID)
	if err != nil || header.Status != usenet.NZBStatusFailed {
		t.Fatalf("timeout-first NZB state = %q, err %v; want %q", header.Status, err, usenet.NZBStatusFailed)
	}
	close(release)
	waitFor(t, time.Second, "timeout-first orphan cleanup", func() bool { return m.nzbOrphans.Count() == 0 })
	if actions.Load() != 0 || job.Entry.Progress != 0 {
		t.Fatal("timeout-first late completion mutated state or fired an action")
	}

	job2 := newNZBJob(t, m, "race-complete", "Race.Complete")
	m.beforeNZBCommitClaim = nil
	metadata2 := &storage.NZB{ID: job2.ID, Name: job2.Entry.Name, Files: []storage.NZBFile{{Name: "video.mkv", Size: 10}}}
	claimed, finish := make(chan struct{}), make(chan struct{})
	m.afterNZBCommitClaim = func() { close(claimed); <-finish }
	m.processNZBJobFn = func(ctx context.Context, got *Job) error { return m.processNZB(ctx, got.Entry, metadata2, got) }
	done2 := make(chan error, 1)
	go func() { done2 <- m.processNZBJobWithTimeout(context.Background(), job2) }()
	<-claimed
	if err := <-done2; err != nil {
		t.Fatalf("completion-first wrapper result = %v, want nil", err)
	}
	close(finish)
	waitFor(t, time.Second, "completion-first commit", func() bool { return actions.Load() == 1 })
	entry2, err := m.queue.GetTorrent(job2.ID)
	if err != nil || entry2.Progress != 1 || entry2.State != storage.EntryStatePausedUP {
		t.Fatalf("completion-first final queue state = %q, progress %v, err %v; want completed", entry2.State, entry2.Progress, err)
	}
	header2, err := m.usenet.GetNZBHeader(job2.ID)
	if err != nil || header2.Status != usenet.NZBStatusCompleted {
		t.Fatalf("completion-first NZB state = %q, err %v; want %q", header2.Status, err, usenet.NZBStatusCompleted)
	}
	if actions.Load() != 1 {
		t.Fatalf("post-processing actions = %d, want exactly 1", actions.Load())
	}
}
