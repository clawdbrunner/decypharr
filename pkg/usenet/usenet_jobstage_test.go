package usenet

import (
	"errors"
	"testing"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/logger"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// newTestUsenet builds a minimal Usenet with file-based NZB storage rooted in
// a temp dir. Only the fields exercised by stage tracking and MarkNZBFailed
// are populated.
func newTestUsenet(t *testing.T) *Usenet {
	t.Helper()
	config.SetConfigPath(t.TempDir())
	nzbStorage, err := NewNZBStorage()
	if err != nil {
		t.Fatalf("NewNZBStorage: %v", err)
	}
	return &Usenet{
		nzbStorage: nzbStorage,
		logger:     logger.New("test"),
		jobStages:  xsync.NewMap[string, string](),
	}
}

func TestJobStageTracking(t *testing.T) {
	u := newTestUsenet(t)

	if got := u.JobStage("nzb-1"); got != "unknown" {
		t.Errorf("untracked NZB stage = %q, want %q", got, "unknown")
	}

	u.setJobStage("nzb-1", JobStageArchiveParse)
	if got := u.JobStage("nzb-1"); got != JobStageArchiveParse {
		t.Errorf("stage = %q, want %q", got, JobStageArchiveParse)
	}

	u.setJobStage("nzb-1", JobStageAvailabilityCheck)
	if got := u.JobStage("nzb-1"); got != JobStageAvailabilityCheck {
		t.Errorf("stage = %q, want %q", got, JobStageAvailabilityCheck)
	}

	u.clearJobStage("nzb-1")
	if got := u.JobStage("nzb-1"); got != "unknown" {
		t.Errorf("cleared stage = %q, want %q", got, "unknown")
	}

	// Empty IDs must be no-ops (defensive: timeout path passes entry IDs).
	u.setJobStage("", JobStageFinalize)
	u.clearJobStage("")
	if got := u.JobStage(""); got != "unknown" {
		t.Errorf("empty-ID stage = %q, want %q", got, "unknown")
	}
}

func TestMarkNZBFailed(t *testing.T) {
	u := newTestUsenet(t)

	nzb := &storage.NZB{
		ID:     "nzb-timeout",
		Name:   "Test.Release.1080p",
		Status: NZBStatusDownloading,
	}
	if err := u.nzbStorage.AddNZB(nzb); err != nil {
		t.Fatalf("AddNZB: %v", err)
	}
	u.setJobStage(nzb.ID, JobStageAvailabilityCheck)

	cause := errors.New("nzb job timed out after 15m (stage: availability_check)")
	if err := u.MarkNZBFailed(nzb.ID, cause); err != nil {
		t.Fatalf("MarkNZBFailed: %v", err)
	}

	got, err := u.nzbStorage.GetNZBHeader(nzb.ID)
	if err != nil {
		t.Fatalf("GetNZBHeader: %v", err)
	}
	if got.Status != NZBStatusFailed {
		t.Errorf("status = %q, want %q", got.Status, NZBStatusFailed)
	}
	if got.FailMessage != cause.Error() {
		t.Errorf("fail message = %q, want %q", got.FailMessage, cause.Error())
	}
	// Stage tracking must be cleared so a later timeout doesn't report a
	// stale stage for an already-terminal NZB.
	if stage := u.JobStage(nzb.ID); stage != "unknown" {
		t.Errorf("stage after failure = %q, want %q", stage, "unknown")
	}

	// Idempotent: re-marking an already-failed NZB is a no-op success.
	if err := u.MarkNZBFailed(nzb.ID, errors.New("again")); err != nil {
		t.Errorf("re-mark failed NZB: %v", err)
	}

	// Unknown NZB IDs return an error (caller logs and continues).
	if err := u.MarkNZBFailed("does-not-exist", cause); err == nil {
		t.Error("expected error for unknown NZB ID")
	}

	// Completed NZBs must not be flipped back to failed by a late timeout.
	completed := &storage.NZB{ID: "nzb-done", Name: "Done.Release", Status: NZBStatusCompleted}
	if err := u.nzbStorage.AddNZB(completed); err != nil {
		t.Fatalf("AddNZB completed: %v", err)
	}
	if err := u.MarkNZBFailed(completed.ID, cause); err != nil {
		t.Fatalf("MarkNZBFailed on completed: %v", err)
	}
	got, err = u.nzbStorage.GetNZBHeader(completed.ID)
	if err != nil {
		t.Fatalf("GetNZBHeader completed: %v", err)
	}
	if got.Status != NZBStatusCompleted {
		t.Errorf("completed NZB status flipped to %q", got.Status)
	}
}
