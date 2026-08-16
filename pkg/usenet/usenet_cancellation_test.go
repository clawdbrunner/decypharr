package usenet

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/nntp"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// newTestUsenetWithNNTP extends newTestUsenet with a real (never-dialed)
// NNTP client so checkAvailability's BatchStat path can be exercised without
// touching the network: the repair pool worker checks ctx.Err() before
// attempting any I/O (see internal/nntp/repair_pool.go worker()).
func newTestUsenetWithNNTP(t *testing.T) *Usenet {
	t.Helper()
	u := newTestUsenet(t)
	cfg := &config.Config{
		Usenet: config.Usenet{
			Providers: []config.UsenetProvider{
				{Host: "127.0.0.1", Port: 1, Username: "u", Password: "p", MaxConnections: 1},
			},
		},
	}
	client, err := nntp.NewClient(cfg)
	if err != nil {
		t.Fatalf("nntp.NewClient: %v", err)
	}
	u.nntp = client
	return u
}

// A canceled/timed-out context must never be reported as a passed
// availability check: a nil return here previously let the caller
// (Process) mark a timed-out NZB "completed" off a check that never
// actually finished (QA round 1, pkg/usenet/usenet.go:638).
func TestCheckNZBAvailabilityCancellation(t *testing.T) {
	u := newTestUsenet(t)
	nzb := &storage.NZB{
		ID:   "nzb-cancel",
		Name: "Test.Release",
		Files: []storage.NZBFile{
			{
				Name:     "test.mkv",
				FileType: storage.NZBFileTypeMedia,
				Segments: []storage.NZBSegment{{Number: 1, MessageID: "<seg1@test>"}},
			},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := u.checkNZBAvailability(ctx, nzb)
	if err == nil {
		t.Fatal("expected error for canceled context, got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("error = %v, want wrapping context.Canceled", err)
	}
}

// checkNZBAvailability only reaches its own ctx.Err() check between files;
// if every file is skipped (deleted/par2/ignore/segment-less) the loop body
// never runs and a canceled ctx would otherwise slip through as nil.
func TestCheckNZBAvailabilityCancellationNoCheckableFiles(t *testing.T) {
	u := newTestUsenet(t)
	nzb := &storage.NZB{
		ID:   "nzb-cancel-empty",
		Name: "Test.Release",
		Files: []storage.NZBFile{
			{Name: "readme.nfo", FileType: storage.NZBFileTypeIgnore, Segments: []storage.NZBSegment{{Number: 1, MessageID: "<seg1@test>"}}},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := u.checkNZBAvailability(ctx, nzb)
	if err == nil {
		t.Fatal("expected error for canceled context, got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("error = %v, want context.Canceled", err)
	}
}

func TestCheckAvailabilityCancellation(t *testing.T) {
	u := newTestUsenetWithNNTP(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := u.checkAvailability(ctx, "test.mkv", []string{"<seg1@test>", "<seg2@test>"})
	if err == nil {
		t.Fatal("expected error for canceled context, got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("error = %v, want wrapping context.Canceled", err)
	}
}

// checkAvailability must not swallow cancellation into "connection error,
// non-fatal" — a caller checking only for a non-nil error would otherwise
// silently accept a check that never happened.
func TestCheckFileAvailabilityCancellation(t *testing.T) {
	u := newTestUsenetWithNNTP(t)

	file := &storage.NZBFile{
		Name:     "test.mkv",
		Segments: []storage.NZBSegment{{Number: 1, MessageID: "<seg1@test>"}, {Number: 2, MessageID: "<seg2@test>"}},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := u.CheckFileAvailability(ctx, file, 100)
	if err == nil {
		t.Fatal("expected error for canceled context, got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("error = %v, want wrapping context.Canceled", err)
	}
}

func TestProcessMidFlightCancellation(t *testing.T) {
	u := newTestUsenetWithNNTP(t)
	files := make([]storage.NZBFile, 2000)
	for i := range files {
		files[i] = storage.NZBFile{Name: fmt.Sprintf("video-%d.mkv", i), FileType: storage.NZBFileTypeMedia,
			Segments: []storage.NZBSegment{{Number: 1, MessageID: fmt.Sprintf("<seg-%d@test>", i)}}}
	}
	nzb := &storage.NZB{ID: "process-midflight-cancel", Name: "Cancel.Release", Files: files}
	ctx, cancel := context.WithCancel(context.Background())
	started, release := make(chan struct{}), make(chan struct{})
	u.beforeArchiveProcess = func() { close(started); <-release }
	done := make(chan error, 1)
	go func() { _, err := u.Process(ctx, nzb, nil); done <- err }()

	<-started
	cancel()
	close(release)
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Process returned nil after mid-flight cancellation")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Process did not return after cancellation")
	}
}
