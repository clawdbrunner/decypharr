package reader

import (
	"testing"

	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/usenet/types"
)

// TestBrokenFileKeyForVolume_SameNameDifferentContentDoesNotCollide is the
// Bug 1 regression test: vol.Name is a human-readable filename (e.g. a RAR
// volume part or "sample.mkv") that different, unrelated releases can share.
// Two Volumes with the same Name but different underlying segments (distinct
// NNTP Message-IDs, i.e. genuinely different content) must produce different
// registry keys, so one release latching broken can never falsely deny
// service to the other, healthy release.
func TestBrokenFileKeyForVolume_SameNameDifferentContentDoesNotCollide(t *testing.T) {
	volA := &types.Volume{
		Name: "sample.mkv",
		Segments: []storage.NZBSegment{
			{Number: 1, MessageID: "<releaseA-seg1@example.com>"},
			{Number: 2, MessageID: "<releaseA-seg2@example.com>"},
		},
	}
	volB := &types.Volume{
		Name: "sample.mkv", // same human-readable name, unrelated release
		Segments: []storage.NZBSegment{
			{Number: 1, MessageID: "<releaseB-seg1@example.com>"},
			{Number: 2, MessageID: "<releaseB-seg2@example.com>"},
		},
	}

	keyA := BrokenFileKeyForVolume(volA)
	keyB := BrokenFileKeyForVolume(volB)

	if keyA == "" || keyB == "" {
		t.Fatalf("BrokenFileKeyForVolume returned an empty key: keyA=%q keyB=%q", keyA, keyB)
	}
	if keyA == keyB {
		t.Fatalf("BrokenFileKeyForVolume returned the same key %q for two Volumes sharing Name %q but with different segment MessageIDs (would cause cross-release false-positive latching)", keyA, volA.Name)
	}
}

// TestBrokenFileKeyForVolume_SameContentStableAcrossReopens confirms the
// registry still works for its original purpose: reopening "the same"
// underlying file (same segments, same MessageIDs, e.g. a fresh
// NewStreamingReader call after a FUSE reopen) must produce the same key
// every time, even if unrelated metadata like Name or Index differs between
// the two Volume values (as could happen if the volume were reconstructed by
// a different code path across opens).
func TestBrokenFileKeyForVolume_SameContentStableAcrossReopens(t *testing.T) {
	segs := []storage.NZBSegment{
		{Number: 1, MessageID: "<stable-seg1@example.com>"},
		{Number: 2, MessageID: "<stable-seg2@example.com>"},
	}
	firstOpen := &types.Volume{Index: 0, Name: "release.mkv", Segments: segs}
	secondOpen := &types.Volume{Index: 1, Name: "release.mkv", Segments: segs}

	keyFirst := BrokenFileKeyForVolume(firstOpen)
	keySecond := BrokenFileKeyForVolume(secondOpen)

	if keyFirst == "" {
		t.Fatal("BrokenFileKeyForVolume returned an empty key for a volume with segments")
	}
	if keyFirst != keySecond {
		t.Fatalf("BrokenFileKeyForVolume returned different keys (%q vs %q) for two opens of the same underlying content", keyFirst, keySecond)
	}
}

// TestBrokenFileKeyForVolume_NilOrEmptyReturnsEmptyKey confirms the registry
// is disabled (empty BrokenFileKey, see Config.BrokenFileKey) rather than
// panicking or hashing garbage, for a nil Volume or one with no segments.
func TestBrokenFileKeyForVolume_NilOrEmptyReturnsEmptyKey(t *testing.T) {
	if got := BrokenFileKeyForVolume(nil); got != "" {
		t.Fatalf("BrokenFileKeyForVolume(nil) = %q, want empty string", got)
	}
	if got := BrokenFileKeyForVolume(&types.Volume{Name: "empty.mkv"}); got != "" {
		t.Fatalf("BrokenFileKeyForVolume(no segments) = %q, want empty string", got)
	}
}
