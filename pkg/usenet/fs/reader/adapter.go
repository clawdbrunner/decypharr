package reader

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/sirrobot01/decypharr/pkg/usenet/types"
)

// VolumeToSegmentMeta converts a types.Volume to []SegmentMeta for the new reader.
func VolumeToSegmentMeta(vol *types.Volume) []SegmentMeta {
	if vol == nil || len(vol.Segments) == 0 {
		return nil
	}
	meta := NewSegmentMetaSlice(vol.Segments)
	return meta
}

// BrokenFileKeyForVolume derives a stable, content-unique key for the
// process-lifetime broken-file registry (broken_registry.go) from a
// Volume's segments.
//
// vol.Name (a human-readable filename like a RAR volume part or
// "sample.mkv") is NOT safe to use directly: it is not guaranteed unique
// across different releases, so two unrelated releases that happen to
// produce identically-named volumes/files would collide in the registry —
// one latching broken would permanently deny service to the other, healthy
// file for the rest of the process lifetime.
//
// NNTP Message-IDs are globally unique per posted article, so hashing the
// first segment's MessageID gives a key that is both content-unique (no
// cross-release collisions) and stable across repeated opens of the same
// underlying file (segment order is deterministic for a given NZB/volume).
// One segment is enough for uniqueness; hashing the whole segment list would
// only add cost without adding safety.
//
// Returns "" (registry disabled, see Config.BrokenFileKey) if vol is nil, has
// no segments, or the first segment's MessageID is empty. The empty-MessageID
// case matters as much as the nil/no-segments one: sha256("") is a fixed,
// predictable digest, so without this guard every malformed Volume (parser
// bug, corrupt NZB) would hash to the same key and collide in the registry —
// the exact cross-file collision this function exists to prevent, just
// reachable through an empty MessageID instead of a missing one.
func BrokenFileKeyForVolume(vol *types.Volume) string {
	if vol == nil || len(vol.Segments) == 0 || vol.Segments[0].MessageID == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(vol.Segments[0].MessageID))
	return hex.EncodeToString(sum[:])
}

// EncryptionFromVolume creates EncryptionConfig from a Volume's encryption settings.
func EncryptionFromVolume(vol *types.Volume) EncryptionConfig {
	if vol == nil || !vol.IsEncrypted {
		return EncryptionConfig{Enabled: false}
	}
	return EncryptionConfig{
		Enabled: true,
		Key:     vol.EncryptionKey,
		IV:      vol.EncryptionIV,
	}
}
