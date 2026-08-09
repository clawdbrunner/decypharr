package config

import "testing"

func TestMaxFailedSegmentsCount(t *testing.T) {
	tests := []struct {
		name      string
		threshold string
		total     int
		want      int
	}{
		{"empty string disabled", "", 1000, 0},
		{"normal integer", "50", 1000, 50},
		{"normal percentage", "5%", 1000, 50},
		{"NaN percent rejected", "NaN%", 1000, 0},
		{"over 100 percent rejected", "500%", 1000, 0},
		{"invalid string", "abc", 1000, 0},
		{"negative percentage", "-5%", 1000, 0},
		{"negative integer", "-5", 1000, 0},
		{"zero integer", "0", 1000, 0},
		{"boundary 100 percent allowed", "100%", 1000, 1000},
		{"small percentage rounds down but floors at 1", "0.01%", 1000, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u := Usenet{MaxFailedSegmentsThreshold: tt.threshold}
			got := u.MaxFailedSegmentsCount(tt.total)
			if got != tt.want {
				t.Errorf("MaxFailedSegmentsCount(%d) with threshold %q = %d, want %d", tt.total, tt.threshold, got, tt.want)
			}
		})
	}
}
