package config

import "testing"

func TestUsenetJobTimeoutDefault(t *testing.T) {
	c := &Config{}
	c.updateUsenetConfig()
	if c.Usenet.JobTimeout != "15m" {
		t.Errorf("default job_timeout = %q, want %q", c.Usenet.JobTimeout, "15m")
	}
	// Must stay >= processing_timeout so the soft deadline fires first.
	if c.Usenet.ProcessingTimeout != "10m" {
		t.Errorf("default processing_timeout = %q, want %q", c.Usenet.ProcessingTimeout, "10m")
	}

	// Explicit values are preserved.
	c = &Config{Usenet: Usenet{JobTimeout: "30m"}}
	c.updateUsenetConfig()
	if c.Usenet.JobTimeout != "30m" {
		t.Errorf("explicit job_timeout = %q, want %q", c.Usenet.JobTimeout, "30m")
	}
}

func TestUsenetJobTimeoutEnvOverride(t *testing.T) {
	t.Setenv("DECYPHARR_USENET__JOB_TIMEOUT", "25m")
	c := &Config{}
	c.applyUsenetEnvVars()
	if c.Usenet.JobTimeout != "25m" {
		t.Errorf("env job_timeout = %q, want %q", c.Usenet.JobTimeout, "25m")
	}
}

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
