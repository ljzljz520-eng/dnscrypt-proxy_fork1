package main

import (
	"testing"
	"time"

	"github.com/BurntSushi/toml"
)

func configPtr[T any](v T) *T { return &v }

// TR-6.1: lb_strategy parsing selects the right strategy; case insensitive;
// empty and invalid values both keep WP2 (the pre-existing behavior).
func TestConfigureLoadBalancingStrategyRegistration(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"empty defaults to wp2", "", "wp2"},
		{"explicit wp2", "wp2", "wp2"},
		{"mds lowercase", "mds", "mds"},
		{"mds case insensitive", "MdS", "mds"},
		{"unknown keeps wp2", "nonsense", "wp2"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			proxy := NewProxy()
			cfg := newConfig()
			cfg.LBStrategy = tc.in
			configureLoadBalancing(proxy, &cfg)

			switch tc.want {
			case "mds":
				if _, ok := proxy.serversInfo.lbStrategy.(*LBStrategyMDS); !ok {
					t.Fatalf("got %T, want *LBStrategyMDS", proxy.serversInfo.lbStrategy)
				}
			case "wp2":
				if _, ok := proxy.serversInfo.lbStrategy.(LBStrategyWP2); !ok {
					t.Fatalf("got %T, want LBStrategyWP2", proxy.serversInfo.lbStrategy)
				}
			}
		})
	}
}

// TR-6.2: absent keys yield defaults; valid custom values apply; every
// out-of-range value is rejected and falls back to the default.
func TestApplyMDSConfigValidation(t *testing.T) {
	// Empty section: pure defaults.
	params, window := applyMDSConfig(MDSSchedulerConfig{})
	if params != DefaultMDSParams() {
		t.Fatalf("empty config changed defaults:\n got %+v\nwant %+v", params, DefaultMDSParams())
	}
	if window != metricsWindow {
		t.Fatalf("window = %v, want %v", window, metricsWindow)
	}

	// Valid custom values.
	cfg := MDSSchedulerConfig{
		WarmupSamples:           configPtr(uint64(30)),
		PenaltyMinSamples:       configPtr(uint64(8)),
		SwitchMargin:            configPtr(0.25),
		TieBand:                 configPtr(0.02),
		Dwell:                   configPtr("2s"),
		ExplorationProbability:  configPtr(0.0),
		CircuitBreakerThreshold: configPtr(uint32(5)),
		HalfOpenInterval:        configPtr("30s"),
		MetricsWindow:           configPtr("2m30s"),
		WeightTimeout:           configPtr(0.9),
		WeightErrors:            configPtr(0.0),
		WeightServfail:          configPtr(0.1),
		WeightDNSSECBogus:       configPtr(0.7),
		WeightTruncated:         configPtr(0.04),
		WeightFallback:          configPtr(0.2),
		WeightConnNew:           configPtr(0.06),
		WeightJitter:            configPtr(0.15),
	}
	params, window = applyMDSConfig(cfg)
	if params.WarmupSamples != 30 || params.PenaltyMinSamples != 8 ||
		params.SwitchMargin != 0.25 || params.TieBand != 0.02 ||
		params.Dwell != 2*time.Second || params.ExploreProb != 0.0 ||
		params.BreakerThreshold != 5 || params.HalfOpenInterval != 30*time.Second {
		t.Fatalf("valid scalar values not applied: %+v", params)
	}
	if window != 150*time.Second {
		t.Fatalf("window = %v, want 2m30s", window)
	}
	if params.WeightTimeout != 0.9 || params.WeightErrors != 0.0 ||
		params.WeightServfail != 0.1 || params.WeightBogus != 0.7 ||
		params.WeightTruncated != 0.04 || params.WeightFallback != 0.2 ||
		params.WeightConnNew != 0.06 || params.WeightJitter != 0.15 {
		t.Fatalf("valid weights not applied: %+v", params)
	}

	// Out-of-range values: each offending key is replaced by its default
	// while untouched keys keep defaults.
	def := DefaultMDSParams()
	bad := MDSSchedulerConfig{
		WarmupSamples:           configPtr(uint64(0)),
		PenaltyMinSamples:       configPtr(uint64(50)), // exceeds warmup default
		SwitchMargin:            configPtr(-0.1),
		TieBand:                 configPtr(0.8),
		Dwell:                   configPtr("not-a-duration"),
		ExplorationProbability:  configPtr(1.5),
		CircuitBreakerThreshold: configPtr(uint32(0)),
		HalfOpenInterval:        configPtr("100ms"),
		MetricsWindow:           configPtr("5s"),
		WeightTimeout:           configPtr(11.0),
		WeightJitter:            configPtr(-1.0),
	}
	params, window = applyMDSConfig(bad)
	if params.WarmupSamples != def.WarmupSamples ||
		params.PenaltyMinSamples != def.PenaltyMinSamples ||
		params.SwitchMargin != def.SwitchMargin ||
		params.TieBand != def.TieBand ||
		params.Dwell != def.Dwell ||
		params.ExploreProb != def.ExploreProb ||
		params.BreakerThreshold != def.BreakerThreshold ||
		params.HalfOpenInterval != def.HalfOpenInterval ||
		params.WeightTimeout != def.WeightTimeout ||
		params.WeightJitter != def.WeightJitter {
		t.Fatalf("invalid values did not fall back to defaults:\n got %+v\nwant %+v", params, def)
	}
	if window != metricsWindow {
		t.Fatalf("invalid window = %v, want default %v", window, metricsWindow)
	}

	// tie_band above switch_margin is rejected even if both individually
	// in range.
	conflict := MDSSchedulerConfig{
		SwitchMargin: configPtr(0.01),
		TieBand:      configPtr(0.2),
	}
	params, _ = applyMDSConfig(conflict)
	if params.SwitchMargin != def.SwitchMargin || params.TieBand != def.TieBand {
		t.Fatalf("tie_band/switch_margin conflict not reset: %+v", params)
	}
}

// The keys documented in example-dnscrypt-proxy.toml decode into Config.
func TestMDSConfigTOMLDecode(t *testing.T) {
	doc := `
lb_strategy = "mds"
[scheduler_mds]
warmup_samples = 25
penalty_min_samples = 5
switch_margin = 0.2
tie_band = 0.01
dwell = "3s"
exploration_probability = 0.05
circuit_breaker_threshold = 4
half_open_interval = "20s"
metrics_window = "10m"
weight_timeout = 0.8
weight_errors = 0.2
weight_servfail = 0.1
weight_dnssec_bogus = 0.6
weight_truncated = 0.03
weight_fallback = 0.15
weight_conn_new = 0.02
weight_jitter = 0.12
`
	var cfg Config
	if _, err := toml.Decode(doc, &cfg); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if cfg.LBStrategy != "mds" || cfg.SchedulerMDS.WarmupSamples == nil ||
		*cfg.SchedulerMDS.WarmupSamples != 25 || cfg.SchedulerMDS.Dwell == nil ||
		*cfg.SchedulerMDS.Dwell != "3s" || cfg.SchedulerMDS.MetricsWindow == nil ||
		*cfg.SchedulerMDS.MetricsWindow != "10m" {
		t.Fatalf("unexpected decode: %+v", cfg.SchedulerMDS)
	}
	params, window := applyMDSConfig(cfg.SchedulerMDS)
	if params.WarmupSamples != 25 || params.BreakerThreshold != 4 ||
		params.WeightTimeout != 0.8 || window != 10*time.Minute {
		t.Fatalf("decoded config not applied: %+v window=%v", params, window)
	}
}
