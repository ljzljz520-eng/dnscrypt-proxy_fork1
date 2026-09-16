package main

import (
	"encoding/json"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	ewma "github.com/VividCortex/ewma"
	stamps "github.com/jedisct1/go-dnsstamps"
)

func newObservabilityProxy(t *testing.T) *Proxy {
	t.Helper()
	proxy := NewProxy()
	si := &proxy.serversInfo
	si.Lock()
	si.inner = append(si.inner,
		&ServerInfo{Name: "resolv-a", initialRtt: 10, rtt: ewma.NewMovingAverage(RTTEwmaDecay),
			Props: stamps.ServerInformalPropertyDNSSEC | stamps.ServerInformalPropertyNoFilter},
		&ServerInfo{Name: `weird"q\z`, initialRtt: 20, rtt: ewma.NewMovingAverage(RTTEwmaDecay)},
	)
	si.Unlock()

	ma := si.MetricsFor("resolv-a")
	feedOutcomes(ma, 20, 10*time.Millisecond, nil)
	feedOutcomes(ma, 2, 5*time.Second, func(o *ExchangeOutcome) { o.Timeout = true })
	feedOutcomes(ma, 1, 20*time.Millisecond, func(o *ExchangeOutcome) { o.Servfail = true })
	feedOutcomes(ma, 1, 20*time.Millisecond, func(o *ExchangeOutcome) { o.DNSSECBogus = true })
	feedOutcomes(ma, 1, 30*time.Millisecond, func(o *ExchangeOutcome) { o.UpstreamTruncated = true })
	feedOutcomes(ma, 1, 40*time.Millisecond, func(o *ExchangeOutcome) { o.TCPFallback = true })
	feedOutcomes(ma, 1, 45*time.Millisecond, func(o *ExchangeOutcome) { o.QUICFallback = true })
	feedOutcomes(ma, 1, 80*time.Millisecond, func(o *ExchangeOutcome) { o.ConnReused = false })
	feedOutcomes(ma, 2, 12*time.Millisecond, func(o *ExchangeOutcome) { o.ECSReturned = true })

	mb := si.MetricsFor(`weird"q\z`)
	feedOutcomes(mb, 20, 20*time.Millisecond, nil)
	return proxy
}

// TR-7.1: log rendering covers every collected dimension for MDS and the
// historical WP2 lines are preserved.
func TestSchedulerLogRendering(t *testing.T) {
	proxy := newObservabilityProxy(t)
	si := &proxy.serversInfo

	si.RLock()
	wp2Lines := si.renderWP2StatsLocked()
	si.RUnlock()
	wp2Text := strings.Join(wp2Lines, "\n")
	for _, want := range []string{"WP2 Strategy Server Statistics:", "RTT=", "Score=", "Success=", "Queries="} {
		if !strings.Contains(wp2Text, want) {
			t.Fatalf("WP2 log line %q missing: %s", want, wp2Text)
		}
	}

	mds := newLBStrategyMDSWithDeps(DefaultMDSParams(), time.Now, rand.New(rand.NewSource(1)))
	si.Lock()
	si.lbStrategy = mds
	idx := mds.selectCandidateLocked(si.candidatesLocked())
	si.Unlock()
	if idx < 0 {
		t.Fatal("no candidate selected")
	}

	si.RLock()
	report := si.buildSchedulerReportLocked()
	si.RUnlock()
	text := strings.Join(renderMDSReport(report), "\n")
	for _, want := range []string{
		"MDS Strategy Server Statistics:", "primary=[", "switches=",
		"samples=", "p50=", "p95=", "p99=", "ewma=", "jitter=",
		"timeout=", "errors=", "servfail=", "bogus=", "trunc=",
		"tcpfb=", "quicfb=", "connnew=", "ecs=", "props=[", "score=",
		"dnssec", "nofilter",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("MDS log missing %q:\n%s", want, text)
		}
	}
}

// TR-8.1: Prometheus exposition contains every new resolver family, keeps
// legacy metric names and escapes label metacharacters.
func TestResolverPrometheusMetrics(t *testing.T) {
	proxy := newObservabilityProxy(t)
	mc := &MetricsCollector{prometheusEnabled: true, proxy: proxy}
	out := mc.generatePrometheusMetrics()

	for _, name := range []string{
		"dnscrypt_proxy_resolver_latency_ms",
		"dnscrypt_proxy_resolver_latency_ewma_ms",
		"dnscrypt_proxy_resolver_jitter_ewma_ms",
		"dnscrypt_proxy_resolver_timeout_window",
		"dnscrypt_proxy_resolver_servfail_window",
		"dnscrypt_proxy_resolver_dnssec_bogus_window",
		"dnscrypt_proxy_resolver_truncated_window",
		"dnscrypt_proxy_resolver_tcp_fallback_window",
		"dnscrypt_proxy_resolver_quic_fallback_window",
		"dnscrypt_proxy_resolver_conn_new_window",
		"dnscrypt_proxy_resolver_conn_reused_window",
		"dnscrypt_proxy_resolver_ecs_returned_window",
		"dnscrypt_proxy_resolver_timeout_ratio",
		"dnscrypt_proxy_resolver_servfail_ratio",
		"dnscrypt_proxy_resolver_truncated_ratio",
		"dnscrypt_proxy_resolver_tcp_fallback_ratio",
		"dnscrypt_proxy_resolver_quic_fallback_ratio",
		"dnscrypt_proxy_resolver_conn_new_ratio",
		"dnscrypt_proxy_resolver_ecs_scope_ratio",
		"dnscrypt_proxy_resolver_warming",
		"dnscrypt_proxy_resolver_features",
		"dnscrypt_proxy_resolver_primary",
		"dnscrypt_proxy_resolver_circuit_open",
		"dnscrypt_proxy_resolver_score",
		"dnscrypt_proxy_resolver_switch_total",
		"dnscrypt_proxy_scheduler_switch_total",
		// Legacy metric names retained.
		"dnscrypt_proxy_queries_total",
		"dnscrypt_proxy_server_queries_total",
	} {
		if !strings.Contains(out, name) {
			t.Fatalf("prometheus output missing %s", name)
		}
	}
	if !strings.Contains(out, `quantile="95"`) {
		t.Fatal("p95 quantile label missing")
	}
	// Sliding-window counts decrease on epoch rotation: they must be
	// gauges; only the switch families are genuine monotonic counters.
	for _, gauge := range []string{
		"dnscrypt_proxy_resolver_timeout_window",
		"dnscrypt_proxy_resolver_tcp_fallback_window",
		"dnscrypt_proxy_resolver_conn_reused_window",
	} {
		if !strings.Contains(out, "# TYPE "+gauge+" gauge") {
			t.Fatalf("%s must be declared as a gauge", gauge)
		}
	}
	if !strings.Contains(out, "# TYPE dnscrypt_proxy_scheduler_switch_total counter") {
		t.Fatal("dnscrypt_proxy_scheduler_switch_total must be a counter")
	}
	// Label escaping: the raw name must not appear unescaped.
	if strings.Contains(out, `server="weird"`) {
		t.Fatal("unescaped quote in server label")
	}
	if !strings.Contains(out, `server="weird\"q\\z"`) {
		t.Fatalf("escaped server label missing, snippet:\n%s", out)
	}
}

// TR-9.1: dashboard snapshot JSON carries the new fields while keeping the
// legacy ones, plus a scheduler summary.
func TestResolverSnapshotJSON(t *testing.T) {
	proxy := newObservabilityProxy(t)
	mds := newLBStrategyMDSWithDeps(DefaultMDSParams(), time.Now, rand.New(rand.NewSource(1)))
	si := &proxy.serversInfo
	si.Lock()
	si.lbStrategy = mds
	mds.selectCandidateLocked(si.candidatesLocked())
	si.Unlock()

	mc := &MetricsCollector{proxy: proxy}
	metrics := mc.GetMetrics()
	raw, err := json.Marshal(metrics)
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)

	// Legacy fields unchanged.
	for _, want := range []string{`"name"`, `"status"`, `"success_rate"`, `"total_queries"`, `"failed_queries"`} {
		if !strings.Contains(text, want) {
			t.Fatalf("legacy snapshot field %s missing", want)
		}
	}
	// New multi-dimensional fields.
	for _, want := range []string{
		`"p95_ms"`, `"p99_ms"`, `"jitter_ewma_ms"`,
		`"timeout_rate"`, `"servfail_rate"`, `"dnssec_bogus_rate"`,
		`"truncated_rate"`, `"tcp_fallback_rate"`, `"quic_fallback_rate"`,
		`"conn_new_rate"`, `"ecs_scope_rate"`,
		`"tcp_fallback_total"`, `"quic_fallback_total"`,
		`"conn_new_total"`, `"conn_reused_total"`,
		`"feature_dnssec"`, `"feature_nofilter"`, `"warming"`,
		`"primary"`, `"circuit_open"`, `"scheduler_score"`, `"switches_to"`,
		`"scheduler"`, `"strategy":"mds"`, `"switches_total"`,
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("snapshot JSON missing %s", want)
		}
	}
}

// TR-9.2: dashboard table and renderer know the new columns and degrade
// missing values.
func TestDashboardStaticAssets(t *testing.T) {
	html, err := os.ReadFile("static/templates/dashboard.html")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"<th>P95</th>", "<th>Timeout%</th>", "<th>SERVFAIL</th>",
		"<th>Trunc%</th>", "<th>Fallbacks</th>", "<th>Jitter</th>",
		"<th>Features</th>",
	} {
		if !strings.Contains(string(html), want) {
			t.Fatalf("dashboard.html missing %s", want)
		}
	}

	js, err := os.ReadFile("static/js/monitoring.js")
	if err != nil {
		t.Fatal(err)
	}
	text := string(js)
	for _, want := range []string{
		"resolver.p95_ms", "resolver.timeout_rate", "resolver.servfail_rate",
		"resolver.truncated_rate", "resolver.tcp_fallback_total",
		"resolver.quic_fallback_total", "resolver.jitter_ewma_ms",
		"resolver.feature_dnssec", "resolver.feature_nofilter",
		"resolver.ecs_scope_rate", "colSpan = 14",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("monitoring.js missing %s", want)
		}
	}
}
