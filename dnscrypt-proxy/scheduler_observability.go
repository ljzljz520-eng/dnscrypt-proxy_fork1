package main

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jedisct1/dlog"
)

// ResolverReportRow is the strategy-neutral per-resolver observability
// view shared by structured logs, Prometheus export and the monitoring
// dashboard snapshot. It is assembled while holding ServersInfo's lock.
type ResolverReportRow struct {
	Name    string
	Warming bool

	Samples      uint64
	TotalSamples uint64

	HasP50           bool
	HasP95           bool
	HasP99           bool
	P50Ms            float64
	P95Ms            float64
	P99Ms            float64
	LatencyEwmaMs    float64
	JitterEwmaMs     float64
	TimeoutRate      float64
	ErrorRate        float64
	ServfailRate     float64
	BogusRate        float64
	TruncatedRate    float64
	TCPFallbackRate  float64
	QUICFallbackRate float64
	ConnNewRate      float64
	ECSRate          float64

	TimeoutCount      uint64
	ErrorCount        uint64
	ServfailCount     uint64
	BogusCount        uint64
	TruncatedCount    uint64
	TCPFallbackCount  uint64
	QUICFallbackCount uint64
	ConnNewCount      uint64
	ConnReusedCount   uint64
	ECSCount          uint64

	DNSSEC   bool
	NoLog    bool
	NoFilter bool

	Primary     bool
	CircuitOpen bool
	SwitchesTo  uint64

	// Score is the strategy-specific selection score (MDS base score;
	// WP2 score). It is 0 for the classic stateless strategies.
	Score float64
}

// SchedulerReport is the full periodic observability snapshot.
type SchedulerReport struct {
	Strategy      string
	PrimaryName   string
	TotalSwitches uint64
	OpenCircuits  []string
	Rows          []ResolverReportRow
}

// buildSchedulerReportLocked assembles the report from the paired
// candidate snapshot. Callers must hold ServersInfo's lock (RLock is
// sufficient: selection state is only read, never mutated).
func (serversInfo *ServersInfo) buildSchedulerReportLocked() SchedulerReport {
	candidates := serversInfo.candidatesLocked()
	rows := make([]ResolverReportRow, 0, len(candidates))

	mds, isMDS := serversInfo.lbStrategy.(*LBStrategyMDS)
	var mdsScored []mdsScored
	var mdsStats MDSTats
	if isMDS {
		mdsScored = mds.scoreAll(candidates)
		mdsStats = mds.Stats()
	}
	warmup := uint64(DefaultMDSParams().WarmupSamples)
	if isMDS {
		warmup = mds.params.WarmupSamples
	}

	for i, c := range candidates {
		w := c.Metrics.Snapshot()
		row := ResolverReportRow{
			Name:          c.Server.Name,
			Warming:       c.Metrics.Warm(warmup),
			Samples:       w.Samples,
			TotalSamples:  w.TotalSamples,
			LatencyEwmaMs: w.LatencyEwmaMs,
			JitterEwmaMs:  w.JitterEwmaMs,
			DNSSEC:        c.Server.SupportsDNSSEC(),
			NoLog:         c.Server.SupportsNoLog(),
			NoFilter:      c.Server.SupportsNoFilter(),
		}
		if d, ok := c.Metrics.Percentile(0.50); ok {
			row.HasP50, row.P50Ms = true, float64(d)/float64(time.Millisecond)
		}
		if d, ok := c.Metrics.Percentile(0.95); ok {
			row.HasP95, row.P95Ms = true, float64(d)/float64(time.Millisecond)
		}
		if d, ok := c.Metrics.Percentile(0.99); ok {
			row.HasP99, row.P99Ms = true, float64(d)/float64(time.Millisecond)
		}
		if w.Samples > 0 {
			s := float64(w.Samples)
			row.TimeoutRate = float64(w.Timeout) / s
			row.ErrorRate = float64(w.Errors) / s
			row.ServfailRate = float64(w.Servfail) / s
			row.BogusRate = float64(w.DNSSECBogus) / s
			row.TruncatedRate = float64(w.Truncated()) / s
			row.TCPFallbackRate = float64(w.TCPFallback) / s
			row.QUICFallbackRate = float64(w.QUICFallback) / s
			row.ConnNewRate = float64(w.ConnNew) / s
			row.ECSRate = float64(w.ECSReturned) / s
		}
		row.TimeoutCount = w.Timeout
		row.ErrorCount = w.Errors
		row.ServfailCount = w.Servfail
		row.BogusCount = w.DNSSECBogus
		row.TruncatedCount = w.Truncated()
		row.TCPFallbackCount = w.TCPFallback
		row.QUICFallbackCount = w.QUICFallback
		row.ConnNewCount = w.ConnNew
		row.ConnReusedCount = w.ConnReused
		row.ECSCount = w.ECSReturned

		if isMDS {
			row.Score = mdsScored[i].base
			// A post-breaker recovery probation is surfaced like warmup.
			row.Warming = row.Warming || mdsScored[i].warming
			row.Primary = mdsStats.PrimaryName == c.Server.Name
			for _, open := range mdsStats.OpenCircuits {
				if open == c.Server.Name {
					row.CircuitOpen = true
					break
				}
			}
			row.SwitchesTo = mdsStats.SwitchesByName[c.Server.Name]
		} else if _, isWP2 := serversInfo.lbStrategy.(LBStrategyWP2); isWP2 {
			row.Score = serversInfo.calculateServerScore(c.Server)
		}
		rows = append(rows, row)
	}

	report := SchedulerReport{Rows: rows, Strategy: schedulerStrategyName(serversInfo.lbStrategy)}
	if isMDS {
		report.PrimaryName = mdsStats.PrimaryName
		report.TotalSwitches = mdsStats.TotalSwitches
		report.OpenCircuits = mdsStats.OpenCircuits
	}
	return report
}

func schedulerStrategyName(s LBStrategy) string {
	switch s.(type) {
	case LBStrategyWP2:
		return "wp2"
	case *LBStrategyMDS:
		return "mds"
	case LBStrategyP2:
		return "p2"
	case LBStrategyPH:
		return "ph"
	case LBStrategyFirst:
		return "first"
	case LBStrategyRandom:
		return "random"
	default:
		return strings.ToLower(fmt.Sprintf("%T", s))
	}
}

func pct(x float64) string { return fmt.Sprintf("%.1f%%", x*100) }

func msOrDash(ok bool, ms float64) string {
	if !ok {
		return "-"
	}
	return fmt.Sprintf("%.1fms", ms)
}

// renderMDSReport renders the MDS report as deterministic debug-log lines.
func renderMDSReport(report SchedulerReport) []string {
	lines := make([]string, 0, len(report.Rows)+1)
	lines = append(lines, fmt.Sprintf(
		"MDS Strategy Server Statistics: primary=[%s] switches=%d open=%v",
		report.PrimaryName, report.TotalSwitches, report.OpenCircuits))
	for i, r := range report.Rows {
		props := make([]string, 0, 3)
		if r.DNSSEC {
			props = append(props, "dnssec")
		}
		if r.NoLog {
			props = append(props, "nolog")
		}
		if r.NoFilter {
			props = append(props, "nofilter")
		}
		primary := ""
		if r.Primary {
			primary = "*"
		}
		state := ""
		switch {
		case r.CircuitOpen:
			state = " open"
		case r.Warming:
			state = " warming"
		}
		lines = append(lines, fmt.Sprintf(
			"[%d]%s %s%s: samples=%d/%d p50=%s p95=%s p99=%s ewma=%.1fms jitter=%.1fms "+
				"timeout=%s errors=%s servfail=%s bogus=%s trunc=%s tcpfb=%s quicfb=%s connnew=%s ecs=%s "+
				"props=[%s] score=%.3f switches=%d",
			i, primary, r.Name, state,
			r.Samples, r.TotalSamples,
			msOrDash(r.HasP50, r.P50Ms), msOrDash(r.HasP95, r.P95Ms), msOrDash(r.HasP99, r.P99Ms),
			r.LatencyEwmaMs, r.JitterEwmaMs,
			pct(r.TimeoutRate), pct(r.ErrorRate), pct(r.ServfailRate), pct(r.BogusRate),
			pct(r.TruncatedRate), pct(r.TCPFallbackRate), pct(r.QUICFallbackRate),
			pct(r.ConnNewRate), pct(r.ECSRate),
			strings.Join(props, ","), r.Score, r.SwitchesTo))
	}
	return lines
}

// renderWP2StatsLocked reproduces the historical WP2 debug lines verbatim.
func (serversInfo *ServersInfo) renderWP2StatsLocked() []string {
	lines := make([]string, 0, len(serversInfo.inner)+1)
	lines = append(lines, "WP2 Strategy Server Statistics:")
	for i, server := range serversInfo.inner {
		if server == nil {
			continue
		}
		score := serversInfo.calculateServerScore(server)
		successRate := 1.0
		if server.totalQueries > 0 {
			successRate = float64(server.totalQueries-server.failedQueries) / float64(server.totalQueries)
		}
		lines = append(lines, fmt.Sprintf("[%d] %s: RTT=%dms, Score=%.3f, Success=%.2f%%, Queries=%d",
			i, server.Name, int(server.rtt.Value()), score, successRate*100, server.totalQueries))
	}
	return lines
}

// promEscapeLabel escapes a Prometheus label value.
func promEscapeLabel(v string) string {
	v = strings.ReplaceAll(v, "\\", "\\\\")
	v = strings.ReplaceAll(v, "\n", "\\n")
	return strings.ReplaceAll(v, "\"", "\\\"")
}

func promWriteHeader(w *strings.Builder, name, typ, help string) {
	w.WriteString("# HELP " + name + " " + help + "\n")
	w.WriteString("# TYPE " + name + " " + typ + "\n")
}

// appendResolverSchedulerMetrics appends the dnscrypt_proxy_resolver_*
// families (and the global scheduler switch counter) to a Prometheus
// exposition. Existing metric names are never renamed or removed.
func (mc *MetricsCollector) appendResolverSchedulerMetrics(result *strings.Builder) {
	if mc.proxy == nil {
		return
	}
	mc.proxy.serversInfo.RLock()
	report := mc.proxy.serversInfo.buildSchedulerReportLocked()
	mc.proxy.serversInfo.RUnlock()

	rows := report.Rows
	sort.Slice(rows, func(i, j int) bool { return rows[i].Name < rows[j].Name })

	promWriteHeader(result, "dnscrypt_proxy_resolver_latency_ms", "gauge",
		"Observed resolver latency quantiles in milliseconds")
	promWriteHeader(result, "dnscrypt_proxy_resolver_latency_ewma_ms", "gauge",
		"EWMA of resolver latency in milliseconds")
	promWriteHeader(result, "dnscrypt_proxy_resolver_jitter_ewma_ms", "gauge",
		"EWMA of resolver latency jitter in milliseconds")
	promWriteHeader(result, "dnscrypt_proxy_resolver_samples", "gauge",
		"Number of exchange samples in the current metric window")
	// Window counts are gauges, not counters: the sliding window rotates
	// and these values legitimately decrease. Exporting them as counters
	// would make Prometheus rate()/increase() emit phantom resets.
	promWriteHeader(result, "dnscrypt_proxy_resolver_timeout_window", "gauge", "Timeouts in the current metric window")
	promWriteHeader(result, "dnscrypt_proxy_resolver_errors_window", "gauge", "Non-timeout exchange errors in the current metric window")
	promWriteHeader(result, "dnscrypt_proxy_resolver_servfail_window", "gauge", "SERVFAIL responses in the current metric window")
	promWriteHeader(result, "dnscrypt_proxy_resolver_dnssec_bogus_window", "gauge", "DNSSEC bogus responses in the current metric window")
	promWriteHeader(result, "dnscrypt_proxy_resolver_truncated_window", "gauge", "Truncated responses in the current metric window")
	promWriteHeader(result, "dnscrypt_proxy_resolver_tcp_fallback_window", "gauge", "TCP fallbacks in the current metric window")
	promWriteHeader(result, "dnscrypt_proxy_resolver_quic_fallback_window", "gauge", "QUIC/H3-to-H2 fallbacks in the current metric window")
	promWriteHeader(result, "dnscrypt_proxy_resolver_conn_new_window", "gauge", "New upstream connections in the current metric window")
	promWriteHeader(result, "dnscrypt_proxy_resolver_conn_reused_window", "gauge", "Reused upstream connections in the current metric window")
	promWriteHeader(result, "dnscrypt_proxy_resolver_ecs_returned_window", "gauge", "Responses carrying a non-zero-scope ECS option in the current metric window")
	promWriteHeader(result, "dnscrypt_proxy_resolver_timeout_ratio", "gauge", "Timeout ratio over the metric window")
	promWriteHeader(result, "dnscrypt_proxy_resolver_error_ratio", "gauge", "Non-timeout error ratio over the metric window")
	promWriteHeader(result, "dnscrypt_proxy_resolver_servfail_ratio", "gauge", "SERVFAIL ratio over the metric window")
	promWriteHeader(result, "dnscrypt_proxy_resolver_dnssec_bogus_ratio", "gauge", "DNSSEC bogus ratio over the metric window")
	promWriteHeader(result, "dnscrypt_proxy_resolver_truncated_ratio", "gauge", "Truncated response ratio over the metric window")
	promWriteHeader(result, "dnscrypt_proxy_resolver_tcp_fallback_ratio", "gauge", "TCP fallback ratio over the metric window")
	promWriteHeader(result, "dnscrypt_proxy_resolver_quic_fallback_ratio", "gauge", "QUIC/H3-to-H2 fallback ratio over the metric window")
	promWriteHeader(result, "dnscrypt_proxy_resolver_conn_new_ratio", "gauge", "New-connection ratio over the metric window")
	promWriteHeader(result, "dnscrypt_proxy_resolver_ecs_scope_ratio", "gauge", "Ratio of responses carrying an ECS scope")
	promWriteHeader(result, "dnscrypt_proxy_resolver_warming", "gauge", "1 while the resolver is within its warmup sample budget")
	promWriteHeader(result, "dnscrypt_proxy_resolver_features", "gauge",
		"Static resolver stamp features (1: feature claimed)")
	promWriteHeader(result, "dnscrypt_proxy_resolver_primary", "gauge",
		"1 for the current sticky primary (MDS strategy)")
	promWriteHeader(result, "dnscrypt_proxy_resolver_circuit_open", "gauge",
		"1 while the resolver circuit breaker is open")
	promWriteHeader(result, "dnscrypt_proxy_resolver_score", "gauge",
		"Strategy-specific scheduler selection score")
	promWriteHeader(result, "dnscrypt_proxy_resolver_switch_total", "counter",
		"Primary switches toward this resolver")
	promWriteHeader(result, "dnscrypt_proxy_scheduler_switch_total", "counter",
		"Total primary switches performed by the scheduler")

	result.WriteString(fmt.Sprintf("dnscrypt_proxy_scheduler_switch_total{strategy=\"%s\"} %d\n",
		promEscapeLabel(report.Strategy), report.TotalSwitches))

	boolGauge := func(b bool) string {
		if b {
			return "1"
		}
		return "0"
	}
	for _, r := range rows {
		s := promEscapeLabel(r.Name)
		writeQuantile := func(q string, ok bool, ms float64) {
			if ok {
				result.WriteString(fmt.Sprintf(
					"dnscrypt_proxy_resolver_latency_ms{server=\"%s\",quantile=\"%s\"} %.2f\n", s, q, ms))
			}
		}
		writeQuantile("50", r.HasP50, r.P50Ms)
		writeQuantile("95", r.HasP95, r.P95Ms)
		writeQuantile("99", r.HasP99, r.P99Ms)
		result.WriteString(fmt.Sprintf("dnscrypt_proxy_resolver_latency_ewma_ms{server=\"%s\"} %.2f\n", s, r.LatencyEwmaMs))
		result.WriteString(fmt.Sprintf("dnscrypt_proxy_resolver_jitter_ewma_ms{server=\"%s\"} %.2f\n", s, r.JitterEwmaMs))
		result.WriteString(fmt.Sprintf("dnscrypt_proxy_resolver_samples{server=\"%s\"} %d\n", s, r.Samples))
		result.WriteString(fmt.Sprintf("dnscrypt_proxy_resolver_timeout_window{server=\"%s\"} %d\n", s, r.TimeoutCount))
		result.WriteString(fmt.Sprintf("dnscrypt_proxy_resolver_errors_window{server=\"%s\"} %d\n", s, r.ErrorCount))
		result.WriteString(fmt.Sprintf("dnscrypt_proxy_resolver_servfail_window{server=\"%s\"} %d\n", s, r.ServfailCount))
		result.WriteString(fmt.Sprintf("dnscrypt_proxy_resolver_dnssec_bogus_window{server=\"%s\"} %d\n", s, r.BogusCount))
		result.WriteString(fmt.Sprintf("dnscrypt_proxy_resolver_truncated_window{server=\"%s\"} %d\n", s, r.TruncatedCount))
		result.WriteString(fmt.Sprintf("dnscrypt_proxy_resolver_tcp_fallback_window{server=\"%s\"} %d\n", s, r.TCPFallbackCount))
		result.WriteString(fmt.Sprintf("dnscrypt_proxy_resolver_quic_fallback_window{server=\"%s\"} %d\n", s, r.QUICFallbackCount))
		result.WriteString(fmt.Sprintf("dnscrypt_proxy_resolver_conn_new_window{server=\"%s\"} %d\n", s, r.ConnNewCount))
		result.WriteString(fmt.Sprintf("dnscrypt_proxy_resolver_conn_reused_window{server=\"%s\"} %d\n", s, r.ConnReusedCount))
		result.WriteString(fmt.Sprintf("dnscrypt_proxy_resolver_ecs_returned_window{server=\"%s\"} %d\n", s, r.ECSCount))
		result.WriteString(fmt.Sprintf("dnscrypt_proxy_resolver_timeout_ratio{server=\"%s\"} %.4f\n", s, r.TimeoutRate))
		result.WriteString(fmt.Sprintf("dnscrypt_proxy_resolver_error_ratio{server=\"%s\"} %.4f\n", s, r.ErrorRate))
		result.WriteString(fmt.Sprintf("dnscrypt_proxy_resolver_servfail_ratio{server=\"%s\"} %.4f\n", s, r.ServfailRate))
		result.WriteString(fmt.Sprintf("dnscrypt_proxy_resolver_dnssec_bogus_ratio{server=\"%s\"} %.4f\n", s, r.BogusRate))
		result.WriteString(fmt.Sprintf("dnscrypt_proxy_resolver_truncated_ratio{server=\"%s\"} %.4f\n", s, r.TruncatedRate))
		result.WriteString(fmt.Sprintf("dnscrypt_proxy_resolver_tcp_fallback_ratio{server=\"%s\"} %.4f\n", s, r.TCPFallbackRate))
		result.WriteString(fmt.Sprintf("dnscrypt_proxy_resolver_quic_fallback_ratio{server=\"%s\"} %.4f\n", s, r.QUICFallbackRate))
		result.WriteString(fmt.Sprintf("dnscrypt_proxy_resolver_conn_new_ratio{server=\"%s\"} %.4f\n", s, r.ConnNewRate))
		result.WriteString(fmt.Sprintf("dnscrypt_proxy_resolver_ecs_scope_ratio{server=\"%s\"} %.4f\n", s, r.ECSRate))
		result.WriteString(fmt.Sprintf("dnscrypt_proxy_resolver_warming{server=\"%s\"} %s\n", s, boolGauge(r.Warming)))
		result.WriteString(fmt.Sprintf(
			"dnscrypt_proxy_resolver_features{server=\"%s\",dnssec=\"%t\",nolog=\"%t\",nofilter=\"%t\"} 1\n",
			s, r.DNSSEC, r.NoLog, r.NoFilter))
		result.WriteString(fmt.Sprintf("dnscrypt_proxy_resolver_primary{server=\"%s\"} %s\n", s, boolGauge(r.Primary)))
		result.WriteString(fmt.Sprintf("dnscrypt_proxy_resolver_circuit_open{server=\"%s\"} %s\n", s, boolGauge(r.CircuitOpen)))
		result.WriteString(fmt.Sprintf("dnscrypt_proxy_resolver_score{server=\"%s\"} %.4f\n", s, r.Score))
		result.WriteString(fmt.Sprintf("dnscrypt_proxy_resolver_switch_total{server=\"%s\"} %d\n", s, r.SwitchesTo))
	}
}

// logSchedulerStats emits the periodic strategy-specific debug output. The
// WP2 output is unchanged; MDS adds the full multi-dimensional view.
func (serversInfo *ServersInfo) logSchedulerStats() {
	serversInfo.RLock()
	defer serversInfo.RUnlock()

	switch serversInfo.lbStrategy.(type) {
	case LBStrategyWP2:
		for _, line := range serversInfo.renderWP2StatsLocked() {
			dlog.Debug(line)
		}
	case *LBStrategyMDS:
		for _, line := range renderMDSReport(serversInfo.buildSchedulerReportLocked()) {
			dlog.Debug(line)
		}
	}
}
