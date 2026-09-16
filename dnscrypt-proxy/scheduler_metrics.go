package main

import (
	"math"
	"sort"
	"sync"
	"time"
)

// Per-resolver multi-dimensional metrics used by the MDS (Multi-Dimensional
// Scheduler) strategy and exposed through logs/Prometheus/the dashboard.
//
// The latency distribution is tracked as a fixed-bucket histogram spread over
// a fixed number of rotating epochs (sliding window). All storage is bounded:
// memory consumption depends only on the number of buckets and epochs, never
// on the number of observed queries.
const (
	// metricsWindow is the total sliding window retained for rates/histograms.
	metricsWindow = 5 * time.Minute
	// metricsEpochCount is the number of epochs the window is split into.
	metricsEpochCount = 10
	// metricsEpochDur is the duration of a single epoch.
	metricsEpochDur = metricsWindow / metricsEpochCount

	// latencyBucketGrowth defines the geometric growth of latency bucket
	// upper bounds in milliseconds. The first bucket covers up to ~1ms and
	// buckets grow by 25% each step until latencyBucketMaxMs is reached.
	latencyBucketFirstMs = 1.0
	latencyBucketGrowth  = 1.25
	latencyBucketMaxMs   = 10000.0

	// latencyEwmaSpan controls the EWMA smoothing (alpha = 2/(span+1)).
	latencyEwmaSpan = 10.0
)

// ExchangeTransport identifies the transport that carried the final upstream
// attempt of an exchange.
type ExchangeTransport uint8

const (
	TransportUnknown ExchangeTransport = iota
	TransportDNSCryptUDP
	TransportDNSCryptTCP
	TransportDoHH2
	TransportDoHH3
	TransportODoH
)

// ExchangeOutcome is the single, rich feedback record emitted after every
// upstream exchange (success or failure).
type ExchangeOutcome struct {
	// Duration of the exchange (waiting until the deadline on timeout, which
	// naturally penalizes timeouts in the latency distribution).
	Duration time.Duration
	// Transport that served the final attempt.
	Transport ExchangeTransport

	Timeout           bool // exchange ended in a timeout
	Error             bool // non-timeout failure before any response (network/parse)
	Servfail          bool // upstream returned SERVFAIL (RCODE 2)
	DNSSECBogus       bool // SERVFAIL received while DNSSEC validation was expected
	UpstreamTruncated bool // upstream set the TC bit on a UDP response
	LocalTruncated    bool // the proxy had to truncate the response to the client
	TCPFallback       bool // retried over TCP after a UDP timeout/TC response
	QUICFallback      bool // fell back from HTTP/3 to HTTP/2
	ConnReused        bool // an existing connection/session was reused

	// ECSReturned is true when the response carried an EDNS CLIENT-SUBNET
	// option with a non-zero scope prefix.
	ECSReturned bool
}

// metricsEpoch holds the counters and histogram buckets for a single time
// slice. A fresh epoch is zero-valued, so latencyBuckets is allocated eagerly
// by newMetricsEpoch.
type metricsEpoch struct {
	latencyBuckets []uint64

	total             uint64
	success           uint64
	timeout           uint64
	errors            uint64
	servfail          uint64
	dnssecBogus       uint64
	upstreamTruncated uint64
	localTruncated    uint64
	tcpFallback       uint64
	quicFallback      uint64
	connNew           uint64
	connReused        uint64
	ecsReturned       uint64
}

func newMetricsEpoch(bucketCount int) metricsEpoch {
	return metricsEpoch{latencyBuckets: make([]uint64, bucketCount)}
}

// reset zeroes an epoch that re-enters rotation.
func (e *metricsEpoch) reset() {
	for i := range e.latencyBuckets {
		e.latencyBuckets[i] = 0
	}
	e.total = 0
	e.success = 0
	e.timeout = 0
	e.errors = 0
	e.servfail = 0
	e.dnssecBogus = 0
	e.upstreamTruncated = 0
	e.localTruncated = 0
	e.tcpFallback = 0
	e.quicFallback = 0
	e.connNew = 0
	e.connReused = 0
	e.ecsReturned = 0
}

// MetricsWindow is a point-in-time aggregation of the sliding-window counters
// plus the cumulative (all-time) information used for warmup gating.
type MetricsWindow struct {
	Samples uint64 // outcomes in the current window
	// TotalSamples is cumulative since registration, including samples that
	// have already aged out of the sliding window.
	TotalSamples uint64

	Success           uint64
	Timeout           uint64
	Errors            uint64
	Servfail          uint64
	DNSSECBogus       uint64
	UpstreamTruncated uint64
	LocalTruncated    uint64
	TCPFallback       uint64
	QUICFallback      uint64
	ConnNew           uint64
	ConnReused        uint64
	ECSReturned       uint64

	LatencyEwmaMs float64
	JitterEwmaMs  float64

	FirstSeen time.Time
	LastSeen  time.Time
}

// Truncated is the total number of responses affected by truncation, either
// signaled by the upstream or applied locally.
func (w MetricsWindow) Truncated() uint64 {
	return w.UpstreamTruncated + w.LocalTruncated
}

// ResolverMetrics is the bounded per-resolver metric store.
type ResolverMetrics struct {
	mu sync.Mutex

	epochs     [metricsEpochCount]metricsEpoch
	curEpoch   int
	epochStart time.Time

	// totalSamples accumulates over the whole lifetime of the resolver
	// (surviving window rotation). It is the denominator for warmup.
	totalSamples uint64
	firstSeen    time.Time
	lastSeen     time.Time

	latencyEwmaMs float64
	jitterEwmaMs  float64
	hasLatency    bool
	prevLatencyMs float64

	window   time.Duration
	epochDur time.Duration

	now func() time.Time
}

// latencyBucketEdges holds the geometric upper bounds (in ms) of the latency
// buckets.
var latencyBucketEdges = buildLatencyBucketEdges()

func buildLatencyBucketEdges() []float64 {
	edges := make([]float64, 0, 48)
	for v := latencyBucketFirstMs; v < latencyBucketMaxMs; v *= latencyBucketGrowth {
		edges = append(edges, v)
	}
	edges = append(edges, latencyBucketMaxMs)
	return edges
}

// latencyBucketIndex returns the bucket for a latency in milliseconds. The
// first and last buckets saturate.
func latencyBucketIndex(latencyMs float64) int {
	if latencyMs <= latencyBucketEdges[0] {
		return 0
	}
	if latencyMs >= latencyBucketMaxMs {
		return len(latencyBucketEdges) - 1
	}
	return sort.SearchFloat64s(latencyBucketEdges, latencyMs)
}

// NewResolverMetrics creates an empty metric store with the default window.
func NewResolverMetrics() *ResolverMetrics {
	return NewResolverMetricsWithWindow(metricsWindow)
}

// NewResolverMetricsWithWindow creates an empty metric store retaining the
// given total window length (split into metricsEpochCount epochs). A
// non-positive window falls back to the default.
func NewResolverMetricsWithWindow(window time.Duration) *ResolverMetrics {
	if window <= 0 {
		window = metricsWindow
	}
	m := &ResolverMetrics{
		now:      time.Now,
		window:   window,
		epochDur: window / time.Duration(metricsEpochCount),
	}
	for i := range m.epochs {
		m.epochs[i] = newMetricsEpoch(len(latencyBucketEdges))
	}
	return m
}

// Window returns the configured sliding-window length.
func (m *ResolverMetrics) Window() time.Duration {
	return m.window
}

// newResolverMetricsWithClock is the test-only constructor with an injectable
// clock.
func newResolverMetricsWithClock(now func() time.Time) *ResolverMetrics {
	m := NewResolverMetrics()
	m.now = now
	return m
}

// advanceLocked rotates epochs until epochStart covers now. An idle store that
// wakes up after more than the window length simply starts from an empty
// window.
func (m *ResolverMetrics) advanceLocked(now time.Time) {
	if m.epochStart.IsZero() {
		m.epochStart = now
		return
	}
	steps := int(now.Sub(m.epochStart) / m.epochDur)
	if steps <= 0 {
		return
	}
	if steps >= metricsEpochCount {
		for i := range m.epochs {
			m.epochs[i].reset()
		}
		m.curEpoch = (m.curEpoch + steps) % metricsEpochCount
	} else {
		for range steps {
			m.curEpoch = (m.curEpoch + 1) % metricsEpochCount
			m.epochs[m.curEpoch].reset()
		}
	}
	m.epochStart = m.epochStart.Add(time.Duration(steps) * m.epochDur)
}

// Observe records one exchange outcome.
func (m *ResolverMetrics) Observe(o ExchangeOutcome) {
	now := m.now()

	latencyMs := float64(o.Duration) / float64(time.Millisecond)
	if latencyMs < 0 {
		latencyMs = 0
	}
	alpha := 2.0 / (latencyEwmaSpan + 1.0)

	m.mu.Lock()
	defer m.mu.Unlock()

	m.advanceLocked(now)

	if m.firstSeen.IsZero() {
		m.firstSeen = now
	}
	m.lastSeen = now
	m.totalSamples++

	if !m.hasLatency {
		m.latencyEwmaMs = latencyMs
		m.hasLatency = true
	} else {
		delta := math.Abs(latencyMs - m.prevLatencyMs)
		m.jitterEwmaMs = alpha*delta + (1-alpha)*m.jitterEwmaMs
		m.latencyEwmaMs = alpha*latencyMs + (1-alpha)*m.latencyEwmaMs
	}
	m.prevLatencyMs = latencyMs

	e := &m.epochs[m.curEpoch]
	e.total++
	if !o.Timeout && !o.Error && !o.Servfail && !o.DNSSECBogus {
		e.success++
	}
	if o.Timeout {
		e.timeout++
	}
	if o.Error {
		e.errors++
	}
	if o.Servfail {
		e.servfail++
	}
	if o.DNSSECBogus {
		e.dnssecBogus++
	}
	if o.UpstreamTruncated {
		e.upstreamTruncated++
	}
	if o.LocalTruncated {
		e.localTruncated++
	}
	if o.TCPFallback {
		e.tcpFallback++
	}
	if o.QUICFallback {
		e.quicFallback++
	}
	if o.ConnReused {
		e.connReused++
	} else {
		e.connNew++
	}
	if o.ECSReturned {
		e.ecsReturned++
	}
	e.latencyBuckets[latencyBucketIndex(latencyMs)]++
}

// aggregateLocked sums all rotated epochs into a fresh window view. Callers
// must hold m.mu and have called advanceLocked for an up-to-date window.
func (m *ResolverMetrics) aggregateLocked() MetricsWindow {
	w := MetricsWindow{
		TotalSamples:  m.totalSamples,
		LatencyEwmaMs: m.latencyEwmaMs,
		JitterEwmaMs:  m.jitterEwmaMs,
		FirstSeen:     m.firstSeen,
		LastSeen:      m.lastSeen,
	}
	for i := range m.epochs {
		e := &m.epochs[i]
		w.Samples += e.total
		w.Success += e.success
		w.Timeout += e.timeout
		w.Errors += e.errors
		w.Servfail += e.servfail
		w.DNSSECBogus += e.dnssecBogus
		w.UpstreamTruncated += e.upstreamTruncated
		w.LocalTruncated += e.localTruncated
		w.TCPFallback += e.tcpFallback
		w.QUICFallback += e.quicFallback
		w.ConnNew += e.connNew
		w.ConnReused += e.connReused
		w.ECSReturned += e.ecsReturned
	}
	return w
}

// Snapshot returns the aggregated sliding-window counters.
func (m *ResolverMetrics) Snapshot() MetricsWindow {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.advanceLocked(m.now())
	return m.aggregateLocked()
}

// Percentile returns the nearest-rank latency quantile (p in [0,1]) over the
// sliding window. The representative value is the upper bound of the selected
// histogram bucket, so the error is at most one bucket width. ok is false when
// the window has no latency sample.
func (m *ResolverMetrics) Percentile(p float64) (time.Duration, bool) {
	d, ok, _ := m.PercentileBuf(p, nil)
	return d, ok
}

// PercentileBuf is Percentile with a caller-provided bucket accumulator, so
// callers evaluating many resolvers (the scheduler hot path) allocate once
// instead of once per resolver. The returned slice may differ from the one
// passed in when its capacity was insufficient; reuse the returned value.
func (m *ResolverMetrics) PercentileBuf(p float64, aggregated []uint64) (time.Duration, bool, []uint64) {
	if p < 0 {
		p = 0
	} else if p > 1 {
		p = 1
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.advanceLocked(m.now())

	bucketCount := len(latencyBucketEdges)
	if cap(aggregated) < bucketCount {
		aggregated = make([]uint64, bucketCount)
	} else {
		aggregated = aggregated[:bucketCount]
		clear(aggregated)
	}
	var total uint64
	for i := range m.epochs {
		total += m.epochs[i].total
		for b, c := range m.epochs[i].latencyBuckets {
			aggregated[b] += c
		}
	}
	if total == 0 {
		return 0, false, aggregated
	}
	rank := uint64(math.Ceil(float64(total) * p))
	if rank == 0 {
		rank = 1
	}
	var cumulative uint64
	for b, c := range aggregated {
		cumulative += c
		if cumulative >= rank {
			return time.Duration(latencyBucketEdges[b] * float64(time.Millisecond)), true, aggregated
		}
	}
	return time.Duration(latencyBucketEdges[bucketCount-1] * float64(time.Millisecond)), true, aggregated
}

// Warm reports whether the resolver still lacks enough real (non-bootstrap)
// samples to be judged by failure-derived penalties.
func (m *ResolverMetrics) Warm(warmupSamples uint64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.totalSamples < warmupSamples
}

// TotalSamples returns the lifetime sample count.
func (m *ResolverMetrics) TotalSamples() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.totalSamples
}
