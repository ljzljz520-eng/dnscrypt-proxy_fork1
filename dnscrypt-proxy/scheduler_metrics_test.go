package main

import (
	"math"
	"sync"
	"testing"
	"time"
)

func msToDuration(ms float64) time.Duration {
	return time.Duration(ms * float64(time.Millisecond))
}

// rawQuantile computes the nearest-rank quantile of the given sorted sample
// set, mirroring the histogram semantics.
func rawQuantile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	rank := int(math.Ceil(float64(len(sorted)) * p))
	if rank <= 0 {
		rank = 1
	}
	if rank > len(sorted) {
		rank = len(sorted)
	}
	return sorted[rank-1]
}

// bucketWidthAt returns the width of the bucket that contains value v.
func bucketWidthAt(v float64) float64 {
	idx := latencyBucketIndex(v)
	if idx == 0 {
		return latencyBucketEdges[0]
	}
	return latencyBucketEdges[idx] - latencyBucketEdges[idx-1]
}

func TestResolverMetricsPercentiles(t *testing.T) {
	m := NewResolverMetrics()

	samples := []float64{10, 20, 30, 40, 50, 60, 70, 80, 90, 100}
	for _, ms := range samples {
		m.Observe(ExchangeOutcome{Duration: msToDuration(ms), ConnReused: true})
	}

	for _, p := range []float64{0.50, 0.95, 0.99} {
		got, ok := m.Percentile(p)
		if !ok {
			t.Fatalf("p%.2f: expected a quantile", p)
		}
		raw := rawQuantile(samples, p)
		gotMs := float64(got) / float64(time.Millisecond)
		tolerance := bucketWidthAt(raw)
		if math.Abs(gotMs-raw) > tolerance {
			t.Fatalf("p%.2f = %.2fms, raw nearest-rank %.2fms, tolerance %.2fms", p, gotMs, raw, tolerance)
		}
	}

	if _, ok := NewResolverMetrics().Percentile(0.99); ok {
		t.Fatal("empty metrics must report no quantile")
	}
}

func TestResolverMetricsCounters(t *testing.T) {
	m := NewResolverMetrics()

	outcomes := []ExchangeOutcome{
		{Duration: msToDuration(10), Transport: TransportDNSCryptUDP, ConnReused: true},
		{Duration: msToDuration(12), Transport: TransportDNSCryptUDP, ConnReused: true},
		{Duration: msToDuration(4990), Transport: TransportDNSCryptUDP, Timeout: true, TCPFallback: true},
		{Duration: msToDuration(30), Servfail: true},
		{Duration: msToDuration(31), Servfail: true, DNSSECBogus: true},
		{Duration: msToDuration(40), UpstreamTruncated: true, TCPFallback: true, ConnReused: true},
		{Duration: msToDuration(41), LocalTruncated: true, ConnReused: true},
		{Duration: msToDuration(50), Transport: TransportDoHH3, QUICFallback: true},
		{Duration: msToDuration(51), ECSReturned: true, ConnReused: true},
	}
	for _, o := range outcomes {
		m.Observe(o)
	}

	w := m.Snapshot()
	if w.Samples != uint64(len(outcomes)) || w.TotalSamples != uint64(len(outcomes)) {
		t.Fatalf("unexpected sample counts: %+v", w)
	}
	// Success means no timeout/SERVFAIL/bogus; truncated or fallbacked
	// exchanges that still returned a response are successful.
	if w.Success != 6 {
		t.Fatalf("Success = %d, want 6", w.Success)
	}
	if w.Timeout != 1 || w.Servfail != 2 || w.DNSSECBogus != 1 {
		t.Fatalf("failure counters wrong: %+v", w)
	}
	if w.UpstreamTruncated != 1 || w.LocalTruncated != 1 || w.Truncated() != 2 {
		t.Fatalf("truncation counters wrong: %+v", w)
	}
	if w.TCPFallback != 2 || w.QUICFallback != 1 {
		t.Fatalf("fallback counters wrong: %+v", w)
	}
	if w.ConnReused != 5 || w.ConnNew != 4 {
		t.Fatalf("connection counters wrong: reused=%d new=%d", w.ConnReused, w.ConnNew)
	}
	if w.ECSReturned != 1 {
		t.Fatalf("ECSReturned = %d, want 1", w.ECSReturned)
	}
	if w.FirstSeen.IsZero() || w.LastSeen.IsZero() {
		t.Fatal("first/last seen must be set")
	}
}

func TestResolverMetricsJitterEwma(t *testing.T) {
	m := NewResolverMetrics()

	latencies := []float64{10, 14, 10, 14}
	for _, ms := range latencies {
		m.Observe(ExchangeOutcome{Duration: msToDuration(ms)})
	}

	const alpha = 2.0 / (latencyEwmaSpan + 1.0)
	var wantJitter float64
	for i := 1; i < len(latencies); i++ {
		delta := math.Abs(latencies[i] - latencies[i-1])
		wantJitter = alpha*delta + (1-alpha)*wantJitter
	}
	w := m.Snapshot()
	if math.Abs(w.JitterEwmaMs-wantJitter) > 1e-9 {
		t.Fatalf("jitter ewma = %.6f, want %.6f", w.JitterEwmaMs, wantJitter)
	}
}

func TestResolverMetricsEpochRotation(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := base
	m := newResolverMetricsWithClock(func() time.Time { return clock })

	m.Observe(ExchangeOutcome{Duration: msToDuration(10)})

	if got := m.Snapshot().Samples; got != 1 {
		t.Fatalf("samples before rotation = %d, want 1", got)
	}

	// One epoch later the sample is still within the 5-minute window.
	clock = base.Add(metricsEpochDur + time.Second)
	if got := m.Snapshot().Samples; got != 1 {
		t.Fatalf("samples after one epoch = %d, want 1", got)
	}

	// Beyond the whole window the aged-out sample must have left the window
	// while the lifetime count survives.
	clock = base.Add(metricsWindow + time.Minute)
	if got := m.Snapshot().Samples; got != 0 {
		t.Fatalf("samples after window expiry = %d, want 0", got)
	}
	if m.TotalSamples() != 1 {
		t.Fatal("lifetime sample count must survive window rotation")
	}
	if _, ok := m.Percentile(0.99); ok {
		t.Fatal("quantile must be unavailable after window rotation")
	}

	// A fresh observation after a long idle gap starts from an empty window.
	m.Observe(ExchangeOutcome{Duration: msToDuration(20)})
	if got := m.Snapshot().Samples; got != 1 {
		t.Fatalf("samples after full window gap = %d, want 1", got)
	}
	if m.TotalSamples() != 2 {
		t.Fatalf("lifetime samples = %d, want 2", m.TotalSamples())
	}
}

func TestResolverMetricsBoundedMemory(t *testing.T) {
	m := NewResolverMetrics()
	for range 100000 {
		m.Observe(ExchangeOutcome{Duration: msToDuration(10)})
	}
	if len(m.epochs) != metricsEpochCount {
		t.Fatalf("epochs = %d, want %d", len(m.epochs), metricsEpochCount)
	}
	for i := range m.epochs {
		if got := len(m.epochs[i].latencyBuckets); got != len(latencyBucketEdges) {
			t.Fatalf("epoch %d buckets = %d, want %d", i, got, len(latencyBucketEdges))
		}
	}
}

func TestResolverMetricsWarm(t *testing.T) {
	m := NewResolverMetrics()
	if !m.Warm(5) {
		t.Fatal("fresh metrics must be warm")
	}
	for range 5 {
		m.Observe(ExchangeOutcome{Duration: msToDuration(10)})
	}
	if m.Warm(5) {
		t.Fatal("metrics with warmup samples must no longer be warm")
	}
	if !m.Warm(6) {
		t.Fatal("5 samples must still be warm against a threshold of 6")
	}
}

func TestResolverMetricsConcurrent(t *testing.T) {
	m := NewResolverMetrics()
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 1000 {
				m.Observe(ExchangeOutcome{Duration: msToDuration(15), ConnReused: true})
				_ = m.Snapshot()
				_, _ = m.Percentile(0.99)
			}
		}()
	}
	wg.Wait()
}
