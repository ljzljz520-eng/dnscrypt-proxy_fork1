package main

import (
	"math/rand"
	"testing"
	"time"

	stamps "github.com/jedisct1/go-dnsstamps"
)

type fakeClock struct{ t time.Time }

func (f *fakeClock) now() time.Time { return f.t }
func (f *fakeClock) advance(d time.Duration) {
	f.t = f.t.Add(d)
}

type mdsHarness struct {
	clk   *fakeClock
	mds   *LBStrategyMDS
	cands []ResolverCandidate
}

func newMDSHarness(seed int64) *mdsHarness {
	clk := &fakeClock{t: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)}
	params := DefaultMDSParams()
	mds := newLBStrategyMDSWithDeps(params, clk.now, rand.New(rand.NewSource(seed)))
	return &mdsHarness{clk: clk, mds: mds}
}

func (h *mdsHarness) add(name string, initialRtt int, props stamps.ServerInformalProperties) *ResolverMetrics {
	metrics := newResolverMetricsWithClock(h.clk.now)
	server := &ServerInfo{Name: name, initialRtt: initialRtt, Props: props}
	h.cands = append(h.cands, ResolverCandidate{Server: server, Metrics: metrics})
	return metrics
}

func feedOutcomes(m *ResolverMetrics, count int, duration time.Duration, mutate func(*ExchangeOutcome)) {
	for range count {
		o := ExchangeOutcome{Duration: duration, ConnReused: true}
		if mutate != nil {
			mutate(&o)
		}
		m.Observe(o)
	}
}

func findScored(scored []mdsScored, name string) mdsScored {
	for _, s := range scored {
		if s.name == name {
			return s
		}
	}
	panic("scored candidate not found: " + name)
}

// TR-5.1: warming resolvers are never penalized or excluded and receive
// the exploration floor; penalties appear only after warmup completes.
func TestMDSWarmupProtection(t *testing.T) {
	h := newMDSHarness(42)
	ma := h.add("mature-a", 10, 0)
	mb := h.add("mature-b", 12, 0)
	warm := h.add("warm-w", 15, 0)
	feedOutcomes(ma, 25, 10*time.Millisecond, nil)
	feedOutcomes(mb, 25, 12*time.Millisecond, nil)
	// 19 samples (< warmup 20), including 2 early timeouts.
	feedOutcomes(warm, 17, 15*time.Millisecond, nil)
	feedOutcomes(warm, 2, 5*time.Second, func(o *ExchangeOutcome) { o.Timeout = true })

	scored := h.mds.scoreAll(h.cands)
	ws := findScored(scored, "warm-w")
	if !ws.warming {
		t.Fatal("warm-w should still be warming below the sample threshold")
	}
	if ws.penalty != 0 {
		t.Fatalf("warming resolver carries penalties: %f", ws.penalty)
	}
	if ws.tailMs != 15 {
		t.Fatalf("warming tail should use initialRtt prior, got %f", ws.tailMs)
	}

	// Settle a primary, then run many selections.
	for range 10 {
		h.mds.selectCandidateLocked(h.cands)
	}
	if h.mds.primaryName != "mature-a" {
		t.Fatalf("unexpected primary: %s", h.mds.primaryName)
	}

	const runs = 8000
	counts := map[string]int{}
	for range runs {
		idx := h.mds.selectCandidateLocked(h.cands)
		if idx < 0 || idx >= len(h.cands) {
			t.Fatalf("selection left candidate set: %d", idx)
		}
		counts[h.cands[idx].Server.Name]++
	}
	if counts["warm-w"] == 0 {
		t.Fatalf("warming resolver was excluded from the candidate set: %v", counts)
	}
	// While warming candidates exist, exploration is dedicated to them,
	// so their share is the full exploration probability.
	share := float64(counts["warm-w"]) / runs
	if share < 0.13 {
		t.Fatalf("warming share %.3f below exploration floor ~%.2f", share, h.mds.params.ExploreProb)
	}

	// Mature the resolver: 2 more samples (21 total). Timeout penalties
	// must now be present.
	feedOutcomes(warm, 2, 15*time.Millisecond, nil)
	scored = h.mds.scoreAll(h.cands)
	ws = findScored(scored, "warm-w")
	if ws.warming {
		t.Fatal("warm-w should be mature after reaching the threshold")
	}
	if ws.penalty <= 0 {
		t.Fatal("expected quality penalties after warmup completed")
	}
}

// TR-5.2: a short synchronized degradation does not cause switching
// inside the dwell window (and not even once the margin stays uncrossed).
func TestMDSAntiOscillation(t *testing.T) {
	h := newMDSHarness(7)
	ma := h.add("a", 10, 0)
	mb := h.add("b", 20, 0)
	mc := h.add("c", 30, 0)
	feedOutcomes(ma, 30, 10*time.Millisecond, nil)
	feedOutcomes(mb, 30, 20*time.Millisecond, nil)
	feedOutcomes(mc, 30, 30*time.Millisecond, nil)

	for range 5 {
		h.mds.selectCandidateLocked(h.cands)
	}
	if h.mds.primaryName != "a" {
		t.Fatalf("expected a as primary, got %s", h.mds.primaryName)
	}
	switchesBefore := h.mds.totalSwitches

	// Brief network-wide jitter: one slow but successful sample each.
	feedOutcomes(ma, 1, 800*time.Millisecond, nil)
	feedOutcomes(mb, 1, 800*time.Millisecond, nil)
	feedOutcomes(mc, 1, 800*time.Millisecond, nil)

	h.clk.advance(2 * time.Second) // inside the 5s dwell window
	for range 100 {
		_ = h.mds.selectCandidateLocked(h.cands)
		// Exploratory single queries may go elsewhere, but the sticky
		// primary must not move.
		if h.mds.primaryName != "a" {
			t.Fatalf("primary flapped during jitter+dwell: %s", h.mds.primaryName)
		}
	}
	if h.mds.totalSwitches != switchesBefore {
		t.Fatalf("switches during jitter window: got %d, want 0", h.mds.totalSwitches-switchesBefore)
	}

	// Past dwell: margin is still uncrossed because everyone degraded
	// together, so the primary must remain.
	h.clk.advance(4 * time.Second)
	for range 100 {
		_ = h.mds.selectCandidateLocked(h.cands)
		if h.mds.primaryName != "a" {
			t.Fatalf("primary changed without a margin: %s", h.mds.primaryName)
		}
	}
	if h.mds.totalSwitches != switchesBefore {
		t.Fatalf("switches after recovery: got %d, want %d", h.mds.totalSwitches, switchesBefore)
	}
}

// TR-5.3: consecutive hard failures trip the breaker for an immediate
// switch that bypasses dwell/margin; a successful half-open probe lets the
// resolver become primary again.
func TestMDSCircuitBreakerAndRecovery(t *testing.T) {
	h := newMDSHarness(99)
	ma := h.add("a", 10, 0)
	mb := h.add("b", 20, 0)
	feedOutcomes(ma, 30, 10*time.Millisecond, nil)
	feedOutcomes(mb, 30, 20*time.Millisecond, nil)

	for range 10 {
		idx := h.mds.selectCandidateLocked(h.cands)
		if h.mds.primaryName == "a" {
			break
		}
		_ = idx
	}
	if h.mds.primaryName != "a" {
		t.Fatalf("expected a as primary, got %s", h.mds.primaryName)
	}

	// Two failures: breaker not yet open, primary stays sticky.
	for range 2 {
		h.mds.observeFeedbackLocked("a", ExchangeOutcome{Timeout: true}, 31)
	}
	for range 20 {
		_ = h.mds.selectCandidateLocked(h.cands)
		if h.mds.primaryName != "a" {
			t.Fatal("primary switched before the breaker threshold")
		}
	}

	// Third consecutive hard failure opens the circuit (mature resolver).
	h.mds.observeFeedbackLocked("a", ExchangeOutcome{Error: true}, 32)
	if !h.mds.circuits["a"].open {
		t.Fatal("expected circuit open after threshold failures")
	}
	// The breaker bypass is exempt from dwell/margin; an exploratory draw
	// may land first, so advance until the forced switch happens.
	for range 10 {
		idx := h.mds.selectCandidateLocked(h.cands)
		if h.mds.primaryName == "b" {
			if h.cands[idx].Server.Name != "b" {
				t.Fatalf("forced switch picked %s", h.cands[idx].Server.Name)
			}
			break
		}
	}
	if h.mds.primaryName != "b" {
		t.Fatalf("breaker did not force an immediate switch: primary=%s", h.mds.primaryName)
	}

	// Before half-open interval, a stays excluded even via exploration.
	h.clk.advance(5 * time.Second)
	for range 50 {
		idx := h.mds.selectCandidateLocked(h.cands)
		if h.cands[idx].Server.Name == "a" {
			t.Fatal("open resolver selected before half-open interval")
		}
	}

	// Half-open probe is served by a.
	h.clk.advance(5 * time.Second)
	idx := h.mds.selectCandidateLocked(h.cands)
	if h.cands[idx].Server.Name != "a" || !h.mds.circuits["a"].probing {
		t.Fatalf("expected half-open probe on a, got %s", h.cands[idx].Server.Name)
	}

	// Probe succeeds: circuit closes; a is clearly better and dwell has
	// elapsed, so it regains primary status.
	feedOutcomes(ma, 1, 10*time.Millisecond, nil)
	h.mds.observeFeedbackLocked("a", ExchangeOutcome{Duration: 10 * time.Millisecond}, 33)
	if h.mds.circuits["a"].open {
		t.Fatal("circuit did not close after a successful probe")
	}
	for range 20 {
		_ = h.mds.selectCandidateLocked(h.cands)
		if h.mds.primaryName == "a" {
			break
		}
	}
	if h.mds.primaryName != "a" {
		t.Fatalf("recovered resolver did not regain primary status: %s", h.mds.primaryName)
	}
}

// TR-5.3b: a successful half-open probe starts a recovery probation. The
// resolver is re-judged under warmup semantics, so outage-contaminated
// window data (2s timeout latencies and timeout counters) cannot delay
// the reintegration of a genuinely recovered resolver.
func TestMDSRecoveryProbation(t *testing.T) {
	h := newMDSHarness(99)
	ma := h.add("a", 10, 0)
	mb := h.add("b", 30, 0)
	feedOutcomes(ma, 30, 10*time.Millisecond, nil)
	feedOutcomes(mb, 30, 30*time.Millisecond, nil)

	for range 10 {
		if h.mds.selectCandidateLocked(h.cands); h.mds.primaryName == "a" {
			break
		}
	}
	if h.mds.primaryName != "a" {
		t.Fatalf("expected a as primary, got %s", h.mds.primaryName)
	}

	// Three real hard failures: they reach the metrics store (as in
	// production) AND the breaker feedback, so the latency window is
	// contaminated by 2s deadline samples.
	for i := range 3 {
		feedOutcomes(ma, 1, 2*time.Second, func(o *ExchangeOutcome) { o.Timeout = true })
		h.mds.observeFeedbackLocked("a", ExchangeOutcome{Timeout: true, Duration: 2 * time.Second}, uint64(31+i))
	}
	cb := h.mds.circuits["a"]
	if !cb.open {
		t.Fatal("expected circuit open after 3 hard failures")
	}

	for range 10 {
		_ = h.mds.selectCandidateLocked(h.cands)
		if h.mds.primaryName == "b" {
			break
		}
	}
	if h.mds.primaryName != "b" {
		t.Fatalf("expected forced switch to b, got %s", h.mds.primaryName)
	}

	// Half-open probe is due after the interval.
	h.clk.advance(10 * time.Second)
	idx := h.mds.selectCandidateLocked(h.cands)
	if h.cands[idx].Server.Name != "a" || !cb.probing {
		t.Fatalf("expected half-open probe on a, picked %s", h.cands[idx].Server.Name)
	}

	// Probe succeeds at 10ms.
	feedOutcomes(ma, 1, 10*time.Millisecond, nil)
	h.mds.observeFeedbackLocked("a", ExchangeOutcome{Duration: 10 * time.Millisecond, ConnReused: true}, 34)
	if cb.open {
		t.Fatal("circuit did not close after the successful probe")
	}
	if cb.recoverUntilTotal <= 34 {
		t.Fatalf("recovery probation not started: recoverUntilTotal=%d", cb.recoverUntilTotal)
	}

	// Despite the contaminated window (3 deadline samples out of 34), a is
	// scored under warmup semantics and regains primary immediately.
	sa := findScored(h.mds.scoreAll(h.cands), "a")
	if !sa.warming {
		t.Fatal("recovered resolver should be warming during probation")
	}
	if sa.penalty != 0 || sa.tailMs != 10 {
		t.Fatalf("probation should neutralize penalties/prior: penalty=%f tail=%f", sa.penalty, sa.tailMs)
	}
	for range 20 {
		_ = h.mds.selectCandidateLocked(h.cands)
		if h.mds.primaryName == "a" {
			break
		}
	}
	if h.mds.primaryName != "a" {
		t.Fatalf("recovered resolver did not regain primary: %s", h.mds.primaryName)
	}

	// Hard failures inside probation cannot re-trip the breaker; once the
	// probation sample quota is exhausted, normal protection returns.
	feedOutcomes(ma, 18, 10*time.Millisecond, nil)
	for i := uint64(35); i < 53; i++ {
		h.mds.observeFeedbackLocked("a", ExchangeOutcome{Duration: 10 * time.Millisecond, ConnReused: true}, i)
	}
	h.mds.observeFeedbackLocked("a", ExchangeOutcome{Timeout: true, Duration: 2 * time.Second}, 53)
	if cb.open {
		t.Fatal("a single failure at the probation boundary must not open the circuit")
	}
	// Probation has ended: two more consecutive hard failures trip it.
	h.mds.observeFeedbackLocked("a", ExchangeOutcome{Timeout: true, Duration: 2 * time.Second}, 54)
	h.mds.observeFeedbackLocked("a", ExchangeOutcome{Timeout: true, Duration: 2 * time.Second}, 55)
	if !cb.open {
		t.Fatal("breaker should re-arm after the probation ends")
	}
}

// TR-5.4: stamp properties and ECS behavior never remove a candidate; they
// only flip ordering inside the soft-preference tie band.
func TestMDSSoftPreferences(t *testing.T) {
	h := newMDSHarness(123)

	build := func(xLatency time.Duration, xProps stamps.ServerInformalProperties) []ResolverCandidate {
		h2 := newMDSHarness(123)
		mx := h2.add("x", 100, xProps)
		my := h2.add("y", 100, 0)
		mr := h2.add("ref", 1000, 0)
		feedOutcomes(mx, 30, xLatency, nil)
		feedOutcomes(my, 30, 100*time.Millisecond, nil)
		feedOutcomes(mr, 30, 1000*time.Millisecond, nil)
		return h2.cands
	}

	// Identical scores: properties decide the tie.
	cands := build(100*time.Millisecond, stamps.ServerInformalPropertyNoFilter|stamps.ServerInformalPropertyDNSSEC)
	scored := h.mds.scoreAll(cands)
	sx, sy := findScored(scored, "x"), findScored(scored, "y")
	if sx.base != sy.base || sx.soft <= sy.soft {
		t.Fatalf("expected equal bases with x preferred on soft: base %f/%f soft %f/%f",
			sx.base, sy.base, sx.soft, sy.soft)
	}
	if !h.mds.better(sx, sy) {
		t.Fatal("soft preference did not decide an exact tie")
	}

	// Slightly worse base, still inside the tie band: soft keeps x ahead.
	cands = build(120*time.Millisecond, stamps.ServerInformalPropertyNoFilter|stamps.ServerInformalPropertyDNSSEC)
	scored = h.mds.scoreAll(cands)
	sx, sy = findScored(scored, "x"), findScored(scored, "y")
	gap := sy.base - sx.base
	if gap <= 0 || gap > h.mds.params.TieBand {
		t.Fatalf("test setup: gap %f not within tie band %f", gap, h.mds.params.TieBand)
	}
	if !h.mds.better(sx, sy) {
		t.Fatal("soft preference failed to flip an in-band ordering")
	}

	// Clearly worse base beyond the band: properties cannot save x.
	cands = build(250*time.Millisecond, stamps.ServerInformalPropertyNoFilter|stamps.ServerInformalPropertyDNSSEC)
	scored = h.mds.scoreAll(cands)
	sx, sy = findScored(scored, "x"), findScored(scored, "y")
	gap = sy.base - sx.base
	if gap <= h.mds.params.TieBand {
		t.Fatalf("test setup: gap %f should exceed tie band %f", gap, h.mds.params.TieBand)
	}
	if h.mds.better(sx, sy) {
		t.Fatal("soft preference overrode a significant score gap")
	}

	// Candidate-set invariance: all three resolvers remain selectable
	// regardless of property combinations.
	h3 := newMDSHarness(321)
	m1 := h3.add("p1", 50, stamps.ServerInformalPropertyDNSSEC)
	m2 := h3.add("p2", 60, stamps.ServerInformalPropertyNoFilter)
	m3 := h3.add("p3", 70, stamps.ServerInformalPropertyDNSSEC|stamps.ServerInformalPropertyNoLog|stamps.ServerInformalPropertyNoFilter)
	feedOutcomes(m1, 30, 50*time.Millisecond, nil)
	feedOutcomes(m2, 30, 60*time.Millisecond, nil)
	feedOutcomes(m3, 30, 70*time.Millisecond, func(o *ExchangeOutcome) { o.ECSReturned = true })
	seen := map[string]bool{}
	for range 3000 {
		idx := h3.mds.selectCandidateLocked(h3.cands)
		seen[h3.cands[idx].Server.Name] = true
	}
	if len(seen) != 3 {
		t.Fatalf("some candidates were excluded by soft preferences: %v", seen)
	}
}
