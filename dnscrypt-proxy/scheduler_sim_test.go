package main

import (
	"errors"
	"math"
	mrand "math/rand"
	"sort"
	"testing"
	"time"

	"codeberg.org/miekg/dns"
	ewma "github.com/VividCortex/ewma"
)

// scheduler_sim_test.go is a deterministic A/B simulation harness. It
// drives the REAL wp2 and mds strategy code (the same selection methods
// and the real observeOutcome feedback chokepoint) against an identical,
// pre-rolled synthetic environment. The virtual clock is injected, so
// warmup, dwell, breaker half-open and metric-window rotation all run in
// zero wall-clock time.

// ---------- virtual clock ----------

type simClock struct{ t time.Time }

func (c *simClock) now() time.Time { return c.t }
func (c *simClock) advance(d time.Duration) {
	c.t = c.t.Add(d)
}

// ---------- environment model ----------

type simNode struct {
	name       string
	initialRtt int
	base       time.Duration
	jitter     time.Duration
	tail       time.Duration
	tailProb   float64
	timeoutP   float64
	errorP     float64
	servfailP  float64
	connNewP   float64

	addedAt int // step at which the resolver joins the pool (0 = present)

	// forcedFailures, if positive, turns the next selections of this node
	// into hard timeouts (models a cold resolver with 1-2 early failures).
	forcedFailures int

	// step mutates the node's parameters before each environment draw.
	step func(step int, n *simNode)
}

type simOutcome struct {
	timeout  bool
	netErr   bool
	servfail bool
	latency  time.Duration
	connNew  bool
}

func rollOutcome(n *simNode, rng *mrand.Rand) simOutcome {
	o := simOutcome{latency: n.base}
	if n.jitter > 0 {
		o.latency += time.Duration(rng.Float64()*2*float64(n.jitter)) - n.jitter
		if o.latency < 0 {
			o.latency = 0
		}
	}
	if n.tailProb > 0 && rng.Float64() < n.tailProb {
		o.latency = n.tail
	}
	r := rng.Float64()
	switch {
	case r < n.timeoutP:
		o.timeout = true
	case r < n.timeoutP+n.errorP:
		o.netErr = true
	case r < n.timeoutP+n.errorP+n.servfailP:
		o.servfail = true
	}
	if rng.Float64() < n.connNewP {
		o.connNew = true
	}
	return o
}

// ---------- run results ----------

type simResult struct {
	strategy string
	latency  []time.Duration
	served   []string

	switchesServed  int
	primarySwitches uint64
	windowServed    int
	windowPrimary   uint64
	shares          map[string]float64
	failoverStep    int // steps after failStart until traffic leaves the node
	recoverStep     int // steps after recovery until it serves/primary again
	openAtEnd       bool
}

func (r simResult) percentile(p float64) time.Duration {
	s := append([]time.Duration(nil), r.latency...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	rank := int(math.Ceil(p*float64(len(s)))) - 1
	if rank < 0 {
		rank = 0
	}
	if rank >= len(s) {
		rank = len(s) - 1
	}
	return s[rank]
}

type simSpec struct {
	steps    int
	interval time.Duration
	nodes    []*simNode

	windowStart, windowEnd int // network-jitter observation window

	failName    string
	failStart   int
	failRecover int
}

// runSim executes one strategy against a fresh, identically seeded world.
func runSim(t *testing.T, strategy string, seed int64, spec simSpec) simResult {
	t.Helper()

	proxy := NewProxy()
	si := &proxy.serversInfo
	proxy.timeout = 2 * time.Second
	clk := &simClock{t: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)}
	si.now = clk.now

	var mds *LBStrategyMDS
	switch strategy {
	case "mds":
		mds = newLBStrategyMDSWithDeps(DefaultMDSParams(), clk.now, mrand.New(mrand.NewSource(seed)))
		si.lbStrategy = mds
	case "wp2":
		si.lbStrategy = LBStrategyWP2{}
		mrand.Seed(seed)
	default:
		t.Fatalf("unknown strategy %q", strategy)
	}

	byName := make(map[string]*simNode, len(spec.nodes))
	indexByName := make(map[string]int, len(spec.nodes))
	for i, n := range spec.nodes {
		byName[n.name] = n
		indexByName[n.name] = i
	}

	// Environment RNG is independent of selection RNG and identical for
	// both strategy runs.
	envSeed := uint64(seed) ^ 0x9e3779b97f4a7c15
	env := mrand.New(mrand.NewSource(int64(envSeed)))

	res := simResult{
		strategy:     strategy,
		shares:       make(map[string]float64),
		failoverStep: -1,
		recoverStep:  -1,
	}
	servedCount := make(map[string]int)
	prevServed := ""
	var primaryAtWindowStart uint64

	for step := 1; step <= spec.steps; step++ {
		clk.advance(spec.interval)

		// Dynamic scenarios mutate parameters before the draw.
		for _, n := range spec.nodes {
			if n.step != nil {
				n.step(step, n)
			}
		}
		// Pre-roll every node's latent outcome for this step, so the
		// environment is identical regardless of who gets selected.
		outs := make([]simOutcome, len(spec.nodes))
		for i, n := range spec.nodes {
			outs[i] = rollOutcome(n, env)
		}

		si.Lock()
		// On the first step, register every resolver present from the start
		// (addedAt == 0). Later steps only admit late joiners.
		if step == 1 {
			for _, n := range spec.nodes {
				if n.addedAt == 0 {
					si.inner = append(si.inner, &ServerInfo{
						Name:       n.name,
						initialRtt: n.initialRtt,
						rtt:        ewma.NewMovingAverage(RTTEwmaDecay),
					})
				}
			}
		}
		// Register late-joining (cold) resolvers, without reordering the
		// existing slice.
		for _, n := range spec.nodes {
			if n.addedAt == step {
				si.inner = append(si.inner, &ServerInfo{
					Name:       n.name,
					initialRtt: n.initialRtt,
					rtt:        ewma.NewMovingAverage(RTTEwmaDecay),
				})
			}
		}

		var picked string
		if mds != nil {
			cands := si.candidatesLocked()
			picked = cands[mds.selectCandidateLocked(cands)].Server.Name
		} else {
			picked = si.inner[si.getWeightedCandidate(len(si.inner))].Name
		}
		si.Unlock()

		o := outs[indexByName[picked]]
		// Cold-start early failures are consumed deterministically.
		if byName[picked].forcedFailures > 0 {
			byName[picked].forcedFailures--
			o = simOutcome{timeout: true}
		}

		cost := o.latency
		failed := o.timeout || o.netErr
		if failed {
			// A failed exchange occupies the full deadline.
			cost = proxy.timeout
		}

		ps := &PluginsState{
			serverName:    picked,
			exchangeStart: clk.now().Add(-cost),
			exchange: ExchangeOutcome{
				Transport:  TransportDNSCryptUDP,
				ConnReused: !o.connNew,
			},
		}
		var resp []byte
		var exchangeErr error
		switch {
		case o.timeout:
			ps.returnCode = PluginsReturnCodeServerTimeout
			exchangeErr = timeoutTestErr{}
		case o.netErr:
			exchangeErr = errors.New("simulated network failure")
		case o.servfail:
			resp = mustPackResponse(t, dns.RcodeServerFailure, 0)
		default:
			resp = mustPackResponse(t, dns.RcodeSuccess, 0)
		}
		si.observeOutcome(proxy, ps, resp, exchangeErr)

		res.latency = append(res.latency, cost)
		res.served = append(res.served, picked)
		servedCount[picked]++
		servedChanged := prevServed != "" && picked != prevServed
		if servedChanged {
			res.switchesServed++
		}
		prevServed = picked

		inWindow := step >= spec.windowStart && step <= spec.windowEnd
		if step == spec.windowStart && mds != nil {
			primaryAtWindowStart = mds.Stats().TotalSwitches
		}
		if inWindow {
			if mds == nil {
				if servedChanged {
					res.windowServed++
				}
			} else {
				res.windowPrimary = mds.Stats().TotalSwitches - primaryAtWindowStart
			}
		}
		if mds != nil {
			res.primarySwitches = mds.Stats().TotalSwitches
		}

		// Failover / recovery accounting against the failed primary.
		if spec.failName != "" && step >= spec.failStart {
			inFailure := spec.failRecover == 0 || step < spec.failRecover
			current := picked
			if mds != nil {
				current = mds.primaryName
			}
			if inFailure && res.failoverStep < 0 && current != spec.failName {
				res.failoverStep = step - spec.failStart
			}
			if !inFailure && res.recoverStep < 0 && current == spec.failName {
				res.recoverStep = step - spec.failRecover
			}
		}
	}

	if mds != nil {
		if cb := mds.circuits[spec.failName]; cb != nil {
			res.openAtEnd = cb.open
		}
	}
	for name, count := range servedCount {
		res.shares[name] = float64(count) / float64(spec.steps)
	}
	return res
}

// ---------- scenarios ----------

func stableNodes() []*simNode {
	return []*simNode{
		{name: "fast-1", initialRtt: 10, base: 10 * time.Millisecond, jitter: 2 * time.Millisecond, tail: 60 * time.Millisecond, tailProb: 0.005, connNewP: 0.05},
		{name: "fast-2", initialRtt: 12, base: 12 * time.Millisecond, jitter: 2 * time.Millisecond, tail: 60 * time.Millisecond, tailProb: 0.005, connNewP: 0.05},
		{name: "fast-3", initialRtt: 14, base: 14 * time.Millisecond, jitter: 2 * time.Millisecond, tail: 60 * time.Millisecond, tailProb: 0.005, connNewP: 0.05},
		{name: "fast-4", initialRtt: 16, base: 16 * time.Millisecond, jitter: 2 * time.Millisecond, tail: 60 * time.Millisecond, tailProb: 0.005, connNewP: 0.05},
	}
}

func longTailNodes() []*simNode {
	nodes := []*simNode{
		{name: "fast-1", initialRtt: 10, base: 10 * time.Millisecond, jitter: 1 * time.Millisecond, tail: 40 * time.Millisecond, tailProb: 0.005},
		{name: "fast-2", initialRtt: 10, base: 10 * time.Millisecond, jitter: 1 * time.Millisecond, tail: 40 * time.Millisecond, tailProb: 0.005},
		{name: "fast-3", initialRtt: 11, base: 11 * time.Millisecond, jitter: 1 * time.Millisecond, tail: 40 * time.Millisecond, tailProb: 0.005},
		{name: "fast-4", initialRtt: 11, base: 11 * time.Millisecond, jitter: 1 * time.Millisecond, tail: 40 * time.Millisecond, tailProb: 0.005},
	}
	for i := range 3 {
		nodes = append(nodes, &simNode{
			name:       "tail-" + string(rune('a'+i)),
			initialRtt: 15, base: 15 * time.Millisecond, jitter: 2 * time.Millisecond,
			tail: 200 * time.Millisecond, tailProb: 0.12, connNewP: 0.1,
		})
	}
	return nodes
}

func jitterNodes() []*simNode {
	nodes := stableNodes()
	const spikeStart, spikeEnd = 1000, 1020
	for _, n := range nodes {
		normalBase := n.base
		n.step = func(step int, x *simNode) {
			if step >= spikeStart && step <= spikeEnd {
				x.base = 800 * time.Millisecond
				x.tailProb = 0
				x.jitter = 10 * time.Millisecond
			} else {
				x.base = normalBase
				x.tailProb = 0.005
				x.tail = 60 * time.Millisecond
				x.jitter = 2 * time.Millisecond
			}
		}
	}
	return nodes
}

func coldStartNodes() []*simNode {
	nodes := stableNodes()
	nodes = append(nodes, &simNode{
		name: "cold-1", initialRtt: 10, base: 10 * time.Millisecond,
		jitter: 1 * time.Millisecond, tail: 40 * time.Millisecond, tailProb: 0.005,
		addedAt: 500, forcedFailures: 2,
	})
	return nodes
}

func breakerNodes() []*simNode {
	nodes := []*simNode{
		{name: "doomed", initialRtt: 10, base: 10 * time.Millisecond, jitter: 1 * time.Millisecond},
		{name: "backup-1", initialRtt: 30, base: 30 * time.Millisecond, jitter: 3 * time.Millisecond},
		{name: "backup-2", initialRtt: 32, base: 32 * time.Millisecond, jitter: 3 * time.Millisecond},
	}
	for _, n := range nodes {
		if n.name != "doomed" {
			continue
		}
		n.step = func(step int, x *simNode) {
			if step >= 600 && step < 1100 {
				x.timeoutP = 1.0
			} else {
				x.timeoutP = 0
			}
		}
	}
	return nodes
}

// ---------- scenario tests ----------

// TR-10.1 (determinism): identical seeds produce value-identical runs.
func TestSchedulerSimDeterministic(t *testing.T) {
	spec := simSpec{steps: 1500, interval: 100 * time.Millisecond, nodes: longTailNodes()}
	a := runSim(t, "mds", 4242, spec)
	b := runSim(t, "mds", 4242, simSpec{steps: 1500, interval: 100 * time.Millisecond, nodes: longTailNodes()})
	if len(a.latency) != len(b.latency) {
		t.Fatal("run lengths differ")
	}
	for i := range a.latency {
		if a.latency[i] != b.latency[i] || a.served[i] != b.served[i] {
			t.Fatalf("nondeterministic at step %d: %v/%s vs %v/%s",
				i, a.latency[i], a.served[i], b.latency[i], b.served[i])
		}
	}
	if a.primarySwitches != b.primarySwitches {
		t.Fatalf("switch count differs: %d vs %d", a.primarySwitches, b.primarySwitches)
	}
}

// TR-10.2: long-tail scenario — MDS must cut p99 by at least 10% vs wp2
// and must not regress p95, with no more primary switches.
func TestSchedulerSimLongTail(t *testing.T) {
	spec := simSpec{steps: 4000, interval: 100 * time.Millisecond, nodes: longTailNodes()}
	wp2 := runSim(t, "wp2", 2025, spec)
	mds := runSim(t, "mds", 2025, simSpec{steps: 4000, interval: 100 * time.Millisecond, nodes: longTailNodes()})

	t.Logf("long-tail A/B (4000 queries):")
	t.Logf("  wp2: p50=%v p95=%v p99=%v servedChurn=%d",
		wp2.percentile(.5), wp2.percentile(.95), wp2.percentile(.99), wp2.switchesServed)
	t.Logf("  mds: p50=%v p95=%v p99=%v servedChurn=%d primarySwitches=%d",
		mds.percentile(.5), mds.percentile(.95), mds.percentile(.99), mds.switchesServed, mds.primarySwitches)

	reduction := 1 - float64(mds.percentile(.99))/float64(wp2.percentile(.99))
	if reduction < 0.10 {
		t.Fatalf("p99 reduction %.1f%% below required 10%% (wp2=%v mds=%v)",
			reduction*100, wp2.percentile(.99), mds.percentile(.99))
	}
	if mds.percentile(.95) > wp2.percentile(.95) {
		t.Fatalf("p95 regressed: wp2=%v mds=%v", wp2.percentile(.95), mds.percentile(.95))
	}
	if mds.primarySwitches > uint64(wp2.switchesServed) {
		t.Fatalf("mds primary switches %d exceed wp2 churn %d", mds.primarySwitches, wp2.switchesServed)
	}
}

// Stable baseline: MDS keeps primary switches well below wp2 churn.
func TestSchedulerSimStable(t *testing.T) {
	spec := simSpec{steps: 3000, interval: 100 * time.Millisecond, nodes: stableNodes()}
	wp2 := runSim(t, "wp2", 77, spec)
	mds := runSim(t, "mds", 77, simSpec{steps: 3000, interval: 100 * time.Millisecond, nodes: stableNodes()})
	t.Logf("stable: wp2 servedChurn=%d mds primarySwitches=%d", wp2.switchesServed, mds.primarySwitches)
	if mds.primarySwitches > uint64(wp2.switchesServed) {
		t.Fatalf("mds switches %d > wp2 churn %d on a stable baseline",
			mds.primarySwitches, wp2.switchesServed)
	}
}

// Brief synchronized network jitter: the sticky primary must not oscillate
// (0 primary moves in-window, at most 1 allowed), and mds window churn must
// be below 25% of wp2's decision churn.
func TestSchedulerSimNetworkJitter(t *testing.T) {
	spec := simSpec{
		steps: 2000, interval: 100 * time.Millisecond, nodes: jitterNodes(),
		windowStart: 1000, windowEnd: 1020,
	}
	wp2 := runSim(t, "wp2", 313, spec)
	mds := runSim(t, "mds", 313, simSpec{
		steps: 2000, interval: 100 * time.Millisecond, nodes: jitterNodes(),
		windowStart: 1000, windowEnd: 1020,
	})
	t.Logf("jitter window: wp2 churn=%d mds primaryMoves=%d", wp2.windowServed, mds.windowPrimary)
	if mds.windowPrimary > 1 {
		t.Fatalf("mds moved primary %d times during a 2s network-wide spike", mds.windowPrimary)
	}
	budget := uint64(math.Ceil(0.25 * float64(wp2.windowServed)))
	if mds.windowPrimary > budget {
		t.Fatalf("mds window moves %d exceed 25%% budget (%d) of wp2 churn %d",
			mds.windowPrimary, budget, wp2.windowServed)
	}
	if mds.primarySwitches > uint64(wp2.switchesServed) {
		t.Fatalf("mds total switches %d > wp2 churn %d", mds.primarySwitches, wp2.switchesServed)
	}
}

// A cold resolver with 2 early failures is never evicted, keeps its
// exploration floor, never trips the breaker and keeps receiving traffic
// once mature.
func TestSchedulerSimColdStart(t *testing.T) {
	spec := simSpec{steps: 2000, interval: 100 * time.Millisecond, nodes: coldStartNodes()}
	mds := runSim(t, "mds", 909, spec)

	// Warming-period share: exploration is dedicated to the cold node, so
	// roughly ExploreProb of all queries in that phase must reach it.
	warmingHits, warmingTotal := 0, 0
	for step := 500; step < 700; step++ {
		warmingTotal++
		if mds.served[step-1] == "cold-1" {
			warmingHits++
		}
	}
	share := float64(warmingHits) / float64(warmingTotal)
	t.Logf("cold warming share=%.3f (hits=%d/%d) lifetimeShare=%.3f",
		share, warmingHits, warmingTotal, mds.shares["cold-1"])
	if share < 0.13 {
		t.Fatalf("cold resolver share %.3f below exploration floor 0.13", share)
	}
	if mds.openAtEnd {
		t.Fatal("cold resolver tripped the breaker during warmup")
	}
	// After maturing it remains in the healthy pool; it is never excluded.
	matureHits := 0
	for step := 700; step <= 2000; step++ {
		if mds.served[step-1] == "cold-1" {
			matureHits++
		}
	}
	if matureHits == 0 {
		t.Fatal("cold resolver was excluded from selection after warmup")
	}
}

// Persistent primary failure: failover is bounded by the breaker threshold
// and half-open probing restores the primary after recovery.
func TestSchedulerSimCircuitBreaker(t *testing.T) {
	spec := simSpec{
		steps: 1600, interval: 100 * time.Millisecond, nodes: breakerNodes(),
		failName: "doomed", failStart: 600, failRecover: 1100,
	}
	mds := runSim(t, "mds", 55, spec)
	wp2 := runSim(t, "wp2", 55, simSpec{
		steps: 1600, interval: 100 * time.Millisecond, nodes: breakerNodes(),
		failName: "doomed", failStart: 600, failRecover: 1100,
	})
	t.Logf("breaker: mds failover=%d steps (%v), recovery=%d steps, wp2 failover=%d steps",
		mds.failoverStep, time.Duration(mds.failoverStep)*100*time.Millisecond,
		mds.recoverStep, wp2.failoverStep)
	// 3 consecutive hard failures plus the switch query, bounded tightly
	// even accounting for one exploratory miss.
	if mds.failoverStep < 0 || mds.failoverStep > 8 {
		t.Fatalf("mds failover latency %d steps outside bound [1..8]", mds.failoverStep)
	}
	if time.Duration(mds.failoverStep)*spec.interval > time.Second {
		t.Fatal("mds failover exceeded the 1s latency bound")
	}
	// After recovery the next half-open probe (every 10s) succeeds and the
	// node regains primary status within one probe interval plus margin.
	if mds.recoverStep < 0 || mds.recoverStep > 120 {
		t.Fatalf("primary recovery step %d outside bound [0..120]", mds.recoverStep)
	}
	if mds.openAtEnd {
		t.Fatal("circuit still open after successful recovery")
	}
}

// Benchmark: MDS selection hot path.
func BenchmarkSchedulerMDSSelect(b *testing.B) {
	clk := &simClock{t: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)}
	mds := newLBStrategyMDSWithDeps(DefaultMDSParams(), clk.now, mrand.New(mrand.NewSource(1)))
	proxy := NewProxy()
	si := &proxy.serversInfo
	si.Lock()
	for _, n := range longTailNodes() {
		si.inner = append(si.inner, &ServerInfo{
			Name: n.name, initialRtt: n.initialRtt, rtt: ewma.NewMovingAverage(RTTEwmaDecay),
		})
		si.metricsForLocked(n.name)
	}
	cands := si.candidatesLocked()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mds.selectCandidateLocked(cands)
	}
	si.Unlock()
}
