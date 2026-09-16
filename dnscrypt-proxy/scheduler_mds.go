package main

import (
	"math"
	"math/rand"
	"time"
)

// MDSParams holds every tunable of the multi-dimensional scheduler. The
// zero value is not valid; use DefaultMDSParams.
type MDSParams struct {
	// WarmupSamples is the lifetime sample count below which a resolver is
	// treated as "warming": quality penalties are neutral, the circuit
	// breaker is disabled and the initial RTT probe is used as prior.
	WarmupSamples uint64
	// PenaltyMinSamples is the minimum number of in-window samples a
	// resolver must have before ratio-based quality penalties apply.
	PenaltyMinSamples uint64

	// SwitchMargin is the minimum score improvement (on the 0..1-ish
	// score scale) a challenger must demonstrate over the sticky primary
	// before a switch is allowed.
	SwitchMargin float64
	// TieBand is the gap below which two scores are considered tied and
	// soft preferences may decide the order.
	TieBand float64
	// Dwell is the minimum time a primary is kept before a non-breaker
	// switch may happen.
	Dwell time.Duration
	// ExploreProb is the probability of routing a query to an exploratory
	// candidate (a warming resolver first, then any healthy non-primary).
	ExploreProb float64

	// BreakerThreshold is the number of consecutive hard failures
	// (timeouts / network errors) that open the circuit on a mature
	// resolver.
	BreakerThreshold uint32
	// HalfOpenInterval is how long an open circuit waits before the
	// resolver gets a single half-open probe query.
	HalfOpenInterval time.Duration

	// Additive quality-penalty weights (applied to 0..1 rates).
	WeightTimeout   float64
	WeightErrors    float64
	WeightServfail  float64
	WeightBogus     float64
	WeightTruncated float64
	WeightFallback  float64
	WeightConnNew   float64
	WeightJitter    float64
}

// DefaultMDSParams returns the production defaults. They are intentionally
// conservative: the scheduler is sticky by default and only reacts to
// statistically meaningful differences.
func DefaultMDSParams() MDSParams {
	return MDSParams{
		WarmupSamples:     20,
		PenaltyMinSamples: 10,
		SwitchMargin:      0.15,
		TieBand:           0.03,
		Dwell:             5 * time.Second,
		ExploreProb:       0.15,
		BreakerThreshold:  3,
		HalfOpenInterval:  10 * time.Second,
		WeightTimeout:     0.5,
		WeightErrors:      0.3,
		WeightServfail:    0.2,
		WeightBogus:       0.5,
		WeightTruncated:   0.05,
		WeightFallback:    0.1,
		WeightConnNew:     0.05,
		WeightJitter:      0.1,
	}
}

// mdsCircuit tracks the breaker state machine of one resolver.
type mdsCircuit struct {
	consecHardFails uint32
	open            bool
	// probing is set while one half-open probe query is in flight.
	probing     bool
	openedAt    time.Time
	nextProbeAt time.Time

	// recoverUntilTotal marks a recovery probation started by a
	// successful half-open probe: while the resolver's lifetime sample
	// count is below this value it is judged under warmup semantics
	// (initial-RTT prior, neutral quality penalties, breaker disarmed).
	// The outage-era samples are stale and unrepresentative, so without
	// probation a genuinely recovered resolver would be kept in the
	// penalty box for minutes by its own sliding window.
	recoverUntilTotal uint64
}

// lbStrategyAdvanced is implemented by stateful strategies that select a
// resolver from the full ResolverCandidate view (metrics + static
// properties) while ServersInfo is locked.
type lbStrategyAdvanced interface {
	selectCandidateLocked(candidates []ResolverCandidate) int
}

// lbFeedbackReceiver is implemented by strategies that consume the unified
// exchange feedback stream. totalSamples is the resolver's lifetime sample
// count after the observation (warmup gating is the strategy's own policy).
type lbFeedbackReceiver interface {
	observeFeedbackLocked(name string, o ExchangeOutcome, totalSamples uint64)
}

// LBStrategyMDS is the pluggable multi-dimensional scheduler. It is
// stateful; all its methods run while the caller holds ServersInfo's lock,
// so it needs no synchronization of its own. It never reorders the inner
// server slice.
type LBStrategyMDS struct {
	params MDSParams
	now    func() time.Time
	rng    *rand.Rand

	primaryName  string
	primarySince time.Time

	circuits map[string]*mdsCircuit

	totalSwitches  uint64
	switchesByName map[string]uint64
}

// NewLBStrategyMDS builds an MDS scheduler with production clock/random
// sources.
func NewLBStrategyMDS(params MDSParams) *LBStrategyMDS {
	return newLBStrategyMDSWithDeps(params, time.Now, rand.New(rand.NewSource(time.Now().UnixNano())))
}

// newLBStrategyMDSWithDeps builds an MDS scheduler with injectable clock
// and RNG (tests and deterministic simulations).
func newLBStrategyMDSWithDeps(params MDSParams, now func() time.Time, rng *rand.Rand) *LBStrategyMDS {
	return &LBStrategyMDS{
		params:         params,
		now:            now,
		rng:            rng,
		circuits:       make(map[string]*mdsCircuit),
		switchesByName: make(map[string]uint64),
	}
}

// LBStrategy interface compliance. MDS is dispatched through the
// lbStrategyAdvanced interface; these are defensive fallbacks only.
func (mds *LBStrategyMDS) getCandidate(int) int { return 0 }
func (mds *LBStrategyMDS) getActiveCount(serversCount int) int {
	return serversCount
}

// MDSTats is an observation snapshot of scheduler state.
type MDSTats struct {
	PrimaryName    string
	TotalSwitches  uint64
	SwitchesByName map[string]uint64
	OpenCircuits   []string
}

// Stats returns a copy of the scheduler state for logging/observability.
func (mds *LBStrategyMDS) Stats() MDSTats {
	open := make([]string, 0)
	for name, cb := range mds.circuits {
		if cb.open {
			open = append(open, name)
		}
	}
	byName := make(map[string]uint64, len(mds.switchesByName))
	for name, n := range mds.switchesByName {
		byName[name] = n
	}
	return MDSTats{
		PrimaryName:    mds.primaryName,
		TotalSwitches:  mds.totalSwitches,
		SwitchesByName: byName,
		OpenCircuits:   open,
	}
}

// mdsScored is the internal per-candidate scoring result.
type mdsScored struct {
	name    string
	warming bool
	open    bool
	// tailMs is the raw tail-latency indicator used for normalization.
	tailMs float64
	// cost is the relatively normalized latency cost in [0,1].
	cost float64
	// penalty is the sum of weighted quality penalties (0 while warming
	// or below the minimum sample denominator).
	penalty float64
	// base is the final score excluding soft preferences (higher better).
	base float64
	// soft is the tie-break-only preference score derived from the
	// resolver's stamp properties and observed ECS behavior.
	soft float64
}

// scoreAll evaluates every candidate. It is allocation-bounded by the
// number of live resolvers and performs only O(n) percentile reads.
func (mds *LBStrategyMDS) scoreAll(candidates []ResolverCandidate) []mdsScored {
	p := mds.params
	scored := make([]mdsScored, len(candidates))

	// One histogram accumulator is reused for every percentile read, so
	// scoring allocates O(1) heap buffers regardless of pool size.
	var pctBuf []uint64
	bestTail, worstTail := math.MaxFloat64, 0.0
	for i, c := range candidates {
		w := c.Metrics.Snapshot()
		warming := mds.isWarmingLocked(c.Server.Name, w.TotalSamples, p.WarmupSamples)

		initialMs := float64(c.Server.initialRtt)
		tail := initialMs
		jitterRatio := 0.0
		penalty := 0.0

		if !warming {
			latency := w.LatencyEwmaMs
			if latency <= 0 {
				latency = initialMs
			}
			tail = latency
			if p95, ok, buf := c.Metrics.PercentileBuf(0.95, pctBuf); ok {
				pctBuf = buf
				p95ms := float64(p95) / float64(time.Millisecond)
				tail = 0.6*latency + 0.4*p95ms
			}
			if tail <= 0 {
				tail = initialMs
			}
			if w.Samples >= p.PenaltyMinSamples {
				samples := float64(w.Samples)
				rate := func(n uint64) float64 { return float64(n) / samples }
				penalty += p.WeightTimeout * rate(w.Timeout)
				penalty += p.WeightErrors * rate(w.Errors)
				penalty += p.WeightServfail * rate(w.Servfail)
				penalty += p.WeightBogus * rate(w.DNSSECBogus)
				penalty += p.WeightTruncated * rate(w.Truncated())
				penalty += p.WeightFallback * rate(w.TCPFallback+w.QUICFallback)
				penalty += p.WeightConnNew * rate(w.ConnNew)
				if tail > 0 {
					jitterRatio = math.Min(1, w.JitterEwmaMs/tail)
				}
				penalty += p.WeightJitter * jitterRatio
			}
		}

		if tail < bestTail {
			bestTail = tail
		}
		if tail > worstTail {
			worstTail = tail
		}
		scored[i] = mdsScored{
			name:    c.Server.Name,
			warming: warming,
			tailMs:  tail,
			penalty: penalty,
			open:    mds.circuits[c.Server.Name] != nil && mds.circuits[c.Server.Name].open,
			soft:    mds.softPreference(c.Server, w, warming),
		}
	}

	spread := worstTail - bestTail
	for i := range scored {
		cost := 0.0
		if spread > 1e-9 {
			cost = (scored[i].tailMs - bestTail) / spread
			if cost < 0 {
				cost = 0
			} else if cost > 1 {
				cost = 1
			}
		}
		scored[i].cost = cost
		scored[i].base = 1 - cost - scored[i].penalty
	}
	return scored
}

// softPreference quantifies non-eliminating characteristics: NoFilter and
// DNSSEC stamp claims plus observed ECS echo behavior. It can only decide
// near ties.
func (mds *LBStrategyMDS) softPreference(server *ServerInfo, w MetricsWindow, warming bool) float64 {
	soft := 0.0
	if server.SupportsNoFilter() {
		soft += 1
	}
	if server.SupportsDNSSEC() {
		soft += 1
	}
	// ECS echo is only trusted once a few samples are in; it is a hint,
	// never an elimination criterion.
	if !warming && w.Samples >= mds.params.PenaltyMinSamples {
		soft += float64(w.ECSReturned) / float64(w.Samples)
	}
	return soft
}

// better reports whether a is strictly preferred to b. Outside the tie
// band the base score decides; inside it soft preferences (and then the
// name for deterministic stability) decide.
func (mds *LBStrategyMDS) better(a, b mdsScored) bool {
	if math.Abs(a.base-b.base) > mds.params.TieBand {
		return a.base > b.base
	}
	if a.soft != b.soft {
		return a.soft > b.soft
	}
	return a.name < b.name
}

// selectCandidateLocked implements lbStrategyAdvanced. The returned index
// addresses the candidates slice; the caller maps it back to inner by name.
func (mds *LBStrategyMDS) selectCandidateLocked(candidates []ResolverCandidate) int {
	n := len(candidates)
	if n == 0 {
		return 0
	}
	if n == 1 {
		mds.becomePrimary(candidates[0].Server.Name)
		return 0
	}

	now := mds.now()
	scored := mds.scoreAll(candidates)

	primaryIdx := -1
	for i := range scored {
		if scored[i].name == mds.primaryName {
			primaryIdx = i
			break
		}
	}

	// 1. Half-open probe: an open resolver due for recovery gets exactly
	// one exploratory query.
	if probeIdx := mds.pickHalfOpenProbe(scored, now); probeIdx >= 0 {
		mds.circuits[scored[probeIdx].name].probing = true
		return probeIdx
	}

	// 2. Random exploration (warming resolvers first). It routes a single
	// query without deposing the sticky primary, so it never adds switch
	// churn.
	if mds.rng.Float64() < mds.params.ExploreProb {
		if exploreIdx := mds.pickExploreTarget(scored, primaryIdx); exploreIdx >= 0 {
			return exploreIdx
		}
	}

	// 3. Sticky primary path: an open primary is bypassed immediately
	// (breaker switch is exempt from dwell/margin).
	if primaryIdx >= 0 && !scored[primaryIdx].open {
		bestOther := mds.bestHealthy(scored, primaryIdx)
		if bestOther < 0 {
			return primaryIdx
		}
		if now.Sub(mds.primarySince) < mds.params.Dwell {
			return primaryIdx
		}
		if scored[bestOther].base-scored[primaryIdx].base >= mds.params.SwitchMargin &&
			mds.better(scored[bestOther], scored[primaryIdx]) {
			mds.becomePrimary(scored[bestOther].name)
			return bestOther
		}
		return primaryIdx
	}

	// 4. No primary (yet) or the primary is circuit-open: take the best
	// healthy resolver immediately.
	best := mds.bestHealthy(scored, -1)
	if best < 0 {
		// Every circuit is open: fall back to the highest score so service
		// continues, and let the breaker probes recover nodes.
		best = 0
		for i := 1; i < n; i++ {
			if mds.better(scored[i], scored[best]) {
				best = i
			}
		}
	}
	mds.becomePrimary(scored[best].name)
	return best
}

// pickHalfOpenProbe returns the open resolver that has waited longest past
// its next-probe time, or -1.
func (mds *LBStrategyMDS) pickHalfOpenProbe(scored []mdsScored, now time.Time) int {
	pick := -1
	var earliest time.Time
	for i := range scored {
		if !scored[i].open {
			continue
		}
		cb := mds.circuits[scored[i].name]
		if cb.probing || cb.nextProbeAt.After(now) {
			continue
		}
		if pick < 0 || cb.openedAt.Before(earliest) {
			pick, earliest = i, cb.openedAt
		}
	}
	return pick
}

// pickExploreTarget returns an exploratory candidate: a warming resolver
// when any exists, otherwise any healthy non-primary resolver.
func (mds *LBStrategyMDS) pickExploreTarget(scored []mdsScored, primaryIdx int) int {
	warming := make([]int, 0, len(scored))
	healthy := make([]int, 0, len(scored))
	for i := range scored {
		if i == primaryIdx || scored[i].open {
			continue
		}
		healthy = append(healthy, i)
		if scored[i].warming {
			warming = append(warming, i)
		}
	}
	pool := healthy
	if len(warming) > 0 {
		pool = warming
	}
	if len(pool) == 0 {
		return -1
	}
	return pool[mds.rng.Intn(len(pool))]
}

// bestHealthy returns the preferred non-open candidate according to score
// ordering, optionally excluding one index.
func (mds *LBStrategyMDS) bestHealthy(scored []mdsScored, exclude int) int {
	best := -1
	for i := range scored {
		if i == exclude || scored[i].open {
			continue
		}
		if best < 0 || mds.better(scored[i], scored[best]) {
			best = i
		}
	}
	return best
}

// becomePrimary records a primary transition. Re-selection of the same
// primary is a no-op.
func (mds *LBStrategyMDS) becomePrimary(name string) {
	if mds.primaryName == name {
		return
	}
	mds.totalSwitches++
	mds.switchesByName[name]++
	mds.primaryName = name
	mds.primarySince = mds.now()
}

// isWarmingLocked reports whether a resolver must be judged under warmup
// semantics: either it has not accumulated enough lifetime samples yet, or
// it is inside a post-breaker recovery probation started by a successful
// half-open probe.
func (mds *LBStrategyMDS) isWarmingLocked(name string, totalSamples, warmupSamples uint64) bool {
	if totalSamples < warmupSamples {
		return true
	}
	if cb := mds.circuits[name]; cb != nil && cb.recoverUntilTotal > totalSamples {
		return true
	}
	return false
}

// circuitFor returns the breaker state of a resolver, creating it lazily.
func (mds *LBStrategyMDS) circuitFor(name string) *mdsCircuit {
	cb := mds.circuits[name]
	if cb == nil {
		cb = &mdsCircuit{}
		mds.circuits[name] = cb
	}
	return cb
}

// observeFeedbackLocked implements lbFeedbackReceiver. It is called once
// per exchange from ServersInfo.observeOutcome, with the resolver's
// post-observation warmup state.
func (mds *LBStrategyMDS) observeFeedbackLocked(name string, o ExchangeOutcome, totalSamples uint64) {
	cb := mds.circuitFor(name)
	now := mds.now()

	if o.Timeout || o.Error {
		if cb.open && cb.probing {
			// Half-open probe failed: close the probe window and wait
			// another interval.
			cb.consecHardFails++
			cb.probing = false
			cb.nextProbeAt = now.Add(mds.params.HalfOpenInterval)
			return
		}
		// A hard failure inside recovery probation proves the successful
		// probe was a false positive: abort the neutral scoring period
		// immediately and resume ordinary breaker accounting. This single
		// failure seeds a fresh mature streak.
		if cb.recoverUntilTotal > totalSamples {
			cb.recoverUntilTotal = 0
			cb.consecHardFails = 1
			return
		}
		// Cold-start protection: failures observed while the resolver has
		// no representative sample base never seed the mature streak and
		// can never trip the breaker.
		if totalSamples < mds.params.WarmupSamples {
			cb.consecHardFails = 0
			return
		}
		cb.consecHardFails++
		if !cb.open && cb.consecHardFails >= mds.params.BreakerThreshold {
			cb.open = true
			cb.openedAt = now
			cb.nextProbeAt = now.Add(mds.params.HalfOpenInterval)
			cb.probing = false
			cb.recoverUntilTotal = 0
		}
		return
	}

	// Any response (including SERVFAIL/bogus) demonstrates reachability.
	cb.consecHardFails = 0
	if cb.open && cb.probing {
		cb.open = false
		cb.probing = false
		// The probe proved reachability. Re-enter warmup semantics until a
		// fresh batch of samples accumulates, so outage-era window data
		// cannot delay reintegration of a genuinely recovered resolver.
		cb.recoverUntilTotal = totalSamples + mds.params.WarmupSamples
	}
}

// Compile-time interface assertion.
var _ LBStrategy = (*LBStrategyMDS)(nil)
