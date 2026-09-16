package main

import (
	"testing"
	"time"

	stamps "github.com/jedisct1/go-dnsstamps"
)

func TestMetricsRegistrySurvivesServerRefresh(t *testing.T) {
	serversInfo := NewServersInfo()

	serversInfo.Lock()
	serversInfo.inner = []*ServerInfo{{Name: "resolver-a"}}
	metrics := serversInfo.metricsForLocked("resolver-a")
	metrics.Observe(ExchangeOutcome{Duration: 12 * time.Millisecond})
	serversInfo.Unlock()

	// Simulate a certificate refresh: the ServerInfo object is replaced by a
	// fresh pointer under the same resolver name.
	serversInfo.Lock()
	serversInfo.inner[0] = &ServerInfo{Name: "resolver-a", initialRtt: 42}
	candidates := serversInfo.candidatesLocked()
	serversInfo.Unlock()

	if len(candidates) != 1 {
		t.Fatalf("candidates = %d, want 1", len(candidates))
	}
	if candidates[0].Metrics != metrics {
		t.Fatal("metrics store must be reused by name across ServerInfo replacement")
	}
	if got := candidates[0].Metrics.TotalSamples(); got != 1 {
		t.Fatalf("historical samples lost across refresh: %d", got)
	}
	if candidates[0].Server.initialRtt != 42 {
		t.Fatal("candidate must expose the refreshed ServerInfo")
	}

	// A second name gets its own independent store, pre-published in the
	// same critical section (mirroring refreshServer's invariant: a name
	// visible in inner always has a registered metrics store).
	serversInfo.Lock()
	serversInfo.inner = append(serversInfo.inner, &ServerInfo{Name: "resolver-b"})
	metricsB := serversInfo.metricsForLocked("resolver-b")
	candidates = serversInfo.candidatesLocked()
	serversInfo.Unlock()
	if len(candidates) != 2 || candidates[1].Metrics == candidates[0].Metrics {
		t.Fatalf("expected two independent metric stores, got %+v", candidates)
	}
	if candidates[1].Metrics != metricsB {
		t.Fatal("candidate must expose the published metrics store")
	}
}

// Refreshed-but-never-queried resolvers (no published metrics store yet in
// test setups) must not let concurrent RLock scrapers mutate the metrics
// map. Before the fix candidatesLocked lazily inserted under a read lock,
// so two scrapers produced concurrent map writes. Run with -race.
func TestCandidatesLockedConcurrentScrape(t *testing.T) {
	serversInfo := NewServersInfo()

	// Live resolvers without any observation, the real production window
	// between certificate refresh and the first dispatched query.
	serversInfo.Lock()
	for _, name := range []string{"r1", "r2", "r3", "r4"} {
		serversInfo.inner = append(serversInfo.inner, &ServerInfo{Name: name})
	}
	serversInfo.Unlock()

	done := make(chan struct{})
	for range 4 {
		go func() {
			defer func() { done <- struct{}{} }()
			for range 200 {
				serversInfo.RLock()
				cands := serversInfo.candidatesLocked()
				for _, c := range cands {
					_ = c.Metrics.Snapshot()
				}
				serversInfo.RUnlock()
			}
		}()
	}
	go func() {
		defer func() { done <- struct{}{} }()
		for i := range 100 {
			serversInfo.Lock()
			name := "late-" + string(rune('a'+i%26)) + string(rune('a'+(i/26)%26))
			serversInfo.inner = append(serversInfo.inner, &ServerInfo{Name: name})
			serversInfo.metricsForLocked(name) // production-style publish
			serversInfo.Unlock()
		}
	}()

	for range 5 {
		<-done
	}

	// After the writer publishes a store, later candidate views must use
	// that exact instance (never a throwaway duplicate).
	serversInfo.RLock()
	for _, c := range serversInfo.candidatesLocked() {
		if c.Server.Name == "late-aa" && c.Metrics != serversInfo.metrics["late-aa"] {
			t.Fatal("published metrics store was shadowed by a throwaway")
		}
	}
	serversInfo.RUnlock()
}

func TestServerInfoProps(t *testing.T) {
	all := &ServerInfo{Props: stamps.ServerInformalPropertyDNSSEC |
		stamps.ServerInformalPropertyNoLog | stamps.ServerInformalPropertyNoFilter}
	if !all.SupportsDNSSEC() || !all.SupportsNoLog() || !all.SupportsNoFilter() {
		t.Fatal("all property bits must be reported")
	}

	none := &ServerInfo{}
	if none.SupportsDNSSEC() || none.SupportsNoLog() || none.SupportsNoFilter() {
		t.Fatal("empty props must report no feature")
	}

	dnssecOnly := &ServerInfo{Props: stamps.ServerInformalPropertyDNSSEC}
	if !dnssecOnly.SupportsDNSSEC() || dnssecOnly.SupportsNoFilter() {
		t.Fatal("property helpers must mask independent bits")
	}
}
