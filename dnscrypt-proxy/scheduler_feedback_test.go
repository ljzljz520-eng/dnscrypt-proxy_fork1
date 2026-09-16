package main

import (
	"net/netip"
	"testing"
	"time"

	"codeberg.org/miekg/dns"
	"github.com/VividCortex/ewma"
)

const feedbackServerName = "test-resolver"

type timeoutTestErr struct{}

func (timeoutTestErr) Error() string   { return "i/o timeout" }
func (timeoutTestErr) Timeout() bool   { return true }
func (timeoutTestErr) Temporary() bool { return true }

type plainTestErr struct{}

func (plainTestErr) Error() string { return "connection refused" }

func newFeedbackHarness() (*Proxy, *ServersInfo, *ServerInfo, time.Time) {
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	proxy := NewProxy()
	proxy.timeout = 5 * time.Second
	si := &proxy.serversInfo
	si.now = func() time.Time { return base }
	server := &ServerInfo{Name: feedbackServerName}
	server.rtt = ewma.NewMovingAverage(RTTEwmaDecay)
	server.rtt.Set(50)
	si.inner = append(si.inner, server)
	return proxy, si, server, base
}

func feedbackState(proxy *Proxy, base time.Time, elapsed time.Duration, o ExchangeOutcome) *PluginsState {
	ps := NewPluginsState(proxy, "udp", nil, "udp", base)
	ps.serverName = feedbackServerName
	ps.exchangeStart = base.Add(-elapsed)
	ps.exchange = o
	return &ps
}

func mustPackResponse(t *testing.T, rcode uint16, ecsScope uint8) []byte {
	t.Helper()
	msg := dns.NewMsg("example.", dns.TypeA)
	msg.Response = true
	msg.Rcode = rcode
	if ecsScope > 0 {
		msg.Pseudo = append(msg.Pseudo, &dns.OPT{Options: []dns.EDNS0{
			&dns.SUBNET{Family: 1, Netmask: 24, Scope: ecsScope, Address: netip.MustParseAddr("192.0.2.0")},
		}})
	}
	if err := msg.Pack(); err != nil {
		t.Fatal(err)
	}
	return msg.Data
}

// A response dropped by a response plugin historically returned before
// noticeSuccess: it counted toward the query total but never touched the
// legacy RTT EWMA. The multi-dimensional metrics still record the real
// upstream sample.
func TestObserveOutcomeDropSkipsLegacyRTT(t *testing.T) {
	proxy, si, server, base := newFeedbackHarness()
	before := server.rtt.Value()

	ps := feedbackState(proxy, base, 800*time.Millisecond, ExchangeOutcome{
		Transport:  TransportDNSCryptUDP,
		ConnReused: true,
	})
	ps.action = PluginsActionDrop
	resp := mustPackResponse(t, dns.RcodeSuccess, 0)
	si.observeOutcome(proxy, ps, resp, nil)

	if got := server.rtt.Value(); got != before {
		t.Fatalf("dropped response polluted the legacy RTT EWMA: %f -> %f", before, got)
	}
	// Legacy WP2 stats still treated the exchange as a counted success.
	if server.totalQueries != 1 || server.failedQueries != 0 {
		t.Fatalf("legacy query stats wrong: total=%d failed=%d", server.totalQueries, server.failedQueries)
	}
	// The rich metrics window keeps the genuine upstream observation.
	w := si.MetricsFor(feedbackServerName).Snapshot()
	if w.Samples != 1 || w.Success != 1 {
		t.Fatalf("dropped exchange missing from new metrics: samples=%d success=%d", w.Samples, w.Success)
	}
}

func TestObserveOutcomeDimensions(t *testing.T) {
	noError := mustPackResponse(t, dns.RcodeSuccess, 0)
	servfail := mustPackResponse(t, dns.RcodeServerFailure, 0)
	servfailECS := mustPackResponse(t, dns.RcodeServerFailure, 24)
	servfailECSZeroScope := mustPackResponse(t, dns.RcodeServerFailure, 0)
	_ = servfailECSZeroScope

	tests := []struct {
		name         string
		outcome      ExchangeOutcome
		dnssec       bool
		returnCode   PluginsReturnCode
		response     []byte
		err          error
		wantSuccess  uint64
		wantTimeout  uint64
		wantError    uint64
		wantServfail uint64
		wantBogus    uint64
		wantTCP      uint64
		wantQUIC     uint64
		wantLocalTC  uint64
		wantUpTC     uint64
		wantReused   uint64
		wantECS      uint64
		wantFailed   uint64
	}{
		{
			name:        "success-noerror-reused-conn",
			outcome:     ExchangeOutcome{Transport: TransportDoHH2, ConnReused: true},
			response:    noError,
			wantSuccess: 1, wantReused: 1,
		},
		{
			name:        "timeout-returncode",
			outcome:     ExchangeOutcome{Transport: TransportDNSCryptUDP},
			returnCode:  PluginsReturnCodeServerTimeout,
			err:         timeoutTestErr{},
			wantTimeout: 1, wantFailed: 1,
		},
		{
			name:        "timeout-net-error",
			outcome:     ExchangeOutcome{Transport: TransportDoHH3},
			returnCode:  PluginsReturnCodeNetworkError,
			err:         timeoutTestErr{},
			wantTimeout: 1, wantFailed: 1,
		},
		{
			name:       "network-error-fast",
			outcome:    ExchangeOutcome{Transport: TransportDNSCryptUDP},
			returnCode: PluginsReturnCodeNetworkError,
			err:        plainTestErr{},
			wantError:  1, wantFailed: 1,
		},
		{
			name:         "servfail-plain",
			outcome:      ExchangeOutcome{ConnReused: true},
			response:     servfail,
			wantServfail: 1, wantReused: 1,
		},
		{
			name:      "dnssec-bogus-servfail",
			outcome:   ExchangeOutcome{ConnReused: true},
			dnssec:    true,
			response:  servfail,
			wantBogus: 1, wantReused: 1,
		},
		{
			name:        "tcp-fallback-after-upstream-tc",
			outcome:     ExchangeOutcome{Transport: TransportDNSCryptTCP, TCPFallback: true, UpstreamTruncated: true},
			response:    noError,
			wantSuccess: 1, wantTCP: 1, wantUpTC: 1,
		},
		{
			name:        "quic-fallback-h2",
			outcome:     ExchangeOutcome{Transport: TransportDoHH2, QUICFallback: true},
			response:    noError,
			wantSuccess: 1, wantQUIC: 1,
		},
		{
			name:        "local-truncation",
			outcome:     ExchangeOutcome{LocalTruncated: true, ConnReused: true},
			response:    noError,
			wantSuccess: 1, wantLocalTC: 1, wantReused: 1,
		},
		{
			name:         "ecs-returned",
			outcome:      ExchangeOutcome{ConnReused: true},
			response:     servfailECS,
			wantServfail: 1, wantECS: 1, wantReused: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			proxy, si, _, base := newFeedbackHarness()
			ps := feedbackState(proxy, base, 30*time.Millisecond, tc.outcome)
			ps.returnCode = tc.returnCode
			ps.dnssec = tc.dnssec

			si.observeOutcome(proxy, ps, tc.response, tc.err)

			w := si.MetricsFor(feedbackServerName).Snapshot()
			if w.Samples != 1 {
				t.Fatalf("Samples=%d, want 1", w.Samples)
			}
			if w.Success != tc.wantSuccess {
				t.Errorf("Success=%d want %d", w.Success, tc.wantSuccess)
			}
			if w.Timeout != tc.wantTimeout {
				t.Errorf("Timeout=%d want %d", w.Timeout, tc.wantTimeout)
			}
			if w.Errors != tc.wantError {
				t.Errorf("Errors=%d want %d", w.Errors, tc.wantError)
			}
			if w.Servfail != tc.wantServfail {
				t.Errorf("Servfail=%d want %d", w.Servfail, tc.wantServfail)
			}
			if w.DNSSECBogus != tc.wantBogus {
				t.Errorf("DNSSECBogus=%d want %d", w.DNSSECBogus, tc.wantBogus)
			}
			if w.TCPFallback != tc.wantTCP {
				t.Errorf("TCPFallback=%d want %d", w.TCPFallback, tc.wantTCP)
			}
			if w.QUICFallback != tc.wantQUIC {
				t.Errorf("QUICFallback=%d want %d", w.QUICFallback, tc.wantQUIC)
			}
			if w.LocalTruncated != tc.wantLocalTC {
				t.Errorf("LocalTruncated=%d want %d", w.LocalTruncated, tc.wantLocalTC)
			}
			if w.UpstreamTruncated != tc.wantUpTC {
				t.Errorf("UpstreamTruncated=%d want %d", w.UpstreamTruncated, tc.wantUpTC)
			}
			if w.ConnReused != tc.wantReused {
				t.Errorf("ConnReused=%d want %d", w.ConnReused, tc.wantReused)
			}
			if w.ECSReturned != tc.wantECS {
				t.Errorf("ECSReturned=%d want %d", w.ECSReturned, tc.wantECS)
			}
			server := si.inner[0]
			if server.failedQueries != tc.wantFailed {
				t.Errorf("legacy failedQueries=%d want %d", server.failedQueries, tc.wantFailed)
			}
			if server.totalQueries != 1 {
				t.Errorf("legacy totalQueries=%d want 1", server.totalQueries)
			}
		})
	}
}

// TestObserveOutcomeLegacyRTTEquivalence replays a fixed mixed sequence
// through observeOutcome and asserts that the legacy RTT EWMA and
// total/failed counters evolve exactly like the pre-overhaul
// noticeSuccess/noticeFailure + updateServerStats rules.
func TestObserveOutcomeLegacyRTTEquivalence(t *testing.T) {
	proxy, si, server, base := newFeedbackHarness()
	noerror := mustPackResponse(t, dns.RcodeSuccess, 0)
	servfail := mustPackResponse(t, dns.RcodeServerFailure, 0)

	// Reference model fed exactly like the old code did.
	ref := ewma.NewMovingAverage(RTTEwmaDecay)
	ref.Set(50)
	timeoutMs := proxy.timeout / time.Millisecond

	type step struct {
		elapsed        time.Duration
		response       []byte
		err            error
		dnssec         bool
		legacyRTTAdded bool // whether the old path touched the EWMA
		legacyMs       float64
		legacyFail     bool
	}
	steps := []step{
		{30 * time.Millisecond, noerror, nil, false, true, 30, false},
		{25 * time.Millisecond, noerror, nil, false, true, 25, false},
		{proxy.timeout, nil, timeoutTestErr{}, false, true, float64(timeoutMs), true},
		{40 * time.Millisecond, servfail, nil, false, true, float64(timeoutMs), false}, // plain SERVFAIL: valid response, timeout penalty
		{40 * time.Millisecond, servfail, nil, true, false, 0, false},                  // bogus: no EWMA touch
		{15 * time.Millisecond, noerror, nil, false, true, 15, false},
		{proxy.timeout, nil, plainTestErr{}, false, true, float64(timeoutMs), true},
	}

	var refTotal, refFailed uint64
	for i, s := range steps {
		ps := feedbackState(proxy, base, s.elapsed, ExchangeOutcome{Transport: TransportDNSCryptUDP})
		ps.dnssec = s.dnssec
		if s.err != nil {
			ps.returnCode = PluginsReturnCodeNetworkError
		}
		si.observeOutcome(proxy, ps, s.response, s.err)

		if s.legacyRTTAdded {
			ref.Add(s.legacyMs)
		}
		refTotal++
		if s.legacyFail {
			refFailed++
		}

		if got, want := server.rtt.Value(), ref.Value(); got != want {
			t.Fatalf("step %d: rtt EWMA = %v, legacy reference = %v", i, got, want)
		}
		if server.totalQueries != refTotal {
			t.Fatalf("step %d: totalQueries=%d want %d", i, server.totalQueries, refTotal)
		}
		if server.failedQueries != refFailed {
			t.Fatalf("step %d: failedQueries=%d want %d", i, server.failedQueries, refFailed)
		}
	}
}

// TestObserveOutcomeIgnoredWithoutExchange ensures cached/synthesized
// queries (no dispatched exchange) produce no feedback.
func TestObserveOutcomeIgnoredWithoutExchange(t *testing.T) {
	proxy, si, _, base := newFeedbackHarness()
	ps := NewPluginsState(proxy, "udp", nil, "udp", base)
	ps.serverName = feedbackServerName
	noerror := mustPackResponse(t, dns.RcodeSuccess, 0)
	si.observeOutcome(proxy, &ps, noerror, nil)
	if w := si.MetricsFor(feedbackServerName).Snapshot(); w.TotalSamples != 0 {
		t.Fatalf("unexpected observation without exchange: %+v", w)
	}
}
