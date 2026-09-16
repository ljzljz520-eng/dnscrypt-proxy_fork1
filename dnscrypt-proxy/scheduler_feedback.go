package main

import (
	"errors"
	"net"
	"time"

	"codeberg.org/miekg/dns"
)

// classifyExchangeResponse inspects a raw DNS response exactly once and
// reports response-level quality signals:
//   - servfail: upstream RCODE is SERVFAIL and DNSSEC was not expected,
//   - bogus:    a SERVFAIL was produced while DNSSEC validation was active,
//   - ecs:      the response carried an ECS option with a non-zero scope.
//
// Malformed packets yield all-false.
func classifyExchangeResponse(response []byte, dnssecExpected bool) (servfail bool, bogus bool, ecs bool) {
	if len(response) < MinDNSPacketSize {
		return false, false, false
	}
	if Rcode(response) == dns.RcodeServerFailure {
		if dnssecExpected {
			bogus = true
		} else {
			servfail = true
		}
	}
	msg := dns.Msg{Data: response}
	if err := msg.Unpack(); err != nil {
		return servfail, bogus, false
	}
	// In this dns fork unpacked EDNS options live directly in the Pseudo
	// section (the OPT RR itself is virtual).
	for _, rr := range msg.Pseudo {
		if subnet, ok := rr.(*dns.SUBNET); ok && subnet.Scope != 0 {
			return servfail, bogus, true
		}
	}
	return servfail, bogus, false
}

// observeOutcome is the single feedback chokepoint for every upstream
// exchange. It feeds the multi-dimensional ResolverMetrics store while
// preserving the legacy RTT EWMA and WP2 counter semantics, so that the
// pre-existing strategies score resolvers exactly as they did before the
// scheduler overhaul.
//
// It must be called exactly once per dispatched exchange, after response
// post-processing (so rcode and DNSSEC state are final). ps.exchange must
// already carry the transport-layer signals; ps.exchangeStart is the
// exchange anchor timestamp.
func (serversInfo *ServersInfo) observeOutcome(proxy *Proxy, ps *PluginsState, response []byte, exchangeErr error) {
	name := ps.serverName
	if len(name) == 0 || ps.exchangeStart.IsZero() {
		return
	}

	now := serversInfo.now()
	o := ps.exchange

	validResponse := len(response) >= MinDNSPacketSize && len(response) <= MaxDNSPacketSize
	failed := exchangeErr != nil || !validResponse
	switch {
	case failed && ps.returnCode == PluginsReturnCodeServerTimeout:
		o.Timeout = true
	case failed && isTimeoutError(exchangeErr):
		o.Timeout = true
	case failed:
		// Fast network/parse errors never produced a response.
		o.Error = true
	case !failed:
		o.Servfail, o.DNSSECBogus, o.ECSReturned = classifyExchangeResponse(response, ps.dnssec)
	}

	elapsed := now.Sub(ps.exchangeStart)
	if failed {
		// A failed exchange occupies the full deadline in the latency
		// distribution, as in the legacy RTT penalty.
		o.Duration = proxy.timeout
	} else {
		o.Duration = elapsed
	}

	serversInfo.Lock()
	server := serversInfo.serverByNameLocked(name)
	if server != nil {
		// Legacy WP2 success definition: any non-error valid response,
		// SERVFAIL/bogus included.
		server.totalQueries++
		if failed {
			server.failedQueries++
		}
		server.lastUpdateTime = now
		if server.totalQueries > 10000 {
			server.totalQueries /= 2
			server.failedQueries /= 2
		}

		// Legacy RTT EWMA dual write:
		//  - failures and plain SERVFAIL pay the timeout penalty,
		//  - DNSSEC-bogus responses did not touch the EWMA historically,
		//  - responses dropped by a response plugin returned before
		//    noticeSuccess historically and never touched the EWMA,
		//  - other successes contribute their measured exchange time.
		switch {
		case failed || o.Servfail:
			server.rtt.Add(float64(proxy.timeout / time.Millisecond))
		case o.DNSSECBogus || ps.action == PluginsActionDrop:
			// no legacy RTT update
		default:
			elapsedMs := elapsed / time.Millisecond
			if elapsedMs > 0 && elapsed < proxy.timeout {
				server.rtt.Add(float64(elapsedMs))
			}
		}
	}
	metrics := serversInfo.metricsForLocked(name)
	metrics.Observe(o)
	if receiver, ok := serversInfo.lbStrategy.(lbFeedbackReceiver); ok {
		receiver.observeFeedbackLocked(name, o, metrics.TotalSamples())
	}
	serversInfo.Unlock()
}

// serverByNameLocked returns the live ServerInfo registered under name.
func (serversInfo *ServersInfo) serverByNameLocked(name string) *ServerInfo {
	for _, server := range serversInfo.inner {
		if server.Name == name {
			return server
		}
	}
	return nil
}

// isTimeoutError reports whether err is (or wraps) a deadline/timeout error.
func isTimeoutError(err error) bool {
	var neterr net.Error
	if errors.As(err, &neterr) {
		return neterr.Timeout()
	}
	return false
}
