package main

import (
	"net"
	"testing"
)

func TestApplyHTTPFetchTrace(t *testing.T) {
	// A nil trace must leave the exchange untouched.
	ps := &PluginsState{}
	applyHTTPFetchTrace(ps, nil, false)
	if ps.exchange.Transport != TransportUnknown {
		t.Fatalf("nil trace changed transport: %v", ps.exchange.Transport)
	}

	// DoH served over HTTP/3 with a reused QUIC connection.
	ps = &PluginsState{}
	applyHTTPFetchTrace(ps, &FetchTrace{
		UsedHTTP3:       true,
		ConnReused:      true,
		NegotiatedProto: "h3",
	}, false)
	if ps.exchange.Transport != TransportDoHH3 {
		t.Fatalf("expected DoHH3, got %v", ps.exchange.Transport)
	}
	if !ps.exchange.ConnReused || ps.exchange.QUICFallback {
		t.Fatalf("unexpected h3 signals: %+v", ps.exchange)
	}

	// DoH that probed HTTP/3 and fell back to HTTP/2 over a fresh
	// connection.
	ps = &PluginsState{}
	applyHTTPFetchTrace(ps, &FetchTrace{
		UsedHTTP3:       false,
		QUICFallback:    true,
		ConnReused:      false,
		NegotiatedProto: "h2",
	}, false)
	if ps.exchange.Transport != TransportDoHH2 {
		t.Fatalf("expected DoHH2, got %v", ps.exchange.Transport)
	}
	if !ps.exchange.QUICFallback || ps.exchange.ConnReused {
		t.Fatalf("unexpected fallback signals: %+v", ps.exchange)
	}

	// ODoH keeps its own transport classification even when served on
	// HTTP/3.
	ps = &PluginsState{}
	applyHTTPFetchTrace(ps, &FetchTrace{UsedHTTP3: true, NegotiatedProto: "h3"}, true)
	if ps.exchange.Transport != TransportODoH {
		t.Fatalf("expected ODoH, got %v", ps.exchange.Transport)
	}
}

func TestUDPConnPoolGetReuseSignal(t *testing.T) {
	pool := NewUDPConnPool()
	defer pool.Close()

	addr, err := net.ResolveUDPAddr("udp", "127.0.0.1:53530")
	if err != nil {
		t.Fatal(err)
	}
	// A first Get must dial a fresh socket.
	conn, reused, err := pool.Get(addr)
	if err != nil {
		t.Fatal(err)
	}
	if reused {
		t.Fatal("first Get unexpectedly reported a pooled connection")
	}

	// Return it to the pool; the next Get must report reuse.
	pool.Put(addr, conn)
	conn2, reused, err := pool.Get(addr)
	if err != nil {
		t.Fatal(err)
	}
	if !reused {
		t.Fatal("second Get did not report pool reuse")
	}
	conn2.Close()
}
