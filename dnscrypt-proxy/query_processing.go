package main

import (
	"math/rand"
	"net"
	"time"

	"codeberg.org/miekg/dns"
	"github.com/jedisct1/dlog"
	stamps "github.com/jedisct1/go-dnsstamps"
)

// validateQuery - Performs basic validation on the incoming query
func validateQuery(query []byte) bool {
	if len(query) < MinDNSPacketSize {
		return false
	}
	if len(query) > MaxDNSPacketSize {
		return false
	}
	return true
}

// applyHTTPFetchTrace records transport-layer observations of an HTTP-based
// exchange (DoH/ODoH) on the plugin state. ODoH keeps its own transport
// classification while still inheriting fallback/connection-reuse signals.
func applyHTTPFetchTrace(pluginsState *PluginsState, ft *FetchTrace, odoh bool) {
	if ft == nil {
		return
	}
	pluginsState.exchange.QUICFallback = ft.QUICFallback
	pluginsState.exchange.ConnReused = ft.ConnReused
	switch {
	case odoh:
		pluginsState.exchange.Transport = TransportODoH
	case ft.UsedHTTP3 || ft.NegotiatedProto == "h3":
		pluginsState.exchange.Transport = TransportDoHH3
	default:
		pluginsState.exchange.Transport = TransportDoHH2
	}
}

// handleSynthesizedResponse - Handles a synthesized DNS response from plugins
func handleSynthesizedResponse(pluginsState *PluginsState, synth *dns.Msg) ([]byte, error) {
	if err := validateResponseForQuery(pluginsState.questionMsg, synth); err != nil {
		pluginsState.returnCode = PluginsReturnCodeParseError
		return nil, err
	}
	if err := synth.Pack(); err != nil {
		pluginsState.returnCode = PluginsReturnCodeParseError
		return nil, err
	}
	return synth.Data, nil
}

// processDNSCryptQuery - Processes a query using the DNSCrypt protocol
func processDNSCryptQuery(
	proxy *Proxy,
	serverInfo *ServerInfo,
	pluginsState *PluginsState,
	query []byte,
	serverProto string,
) ([]byte, error) {
	sharedKey, encryptedQuery, clientNonce, queryEpoch, err := proxy.Encrypt(serverInfo, query, serverProto)
	if err != nil && serverProto == "udp" {
		dlog.Debug("Unable to pad for UDP, re-encrypting query for TCP")
		serverProto = "tcp"
		sharedKey, encryptedQuery, clientNonce, queryEpoch, err = proxy.Encrypt(serverInfo, query, serverProto)
	}

	if err != nil {
		pluginsState.returnCode = PluginsReturnCodeParseError
		pluginsState.ApplyLoggingPlugins(&proxy.pluginsGlobals)
		return nil, err
	}

	serverInfo.noticeBegin(proxy)
	var response []byte
	pluginsState.exchange.Transport = TransportDNSCryptUDP

	if serverProto == "udp" {
		var connReused bool
		response, connReused, err = proxy.exchangeWithUDPServer(serverInfo, sharedKey, encryptedQuery, clientNonce, queryEpoch)
		pluginsState.exchange.ConnReused = connReused
		retryOverTCP := false
		if err == nil && len(response) >= MinDNSPacketSize && response[2]&0x02 == 0x02 {
			pluginsState.exchange.UpstreamTruncated = true
			retryOverTCP = true
		} else if neterr, ok := err.(net.Error); ok && neterr.Timeout() {
			dlog.Debugf("[%v] Retry over TCP after UDP timeouts", serverInfo.Name)
			retryOverTCP = true
		}
		if retryOverTCP {
			pluginsState.exchange.TCPFallback = true
			serverProto = "tcp"
			sharedKey, encryptedQuery, clientNonce, queryEpoch, err = proxy.Encrypt(serverInfo, query, serverProto)
			if err != nil {
				pluginsState.returnCode = PluginsReturnCodeParseError
				pluginsState.ApplyLoggingPlugins(&proxy.pluginsGlobals)
				return nil, err
			}
			pluginsState.exchange.Transport = TransportDNSCryptTCP
			pluginsState.exchange.ConnReused = false
			response, err = proxy.exchangeWithTCPServer(serverInfo, sharedKey, encryptedQuery, clientNonce, queryEpoch)
		}
	} else {
		pluginsState.exchange.Transport = TransportDNSCryptTCP
		pluginsState.exchange.ConnReused = false
		response, err = proxy.exchangeWithTCPServer(serverInfo, sharedKey, encryptedQuery, clientNonce, queryEpoch)
	}

	// Check for stale response if there was an error
	if err != nil {
		if stale, ok := pluginsState.sessionData["stale"]; ok {
			dlog.Debug("Serving stale response")
			staleMsg := stale.(*dns.Msg)
			if packErr := staleMsg.Pack(); packErr == nil {
				return staleMsg.Data, nil
			}
		}
		// No stale response available; this is a definitive failure
		if neterr, ok := err.(net.Error); ok && neterr.Timeout() {
			pluginsState.returnCode = PluginsReturnCodeServerTimeout
		} else {
			pluginsState.returnCode = PluginsReturnCodeNetworkError
		}
		pluginsState.ApplyLoggingPlugins(&proxy.pluginsGlobals)
		return nil, err
	}

	return response, nil
}

// processDoHQuery - Processes a query using the DoH protocol
func processDoHQuery(
	proxy *Proxy,
	serverInfo *ServerInfo,
	pluginsState *PluginsState,
	query []byte,
) ([]byte, error) {
	tid := TransactionID(query)
	SetTransactionID(query, 0)
	serverInfo.noticeBegin(proxy)
	serverResponse, _, tls, _, fetchTrace, err := proxy.xTransport.DoHQuery(serverInfo.useGet, serverInfo.URL, query, proxy.timeout)
	SetTransactionID(query, tid)
	applyHTTPFetchTrace(pluginsState, fetchTrace, false)

	// A response was received, and the TLS handshake was complete.
	if err == nil && tls != nil && tls.HandshakeComplete {
		// Restore the original transaction ID
		response := serverResponse
		if len(response) >= MinDNSPacketSize {
			SetTransactionID(response, tid)
		}
		return response, nil
	}

	// Attempt to serve a stale response as a fallback.
	if stale, ok := pluginsState.sessionData["stale"]; ok {
		dlog.Debug("Serving stale response")
		staleMsg := stale.(*dns.Msg)
		if packErr := staleMsg.Pack(); packErr == nil {
			return staleMsg.Data, nil
		}
	}

	// No stale response available; this is a definitive failure
	pluginsState.returnCode = PluginsReturnCodeNetworkError
	pluginsState.ApplyLoggingPlugins(&proxy.pluginsGlobals)
	return nil, err
}

// refreshODoHKey claims the per-server refresh slot, drives the actual
// refresh, and releases the slot under defer so a panic in refreshServer
// cannot leak the in-flight flag. It returns the refresh error so the
// caller can propagate it the way the original 401 handler did.
func refreshODoHKey(proxy *Proxy, serverInfo *ServerInfo, stamp stamps.ServerStamp) error {
	if !proxy.serversInfo.beginODoHRefresh(serverInfo.Name, 10*time.Second) {
		dlog.Debugf("Skipping key update for [%v] (refresh in flight or recently failed)", serverInfo.Name)
		return nil
	}
	success := false
	defer func() { proxy.serversInfo.endODoHRefresh(serverInfo.Name, success) }()
	dlog.Infof("Forcing key update for [%v]", serverInfo.Name)
	if err := proxy.serversInfo.refreshServer(proxy, serverInfo.Name, stamp); err != nil {
		dlog.Noticef("Key update failed for [%v]", serverInfo.Name)
		return err
	}
	success = true
	return nil
}

// processODoHQuery - Processes a query using the ODoH protocol
func processODoHQuery(
	proxy *Proxy,
	serverInfo *ServerInfo,
	pluginsState *PluginsState,
	query []byte,
) ([]byte, error) {
	tid := TransactionID(query)
	if len(serverInfo.odohTargetConfigs) == 0 {
		return nil, nil
	}

	serverInfo.noticeBegin(proxy)

	target := serverInfo.odohTargetConfigs[rand.Intn(len(serverInfo.odohTargetConfigs))]
	odohQuery, err := target.encryptQuery(query)
	if err != nil {
		dlog.Errorf("Failed to encrypt query for [%v]", serverInfo.Name)
		return nil, err
	}

	targetURL := serverInfo.URL
	if serverInfo.Relay != nil && serverInfo.Relay.ODoH != nil {
		targetURL = serverInfo.Relay.ODoH.URL
	}

	responseBody, responseCode, _, _, fetchTrace, err := proxy.xTransport.ObliviousDoHQuery(
		serverInfo.useGet, targetURL, odohQuery.odohMessage, proxy.timeout,
	)
	applyHTTPFetchTrace(pluginsState, fetchTrace, true)

	if err == nil && len(responseBody) > 0 && responseCode == 200 {
		response, err := odohQuery.decryptResponse(responseBody)
		if err != nil {
			dlog.Warnf("Failed to decrypt response from [%v]", serverInfo.Name)
			return nil, err
		}

		// Restore the original transaction ID
		if len(response) >= MinDNSPacketSize {
			SetTransactionID(response, tid)
		}

		return response, nil
	} else if responseCode == 401 || (responseCode == 200 && len(responseBody) == 0) {
		dlog.Warnf(
			"ODoH request for [%v] needs a key refresh via [%v]: HTTP status [%d], response length: %d, transport error: [%v]",
			serverInfo.Name,
			targetURL,
			responseCode,
			len(responseBody),
			err,
		)
		if responseCode == 200 {
			dlog.Warnf("ODoH relay for [%v] is buggy and returns a 200 status code instead of 401 after a key update", serverInfo.Name)
		}

		var stamp stamps.ServerStamp
		matched := false
		proxy.serversInfo.RLock()
		for _, registeredServer := range proxy.serversInfo.registeredServers {
			if registeredServer.name == serverInfo.Name {
				stamp = registeredServer.stamp
				matched = true
				break
			}
		}
		proxy.serversInfo.RUnlock()
		if matched {
			if refreshErr := refreshODoHKey(proxy, serverInfo, stamp); refreshErr != nil {
				err = refreshErr
			}
		}
	} else {
		if err != nil {
			dlog.Warnf(
				"ODoH request for [%v] failed via [%v]: HTTP status [%d], transport error: [%v]",
				serverInfo.Name,
				targetURL,
				responseCode,
				err,
			)
		} else {
			dlog.Warnf(
				"ODoH request for [%v] failed via [%v]: HTTP status [%d], response length: %d",
				serverInfo.Name,
				targetURL,
				responseCode,
				len(responseBody),
			)
		}
	}

	pluginsState.returnCode = PluginsReturnCodeNetworkError
	pluginsState.ApplyLoggingPlugins(&proxy.pluginsGlobals)

	return nil, err
}

// handleDNSExchange - Handles the DNS exchange with a server
func handleDNSExchange(
	proxy *Proxy,
	serverInfo *ServerInfo,
	pluginsState *PluginsState,
	query []byte,
	serverProto string,
) ([]byte, error) {
	var err error
	var response []byte

	// Anchor exchange timing for the unified feedback path.
	pluginsState.exchangeStart = time.Now()

	if serverInfo.Proto == stamps.StampProtoTypeDNSCrypt {
		response, err = processDNSCryptQuery(proxy, serverInfo, pluginsState, query, serverProto)
	} else if serverInfo.Proto == stamps.StampProtoTypeDoH {
		response, err = processDoHQuery(proxy, serverInfo, pluginsState, query)
	} else if serverInfo.Proto == stamps.StampProtoTypeODoHTarget {
		response, err = processODoHQuery(proxy, serverInfo, pluginsState, query)
	} else {
		dlog.Fatal("Unsupported protocol")
	}

	if err != nil {
		return nil, err
	}

	if len(response) < MinDNSPacketSize || len(response) > MaxDNSPacketSize {
		pluginsState.returnCode = PluginsReturnCodeParseError
		pluginsState.ApplyLoggingPlugins(&proxy.pluginsGlobals)
		return nil, err
	}

	return response, nil
}

// processPlugins - Processes plugins for both query and response
func processPlugins(
	proxy *Proxy,
	pluginsState *PluginsState,
	query []byte,
	serverInfo *ServerInfo,
	response []byte,
) ([]byte, error) {
	var err error

	response, err = pluginsState.ApplyResponsePlugins(&proxy.pluginsGlobals, response)
	if err != nil {
		pluginsState.returnCode = PluginsReturnCodeParseError
		pluginsState.ApplyLoggingPlugins(&proxy.pluginsGlobals)
		return response, err
	}

	if pluginsState.action == PluginsActionDrop {
		pluginsState.returnCode = PluginsReturnCodeDrop
		pluginsState.ApplyLoggingPlugins(&proxy.pluginsGlobals)
		return response, nil
	}

	if pluginsState.synthResponse != nil {
		response, err = handleSynthesizedResponse(pluginsState, pluginsState.synthResponse)
		if err != nil {
			pluginsState.ApplyLoggingPlugins(&proxy.pluginsGlobals)
			return response, err
		}
	}

	// RCODE/DNSSEC quality signals are derived by the unified feedback
	// path (ServersInfo.observeOutcome) once processing is complete.
	if rcode := Rcode(response); rcode == dns.RcodeServerFailure { // SERVFAIL
		if pluginsState.dnssec {
			dlog.Debug("A response had an invalid DNSSEC signature")
		} else {
			dlog.Infof("A response with status code 2 was received - this is usually a temporary, remote issue with the configuration of the domain name")
		}
	}

	return response, nil
}

// sendResponse - Sends the response back to the client
func sendResponse(
	proxy *Proxy,
	pluginsState *PluginsState,
	response []byte,
	clientProto string,
	clientAddr *net.Addr,
	clientPc net.Conn,
) {
	if len(response) < MinDNSPacketSize || len(response) > MaxDNSPacketSize {
		if len(response) == 0 {
			pluginsState.returnCode = PluginsReturnCodeNotReady
		} else {
			pluginsState.returnCode = PluginsReturnCodeParseError
		}
		pluginsState.ApplyLoggingPlugins(&proxy.pluginsGlobals)
		return
	}

	var err error
	if clientProto == "udp" {
		if len(response) > pluginsState.maxUnencryptedUDPSafePayloadSize {
			response, err = TruncatedResponse(response)
			if err != nil {
				pluginsState.returnCode = PluginsReturnCodeParseError
				pluginsState.ApplyLoggingPlugins(&proxy.pluginsGlobals)
				return
			}
			pluginsState.exchange.LocalTruncated = true
		}
		clientPc.(net.PacketConn).WriteTo(response, *clientAddr)
		if HasTCFlag(response) {
			proxy.questionSizeEstimator.blindAdjust()
		} else {
			proxy.questionSizeEstimator.adjust(ResponseOverhead + len(response))
		}
	} else if clientProto == "tcp" {
		response, err = PrefixWithSize(response)
		if err != nil {
			pluginsState.returnCode = PluginsReturnCodeParseError
			pluginsState.ApplyLoggingPlugins(&proxy.pluginsGlobals)
			return
		}
		if clientPc != nil {
			clientPc.Write(response)
		}
	}
}

// updateMonitoringMetrics - Updates monitoring metrics if enabled
func updateMonitoringMetrics(
	proxy *Proxy,
	pluginsState *PluginsState,
) {
	if proxy.monitoringUI.Enabled && proxy.monitoringInstance != nil && pluginsState.questionMsg != nil {
		proxy.monitoringInstance.UpdateMetrics(*pluginsState, pluginsState.questionMsg)
	} else {
		if pluginsState.questionMsg == nil {
			dlog.Debugf("Question message is nil")
		}
	}
}
