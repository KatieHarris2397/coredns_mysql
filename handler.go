package coredns_mysql

import (
	"fmt"
	"time"

	"github.com/coredns/coredns/plugin"
	clog "github.com/coredns/coredns/plugin/pkg/log"
	"github.com/coredns/coredns/request"
	_ "github.com/go-sql-driver/mysql"
	"github.com/miekg/dns"
	"golang.org/x/net/context"
)

type CoreDNSMySql struct {
	Next               plugin.Handler
	Dsn                string
	TablePrefix        string
	MaxLifetime        time.Duration
	MaxOpenConnections int
	MaxIdleConnections int
	Ttl                uint32

	tableName      string
	lastZoneUpdate time.Time
	zoneUpdateTime time.Duration
	zones          []string
}

// ServeDNS implements the plugin.Handler interface.
func (handler *CoreDNSMySql) ServeDNS(ctx context.Context, w dns.ResponseWriter, r *dns.Msg) (int, error) {
	fmt.Printf("[DEBUG] === Starting DNS request handling ===\n")
	clog.Info("=== Starting DNS request handling ===")
	state := request.Request{W: w, Req: r}

	qName := state.Name()
	qType := state.Type()
	fmt.Printf("[DEBUG] DNS Query - Name: %s, Type: %s\n", qName, qType)
	clog.Info("DNS Query - Name: " + qName + ", Type: " + qType)

	if time.Since(handler.lastZoneUpdate) > handler.zoneUpdateTime {
		fmt.Printf("[DEBUG] Updating zones...\n")
		clog.Info("Updating zones...")
		err := handler.loadZones()
		if err != nil {
			fmt.Printf("[ERROR] Failed to load zones: %v\n", err)
			clog.Error(err)
			return handler.errorResponse(state, dns.RcodeServerFailure, err)
		}
	}

	qZone := plugin.Zones(handler.zones).Matches(qName)
	fmt.Printf("[DEBUG] Matched zone: %s\n", qZone)
	clog.Info("Matched zone: " + qZone)
	if qZone == "" {
		fmt.Printf("[DEBUG] No matching zone found, passing to next handler\n")
		clog.Info("No matching zone found, passing to next handler")
		return plugin.NextOrFailure(handler.Name(), handler.Next, ctx, w, r)
	}

	fmt.Printf("[DEBUG] Searching for records in zone: %s\n", qZone)
	clog.Info("Searching for records in zone: " + qZone)
	records, err := handler.findRecord(qZone, qName, qType)
	if err != nil {
		fmt.Printf("[ERROR] Failed to find records: %v\n", err)
		clog.Error(err)
		return handler.errorResponse(state, dns.RcodeServerFailure, err)
	}

	var recordNotFound bool
	if len(records) == 0 {
		recordNotFound = true
		// no record found but we are going to return a SOA
		recs, err := handler.findRecord(qZone, "", "SOA")
		if err != nil {
			return handler.errorResponse(state, dns.RcodeServerFailure, err)
		}
		records = append(records, recs...)
	}

	if qType == "SOA" {
		recsNs, err := handler.findRecord(qZone, qName, "NS")
		if err != nil {
			return handler.errorResponse(state, dns.RcodeServerFailure, err)
		}
		records = append(records, recsNs...)
	}

	if qType == "AXFR" {
		return handler.errorResponse(state, dns.RcodeNotImplemented, nil)
	}

	answers := make([]dns.RR, 0, 10)
	extras := make([]dns.RR, 0, 10)

	for _, record := range records {
		var answer dns.RR
		switch record.RecordType {
		case "A":
			answer, extras, err = record.AsARecord()
		case "AAAA":
			answer, extras, err = record.AsAAAARecord()
		case "CNAME":
			answer, extras, err = record.AsCNAMERecord()
		case "SOA":
			answer, extras, err = record.AsSOARecord()
		case "SRV":
			answer, extras, err = record.AsSRVRecord()
		case "NS":
			answer, extras, err = record.AsNSRecord()
		case "MX":
			answer, extras, err = record.AsMXRecord()
		case "TXT":
			answer, extras, err = record.AsTXTRecord()
		case "CAA":
			answer, extras, err = record.AsCAARecord()
		default:
			return handler.errorResponse(state, dns.RcodeNotImplemented, nil)
		}

		if err != nil {
			return handler.errorResponse(state, dns.RcodeServerFailure, err)
		}
		if answer != nil {
			answers = append(answers, answer)
		}
	}

	m := new(dns.Msg)
	m.SetReply(r)
	m.Authoritative = true
	m.RecursionAvailable = false
	m.Compress = true

	if !recordNotFound {
		m.Answer = append(m.Answer, answers...)
	} else {
		m.Ns = append(m.Ns, answers...)
		m.Rcode = dns.RcodeNameError
	}
	m.Extra = append(m.Extra, extras...)

	state.SizeAndDo(m)
	m = state.Scrub(m)
	_ = w.WriteMsg(m)
	return dns.RcodeSuccess, nil
}

// Name implements the Handler interface.
func (handler *CoreDNSMySql) Name() string { return "handler" }

func (handler *CoreDNSMySql) errorResponse(state request.Request, rCode int, err error) (int, error) {
	m := new(dns.Msg)
	m.SetRcode(state.Req, rCode)
	m.Authoritative, m.RecursionAvailable, m.Compress = true, false, true

	state.SizeAndDo(m)
	_ = state.W.WriteMsg(m)
	// Return success as the rCode to signal we have written to the client.
	return dns.RcodeSuccess, err
}
