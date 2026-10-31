package main

import (
	"math"
	"sync"
	"time"
)

// histogram uses 1% logarithmic buckets in nanoseconds, bounding memory even
// for millions of deliveries. Quantiles report bucket upper bounds.
type histogram struct {
	buckets [4096]uint64
	count   uint64
}

func (h *histogram) add(d time.Duration) {
	i := 0
	if d > 0 {
		i = int(math.Ceil(math.Log(float64(d)) / math.Log(1.01)))
	}
	i = max(0, min(len(h.buckets)-1, i))
	h.buckets[i]++
	h.count++
}
func (h *histogram) quantile(q float64) float64 {
	if h.count == 0 {
		return 0
	}
	rank := uint64(math.Ceil(q * float64(h.count)))
	var n uint64
	for i, count := range h.buckets {
		n += count
		if n >= rank {
			return math.Pow(1.01, float64(i)) / 1e6
		}
	}
	return 0
}

type latency struct {
	Samples uint64  `json:"samples"`
	P50     float64 `json:"p50_ms"`
	P95     float64 `json:"p95_ms"`
	P99     float64 `json:"p99_ms"`
}

func (h *histogram) summary() latency {
	return latency{h.count, h.quantile(.5), h.quantile(.95), h.quantile(.99)}
}

type report struct {
	Clients      int     `json:"clients"`
	Docs         int     `json:"docs"`
	TargetRate   int     `json:"target_ops_per_second"`
	Seconds      float64 `json:"seconds"`
	DrainSeconds float64 `json:"drain_seconds"`
	Offered      uint64  `json:"offered"`
	Sent         uint64  `json:"sent"`
	Acked        uint64  `json:"acked"`
	Errors       uint64  `json:"errors"`
	OpsPerSecond float64 `json:"ops_per_second"`
	Ack          latency `json:"ack_latency"`
	Fanout       latency `json:"fanout_latency"`
}

// mu guards counters and histograms across all readers and writers.
type statistics struct {
	mu                           sync.Mutex
	sent, acked, errors, offered uint64
	ack, fanout                  histogram
	outstanding                  int
}

func (s *statistics) error() { s.mu.Lock(); s.errors++; s.mu.Unlock() }
func (s *statistics) snapshot(o options, elapsed, drain time.Duration) report {
	s.mu.Lock()
	defer s.mu.Unlock()
	return report{Clients: o.Clients, Docs: o.Docs, TargetRate: o.Rate, Seconds: elapsed.Seconds(), DrainSeconds: drain.Seconds(), Offered: s.offered, Sent: s.sent, Acked: s.acked, Errors: s.errors, OpsPerSecond: float64(s.acked) / (elapsed + drain).Seconds(), Ack: s.ack.summary(), Fanout: s.fanout.summary()}
}
