package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
)

// Metrics owns a node-local registry. Prometheus collectors are concurrency safe;
// no process globals are used, so embedded nodes have independent measurements.
type Metrics struct {
	Registry       *prometheus.Registry
	Ops            *prometheus.CounterVec
	BatchRequests  prometheus.Histogram
	CommitSeconds  prometheus.Histogram
	Sessions       prometheus.Gauge
	Subscriptions  prometheus.Gauge
	SlowConsumers  prometheus.Counter
	ReplicationLag *prometheus.GaugeVec
}

func New() *Metrics {
	m := &Metrics{
		Registry:       prometheus.NewRegistry(),
		Ops:            prometheus.NewCounterVec(prometheus.CounterOpts{Name: "tidepool_ops_total", Help: "Client operations by acknowledged status."}, []string{"status"}),
		BatchRequests:  prometheus.NewHistogram(prometheus.HistogramOpts{Name: "tidepool_commit_batch_requests", Help: "Requests covered by a storage commit.", Buckets: []float64{1, 2, 4, 8, 16, 32, 64, 128, 256}}),
		CommitSeconds:  prometheus.NewHistogram(prometheus.HistogramOpts{Name: "tidepool_commit_seconds", Help: "Storage Commit duration, including failed attempts."}),
		Sessions:       prometheus.NewGauge(prometheus.GaugeOpts{Name: "tidepool_sessions", Help: "Open client WebSocket sessions, including handshakes."}),
		Subscriptions:  prometheus.NewGauge(prometheus.GaugeOpts{Name: "tidepool_subscriptions", Help: "Active shape subscriptions."}),
		SlowConsumers:  prometheus.NewCounter(prometheus.CounterOpts{Name: "tidepool_slow_consumer_disconnects_total", Help: "Client sessions disconnected for full send buffers."}),
		ReplicationLag: prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "tidepool_replication_lag_ops", Help: "Latest observed peer feed position minus durable cursor; a lower bound while disconnected."}, []string{"peer"}),
	}
	m.Registry.MustRegister(m.Ops, m.BatchRequests, m.CommitSeconds, m.Sessions, m.Subscriptions, m.SlowConsumers, m.ReplicationLag)
	for _, status := range []string{"ok", "dup", "retry", "rejected", "invalid"} {
		m.Ops.WithLabelValues(status).Add(0)
	}
	return m
}

func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{})
}
