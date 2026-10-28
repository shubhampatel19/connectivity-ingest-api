package ingestion

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type metrics struct {
	requests        *prometheus.CounterVec
	duration        *prometheus.HistogramVec
	samplesAccepted prometheus.Counter
	produceErrors   *prometheus.CounterVec
	authFailures    prometheus.Counter
}

func newMetrics(reg prometheus.Registerer) *metrics {
	f := promauto.With(reg)
	return &metrics{
		requests: f.NewCounterVec(prometheus.CounterOpts{
			Name: "connectivity_ingest_http_requests_total",
			Help: "HTTP requests handled, by route and status code.",
		}, []string{"route", "code"}),
		duration: f.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "connectivity_ingest_http_request_duration_seconds",
			Help:    "HTTP request latency by route.",
			Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		}, []string{"route"}),
		samplesAccepted: f.NewCounter(prometheus.CounterOpts{
			Name: "connectivity_ingest_samples_accepted_total",
			Help: "Telemetry samples acknowledged to devices.",
		}),
		produceErrors: f.NewCounterVec(prometheus.CounterOpts{
			Name: "connectivity_ingest_produce_errors_total",
			Help: "Failed writes to Kafka, by topic.",
		}, []string{"topic"}),
		authFailures: f.NewCounter(prometheus.CounterOpts{
			Name: "connectivity_ingest_auth_failures_total",
			Help: "Requests rejected for bad device credentials or enrollment key.",
		}),
	}
}
