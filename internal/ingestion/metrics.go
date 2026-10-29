package ingestion

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type metrics struct {
	requests        *prometheus.CounterVec
	duration        *prometheus.HistogramVec
	alertsAccepted  *prometheus.CounterVec
	alertsRejected  prometheus.Counter
	samplesAccepted prometheus.Counter
	produceErrors   *prometheus.CounterVec
	authFailures    prometheus.Counter

	bundlesStarted        prometheus.Counter
	bundlesCompleted      *prometheus.CounterVec
	bundleBytes           prometheus.Counter
	chunkChecksumFailures prometheus.Counter
	assemblyFailures      *prometheus.CounterVec
	assemblyDuration      prometheus.Histogram
	storeErrors           *prometheus.CounterVec
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
		alertsAccepted: f.NewCounterVec(prometheus.CounterOpts{
			Name: "connectivity_ingest_alerts_accepted_total",
			Help: "Device alerts published to Kafka, by severity.",
		}, []string{"severity"}),
		alertsRejected: f.NewCounter(prometheus.CounterOpts{
			Name: "connectivity_ingest_alerts_rejected_total",
			Help: "Device alerts refused as invalid (400). A spike usually means an agent release sends something the cloud doesn't accept.",
		}),
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
		bundlesStarted: f.NewCounter(prometheus.CounterOpts{
			Name: "connectivity_ingest_bundles_started_total",
			Help: "New log bundle upload sessions.",
		}),
		bundlesCompleted: f.NewCounterVec(prometheus.CounterOpts{
			Name: "connectivity_ingest_bundles_completed_total",
			Help: "Log bundles assembled, verified, stored and published, by reason.",
		}, []string{"reason"}),
		bundleBytes: f.NewCounter(prometheus.CounterOpts{
			Name: "connectivity_ingest_bundle_bytes_received_total",
			Help: "Bundle chunk bytes stored.",
		}),
		chunkChecksumFailures: f.NewCounter(prometheus.CounterOpts{
			Name: "connectivity_ingest_bundle_chunk_checksum_failures_total",
			Help: "Chunks rejected because their bytes didn't match the declared SHA-256.",
		}),
		assemblyFailures: f.NewCounterVec(prometheus.CounterOpts{
			Name: "connectivity_ingest_bundle_assembly_failures_total",
			Help: "Bundle completions that failed, by reason (store, checksum).",
		}, []string{"reason"}),
		assemblyDuration: f.NewHistogram(prometheus.HistogramOpts{
			Name:    "connectivity_ingest_bundle_assembly_duration_seconds",
			Help:    "Time to stream a bundle's chunks into its final object.",
			Buckets: []float64{.01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		}),
		storeErrors: f.NewCounterVec(prometheus.CounterOpts{
			Name: "connectivity_ingest_object_store_errors_total",
			Help: "Failed object store operations, by operation.",
		}, []string{"op"}),
	}
}
