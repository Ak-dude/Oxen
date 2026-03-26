// Package metrics registers and exposes Prometheus metrics for OxenDB.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Registered metrics.
var (
	// RequestsTotal counts all HTTP requests by method, path, and status code.
	RequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "oxendb",
			Name:      "http_requests_total",
			Help:      "Total number of HTTP requests handled.",
		},
		[]string{"method", "path", "status"},
	)

	// RequestDuration is a histogram of request latencies.
	RequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "oxendb",
			Name:      "http_request_duration_seconds",
			Help:      "HTTP request latency distribution.",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{"method", "path"},
	)

	// DBOperationsTotal counts database operations by type and result.
	DBOperationsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "oxendb",
			Name:      "db_operations_total",
			Help:      "Total number of storage engine operations.",
		},
		[]string{"op", "result"},
	)

	// DBOperationDuration is a histogram of storage engine call latencies.
	DBOperationDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "oxendb",
			Name:      "db_operation_duration_seconds",
			Help:      "Storage engine operation latency.",
			Buckets:   []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0},
		},
		[]string{"op"},
	)

	// ActiveConnections is a gauge of currently open HTTP connections.
	ActiveConnections = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "oxendb",
			Name:      "active_connections",
			Help:      "Number of currently active HTTP connections.",
		},
	)

	// CompactionRuns counts compaction events.
	CompactionRuns = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "oxendb",
			Name:      "compaction_runs_total",
			Help:      "Total number of compaction operations triggered.",
		},
	)
)

// RecordOp records the result of one storage engine operation.
func RecordOp(op, result string) {
	DBOperationsTotal.WithLabelValues(op, result).Inc()
}
