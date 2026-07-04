package fleetstore

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	PollLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "fleetstore_poll_latency_seconds",
		Help:    "Latency of poll queries against Postgres.",
		Buckets: prometheus.DefBuckets,
	})

	PollRows = promauto.NewCounter(prometheus.CounterOpts{
		Name: "fleetstore_poll_rows_total",
		Help: "Total rows returned by poll queries.",
	})

	CursorLag = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "fleetstore_cursor_lag_seconds",
		Help: "Age of the newest committed row beyond the cursor.",
	})

	WriteLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "fleetstore_write_latency_seconds",
		Help:    "Write latency by verb.",
		Buckets: prometheus.DefBuckets,
	}, []string{"verb"})

	WriteConflicts = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "fleetstore_write_conflicts_total",
		Help: "Write conflicts by verb.",
	}, []string{"verb"})

	StatusNoop = promauto.NewCounter(prometheus.CounterOpts{
		Name: "fleetstore_status_noop_total",
		Help: "Status writes skipped (client-side or trigger-suppressed).",
	})

	AuditCorrections = promauto.NewCounter(prometheus.CounterOpts{
		Name: "fleetstore_audit_corrections_total",
		Help: "Corrections made by the rolling audit. Alert > 0.",
	})

	IsLeader = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "fleetstore_is_leader",
		Help: "Whether this pod is the leader.",
	}, []string{"pod"})

	WatchEventLag = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "fleetstore_watch_event_lag_seconds",
		Help:    "Time from row updated_at to event emit.",
		Buckets: prometheus.DefBuckets,
	})

	ListenReconnects = promauto.NewCounter(prometheus.CounterOpts{
		Name: "fleetstore_listen_reconnects_total",
		Help: "LISTEN connection reconnects.",
	})

	FreshnessFloorHits = promauto.NewCounter(prometheus.CounterOpts{
		Name: "fleetstore_freshness_floor_hits_total",
		Help: "Times a reconcile was deferred by the freshness floor. Alert > 0.",
	})

	GCDeletions = promauto.NewCounter(prometheus.CounterOpts{
		Name: "fleetstore_gc_deletions_total",
		Help: "Resources deleted by ownerRef garbage collection.",
	})
)
