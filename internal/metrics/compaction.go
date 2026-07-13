package metrics

import "github.com/prometheus/client_golang/prometheus"

type CompactionMetrics struct {
	RunDuration  prometheus.Histogram
	DeletedTotal prometheus.Counter
	RunsTotal    *prometheus.CounterVec
}

func NewCompactionMetrics(reg prometheus.Registerer) *CompactionMetrics {
	m := &CompactionMetrics{
		RunDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "pgctl",
			Subsystem: "compaction",
			Name:      "run_duration_seconds",
			Help:      "Duration of a single compaction run.",
			Buckets:   []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0},
		}),
		DeletedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "pgctl",
			Subsystem: "compaction",
			Name:      "tombstones_deleted_total",
			Help:      "Total number of tombstones deleted by compaction.",
		}),
		RunsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "pgctl",
			Subsystem: "compaction",
			Name:      "runs_total",
			Help:      "Total compaction runs by result.",
		}, []string{"result"}),
	}
	reg.MustRegister(m.RunDuration, m.DeletedTotal, m.RunsTotal)
	return m
}
