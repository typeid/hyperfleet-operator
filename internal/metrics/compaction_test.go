package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestNewCompactionMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewCompactionMetrics(reg)

	m.RunsTotal.WithLabelValues("success").Inc()
	m.RunsTotal.WithLabelValues("error").Inc()
	m.DeletedTotal.Add(42)
	m.RunDuration.Observe(0.5)

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}

	want := map[string]bool{
		"pgctl_compaction_run_duration_seconds":  false,
		"pgctl_compaction_tombstones_deleted_total": false,
		"pgctl_compaction_runs_total":            false,
	}
	for _, f := range families {
		if _, ok := want[f.GetName()]; ok {
			want[f.GetName()] = true
		}
	}
	for name, found := range want {
		if !found {
			t.Errorf("metric %q not found in gathered families", name)
		}
	}
}
