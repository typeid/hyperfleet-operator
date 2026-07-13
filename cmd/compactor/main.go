package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jmelis/postgres-controller-backend/pkg/compaction"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/typeid/hyperfleet-operator/internal/metrics"
)

func main() {
	var interval time.Duration
	var retention time.Duration
	var metricsAddr string

	flag.DurationVar(&interval, "interval", 5*time.Minute, "How often to run compaction.")
	flag.DurationVar(&retention, "retention", 24*time.Hour, "How long tombstones survive before deletion.")
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":9090", "The address the metrics/health endpoint binds to.")
	flag.Parse()

	log := slog.Default()

	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		log.Error("POSTGRES_DSN environment variable is required")
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	reg := prometheus.DefaultRegisterer
	m := metrics.NewCompactionMetrics(reg)

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	srv := &http.Server{Addr: metricsAddr, Handler: mux}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("metrics server failed", "error", err)
			os.Exit(1)
		}
	}()

	log.Info("compactor starting",
		"interval", interval,
		"retention", retention,
		"metricsAddr", metricsAddr,
	)

	cfg := compaction.Config{Retention: retention}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := runCompaction(ctx, dsn, cfg, m, log); err != nil {
				log.Error("compaction failed", "error", err)
			}
		case <-ctx.Done():
			log.Info("shutting down")
			srv.Close()
			return
		}
	}
}

func runCompaction(ctx context.Context, dsn string, cfg compaction.Config, m *metrics.CompactionMetrics, log *slog.Logger) error {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		m.RunsTotal.WithLabelValues("error").Inc()
		return fmt.Errorf("connect: %w", err)
	}
	defer conn.Close(ctx)

	start := time.Now()
	res, err := compaction.Compact(ctx, conn, cfg)
	elapsed := time.Since(start)

	m.RunDuration.Observe(elapsed.Seconds())

	if err != nil {
		m.RunsTotal.WithLabelValues("error").Inc()
		return fmt.Errorf("compact: %w", err)
	}

	m.RunsTotal.WithLabelValues("success").Inc()
	m.DeletedTotal.Add(float64(res.Deleted))

	log.Info("compaction complete", "deleted", res.Deleted, "duration", elapsed)
	return nil
}
