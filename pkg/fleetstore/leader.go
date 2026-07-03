package fleetstore

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// LeaderConfig configures the leader lease.
type LeaderConfig struct {
	TTL       time.Duration
	Heartbeat time.Duration
	Identity  string
}

// DefaultLeaderConfig returns sensible defaults per §9.
func DefaultLeaderConfig() LeaderConfig {
	hostname, _ := os.Hostname()
	return LeaderConfig{
		TTL:       30 * time.Second,
		Heartbeat: 10 * time.Second,
		Identity:  hostname,
	}
}

// LeaderElector manages a single-row Postgres lease per §9.
type LeaderElector struct {
	pool   *pgxpool.Pool
	cfg    LeaderConfig
	logger *slog.Logger
}

// NewLeaderElector creates a new leader elector.
func NewLeaderElector(pool *pgxpool.Pool, cfg LeaderConfig, logger *slog.Logger) *LeaderElector {
	return &LeaderElector{pool: pool, cfg: cfg, logger: logger}
}

// Acquire blocks until this pod becomes the leader or the context is cancelled.
func (le *LeaderElector) Acquire(ctx context.Context) error {
	le.logger.Info("waiting to acquire leader lease", "identity", le.cfg.Identity)
	defer func() { IsLeader.WithLabelValues(le.cfg.Identity).Set(1) }()

	ticker := time.NewTicker(le.cfg.Heartbeat)
	defer ticker.Stop()

	for {
		if ok, err := le.tryAcquire(ctx); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			le.logger.Warn("leader acquire error", "error", err)
		} else if ok {
			le.logger.Info("leader lease acquired", "identity", le.cfg.Identity)
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// Run renews the lease at heartbeat intervals. On lease loss or error past TTL,
// exits the process (§9: blunt exit guarantees no reconcile outlives leadership).
func (le *LeaderElector) Run(ctx context.Context) {
	ticker := time.NewTicker(le.cfg.Heartbeat)
	defer ticker.Stop()

	var lastSuccess time.Time = time.Now()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ok, err := le.tryAcquire(ctx)
			if err != nil {
				le.logger.Error("leader renew error", "error", err)
				if time.Since(lastSuccess) > le.cfg.TTL {
					le.logger.Error("leader lease lost, exiting process")
					os.Exit(1)
				}
				continue
			}
			if !ok {
				le.logger.Error("leader lease lost to another holder, exiting process")
				os.Exit(1)
			}
			lastSuccess = time.Now()
		}
	}
}

func (le *LeaderElector) tryAcquire(ctx context.Context) (bool, error) {
	tag, err := le.pool.Exec(ctx, `
		INSERT INTO leader (holder, expires_at) VALUES ($1, now() + $2::interval)
		ON CONFLICT (singleton) DO UPDATE SET
			holder = EXCLUDED.holder, expires_at = EXCLUDED.expires_at
		WHERE leader.holder = EXCLUDED.holder OR leader.expires_at < now()`,
		le.cfg.Identity,
		fmt.Sprintf("%d seconds", int(le.cfg.TTL.Seconds())),
	)
	if err != nil {
		return false, fmt.Errorf("leader upsert: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}
