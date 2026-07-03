package fleetstore

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// AuditConfig configures the audit loop.
type AuditConfig struct {
	AuditInterval      time.Duration
	TombstoneRetention time.Duration
	CleanupInterval    time.Duration
}

// DefaultAuditConfig returns defaults per §10.
func DefaultAuditConfig() AuditConfig {
	return AuditConfig{
		AuditInterval:      30 * time.Minute,
		TombstoneRetention: 72 * time.Hour,
		CleanupInterval:    1 * time.Hour,
	}
}

// Auditor runs the rolling audit and tombstone cleanup per §10.
type Auditor struct {
	pool    *pgxpool.Pool
	watcher *Watcher
	stores  map[string]*InformerStore
	cfg     AuditConfig
	logger  *slog.Logger
}

// NewAuditor creates a new auditor.
func NewAuditor(pool *pgxpool.Pool, watcher *Watcher, stores map[string]*InformerStore, cfg AuditConfig, logger *slog.Logger) *Auditor {
	return &Auditor{
		pool:    pool,
		watcher: watcher,
		stores:  stores,
		cfg:     cfg,
		logger:  logger,
	}
}

// Run starts the audit and cleanup loops.
func (a *Auditor) Run(ctx context.Context) {
	go a.auditLoop(ctx)
	go a.cleanupLoop(ctx)
}

func (a *Auditor) auditLoop(ctx context.Context) {
	ticker := time.NewTicker(a.cfg.AuditInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := a.runAudit(ctx); err != nil {
				a.logger.Warn("audit error", "error", err)
			}
		}
	}
}

func (a *Auditor) runAudit(ctx context.Context) error {
	rows, err := a.pool.Query(ctx,
		`SELECT kind, namespace, name, uid, generation,
			labels, annotations, owner_refs, finalizers,
			spec, status, created_at, deletion_timestamp,
			seq, aws_account_id, updated_at, deleted_at
		FROM resources ORDER BY seq`)
	if err != nil {
		return fmt.Errorf("audit query: %w", err)
	}
	defer rows.Close()

	var auditMaxSeq int64
	listedKeys := make(map[string]map[string]bool) // kind → set of keys
	for rows.Next() {
		row, err := scanResourceRow(rows)
		if err != nil {
			return fmt.Errorf("scan: %w", err)
		}

		if row.Seq > auditMaxSeq {
			auditMaxSeq = row.Seq
		}

		if _, ok := listedKeys[row.Kind]; !ok {
			listedKeys[row.Kind] = make(map[string]bool)
		}
		key := storeKey(row.Namespace, row.Name)
		listedKeys[row.Kind][key] = true

		store, ok := a.stores[row.Kind]
		if !ok {
			continue
		}
		store.Apply(row, true)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("rows: %w", err)
	}

	// Rule 1: detect store keys absent from table, but only if their seq
	// is within the audit snapshot. Keys with seq > auditMaxSeq were added
	// by the poll loop after the audit query started and must not be evicted.
	var corrections int
	for kind, store := range a.stores {
		keys, ok := listedKeys[kind]
		if !ok {
			keys = make(map[string]bool)
		}
		missing := store.AuditDiff(keys, auditMaxSeq)
		for _, key := range missing {
			store.RemoveKey(key)
			corrections++
		}
	}

	if corrections > 0 {
		AuditCorrections.Add(float64(corrections))
		a.logger.Error("audit found corrections", "count", corrections)
	}

	return nil
}

func (a *Auditor) cleanupLoop(ctx context.Context) {
	ticker := time.NewTicker(a.cfg.CleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := a.cleanupTombstones(ctx); err != nil {
				a.logger.Warn("tombstone cleanup error", "error", err)
			}
		}
	}
}

func (a *Auditor) cleanupTombstones(ctx context.Context) error {
	tag, err := a.pool.Exec(ctx,
		`DELETE FROM resources WHERE deleted_at < now() - $1::interval`,
		fmt.Sprintf("%d hours", int(a.cfg.TombstoneRetention.Hours())),
	)
	if err != nil {
		return fmt.Errorf("tombstone cleanup: %w", err)
	}
	if tag.RowsAffected() > 0 {
		a.logger.Info("tombstones cleaned", "count", tag.RowsAffected())
	}
	return nil
}
