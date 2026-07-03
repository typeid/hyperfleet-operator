package fleetstore

import (
	"context"
	_ "embed"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/001_schema.sql
var schemaSQL string

func Migrate(ctx context.Context, pool *pgxpool.Pool, logger *slog.Logger) error {
	logger.Info("running FleetStore schema migration")
	if _, err := pool.Exec(ctx, schemaSQL); err != nil {
		return fmt.Errorf("apply schema migration: %w", err)
	}
	logger.Info("FleetStore schema migration complete")
	return nil
}
