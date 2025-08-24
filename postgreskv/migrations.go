package postgreskv

import (
	"context"
	"embed"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// PostgresExecutor implements the MigrationExecutor interface for PostgreSQL.
type PostgresExecutor struct {
	pool *pgxpool.Pool
}

func (pe *PostgresExecutor) Exec(ctx context.Context, sql string) error {
	_, err := pe.pool.Exec(ctx, sql)
	return err
}

func (pe *PostgresExecutor) QueryIntScalar(ctx context.Context, sql string) (int, error) {
	var version int
	err := pe.pool.QueryRow(ctx, sql).Scan(&version)
	return version, err
}
