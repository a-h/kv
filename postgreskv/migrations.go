package postgreskv

import (
	"context"
	"embed"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

const migrationLockID = 1001

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

func (pe *PostgresExecutor) GetVersion(ctx context.Context) (int, error) {
	var version int
	err := pe.pool.QueryRow(ctx, "select max(version) from migration_version").Scan(&version)
	if err != nil {
		if strings.Contains(err.Error(), "relation") && strings.Contains(err.Error(), "does not exist") {
			return 0, nil
		}
		return 0, err
	}
	return version, nil
}

func (pe *PostgresExecutor) SetVersion(ctx context.Context, migrationSQL string, version int) error {
	tx, err := pe.pool.Begin(ctx)
	if err != nil {
		return err
	}

	if _, err := tx.Exec(ctx, migrationSQL); err != nil {
		return err
	}

	if _, err := tx.Exec(ctx, "insert into migration_version (version) values ($1)", version); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (pe *PostgresExecutor) AcquireMigrationLock(ctx context.Context) error {
	_, err := pe.pool.Exec(ctx, "select pg_advisory_lock($1)", migrationLockID)
	return err
}

func (pe *PostgresExecutor) ReleaseMigrationLock(ctx context.Context) error {
	_, err := pe.pool.Exec(ctx, "select pg_advisory_unlock($1)", migrationLockID)
	return err
}
