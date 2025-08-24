package sqlitekv

import (
	"context"
	"embed"

	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// SqliteExecutor implements the MigrationExecutor interface for SQLite.
type SqliteExecutor struct {
	pool *sqlitex.Pool
}

func (se *SqliteExecutor) Exec(ctx context.Context, sql string) error {
	conn, err := se.pool.Take(ctx)
	if err != nil {
		return err
	}
	defer se.pool.Put(conn)
	return sqlitex.ExecScript(conn, sql)
}

func (se *SqliteExecutor) QueryIntScalar(ctx context.Context, sql string) (v int, err error) {
	conn, err := se.pool.Take(ctx)
	if err != nil {
		return 0, err
	}
	defer se.pool.Put(conn)

	opts := &sqlitex.ExecOptions{
		ResultFunc: func(stmt *sqlite.Stmt) error {
			v = int(stmt.ColumnInt64(0))
			return nil
		},
	}
	err = sqlitex.Execute(conn, sql, opts)
	return v, err
}
