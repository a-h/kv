package sqlitekv

import (
	"context"
	"embed"
	"strings"

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

func (se *SqliteExecutor) GetVersion(ctx context.Context) (v int, err error) {
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
	err = sqlitex.Execute(conn, "select max(version) from migration_version", opts)
	if err != nil {
		if strings.Contains(err.Error(), "no such table") {
			return 0, nil
		}
		return 0, err
	}
	return v, nil
}

func (se *SqliteExecutor) SetVersion(ctx context.Context, migrationSQL string, version int) error {
	conn, err := se.pool.Take(ctx)
	if err != nil {
		return err
	}
	defer se.pool.Put(conn)

	endTxn, err := sqlitex.ImmediateTransaction(conn)
	if err != nil {
		return err
	}
	defer endTxn(&err)

	if err := sqlitex.ExecScript(conn, migrationSQL); err != nil {
		return err
	}
	return sqlitex.Execute(conn, "insert into migration_version (version) values (?)", &sqlitex.ExecOptions{
		Args: []any{version},
	})
}

func (se *SqliteExecutor) AcquireMigrationLock(ctx context.Context) error {
	conn, err := se.pool.Take(ctx)
	if err != nil {
		return err
	}
	defer se.pool.Put(conn)

	err = sqlitex.Execute(conn, `
		create table if not exists migration_lock (
			id integer primary key check (id = 1),
			locked_at text not null
		)
	`, nil)
	if err != nil {
		return err
	}

	return sqlitex.Execute(conn, `
		insert or ignore into migration_lock (id, locked_at) 
		values (1, datetime('now'))
	`, nil)
}

func (se *SqliteExecutor) ReleaseMigrationLock(ctx context.Context) error {
	conn, err := se.pool.Take(ctx)
	if err != nil {
		return err
	}
	defer se.pool.Put(conn)

	return sqlitex.Execute(conn, "delete from migration_lock where id = 1", nil)
}
