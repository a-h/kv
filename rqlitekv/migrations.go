package rqlitekv

import (
	"context"
	"embed"
	"fmt"
	"strings"
	"time"

	rqlitehttp "github.com/rqlite/rqlite-go-http"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// RqliteExecutor implements the MigrationExecutor interface for RQLite.
type RqliteExecutor struct {
	client          *rqlitehttp.Client
	timeout         time.Duration
	readConsistency rqlitehttp.ReadConsistencyLevel
}

func (re *RqliteExecutor) Exec(ctx context.Context, sql string) error {
	stmts := rqlitehttp.SQLStatements{
		{
			SQL: sql,
		},
	}
	opts := &rqlitehttp.ExecuteOptions{
		Transaction: true,
		Wait:        true,
	}
	qr, err := re.client.Execute(ctx, stmts, opts)
	if err != nil {
		return err
	}

	for i, result := range qr.Results {
		if result.Error != "" {
			return fmt.Errorf("sql execution failed: index %d: %s", i, result.Error)
		}
	}

	return nil
}

func (re *RqliteExecutor) QueryIntScalar(ctx context.Context, sql string) (int, error) {
	opts := &rqlitehttp.QueryOptions{
		Timeout: re.timeout,
		Level:   re.readConsistency,
	}
	q := rqlitehttp.SQLStatement{
		SQL: sql,
	}
	qr, err := re.client.Query(ctx, rqlitehttp.SQLStatements{q}, opts)
	if err != nil {
		return 0, err
	}
	if len(qr.Results) != 1 {
		return 0, fmt.Errorf("expected 1 result, got %d", len(qr.Results))
	}
	if qr.Results[0].Error != "" {
		return 0, fmt.Errorf("%s", qr.Results[0].Error)
	}
	if len(qr.Results[0].Values) != 1 {
		return 0, fmt.Errorf("expected 1 row, got %d", len(qr.Results[0].Values))
	}
	if len(qr.Results[0].Values[0]) != 1 {
		return 0, fmt.Errorf("expected 1 column, got %d", len(qr.Results[0].Values[0]))
	}
	vt, ok := qr.Results[0].Values[0][0].(float64)
	if !ok {
		return 0, fmt.Errorf("expected float64, got %T", qr.Results[0].Values[0][0])
	}
	return int(vt), nil
}

func (re *RqliteExecutor) GetVersion(ctx context.Context) (int, error) {
	opts := &rqlitehttp.QueryOptions{
		Timeout: re.timeout,
		Level:   re.readConsistency,
	}
	q := rqlitehttp.SQLStatement{
		SQL: "select max(version) from migration_version",
	}
	qr, err := re.client.Query(ctx, rqlitehttp.SQLStatements{q}, opts)
	if err != nil {
		return 0, err
	}
	if len(qr.Results) != 1 {
		return 0, fmt.Errorf("expected 1 result, got %d", len(qr.Results))
	}
	if qr.Results[0].Error != "" {
		if strings.Contains(qr.Results[0].Error, "no such table") {
			return 0, nil
		}
		return 0, fmt.Errorf("%s", qr.Results[0].Error)
	}
	if len(qr.Results[0].Values) != 1 {
		return 0, fmt.Errorf("expected 1 row, got %d", len(qr.Results[0].Values))
	}
	if len(qr.Results[0].Values[0]) != 1 {
		return 0, fmt.Errorf("expected 1 column, got %d", len(qr.Results[0].Values[0]))
	}
	vt, ok := qr.Results[0].Values[0][0].(float64)
	if !ok {
		return 0, fmt.Errorf("expected float64, got %T", qr.Results[0].Values[0][0])
	}
	return int(vt), nil
}

func (re *RqliteExecutor) SetVersion(ctx context.Context, migrationSQL string, version int) error {
	stmts := rqlitehttp.SQLStatements{
		{
			SQL: migrationSQL,
		},
		{
			SQL: "insert into migration_version (version) values (:version)",
			NamedParams: map[string]any{
				"version": version,
			},
		},
	}
	opts := &rqlitehttp.ExecuteOptions{
		Transaction: true,
		Wait:        true,
	}
	qr, err := re.client.Execute(ctx, stmts, opts)
	if err != nil {
		return err
	}

	for i, result := range qr.Results {
		if result.Error != "" {
			return fmt.Errorf("sql execution failed: index %d: %s", i, result.Error)
		}
	}

	return nil
}

func (re *RqliteExecutor) AcquireMigrationLock(ctx context.Context) error {
	createTableStmt := rqlitehttp.SQLStatement{
		SQL: `create table if not exists migration_lock (
			id integer primary key check (id = 1),
			locked_at text not null
		)`,
	}
	opts := &rqlitehttp.ExecuteOptions{
		Transaction: true,
		Wait:        true,
	}
	qr, err := re.client.Execute(ctx, rqlitehttp.SQLStatements{createTableStmt}, opts)
	if err != nil {
		return err
	}

	for i, result := range qr.Results {
		if result.Error != "" {
			return fmt.Errorf("failed to create migration_lock table: index %d: %s", i, result.Error)
		}
	}

	stmt := rqlitehttp.SQLStatement{
		SQL: "insert or ignore into migration_lock (id, locked_at) values (1, datetime('now'))",
	}
	qr, err = re.client.Execute(ctx, rqlitehttp.SQLStatements{stmt}, opts)
	if err != nil {
		return err
	}

	for i, result := range qr.Results {
		if result.Error != "" {
			return fmt.Errorf("failed to acquire migration lock: index %d: %s", i, result.Error)
		}
	}

	return nil
}

func (re *RqliteExecutor) ReleaseMigrationLock(ctx context.Context) error {
	stmt := rqlitehttp.SQLStatement{
		SQL: "delete from migration_lock where id = 1",
	}
	opts := &rqlitehttp.ExecuteOptions{
		Transaction: true,
		Wait:        true,
	}
	qr, err := re.client.Execute(ctx, rqlitehttp.SQLStatements{stmt}, opts)
	if err != nil {
		return err
	}

	for i, result := range qr.Results {
		if result.Error != "" {
			return fmt.Errorf("failed to release migration lock: index %d: %s", i, result.Error)
		}
	}

	return nil
}
