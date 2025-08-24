package rqlitekv

import (
	"context"
	"embed"
	"fmt"
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
	_, err := re.client.Execute(ctx, stmts, opts)
	return err
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
