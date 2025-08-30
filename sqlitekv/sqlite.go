package sqlitekv

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/a-h/kv"
	"zombiezen.com/go/sqlite/sqlitex"
)

func newPool(pool *sqlitex.Pool) *Sqlite {
	return &Sqlite{
		Pool: pool,
		Now:  time.Now,
	}
}

type Sqlite struct {
	Pool *sqlitex.Pool
	Now  func() time.Time
}

func (s *Sqlite) SetNow(now func() time.Time) {
	if now == nil {
		now = time.Now
	}
	s.Now = now
}

var (
	initOnce sync.Once
	initErr  error
)

func (s *Sqlite) Init(ctx context.Context) error {
	initOnce.Do(func() {
		initErr = kv.NewMigrationRunner(&SqliteExecutor{pool: s.Pool}, migrationsFS).Migrate(ctx)
	})
	return initErr
}

type SQLStatement struct {
	SQL         string
	NamedParams map[string]any
}

func (s *Sqlite) Mutate(ctx context.Context, stmts []SQLStatement) (rowsAffected []int, err error) {
	conn, err := s.Pool.Take(ctx)
	if err != nil {
		return nil, err
	}
	defer s.Pool.Put(conn)
	defer sqlitex.Transaction(conn)(&err)

	rowsAffected = make([]int, len(stmts))
	errs := make([]error, len(stmts))

	for i, stmt := range stmts {
		execErr := sqlitex.Execute(conn, stmt.SQL, &sqlitex.ExecOptions{
			Named: stmt.NamedParams,
		})
		if execErr != nil {
			if strings.Contains(execErr.Error(), "constraint failed: NOT NULL constraint failed: kv.version") {
				execErr = kv.ErrVersionMismatch
			}
			errs[i] = fmt.Errorf("mutate: index %d: %w", i, execErr)
			continue
		}
		rowsAffected[i] = conn.Changes()
	}

	joinedErr := errors.Join(errs...)
	if joinedErr != nil {
		err = joinedErr
		rowsAffected = make([]int, len(stmts))
	}
	return rowsAffected, err
}
