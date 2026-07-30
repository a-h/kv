package sqlitekv

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/a-h/kv"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

// NewPool creates a SQLite connection pool with production-ready pragma defaults:
// a 5s busy timeout to ride out brief write contention, and a WAL size cap to
// prevent unbounded file growth. If opts.PrepareConn is set, it runs after the
// default pragmas.
func NewPool(uri string, opts sqlitex.PoolOptions) (pool *sqlitex.Pool, err error) {
	userPrepare := opts.PrepareConn
	opts.PrepareConn = func(conn *sqlite.Conn) error {
		for _, p := range []string{
			`pragma busy_timeout = 5000`,
			`pragma journal_size_limit = 67108864`,
		} {
			if err := sqlitex.Execute(conn, p, nil); err != nil {
				return err
			}
		}
		if userPrepare == nil {
			return nil
		}
		return userPrepare(conn)
	}
	return sqlitex.NewPool(uri, opts)
}

func newPool(pool *sqlitex.Pool) *Sqlite {
	return &Sqlite{
		Pool: pool,
		Now:  time.Now,
	}
}

type Sqlite struct {
	Pool     *sqlitex.Pool
	Now      func() time.Time
	initOnce sync.Once
	initErr  error
}

func (s *Sqlite) SetNow(now func() time.Time) {
	if now == nil {
		now = time.Now
	}
	s.Now = now
}

func (s *Sqlite) Init(ctx context.Context) error {
	s.initOnce.Do(func() {
		s.initErr = kv.NewMigrationRunner(&SqliteExecutor{pool: s.Pool}, migrationsFS).Migrate(ctx)
	})
	return s.initErr
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
