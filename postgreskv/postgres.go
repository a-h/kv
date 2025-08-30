package postgreskv

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/a-h/kv"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Postgres struct {
	Pool     *pgxpool.Pool
	Now      func() time.Time
	initOnce sync.Once
	initErr  error
}

type SQLStatement struct {
	SQL         string
	NamedParams pgx.NamedArgs
}

func New(pool *pgxpool.Pool) *Postgres {
	return &Postgres{
		Pool: pool,
		Now:  time.Now,
	}
}

func (p *Postgres) SetNow(now func() time.Time) {
	if now == nil {
		now = time.Now
	}
	p.Now = now
}

func (p *Postgres) Init(ctx context.Context) error {
	p.initOnce.Do(func() {
		p.initErr = kv.NewMigrationRunner(&PostgresExecutor{pool: p.Pool}, migrationsFS).Migrate(ctx)
	})
	return p.initErr
}

func (p *Postgres) queryStream(ctx context.Context, sql string, args pgx.NamedArgs) (records []kv.StreamRecord, err error) {
	rows, err := p.Pool.Query(ctx, sql, args)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var r kv.StreamRecord
		if err = rows.Scan(&r.Seq, &r.Action, &r.Record.Key, &r.Record.Version, &r.Record.Value, &r.Record.Type, &r.Record.Created); err != nil {
			return nil, fmt.Errorf("query scan: %w", err)
		}
		records = append(records, r)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("query rows: %w", err)
	}
	return records, nil
}

func (p *Postgres) queryScalarInt(ctx context.Context, sql string, args pgx.NamedArgs) (v int, err error) {
	row := p.Pool.QueryRow(ctx, sql, args)
	if err = row.Scan(&v); err != nil {
		return 0, fmt.Errorf("queryscalarint: %w", err)
	}
	return v, nil
}

func (p *Postgres) Mutate(ctx context.Context, stmts []SQLStatement) ([]int, error) {
	tx, err := p.Pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("mutate: begin: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback(ctx)
		} else {
			err = tx.Commit(ctx)
		}
	}()
	rowsAffected := make([]int, len(stmts))
	for i, stmt := range stmts {
		tag, execErr := tx.Exec(ctx, stmt.SQL, stmt.NamedParams)
		if execErr != nil {
			if strings.Contains(execErr.Error(), "null value in column") {
				return rowsAffected, kv.ErrVersionMismatch
			}
			return rowsAffected, fmt.Errorf("mutate: index %d: %w", i, execErr)
		}
		rowsAffected[i] = int(tag.RowsAffected())
	}
	return rowsAffected, err
}

func (p *Postgres) Stream(ctx context.Context, t kv.Type, seq int, limit int) (records []kv.StreamRecord, err error) {
	if seq < 0 {
		seq = 0
	}
	if limit < 0 {
		limit = 100
	}

	var sql string
	var args pgx.NamedArgs

	if t == kv.TypeAll {
		sql = `select seq, action, key, version, value, type, created from stream where seq >= @seq order by seq limit @limit;`
		args = pgx.NamedArgs{
			"seq":   seq,
			"limit": limit,
		}
	} else {
		sql = `select seq, action, key, version, value, type, created from stream where seq >= @seq and type = @type order by seq limit @limit;`
		args = pgx.NamedArgs{
			"seq":   seq,
			"type":  string(t),
			"limit": limit,
		}
	}

	return p.queryStream(ctx, sql, args)
}

func (p *Postgres) StreamSeq(ctx context.Context) (int, error) {
	return p.queryScalarInt(ctx, `select last_value from stream_seq_seq;`, nil)
}

func (p *Postgres) StreamTrim(ctx context.Context, seq int) error {
	sql := `delete from stream;`
	args := pgx.NamedArgs{}
	if seq > 0 {
		sql = `delete from stream where seq <= @seq;`
		args["seq"] = seq
	}
	_, err := p.Pool.Exec(ctx, sql, args)
	if err != nil {
		return fmt.Errorf("streamtrim: exec: %w", err)
	}
	return nil
}

func (p *Postgres) LockAcquire(ctx context.Context, name string, lockedBy string, forDuration time.Duration) (bool, error) {
	now := p.Now()
	sql := `
insert into locks (name, locked_by, locked_at, lock_until)
values (@name, @locked_by, @now, @lock_until)
on conflict (name) do update
set locked_by = excluded.locked_by,
    locked_at = @now,
    lock_until = @lock_until
where locks.locked_by = excluded.locked_by
   or locks.lock_until < @now;`
	args := pgx.NamedArgs{
		"name":       name,
		"locked_by":  lockedBy,
		"now":        now,
		"lock_until": now.Add(forDuration),
	}
	rowsAffected, err := p.Mutate(ctx, []SQLStatement{{SQL: sql, NamedParams: args}})
	if err != nil {
		return false, fmt.Errorf("lockacquire: mutate: %w", err)
	}
	return rowsAffected[0] > 0, nil
}

func (p *Postgres) LockRelease(ctx context.Context, name string, lockedBy string) error {
	sql := `delete from locks where name = @name and locked_by = @locked_by;`
	args := pgx.NamedArgs{
		"name":      name,
		"locked_by": lockedBy,
	}
	_, err := p.Pool.Exec(ctx, sql, args)
	if err != nil {
		return fmt.Errorf("lockrelease: exec: %w", err)
	}
	return nil
}

func (p *Postgres) LockStatus(ctx context.Context, name string) (status kv.LockStatus, ok bool, err error) {
	row := p.Pool.QueryRow(ctx, `select name, locked_by, locked_at, lock_until from locks where name = @name limit 1;`, pgx.NamedArgs{"name": name})
	var lockedAt, expiresAt time.Time
	err = row.Scan(&status.Name, &status.LockedBy, &lockedAt, &expiresAt)
	if err == pgx.ErrNoRows {
		return status, false, nil
	}
	if err != nil {
		return status, false, err
	}
	status.LockedAt = lockedAt
	status.ExpiresAt = expiresAt
	return status, true, nil
}
