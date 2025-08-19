package postgreskv

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	_ "embed"

	"github.com/a-h/kv"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Postgres struct {
	Pool *pgxpool.Pool
	Now  func() time.Time
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

//go:embed init.sql
var initSQL string

func (p *Postgres) Init(ctx context.Context) (err error) {
	tx, err := p.Pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("init: begin: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback(ctx)
		} else {
			err = tx.Commit(ctx)
		}
	}()

	if _, err = tx.Exec(ctx, initSQL); err != nil {
		return fmt.Errorf("init: exec: %w", err)
	}

	return err
}

func (p *Postgres) Get(ctx context.Context, key string, v any) (r kv.Record, ok bool, err error) {
	rows, err := p.Pool.Query(ctx, `SELECT key, version, value, created FROM kv WHERE key = @key;`, pgx.NamedArgs{"key": key})
	if err != nil {
		return r, false, fmt.Errorf("get: query: %w", err)
	}
	defer rows.Close()
	if rows.Next() {
		if err = rows.Scan(&r.Key, &r.Version, &r.Value, &r.Created); err != nil {
			return r, false, fmt.Errorf("get: scan: %w", err)
		}
		ok = true
	}
	if err = rows.Err(); err != nil {
		return r, false, fmt.Errorf("get: rows error: %w", err)
	}
	if !ok {
		return r, false, nil
	}
	err = json.Unmarshal(r.Value, v)
	return r, ok, err
}

func (p *Postgres) GetPrefix(ctx context.Context, prefix string, offset, limit int) ([]kv.Record, error) {
	if offset < 0 {
		offset = 0
	}
	if limit < 0 {
		limit = math.MaxInt
	}
	sql := `SELECT key, version, value, created FROM kv WHERE key LIKE @prefix ORDER BY key LIMIT @limit OFFSET @offset;`
	args := pgx.NamedArgs{
		"prefix": prefix + "%",
		"limit":  limit,
		"offset": offset,
	}
	return p.query(ctx, sql, args)
}

func (p *Postgres) GetRange(ctx context.Context, from, to string, offset, limit int) ([]kv.Record, error) {
	if offset < 0 {
		offset = 0
	}
	if limit < 0 {
		limit = math.MaxInt
	}
	sql := `SELECT key, version, value, created FROM kv WHERE key >= @from AND key < @to ORDER BY key LIMIT @limit OFFSET @offset;`
	args := pgx.NamedArgs{
		"from":   from,
		"to":     to,
		"limit":  limit,
		"offset": offset,
	}
	return p.query(ctx, sql, args)
}

func (p *Postgres) List(ctx context.Context, offset, limit int) ([]kv.Record, error) {
	if offset < 0 {
		offset = 0
	}
	if limit < 0 {
		limit = math.MaxInt
	}
	sql := `SELECT key, version, value, created FROM kv ORDER BY key LIMIT @limit OFFSET @offset;`
	args := pgx.NamedArgs{
		"limit":  limit,
		"offset": offset,
	}
	return p.query(ctx, sql, args)
}

func (p *Postgres) Put(ctx context.Context, key string, version int, value any) error {
	stmt, err := p.createPutMutationStatement(kv.PutMutation{Key: key, Version: version, Value: value})
	if err != nil {
		return fmt.Errorf("put: create statement: %w", err)
	}
	rowsAffected, err := p.Mutate(ctx, []SQLStatement{stmt})
	if err != nil {
		return fmt.Errorf("put: mutate: %w", err)
	}
	if rowsAffected[0] == 0 {
		return kv.ErrVersionMismatch
	}
	return nil
}

func (p *Postgres) Patch(ctx context.Context, key string, version int, patch any) error {
	stmt, err := p.createPatchMutationStatement(kv.PatchMutation{Key: key, Version: version, Value: patch})
	if err != nil {
		return fmt.Errorf("patch: create statement: %w", err)
	}
	if _, err = p.Mutate(ctx, []SQLStatement{stmt}); err != nil {
		if errors.Is(err, kv.ErrVersionMismatch) {
			return kv.ErrVersionMismatch
		}
		return fmt.Errorf("patch: mutate: %w", err)
	}
	return nil
}

func (p *Postgres) Delete(ctx context.Context, keys ...string) (int, error) {
	stmt, err := p.createDeleteMutationStatement(kv.DeleteMutation{Keys: keys})
	if err != nil {
		return 0, fmt.Errorf("delete: create statement: %w", err)
	}
	rowsAffected, err := p.Mutate(ctx, []SQLStatement{stmt})
	if err != nil {
		return 0, fmt.Errorf("delete: mutate: %w", err)
	}
	return rowsAffected[0], nil
}

func (p *Postgres) DeletePrefix(ctx context.Context, prefix string, offset, limit int) (int, error) {
	stmt, err := p.createDeletePrefixMutationStatement(kv.DeletePrefixMutation{Prefix: prefix, Offset: offset, Limit: limit})
	if err != nil {
		return 0, fmt.Errorf("deleteprefix: create statement: %w", err)
	}
	rowsAffected, err := p.Mutate(ctx, []SQLStatement{stmt})
	if err != nil {
		return 0, fmt.Errorf("deleteprefix: mutate: %w", err)
	}
	return rowsAffected[0], nil
}

func (p *Postgres) DeleteRange(ctx context.Context, from, to string, offset, limit int) (int, error) {
	stmt, err := p.createDeleteRangeMutationStatement(kv.DeleteRangeMutation{From: from, To: to, Offset: offset, Limit: limit})
	if err != nil {
		return 0, fmt.Errorf("deleterange: create statement: %w", err)
	}
	rowsAffected, err := p.Mutate(ctx, []SQLStatement{stmt})
	if err != nil {
		return 0, fmt.Errorf("deleterange: mutate: %w", err)
	}
	return rowsAffected[0], nil
}

func (p *Postgres) Count(ctx context.Context) (int, error) {
	return p.queryScalarInt(ctx, `SELECT count(*) FROM kv;`, nil)
}

func (p *Postgres) CountPrefix(ctx context.Context, prefix string) (int, error) {
	args := pgx.NamedArgs{"prefix": prefix + "%"}
	return p.queryScalarInt(ctx, `SELECT count(*) FROM kv WHERE key LIKE @prefix;`, args)
}

func (p *Postgres) CountRange(ctx context.Context, from, to string) (int, error) {
	args := pgx.NamedArgs{"from": from, "to": to}
	return p.queryScalarInt(ctx, `SELECT count(*) FROM kv WHERE key >= @from AND key < @to;`, args)
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

func (p *Postgres) MutateAll(ctx context.Context, mutations ...kv.Mutation) ([]int, error) {
	stmts := make([]SQLStatement, len(mutations))
	for i, m := range mutations {
		var stmt SQLStatement
		var err error
		switch m := m.(type) {
		case kv.PutMutation:
			stmt, err = p.createPutMutationStatement(m)
		case kv.PatchMutation:
			stmt, err = p.createPatchMutationStatement(m)
		case kv.DeleteMutation:
			stmt, err = p.createDeleteMutationStatement(m)
		case kv.DeletePrefixMutation:
			stmt, err = p.createDeletePrefixMutationStatement(m)
		case kv.DeleteRangeMutation:
			stmt, err = p.createDeleteRangeMutationStatement(m)
		default:
			return nil, fmt.Errorf("mutateall: unsupported mutation type %T", m)
		}
		if err != nil {
			return nil, err
		}
		stmts[i] = stmt
	}
	return p.Mutate(ctx, stmts)
}

func (p *Postgres) query(ctx context.Context, sql string, args pgx.NamedArgs) (records []kv.Record, err error) {
	rows, err := p.Pool.Query(ctx, sql, args)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var r kv.Record
		if err = rows.Scan(&r.Key, &r.Version, &r.Value, &r.Created); err != nil {
			return nil, fmt.Errorf("query scan: %w", err)
		}
		records = append(records, r)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("query rows: %w", err)
	}
	return records, nil
}

func (p *Postgres) queryStream(ctx context.Context, sql string, args pgx.NamedArgs) (records []kv.StreamRecord, err error) {
	rows, err := p.Pool.Query(ctx, sql, args)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var r kv.StreamRecord
		if err = rows.Scan(&r.Seq, &r.Action, &r.Record.Key, &r.Record.Version, &r.Record.Value, &r.Record.Created); err != nil {
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

func (p *Postgres) createPutMutationStatement(m kv.PutMutation) (SQLStatement, error) {
	jsonValue, err := json.Marshal(m.Value)
	if err != nil {
		return SQLStatement{}, err
	}
	return SQLStatement{
		SQL: `INSERT INTO kv (key, version, value, created)
VALUES (@key, 1, @value::jsonb, @now)
ON CONFLICT(key) DO UPDATE
SET version = CASE WHEN (@version = -1 OR kv.version = @version) THEN kv.version + 1 ELSE NULL END,
    value = EXCLUDED.value;`,
		NamedParams: pgx.NamedArgs{
			"key":     m.Key,
			"version": m.Version,
			"value":   string(jsonValue),
			"now":     p.Now(),
		},
	}, nil
}

func (p *Postgres) createPatchMutationStatement(m kv.PatchMutation) (SQLStatement, error) {
	jsonValue, err := json.Marshal(m.Value)
	if err != nil {
		return SQLStatement{}, err
	}
	return SQLStatement{
		SQL: `INSERT INTO kv (key, version, value, created)
VALUES (@key, 1, @value::jsonb, @now)
ON CONFLICT(key) DO UPDATE
SET version = CASE WHEN (@version = -1 OR kv.version = @version) THEN kv.version + 1 ELSE NULL END,
    value = kv.value || EXCLUDED.value;`,
		NamedParams: pgx.NamedArgs{
			"key":     m.Key,
			"version": m.Version,
			"value":   string(jsonValue),
			"now":     p.Now(),
		},
	}, nil
}

func (p *Postgres) createDeleteMutationStatement(m kv.DeleteMutation) (SQLStatement, error) {
	keysJSON, err := json.Marshal(m.Keys)
	if err != nil {
		return SQLStatement{}, err
	}
	return SQLStatement{
		SQL: `DELETE FROM kv WHERE key IN (SELECT * FROM jsonb_array_elements_text(@keys::jsonb));`,
		NamedParams: pgx.NamedArgs{
			"keys": string(keysJSON),
		},
	}, nil
}

func (p *Postgres) createDeletePrefixMutationStatement(m kv.DeletePrefixMutation) (SQLStatement, error) {
	if m.Prefix == "" {
		return SQLStatement{}, fmt.Errorf("deleteprefix: prefix cannot be empty")
	}
	if m.Prefix == "*" {
		m.Prefix = ""
	}
	if m.Offset < 0 {
		m.Offset = 0
	}
	if m.Limit < 0 {
		m.Limit = math.MaxInt
	}
	return SQLStatement{
		SQL: `WITH keys_to_delete AS (
	SELECT key FROM kv
	WHERE key LIKE @prefix
	ORDER BY key
	LIMIT @limit OFFSET @offset
)
DELETE FROM kv
USING keys_to_delete
WHERE kv.key = keys_to_delete.key;`,
		NamedParams: pgx.NamedArgs{
			"prefix": m.Prefix + "%",
			"limit":  m.Limit,
			"offset": m.Offset,
		},
	}, nil
}

func (p *Postgres) createDeleteRangeMutationStatement(m kv.DeleteRangeMutation) (SQLStatement, error) {
	if m.Offset < 0 {
		m.Offset = 0
	}
	if m.Limit < 0 {
		m.Limit = math.MaxInt
	}
	return SQLStatement{
		SQL: `WITH keys_to_delete AS (
				SELECT key FROM kv
				WHERE key >= @from AND key < @to
				ORDER BY key
				LIMIT @limit OFFSET @offset
			)
			DELETE FROM kv
			WHERE key = ANY (SELECT key FROM keys_to_delete);`,
		NamedParams: pgx.NamedArgs{
			"from":   m.From,
			"to":     m.To,
			"limit":  m.Limit,
			"offset": m.Offset,
		},
	}, nil
}

func (p *Postgres) Stream(ctx context.Context, seq int, limit int) (records []kv.StreamRecord, err error) {
	if seq < 0 {
		seq = 0
	}
	if limit < 0 {
		limit = 100
	}
	sql := `SELECT seq, action, key, version, value, created FROM stream WHERE seq >= @seq LIMIT @limit;`
	args := pgx.NamedArgs{
		"seq":   seq,
		"limit": limit,
	}
	return p.queryStream(ctx, sql, args)
}

func (p *Postgres) StreamSeq(ctx context.Context) (int, error) {
	return p.queryScalarInt(ctx, `SELECT last_value FROM stream_seq_seq;`, nil)
}

func (p *Postgres) StreamTrim(ctx context.Context, seq int) error {
	sql := `DELETE FROM stream;`
	args := pgx.NamedArgs{}
	if seq > 0 {
		sql = `DELETE FROM stream WHERE seq <= @seq;`
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
INSERT INTO locks (name, locked_by, locked_at, lock_until)
VALUES (@name, @locked_by, @now, @lock_until)
ON CONFLICT (name) DO UPDATE
SET locked_by = EXCLUDED.locked_by,
    locked_at = @now,
    lock_until = @lock_until
WHERE locks.locked_by = EXCLUDED.locked_by
   OR locks.lock_until < @now;`
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
	sql := `DELETE FROM locks WHERE name = @name AND locked_by = @locked_by;`
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
	row := p.Pool.QueryRow(ctx, `SELECT name, locked_by, locked_at, lock_until FROM locks WHERE name = @name LIMIT 1;`, pgx.NamedArgs{"name": name})
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
