package sqlitekv

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/a-h/kv"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

func New(pool *sqlitex.Pool) *Sqlite {
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

func (s *Sqlite) Init(ctx context.Context) (err error) {
	conn, err := s.Pool.Take(ctx)
	if err != nil {
		return err
	}
	defer s.Pool.Put(conn)
	script := `create table if not exists kv (key text primary key, version integer not null, value jsonb not null, created text not null) without rowid;
create index if not exists kv_key on kv(key);
create index if not exists kv_created on kv(created);`
	return sqlitex.ExecScript(conn, script)
}

func (s *Sqlite) Get(ctx context.Context, key string, v any) (r kv.Record, ok bool, err error) {
	sql := `select key, version, json(value) as value, created from kv where key = :key;`
	args := map[string]any{
		":key": key,
	}
	records, err := s.Query(ctx, sql, args)
	if err != nil {
		return kv.Record{}, false, fmt.Errorf("get: %w", err)
	}
	if len(records) == 0 {
		return kv.Record{}, false, nil
	}
	r = records[0]
	err = json.Unmarshal(r.Value, v)
	return r, true, err
}

func (s *Sqlite) GetPrefix(ctx context.Context, prefix string, offset, limit int) (records []kv.Record, err error) {
	sql := `select key, version, json(value) as value, created from kv where key like :prefix order by key limit :limit offset :offset;`
	args := map[string]any{
		":prefix": prefix + "%",
		":limit":  limit,
		":offset": offset,
	}
	records, err = s.Query(ctx, sql, args)
	if err != nil {
		return nil, fmt.Errorf("getprefix: %w", err)
	}
	return records, nil
}

func (s *Sqlite) GetRange(ctx context.Context, from, to string, offset, limit int) (records []kv.Record, err error) {
	sql := `select key, version, json(value) as value, created from kv where key >= :from and key < :to order by key limit :limit offset :offset;`
	args := map[string]any{
		":from":   from,
		":to":     to,
		":limit":  limit,
		":offset": offset,
	}
	records, err = s.Query(ctx, sql, args)
	if err != nil {
		return nil, fmt.Errorf("getrange: %w", err)
	}
	return records, nil
}

func (s *Sqlite) List(ctx context.Context, start, limit int) (records []kv.Record, err error) {
	sql := `select key, version, json(value) as value, created from kv order by key limit :limit offset :offset;`
	args := map[string]any{
		":offset": start,
		":limit":  limit,
	}
	records, err = s.Query(ctx, sql, args)
	if err != nil {
		return nil, fmt.Errorf("list: %w", err)
	}
	return records, nil
}

func (s *Sqlite) Put(ctx context.Context, key string, version int64, value any) (err error) {
	stmt, err := s.createPutMutationStatement(kv.PutMutation{Key: key, Version: version, Value: value})
	if err != nil {
		return fmt.Errorf("put: %w", err)
	}
	allRowsAffected, err := s.Mutate(ctx, []SQLStatement{stmt})
	if err != nil {
		if errors.Is(err, kv.ErrVersionMismatch) {
			return kv.ErrVersionMismatch
		}
		return fmt.Errorf("put: %w", err)
	}
	if len(allRowsAffected) == 0 {
		return nil
	}
	if allRowsAffected[0] == 0 {
		return kv.ErrVersionMismatch
	}
	return nil
}

func (s *Sqlite) Delete(ctx context.Context, keys ...string) (rowsAffected int64, err error) {
	stmt, err := s.createDeleteMutationStatement(kv.DeleteMutation{Keys: keys})
	if err != nil {
		return 0, fmt.Errorf("delete: %w", err)
	}
	allRowsAffected, err := s.Mutate(ctx, []SQLStatement{stmt})
	if err != nil {
		return 0, fmt.Errorf("delete: %w", err)
	}
	if len(allRowsAffected) == 0 {
		return 0, nil
	}
	return allRowsAffected[0], nil
}

// SQLite supports the `limit` and `offset` clauses in `delete` statements, but
// it's a compiler option (SQLITE_ENABLE_UPDATE_DELETE_LIMIT) that is disabled
// by default (although it is enabled in Ubuntu and MacOS builds of sqlite).
//
// CTEs are not supported with a join, so the simplest way to delete a prefix
// is to use a subquery.

func (s *Sqlite) DeletePrefix(ctx context.Context, prefix string, offset, limit int) (rowsAffected int64, err error) {
	stmt, err := s.createDeletePrefixMutationStatement(kv.DeletePrefixMutation{Prefix: prefix, Offset: offset, Limit: limit})
	if err != nil {
		return 0, fmt.Errorf("deleteprefix: %w", err)
	}
	allRowsAffected, err := s.Mutate(ctx, []SQLStatement{stmt})
	if err != nil {
		return 0, fmt.Errorf("deleteprefix: %w", err)
	}
	if len(allRowsAffected) == 0 {
		return 0, nil
	}
	return allRowsAffected[0], nil
}

func (s *Sqlite) DeleteRange(ctx context.Context, from, to string, offset, limit int) (rowsAffected int64, err error) {
	stmt, err := s.createDeleteRangeMutationStatement(kv.DeleteRangeMutation{From: from, To: to, Offset: offset, Limit: limit})
	if err != nil {
		return 0, fmt.Errorf("deleterange: %w", err)
	}
	allRowsAffected, err := s.Mutate(ctx, []SQLStatement{stmt})
	if err != nil {
		return 0, fmt.Errorf("deleterange: %w", err)
	}
	if len(allRowsAffected) != 1 {
		return 0, fmt.Errorf("deleterange: expected 1 result, got %d", len(allRowsAffected))
	}
	return allRowsAffected[0], nil
}

func (s *Sqlite) Count(ctx context.Context) (n int64, err error) {
	sql := `select count(*) from kv;`
	return s.QueryScalarInt64(ctx, sql, nil)
}

func (s *Sqlite) CountPrefix(ctx context.Context, prefix string) (count int64, err error) {
	sql := `select count(*) from kv where key like :prefix;`
	args := map[string]any{
		":prefix": prefix + "%",
	}
	return s.QueryScalarInt64(ctx, sql, args)
}

func (s *Sqlite) CountRange(ctx context.Context, from, to string) (count int64, err error) {
	sql := `select count(*) from kv where key >= :from and key < :to;`
	args := map[string]any{
		":from": from,
		":to":   to,
	}
	return s.QueryScalarInt64(ctx, sql, args)
}

func (s *Sqlite) Patch(ctx context.Context, key string, version int64, patch any) (err error) {
	stmt, err := s.createPatchMutationStatement(kv.PatchMutation{Key: key, Version: version, Value: patch})
	if err != nil {
		return fmt.Errorf("patch: %w", err)
	}
	_, err = s.Mutate(ctx, []SQLStatement{stmt})
	if err != nil {
		if errors.Is(err, kv.ErrVersionMismatch) {
			return kv.ErrVersionMismatch
		}
		return fmt.Errorf("patch: %w: %T", err, err)
	}
	return nil
}

func (s *Sqlite) Query(ctx context.Context, sql string, args map[string]any) (rows []kv.Record, err error) {
	conn, err := s.Pool.Take(ctx)
	if err != nil {
		return nil, err
	}
	defer s.Pool.Put(conn)
	opts := &sqlitex.ExecOptions{
		Named: args,
		ResultFunc: func(stmt *sqlite.Stmt) (err error) {
			valueBytes, err := io.ReadAll(stmt.GetReader("value"))
			if err != nil {
				return fmt.Errorf("query: error reading value: %w", err)
			}
			created, err := time.Parse(time.RFC3339Nano, stmt.GetText("created"))
			if err != nil {
				return fmt.Errorf("query: error parsing created time: %w", err)
			}
			r := kv.Record{
				Key:     stmt.GetText("key"),
				Version: stmt.GetInt64("version"),
				Value:   valueBytes,
				Created: created,
			}
			rows = append(rows, r)
			return nil
		},
	}
	if err = sqlitex.Execute(conn, sql, opts); err != nil {
		return nil, fmt.Errorf("query: error in query: %w", err)
	}
	return rows, nil
}

func (s *Sqlite) QueryScalarInt64(ctx context.Context, sql string, params map[string]any) (v int64, err error) {
	conn, err := s.Pool.Take(ctx)
	if err != nil {
		return 0, err
	}
	defer s.Pool.Put(conn)
	opts := &sqlitex.ExecOptions{
		Named: params,
		ResultFunc: func(stmt *sqlite.Stmt) (err error) {
			if stmt.ColumnType(0) != sqlite.TypeInteger {
				return fmt.Errorf("expected integer, got %s", stmt.ColumnType(0).String())
			}
			v = stmt.ColumnInt64(0)
			return nil
		},
	}
	if err := sqlitex.Execute(conn, sql, opts); err != nil {
		return 0, err
	}
	return v, nil
}

type SQLStatement struct {
	SQL         string
	NamedParams map[string]any
}

func (s *Sqlite) Mutate(ctx context.Context, stmts []SQLStatement) (rowsAffected []int64, err error) {
	conn, err := s.Pool.Take(ctx)
	if err != nil {
		return nil, err
	}
	defer s.Pool.Put(conn)
	defer sqlitex.Transaction(conn)(&err)

	rowsAffected = make([]int64, len(stmts))
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
		rowsAffected[i] = int64(conn.Changes())
	}

	joinedErr := errors.Join(errs...)
	if joinedErr != nil {
		err = joinedErr
		rowsAffected = make([]int64, len(stmts))
	}
	return rowsAffected, err
}

func (s *Sqlite) MutateAll(ctx context.Context, mutations ...kv.Mutation) ([]int64, error) {
	stmts := make([]SQLStatement, len(mutations))
	for i, m := range mutations {
		var stmt SQLStatement
		var err error
		switch m := m.(type) {
		case kv.PutMutation:
			stmt, err = s.createPutMutationStatement(m)
		case kv.PatchMutation:
			stmt, err = s.createPatchMutationStatement(m)
		case kv.DeleteMutation:
			stmt, err = s.createDeleteMutationStatement(m)
		case kv.DeletePrefixMutation:
			stmt, err = s.createDeletePrefixMutationStatement(m)
		case kv.DeleteRangeMutation:
			stmt, err = s.createDeleteRangeMutationStatement(m)
		default:
			return nil, fmt.Errorf("mutateall: unsupported mutation type %T", m)
		}
		if err != nil {
			return nil, fmt.Errorf("mutateall: error creating statement: %w", err)
		}
		stmts[i] = stmt
	}
	return s.Mutate(ctx, stmts)
}

func (s *Sqlite) createPutMutationStatement(m kv.PutMutation) (stmt SQLStatement, err error) {
	jsonValue, err := json.Marshal(m.Value)
	if err != nil {
		return stmt, err
	}
	stmt.SQL = `insert into kv (key, version, value, created)
values (:key, 1, jsonb(:value), :now)
on conflict(key) do update 
set version = case 
      when (:version = -1 or version = :version)
      then excluded.version + 1 
			else null -- Will fail, because version must not be null
    end,
    value = excluded.value;`
	stmt.NamedParams = map[string]any{
		":key":     m.Key,
		":version": m.Version,
		":value":   string(jsonValue),
		":now":     s.Now().Format(time.RFC3339Nano),
	}
	return stmt, nil
}

func (s *Sqlite) createPatchMutationStatement(m kv.PatchMutation) (stmt SQLStatement, err error) {
	jsonValue, err := json.Marshal(m.Value)
	if err != nil {
		return stmt, err
	}
	stmt.SQL = `insert into kv (key, version, value, created)
values (:key, 1, jsonb(:value), :now)
on conflict(key) do update 
set version = case 
      when (:version = -1 or version = :version)
      then excluded.version + 1 
      else null -- Will fail because version must not be null
    end,
    value = jsonb_patch(kv.value, excluded.value);`
	stmt.NamedParams = map[string]any{
		":key":     m.Key,
		":version": m.Version,
		":value":   string(jsonValue),
		":now":     s.Now().Format(time.RFC3339Nano),
	}
	return stmt, nil
}

func (s *Sqlite) createDeleteMutationStatement(m kv.DeleteMutation) (stmt SQLStatement, err error) {
	keysJSON, err := json.Marshal(m.Keys)
	if err != nil {
		return stmt, err
	}
	stmt.SQL = `delete from kv where key in (select value from json_each(:keys));`
	stmt.NamedParams = map[string]any{
		":keys": string(keysJSON),
	}
	return stmt, nil
}

func (s *Sqlite) createDeletePrefixMutationStatement(m kv.DeletePrefixMutation) (stmt SQLStatement, err error) {
	if m.Prefix == "" {
		return stmt, fmt.Errorf("deleteprefix: prefix cannot be empty, use '*' to delete all records")
	}
	if m.Prefix == "*" {
		m.Prefix = ""
	}
	stmt.SQL = `delete from kv where key in (select key from kv where key like :prefix order by key limit :limit offset :offset);`
	stmt.NamedParams = map[string]any{
		":prefix": m.Prefix + "%",
		":limit":  m.Limit,
		":offset": m.Offset,
	}
	return stmt, nil
}

func (s *Sqlite) createDeleteRangeMutationStatement(m kv.DeleteRangeMutation) (stmt SQLStatement, err error) {
	stmt.SQL = `delete from kv where key in (select key from kv where key >= :from and key < :to order by key limit :limit offset :offset);`
	stmt.NamedParams = map[string]any{
		":from":   m.From,
		":to":     m.To,
		":limit":  m.Limit,
		":offset": m.Offset,
	}
	return stmt, nil
}
