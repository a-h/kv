package sqlitekv

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/a-h/kv"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitex"
)

const getBatchMaxKeyCount = 256

func NewStore(pool *sqlitex.Pool) *Store {
	return &Store{
		Sqlite: newPool(pool),
	}
}

type Store struct {
	*Sqlite
}

func (s *Store) Get(ctx context.Context, key string, v any) (r kv.Record, ok bool, err error) {
	sql := `select key, version, json(value) as value, type, created from kv where key = :key;`
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

func (s *Store) GetBatch(ctx context.Context, keys ...string) (items map[string]kv.Record, err error) {
	if len(keys) == 0 {
		return map[string]kv.Record{}, nil
	}

	items = make(map[string]kv.Record)

	// Process keys in chunks to avoid hitting database limits.
	for i := 0; i < len(keys); i += getBatchMaxKeyCount {
		end := min(i+getBatchMaxKeyCount, len(keys))
		chunk := keys[i:end]

		// Use JSON array approach to pass keys to SQLite.
		keysJSON, err := json.Marshal(chunk)
		if err != nil {
			return nil, fmt.Errorf("getbatch: failed to marshal keys: %w", err)
		}

		sql := `
		with batch_keys(key) as (
			select json_each.value as key 
			from json_each(:keys)
		)
		select kv.key, kv.version, json(kv.value) as value, kv.type, kv.created 
		from kv 
		inner join batch_keys on kv.key = batch_keys.key;`

		args := map[string]any{
			":keys": string(keysJSON),
		}

		records, err := s.Query(ctx, sql, args)
		if err != nil {
			return nil, fmt.Errorf("getbatch: %w", err)
		}

		for _, record := range records {
			items[record.Key] = record
		}
	}

	return items, nil
}

func (s *Store) GetPrefix(ctx context.Context, prefix string, offset, limit int) (records []kv.Record, err error) {
	sql := `select key, version, json(value) as value, type, created from kv where key like :prefix order by key limit :limit offset :offset;`
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

func (s *Store) GetRange(ctx context.Context, from, to string, offset, limit int) (records []kv.Record, err error) {
	sql := `select key, version, json(value) as value, type, created from kv where key >= :from and key < :to order by key limit :limit offset :offset;`
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

func (s *Store) List(ctx context.Context, start, limit int) (records []kv.Record, err error) {
	sql := `select key, version, json(value) as value, type, created from kv order by key limit :limit offset :offset;`
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

func (s *Store) GetType(ctx context.Context, t kv.Type, offset, limit int) (records []kv.Record, err error) {
	var sql string
	var args map[string]any

	if t == kv.TypeAll {
		sql = `select key, version, json(value) as value, type, created from kv order by key limit :limit offset :offset;`
		args = map[string]any{
			":limit":  limit,
			":offset": offset,
		}
	} else {
		sql = `select key, version, json(value) as value, type, created from kv where type = :type order by key limit :limit offset :offset;`
		args = map[string]any{
			":type":   string(t),
			":limit":  limit,
			":offset": offset,
		}
	}

	records, err = s.Query(ctx, sql, args)
	if err != nil {
		return nil, fmt.Errorf("gettype: %w", err)
	}
	return records, nil
}

func (s *Store) Put(ctx context.Context, key string, version int, value any) (err error) {
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

func (s *Store) Delete(ctx context.Context, keys ...string) (rowsAffected int, err error) {
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

func (s *Store) DeletePrefix(ctx context.Context, prefix string, offset, limit int) (rowsAffected int, err error) {
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

func (s *Store) DeleteRange(ctx context.Context, from, to string, offset, limit int) (rowsAffected int, err error) {
	stmt := s.createDeleteRangeMutationStatement(kv.DeleteRangeMutation{From: from, To: to, Offset: offset, Limit: limit})
	allRowsAffected, err := s.Mutate(ctx, []SQLStatement{stmt})
	if err != nil {
		return 0, fmt.Errorf("deleterange: %w", err)
	}
	if len(allRowsAffected) != 1 {
		return 0, fmt.Errorf("deleterange: expected 1 result, got %d", len(allRowsAffected))
	}
	return allRowsAffected[0], nil
}

func (s *Store) Count(ctx context.Context) (n int, err error) {
	sql := `select count(*) from kv;`
	return s.QueryScalarInt64(ctx, sql, nil)
}

func (s *Store) CountPrefix(ctx context.Context, prefix string) (count int, err error) {
	sql := `select count(*) from kv where key like :prefix;`
	args := map[string]any{
		":prefix": prefix + "%",
	}
	return s.QueryScalarInt64(ctx, sql, args)
}

func (s *Store) CountRange(ctx context.Context, from, to string) (count int, err error) {
	sql := `select count(*) from kv where key >= :from and key < :to;`
	args := map[string]any{
		":from": from,
		":to":   to,
	}
	return s.QueryScalarInt64(ctx, sql, args)
}

func (s *Store) Patch(ctx context.Context, key string, version int, patch any) (err error) {
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

func (s *Store) Query(ctx context.Context, sql string, args map[string]any) (rows []kv.Record, err error) {
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
				Version: int(stmt.GetInt64("version")),
				Value:   valueBytes,
				Type:    stmt.GetText("type"),
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

func (s *Store) QueryStream(ctx context.Context, sql string, args map[string]any) (rows []kv.StreamRecord, err error) {
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
			r := kv.StreamRecord{
				Seq:    int(stmt.GetInt64("seq")),
				Action: kv.Action(stmt.GetText("action")),
				Record: kv.Record{
					Key:     stmt.GetText("key"),
					Version: int(stmt.GetInt64("version")),
					Value:   valueBytes,
					Type:    stmt.GetText("type"),
					Created: created,
				},
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

func (s *Store) QueryScalarInt64(ctx context.Context, sql string, params map[string]any) (v int, err error) {
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
			v = int(stmt.ColumnInt64(0))
			return nil
		},
	}
	if err := sqlitex.Execute(conn, sql, opts); err != nil {
		return 0, err
	}
	return v, nil
}

func (s *Store) MutateAll(ctx context.Context, mutations ...kv.Mutation) ([]int, error) {
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
			stmt = s.createDeleteRangeMutationStatement(m)
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

func (s *Store) createPutMutationStatement(m kv.PutMutation) (stmt SQLStatement, err error) {
	jsonValue, err := json.Marshal(m.Value)
	if err != nil {
		return stmt, err
	}
	typeName := kv.TypeOf(m.Value)
	stmt.SQL = `insert into kv (key, version, value, type, created)
values (:key, 1, jsonb(:value), :type, :now)
on conflict(key) do update 
set version = case 
      when (:version = -1 or version = :version)
      then kv.version + 1
			else null -- Will fail, because version must not be null
    end,
    value = excluded.value,
    type = excluded.type;`
	stmt.NamedParams = map[string]any{
		":key":     m.Key,
		":version": m.Version,
		":value":   string(jsonValue),
		":type":    typeName,
		":now":     s.Now().Format(time.RFC3339Nano),
	}
	return stmt, nil
}

func (s *Store) createPatchMutationStatement(m kv.PatchMutation) (stmt SQLStatement, err error) {
	jsonValue, err := json.Marshal(m.Value)
	if err != nil {
		return stmt, err
	}
	typeName := kv.TypeOf(m.Value)
	stmt.SQL = `insert into kv (key, version, value, type, created)
values (:key, 1, jsonb(:value), :type, :now)
on conflict(key) do update 
set version = case 
      when (:version = -1 or version = :version)
      then kv.version + 1
      else null -- Will fail because version must not be null
    end,
    value = jsonb_patch(kv.value, excluded.value),
    type = excluded.type;`
	stmt.NamedParams = map[string]any{
		":key":     m.Key,
		":version": m.Version,
		":value":   string(jsonValue),
		":type":    typeName,
		":now":     s.Now().Format(time.RFC3339Nano),
	}
	return stmt, nil
}

func (s *Store) createDeleteMutationStatement(m kv.DeleteMutation) (stmt SQLStatement, err error) {
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

func (s *Store) createDeletePrefixMutationStatement(m kv.DeletePrefixMutation) (stmt SQLStatement, err error) {
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

func (s *Store) createDeleteRangeMutationStatement(m kv.DeleteRangeMutation) SQLStatement {
	return SQLStatement{
		SQL: `delete from kv where key in (select key from kv where key >= :from and key < :to order by key limit :limit offset :offset);`,
		NamedParams: map[string]any{
			":from":   m.From,
			":to":     m.To,
			":limit":  m.Limit,
			":offset": m.Offset,
		},
	}
}

func (s *Store) Stream(ctx context.Context, t kv.Type, seq int, limit int) (records []kv.StreamRecord, err error) {
	sql := `select seq, action, key, version, json(value) as value, type, created from stream where seq >= :seq order by seq limit :limit;`
	args := map[string]any{
		":seq":   seq,
		":limit": limit,
	}
	if t != kv.TypeAll {
		sql = `select seq, action, key, version, json(value) as value, type, created from stream where seq >= :seq and type = :type order by seq limit :limit;`
		args = map[string]any{
			":seq":   seq,
			":type":  string(t),
			":limit": limit,
		}
	}

	records, err = s.QueryStream(ctx, sql, args)
	if err != nil {
		return nil, fmt.Errorf("stream: %w", err)
	}
	return records, nil
}

var deleteStreamAll = SQLStatement{
	SQL:         `delete from stream;`,
	NamedParams: map[string]any{},
}

func (s *Store) StreamSeq(ctx context.Context) (seq int, err error) {
	sql := `select coalesce(seq, 0) from sqlite_sequence where name = 'stream';`
	return s.QueryScalarInt64(ctx, sql, nil)
}

func (s *Store) StreamTrim(ctx context.Context, seq int) (err error) {
	stmt := SQLStatement{
		SQL: `delete from stream where seq <= :seq;`,
		NamedParams: map[string]any{
			":seq": seq,
		},
	}
	if seq < 0 {
		stmt = deleteStreamAll
	}
	_, err = s.Mutate(ctx, []SQLStatement{stmt})
	if err != nil {
		return fmt.Errorf("streamtrim: %w", err)
	}
	return nil
}

func (s *Store) LockAcquire(ctx context.Context, name string, lockedBy string, forDuration time.Duration) (acquired bool, err error) {
	stmt := SQLStatement{
		SQL: `insert into locks (name, locked_by, locked_at, expires_at)
values (:name, :locked_by, :now, :expires_at)
on conflict(name) do update set
    locked_by  = excluded.locked_by,
    locked_at  = excluded.locked_at,
    expires_at = excluded.expires_at
where locks.expires_at <= :now
   or locks.locked_by = excluded.locked_by;`,
		NamedParams: map[string]any{
			":name":       name,
			":locked_by":  lockedBy,
			":now":        s.Now().Format(time.RFC3339Nano),
			":expires_at": s.Now().Add(forDuration).Format(time.RFC3339Nano),
		},
	}
	rowsAffected, err := s.Mutate(ctx, []SQLStatement{stmt})
	if err != nil {
		return false, fmt.Errorf("lockacquire: %w", err)
	}
	return len(rowsAffected) > 0 && rowsAffected[0] > 0, nil
}

func (s *Store) LockRelease(ctx context.Context, name string, lockedBy string) (err error) {
	stmt := SQLStatement{
		SQL: `delete from locks where name = :name and locked_by = :locked_by;`,
		NamedParams: map[string]any{
			":name":      name,
			":locked_by": lockedBy,
		},
	}
	_, err = s.Mutate(ctx, []SQLStatement{stmt})
	if err != nil {
		return fmt.Errorf("lockrelease: %w", err)
	}
	return nil
}

func (s *Store) LockStatus(ctx context.Context, name string) (status kv.LockStatus, ok bool, err error) {
	conn, err := s.Pool.Take(ctx)
	if err != nil {
		return status, false, err
	}
	defer s.Pool.Put(conn)
	var found bool
	opts := &sqlitex.ExecOptions{
		Named: map[string]any{":name": name},
		ResultFunc: func(stmt *sqlite.Stmt) (err error) {
			found = true
			status.Name = stmt.GetText("name")
			status.LockedBy = stmt.GetText("locked_by")
			status.LockedAt, err = time.Parse(time.RFC3339Nano, stmt.GetText("locked_at"))
			if err != nil {
				return fmt.Errorf("lockstatus: error parsing locked_at time: %w", err)
			}
			status.ExpiresAt, err = time.Parse(time.RFC3339Nano, stmt.GetText("expires_at"))
			if err != nil {
				return fmt.Errorf("lockstatus: error parsing expires_at time: %w", err)
			}
			return nil
		},
	}
	err = sqlitex.Execute(conn, `select name, locked_by, locked_at, expires_at from locks where name = :name limit 1;`, opts)
	if err != nil {
		return status, false, err
	}
	if !found {
		return status, false, nil
	}
	return status, true, nil
}
