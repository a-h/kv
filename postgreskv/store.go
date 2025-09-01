package postgreskv

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/a-h/kv"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const getBatchMaxKeyCount = 1000

func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{
		Postgres: New(pool),
	}
}

type Store struct {
	*Postgres
}

func (s *Store) Get(ctx context.Context, key string, v any) (r kv.Record, ok bool, err error) {
	rows, err := s.Pool.Query(ctx, `select key, version, value, type, created from kv where key = @key;`, pgx.NamedArgs{"key": key})
	if err != nil {
		return r, false, fmt.Errorf("get: query: %w", err)
	}
	defer rows.Close()
	if rows.Next() {
		if err = rows.Scan(&r.Key, &r.Version, &r.Value, &r.Type, &r.Created); err != nil {
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

func (s *Store) GetBatch(ctx context.Context, keys ...string) (items map[string]kv.Record, err error) {
	if len(keys) == 0 {
		return map[string]kv.Record{}, nil
	}

	items = make(map[string]kv.Record)

	// Process keys in chunks to avoid hitting database limits.
	for i := 0; i < len(keys); i += getBatchMaxKeyCount {
		end := min(i+getBatchMaxKeyCount, len(keys))
		chunk := keys[i:end]

		// Use array parameter with ANY to match keys efficiently.
		sql := `select key, version, value, type, created from kv where key = any(@keys);`
		args := pgx.NamedArgs{"keys": chunk}

		records, err := s.query(ctx, sql, args)
		if err != nil {
			return nil, fmt.Errorf("getbatch: %w", err)
		}

		for _, record := range records {
			items[record.Key] = record
		}
	}

	return items, nil
}

func (s *Store) GetPrefix(ctx context.Context, prefix string, offset, limit int) ([]kv.Record, error) {
	if offset < 0 {
		offset = 0
	}
	if limit < 0 {
		limit = math.MaxInt
	}
	sql := `select key, version, value, type, created from kv where key like @prefix order by key limit @limit offset @offset;`
	args := pgx.NamedArgs{
		"prefix": prefix + "%",
		"limit":  limit,
		"offset": offset,
	}
	return s.query(ctx, sql, args)
}

func (s *Store) GetRange(ctx context.Context, from, to string, offset, limit int) ([]kv.Record, error) {
	if offset < 0 {
		offset = 0
	}
	if limit < 0 {
		limit = math.MaxInt
	}
	sql := `select key, version, value, type, created from kv where key >= @from and key < @to order by key limit @limit offset @offset;`
	args := pgx.NamedArgs{
		"from":   from,
		"to":     to,
		"limit":  limit,
		"offset": offset,
	}
	return s.query(ctx, sql, args)
}

func (s *Store) List(ctx context.Context, offset, limit int) ([]kv.Record, error) {
	if offset < 0 {
		offset = 0
	}
	if limit < 0 {
		limit = math.MaxInt
	}
	sql := `select key, version, value, type, created from kv order by key limit @limit offset @offset;`
	args := pgx.NamedArgs{
		"limit":  limit,
		"offset": offset,
	}
	return s.query(ctx, sql, args)
}

func (s *Store) GetType(ctx context.Context, t kv.Type, offset, limit int) ([]kv.Record, error) {
	if offset < 0 {
		offset = 0
	}
	if limit < 0 {
		limit = math.MaxInt
	}

	sql := `select key, version, value, type, created from kv order by key limit @limit offset @offset;`
	args := pgx.NamedArgs{
		"limit":  limit,
		"offset": offset,
	}
	if t != kv.TypeAll {
		sql = `select key, version, value, type, created from kv where type = @type order by key limit @limit offset @offset;`
		args["type"] = string(t)
	}

	return s.query(ctx, sql, args)
}

func (s *Store) Put(ctx context.Context, key string, version int, value any) error {
	stmt, err := s.createPutMutationStatement(kv.PutMutation{Key: key, Version: version, Value: value})
	if err != nil {
		return fmt.Errorf("put: create statement: %w", err)
	}
	rowsAffected, err := s.Mutate(ctx, []SQLStatement{stmt})
	if err != nil {
		return fmt.Errorf("put: mutate: %w", err)
	}
	if rowsAffected[0] == 0 {
		return kv.ErrVersionMismatch
	}
	return nil
}

func (s *Store) Patch(ctx context.Context, key string, version int, patch any) error {
	stmt, err := s.createPatchMutationStatement(kv.PatchMutation{Key: key, Version: version, Value: patch})
	if err != nil {
		return fmt.Errorf("patch: create statement: %w", err)
	}
	if _, err = s.Mutate(ctx, []SQLStatement{stmt}); err != nil {
		if errors.Is(err, kv.ErrVersionMismatch) {
			return kv.ErrVersionMismatch
		}
		return fmt.Errorf("patch: mutate: %w", err)
	}
	return nil
}

func (s *Store) Delete(ctx context.Context, keys ...string) (int, error) {
	stmt, err := s.createDeleteMutationStatement(kv.DeleteMutation{Keys: keys})
	if err != nil {
		return 0, fmt.Errorf("delete: create statement: %w", err)
	}
	rowsAffected, err := s.Mutate(ctx, []SQLStatement{stmt})
	if err != nil {
		return 0, fmt.Errorf("delete: mutate: %w", err)
	}
	return rowsAffected[0], nil
}

func (s *Store) DeletePrefix(ctx context.Context, prefix string, offset, limit int) (int, error) {
	stmt, err := s.createDeletePrefixMutationStatement(kv.DeletePrefixMutation{Prefix: prefix, Offset: offset, Limit: limit})
	if err != nil {
		return 0, fmt.Errorf("deleteprefix: create statement: %w", err)
	}
	rowsAffected, err := s.Mutate(ctx, []SQLStatement{stmt})
	if err != nil {
		return 0, fmt.Errorf("deleteprefix: mutate: %w", err)
	}
	return rowsAffected[0], nil
}

func (s *Store) DeleteRange(ctx context.Context, from, to string, offset, limit int) (int, error) {
	stmt := s.createDeleteRangeMutationStatement(kv.DeleteRangeMutation{From: from, To: to, Offset: offset, Limit: limit})
	rowsAffected, err := s.Mutate(ctx, []SQLStatement{stmt})
	if err != nil {
		return 0, fmt.Errorf("deleterange: mutate: %w", err)
	}
	return rowsAffected[0], nil
}

func (s *Store) Count(ctx context.Context) (int, error) {
	return s.queryScalarInt(ctx, `select count(*) from kv;`, nil)
}

func (s *Store) CountPrefix(ctx context.Context, prefix string) (int, error) {
	args := pgx.NamedArgs{"prefix": prefix + "%"}
	return s.queryScalarInt(ctx, `select count(*) from kv where key like @prefix;`, args)
}

func (s *Store) CountRange(ctx context.Context, from, to string) (int, error) {
	args := pgx.NamedArgs{"from": from, "to": to}
	return s.queryScalarInt(ctx, `select count(*) from kv where key >= @from and key < @to;`, args)
}

func (s *Store) Mutate(ctx context.Context, stmts []SQLStatement) ([]int, error) {
	tx, err := s.Pool.Begin(ctx)
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
			return nil, err
		}
		stmts[i] = stmt
	}
	return s.Mutate(ctx, stmts)
}

func (s *Store) query(ctx context.Context, sql string, args pgx.NamedArgs) (records []kv.Record, err error) {
	rows, err := s.Pool.Query(ctx, sql, args)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var r kv.Record
		if err = rows.Scan(&r.Key, &r.Version, &r.Value, &r.Type, &r.Created); err != nil {
			return nil, fmt.Errorf("query scan: %w", err)
		}
		records = append(records, r)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("query rows: %w", err)
	}
	return records, nil
}

func (s *Store) queryScalarInt(ctx context.Context, sql string, args pgx.NamedArgs) (v int, err error) {
	row := s.Pool.QueryRow(ctx, sql, args)
	if err = row.Scan(&v); err != nil {
		return 0, fmt.Errorf("queryscalarint: %w", err)
	}
	return v, nil
}

func (s *Store) createPutMutationStatement(m kv.PutMutation) (SQLStatement, error) {
	jsonValue, err := json.Marshal(m.Value)
	if err != nil {
		return SQLStatement{}, err
	}
	typeName := kv.TypeOf(m.Value)
	return SQLStatement{
		SQL: `insert into kv (key, version, value, type, created)
values (@key, 1, @value::jsonb, @type, @now)
on conflict(key) do update
set version = case when (@version = -1 or kv.version = @version) then kv.version + 1 else null end,
    value = excluded.value,
    type = excluded.type;`,
		NamedParams: pgx.NamedArgs{
			"key":     m.Key,
			"version": m.Version,
			"value":   string(jsonValue),
			"type":    typeName,
			"now":     s.Now(),
		},
	}, nil
}

func (s *Store) createPatchMutationStatement(m kv.PatchMutation) (SQLStatement, error) {
	jsonValue, err := json.Marshal(m.Value)
	if err != nil {
		return SQLStatement{}, err
	}
	typeName := kv.TypeOf(m.Value)
	return SQLStatement{
		SQL: `insert into kv (key, version, value, type, created)
values (@key, 1, @value::jsonb, @type, @now)
on conflict(key) do update
set version = case when (@version = -1 or kv.version = @version) then kv.version + 1 else null end,
    value = kv.value || excluded.value,
    type = excluded.type;`,
		NamedParams: pgx.NamedArgs{
			"key":     m.Key,
			"version": m.Version,
			"value":   string(jsonValue),
			"type":    typeName,
			"now":     s.Now(),
		},
	}, nil
}

func (s *Store) createDeleteMutationStatement(m kv.DeleteMutation) (SQLStatement, error) {
	keysJSON, err := json.Marshal(m.Keys)
	if err != nil {
		return SQLStatement{}, err
	}
	return SQLStatement{
		SQL: `delete from kv where key in (select * from jsonb_array_elements_text(@keys::jsonb));`,
		NamedParams: pgx.NamedArgs{
			"keys": string(keysJSON),
		},
	}, nil
}

func (s *Store) createDeletePrefixMutationStatement(m kv.DeletePrefixMutation) (SQLStatement, error) {
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
		SQL: `with keys_to_delete as (
	select key from kv
	where key like @prefix
	order by key
	limit @limit offset @offset
)
delete from kv
using keys_to_delete
where kv.key = keys_to_delete.key;`,
		NamedParams: pgx.NamedArgs{
			"prefix": m.Prefix + "%",
			"limit":  m.Limit,
			"offset": m.Offset,
		},
	}, nil
}

func (s *Store) createDeleteRangeMutationStatement(m kv.DeleteRangeMutation) SQLStatement {
	if m.Offset < 0 {
		m.Offset = 0
	}
	if m.Limit < 0 {
		m.Limit = math.MaxInt
	}
	return SQLStatement{
		SQL: `with keys_to_delete as (
				select key from kv
				where key >= @from and key < @to
				order by key
				limit @limit offset @offset
			)
			delete from kv
			where key = any (select key from keys_to_delete);`,
		NamedParams: pgx.NamedArgs{
			"from":   m.From,
			"to":     m.To,
			"limit":  m.Limit,
			"offset": m.Offset,
		},
	}
}
