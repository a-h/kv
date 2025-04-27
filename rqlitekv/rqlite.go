package rqlitekv

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/a-h/kv"
	rqlitehttp "github.com/rqlite/rqlite-go-http"
)

func New(client *rqlitehttp.Client) *Rqlite {
	return &Rqlite{
		Client:          client,
		Timeout:         time.Second * 10,
		ReadConsistency: rqlitehttp.ReadConsistencyLevelStrong,
		Now:             time.Now,
	}
}

type Rqlite struct {
	Client          *rqlitehttp.Client
	Timeout         time.Duration
	ReadConsistency rqlitehttp.ReadConsistencyLevel
	Now             func() time.Time
}

func (rq *Rqlite) SetNow(now func() time.Time) {
	if now == nil {
		now = time.Now
	}
	rq.Now = now
}

func (rq *Rqlite) Init(ctx context.Context) (err error) {
	stmts := rqlitehttp.SQLStatements{
		{
			SQL: `create table if not exists kv (key text primary key, version integer not null, value jsonb not null, created text not null) without rowid;`,
		},
		{
			SQL: `create index if not exists kv_key on kv(key);`,
		},
		{
			SQL: `create index if not exists kv_created on kv(created);`,
		},
	}
	opts := &rqlitehttp.ExecuteOptions{
		Transaction: true,
		Wait:        true,
	}
	if _, err = rq.Client.Execute(ctx, stmts, opts); err != nil {
		return fmt.Errorf("init: %w", err)
	}
	return nil
}

func (rq *Rqlite) Get(ctx context.Context, key string, v any) (r kv.Record, ok bool, err error) {
	stmts := rqlitehttp.SQLStatements{
		{
			SQL: `select key, version, json(value) as value, created from kv where key = :key;`,
			NamedParams: map[string]any{
				"key": key,
			},
		},
	}
	outputs, err := rq.Query(ctx, stmts)
	if err != nil {
		return kv.Record{}, false, fmt.Errorf("get: %w", err)
	}
	rows := outputs[0]
	if len(rows) == 0 {
		return kv.Record{}, false, nil
	}
	if len(rows) != 1 {
		return kv.Record{}, false, fmt.Errorf("get: multiple rows found for key %q", key)
	}
	r = rows[0]
	err = json.Unmarshal(r.Value, v)
	return r, true, err
}

func (rq *Rqlite) GetPrefix(ctx context.Context, prefix string, offset, limit int) (rows []kv.Record, err error) {
	stmts := rqlitehttp.SQLStatements{
		{
			SQL: `select key, version, json(value) as value, created from kv where key like :prefix order by key limit :limit offset :offset;`,
			NamedParams: map[string]any{
				"prefix": prefix + "%",
				"limit":  limit,
				"offset": offset,
			},
		},
	}
	outputs, err := rq.Query(ctx, stmts)
	if err != nil {
		return nil, fmt.Errorf("getprefix: %w", err)
	}
	return outputs[0], nil
}

func (rq *Rqlite) GetRange(ctx context.Context, from, to string, offset, limit int) (rows []kv.Record, err error) {
	stmts := rqlitehttp.SQLStatements{
		{
			SQL: `select key, version, json(value) as value, created from kv where key >= :from and key < :to order by key limit :limit offset :offset;`,
			NamedParams: map[string]any{
				"from":   from,
				"to":     to,
				"limit":  limit,
				"offset": offset,
			},
		},
	}
	outputs, err := rq.Query(ctx, stmts)
	if err != nil {
		return nil, fmt.Errorf("getrange: %w", err)
	}
	return outputs[0], nil
}

func (rq *Rqlite) List(ctx context.Context, start, limit int) (rows []kv.Record, err error) {
	stmts := rqlitehttp.SQLStatements{
		{
			SQL: `select key, version, json(value) as value, created from kv order by key limit :limit offset :offset;`,
			NamedParams: map[string]any{
				"limit":  limit,
				"offset": start,
			},
		},
	}
	outputs, err := rq.Query(ctx, stmts)
	if err != nil {
		return nil, fmt.Errorf("list: %w", err)
	}
	return outputs[0], nil
}

func (rq *Rqlite) Put(ctx context.Context, key string, version int64, value any) (err error) {
	stmt, err := rq.createPutMutationStatement(kv.Put(key, version, value))
	if err != nil {
		return fmt.Errorf("put: %w", err)
	}
	rowsAffected, err := rq.Mutate(ctx, rqlitehttp.SQLStatements{stmt})
	if err != nil {
		if errors.Is(err, kv.ErrVersionMismatch) {
			return kv.ErrVersionMismatch
		}
		return fmt.Errorf("put: %w", err)
	}
	if len(rowsAffected) != 1 {
		return fmt.Errorf("put: expected 1 result, got %d", len(rowsAffected))
	}
	if rowsAffected[0] == 0 {
		return kv.ErrVersionMismatch
	}
	return nil
}

func (rq *Rqlite) Delete(ctx context.Context, keys ...string) (rowsAffected int64, err error) {
	stmt, err := rq.createDeleteMutationStatement(kv.Delete(keys...))
	if err != nil {
		return 0, fmt.Errorf("delete: %w", err)
	}
	allRowsAffected, err := rq.Mutate(ctx, rqlitehttp.SQLStatements{stmt})
	if err != nil {
		return 0, fmt.Errorf("delete: %w", err)
	}
	if len(allRowsAffected) != 1 {
		return 0, fmt.Errorf("delete: expected 1 result, got %d", len(allRowsAffected))
	}
	return allRowsAffected[0], nil
}

func (rq *Rqlite) DeletePrefix(ctx context.Context, prefix string, offset, limit int) (rowsAffected int64, err error) {
	stmt, err := rq.createDeletePrefixMutationStatement(kv.DeletePrefix(prefix, offset, limit))
	if err != nil {
		return 0, fmt.Errorf("deleteprefix: %w", err)
	}
	allRowsAffected, err := rq.Mutate(ctx, rqlitehttp.SQLStatements{stmt})
	if err != nil {
		return 0, fmt.Errorf("deleteprefix: %w", err)
	}
	if len(allRowsAffected) != 1 {
		return 0, fmt.Errorf("deleteprefix: expected 1 result, got %d", len(allRowsAffected))
	}
	return allRowsAffected[0], nil
}

func (rq *Rqlite) DeleteRange(ctx context.Context, from, to string, offset, limit int) (rowsAffected int64, err error) {
	stmt, err := rq.createDeleteRangeMutationStatement(kv.DeleteRange(from, to, offset, limit))
	if err != nil {
		return 0, fmt.Errorf("deleterange: %w", err)
	}
	allRowsAffected, err := rq.Mutate(ctx, rqlitehttp.SQLStatements{stmt})
	if err != nil {
		return 0, fmt.Errorf("deleterange: %w", err)
	}
	if len(allRowsAffected) != 1 {
		return 0, fmt.Errorf("deleterange: expected 1 result, got %d", len(allRowsAffected))
	}
	return allRowsAffected[0], nil
}

func (rq *Rqlite) Count(ctx context.Context) (count int64, err error) {
	sql := `select count(*) from kv;`
	return rq.QueryScalarInt64(ctx, sql, nil)
}

func (rq *Rqlite) CountPrefix(ctx context.Context, prefix string) (count int64, err error) {
	sql := `select count(*) from kv where key like :prefix;`
	args := map[string]any{
		"prefix": prefix + "%",
	}
	return rq.QueryScalarInt64(ctx, sql, args)
}

func (rq *Rqlite) CountRange(ctx context.Context, from, to string) (count int64, err error) {
	sql := `select count(*) from kv where key >= :from and key < :to;`
	args := map[string]any{
		"from": from,
		"to":   to,
	}
	return rq.QueryScalarInt64(ctx, sql, args)
}

func (rq *Rqlite) Patch(ctx context.Context, key string, version int64, patch any) (err error) {
	stmt, err := rq.createPatchMutationStatement(kv.Patch(key, version, patch))
	if err != nil {
		return fmt.Errorf("patch: %w", err)
	}
	allRowsAffected, err := rq.Mutate(ctx, rqlitehttp.SQLStatements{stmt})
	if err != nil {
		if errors.Is(err, kv.ErrVersionMismatch) {
			return kv.ErrVersionMismatch
		}
		return fmt.Errorf("patch: %w", err)
	}
	if len(allRowsAffected) != 1 {
		return fmt.Errorf("patch: expected 1 result, got %d", len(allRowsAffected))
	}
	if allRowsAffected[0] == 0 {
		return kv.ErrVersionMismatch
	}
	return nil
}

func (rq *Rqlite) Query(ctx context.Context, stmts rqlitehttp.SQLStatements) (outputs [][]kv.Record, err error) {
	opts := &rqlitehttp.QueryOptions{
		Timeout: rq.Timeout,
		Level:   rq.ReadConsistency,
	}
	qr, err := rq.Client.Query(ctx, stmts, opts)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	outputs = make([][]kv.Record, len(qr.Results))
	for i, result := range qr.Results {
		if result.Error != "" {
			return nil, fmt.Errorf("query: index %d: %s", i, result.Error)
		}
		if err := checkResultColumns(result); err != nil {
			return nil, fmt.Errorf("query: %w", err)
		}
		outputs[i] = make([]kv.Record, len(result.Values))
		for j, values := range result.Values {
			r, err := newRowFromValues(values)
			if err != nil {
				return nil, fmt.Errorf("query: index %d: row %d: %w", i, j, err)
			}
			outputs[i][j] = r
		}
	}
	return outputs, nil
}

func (rq *Rqlite) QueryScalarInt64(ctx context.Context, sql string, params map[string]any) (int64, error) {
	opts := &rqlitehttp.QueryOptions{
		Timeout: rq.Timeout,
		Level:   rq.ReadConsistency,
	}
	q := rqlitehttp.SQLStatement{
		SQL:         sql,
		NamedParams: params,
	}
	qr, err := rq.Client.Query(ctx, rqlitehttp.SQLStatements{q}, opts)
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
	return int64(vt), nil
}

func checkResultColumns(result rqlitehttp.QueryResult) (err error) {
	if len(result.Columns) != 4 {
		return fmt.Errorf("record: expected 4 columns, got %d", len(result.Columns))
	}
	if result.Columns[0] != "key" || result.Columns[1] != "version" || result.Columns[2] != "value" || result.Columns[3] != "created" {
		return fmt.Errorf("record: expected id, key, version, value and created columns not found, got: %#v", result.Columns)
	}
	return nil
}

func newRowFromValues(values []any) (r kv.Record, err error) {
	if len(values) != 4 {
		return r, fmt.Errorf("row: expected 4 columns, got %d", len(values))
	}
	var ok bool
	r.Key, ok = values[0].(string)
	if !ok {
		return r, fmt.Errorf("row: key: expected string, got %T", values[1])
	}
	if r.Version, err = tryGetInt64(values[1]); err != nil {
		return r, fmt.Errorf("row: version: %w", err)
	}
	if values[2] != nil {
		r.Value = []byte(values[2].(string))
	}
	r.Created, err = time.Parse(time.RFC3339Nano, values[3].(string))
	if err != nil {
		return r, fmt.Errorf("row: failed to parse created time: %w", err)
	}
	return r, nil
}

func tryGetInt64(v any) (int64, error) {
	floatValue, ok := v.(float64)
	if !ok {
		return 0, fmt.Errorf("expected float64, got %T", v)
	}
	return int64(floatValue), nil
}

func (rq *Rqlite) Mutate(ctx context.Context, stmts rqlitehttp.SQLStatements) (rowsAffected []int64, err error) {
	opts := &rqlitehttp.ExecuteOptions{
		Transaction: true,
		Wait:        true,
		Timeout:     rq.Timeout,
	}
	qr, err := rq.Client.Execute(ctx, stmts, opts)
	if err != nil {
		return nil, fmt.Errorf("mutate: %w", err)
	}
	rowsAffected = make([]int64, len(stmts))
	errs := make([]error, len(stmts))
	for i, result := range qr.Results {
		if result.Error != "" {
			if result.Error == "NOT NULL constraint failed: kv.version" {
				errs[i] = kv.ErrVersionMismatch
				continue
			}
			errs[i] = fmt.Errorf("mutate: index %d: %s", i, result.Error)
			continue
		}
		rowsAffected[i] = result.RowsAffected
	}
	return rowsAffected, errors.Join(errs...)
}

func (rq *Rqlite) MutateAll(ctx context.Context, mutations ...kv.Mutation) ([]int64, error) {
	stmts := make(rqlitehttp.SQLStatements, len(mutations))
	for i, m := range mutations {
		var s rqlitehttp.SQLStatement
		var err error
		switch m := m.(type) {
		case kv.PutMutation:
			s, err = rq.createPutMutationStatement(m)
		case kv.PatchMutation:
			s, err = rq.createPatchMutationStatement(m)
		case kv.DeleteMutation:
			s, err = rq.createDeleteMutationStatement(m)
		case kv.DeletePrefixMutation:
			s, err = rq.createDeletePrefixMutationStatement(m)
		case kv.DeleteRangeMutation:
			s, err = rq.createDeleteRangeMutationStatement(m)
		default:
			return nil, fmt.Errorf("mutateall: unsupported mutation type %T", m)
		}
		if err != nil {
			return nil, fmt.Errorf("mutateall: error creating statement: %w", err)
		}
		stmts[i] = s
	}
	return rq.Mutate(ctx, stmts)
}

func (rq *Rqlite) createPutMutationStatement(m kv.PutMutation) (s rqlitehttp.SQLStatement, err error) {
	jsonValue, err := json.Marshal(m.Value)
	if err != nil {
		return s, err
	}
	s.SQL = `insert into kv (key, version, value, created)
values (:key, 1, jsonb(:value), :now)
on conflict(key) do update 
set version = case 
      when (:version = -1 or version = :version)
      then excluded.version + 1 
			else null -- Will fail, because version must not be null
    end,
    value = excluded.value;`
	s.NamedParams = map[string]any{
		"key":     m.Key,
		"version": m.Version,
		"value":   string(jsonValue),
		"now":     rq.Now().Format(time.RFC3339Nano),
	}
	return s, nil
}

func (rq *Rqlite) createPatchMutationStatement(m kv.PatchMutation) (s rqlitehttp.SQLStatement, err error) {
	jsonValue, err := json.Marshal(m.Value)
	if err != nil {
		return s, err
	}
	s.SQL = `insert into kv (key, version, value, created)
values (:key, 1, jsonb(:value), :now)
on conflict(key) do update 
set version = case 
      when (:version = -1 or version = :version)
      then excluded.version + 1 
      else null -- Will fail because version must not be null
    end,
    value = jsonb_patch(kv.value, excluded.value);`
	s.NamedParams = map[string]any{
		"key":     m.Key,
		"version": m.Version,
		"value":   string(jsonValue),
		"now":     rq.Now().Format(time.RFC3339Nano),
	}
	return s, nil
}

func (rq *Rqlite) createDeleteMutationStatement(m kv.DeleteMutation) (s rqlitehttp.SQLStatement, err error) {
	keysJSON, err := json.Marshal(m.Keys)
	if err != nil {
		return s, err
	}
	s.SQL = `delete from kv where key in (select value from json_each(:keys));`
	s.NamedParams = map[string]any{
		"keys": string(keysJSON),
	}
	return s, nil
}

// SQLite supports the `limit` and `offset` clauses in `delete` statements, but
// it's a compiler option (SQLITE_ENABLE_UPDATE_DELETE_LIMIT) that is disabled
// by default (although it is enabled in Ubuntu and MacOS builds of sqlite).
//
// CTEs are not supported with a join, so the simplest way to delete a prefix
// is to use a subquery.

func (rq *Rqlite) createDeletePrefixMutationStatement(m kv.DeletePrefixMutation) (s rqlitehttp.SQLStatement, err error) {
	if m.Prefix == "" {
		return s, fmt.Errorf("deleteprefix: prefix cannot be empty, use '*' to delete all records")
	}
	if m.Prefix == "*" {
		m.Prefix = ""
	}
	s.SQL = `delete from kv where key in (select key from kv where key like :prefix order by key limit :limit offset :offset);`
	s.NamedParams = map[string]any{
		"prefix": m.Prefix + "%",
		"limit":  m.Limit,
		"offset": m.Offset,
	}
	return s, nil
}

func (rq *Rqlite) createDeleteRangeMutationStatement(m kv.DeleteRangeMutation) (s rqlitehttp.SQLStatement, err error) {
	s.SQL = `delete from kv where key in (select key from kv where key >= :from and key < :to order by key limit :limit offset :offset);`
	s.NamedParams = map[string]any{
		"from":   m.From,
		"to":     m.To,
		"limit":  m.Limit,
		"offset": m.Offset,
	}
	return s, nil
}
