package rqlitekv

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/a-h/kv"
	rqlitehttp "github.com/rqlite/rqlite-go-http"
)

const getBatchMaxKeyCount = 200

func NewStore(client *rqlitehttp.Client) *Store {
	return &Store{
		Rqlite: New(client),
	}
}

type Store struct {
	*Rqlite
}

func (s *Store) Get(ctx context.Context, key string, v any) (r kv.Record, ok bool, err error) {
	stmts := rqlitehttp.SQLStatements{
		{
			SQL: `select key, version, json(value) as value, type, created from kv where key = :key;`,
			NamedParams: map[string]any{
				"key": key,
			},
		},
	}
	outputs, err := s.Query(ctx, stmts)
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

func (s *Store) GetBatch(ctx context.Context, keys ...string) (items map[string]kv.Record, err error) {
	if len(keys) == 0 {
		return map[string]kv.Record{}, nil
	}

	items = make(map[string]kv.Record)

	// Process keys in chunks to avoid hitting database limits.
	for i := 0; i < len(keys); i += getBatchMaxKeyCount {
		end := min(i+getBatchMaxKeyCount, len(keys))
		chunk := keys[i:end]

		// Use JSON array approach with json_each to avoid string concatenation.
		keysJSON, err := json.Marshal(chunk)
		if err != nil {
			return nil, fmt.Errorf("getbatch: failed to marshal keys: %w", err)
		}

		stmts := rqlitehttp.SQLStatements{
			{
				SQL: `select kv.key, kv.version, json(kv.value) as value, kv.type, kv.created 
				      from kv 
				      where kv.key in (select value from json_each(:keys));`,
				NamedParams: map[string]any{
					"keys": string(keysJSON),
				},
			},
		}

		outputs, err := s.Query(ctx, stmts)
		if err != nil {
			return nil, fmt.Errorf("getbatch: %w", err)
		}

		for _, record := range outputs[0] {
			items[record.Key] = record
		}
	}

	return items, nil
}

func (s *Store) GetPrefix(ctx context.Context, prefix string, offset, limit int) (rows []kv.Record, err error) {
	stmts := rqlitehttp.SQLStatements{
		{
			SQL: `select key, version, json(value) as value, type, created from kv where key like :prefix order by key limit :limit offset :offset;`,
			NamedParams: map[string]any{
				"prefix": prefix + "%",
				"limit":  limit,
				"offset": offset,
			},
		},
	}
	outputs, err := s.Query(ctx, stmts)
	if err != nil {
		return nil, fmt.Errorf("getprefix: %w", err)
	}
	return outputs[0], nil
}

func (s *Store) GetRange(ctx context.Context, from, to string, offset, limit int) (rows []kv.Record, err error) {
	stmts := rqlitehttp.SQLStatements{
		{
			SQL: `select key, version, json(value) as value, type, created from kv where key >= :from and key < :to order by key limit :limit offset :offset;`,
			NamedParams: map[string]any{
				"from":   from,
				"to":     to,
				"limit":  limit,
				"offset": offset,
			},
		},
	}
	outputs, err := s.Query(ctx, stmts)
	if err != nil {
		return nil, fmt.Errorf("getrange: %w", err)
	}
	return outputs[0], nil
}

func (s *Store) List(ctx context.Context, offset, limit int) (rows []kv.Record, err error) {
	stmts := rqlitehttp.SQLStatements{
		{
			SQL: `select key, version, json(value) as value, type, created from kv order by key limit :limit offset :offset;`,
			NamedParams: map[string]any{
				"limit":  limit,
				"offset": offset,
			},
		},
	}
	outputs, err := s.Query(ctx, stmts)
	if err != nil {
		return nil, fmt.Errorf("list: %w", err)
	}
	return outputs[0], nil
}

func (s *Store) GetType(ctx context.Context, t kv.Type, offset, limit int) (rows []kv.Record, err error) {
	var stmt rqlitehttp.SQLStatement

	if t == kv.TypeAll {
		stmt = rqlitehttp.SQLStatement{
			SQL: `select key, version, json(value) as value, type, created from kv order by key limit :limit offset :offset;`,
			NamedParams: map[string]any{
				"limit":  limit,
				"offset": offset,
			},
		}
	} else {
		stmt = rqlitehttp.SQLStatement{
			SQL: `select key, version, json(value) as value, type, created from kv where type = :type order by key limit :limit offset :offset;`,
			NamedParams: map[string]any{
				"type":   string(t),
				"limit":  limit,
				"offset": offset,
			},
		}
	}

	outputs, err := s.Query(ctx, rqlitehttp.SQLStatements{stmt})
	if err != nil {
		return nil, fmt.Errorf("gettype: %w", err)
	}
	return outputs[0], nil
}

func (s *Store) Put(ctx context.Context, key string, version int, value any) (err error) {
	stmt, err := s.createPutMutationStatement(kv.Put(key, version, value))
	if err != nil {
		return fmt.Errorf("put: %w", err)
	}
	rowsAffected, err := s.Mutate(ctx, rqlitehttp.SQLStatements{stmt})
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

func (s *Store) Patch(ctx context.Context, key string, version int, patch any) (err error) {
	stmt, err := s.createPatchMutationStatement(kv.Patch(key, version, patch))
	if err != nil {
		return fmt.Errorf("patch: %w", err)
	}
	allRowsAffected, err := s.Mutate(ctx, rqlitehttp.SQLStatements{stmt})
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

func (s *Store) Delete(ctx context.Context, keys ...string) (rowsAffected int, err error) {
	stmt, err := s.createDeleteMutationStatement(kv.Delete(keys...))
	if err != nil {
		return 0, fmt.Errorf("delete: %w", err)
	}
	allRowsAffected, err := s.Mutate(ctx, rqlitehttp.SQLStatements{stmt})
	if err != nil {
		return 0, fmt.Errorf("delete: %w", err)
	}
	if len(allRowsAffected) != 1 {
		return 0, fmt.Errorf("delete: expected 1 result, got %d", len(allRowsAffected))
	}
	return allRowsAffected[0], nil
}

func (s *Store) DeletePrefix(ctx context.Context, prefix string, offset, limit int) (rowsAffected int, err error) {
	stmt, err := s.createDeletePrefixMutationStatement(kv.DeletePrefix(prefix, offset, limit))
	if err != nil {
		return 0, fmt.Errorf("deleteprefix: %w", err)
	}
	allRowsAffected, err := s.Mutate(ctx, rqlitehttp.SQLStatements{stmt})
	if err != nil {
		return 0, fmt.Errorf("deleteprefix: %w", err)
	}
	if len(allRowsAffected) != 1 {
		return 0, fmt.Errorf("deleteprefix: expected 1 result, got %d", len(allRowsAffected))
	}
	return allRowsAffected[0], nil
}

func (s *Store) DeleteRange(ctx context.Context, from, to string, offset, limit int) (rowsAffected int, err error) {
	stmt := s.createDeleteRangeMutationStatement(kv.DeleteRange(from, to, offset, limit))
	allRowsAffected, err := s.Mutate(ctx, rqlitehttp.SQLStatements{stmt})
	if err != nil {
		return 0, fmt.Errorf("deleterange: %w", err)
	}
	if len(allRowsAffected) != 1 {
		return 0, fmt.Errorf("deleterange: expected 1 result, got %d", len(allRowsAffected))
	}
	return allRowsAffected[0], nil
}

func (s *Store) Count(ctx context.Context) (count int, err error) {
	sql := `select count(*) from kv;`
	return s.QueryScalarInt64(ctx, sql, nil)
}

func (s *Store) CountPrefix(ctx context.Context, prefix string) (count int, err error) {
	sql := `select count(*) from kv where key like :prefix;`
	args := map[string]any{
		"prefix": prefix + "%",
	}
	return s.QueryScalarInt64(ctx, sql, args)
}

func (s *Store) CountRange(ctx context.Context, from, to string) (count int, err error) {
	sql := `select count(*) from kv where key >= :from and key < :to;`
	args := map[string]any{
		"from": from,
		"to":   to,
	}
	return s.QueryScalarInt64(ctx, sql, args)
}

func (s *Store) MutateAll(ctx context.Context, mutations ...kv.Mutation) ([]int, error) {
	stmts := make(rqlitehttp.SQLStatements, len(mutations))
	for i, m := range mutations {
		var stmt rqlitehttp.SQLStatement
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
