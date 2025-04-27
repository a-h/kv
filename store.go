package kv

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

// Record is the record stored in the store prior to being unmarshaled.
type Record struct {
	Key     string    `json:"key"`
	Version int64     `json:"version"`
	Value   []byte    `json:"value"`
	Created time.Time `json:"created"`
}

var ErrVersionMismatch = errors.New("version mismatch")

// ValuesOf returns the values of the records, unmarshaled into the given type.
func ValuesOf[T any](records []Record) (values []T, err error) {
	values = make([]T, len(records))
	for i, r := range records {
		err = json.Unmarshal(r.Value, &values[i])
		if err != nil {
			return nil, err
		}
	}
	return values, nil
}

type RecordOf[T any] struct {
	Key     string    `json:"key"`
	Version int64     `json:"version"`
	Value   T         `json:"value"`
	Created time.Time `json:"created"`
}

// RecordsOf returns the records, with the value unmarshaled into a type.
// Use map[string]any if you don't know the type.
func RecordsOf[T any](records []Record) (values []RecordOf[T], err error) {
	values = make([]RecordOf[T], len(records))
	for i, r := range records {
		err = json.Unmarshal(r.Value, &values[i].Value)
		if err != nil {
			return nil, err
		}
		values[i].Key = r.Key
		values[i].Version = r.Version
		values[i].Created = r.Created
	}
	return values, nil
}

type Store interface {
	// Init initializes the store. It should be called before any other method, and creates the necessary table.
	Init(ctx context.Context) error
	// Get gets a key from the store, and populates v with the value. If the key does not exist, it returns ok=false.
	Get(ctx context.Context, key string, v any) (r Record, ok bool, err error)
	// GetPrefix gets all keys with a given prefix from the store.
	GetPrefix(ctx context.Context, prefix string, offset, limit int) (rows []Record, err error)
	// GetRange gets all keys between the key from (inclusive) and to (exclusive).
	// e.g. select key from kv where key >= 'a' and key < 'c';
	GetRange(ctx context.Context, from, to string, offset, limit int) (rows []Record, err error)
	// List gets all keys from the store, starting from the given offset and limiting the number of results to the given limit.
	List(ctx context.Context, start, limit int) (rows []Record, err error)
	// Put a key into the store. If the key already exists, it will update the value if the version matches, and increment the version.
	//
	// If the key does not exist, it will insert the key with version 1.
	//
	// If the key exists but the version does not match, it will return an error.
	//
	// If the version is -1, it will skip the version check.
	//
	// If the version is 0, it will only insert the key if it does not already exist.
	Put(ctx context.Context, key string, version int64, value any) (err error)
	// Delete deletes keys from the store. If the key does not exist, no error is returned.
	Delete(ctx context.Context, keys ...string) (rowsAffected int64, err error)
	// DeletePrefix deletes all keys with a given prefix from the store.
	DeletePrefix(ctx context.Context, prefix string, offset, limit int) (rowsAffected int64, err error)
	// DeleteRange deletes all keys between the key from (inclusive) and to (exclusive).
	DeleteRange(ctx context.Context, from, to string, offset, limit int) (rowsAffected int64, err error)
	// Count returns the number of keys in the store.
	Count(ctx context.Context) (n int64, err error)
	// CountPrefix returns the number of keys in the store with a given prefix.
	CountPrefix(ctx context.Context, prefix string) (count int64, err error)
	// CountRange returns the number of keys in the store between the key from (inclusive) and to (exclusive).
	CountRange(ctx context.Context, from, to string) (count int64, err error)
	// Patch patches a key in the store. The patch is a JSON merge patch (RFC 7396), so would look something like map[string]any{"key": "value"}.
	Patch(ctx context.Context, key string, version int64, patch any) (err error)
	// MutateAll runs the mutations against the store, as a single transaction.
	//
	// Use the Put, Patch, PutPatches, Delete, DeleteKeys, DeletePrefix and DeleteRange functions to populate the mutations argument.
	MutateAll(ctx context.Context, mutations ...Mutation) (rowsAffected []int64, err error)
	// SetNow sets the function to use for getting the current time. This is used for testing purposes.
	SetNow(now func() time.Time)
}

type Mutation interface {
	isMutation()
}

type MutationResult struct {
	RowsAffected int64
	Err          error
}

func Put(key string, version int64, value any) PutMutation {
	return PutMutation{
		Key:     key,
		Version: version,
		Value:   value,
	}
}

type PutMutation struct {
	Key     string
	Version int64
	Value   any
}

func (PutMutation) isMutation() {}

func Patch(key string, version int64, value any) PatchMutation {
	return PatchMutation{
		Key:     key,
		Version: version,
		Value:   value,
	}
}

type PatchMutation struct {
	Key     string
	Version int64
	Value   any
}

func (PatchMutation) isMutation() {}

func Delete(keys ...string) DeleteMutation {
	return DeleteMutation{
		Keys: keys,
	}
}

type DeleteMutation struct {
	Keys []string
}

func (DeleteMutation) isMutation() {}

func DeletePrefix(prefix string, offset, limit int) DeletePrefixMutation {
	return DeletePrefixMutation{
		Prefix: prefix,
		Offset: offset,
		Limit:  limit,
	}
}

type DeletePrefixMutation struct {
	Prefix string
	Offset int
	Limit  int
}

func (DeletePrefixMutation) isMutation() {}

func DeleteRange(from, to string, offset, limit int) DeleteRangeMutation {
	return DeleteRangeMutation{
		From:   from,
		To:     to,
		Offset: offset,
		Limit:  limit,
	}
}

type DeleteRangeMutation struct {
	From   string
	To     string
	Offset int
	Limit  int
}

func (DeleteRangeMutation) isMutation() {}
