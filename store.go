package kv

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"time"

	"github.com/google/uuid"
)

func init() {
	if math.MaxInt < math.MaxInt64 {
		panic("math.MaxInt is less than math.MaxInt64, this is not supported")
	}
}

// TypeOf extracts the type name from a value using reflection.
// For Entity Component System, this returns the struct name for struct types,
// or the underlying type name for other types.
func TypeOf(value any) string {
	if value == nil {
		return "nil"
	}

	t := reflect.TypeOf(value)

	// Handle pointers by getting the element type.
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	// For structs, return just the name (not the full package path).
	if t.Kind() == reflect.Struct {
		return t.Name()
	}

	// For other types, return the string representation.
	return t.String()
}

// Record is the record stored in the store prior to being unmarshaled.
type Record struct {
	Key     string    `json:"key"`
	Version int       `json:"version"`
	Value   []byte    `json:"value"`
	Type    string    `json:"type"`
	Created time.Time `json:"created"`
}

type Action string

const (
	ActionCreate Action = "create"
	ActionUpdate Action = "update"
	ActionDelete Action = "delete"
)

type Type string

const (
	TypeAll Type = "all"
)

// StreamRecord represents a record in the stream.
type StreamRecord struct {
	Seq    int    `json:"seq"`
	Action Action `json:"action"`
	Record Record
}

var ErrVersionMismatch = errors.New("version mismatch")

// TaskStatus represents the current state of a task.
type TaskStatus string

const (
	TaskStatusPending   TaskStatus = "pending"
	TaskStatusRunning   TaskStatus = "running"
	TaskStatusCompleted TaskStatus = "completed"
	TaskStatusFailed    TaskStatus = "failed"
	TaskStatusCancelled TaskStatus = "cancelled"
)

// Task represents a one-off task.
type Task struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	// Payload is the task data/configuration as JSON.
	Payload []byte     `json:"payload"`
	Status  TaskStatus `json:"status"`
	Created time.Time  `json:"created"`
	// ScheduledFor is when the task should run.
	ScheduledFor time.Time `json:"scheduledFor"`
	// StartedAt is when the task actually started.
	StartedAt *time.Time `json:"startedAt"`
	// CompletedAt is when the task finished.
	CompletedAt *time.Time `json:"completedAt"`
	// LastError is the last error message if failed.
	LastError      string `json:"lastError"`
	RetryCount     int    `json:"retryCount"`
	MaxRetries     int    `json:"maxRetries"`
	TimeoutSeconds int    `json:"timeoutSeconds"`
	// LockedBy is which runner is processing this task.
	LockedBy string `json:"lockedBy"`
	// LockedAt is when the task was locked.
	LockedAt *time.Time `json:"lockedAt"`
	// LockExpiresAt is when the lock expires.
	LockExpiresAt *time.Time `json:"lockExpiresAt"`
}

// NewTask creates a new task with required fields and sensible defaults.
// The ID will be generated automatically.
func NewTask(name string, payload []byte) Task {
	now := time.Now()
	return Task{
		ID:             uuid.New().String(),
		Name:           name,
		Payload:        payload,
		Status:         TaskStatusPending,
		Created:        now,
		ScheduledFor:   now,
		MaxRetries:     3,
		TimeoutSeconds: 300,
	}
}

// NewScheduledTask creates a new task scheduled to run at a specific time.
func NewScheduledTask(name string, payload []byte, scheduledFor time.Time) Task {
	task := NewTask(name, payload)
	task.ScheduledFor = scheduledFor
	return task
}

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

// ValueOf returns the value of a single record, unmarshaled into the given type.
func ValueOf[T any](record Record) (value T, err error) {
	err = json.Unmarshal(record.Value, &value)
	if err != nil {
		return value, err
	}
	return value, nil
}

type RecordOf[T any] struct {
	Key     string    `json:"key"`
	Version int       `json:"version"`
	Value   T         `json:"value"`
	Type    string    `json:"type"`
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
		values[i].Type = r.Type
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
	// GetType gets all keys of a given type from the store.
	GetType(ctx context.Context, t Type, offset, limit int) (rows []Record, err error)
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
	Put(ctx context.Context, key string, version int, value any) (err error)
	// Delete deletes keys from the store. If the key does not exist, no error is returned.
	Delete(ctx context.Context, keys ...string) (rowsAffected int, err error)
	// DeletePrefix deletes all keys with a given prefix from the store.
	DeletePrefix(ctx context.Context, prefix string, offset, limit int) (rowsAffected int, err error)
	// DeleteRange deletes all keys between the key from (inclusive) and to (exclusive).
	DeleteRange(ctx context.Context, from, to string, offset, limit int) (rowsAffected int, err error)
	// Count returns the number of keys in the store.
	Count(ctx context.Context) (n int, err error)
	// CountPrefix returns the number of keys in the store with a given prefix.
	CountPrefix(ctx context.Context, prefix string) (count int, err error)
	// CountRange returns the number of keys in the store between the key from (inclusive) and to (exclusive).
	CountRange(ctx context.Context, from, to string) (count int, err error)
	// Patch patches a key in the store. The patch is a JSON merge patch (RFC 7396), so would look something like map[string]any{"key": "value"}.
	Patch(ctx context.Context, key string, version int, patch any) (err error)
	// MutateAll runs the mutations against the store, as a single transaction.
	//
	// Use the Put, Patch, PutPatches, Delete, DeleteKeys, DeletePrefix and DeleteRange functions to populate the mutations argument.
	MutateAll(ctx context.Context, mutations ...Mutation) (rowsAffected []int, err error)
	// Stream returns all mutations that have happened, in the order they were applied.
	// It is used to follow changes to the store.
	Stream(ctx context.Context, t Type, seq int, limit int) (rows []StreamRecord, err error)
	// StreamSeq returns the current latest sequence number of the stream.
	StreamSeq(ctx context.Context) (seq int, err error)
	// StreamTrim trims the stream to the given sequence number.
	StreamTrim(ctx context.Context, seq int) (err error)
	// LockAcquire tries to acquire a lock, for a given duration.
	// Returns true if the lock was acquired, false if it was already locked.
	// Used to implementing distributed locks for stream processing.
	LockAcquire(ctx context.Context, name string, lockedBy string, duration time.Duration) (acquired bool, err error)
	// LockRelease releases a lock on a name, if it was acquired by the given lockedBy.
	LockRelease(ctx context.Context, name string, lockedBy string) (err error)
	// SetNow sets the function to use for getting the current time. This is used for testing purposes.
	SetNow(now func() time.Time)
	// LockStatus returns the status of a lock.
	LockStatus(ctx context.Context, name string) (status LockStatus, ok bool, err error)
}

type Scheduler interface {
	// New creates a new task with the given parameters.
	New(ctx context.Context, task Task) (err error)
	// Get retrieves a task by ID.
	Get(ctx context.Context, id string) (task Task, ok bool, err error)
	// List retrieves tasks with optional filtering by status and name.
	List(ctx context.Context, status TaskStatus, name string, offset, limit int) (tasks []Task, err error)
	// Cancel cancels a task by setting its status to cancelled.
	// If the task is already running, it will continue but won't be retried.
	Cancel(ctx context.Context, id string) (err error)
	// Lock retrieves the next pending task that should be run and marks it as running.
	// Returns the task and whether it was successfully locked by the given runner ID.
	// If taskTypes is empty, it handles all task types.
	Lock(ctx context.Context, runnerID string, lockDuration time.Duration, taskTypes ...string) (task Task, locked bool, err error)
	// Release releases the lock on a task and updates its final status.
	// Based on the status, it handles retry logic and scheduling automatically.
	Release(ctx context.Context, id string, runnerID string, status TaskStatus, errorMessage string) (err error)
}

type Mutation interface {
	isMutation()
}

type MutationResult struct {
	RowsAffected int
	Err          error
}

func Put(key string, version int, value any) PutMutation {
	return PutMutation{
		Key:     key,
		Version: version,
		Value:   value,
	}
}

type PutMutation struct {
	Key     string
	Version int
	Value   any
}

func (PutMutation) isMutation() {}

func Patch(key string, version int, value any) PatchMutation {
	return PatchMutation{
		Key:     key,
		Version: version,
		Value:   value,
	}
}

type PatchMutation struct {
	Key     string
	Version int
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

// LockStatus contains information about a lock.
type LockStatus struct {
	Name      string    `json:"name"`
	LockedBy  string    `json:"lockedBy"`
	LockedAt  time.Time `json:"lockedAt"`
	ExpiresAt time.Time `json:"expiresAt"`
}
