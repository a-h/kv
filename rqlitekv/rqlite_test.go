package rqlitekv

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/a-h/kv/tests"
	rqlitehttp "github.com/rqlite/rqlite-go-http"
)

func TestRqlite(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	client, err := rqlitehttp.NewClient("http://localhost:4001", nil)
	if err != nil {
		t.Fatal(err)
	}
	client.SetBasicAuth("admin", "secret")

	store := NewStore(client)
	scheduler := NewScheduler(client)
	tests.Run(t, store, scheduler)
}

func newTestRqlite(t *testing.T) (*Rqlite, error) {
	t.Helper()
	client, err := rqlitehttp.NewClient("http://localhost:4001", nil)
	if err != nil {
		return nil, err
	}
	client.SetBasicAuth("admin", "secret")
	rq := New(client)
	if err = rq.Init(context.Background()); err != nil {
		return nil, err
	}
	return rq, nil
}

func TestTryGetInt(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		expected int
		wantErr  bool
	}{
		{name: "json.Number integer can be converted", input: json.Number("42"), expected: 42},
		{name: "json.Number zero can be converted", input: json.Number("0"), expected: 0},
		{name: "nil returns error", input: nil, wantErr: true},
		{name: "string returns error", input: "42", wantErr: true},
		{name: "float64 returns error", input: float64(42), wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tryGetInt(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.expected {
				t.Errorf("expected %d, got %d", tt.expected, got)
			}
		})
	}
}

func TestNewRowFromValues(t *testing.T) {
	t.Run("nil value column is handled without panic", func(t *testing.T) {
		values := []any{
			"key1",
			json.Number("1"),
			nil,
			"Person",
			"2026-01-01T00:00:00Z",
		}
		r, err := newRowFromValues(values)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if r.Value != nil {
			t.Errorf("expected nil value, got %v", r.Value)
		}
	})

	t.Run("non-string value column returns error not panic", func(t *testing.T) {
		values := []any{
			"key1",
			json.Number("1"),
			json.Number("123"),
			"Person",
			"2026-01-01T00:00:00Z",
		}
		_, err := newRowFromValues(values)
		if err == nil {
			t.Fatalf("expected error for non-string value column, got nil")
		}
	})

	t.Run("nil created column returns error not panic", func(t *testing.T) {
		values := []any{
			"key1",
			json.Number("1"),
			`{"name":"Alice"}`,
			"Person",
			nil,
		}
		_, err := newRowFromValues(values)
		if err == nil {
			t.Fatalf("expected error for nil created column, got nil")
		}
	})
}

func TestStreamSeqWhenNoSequenceRowExists(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	rq, err := newTestRqlite(t)
	if err != nil {
		t.Fatalf("failed to connect to rqlite: %v", err)
	}

	// sqlite_sequence only records a table after its first autoincrement insert.
	// Simulate a fresh DB by querying a table name that has never had an insert.
	seq, err := rq.QueryScalarInt64(t.Context(),
		`select coalesce(max(seq), 0) from sqlite_sequence where name = 'nonexistent_table';`,
		nil)
	if err != nil {
		t.Fatalf("QueryScalarInt64 on missing sqlite_sequence entry returned error: %v", err)
	}
	if seq != 0 {
		t.Errorf("expected 0, got %d", seq)
	}
}

func TestLockAcquireUsesConsistentNow(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	rq, err := newTestRqlite(t)
	if err != nil {
		t.Fatalf("failed to connect to rqlite: %v", err)
	}

	callCount := 0
	fixed := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	rq.Now = func() time.Time {
		callCount++
		return fixed
	}

	acquired, err := rq.LockAcquire(t.Context(), "test-lock", "runner1", time.Minute)
	if err != nil {
		t.Fatalf("unexpected error acquiring lock: %v", err)
	}
	if !acquired {
		t.Fatal("expected lock to be acquired")
	}
	if callCount != 1 {
		t.Errorf("expected Now() to be called exactly once, got %d calls", callCount)
	}
}
