package rqlitekv

import (
	"context"
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
