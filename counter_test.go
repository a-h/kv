package kv_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/a-h/kv"
	"github.com/a-h/kv/sqlitekv"
	"zombiezen.com/go/sqlite/sqlitex"
)

func newTestStore(t *testing.T) kv.Store {
	t.Helper()
	pool, err := sqlitex.NewPool("file::memory:?mode=memory&cache=shared", sqlitex.PoolOptions{})
	if err != nil {
		t.Fatalf("failed to create sqlite pool: %v", err)
	}
	t.Cleanup(func() { pool.Close() })
	store := sqlitekv.NewStore(pool)
	if err = store.Init(context.Background()); err != nil {
		t.Fatalf("failed to init store: %v", err)
	}
	return store
}

func TestAsyncCounterIncrement(t *testing.T) {
	store := newTestStore(t)
	c := kv.NewAsyncCounter(store, 100)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- c.Run(ctx)
	}()

	c.Increment("npm", "lodash@4.17.21")
	c.Increment("npm", "lodash@4.17.21")
	c.Increment("npm", "express@4.18.0")

	// Allow the background goroutine to process events.
	time.Sleep(100 * time.Millisecond)
	cancel()
	<-done

	// Check lodash counter.
	var lodashStat kv.CounterStat
	_, ok, err := store.Get(context.Background(), fmt.Sprintf("%snpm/lodash@4.17.21", kv.CounterKeyPrefix), &lodashStat)
	if err != nil {
		t.Fatalf("unexpected error getting lodash stat: %v", err)
	}
	if !ok {
		t.Fatal("lodash stat not found")
	}
	if lodashStat.Count != 2 {
		t.Errorf("expected lodash count 2, got %d", lodashStat.Count)
	}
	if lodashStat.LastAccessed.IsZero() {
		t.Error("expected lodash LastAccessed to be set")
	}

	// Check express counter.
	var expressStat kv.CounterStat
	_, ok, err = store.Get(context.Background(), fmt.Sprintf("%snpm/express@4.18.0", kv.CounterKeyPrefix), &expressStat)
	if err != nil {
		t.Fatalf("unexpected error getting express stat: %v", err)
	}
	if !ok {
		t.Fatal("express stat not found")
	}
	if expressStat.Count != 1 {
		t.Errorf("expected express count 1, got %d", expressStat.Count)
	}
}

func TestAsyncCounterDropsWhenFull(t *testing.T) {
	store := newTestStore(t)
	// Buffer size of 0 means all increments are dropped.
	c := kv.NewAsyncCounter(store, 0)

	c.Increment("npm", "lodash@4.17.21")

	// No Run goroutine started; nothing should be in the store.
	var stat kv.CounterStat
	_, ok, err := store.Get(context.Background(), fmt.Sprintf("%snpm/lodash@4.17.21", kv.CounterKeyPrefix), &stat)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Error("expected no stat when channel buffer is full, but got one")
	}
}

func TestAsyncCounterMultipleGroups(t *testing.T) {
	store := newTestStore(t)
	c := kv.NewAsyncCounter(store, 100)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- c.Run(ctx)
	}()

	groups := []struct {
		group string
		name  string
		times int
	}{
		{"npm", "lodash@4.17.21", 3},
		{"nix", "sha256-abc123", 5},
		{"python", "requests==2.31.0", 2},
	}

	for _, g := range groups {
		for range g.times {
			c.Increment(g.group, g.name)
		}
	}

	time.Sleep(100 * time.Millisecond)
	cancel()
	<-done

	for _, g := range groups {
		var stat kv.CounterStat
		key := fmt.Sprintf("%s%s/%s", kv.CounterKeyPrefix, g.group, g.name)
		_, ok, err := store.Get(context.Background(), key, &stat)
		if err != nil {
			t.Fatalf("unexpected error getting stat for %s/%s: %v", g.group, g.name, err)
		}
		if !ok {
			t.Errorf("stat not found for %s/%s", g.group, g.name)
			continue
		}
		if stat.Count != int64(g.times) {
			t.Errorf("expected count %d for %s/%s, got %d", g.times, g.group, g.name, stat.Count)
		}
	}
}

func TestNewCleanupTaskHandler(t *testing.T) {
	store := newTestStore(t)

	var cleanupCalled bool
	cleanupFn := func(ctx context.Context, s kv.Store) (kv.CleanupResult, error) {
		cleanupCalled = true
		return kv.CleanupResult{FilesDeleted: 3}, nil
	}

	handler := kv.NewCleanupTaskHandler(cleanupFn, store)

	task := kv.NewTask("daily-cleanup", nil)
	if err := handler(context.Background(), task); err != nil {
		t.Fatalf("unexpected error from cleanup handler: %v", err)
	}

	if !cleanupCalled {
		t.Error("expected cleanup function to be called")
	}
}

func TestScheduleDailyCleanup(t *testing.T) {
	pool, err := sqlitex.NewPool("file::memory:?mode=memory&cache=shared", sqlitex.PoolOptions{})
	if err != nil {
		t.Fatalf("failed to create sqlite pool: %v", err)
	}
	defer pool.Close()

	store := sqlitekv.NewStore(pool)
	scheduler := sqlitekv.NewScheduler(pool)

	ctx := context.Background()
	if err = store.Init(ctx); err != nil {
		t.Fatalf("failed to init store: %v", err)
	}

	const taskName = "daily-cleanup"
	if err = kv.ScheduleDailyCleanup(ctx, scheduler, taskName, 2*time.Hour); err != nil {
		t.Fatalf("unexpected error scheduling daily cleanup: %v", err)
	}

	tasks, err := scheduler.List(ctx, kv.TaskStatusPending, taskName, 0, 10)
	if err != nil {
		t.Fatalf("unexpected error listing tasks: %v", err)
	}
	if len(tasks) != 1 {
		t.Fatalf("expected 1 pending task, got %d", len(tasks))
	}

	task := tasks[0]
	if task.Name != taskName {
		t.Errorf("expected task name %q, got %q", taskName, task.Name)
	}

	// Verify the scheduled time is in the future.
	if !task.ScheduledFor.After(time.Now()) {
		t.Errorf("expected task to be scheduled in the future, got %v", task.ScheduledFor)
	}

	// Verify the scheduled time is within the next 24 hours.
	if task.ScheduledFor.After(time.Now().Add(24 * time.Hour)) {
		t.Errorf("expected task to be scheduled within 24 hours, got %v", task.ScheduledFor)
	}
}
