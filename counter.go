package kv

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

// Counter records access counts for grouped items.
type Counter interface {
	Increment(group, name string)
}

// CounterStat holds the access statistics for a single item.
type CounterStat struct {
	Count        int64     `json:"count"`
	LastAccessed time.Time `json:"lastAccessed"`
}

// CounterKeyPrefix is the key prefix used for counter statistics in the store.
const CounterKeyPrefix = "counter/"

// counterAccessEvent records a single access to a cached item.
type counterAccessEvent struct {
	Delta      int64     `json:"delta"`
	AccessedAt time.Time `json:"accessedAt"`
}

// process applies an access event to the stat (mirrors statemachine Counter.Process).
func (s *CounterStat) process(e counterAccessEvent) {
	s.Count += e.Delta
	if e.AccessedAt.After(s.LastAccessed) {
		s.LastAccessed = e.AccessedAt
	}
}

type counterEvent struct {
	group      string
	name       string
	accessedAt time.Time
}

type counterBatchEntry struct {
	group      string
	name       string
	delta      int64
	lastAccess time.Time
}

// AsyncCounter is a Counter that processes increments asynchronously
// using an internal buffered channel to avoid blocking callers.
type AsyncCounter struct {
	store Store
	ch    chan counterEvent
}

// NewAsyncCounter creates an AsyncCounter backed by the given store.
// bufferSize controls the size of the internal channel buffer.
func NewAsyncCounter(store Store, bufferSize int) *AsyncCounter {
	return &AsyncCounter{
		store: store,
		ch:    make(chan counterEvent, bufferSize),
	}
}

// Increment queues an increment for the given group and name.
// If the internal buffer is full, the increment is silently dropped.
func (c *AsyncCounter) Increment(group, name string) {
	select {
	case c.ch <- counterEvent{group: group, name: name, accessedAt: time.Now()}:
	default:
	}
}

// Run processes queued increments until the context is cancelled.
func (c *AsyncCounter) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event := <-c.ch:
			batch := c.drainChannel(event)
			for _, b := range batch {
				if err := c.applyDelta(ctx, b.group, b.name, b.delta, b.lastAccess); err != nil {
					slog.WarnContext(ctx, "failed to apply counter delta",
						slog.String("group", b.group),
						slog.String("name", b.name),
						slog.Int64("delta", b.delta),
						slog.Any("err", err),
					)
				}
			}
		}
	}
}

// drainChannel collects available events from the channel into a batch.
// It starts with the given initial event and reads until the channel is empty.
func (c *AsyncCounter) drainChannel(initial counterEvent) []counterBatchEntry {
	acc := map[string]*counterBatchEntry{}
	addEvent := func(e counterEvent) {
		key := e.group + "/" + e.name
		if entry, ok := acc[key]; ok {
			entry.delta++
			if e.accessedAt.After(entry.lastAccess) {
				entry.lastAccess = e.accessedAt
			}
			return
		}
		acc[key] = &counterBatchEntry{group: e.group, name: e.name, delta: 1, lastAccess: e.accessedAt}
	}
	addEvent(initial)
	for {
		select {
		case e := <-c.ch:
			addEvent(e)
		default:
			result := make([]counterBatchEntry, 0, len(acc))
			for _, e := range acc {
				result = append(result, *e)
			}
			return result
		}
	}
}

// applyDelta loads the current stat, applies the access event, and writes it back atomically.
// It mirrors the statemachine Processor.Process approach: load state, process event in-memory,
// then commit with MutateAll. It retries on version mismatch.
func (c *AsyncCounter) applyDelta(ctx context.Context, group, name string, delta int64, lastAccess time.Time) error {
	key := fmt.Sprintf("%s%s/%s", CounterKeyPrefix, group, name)
	event := counterAccessEvent{Delta: delta, AccessedAt: lastAccess}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		var stat CounterStat
		r, ok, err := c.store.Get(ctx, key, &stat)
		if err != nil {
			return err
		}
		// Apply the event in-memory (mirrors statemachine Counter.Process).
		stat.process(event)
		version := 0
		if ok {
			version = r.Version
		}
		_, err = c.store.MutateAll(ctx, Put(key, version, stat))
		if err == nil {
			return nil
		}
		if !errors.Is(err, ErrVersionMismatch) {
			return err
		}
		// Retry on version mismatch (mirrors statemachine's ErrOptimisticConcurrency handling).
	}
}
