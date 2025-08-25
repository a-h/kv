package kv

import (
	"context"
	"fmt"
	"time"
)

// streamKeyFromName generates a unique key for stream consumer state.
func streamKeyFromName(name string) string {
	return fmt.Sprintf("github.com/a-h/kv/stream/%s", name)
}

// NewStreamConsumer creates a new stream consumer for reading records from a KV store stream.
// The consumer maintains position state and supports exclusive access via locking.
func NewStreamConsumer(ctx context.Context, store Store, streamName, consumerName string, ofType Type) *StreamConsumer {
	return &StreamConsumer{
		Locker:       NewLocker(store, streamName, consumerName, 5*time.Minute),
		Store:        store,
		StreamName:   streamName,
		ConsumerName: consumerName,
		OfType:       ofType,
		Seq:          -1,
		MinBackoff:   100 * time.Millisecond,
		MaxBackoff:   5 * time.Second,
	}
}

// StreamConsumer provides safe, exclusive access to a stream of records from a KV store.
// It maintains position state and prevents multiple consumers from processing the same records.
type StreamConsumer struct {
	Locker       *Locker
	Store        Store
	StreamName   string
	ConsumerName string
	OfType       Type
	Seq          int
	Version      int
	MinBackoff   time.Duration
	MaxBackoff   time.Duration
}

// Commit acknowledges that all records in the batch have been successfully processed.
// This updates the consumer's position in the stream so these records won't be redelivered
// but keeps the lock held for future Get() calls.
// Call this only after successfully processing all records from the batch returned by Get().
func (s *StreamConsumer) Commit(ctx context.Context, batch []StreamRecord) error {
	if len(batch) == 0 {
		return nil
	}

	maxSeq := batch[0].Seq
	for _, record := range batch {
		if record.Seq > maxSeq {
			maxSeq = record.Seq
		}
	}

	return s.CommitUpTo(ctx, maxSeq)
}

// CommitUpTo acknowledges that all records up to and including the specified sequence have been processed.
// This updates the consumer's position but keeps the lock held for future Get() calls.
// This is useful when you want to commit only a subset of records from a batch.
func (s *StreamConsumer) CommitUpTo(ctx context.Context, seq int) error {
	cs := StreamConsumerStatus{
		Name:        s.StreamName,
		OfType:      s.OfType,
		Seq:         seq + 1,
		LastUpdated: time.Now().UTC().Format(time.RFC3339),
	}
	err := s.Store.Put(ctx, streamKeyFromName(s.StreamName), s.Version, cs)
	if err != nil {
		return fmt.Errorf("stream: failed to commit offset %d: %w", seq, err)
	}
	s.Version++
	s.Seq = seq + 1

	return nil
}

// Unlock explicitly releases the lock held by this consumer.
// This is useful when you're done processing and want to release the lock immediately
// rather than waiting for it to expire.
func (s *StreamConsumer) Unlock(ctx context.Context) (err error) {
	if err = s.Locker.Unlock(ctx); err != nil {
		return fmt.Errorf("stream: failed to unlock consumer: %w", err)
	}
	s.Locker.LockedUntil = time.Time{}
	return nil
}

// Delete removes the consumer record from the store.
// This resets the consumer's position and allows it to start from the beginning.
func (s *StreamConsumer) Delete(ctx context.Context) error {
	_, err := s.Store.Delete(ctx, streamKeyFromName(s.StreamName))
	return err
}

// Status returns the current status of the consumer including its position in the stream.
func (s *StreamConsumer) Status(ctx context.Context) (StreamConsumerStatus, bool, error) {
	var cs StreamConsumerStatus
	_, ok, err := s.Store.Get(ctx, streamKeyFromName(s.StreamName), &cs)
	if err != nil {
		return cs, false, fmt.Errorf("stream: failed to get consumer status: %w", err)
	}
	return cs, ok, nil
}

// Get attempts to acquire/extend a lock and return a batch of records.
// The lock is acquired if we don't have one, or extended if the remaining time is less than lockPeriod.
// If successful, returns records and the lock is held until explicitly released with Unlock().
// If unsuccessful, returns an empty slice and a waitFor duration suggesting how long to wait before retrying.
// The caller is responsible for implementing retry logic and waiting.
// Call Commit() to acknowledge processing, or Unlock() to release the lock when done.
func (s *StreamConsumer) Get(ctx context.Context, lockPeriod time.Duration, batchSize int) (records []StreamRecord, waitFor time.Duration, err error) {
	now := time.Now().UTC()
	timeRemaining := s.Locker.LockedUntil.Sub(now)

	if timeRemaining < lockPeriod {
		s.Locker.MsgTimeout = lockPeriod
		locked, err := s.Locker.Lock(ctx)
		if err != nil {
			return nil, 0, fmt.Errorf("stream: failed to acquire lock: %w", err)
		}
		if !locked {
			return nil, s.MinBackoff, nil
		}
	}

	if s.Seq < 0 {
		var cs StreamConsumerStatus
		r, ok, err := s.Store.Get(ctx, streamKeyFromName(s.StreamName), &cs)
		if err != nil {
			_ = s.Unlock(ctx)
			return nil, 0, fmt.Errorf("stream: failed to get consumer record: %w", err)
		}
		s.Version = 0
		s.Seq = 0
		if ok {
			s.Version = r.Version
			s.Seq = cs.Seq
		}
	}

	records, err = s.Store.Stream(ctx, s.OfType, s.Seq, batchSize)
	if err != nil {
		_ = s.Unlock(ctx)
		return nil, 0, fmt.Errorf("stream: failed to get records: %w", err)
	}

	if len(records) == 0 {
		return nil, s.MinBackoff, nil
	}

	return records, 0, nil
}

// NewLocker creates a new locker for stream consumer coordination.
func NewLocker(store Store, streamName, lockedBy string, msgTimeout time.Duration) *Locker {
	return &Locker{
		Store:      store,
		StreamName: streamName,
		LockedBy:   lockedBy,
		MsgTimeout: msgTimeout,
	}
}

// Locker provides distributed locking for stream consumers to ensure exclusive access.
type Locker struct {
	Store       Store
	StreamName  string
	LockedBy    string
	MsgTimeout  time.Duration
	LockedUntil time.Time
}

// Lock attempts to acquire an exclusive lock for this consumer.
// Returns true if the lock was acquired, false if another consumer holds it.
func (l *Locker) Lock(ctx context.Context) (acquired bool, err error) {
	lockedUntil := time.Now().UTC().Add(l.MsgTimeout)
	acquired, err = l.Store.LockAcquire(ctx, streamKeyFromName(l.StreamName), l.LockedBy, l.MsgTimeout)
	if err != nil {
		return false, fmt.Errorf("stream: failed to acquire lock: %w", err)
	}
	if !acquired {
		l.LockedUntil = time.Time{}
		return false, nil
	}
	l.LockedUntil = lockedUntil
	return true, nil
}

// Unlock releases the lock held by this consumer.
func (l *Locker) Unlock(ctx context.Context) (err error) {
	return l.Store.LockRelease(ctx, streamKeyFromName(l.StreamName), l.LockedBy)
}

type StreamConsumerStatus struct {
	Name        string `json:"name"`
	OfType      Type   `json:"ofType"`
	Seq         int    `json:"seq"`
	LastUpdated string `json:"lastUpdated"`
}

// GetStreamConsumerStatus returns the status of a consumer (for administrative use).
func GetStreamConsumerStatus(ctx context.Context, store Store, streamName string) (StreamConsumerStatus, bool, error) {
	var cs StreamConsumerStatus
	_, ok, err := store.Get(ctx, streamKeyFromName(streamName), &cs)
	if err != nil {
		return cs, false, fmt.Errorf("stream: failed to get consumer status: %w", err)
	}
	return cs, ok, nil
}

// DeleteStreamConsumer removes a consumer record (for administrative use).
func DeleteStreamConsumer(ctx context.Context, store Store, streamName string) error {
	_, err := store.Delete(ctx, streamKeyFromName(streamName))
	return err
}

// CommitStreamConsumer sets the consumer position (for administrative use).
func CommitStreamConsumer(ctx context.Context, store Store, streamName string, seq int) error {
	var cs StreamConsumerStatus
	r, ok, err := store.Get(ctx, streamKeyFromName(streamName), &cs)
	if err != nil {
		return fmt.Errorf("stream: failed to get consumer record: %w", err)
	}
	if !ok {
		return fmt.Errorf("stream: consumer not found")
	}

	cs.Seq = seq
	cs.LastUpdated = time.Now().UTC().Format(time.RFC3339)

	err = store.Put(ctx, streamKeyFromName(streamName), r.Version, cs)
	if err != nil {
		return fmt.Errorf("stream: failed to commit offset %d: %w", seq, err)
	}
	return nil
}

// GetStreamConsumerRecords gets records for a consumer (for administrative use).
func GetStreamConsumerRecords(ctx context.Context, store Store, streamName string, t Type, limit int) ([]StreamRecord, error) {
	var cs StreamConsumerStatus
	_, ok, err := store.Get(ctx, streamKeyFromName(streamName), &cs)
	if err != nil {
		return nil, fmt.Errorf("stream: failed to get consumer record: %w", err)
	}
	if !ok {
		return nil, fmt.Errorf("stream: consumer not found")
	}
	if cs.Seq < 0 {
		return nil, fmt.Errorf("stream: offset is not set")
	}
	if limit <= 0 {
		limit = 10
	}
	records, err := store.Stream(ctx, t, cs.Seq, limit)
	if err != nil {
		return nil, fmt.Errorf("stream: failed to get records: %w", err)
	}
	return records, nil
}
