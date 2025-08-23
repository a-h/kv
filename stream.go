package kv

import (
	"context"
	"fmt"
	"iter"
	"time"
)

type CommitMode string

const (
	CommitModeNone  CommitMode = "none"
	CommitModeBatch CommitMode = "batch"
	CommitModeAll   CommitMode = "all"
)

func streamKeyFromName(name string) string {
	return fmt.Sprintf("github.com/a-h/kv/stream/%s", name)
}

func NewStreamConsumer(ctx context.Context, store Store, streamName, consumerName string) *StreamConsumer {
	return &StreamConsumer{
		Locker:              NewLocker(store, streamName, consumerName, 5*time.Minute),
		Store:               store,
		StreamName:          streamName,
		ConsumerName:        consumerName,
		Seq:                 -1,
		Limit:               10,
		MinBackoff:          100 * time.Millisecond,
		MaxBackoff:          5 * time.Second,
		LockExtendThreshold: 10 * time.Second,
		CommitMode:          CommitModeBatch,
	}
}

type StreamConsumer struct {
	Locker       *Locker
	Store        Store
	StreamName   string
	ConsumerName string
	// Seq is the current read position.
	Seq int
	// LastSeq is the last maximum sequence number seen.
	LastSeq int
	// Version for optimistic concurrency control.
	Version int
	// Limit is the maximum number of records to return in a single batch.
	Limit               int
	MinBackoff          time.Duration
	MaxBackoff          time.Duration
	LockExtendThreshold time.Duration
	// CommitMode controls when commits happen: "none", "batch", "all"
	CommitMode CommitMode
}

// Commit acknowledges that the records returned by Read have been processed.
func (s *StreamConsumer) Commit(ctx context.Context) error {
	if s.LastSeq < 0 {
		return fmt.Errorf("stream: LastSeq is not set, call Get before Commit or set LastSeq explicitly")
	}
	cs := StreamConsumerStatus{
		Name:        s.StreamName,
		Seq:         s.LastSeq + 1,
		LastUpdated: time.Now().UTC().Format(time.RFC3339),
	}
	err := s.Store.Put(ctx, streamKeyFromName(s.StreamName), s.Version, cs)
	if err != nil {
		return fmt.Errorf("stream: failed to commit offset %d: %w", s.Seq, err)
	}
	s.Version++
	s.Seq = s.LastSeq + 1
	return nil
}

// CommitUpTo acknowledges that all records up to and including the specified sequence have been processed.
func (s *StreamConsumer) CommitUpTo(ctx context.Context, seq int) error {
	s.LastSeq = seq
	return s.Commit(ctx)
}

// Delete removes the consumer record from the store.
func (s *StreamConsumer) Delete(ctx context.Context) error {
	_, err := s.Store.Delete(ctx, streamKeyFromName(s.StreamName))
	return err
}

// Status returns the current status of the consumer.
func (s *StreamConsumer) Status(ctx context.Context) (StreamConsumerStatus, bool, error) {
	var cs StreamConsumerStatus
	_, ok, err := s.Store.Get(ctx, streamKeyFromName(s.StreamName), &cs)
	if err != nil {
		return cs, false, fmt.Errorf("stream: failed to get consumer status: %w", err)
	}
	return cs, ok, nil
}

func (s *StreamConsumer) Read(ctx context.Context) iter.Seq2[StreamRecord, error] {
	return func(yield func(StreamRecord, error) bool) {
		var lockAcquired bool
		defer func() {
			if lockAcquired {
				_ = s.Locker.Unlock(ctx)
			}
		}()

		if s.Seq < 0 {
			var cs StreamConsumerStatus
			r, ok, err := s.Store.Get(ctx, streamKeyFromName(s.StreamName), &cs)
			if err != nil {
				if !yield(StreamRecord{}, fmt.Errorf("stream: failed to get consumer record: %w", err)) {
					return
				}
				return
			}
			s.Version = 0
			s.Seq = 0
			if ok {
				s.Version = r.Version
				s.Seq = cs.Seq
			}
		}

		lockBackoff := s.MinBackoff
		emptyBackoff := s.MinBackoff
		for {
			if err := ctx.Err(); err != nil {
				if !yield(StreamRecord{}, err) {
					return
				}
				return
			}

			locked, err := s.Locker.Lock(ctx)
			if err != nil {
				if !yield(StreamRecord{}, err) {
					return
				}
			}
			if !locked {
				time.Sleep(lockBackoff)
				lockBackoff *= 2
				if lockBackoff > s.MaxBackoff {
					lockBackoff = s.MaxBackoff
				}
				continue
			}
			lockAcquired = true
			lockBackoff = s.MinBackoff

			if s.Seq < 0 {
				if !yield(StreamRecord{}, fmt.Errorf("stream: offset is not set")) {
					return
				}
				return
			}
			records, err := s.Store.Stream(ctx, TypeAll, s.Seq, s.Limit)
			if err != nil {
				if !yield(StreamRecord{}, fmt.Errorf("stream: failed to get records: %w", err)) {
					return
				}
			}
			if len(records) == 0 {
				time.Sleep(emptyBackoff)
				emptyBackoff *= 2
				if emptyBackoff > s.MaxBackoff {
					emptyBackoff = s.MaxBackoff
				}
				continue
			}
			emptyBackoff = s.MinBackoff
			for _, r := range records {
				timeLeft := time.Until(s.Locker.LockedUntil)
				if timeLeft < s.LockExtendThreshold {
					acquired, err := s.Locker.Lock(ctx)
					if err != nil {
						if !yield(StreamRecord{}, err) {
							return
						}
						return
					}
					if !acquired {
						if !yield(StreamRecord{}, fmt.Errorf("stream: failed to extend lock")) {
							return
						}
						return
					}
				}
				if !yield(r, nil) {
					return
				}
				s.LastSeq = r.Seq
				if s.CommitMode == CommitModeAll {
					if err := s.Commit(ctx); err != nil {
						if !yield(StreamRecord{}, err) {
							return
						}
						return
					}
				}
			}

			if s.CommitMode == CommitModeBatch {
				if err := s.Commit(ctx); err != nil {
					if !yield(StreamRecord{}, err) {
						return
					}
					return
				}
			}
			if s.CommitMode == CommitModeNone {
				s.Seq = s.LastSeq + 1
			}
		}
	}
}

func NewLocker(store Store, streamName, lockedBy string, msgTimeout time.Duration) *Locker {
	return &Locker{
		Store:      store,
		StreamName: streamName,
		LockedBy:   lockedBy,
		MsgTimeout: msgTimeout,
	}
}

type Locker struct {
	Store       Store
	StreamName  string
	LockedBy    string
	MsgTimeout  time.Duration
	LockedUntil time.Time
}

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

func (l *Locker) Unlock(ctx context.Context) (err error) {
	return l.Store.LockRelease(ctx, streamKeyFromName(l.StreamName), l.LockedBy)
}

type StreamConsumerStatus struct {
	Name        string `json:"name"`
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
func GetStreamConsumerRecords(ctx context.Context, store Store, streamName string, limit int) ([]StreamRecord, error) {
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
	records, err := store.Stream(ctx, TypeAll, cs.Seq, limit)
	if err != nil {
		return nil, fmt.Errorf("stream: failed to get records: %w", err)
	}
	return records, nil
}
