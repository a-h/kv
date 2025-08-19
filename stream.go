package kv

import (
	"context"
	"fmt"
	"time"
)

func NewConsumer(ctx context.Context, store Store, name string) (s *Consumer, err error) {
	s = &Consumer{
		Store: store,
		key:   fmt.Sprintf("github.com/a-h/kv/stream/%s", name),
		Seq:   -1,
		Limit: 10,
	}
	var sr streamRecord
	r, _, err := s.Store.Get(ctx, s.key, &sr)
	if err != nil {
		return nil, fmt.Errorf("stream: failed to get consumer record: %w", err)
	}
	s.Version = r.Version
	s.Seq = sr.Seq
	return s, nil
}

type streamRecord struct {
	Seq int `json:"seq"`
	// LastUpdated is the last time the consumer record was updated.
	LastUpdated string `json:"lastUpdated"`
}

type Consumer struct {
	Store Store
	key   string
	// Seq is the read position.
	Seq int
	// lastMaxSeq is the last maximum sequence number seen by the consumer.
	// This is updated by Get, and used by Commit to ensure that the Consumer
	// does not commit a record that has already been processed.
	lastMaxSeq int
	// Version of the streamRecord, used to apply optimistic concurrency control to the consumer record.
	Version int
	// Limit is the maximum number of records to return in a single Get call.
	Limit int
}

// Get a batch of records.
func (s *Consumer) Get(ctx context.Context) (records []StreamRecord, err error) {
	if s.Seq < 0 {
		return nil, fmt.Errorf("stream: offset is not set")
	}
	records, err = s.Store.Stream(ctx, s.Seq, s.Limit)
	if err != nil {
		return nil, fmt.Errorf("stream: failed to get records: %w", err)
	}
	if len(records) > 0 {
		s.lastMaxSeq = records[len(records)-1].Seq
	}
	return records, nil
}

// Commit acknowledges that the records returned by Get have been processed.
func (s *Consumer) Commit(ctx context.Context) (err error) {
	if s.lastMaxSeq < 0 {
		return fmt.Errorf("stream: lastMaxSeq is not set, call Get before Commit")
	}
	sr := streamRecord{
		Seq:         s.lastMaxSeq + 1,
		LastUpdated: time.Now().UTC().Format(time.RFC3339),
	}
	err = s.Store.Put(ctx, s.key, s.Version, sr)
	if err != nil {
		return fmt.Errorf("stream: failed to commit offset %d: %w", s.Seq, err)
	}
	s.Version++
	s.Seq = s.lastMaxSeq + 1
	return nil
}

// Delete the consumer record from the store.
func (s *Consumer) Delete(ctx context.Context) (err error) {
	_, err = s.Store.Delete(ctx, s.key)
	return err
}
