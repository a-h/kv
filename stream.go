package kv

import (
	"context"
	"fmt"
	"time"
)

func NewStreamConsumer(ctx context.Context, store Store, name string) (s *StreamConsumer, err error) {
	s = &StreamConsumer{
		Store: store,
		key:   fmt.Sprintf("github.com/a-h/kv/stream/%s", name),
		Name:  name,
		Seq:   -1,
		Limit: 10,
	}
	var cs StreamConsumerStatus
	r, _, err := s.Store.Get(ctx, s.key, &cs)
	if err != nil {
		return nil, fmt.Errorf("stream: failed to get consumer record: %w", err)
	}
	s.Version = r.Version
	s.Seq = cs.Seq
	return s, nil
}

type StreamConsumerStatus struct {
	Name        string `json:"name"`
	Seq         int    `json:"seq"`
	LastUpdated string `json:"lastUpdated"`
}

type StreamConsumer struct {
	Store Store
	key   string
	Name  string
	// Seq is the read position.
	Seq int
	// LastSeq is the last maximum sequence number seen by the consumer.
	// This is updated by Get, and used by Commit to ensure that the Consumer
	// does not commit a record that has already been processed.
	LastSeq int
	// Version of the streamRecord, used to apply optimistic concurrency control to the consumer record.
	Version int
	// Limit is the maximum number of records to return in a single Get call.
	Limit int
}

// Get a batch of records.
func (s *StreamConsumer) Get(ctx context.Context) (records []StreamRecord, err error) {
	if s.Seq < 0 {
		return nil, fmt.Errorf("stream: offset is not set")
	}
	records, err = s.Store.Stream(ctx, s.Seq, s.Limit)
	if err != nil {
		return nil, fmt.Errorf("stream: failed to get records: %w", err)
	}
	if len(records) > 0 {
		s.LastSeq = records[len(records)-1].Seq
	}
	return records, nil
}

// Commit acknowledges that the records returned by Get have been processed.
func (s *StreamConsumer) Commit(ctx context.Context) (err error) {
	if s.LastSeq < 0 {
		return fmt.Errorf("stream: LastSeq is not set, call Get before Commit or set LastSeq explicitly")
	}
	cs := StreamConsumerStatus{
		Name:        s.Name,
		Seq:         s.LastSeq + 1,
		LastUpdated: time.Now().UTC().Format(time.RFC3339),
	}
	err = s.Store.Put(ctx, s.key, s.Version, cs)
	if err != nil {
		return fmt.Errorf("stream: failed to commit offset %d: %w", s.Seq, err)
	}
	s.Version++
	s.Seq = s.LastSeq + 1
	return nil
}

// Delete the consumer record from the store.
func (s *StreamConsumer) Delete(ctx context.Context) (err error) {
	_, err = s.Store.Delete(ctx, s.key)
	return err
}

// Status returns the current status of the consumer.
func (s *StreamConsumer) Status(ctx context.Context) (cs StreamConsumerStatus, ok bool, err error) {
	_, ok, err = s.Store.Get(ctx, s.key, &cs)
	if err != nil {
		return cs, false, fmt.Errorf("stream: failed to get consumer status: %w", err)
	}
	return cs, ok, nil
}
