package kv

import (
	"context"
	"fmt"
	"time"
)

func NewConsumer(ctx context.Context, store Store, name string) (s *Consumer, err error) {
	s = &Consumer{
		Store:  store,
		key:    fmt.Sprintf("github.com/a-h/kv/stream/%s", name),
		Offset: -1,
		Limit:  10,
	}
	var sr streamRecord
	r, _, err := s.Store.Get(ctx, s.key, &sr)
	if err != nil {
		return nil, fmt.Errorf("stream: failed to get consumer record: %w", err)
	}
	s.Version = r.Version
	s.Offset = sr.Offset
	return s, nil
}

type streamRecord struct {
	Offset int `json:"offset"`
	// LastUpdated is the last time the consumer record was updated in ns.
	LastUpdated int64 `json:"lastUpdated"`
}

type Consumer struct {
	Store Store
	key   string
	// Offset is the read position.
	Offset int
	// Version of the streamRecord.
	Version int64
	// recordCount is the number of records last returned.
	// offset + recordCount is the next offset to read from.
	recordCount int
	Limit       int
}

// Get a batch of records.
func (s *Consumer) Get(ctx context.Context) (records []Record, err error) {
	if s.Offset < 0 {
		return nil, fmt.Errorf("stream: offset is not set")
	}
	records, err = s.Store.Stream(ctx, s.Offset, s.Limit)
	if err != nil {
		return nil, fmt.Errorf("stream: failed to get records: %w", err)
	}
	s.recordCount = len(records)
	return records, nil
}

// Commit acknowledges that the records returned by Get have been processed.
func (s *Consumer) Commit(ctx context.Context) (err error) {
	if s.Offset < 0 {
		return fmt.Errorf("stream: offset is not set")
	}
	sr := streamRecord{
		Offset:      s.Offset + s.recordCount,
		LastUpdated: time.Now().UnixNano(),
	}
	err = s.Store.Put(ctx, s.key, s.Version, sr)
	if err != nil {
		return fmt.Errorf("stream: failed to commit offset %d: %w", s.Offset, err)
	}
	s.Version++
	s.Offset = sr.Offset
	s.recordCount = 0
	return nil
}

// Delete the consumer record from the store.
func (s *Consumer) Delete(ctx context.Context) (err error) {
	_, err = s.Store.Delete(ctx, s.key)
	return err
}
