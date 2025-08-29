package kv

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

// createTestRecord creates test stream records.
func createTestRecord(seq int, t Type, key string, value any) StreamRecord {
	valueBytes, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return StreamRecord{
		Seq: seq,
		Record: Record{
			Key:     key,
			Version: 1,
			Value:   valueBytes,
			Type:    string(t),
			Created: time.Now(),
		},
	}
}

// mockStore for testing error conditions.
type mockStore struct {
	lockAcquireFunc  func(ctx context.Context, key, lockedBy string, timeout time.Duration) (bool, error)
	lockReleaseFunc  func(ctx context.Context, key, lockedBy string) error
	streamFunc       func(ctx context.Context, t Type, seq, limit int) ([]StreamRecord, error)
	putFunc          func(ctx context.Context, key string, version int, value any) error
	getFunc          func(ctx context.Context, key string, value any) (Record, bool, error)
	deleteFunc       func(ctx context.Context, keys ...string) (int, error)
	initFunc         func(ctx context.Context) error
	countFunc        func(ctx context.Context) (int, error)
	countPrefixFunc  func(ctx context.Context, prefix string) (int, error)
	countRangeFunc   func(ctx context.Context, from, to string) (int, error)
	getPrefixFunc    func(ctx context.Context, prefix string, offset, limit int) ([]Record, error)
	getRangeFunc     func(ctx context.Context, from, to string, offset, limit int) ([]Record, error)
	getTypeFunc      func(ctx context.Context, t Type, offset, limit int) ([]Record, error)
	listFunc         func(ctx context.Context, start, limit int) ([]Record, error)
	deletePrefixFunc func(ctx context.Context, prefix string, offset, limit int) (int, error)
	deleteRangeFunc  func(ctx context.Context, from, to string, offset, limit int) (int, error)
	patchFunc        func(ctx context.Context, key string, version int, patch any) error
	mutateAllFunc    func(ctx context.Context, mutations ...Mutation) ([]int, error)
	streamSeqFunc    func(ctx context.Context) (int, error)
	streamTrimFunc   func(ctx context.Context, seq int) error
	setNowFunc       func(now func() time.Time)
	lockStatusFunc   func(ctx context.Context, name string) (LockStatus, bool, error)
}

func (m *mockStore) Init(ctx context.Context) error {
	if m.initFunc != nil {
		return m.initFunc(ctx)
	}
	return nil
}
func (m *mockStore) LockStatus(ctx context.Context, name string) (LockStatus, bool, error) {
	if m.lockStatusFunc != nil {
		return m.lockStatusFunc(ctx, name)
	}
	return LockStatus{}, false, nil
}
func (m *mockStore) LockAcquire(ctx context.Context, key, lockedBy string, timeout time.Duration) (bool, error) {
	if m.lockAcquireFunc != nil {
		return m.lockAcquireFunc(ctx, key, lockedBy, timeout)
	}
	return true, nil
}
func (m *mockStore) LockRelease(ctx context.Context, key, lockedBy string) error {
	if m.lockReleaseFunc != nil {
		return m.lockReleaseFunc(ctx, key, lockedBy)
	}
	return nil
}
func (m *mockStore) Stream(ctx context.Context, t Type, seq, limit int) ([]StreamRecord, error) {
	if m.streamFunc != nil {
		return m.streamFunc(ctx, t, seq, limit)
	}
	return []StreamRecord{}, nil
}
func (m *mockStore) Put(ctx context.Context, key string, version int, value any) error {
	if m.putFunc != nil {
		return m.putFunc(ctx, key, version, value)
	}
	return nil
}
func (m *mockStore) Get(ctx context.Context, key string, value any) (Record, bool, error) {
	if m.getFunc != nil {
		return m.getFunc(ctx, key, value)
	}
	return Record{}, false, nil
}
func (m *mockStore) Delete(ctx context.Context, keys ...string) (int, error) {
	if m.deleteFunc != nil {
		return m.deleteFunc(ctx, keys...)
	}
	return 0, nil
}
func (m *mockStore) Count(ctx context.Context) (int, error) {
	if m.countFunc != nil {
		return m.countFunc(ctx)
	}
	return 0, nil
}
func (m *mockStore) CountPrefix(ctx context.Context, prefix string) (int, error) {
	if m.countPrefixFunc != nil {
		return m.countPrefixFunc(ctx, prefix)
	}
	return 0, nil
}
func (m *mockStore) CountRange(ctx context.Context, from, to string) (int, error) {
	if m.countRangeFunc != nil {
		return m.countRangeFunc(ctx, from, to)
	}
	return 0, nil
}
func (m *mockStore) GetPrefix(ctx context.Context, prefix string, offset, limit int) ([]Record, error) {
	if m.getPrefixFunc != nil {
		return m.getPrefixFunc(ctx, prefix, offset, limit)
	}
	return nil, nil
}
func (m *mockStore) GetRange(ctx context.Context, from, to string, offset, limit int) ([]Record, error) {
	if m.getRangeFunc != nil {
		return m.getRangeFunc(ctx, from, to, offset, limit)
	}
	return nil, nil
}
func (m *mockStore) GetType(ctx context.Context, t Type, offset, limit int) ([]Record, error) {
	if m.getTypeFunc != nil {
		return m.getTypeFunc(ctx, t, offset, limit)
	}
	return nil, nil
}
func (m *mockStore) List(ctx context.Context, start, limit int) ([]Record, error) {
	if m.listFunc != nil {
		return m.listFunc(ctx, start, limit)
	}
	return nil, nil
}
func (m *mockStore) DeletePrefix(ctx context.Context, prefix string, offset, limit int) (int, error) {
	if m.deletePrefixFunc != nil {
		return m.deletePrefixFunc(ctx, prefix, offset, limit)
	}
	return 0, nil
}
func (m *mockStore) DeleteRange(ctx context.Context, from, to string, offset, limit int) (int, error) {
	if m.deleteRangeFunc != nil {
		return m.deleteRangeFunc(ctx, from, to, offset, limit)
	}
	return 0, nil
}
func (m *mockStore) Patch(ctx context.Context, key string, version int, patch any) error {
	if m.patchFunc != nil {
		return m.patchFunc(ctx, key, version, patch)
	}
	return nil
}
func (m *mockStore) MutateAll(ctx context.Context, mutations ...Mutation) ([]int, error) {
	if m.mutateAllFunc != nil {
		return m.mutateAllFunc(ctx, mutations...)
	}
	return nil, nil
}
func (m *mockStore) StreamSeq(ctx context.Context) (int, error) {
	if m.streamSeqFunc != nil {
		return m.streamSeqFunc(ctx)
	}
	return 0, nil
}
func (m *mockStore) StreamTrim(ctx context.Context, seq int) error {
	if m.streamTrimFunc != nil {
		return m.streamTrimFunc(ctx, seq)
	}
	return nil
}
func (m *mockStore) SetNow(now func() time.Time) {
	if m.setNowFunc != nil {
		m.setNowFunc(now)
	}
}
func (m *mockStore) TaskCreate(ctx context.Context, task Task) error {
	return nil
}
func (m *mockStore) TaskGet(ctx context.Context, id string) (task Task, ok bool, err error) {
	return Task{}, false, nil
}
func (m *mockStore) TaskList(ctx context.Context, status TaskStatus, name string, offset, limit int) ([]Task, error) {
	return nil, nil
}
func (m *mockStore) TaskUpdate(ctx context.Context, task Task) error {
	return nil
}
func (m *mockStore) TaskDelete(ctx context.Context, id string) error {
	return nil
}
func (m *mockStore) TaskGetNextPending(ctx context.Context, runnerID string, lockDuration time.Duration, taskTypes ...string) (task Task, locked bool, err error) {
	return Task{}, false, nil
}
func (m *mockStore) TaskReleaseLock(ctx context.Context, id string, runnerID string) error {
	return nil
}

func TestStreamConsumer(t *testing.T) {
	t.Run("If no records are returned, a sleep is returned from Get", func(t *testing.T) {
		ctx := context.Background()

		store := &mockStore{
			lockAcquireFunc: func(ctx context.Context, key, lockedBy string, timeout time.Duration) (bool, error) {
				return true, nil
			},
			getFunc: func(ctx context.Context, key string, value any) (Record, bool, error) {
				return Record{}, false, nil
			},
			streamFunc: func(ctx context.Context, t Type, seq, limit int) ([]StreamRecord, error) {
				return []StreamRecord{}, nil
			},
		}

		consumer := NewStreamConsumer(ctx, store, "test-stream", "consumer1", TypeAll)
		consumer.MinBackoff = 50 * time.Millisecond

		records, waitFor, err := consumer.Get(ctx, 5*time.Minute, 10)
		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}
		if len(records) != 0 {
			t.Errorf("Expected empty records, got %d records", len(records))
		}
		if waitFor == 0 {
			t.Error("Expected non-zero waitFor when no records available")
		}
		if waitFor != consumer.MinBackoff {
			t.Errorf("Expected waitFor to be %v, got %v", consumer.MinBackoff, waitFor)
		}
	})
	t.Run("Messages are not returned if the lock cannot be acquired, and the waitFor value is not zero", func(t *testing.T) {
		ctx := context.Background()

		store := &mockStore{
			lockAcquireFunc: func(ctx context.Context, key, lockedBy string, timeout time.Duration) (bool, error) {
				// Lock not acquired.
				return false, nil
			},
			getFunc: func(ctx context.Context, key string, value any) (Record, bool, error) {
				return Record{}, false, nil
			},
			streamFunc: func(ctx context.Context, t Type, seq, limit int) ([]StreamRecord, error) {
				return []StreamRecord{createTestRecord(1, TypeAll, "key1", "value1")}, nil
			},
		}

		consumer := NewStreamConsumer(ctx, store, "test-stream", "consumer1", TypeAll)
		consumer.MinBackoff = 100 * time.Millisecond

		records, waitFor, err := consumer.Get(ctx, 5*time.Minute, 10)
		if err != nil {
			t.Fatalf("Expected no error when lock cannot be acquired, got: %v", err)
		}
		if len(records) != 0 {
			t.Errorf("Expected no records when lock cannot be acquired, got %d records", len(records))
		}
		if waitFor == 0 {
			t.Error("Expected non-zero waitFor when lock cannot be acquired")
		}
		if waitFor != consumer.MinBackoff {
			t.Errorf("Expected waitFor to be %v, got %v", consumer.MinBackoff, waitFor)
		}
	})
	t.Run("Messages are returned if the lock is acquired", func(t *testing.T) {
		ctx := context.Background()

		var commitCalled bool
		var commitSeq int
		store := &mockStore{
			lockAcquireFunc: func(ctx context.Context, key, lockedBy string, timeout time.Duration) (bool, error) {
				// Lock acquired.
				return true, nil
			},
			getFunc: func(ctx context.Context, key string, value any) (Record, bool, error) {
				// No existing consumer state.
				return Record{}, false, nil
			},
			streamFunc: func(ctx context.Context, t Type, seq, limit int) ([]StreamRecord, error) {
				return []StreamRecord{
					createTestRecord(1, TypeAll, "key1", "value1"),
					createTestRecord(2, TypeAll, "key2", "value2"),
				}, nil
			},
			putFunc: func(ctx context.Context, key string, version int, value any) error {
				commitCalled = true
				if status, ok := value.(StreamConsumerStatus); ok {
					commitSeq = status.Seq
				}
				return nil
			},
		}

		consumer := NewStreamConsumer(ctx, store, "test-stream", "consumer1", TypeAll)

		records, waitFor, err := consumer.Get(ctx, 5*time.Minute, 10)
		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}
		if len(records) != 2 {
			t.Errorf("Expected 2 records, got %d", len(records))
		}
		if waitFor != 0 {
			t.Errorf("Expected waitFor to be 0 when records are returned, got %v", waitFor)
		}

		// Test commit.
		err = consumer.Commit(ctx, records)
		if err != nil {
			t.Fatalf("Expected no error during commit, got: %v", err)
		}
		if !commitCalled {
			t.Error("Expected commit to be called")
		}
		// Seq should be max(record.Seq) + 1.
		if commitSeq != 3 {
			t.Errorf("Expected commit seq to be 3, got %d", commitSeq)
		}
	})
	t.Run("If there's an error getting the lock, it's returned by Get", func(t *testing.T) {
		ctx := context.Background()
		expectedErr := errors.New("lock acquisition failed")

		store := &mockStore{
			lockAcquireFunc: func(ctx context.Context, key, lockedBy string, timeout time.Duration) (bool, error) {
				return false, expectedErr
			},
			getFunc: func(ctx context.Context, key string, value any) (Record, bool, error) {
				return Record{}, false, nil
			},
		}

		consumer := NewStreamConsumer(ctx, store, "test-stream", "consumer1", TypeAll)

		_, _, err := consumer.Get(ctx, 5*time.Minute, 10)
		if err == nil {
			t.Fatal("Expected error when lock acquisition fails")
		}
		if !errors.Is(err, expectedErr) {
			t.Errorf("Expected error to contain %v, got %v", expectedErr, err)
		}
	})
	t.Run("Consumer state is persisted and reloaded correctly", func(t *testing.T) {
		ctx := context.Background()

		// Simulate persisted state.
		var consumerState map[string]StreamConsumerStatus = make(map[string]StreamConsumerStatus)
		var consumerVersion int

		store := &mockStore{
			lockAcquireFunc: func(ctx context.Context, key, lockedBy string, timeout time.Duration) (bool, error) {
				return true, nil
			},
			getFunc: func(ctx context.Context, key string, value any) (Record, bool, error) {
				if state, exists := consumerState[key]; exists {
					if cs, ok := value.(*StreamConsumerStatus); ok {
						*cs = state
						return Record{Version: consumerVersion}, true, nil
					}
				}
				return Record{}, false, nil
			},
			streamFunc: func(ctx context.Context, t Type, seq, limit int) ([]StreamRecord, error) {
				if seq <= 2 {
					return []StreamRecord{
						createTestRecord(seq+1, TypeAll, "key1", "value1"),
						createTestRecord(seq+2, TypeAll, "key2", "value2"),
					}, nil
				}
				return []StreamRecord{}, nil
			},
			putFunc: func(ctx context.Context, key string, version int, value any) error {
				if status, ok := value.(StreamConsumerStatus); ok {
					consumerState[key] = status
					consumerVersion = version + 1
				}
				return nil
			},
		}

		// First consumer.
		consumer1 := NewStreamConsumer(ctx, store, "test-stream", "consumer1", TypeAll)

		records1, _, err := consumer1.Get(ctx, 5*time.Minute, 10)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if len(records1) != 2 {
			t.Errorf("Expected 2 records, got %d", len(records1))
		}

		// Commit first batch.
		err = consumer1.Commit(ctx, records1)
		if err != nil {
			t.Fatalf("Unexpected error during commit: %v", err)
		}

		// Second consumer should start from where first left off.
		consumer2 := NewStreamConsumer(ctx, store, "test-stream", "consumer1", TypeAll)

		records2, _, err := consumer2.Get(ctx, 5*time.Minute, 10)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		// Should get records starting from seq 3 (after committed seq 2).
		if len(records2) > 0 && records2[0].Seq <= 2 {
			t.Errorf("Second consumer should not get already processed records, got seq %d", records2[0].Seq)
		}
	})
	t.Run("Consumer streams can be filtered by types", func(t *testing.T) {
		ctx := context.Background()

		var requestedType Type
		store := &mockStore{
			lockAcquireFunc: func(ctx context.Context, key, lockedBy string, timeout time.Duration) (bool, error) {
				return true, nil
			},
			getFunc: func(ctx context.Context, key string, value any) (Record, bool, error) {
				return Record{}, false, nil
			},
			streamFunc: func(ctx context.Context, t Type, seq, limit int) ([]StreamRecord, error) {
				requestedType = t
				if t == "Weapon" {
					return []StreamRecord{createTestRecord(1, "Weapon", "sword", "sharp")}, nil
				}
				return []StreamRecord{}, nil
			},
		}

		consumer := NewStreamConsumer(ctx, store, "test-stream", "consumer1", "Weapon")

		records, _, err := consumer.Get(ctx, 5*time.Minute, 10)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if requestedType != "Weapon" {
			t.Errorf("Expected type filter 'Weapon', got %s", requestedType)
		}
		if len(records) != 1 {
			t.Errorf("Expected 1 record, got %d", len(records))
		}
		if len(records) > 0 && records[0].Record.Type != "Weapon" {
			t.Errorf("Expected record type 'Weapon', got %s", records[0].Record.Type)
		}
	})
	t.Run("Consumer streams can be all types", func(t *testing.T) {
		ctx := context.Background()

		var requestedType Type
		store := &mockStore{
			lockAcquireFunc: func(ctx context.Context, key, lockedBy string, timeout time.Duration) (bool, error) {
				return true, nil
			},
			getFunc: func(ctx context.Context, key string, value any) (Record, bool, error) {
				return Record{}, false, nil
			},
			streamFunc: func(ctx context.Context, t Type, seq, limit int) ([]StreamRecord, error) {
				requestedType = t
				return []StreamRecord{
					createTestRecord(1, "Weapon", "sword", "sharp"),
					createTestRecord(2, "Armor", "shield", "strong"),
				}, nil
			},
		}

		consumer := NewStreamConsumer(ctx, store, "test-stream", "consumer1", TypeAll)

		records, _, err := consumer.Get(ctx, 5*time.Minute, 10)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if requestedType != TypeAll {
			t.Errorf("Expected type filter TypeAll, got %s", requestedType)
		}
		if len(records) != 2 {
			t.Errorf("Expected 2 records, got %d", len(records))
		}
	})

	t.Run("Error is returned if stream fetch fails", func(t *testing.T) {
		ctx := context.Background()
		expectedErr := errors.New("stream fetch failed")

		store := &mockStore{
			lockAcquireFunc: func(ctx context.Context, key, lockedBy string, timeout time.Duration) (bool, error) {
				return true, nil
			},
			getFunc: func(ctx context.Context, key string, value any) (Record, bool, error) {
				return Record{}, false, nil
			},
			streamFunc: func(ctx context.Context, t Type, seq, limit int) ([]StreamRecord, error) {
				return nil, expectedErr
			},
		}

		consumer := NewStreamConsumer(ctx, store, "test-stream", "consumer1", TypeAll)

		_, _, err := consumer.Get(ctx, 5*time.Minute, 10)
		if err == nil {
			t.Fatal("Expected error when stream fetch fails")
		}
		if !errors.Is(err, expectedErr) {
			t.Errorf("Expected error to contain %v, got %v", expectedErr, err)
		}
	})
	t.Run("Error is returned if consumer state loading fails", func(t *testing.T) {
		ctx := context.Background()
		expectedErr := errors.New("consumer state loading failed")

		store := &mockStore{
			lockAcquireFunc: func(ctx context.Context, key, lockedBy string, timeout time.Duration) (bool, error) {
				return true, nil
			},
			getFunc: func(ctx context.Context, key string, value any) (Record, bool, error) {
				return Record{}, false, expectedErr
			},
		}

		consumer := NewStreamConsumer(ctx, store, "test-stream", "consumer1", TypeAll)

		_, _, err := consumer.Get(ctx, 5*time.Minute, 10)
		if err == nil {
			t.Fatal("Expected error when consumer state loading fails")
		}
		if !errors.Is(err, expectedErr) {
			t.Errorf("Expected error to contain %v, got %v", expectedErr, err)
		}
	})
}
