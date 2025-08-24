package kv

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

func (m *mockStore) Init(ctx context.Context) error {
	return nil
}

type mockStore struct {
	lockAcquireFunc  func(ctx context.Context, key, lockedBy string, timeout time.Duration) (bool, error)
	lockReleaseFunc  func(ctx context.Context, key, lockedBy string) error
	streamFunc       func(ctx context.Context, t Type, seq, limit int) ([]StreamRecord, error)
	putFunc          func(ctx context.Context, key string, version int, value any) error
	getFunc          func(ctx context.Context, key string, value any) (Record, bool, error)
	deleteFunc       func(ctx context.Context, keys ...string) (int, error)
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

func (m *mockStore) LockStatus(ctx context.Context, name string) (LockStatus, bool, error) {
	if m.lockStatusFunc != nil {
		return m.lockStatusFunc(ctx, name)
	}
	return LockStatus{}, false, nil
}
func (m *mockStore) LockAcquire(ctx context.Context, key, lockedBy string, timeout time.Duration) (bool, error) {
	return m.lockAcquireFunc(ctx, key, lockedBy, timeout)
}
func (m *mockStore) LockRelease(ctx context.Context, key, lockedBy string) error {
	if m.lockReleaseFunc != nil {
		return m.lockReleaseFunc(ctx, key, lockedBy)
	}
	return nil
}
func (m *mockStore) Stream(ctx context.Context, t Type, seq, limit int) ([]StreamRecord, error) {
	return m.streamFunc(ctx, t, seq, limit)
}
func (m *mockStore) Put(ctx context.Context, key string, version int, value any) error {
	return m.putFunc(ctx, key, version, value)
}
func (m *mockStore) Get(ctx context.Context, key string, value any) (Record, bool, error) {
	return m.getFunc(ctx, key, value)
}
func (m *mockStore) Delete(ctx context.Context, keys ...string) (int, error) {
	return m.deleteFunc(ctx, keys...)
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

func TestStreamConsumer(t *testing.T) {
	t.Run("Error is returned if stream fetch fails", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		getErr := errors.New("get failed")
		store := &mockStore{
			lockAcquireFunc: func(ctx context.Context, key, lockedBy string, timeout time.Duration) (bool, error) {
				return true, nil
			},
			getFunc: func(ctx context.Context, key string, value any) (Record, bool, error) {
				return Record{Version: 1}, true, nil
			},
			streamFunc: func(ctx context.Context, t Type, seq, limit int) ([]StreamRecord, error) {
				return nil, getErr
			},
			putFunc: func(ctx context.Context, key string, version int, value any) error {
				return nil
			},
			deleteFunc: func(ctx context.Context, keys ...string) (int, error) {
				return 0, nil
			},
		}

		sr := NewStreamConsumer(ctx, store, "teststream", "consumer1", TypeAll)
		sr.MinBackoff = 1 * time.Millisecond
		sr.MaxBackoff = 2 * time.Millisecond

		var gotErr bool
		for _, err := range sr.Read(ctx) {
			if err != nil {
				gotErr = true
				if !errors.Is(err, getErr) {
					t.Errorf("expected error %v, got %v", getErr, err)
				}
				break
			}
		}
		if !gotErr {
			t.Error("expected error from stream fetch, got none")
		}
	})
	t.Run("Error is returned if consumer state loading fails", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		stateErr := errors.New("state loading failed")
		store := &mockStore{
			lockAcquireFunc: func(ctx context.Context, key, lockedBy string, timeout time.Duration) (bool, error) {
				return true, nil
			},
			getFunc: func(ctx context.Context, key string, value any) (Record, bool, error) {
				return Record{}, false, stateErr
			},
			streamFunc: func(ctx context.Context, t Type, seq, limit int) ([]StreamRecord, error) {
				return []StreamRecord{{Seq: 1}}, nil
			},
			putFunc: func(ctx context.Context, key string, version int, value any) error {
				return nil
			},
			deleteFunc: func(ctx context.Context, keys ...string) (int, error) {
				return 0, nil
			},
		}

		sr := NewStreamConsumer(ctx, store, "teststream", "consumer1", TypeAll)
		sr.MinBackoff = 1 * time.Millisecond
		sr.MaxBackoff = 2 * time.Millisecond

		var gotErr bool
		for _, err := range sr.Read(ctx) {
			if err != nil {
				gotErr = true
				if !errors.Is(err, stateErr) {
					t.Errorf("expected error %v, got %v", stateErr, err)
				}
				break
			}
		}
		if !gotErr {
			t.Error("expected error from consumer state loading, got none")
		}
	})
	t.Run("If no records are returned, code sleeps before retrying", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()

		var callCount int
		store := &mockStore{
			lockAcquireFunc: func(ctx context.Context, key, lockedBy string, timeout time.Duration) (bool, error) {
				return true, nil
			},
			getFunc: func(ctx context.Context, key string, value any) (Record, bool, error) {
				return Record{Version: 1}, true, nil
			},
			streamFunc: func(ctx context.Context, t Type, seq, limit int) ([]StreamRecord, error) {
				callCount++
				return nil, nil
			},
			putFunc: func(ctx context.Context, key string, version int, value any) error {
				return nil
			},
			deleteFunc: func(ctx context.Context, keys ...string) (int, error) {
				return 0, nil
			},
		}

		sr := NewStreamConsumer(ctx, store, "teststream", "consumer1", TypeAll)
		sr.MinBackoff = 1 * time.Millisecond
		sr.MaxBackoff = 2 * time.Millisecond

		for _, err := range sr.Read(ctx) {
			if err != nil {
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Errorf("expected context deadline exceeded, got %v", err)
				}
				break
			}
		}

		if callCount < 2 {
			t.Errorf("expected at least 2 calls to streamFunc, got %d", callCount)
		}
	})
	t.Run("If not enough time left in lock, lock is extended before yielding result", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		var lockExtendCalled bool
		store := &mockStore{
			lockAcquireFunc: func(ctx context.Context, key, lockedBy string, timeout time.Duration) (bool, error) {
				lockExtendCalled = true
				return true, nil
			},
			getFunc: func(ctx context.Context, key string, value any) (Record, bool, error) {
				return Record{Version: 1}, true, nil
			},
			streamFunc: func(ctx context.Context, t Type, seq, limit int) ([]StreamRecord, error) {
				return []StreamRecord{{Seq: 1}}, nil
			},
			putFunc: func(ctx context.Context, key string, version int, value any) error {
				return nil
			},
			deleteFunc: func(ctx context.Context, keys ...string) (int, error) {
				return 0, nil
			},
		}

		sr := NewStreamConsumer(ctx, store, "teststream", "consumer1", TypeAll)
		sr.MinBackoff = 1 * time.Millisecond
		sr.MaxBackoff = 2 * time.Millisecond
		sr.LockExtendThreshold = 10 * time.Second
		sr.Locker.LockedUntil = time.Now().Add(1 * time.Second)

		var got bool
		for _, err := range sr.Read(ctx) {
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			got = true
			break
		}
		if !got {
			t.Error("expected to get a record")
		}
		if !lockExtendCalled {
			t.Error("expected lock to be extended before yielding record")
		}
	})
	t.Run("Error during lock extension is propagated", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		extendErr := errors.New("lock extend failed")
		var lockCallCount int
		store := &mockStore{
			lockAcquireFunc: func(ctx context.Context, key, lockedBy string, timeout time.Duration) (bool, error) {
				lockCallCount++
				if lockCallCount == 1 {
					return true, nil
				}
				return false, extendErr
			},
			getFunc: func(ctx context.Context, key string, value any) (Record, bool, error) {
				return Record{Version: 1}, true, nil
			},
			streamFunc: func(ctx context.Context, t Type, seq, limit int) ([]StreamRecord, error) {
				return []StreamRecord{{Seq: 1}}, nil
			},
			putFunc: func(ctx context.Context, key string, version int, value any) error {
				return nil
			},
			deleteFunc: func(ctx context.Context, keys ...string) (int, error) {
				return 0, nil
			},
		}

		sr := NewStreamConsumer(ctx, store, "teststream", "consumer1", TypeAll)
		sr.MinBackoff = 1 * time.Millisecond
		sr.MaxBackoff = 2 * time.Millisecond
		sr.LockExtendThreshold = 10 * time.Second
		sr.CommitMode = CommitModeNone
		sr.Locker.LockedUntil = time.Now().Add(1 * time.Second)

		var gotErr bool
		for _, err := range sr.Read(ctx) {
			if err != nil {
				gotErr = true
				if !errors.Is(err, extendErr) {
					t.Errorf("expected error %v, got %v", extendErr, err)
				}
				break
			}
		}
		if !gotErr {
			t.Error("expected error from lock extension, got none")
		}
		if lockCallCount < 2 {
			t.Errorf("expected at least 2 lock calls (initial + extension), got %d", lockCallCount)
		}
	})
	t.Run("Lock extension failure (not acquired) returns error", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		var lockCallCount int
		store := &mockStore{
			lockAcquireFunc: func(ctx context.Context, key, lockedBy string, timeout time.Duration) (bool, error) {
				lockCallCount++
				if lockCallCount == 1 {
					return true, nil
				}
				return false, nil
			},
			getFunc: func(ctx context.Context, key string, value any) (Record, bool, error) {
				return Record{Version: 1}, true, nil
			},
			streamFunc: func(ctx context.Context, t Type, seq, limit int) ([]StreamRecord, error) {
				return []StreamRecord{{Seq: 1}}, nil
			},
			putFunc: func(ctx context.Context, key string, version int, value any) error {
				return nil
			},
			deleteFunc: func(ctx context.Context, keys ...string) (int, error) {
				return 0, nil
			},
		}

		sr := NewStreamConsumer(ctx, store, "teststream", "consumer1", TypeAll)
		sr.MinBackoff = 1 * time.Millisecond
		sr.MaxBackoff = 2 * time.Millisecond
		sr.LockExtendThreshold = 10 * time.Second
		sr.Locker.MsgTimeout = 1 * time.Second

		var gotErr bool
		for _, err := range sr.Read(ctx) {
			if err != nil {
				gotErr = true
				if err.Error() != "stream: failed to extend lock" {
					t.Errorf("expected 'stream: failed to extend lock' error, got %v", err)
				}
				break
			}
		}
		if !gotErr {
			t.Error("expected error from lock extension failure, got none")
		}
		if lockCallCount < 2 {
			t.Errorf("expected at least 2 lock calls (initial + extension attempt), got %d", lockCallCount)
		}
	})
	t.Run("For loop can be terminated early by breaking", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		var callCount int
		store := &mockStore{
			lockAcquireFunc: func(ctx context.Context, key, lockedBy string, timeout time.Duration) (bool, error) {
				return true, nil
			},
			getFunc: func(ctx context.Context, key string, value any) (Record, bool, error) {
				return Record{Version: 1}, true, nil
			},
			streamFunc: func(ctx context.Context, t Type, seq, limit int) ([]StreamRecord, error) {
				callCount++
				if callCount == 1 {
					return []StreamRecord{{Seq: 1}}, nil
				}
				return []StreamRecord{}, nil
			},
			putFunc: func(ctx context.Context, key string, version int, value any) error {
				return nil
			},
			deleteFunc: func(ctx context.Context, keys ...string) (int, error) {
				return 0, nil
			},
		}

		sr := NewStreamConsumer(ctx, store, "teststream", "consumer1", TypeAll)
		sr.MinBackoff = 1 * time.Millisecond
		sr.MaxBackoff = 2 * time.Millisecond

		var count int
		for _, err := range sr.Read(ctx) {
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			count++
			if count >= 1 {
				break
			}
		}
		if count != 1 {
			t.Errorf("expected to process 1 record, got %d", count)
		}
	})
	t.Run("Messages are not returned if the lock cannot be acquired", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		store := &mockStore{
			lockAcquireFunc: func(ctx context.Context, key, lockedBy string, timeout time.Duration) (bool, error) {
				return false, nil
			},
			getFunc: func(ctx context.Context, key string, value any) (Record, bool, error) {
				return Record{Version: 1}, true, nil
			},
			streamFunc: func(ctx context.Context, t Type, seq, limit int) ([]StreamRecord, error) {
				return []StreamRecord{{Seq: 1}}, nil
			},
			putFunc: func(ctx context.Context, key string, version int, value any) error {
				return nil
			},
			deleteFunc: func(ctx context.Context, keys ...string) (int, error) {
				return 0, nil
			},
		}

		sr := NewStreamConsumer(ctx, store, "teststream", "consumer1", TypeAll)
		sr.MinBackoff = 10 * time.Millisecond
		sr.MaxBackoff = 20 * time.Millisecond

		ch := make(chan StreamRecord, 1)
		go func() {
			for rec, err := range sr.Read(ctx) {
				if err != nil {
					break
				}
				ch <- rec
			}
			close(ch)
		}()

		select {
		case <-ch:
			t.Error("expected no messages, but got one")
		case <-time.After(50 * time.Millisecond):
		}
	})
	t.Run("Messages are returned if the lock is acquired", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		var putCalled bool
		var putKey string
		var putVersion int
		var putValue any
		lastSeq := 0

		store := &mockStore{
			lockAcquireFunc: func(ctx context.Context, key, lockedBy string, timeout time.Duration) (bool, error) {
				return true, nil
			},
			getFunc: func(ctx context.Context, key string, value any) (Record, bool, error) {
				return Record{}, false, nil
			},
			streamFunc: func(ctx context.Context, t Type, seq, limit int) ([]StreamRecord, error) {
				if seq <= lastSeq {
					return []StreamRecord{{Seq: 1}}, nil
				}
				return nil, nil
			},
			putFunc: func(ctx context.Context, key string, version int, value any) error {
				putCalled = true
				putKey = key
				putVersion = version
				putValue = value
				if status, ok := value.(StreamConsumerStatus); ok {
					lastSeq = status.Seq - 1
				}
				return nil
			},
			deleteFunc: func(ctx context.Context, keys ...string) (int, error) {
				return 0, nil
			},
		}

		streamName := "teststream-messages-returned"
		sr := NewStreamConsumer(ctx, store, streamName, "consumer1", TypeAll)
		sr.MinBackoff = 1 * time.Millisecond
		sr.MaxBackoff = 2 * time.Millisecond

		ch := make(chan StreamRecord, 1)
		var once sync.Once
		go func() {
			for rec, err := range sr.Read(ctx) {
				if err != nil {
					break
				}
				once.Do(func() { ch <- rec })
			}
			close(ch)
		}()

		select {
		case rec := <-ch:
			if rec.Seq != 1 {
				t.Errorf("expected Seq=1, got %v", rec.Seq)
			}
		case <-time.After(50 * time.Millisecond):
			t.Error("expected a message, but got none")
		}

		time.Sleep(10 * time.Millisecond)

		if !putCalled {
			t.Error("expected commit (putFunc) to be called, but it wasn't")
		}
		expectedKey := "github.com/a-h/kv/stream/" + streamName
		if putKey != expectedKey {
			t.Errorf("expected putKey to be %s, got %s", expectedKey, putKey)
		}
		if putVersion != 0 {
			t.Errorf("expected putVersion to be 0, got %d", putVersion)
		}

		status, ok := putValue.(StreamConsumerStatus)
		if !ok {
			t.Fatalf("expected putValue to be StreamConsumerStatus, got %T", putValue)
		}
		if status.Name != streamName {
			t.Errorf("expected status.Name to be %s, got %s", streamName, status.Name)
		}
		if status.Seq != 2 {
			t.Errorf("expected status.Seq to be 2, got %d", status.Seq)
		}
	})
	t.Run("Commit modes work correctly", func(t *testing.T) {
		testCases := []struct {
			name            string
			commitMode      CommitMode
			recordCount     int
			expectedCommits int
		}{
			{"none mode never commits", CommitModeNone, 3, 0},
			{"batch mode commits once per batch", CommitModeBatch, 3, 1},
			{"all mode commits for each record", CommitModeAll, 3, 3},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()

				var commitCount int
				var records []StreamRecord
				for i := 0; i < tc.recordCount; i++ {
					records = append(records, StreamRecord{Seq: i + 1})
				}
				lastSeq := 0

				store := &mockStore{
					lockAcquireFunc: func(ctx context.Context, key, lockedBy string, timeout time.Duration) (bool, error) {
						return true, nil
					},
					getFunc: func(ctx context.Context, key string, value any) (Record, bool, error) {
						return Record{}, false, nil
					},
					streamFunc: func(ctx context.Context, t Type, seq, limit int) ([]StreamRecord, error) {
						if seq <= lastSeq {
							var result []StreamRecord
							for _, r := range records {
								if r.Seq > lastSeq {
									result = append(result, r)
								}
							}
							return result, nil
						}
						return nil, nil
					},
					putFunc: func(ctx context.Context, key string, version int, value any) error {
						commitCount++
						if status, ok := value.(StreamConsumerStatus); ok {
							lastSeq = status.Seq - 1
						}
						return nil
					},
					deleteFunc: func(ctx context.Context, keys ...string) (int, error) {
						return 0, nil
					},
				}

				streamName := "teststream-" + strings.ReplaceAll(tc.name, " ", "-")
				sr := NewStreamConsumer(ctx, store, streamName, "consumer1", TypeAll)
				sr.CommitMode = tc.commitMode
				sr.MinBackoff = 1 * time.Millisecond
				sr.MaxBackoff = 2 * time.Millisecond

				processedCount := 0
				iterCtx, iterCancel := context.WithTimeout(ctx, 200*time.Millisecond)
				defer iterCancel()

				for _, err := range sr.Read(iterCtx) {
					if err != nil {
						break
					}
					processedCount++
					if processedCount >= tc.recordCount {
						go func() {
							time.Sleep(20 * time.Millisecond)
							iterCancel()
						}()
					}
				}

				time.Sleep(50 * time.Millisecond)

				if commitCount != tc.expectedCommits {
					t.Errorf("expected %d commits, got %d", tc.expectedCommits, commitCount)
				}
			})
		}
	})
	t.Run("Error on lock acquire propagates", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		errTest := errors.New("lock error")
		store := &mockStore{
			lockAcquireFunc: func(ctx context.Context, key, lockedBy string, timeout time.Duration) (bool, error) {
				return false, errTest
			},
			getFunc: func(ctx context.Context, key string, value any) (Record, bool, error) {
				return Record{Version: 1}, true, nil
			},
			streamFunc: func(ctx context.Context, t Type, seq, limit int) ([]StreamRecord, error) {
				return nil, nil
			},
			putFunc: func(ctx context.Context, key string, version int, value any) error {
				return nil
			},
			deleteFunc: func(ctx context.Context, keys ...string) (int, error) {
				return 0, nil
			},
		}

		sr := NewStreamConsumer(ctx, store, "teststream", "consumer1", TypeAll)
		sr.MinBackoff = 1 * time.Millisecond
		sr.MaxBackoff = 2 * time.Millisecond

		ch := make(chan error, 1)
		go func() {
			for _, err := range sr.Read(ctx) {
				if err != nil {
					ch <- err
					return
				}
			}
			close(ch)
		}()

		select {
		case e := <-ch:
			if !errors.Is(e, errTest) {
				t.Errorf("expected error %v, got %v", errTest, e)
			}
		case <-time.After(50 * time.Millisecond):
			t.Error("expected error, but got none")
		}
	})
	t.Run("Consumer state is persisted and reloaded correctly", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		consumerState := map[string]any{}
		var consumerVersion int

		store := &mockStore{
			lockAcquireFunc: func(ctx context.Context, key, lockedBy string, timeout time.Duration) (bool, error) {
				return true, nil
			},
			getFunc: func(ctx context.Context, key string, value any) (Record, bool, error) {
				if storedValue, exists := consumerState[key]; exists {
					if cs, ok := value.(*StreamConsumerStatus); ok {
						if stored, ok := storedValue.(StreamConsumerStatus); ok {
							*cs = stored
							return Record{Version: consumerVersion}, true, nil
						}
					}
				}
				return Record{Version: 0}, false, nil
			},
			streamFunc: func(ctx context.Context, t Type, seq, limit int) ([]StreamRecord, error) {
				if seq == 0 {
					return []StreamRecord{{Seq: 1}, {Seq: 2}}, nil
				}
				if seq == 3 {
					return []StreamRecord{{Seq: 3}, {Seq: 4}}, nil
				}
				return []StreamRecord{}, nil
			},
			putFunc: func(ctx context.Context, key string, version int, value any) error {
				consumerState[key] = value
				consumerVersion = version + 1
				return nil
			},
			deleteFunc: func(ctx context.Context, keys ...string) (int, error) {
				return 0, nil
			},
		}

		sr1 := NewStreamConsumer(ctx, store, "teststream", "consumer1", TypeAll)
		sr1.MinBackoff = 1 * time.Millisecond
		sr1.MaxBackoff = 2 * time.Millisecond

		var recordCount int
		for record, err := range sr1.Read(ctx) {
			if err != nil {
				t.Fatalf("unexpected error reading: %v", err)
			}
			recordCount++
			if recordCount >= 2 {
				if err := sr1.CommitUpTo(ctx, record.Seq); err != nil {
					t.Fatalf("failed to commit: %v", err)
				}
				break
			}
		}

		if recordCount != 2 {
			t.Errorf("expected to read 2 records, got %d", recordCount)
		}

		sr2 := NewStreamConsumer(ctx, store, "teststream", "consumer1", TypeAll)
		sr2.MinBackoff = 1 * time.Millisecond
		sr2.MaxBackoff = 2 * time.Millisecond

		var nextRecordCount int
		for record, err := range sr2.Read(ctx) {
			if err != nil {
				if errors.Is(err, context.DeadlineExceeded) {
					break
				}
				t.Fatalf("unexpected error reading from second reader: %v", err)
			}
			nextRecordCount++
			if record.Seq <= 2 {
				t.Errorf("second reader got already processed record with seq %d", record.Seq)
			}
			if nextRecordCount >= 2 {
				break
			}
		}

		if nextRecordCount < 1 {
			t.Error("second reader should have read new records, not reprocessed old ones")
		}
	})
	t.Run("Lock is automatically released when iterator exits", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		var lockReleased bool
		store := &mockStore{
			lockAcquireFunc: func(ctx context.Context, key, lockedBy string, timeout time.Duration) (bool, error) {
				return true, nil
			},
			lockReleaseFunc: func(ctx context.Context, key, lockedBy string) error {
				lockReleased = true
				return nil
			},
			getFunc: func(ctx context.Context, key string, value any) (Record, bool, error) {
				return Record{Version: 1}, true, nil
			},
			streamFunc: func(ctx context.Context, t Type, seq, limit int) ([]StreamRecord, error) {
				return []StreamRecord{{Seq: 1}}, nil
			},
			putFunc: func(ctx context.Context, key string, version int, value any) error {
				return nil
			},
			deleteFunc: func(ctx context.Context, keys ...string) (int, error) {
				return 0, nil
			},
		}

		sr := NewStreamConsumer(ctx, store, "teststream", "consumer1", TypeAll)
		sr.MinBackoff = 1 * time.Millisecond
		sr.MaxBackoff = 2 * time.Millisecond

		for _, err := range sr.Read(ctx) {
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			break
		}

		if !lockReleased {
			t.Error("expected lock to be automatically released when iterator exits")
		}
	})
	t.Run("Streams can be filtered by types", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var requestedTypes []Type
		store := &mockStore{
			lockAcquireFunc: func(ctx context.Context, key, lockedBy string, timeout time.Duration) (bool, error) {
				return true, nil
			},
			getFunc: func(ctx context.Context, key string, value any) (Record, bool, error) {
				return Record{}, false, nil
			},
			streamFunc: func(ctx context.Context, t Type, seq, limit int) ([]StreamRecord, error) {
				requestedTypes = append(requestedTypes, t)
				if t == "Weapon" {
					return []StreamRecord{{Seq: 1, Record: Record{Type: "Weapon"}}}, nil
				}
				return []StreamRecord{}, nil
			},
			putFunc: func(ctx context.Context, key string, version int, value any) error {
				return nil
			},
			deleteFunc: func(ctx context.Context, keys ...string) (int, error) {
				return 0, nil
			},
		}

		sr := NewStreamConsumer(ctx, store, "teststream", "consumer1", "Weapon")
		sr.MinBackoff = 1 * time.Millisecond
		sr.MaxBackoff = 2 * time.Millisecond

		var receivedRecord bool
		iterCtx, iterCancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer iterCancel()

		for record, err := range sr.Read(iterCtx) {
			if err != nil {
				break
			}
			if record.Record.Type != "Weapon" {
				t.Errorf("expected record type 'Weapon', got %s", record.Record.Type)
			}
			receivedRecord = true
			break
		}

		if !receivedRecord {
			t.Error("expected to receive a record")
		}

		if len(requestedTypes) == 0 {
			t.Fatal("expected store.Stream to be called")
		}
		if requestedTypes[0] != "Weapon" {
			t.Errorf("expected first stream request with type 'Weapon', got %s", requestedTypes[0])
		}
	})
	t.Run("TypeAll behavior is preserved", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var requestedTypes []Type
		store := &mockStore{
			lockAcquireFunc: func(ctx context.Context, key, lockedBy string, timeout time.Duration) (bool, error) {
				return true, nil
			},
			getFunc: func(ctx context.Context, key string, value any) (Record, bool, error) {
				return Record{}, false, nil
			},
			streamFunc: func(ctx context.Context, t Type, seq, limit int) ([]StreamRecord, error) {
				requestedTypes = append(requestedTypes, t)
				return []StreamRecord{{Seq: 1, Record: Record{Type: "Any"}}}, nil
			},
			putFunc: func(ctx context.Context, key string, version int, value any) error {
				return nil
			},
			deleteFunc: func(ctx context.Context, keys ...string) (int, error) {
				return 0, nil
			},
		}

		sr := NewStreamConsumer(ctx, store, "teststream", "consumer1", TypeAll)
		sr.MinBackoff = 1 * time.Millisecond
		sr.MaxBackoff = 2 * time.Millisecond

		iterCtx, iterCancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer iterCancel()

		for _, err := range sr.Read(iterCtx) {
			if err != nil {
				break
			}
			break
		}

		if len(requestedTypes) == 0 {
			t.Fatal("expected store.Stream to be called")
		}
		if requestedTypes[0] != TypeAll {
			t.Errorf("expected first stream request with TypeAll, got %s", requestedTypes[0])
		}
	})
	t.Run("Administrative functions handle type correctly", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		consumerState := map[string]StreamConsumerStatus{
			"github.com/a-h/kv/stream/teststream": {
				Name:        "teststream",
				OfType:      "Position",
				Seq:         5,
				LastUpdated: time.Now().UTC().Format(time.RFC3339),
			},
		}

		var streamCallType Type
		store := &mockStore{
			getFunc: func(ctx context.Context, key string, value any) (Record, bool, error) {
				if cs, ok := value.(*StreamConsumerStatus); ok {
					if stored, exists := consumerState[key]; exists {
						*cs = stored
						return Record{Version: 1}, true, nil
					}
				}
				return Record{}, false, nil
			},
			streamFunc: func(ctx context.Context, t Type, seq, limit int) ([]StreamRecord, error) {
				streamCallType = t
				return []StreamRecord{{Seq: 5, Record: Record{Type: string(t)}}}, nil
			},
		}

		records, err := GetStreamConsumerRecords(ctx, store, "teststream", Type("Position"), 10)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(records) != 1 {
			t.Errorf("expected 1 record, got %d", len(records))
		}
		if streamCallType != "Position" {
			t.Errorf("expected stream call with type 'Position', got %s", streamCallType)
		}
	})
}
