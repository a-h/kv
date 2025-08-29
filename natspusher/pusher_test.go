package natspusher

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/a-h/kv"
	"github.com/a-h/kv/sqlitekv"
	"github.com/nats-io/nats.go"
	"zombiezen.com/go/sqlite/sqlitex"
)

// mockPublisher is a mock implementation of the Publisher interface for testing.
type mockPublisher struct {
	messages []*nats.Msg
	err      error
}

func (m *mockPublisher) PublishMsg(msg *nats.Msg) error {
	if m.err != nil {
		return m.err
	}
	m.messages = append(m.messages, msg)
	return nil
}

// createTestStore creates an in-memory SQLite store for testing.
func createTestStore(t *testing.T) kv.Store {
	t.Helper()
	pool, err := sqlitex.NewPool("file::memory:?mode=memory&cache=shared", sqlitex.PoolOptions{})
	if err != nil {
		t.Fatalf("failed to create SQLite pool: %v", err)
	}
	t.Cleanup(func() { pool.Close() })

	store := sqlitekv.NewStore(pool)
	if err := store.Init(context.Background()); err != nil {
		t.Fatalf("failed to initialize store: %v", err)
	}
	return store
}

func TestNew(t *testing.T) {
	store := createTestStore(t)
	nc := &nats.Conn{}
	consumer := kv.NewStreamConsumer(context.Background(), store, "test-stream", "test-consumer", kv.TypeAll)
	config := Config{}

	pusher, err := New(nc, consumer, config)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if pusher.config.MaxRetries != 3 {
		t.Errorf("expected default MaxRetries=3, got %d", pusher.config.MaxRetries)
	}
}

func TestNewWithNilConnection(t *testing.T) {
	store := createTestStore(t)
	consumer := kv.NewStreamConsumer(context.Background(), store, "test-stream", "test-consumer", kv.TypeAll)
	config := Config{}

	_, err := New(nil, consumer, config)
	if err == nil {
		t.Fatal("expected error when NATS connection is nil")
	}
	if err.Error() != "NATS connection cannot be nil" {
		t.Errorf("unexpected error message: %s", err.Error())
	}
}

func TestNewWithNilConsumer(t *testing.T) {
	nc := &nats.Conn{}
	config := Config{}

	_, err := New(nc, nil, config)
	if err == nil {
		t.Fatal("expected error when consumer is nil")
	}
	if err.Error() != "stream consumer cannot be nil" {
		t.Errorf("unexpected error message: %s", err.Error())
	}
}

func TestBuildSubject(t *testing.T) {
	testCases := []struct {
		name       string
		prefix     string
		key        string
		recordType string
		expected   string
	}{
		// Tests with prefix
		{
			name:       "simple key with prefix",
			prefix:     "kv",
			key:        "user123",
			recordType: "User",
			expected:   "kv.user123.User",
		},
		{
			name:       "key with slashes with prefix",
			prefix:     "kv",
			key:        "users/123/profile",
			recordType: "Profile",
			expected:   "kv.users.123.profile.Profile",
		},
		{
			name:       "key with colons with prefix",
			prefix:     "kv",
			key:        "session:abc123",
			recordType: "Session",
			expected:   "kv.session.abc123.Session",
		},
		{
			name:       "key with underscores with prefix",
			prefix:     "kv",
			key:        "cache_key_test",
			recordType: "Cache",
			expected:   "kv.cache.key.test.Cache",
		},
		{
			name:       "key with mixed separators with prefix",
			prefix:     "kv",
			key:        "app/users:123_profile",
			recordType: "UserProfile",
			expected:   "kv.app.users.123.profile.UserProfile",
		},
		{
			name:       "empty key with prefix",
			prefix:     "kv",
			key:        "",
			recordType: "Event",
			expected:   "kv.Event",
		},
		// Tests without prefix
		{
			name:       "simple key without prefix",
			prefix:     "",
			key:        "user123",
			recordType: "User",
			expected:   "user123.User",
		},
		{
			name:       "key with slashes without prefix",
			prefix:     "",
			key:        "users/123/profile",
			recordType: "Profile",
			expected:   "users.123.profile.Profile",
		},
		{
			name:       "key with colons without prefix",
			prefix:     "",
			key:        "session:abc123",
			recordType: "Session",
			expected:   "session.abc123.Session",
		},
		{
			name:       "key with underscores without prefix",
			prefix:     "",
			key:        "cache_key_test",
			recordType: "Cache",
			expected:   "cache.key.test.Cache",
		},
		{
			name:       "key with mixed separators without prefix",
			prefix:     "",
			key:        "app/users:123_profile",
			recordType: "UserProfile",
			expected:   "app.users.123.profile.UserProfile",
		},
		{
			name:       "empty key without prefix",
			prefix:     "",
			key:        "",
			recordType: "Event",
			expected:   "Event",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			config := Config{SubjectPrefix: tc.prefix}
			pusher := &Pusher{config: config}

			result := pusher.buildSubject(tc.key, tc.recordType)
			if result != tc.expected {
				t.Errorf("buildSubject(%q, %q) = %q, want %q", tc.key, tc.recordType, result, tc.expected)
			}
		})
	}
}

func TestPublishRecord(t *testing.T) {
	mock := &mockPublisher{}
	config := Config{SubjectPrefix: "test"}
	pusher := &Pusher{config: config, publisher: mock}

	record := kv.StreamRecord{
		Seq:    42,
		Action: kv.ActionCreate,
		Record: kv.Record{
			Key:     "test:key",
			Version: 1,
			Type:    "TestType",
			Value:   []byte(`{"field":"value"}`),
			Created: time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC),
		},
	}

	ctx := context.Background()
	err := pusher.publishRecord(ctx, record)
	if err != nil {
		t.Fatalf("publishRecord failed: %v", err)
	}

	if len(mock.messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(mock.messages))
	}

	msg := mock.messages[0]
	expectedSubject := "test.test.key.TestType"
	if msg.Subject != expectedSubject {
		t.Errorf("expected subject %q, got %q", expectedSubject, msg.Subject)
	}

	expectedData := `{"field":"value"}`
	if string(msg.Data) != expectedData {
		t.Errorf("expected data %q, got %q", expectedData, string(msg.Data))
	}

	if msg.Header.Get("kv-seq") != "42" {
		t.Errorf("expected kv-seq header '42', got %q", msg.Header.Get("kv-seq"))
	}
	if msg.Header.Get("kv-action") != "create" {
		t.Errorf("expected kv-action header 'create', got %q", msg.Header.Get("kv-action"))
	}
	if msg.Header.Get("kv-type") != "TestType" {
		t.Errorf("expected kv-type header 'TestType', got %q", msg.Header.Get("kv-type"))
	}
	if msg.Header.Get("kv-key") != "test:key" {
		t.Errorf("expected kv-key header 'test:key', got %q", msg.Header.Get("kv-key"))
	}
	if msg.Header.Get("kv-version") != "1" {
		t.Errorf("expected kv-version header '1', got %q", msg.Header.Get("kv-version"))
	}
	if msg.Header.Get("kv-created") != "2025-01-01T12:00:00Z" {
		t.Errorf("expected kv-created header '2025-01-01T12:00:00Z', got %q", msg.Header.Get("kv-created"))
	}
}

func TestPublishRecordWithCustomHeaders(t *testing.T) {
	mock := &mockPublisher{}
	config := Config{
		SubjectPrefix: "test",
		Headers: nats.Header{
			"source": []string{"kv-store"},
			"env":    []string{"test"},
		},
	}
	pusher := &Pusher{config: config, publisher: mock}

	record := kv.StreamRecord{
		Seq:    1,
		Action: kv.ActionUpdate,
		Record: kv.Record{
			Key:     "user",
			Version: 2,
			Type:    "User",
			Value:   []byte(`{"name":"test"}`),
			Created: time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC),
		},
	}

	ctx := context.Background()
	err := pusher.publishRecord(ctx, record)
	if err != nil {
		t.Fatalf("publishRecord failed: %v", err)
	}

	if len(mock.messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(mock.messages))
	}

	msg := mock.messages[0]

	if msg.Header.Get("source") != "kv-store" {
		t.Errorf("expected source header 'kv-store', got %q", msg.Header.Get("source"))
	}
	if msg.Header.Get("env") != "test" {
		t.Errorf("expected env header 'test', got %q", msg.Header.Get("env"))
	}

	if msg.Header.Get("kv-action") != "update" {
		t.Errorf("expected kv-action header 'update', got %q", msg.Header.Get("kv-action"))
	}
}

func TestStreamConsumerWithStore(t *testing.T) {
	store := createTestStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	type TestEvent struct {
		Message string `json:"message"`
		Count   int    `json:"count"`
	}

	events := []TestEvent{
		{Message: "first", Count: 1},
		{Message: "second", Count: 2},
		{Message: "third", Count: 3},
	}

	for i, event := range events {
		key := "events/" + event.Message
		if err := store.Put(ctx, key, -1, event); err != nil {
			t.Fatalf("failed to put event %d: %v", i, err)
		}
	}

	consumer := kv.NewStreamConsumer(ctx, store, "test-stream", "test-consumer", kv.TypeAll)
	consumer.MinBackoff = 1 * time.Millisecond
	consumer.MaxBackoff = 10 * time.Millisecond

	var recordCount int
	for {
		records, waitFor, err := consumer.Get(ctx, 5*time.Minute, 20)
		if err != nil {
			if ctx.Err() != nil {
				// Context timeout/cancellation is fine for this test.
				break
			}
			t.Fatalf("unexpected error getting records: %v", err)
		}

		if len(records) == 0 {
			time.Sleep(waitFor)
			continue
		}

		for _, record := range records {
			recordCount++

			if record.Action != kv.ActionCreate {
				t.Errorf("expected action %s, got %s", kv.ActionCreate, record.Action)
			}
			if record.Record.Type != "TestEvent" {
				t.Errorf("expected type TestEvent, got %s", record.Record.Type)
			}

			var event TestEvent
			if err := json.Unmarshal(record.Record.Value, &event); err != nil {
				t.Errorf("failed to unmarshal event: %v", err)
			}
			if event.Count != recordCount {
				t.Errorf("expected count %d, got %d", recordCount, event.Count)
			}

			if recordCount >= len(events) {
				break
			}
		}

		if len(records) > 0 {
			if err := consumer.CommitUpTo(ctx, records[len(records)-1].Seq); err != nil {
				t.Fatalf("failed to commit records: %v", err)
			}
		}

		if recordCount >= len(events) {
			break
		}
	}

	if recordCount != len(events) {
		t.Errorf("expected to read %d records, got %d", len(events), recordCount)
	}
}
