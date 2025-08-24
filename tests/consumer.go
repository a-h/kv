package tests

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/a-h/kv"
)

func newConsumerTest(ctx context.Context, store kv.Store) func(t *testing.T) {
	return func(t *testing.T) {
		if err := store.StreamTrim(ctx, -1); err != nil {
			t.Fatalf("could not trim stream prior to running test: %v", err)
		}

		defer func() {
			if _, err := store.DeletePrefix(ctx, "*", 0, -1); err != nil {
				t.Logf("cleanup error: %v", err)
			}
		}()
		defer func() {
			if err := store.StreamTrim(ctx, -1); err != nil {
				t.Logf("cleanup error: %v", err)
			}
		}()

		expected := []Person{
			{
				Name:         "Frank",
				PhoneNumbers: []string{"678-901-2345"},
			},
			{
				Name:         "Eve",
				PhoneNumbers: []string{"567-890-1234"},
			},
			{
				Name:         "David",
				PhoneNumbers: []string{"456-789-0123"},
			},
			{
				Name:         "Charlie",
				PhoneNumbers: []string{"345-678-9012"},
			},
			{
				Name:         "Bob",
				PhoneNumbers: []string{"234-567-8901"},
			},
			{
				Name:         "Alice",
				PhoneNumbers: []string{"123-456-7890"},
			},
		}

		for _, person := range expected {
			if err := store.Put(ctx, "consumer/"+strings.ToLower(person.Name), -1, person); err != nil {
				t.Errorf("unexpected error putting data: %v", err)
			}
		}

		t.Run("Calling get multiple times without committing maintains position", func(t *testing.T) {
			reader := kv.NewStreamConsumer(ctx, store, "consumer-1", "test-consumer-1", kv.TypeAll)
			reader.Limit = 1
			reader.MinBackoff = 1 * time.Millisecond
			reader.MaxBackoff = 10 * time.Millisecond
			defer func() {
				if err := reader.Delete(ctx); err != nil {
					t.Logf("cleanup error: %v", err)
				}
			}()

			for i := range 3 {
				var count int
				timeoutCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
				for record, err := range reader.Read(timeoutCtx) {
					if err != nil {
						if errors.Is(err, context.DeadlineExceeded) && count == 0 {
							t.Errorf("iteration %d: timed out waiting for data", i)
						} else {
							t.Errorf("iteration %d: unexpected error getting data: %v", i, err)
						}
						break
					}
					count++
					if record.Record.Key != "consumer/frank" {
						t.Fatalf("iteration %d: expected key 'consumer/frank', got %s", i, record.Record.Key)
					}
					if count >= 1 {
						break
					}
				}
				cancel()
				if count != 1 {
					t.Fatalf("iteration %d: expected 1 record, got %d", i, count)
				}
			}
		})
		t.Run("Can iterate through records", func(t *testing.T) {
			reader := kv.NewStreamConsumer(ctx, store, "consumer-2", "test-consumer-2", kv.TypeAll)
			reader.Limit = 1
			reader.MinBackoff = 1 * time.Millisecond
			reader.MaxBackoff = 10 * time.Millisecond
			defer func() {
				if err := reader.Delete(ctx); err != nil {
					t.Logf("cleanup error: %v", err)
				}
			}()

			timeoutCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			defer cancel()

			var expectedIndex int
			for record, err := range reader.Read(timeoutCtx) {
				if err != nil {
					if errors.Is(err, context.DeadlineExceeded) && expectedIndex >= len(expected) {
						break
					}
					t.Errorf("iteration %d: unexpected error getting data: %v", expectedIndex, err)
					break
				}
				if expectedIndex >= len(expected) {
					break
				}

				person, err := kv.ValueOf[Person](record.Record)
				if err != nil {
					t.Fatalf("iteration %d: unexpected error getting value: %v", expectedIndex, err)
				}

				if person.Name != expected[expectedIndex].Name {
					t.Errorf("iteration %d: expected name '%s', got '%s'", expectedIndex, expected[expectedIndex].Name, person.Name)
				}

				if err = reader.CommitUpTo(ctx, record.Seq); err != nil {
					t.Fatalf("iteration %d: unexpected error committing data: %v", expectedIndex, err)
				}
				expectedIndex++
			}

			if expectedIndex != len(expected) {
				t.Errorf("expected to read %d records, got %d", len(expected), expectedIndex)
			}
		})
		t.Run("Can reload consumer state", func(t *testing.T) {
			reader := kv.NewStreamConsumer(ctx, store, "consumer-3", "test-consumer-3", kv.TypeAll)
			reader.MinBackoff = 1 * time.Millisecond
			reader.MaxBackoff = 10 * time.Millisecond
			defer func() {
				if err := reader.Delete(ctx); err != nil {
					t.Logf("cleanup error: %v", err)
				}
			}()

			var recordCount int
			timeoutCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			for record, err := range reader.Read(timeoutCtx) {
				if err != nil {
					if errors.Is(err, context.DeadlineExceeded) {
						t.Fatalf("timed out reading records after %d records", recordCount)
					}
					t.Fatalf("unexpected error getting data: %v", err)
					break
				}
				recordCount++
				if recordCount >= len(expected) {
					if err = reader.CommitUpTo(ctx, record.Seq); err != nil {
						t.Fatalf("unexpected error committing data: %v", err)
					}
					break
				}
			}
			cancel()

			reader = kv.NewStreamConsumer(ctx, store, "consumer-3", "test-consumer-3", kv.TypeAll)
			reader.MinBackoff = 1 * time.Millisecond
			reader.MaxBackoff = 10 * time.Millisecond

			var hasRecords bool
			timeoutCtx, cancel = context.WithTimeout(ctx, 2*time.Second)
			defer cancel()
			for _, err := range reader.Read(timeoutCtx) {
				if err != nil {
					if errors.Is(err, context.DeadlineExceeded) {
						break
					}
					t.Fatalf("unexpected error getting data after reload: %v", err)
					break
				}
				hasRecords = true
				break
			}
			if hasRecords {
				t.Error("expected no records after consuming all and reloading")
			}
		})
	}
}
