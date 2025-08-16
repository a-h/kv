package tests

import (
	"context"
	"strings"
	"testing"

	"github.com/a-h/kv"
)

func newConsumerTest(ctx context.Context, store kv.Store) func(t *testing.T) {
	return func(t *testing.T) {
		defer store.DeletePrefix(ctx, "*", 0, -1)

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
			consumer, err := kv.NewConsumer(ctx, store, "consumer")
			if err != nil {
				t.Fatalf("unexpected error creating consumer: %v", err)
			}
			consumer.Limit = 1
			defer consumer.Delete(ctx)

			for i := range 3 {
				records, err := consumer.Get(ctx)
				if err != nil {
					t.Errorf("iteration %d: unexpected error getting data: %v", i, err)
				}
				if len(records) != 1 {
					t.Fatalf("iteration %d: expected 1 record, got %d", i, len(records))
				}
				if records[0].Key != "consumer/frank" {
					t.Fatalf("iteration %d: expected key 'consumer/frank', got %s", i, records[0].Key)
				}
			}
		})
		t.Run("Can iterate through records", func(t *testing.T) {
			consumer, err := kv.NewConsumer(ctx, store, "consumer")
			if err != nil {
				t.Fatalf("unexpected error creating consumer: %v", err)
			}
			consumer.Limit = 1
			defer consumer.Delete(ctx)

			for i, expected := range expected {
				records, err := consumer.Get(ctx)
				if err != nil {
					t.Errorf("iteration %d: unexpected error getting data: %v", i, err)
				}
				if len(records) != 1 {
					t.Fatalf("iteration %d: expected 1 record, got %d", i, len(records))
				}
				values, err := kv.ValuesOf[Person](records)
				if err != nil {
					t.Fatalf("iteration %d: unexpected error getting values: %v", i, err)
				}
				if values[0].Name != expected.Name {
					t.Errorf("iteration %d: expected name '%s', got '%s'", i, expected.Name, values[0].Name)
				}
				if err = consumer.Commit(ctx); err != nil {
					t.Fatalf("iteration %d: unexpected error committing data: %v, %#v", i, err, consumer)
				}
			}
		})
		t.Run("Can reload consumer state", func(t *testing.T) {
			consumer, err := kv.NewConsumer(ctx, store, "consumer")
			if err != nil {
				t.Fatalf("unexpected error creating consumer: %v", err)
			}
			defer consumer.Delete(ctx)

			// Consume all records in the first run.
			if _, err := consumer.Get(ctx); err != nil {
				t.Fatalf("unexpected error getting data: %v", err)
			}
			if err = consumer.Commit(ctx); err != nil {
				t.Fatalf("unexpected error committing data: %v", err)
			}

			// Reload the consumer.
			consumer, err = kv.NewConsumer(ctx, store, "consumer")
			if err != nil {
				t.Fatalf("unexpected error reloading consumer: %v", err)
			}
			if consumer.Offset != len(expected) {
				t.Errorf("expected consumer offset to be %d, got %d", len(expected), consumer.Offset)
			}
			records, err := consumer.Get(ctx)
			if err != nil {
				t.Fatalf("unexpected error getting data after reload: %v", err)
			}
			// Filter out any stream records.
			records = filter(records, func(r kv.Record) bool {
				return !strings.HasPrefix(r.Key, "github.com/a-h/kv/stream/")
			})
			if len(records) != 0 {
				t.Errorf("record[0]: %#v", string(records[0].Value))
				t.Fatalf("expected no records after reload, got %d", len(records))
			}
		})
	}
}

func filter(records []kv.Record, f func(r kv.Record) bool) (filtered []kv.Record) {
	for _, r := range records {
		if f(r) {
			filtered = append(filtered, r)
		}
	}
	return filtered
}
