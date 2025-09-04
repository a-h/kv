package tests

import (
	"context"
	"testing"

	"github.com/a-h/kv"
)

func newGetBatchTest(ctx context.Context, store kv.Store) func(t *testing.T) {
	return func(t *testing.T) {
		defer func() {
			if _, err := store.DeletePrefix(ctx, "*", 0, -1); err != nil {
				t.Logf("cleanup error: %v", err)
			}
		}()

		// Setup test data.
		person1 := Person{Name: "Alice", PhoneNumbers: []string{"123"}}
		person2 := Person{Name: "Bob", PhoneNumbers: []string{"456"}}
		person3 := Person{Name: "Charlie", PhoneNumbers: []string{"789"}}

		if err := store.Put(ctx, "person1", -1, person1); err != nil {
			t.Fatalf("unexpected error putting person1: %v", err)
		}
		if err := store.Put(ctx, "person2", -1, person2); err != nil {
			t.Fatalf("unexpected error putting person2: %v", err)
		}
		if err := store.Put(ctx, "person3", -1, person3); err != nil {
			t.Fatalf("unexpected error putting person3: %v", err)
		}

		t.Run("Can get multiple existing keys", func(t *testing.T) {
			items, err := store.GetBatch(ctx, "person1", "person2", "person3")
			if err != nil {
				t.Fatalf("unexpected error getting batch: %v", err)
			}

			if len(items) != 3 {
				t.Errorf("expected 3 items, got %d", len(items))
			}

			if record, ok := items["person1"]; !ok {
				t.Error("person1 not found in batch")
			} else if record.Key != "person1" {
				t.Errorf("expected key person1, got %s", record.Key)
			}

			if record, ok := items["person2"]; !ok {
				t.Error("person2 not found in batch")
			} else if record.Key != "person2" {
				t.Errorf("expected key person2, got %s", record.Key)
			}

			if record, ok := items["person3"]; !ok {
				t.Error("person3 not found in batch")
			} else if record.Key != "person3" {
				t.Errorf("expected key person3, got %s", record.Key)
			}
		})

		t.Run("Handles mix of existing and non-existing keys", func(t *testing.T) {
			items, err := store.GetBatch(ctx, "person1", "nonexistent", "person2")
			if err != nil {
				t.Fatalf("unexpected error getting batch: %v", err)
			}

			if len(items) != 2 {
				t.Errorf("expected 2 items, got %d", len(items))
			}

			if _, ok := items["person1"]; !ok {
				t.Error("person1 not found in batch")
			}

			if _, ok := items["person2"]; !ok {
				t.Error("person2 not found in batch")
			}

			if _, ok := items["nonexistent"]; ok {
				t.Error("nonexistent key should not be in batch")
			}
		})

		t.Run("Returns empty map for empty key list", func(t *testing.T) {
			items, err := store.GetBatch(ctx)
			if err != nil {
				t.Fatalf("unexpected error getting empty batch: %v", err)
			}

			if len(items) != 0 {
				t.Errorf("expected 0 items, got %d", len(items))
			}
		})

		t.Run("Returns empty map when no keys exist", func(t *testing.T) {
			items, err := store.GetBatch(ctx, "nonexistent1", "nonexistent2")
			if err != nil {
				t.Fatalf("unexpected error getting batch: %v", err)
			}

			if len(items) != 0 {
				t.Errorf("expected 0 items, got %d", len(items))
			}
		})

		t.Run("Handles large batch sizes", func(t *testing.T) {
			// Test chunking by creating more keys than a typical batch limit.
			// Use 350 keys to test chunking across different implementations.
			var keys []string
			for i := range 350 {
				key := "large_test_" + string(rune('A'+i%26)) + string(rune('A'+(i/26)%26)) + string(rune('0'+i%10))
				keys = append(keys, key)
				person := Person{Name: "Person" + key, PhoneNumbers: []string{"000"}}
				if err := store.Put(ctx, key, -1, person); err != nil {
					t.Fatalf("unexpected error putting %s: %v", key, err)
				}
			}

			items, err := store.GetBatch(ctx, keys...)
			if err != nil {
				t.Fatalf("unexpected error getting large batch: %v", err)
			}

			if len(items) != len(keys) {
				t.Errorf("expected %d items, got %d", len(keys), len(items))
			}

			for _, key := range keys {
				if _, ok := items[key]; !ok {
					t.Errorf("key %s not found in batch", key)
				}
			}
		})
	}
}
