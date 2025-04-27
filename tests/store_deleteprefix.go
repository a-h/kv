package tests

import (
	"context"
	"testing"

	"github.com/a-h/kv"
)

func newDeletePrefixTest(ctx context.Context, store kv.Store) func(t *testing.T) {
	return func(t *testing.T) {
		t.Run("Can delete data with matching prefix", func(t *testing.T) {
			defer store.DeletePrefix(ctx, "deleteprefix", 0, -1)

			store.Put(ctx, "deleteprefix/a", -1, Person{Name: "Alice"})
			store.Put(ctx, "deleteprefix/b", -1, Person{Name: "Bob"})
			store.Put(ctx, "deleteprefix/c", -1, Person{Name: "Charlie"})
			store.Put(ctx, "deleteprefix/c2", -1, Person{Name: "David"})

			deleted, err := store.DeletePrefix(ctx, "deleteprefix/c", 0, -1)
			if err != nil {
				t.Errorf("unexpected error deleting data: %v", err)
			}
			if deleted != 2 {
				t.Errorf("expected 2 records to be deleted, got %d", deleted)
			}
			count, err := store.Count(ctx)
			if err != nil {
				t.Errorf("unexpected error counting data: %v", err)
			}
			if count != 2 {
				t.Errorf("expected 2 records, got %d", count)
			}
		})
		t.Run("Can delete all data", func(t *testing.T) {
			defer store.DeletePrefix(ctx, "*", 0, -1)

			store.Put(ctx, "deleteprefix/a", -1, Person{Name: "Alice"})
			store.Put(ctx, "deleteprefix/b", -1, Person{Name: "Bob"})
			store.Put(ctx, "deleteprefix/c", -1, Person{Name: "Charlie"})
			store.Put(ctx, "otherprefix/c2", -1, Person{Name: "David"})

			deleted, err := store.DeletePrefix(ctx, "*", 0, -1)
			if err != nil {
				t.Errorf("unexpected error deleting data: %v", err)
			}
			if deleted != 4 {
				t.Errorf("expected 4 records to be deleted, got %d", deleted)
			}
			count, err := store.Count(ctx)
			if err != nil {
				t.Errorf("unexpected error counting data: %v", err)
			}
			if count != 0 {
				t.Errorf("expected 0 records, got %d", count)
			}
		})
		t.Run("Deleting non-existent prefixes does not return an error", func(t *testing.T) {
			defer store.DeletePrefix(ctx, "*", 0, -1)

			if _, err := store.DeletePrefix(ctx, "deleteprefix-does-not-exist", 0, -1); err != nil {
				t.Errorf("unexpected error deleting data: %v", err)
			}
		})
		t.Run("Can limit the number of records to delete", func(t *testing.T) {
			defer store.DeletePrefix(ctx, "*", 0, -1)

			store.Put(ctx, "deleteprefix/a", -1, Person{Name: "Alice"})
			store.Put(ctx, "deleteprefix/b", -1, Person{Name: "Bob"})
			store.Put(ctx, "deleteprefix/c", -1, Person{Name: "Charlie"})

			deleted, err := store.DeletePrefix(ctx, "deleteprefix", 0, 2)
			if err != nil {
				t.Errorf("unexpected error deleting data: %v", err)
			}
			if deleted != 2 {
				t.Errorf("expected 2 records to be deleted, got %d", deleted)
			}

			count, err := store.Count(ctx)
			if err != nil {
				t.Errorf("unexpected error counting data: %v", err)
			}
			if count != 1 {
				t.Errorf("expected 1 record, got %d", count)
			}
		})
		t.Run("Can offset the records to delete", func(t *testing.T) {
			defer store.DeletePrefix(ctx, "*", 0, -1)

			store.Put(ctx, "deleteprefix/a", -1, Person{Name: "Alice"})
			store.Put(ctx, "deleteprefix/b", -1, Person{Name: "Bob"})
			store.Put(ctx, "deleteprefix/c", -1, Person{Name: "Charlie"})

			deleted, err := store.DeletePrefix(ctx, "deleteprefix", 1, -1)
			if err != nil {
				t.Errorf("unexpected error deleting data: %v", err)
			}
			if deleted != 2 {
				t.Errorf("expected 2 records to be deleted, got %d", deleted)
			}

			count, err := store.Count(ctx)
			if err != nil {
				t.Errorf("unexpected error counting data: %v", err)
			}
			if count != 1 {
				t.Errorf("expected 1 record, got %d", count)
			}

			var r struct{} // We don't care about the value, just the existence of a record.
			_, ok, err := store.Get(ctx, "deleteprefix/a", &r)
			if err != nil {
				t.Errorf("unexpected error getting data: %v", err)
			}
			if !ok {
				t.Error("expected a record to exist")
			}
		})
	}
}
