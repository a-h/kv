package tests

import (
	"context"
	"testing"

	"github.com/a-h/kv"
)

func newCountTest(ctx context.Context, store kv.Store) func(t *testing.T) {
	return func(t *testing.T) {
		defer store.DeletePrefix(ctx, "*", 0, -1)

		t.Run("Can count data", func(t *testing.T) {
			store.Put(ctx, "count/a", -1, Person{Name: "Alice"})
			store.Put(ctx, "count/b", -1, Person{Name: "Bob"})
			store.Put(ctx, "count/c", -1, Person{Name: "Charlie"})

			count, err := store.Count(ctx)
			if err != nil {
				t.Errorf("unexpected error counting data: %v", err)
			}
			if count != 3 {
				t.Errorf("expected 3 records, got %d", count)
			}
		})
	}
}
