package tests

import (
	"context"
	"testing"

	"github.com/a-h/kv"
)

func newCountPrefixTest(ctx context.Context, store kv.Store) func(t *testing.T) {
	return func(t *testing.T) {
		t.Run("Can count data", func(t *testing.T) {
			defer store.DeletePrefix(ctx, "*", 0, -1)

			store.Put(ctx, "count/a", -1, Person{Name: "Alice"})
			store.Put(ctx, "count/b", -1, Person{Name: "Bob"})
			store.Put(ctx, "count/c", -1, Person{Name: "Charlie"})
			store.Put(ctx, "otherprefix/c2", -1, Person{Name: "David"})

			count, err := store.CountPrefix(ctx, "count")
			if err != nil {
				t.Errorf("unexpected error counting data: %v", err)
			}
			if count != 3 {
				t.Errorf("expected 3 records, got %d", count)
			}
		})
	}
}
