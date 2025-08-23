package tests

import (
	"context"
	"testing"

	"github.com/a-h/kv"
)

func newCountRangeTest(ctx context.Context, store kv.Store) func(t *testing.T) {
	return func(t *testing.T) {
		t.Run("Can count range", func(t *testing.T) {
			defer func() {
				if _, err := store.DeletePrefix(ctx, "*", 0, -1); err != nil {
					t.Logf("cleanup error: %v", err)
				}
			}()

			if err := store.Put(ctx, "count/a", -1, Person{Name: "Alice"}); err != nil {
				t.Fatalf("failed to put data: %v", err)
			}
			if err := store.Put(ctx, "count/b", -1, Person{Name: "Bob"}); err != nil {
				t.Fatalf("failed to put data: %v", err)
			}
			if err := store.Put(ctx, "count/c", -1, Person{Name: "Charlie"}); err != nil {
				t.Fatalf("failed to put data: %v", err)
			}
			if err := store.Put(ctx, "otherprefix/c2", -1, Person{Name: "David"}); err != nil {
				t.Fatalf("failed to put data: %v", err)
			}

			count, err := store.CountRange(ctx, "count/a", "count/d")
			if err != nil {
				t.Errorf("unexpected error counting data: %v", err)
			}
			if count != 3 {
				t.Errorf("expected 3 records, got %d", count)
			}
		})
		t.Run("Data outside of the range is not returned", func(t *testing.T) {
			defer func() {
				if _, err := store.DeletePrefix(ctx, "*", 0, -1); err != nil {
					t.Logf("cleanup error: %v", err)
				}
			}()

			if err := store.Put(ctx, "count/a", -1, Person{Name: "Alice"}); err != nil {
				t.Fatalf("failed to put data: %v", err)
			}
			if err := store.Put(ctx, "count/b", -1, Person{Name: "Bob"}); err != nil {
				t.Fatalf("failed to put data: %v", err)
			}
			if err := store.Put(ctx, "count/c", -1, Person{Name: "Charlie"}); err != nil {
				t.Fatalf("failed to put data: %v", err)
			}
			if err := store.Put(ctx, "count/d", -1, Person{Name: "David"}); err != nil {
				t.Fatalf("failed to put data: %v", err)
			}
			if err := store.Put(ctx, "otherprefix/c2", -1, Person{Name: "Eve"}); err != nil {
				t.Fatalf("failed to put data: %v", err)
			}

			count, err := store.CountRange(ctx, "count/e", "count/z")
			if err != nil {
				t.Errorf("unexpected error counting data: %v", err)
			}
			if count != 0 {
				t.Errorf("expected no records, got %d", count)
			}
		})
	}
}
