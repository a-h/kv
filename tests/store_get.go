package tests

import (
	"context"
	"testing"

	"github.com/a-h/kv"
)

func newGetTest(ctx context.Context, store kv.Store) func(t *testing.T) {
	return func(t *testing.T) {
		defer func() {
			if _, err := store.DeletePrefix(ctx, "*", 0, -1); err != nil {
				t.Logf("cleanup error: %v", err)
			}
		}()

		expected := Person{
			Name:         "Alice",
			PhoneNumbers: []string{"123-456-7890", "234-567-8901"},
		}
		if err := store.Put(ctx, "get", -1, expected); err != nil {
			t.Errorf("unexpected error putting data: %v", err)
		}

		t.Run("Can get data", func(t *testing.T) {
			var actual Person
			r, ok, err := store.Get(ctx, "get", &actual)
			if err != nil {
				t.Errorf("unexpected error getting data: %v", err)
			}
			if !ok {
				t.Error("expected data to be found")
			}
			if !expected.Equals(actual) {
				t.Errorf("expected %#v, got %#v", expected, actual)
			}
			if r.Version != 1 {
				t.Errorf("expected version 1, got %d", r.Version)
			}
		})
		t.Run("Returns ok=false if the key does not exist", func(t *testing.T) {
			var actual Person
			_, ok, err := store.Get(ctx, "get-does-not-exist", &actual)
			if err != nil {
				t.Errorf("unexpected error getting data: %v", err)
			}
			if ok {
				t.Error("expected data not to be found")
			}
		})
	}
}
