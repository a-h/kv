package tests

import (
	"context"
	"fmt"
	"testing"

	"github.com/a-h/kv"
)

func newDeleteTest(ctx context.Context, store kv.Store) func(t *testing.T) {
	return func(t *testing.T) {
		defer store.DeletePrefix(ctx, "*", 0, -1)

		t.Run("Can delete", func(t *testing.T) {
			data := []Person{
				{
					Name:         "Alice",
					PhoneNumbers: []string{"123-456-7890"},
				},
				{
					Name:         "Bob",
					PhoneNumbers: []string{"987-654-3210"},
				},
				{
					Name:         "Charlie",
					PhoneNumbers: []string{"555-555-5555"},
				},
			}
			for i, d := range data {
				if err := store.Put(ctx, fmt.Sprintf("delete-%d", i), -1, d); err != nil {
					t.Errorf("unexpected error putting data: %v", err)
				}
			}

			if _, err := store.Delete(ctx, "delete-1", "delete-2"); err != nil {
				t.Errorf("unexpected error deleting data: %v", err)
			}

			expectedOK := []bool{true, false, false}
			for i := range len(data) {
				var r Person
				_, ok, err := store.Get(ctx, fmt.Sprintf("delete-%d", i), &r)
				if err != nil {
					t.Errorf("unexpected error getting data: %v", err)
				}
				if expectedOK[i] != ok {
					t.Errorf("expected ok=%v, got %v", expectedOK[i], ok)
				}
			}
		})
		t.Run("Deleting non-existent keys does not return an error", func(t *testing.T) {
			deleted, err := store.Delete(ctx, "delete-does-not-exist")
			if err != nil {
				t.Errorf("unexpected error deleting data: %v", err)
			}
			if deleted != 0 {
				t.Errorf("expected 0 rows to be deleted, got %d", deleted)
			}
		})
	}
}
