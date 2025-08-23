package tests

import (
	"context"
	"strings"
	"testing"

	"github.com/a-h/kv"
)

func newGetRangeTest(ctx context.Context, store kv.Store) func(t *testing.T) {
	return func(t *testing.T) {
		defer func() {
			if _, err := store.DeletePrefix(ctx, "*", 0, -1); err != nil {
				t.Logf("cleanup error: %v", err)
			}
		}()

		expected := []Person{
			{
				Name:         "Bob",
				PhoneNumbers: []string{"234-567-8901"},
			},
			{
				Name:         "Charlie",
				PhoneNumbers: []string{"345-678-9012"},
			},
			{
				Name:         "David",
				PhoneNumbers: []string{"456-789-0123"},
			},
			{
				Name:         "Eve",
				PhoneNumbers: []string{"567-890-1234"},
			},
			{
				Name:         "Frank",
				PhoneNumbers: []string{"678-901-2345"},
			},
		}

		for _, person := range expected {
			if err := store.Put(ctx, "getrange/"+strings.ToLower(person.Name), -1, person); err != nil {
				t.Errorf("unexpected error putting data: %v", err)
			}
		}

		if err := store.Put(ctx, "otherprefix/greg", -1, Person{Name: "Greg"}); err != nil {
			t.Errorf("unexpected error putting data: %v", err)
		}

		t.Run("Can get range", func(t *testing.T) {
			actual, err := store.GetRange(ctx, "getrange/a", "getrange/z", 0, -1)
			if err != nil {
				t.Errorf("unexpected error getting data: %v", err)
			}
			actualValues, err := kv.ValuesOf[Person](actual)
			if err != nil {
				t.Errorf("unexpected error converting rows: %v", err)
			}
			if !personSliceIsEqual(expected, actualValues) {
				t.Errorf("expected %#v, got %#v", expected, actualValues)
			}
		})
		t.Run("Can retrieve a subset", func(t *testing.T) {
			actual, err := store.GetRange(ctx, "getrange/c", "getrange/e", 0, -1)
			if err != nil {
				t.Errorf("unexpected error getting data: %v", err)
			}
			actualValues, err := kv.ValuesOf[Person](actual)
			if err != nil {
				t.Errorf("unexpected error converting rows: %v", err)
			}
			// The range operation is not inclusive of the end key, so we expect Charlie and David.
			if !personSliceIsEqual(expected[1:3], actualValues) {
				t.Errorf("expected %#v, got %#v", expected[1:3], actualValues)
			}
		})
		t.Run("Can limit the number of results", func(t *testing.T) {
			actual, err := store.GetRange(ctx, "getrange/a", "getrange/z", 0, 2)
			if err != nil {
				t.Errorf("unexpected error getting data: %v", err)
			}
			actualValues, err := kv.ValuesOf[Person](actual)
			if err != nil {
				t.Errorf("unexpected error converting rows: %v", err)
			}
			if !personSliceIsEqual(expected[:2], actualValues) {
				t.Errorf("expected %#v, got %#v", expected[:2], actualValues)
			}
		})
		t.Run("Can offset the results", func(t *testing.T) {
			actual, err := store.GetRange(ctx, "getrange/a", "getrange/z", 1, -1)
			if err != nil {
				t.Errorf("unexpected error getting data: %v", err)
			}
			actualValues, err := kv.ValuesOf[Person](actual)
			if err != nil {
				t.Errorf("unexpected error converting rows: %v", err)
			}
			if !personSliceIsEqual(expected[1:], actualValues) {
				t.Errorf("expected %#v, got %#v", expected[1:], actualValues)
			}
		})
	}
}
