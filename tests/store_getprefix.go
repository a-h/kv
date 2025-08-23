package tests

import (
	"context"
	"strings"
	"testing"

	"github.com/a-h/kv"
)

func newGetPrefixTest(ctx context.Context, store kv.Store) func(t *testing.T) {
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
		}

		for _, person := range expected {
			if err := store.Put(ctx, "getprefix/"+strings.ToLower(person.Name), -1, person); err != nil {
				t.Errorf("unexpected error putting data: %v", err)
			}
		}

		if err := store.Put(ctx, "otherprefix/eve", -1, Person{Name: "Eve"}); err != nil {
			t.Errorf("unexpected error putting data: %v", err)
		}

		t.Run("Can get records with a given prefix", func(t *testing.T) {
			actual, err := store.GetPrefix(ctx, "getprefix", 0, -1)
			if err != nil {
				t.Errorf("unexpected error getting data: %v", err)
			}
			actualValues, err := kv.ValuesOf[Person](actual)
			if err != nil {
				t.Errorf("unexpected error getting data: %v", err)
			}
			if !personSliceIsEqual(expected, actualValues) {
				t.Errorf("expected %#v, got %#v", expected, actualValues)
			}
		})
		t.Run("Can limit the number of results", func(t *testing.T) {
			actual, err := store.GetPrefix(ctx, "getprefix", 0, 2)
			if err != nil {
				t.Errorf("unexpected error getting data: %v", err)
			}
			actualValues, err := kv.ValuesOf[Person](actual)
			if err != nil {
				t.Errorf("unexpected error getting data: %v", err)
			}
			if !personSliceIsEqual(expected[:2], actualValues) {
				t.Errorf("expected %#v, got %#v", expected[:2], actualValues)
			}
		})
		t.Run("Can offset the results", func(t *testing.T) {
			actual, err := store.GetPrefix(ctx, "getprefix", 1, -1)
			if err != nil {
				t.Errorf("unexpected error getting data: %v", err)
			}
			actualValues, err := kv.ValuesOf[Person](actual)
			if err != nil {
				t.Errorf("unexpected error getting data: %v", err)
			}
			if !personSliceIsEqual(expected[1:], actualValues) {
				t.Errorf("expected %#v, got %#v", expected[1:], actualValues)
			}
		})
		t.Run("Outside the prefix, no records are returned", func(t *testing.T) {
			actual, err := store.GetPrefix(ctx, "getprefix/zzz", 0, -1)
			if err != nil {
				t.Errorf("unexpected error getting data: %v", err)
			}
			actualValues, err := kv.ValuesOf[Person](actual)
			if err != nil {
				t.Errorf("unexpected error getting data: %v", err)
			}
			if len(actualValues) != 0 {
				t.Errorf("expected no records, got %d", len(actualValues))
			}
		})
	}
}
