package tests

import (
	"context"
	"strings"
	"testing"

	"github.com/a-h/kv"
)

func newStreamTest(ctx context.Context, store kv.Store) func(t *testing.T) {
	return func(t *testing.T) {
		if err := store.StreamTrim(ctx, -1); err != nil {
			t.Fatalf("could not trim stream prior to running test: %v", err)
		}

		defer func() {
			if _, err := store.DeletePrefix(ctx, "*", 0, -1); err != nil {
				t.Logf("cleanup error: %v", err)
			}
		}()
		defer func() {
			if err := store.StreamTrim(ctx, -1); err != nil {
				t.Logf("cleanup error: %v", err)
			}
		}()

		startSeq, err := store.StreamSeq(ctx)
		if err != nil {
			t.Fatalf("could not get stream sequence: %v", err)
		}

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
			if err := store.Put(ctx, "stream/"+strings.ToLower(person.Name), -1, person); err != nil {
				t.Errorf("unexpected error putting data: %v", err)
			}
		}

		t.Run("Can get all", func(t *testing.T) {
			actual, err := store.Stream(ctx, startSeq, -1)
			if err != nil {
				t.Errorf("unexpected error getting data: %v", err)
			}
			var actualValues []Person
			for _, sr := range actual {
				person, err := kv.ValueOf[Person](sr.Record)
				if err != nil {
					t.Errorf("unexpected error getting value: %v", err)
				}
				actualValues = append(actualValues, person)
			}
			if !personSliceIsEqual(expected, actualValues) {
				t.Errorf("expected %#v, got %#v", expected, actualValues)
			}
		})
		t.Run("Can limit the number of results", func(t *testing.T) {
			actual, err := store.Stream(ctx, startSeq, 2)
			if err != nil {
				t.Errorf("unexpected error getting data: %v", err)
			}
			var actualValues []Person
			for _, sr := range actual {
				person, err := kv.ValueOf[Person](sr.Record)
				if err != nil {
					t.Errorf("unexpected error getting value: %v", err)
				}
				actualValues = append(actualValues, person)
			}
			if !personSliceIsEqual(expected[:2], actualValues) {
				t.Errorf("expected %#v, got %#v", expected[:2], actualValues)
			}
		})
		t.Run("Can offset the results", func(t *testing.T) {
			// startSeq = whatever action was previously taken, or zero.
			// startSeq + 1 = the insert of "Frank".
			// startSeq + 2 = the insert of "Eve".
			// ...
			actual, err := store.Stream(ctx, startSeq+2, -1)
			if err != nil {
				t.Errorf("unexpected error getting data: %v", err)
			}
			var actualValues []Person
			for _, sr := range actual {
				person, err := kv.ValueOf[Person](sr.Record)
				if err != nil {
					t.Errorf("unexpected error getting value: %v", err)
				}
				actualValues = append(actualValues, person)
			}
			if err != nil {
				t.Errorf("unexpected error getting values: %v", err)
			}
			if !personSliceIsEqual(expected[1:], actualValues) {
				t.Errorf("expected %#v, got %#v", expected[1:], actualValues)
			}
		})
	}
}
