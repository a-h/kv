package tests

import (
	"context"
	"testing"

	"github.com/a-h/kv"
)

func newPutTest(ctx context.Context, store kv.Store) func(t *testing.T) {
	return func(t *testing.T) {
		t.Run("Can put data", func(t *testing.T) {
			defer store.DeletePrefix(ctx, "*", 0, -1)

			expected := Person{
				Name:         "Alice",
				PhoneNumbers: []string{"123-456-7890"},
			}
			err := store.Put(ctx, "put", -1, expected)
			if err != nil {
				t.Errorf("unexpected error putting data: %v", err)
			}
			var p Person
			_, ok, err := store.Get(ctx, "put", &p)
			if err != nil {
				t.Errorf("unexpected error getting data: %v", err)
			}
			if !ok {
				t.Error("expected data not found")
			}
			if !expected.Equals(p) {
				t.Errorf("expected %#v, got %#v", expected, p)
			}
		})
		t.Run("Can overwrite existing data if version is set to -1", func(t *testing.T) {
			defer store.DeletePrefix(ctx, "*", 0, -1)

			expected := Person{
				Name:         "Alice",
				PhoneNumbers: []string{"123-456-7890"},
			}
			err := store.Put(ctx, "put", -1, expected)
			if err != nil {
				t.Errorf("unexpected error putting data: %v", err)
			}
			expected.PhoneNumbers = []string{"234-567-8901"}
			err = store.Put(ctx, "put", -1, expected)
			if err != nil {
				t.Errorf("unexpected error putting data: %v", err)
			}
			var overwritten Person
			_, ok, err := store.Get(ctx, "put", &overwritten)
			if err != nil {
				t.Errorf("unexpected error getting data: %v", err)
			}
			if !ok {
				t.Error("expected data not found")
			}
			if !expected.Equals(overwritten) {
				t.Errorf("expected %#v, got %#v", expected, overwritten)
			}
		})
		t.Run("Can not insert a record if one already exists and version is set to 0", func(t *testing.T) {
			defer store.DeletePrefix(ctx, "*", 0, -1)

			expected := Person{Name: "Alice"}
			err := store.Put(ctx, "put", -1, expected)
			if err != nil {
				t.Errorf("unexpected error putting data: %v", err)
			}
			err = store.Put(ctx, "put", 0, expected)
			if err == nil {
				t.Error("expected error putting data: got nil")
			}
		})
		t.Run("Can overwrite existing data with specified version", func(t *testing.T) {
			defer store.DeletePrefix(ctx, "*", 0, -1)

			expected := Person{
				Name:         "Alice",
				PhoneNumbers: []string{"123-456-7890"},
			}
			err := store.Put(ctx, "put", -1, expected)
			if err != nil {
				t.Errorf("unexpected error putting data: %v", err)
			}
			expected.PhoneNumbers = []string{"234-567-8901"}
			err = store.Put(ctx, "put", 1, expected)
			if err != nil {
				t.Errorf("unexpected error putting data: %v", err)
			}
			var actual Person
			r, ok, err := store.Get(ctx, "put", &actual)
			if err != nil {
				t.Errorf("unexpected error getting data: %v", err)
			}
			if !ok {
				t.Error("expected data not found")
			}
			if !expected.Equals(actual) {
				t.Errorf("expected %#v, got %#v", expected, actual)
			}
			if r.Version != 2 {
				t.Errorf("expected version 2, got %d", r.Version)
			}
		})
		t.Run("Can use optimistic concurrency to ensure version being updated has not been changed", func(t *testing.T) {
			defer store.DeletePrefix(ctx, "*", 0, -1)

			expected := Person{Name: "Alice"}
			err := store.Put(ctx, "put", -1, expected)
			if err != nil {
				t.Errorf("unexpected error putting data: %v", err)
			}
			expected.PhoneNumbers = []string{"234-567-8901"}
			err = store.Put(ctx, "put", 3, expected)
			if err == nil {
				t.Error("expected error putting data: got nil")
			}
		})
		t.Run("The created field is set and not updated", func(t *testing.T) {
			defer store.DeletePrefix(ctx, "*", 0, -1)

			data := map[string]any{
				"key": "value",
			}
			err := store.Put(ctx, "put", -1, data)
			if err != nil {
				t.Errorf("unexpected error putting data: %v", err)
			}

			var v struct{}
			r1, ok, err := store.Get(ctx, "put", &v)
			if err != nil {
				t.Errorf("unexpected error getting data: %v", err)
			}
			if !ok {
				t.Error("expected data not found")
			}
			if r1.Created.IsZero() {
				t.Error("expected created time to be set")
			}

			// Update.
			data["key"] = "value2"
			err = store.Put(ctx, "put", -1, data)
			if err != nil {
				t.Errorf("unexpected error putting data: %v", err)
			}
			r2, ok, err := store.Get(ctx, "put", &v)
			if err != nil {
				t.Errorf("unexpected error getting data: %v", err)
			}
			if !ok {
				t.Error("expected data not found")
			}
			if !r2.Created.Equal(r1.Created) {
				t.Errorf("expected created time to not change from %v, but got %v", r1.Created, r2.Created)
			}
		})
		t.Run("The version number is incremented", func(t *testing.T) {
			defer store.DeletePrefix(ctx, "*", 0, -1)

			data := map[string]any{}

			for v := range 10 {
				err := store.Put(ctx, "put", int64(v), data)
				if err != nil {
					t.Errorf("v: %d: unexpected error putting data: %v", v, err)
				}
			}
		})
	}
}
