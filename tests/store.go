package tests

import (
	"context"
	"testing"
	"time"

	"github.com/a-h/kv"
)

func TestRecordsOf(t *testing.T) {
	records := []kv.Record{
		{
			Key:     "key1",
			Version: 1,
			Value:   []byte(`{"name": "Alice", "phone_numbers": ["123", "456"]}`),
			Created: time.Date(2025, 3, 10, 8, 16, 13, 0, time.UTC),
		},
		{
			Key:     "key2",
			Version: 3,
			Value:   []byte(`{"name": "Bob", "phone_numbers": ["789"]}`),
			Created: time.Date(2025, 3, 10, 8, 16, 13, 0, time.UTC),
		},
	}
	peopleRecords, err := kv.RecordsOf[Person](records)
	if err != nil {
		t.Fatalf("unexpected error unmarshaling records: %v", err)
	}
	if len(peopleRecords) != 2 {
		t.Fatalf("expected 2 people, got %d", len(peopleRecords))
	}
	if peopleRecords[0].Key != "key1" {
		t.Fatalf("expected key1, got %s", peopleRecords[0].Key)
	}
	if peopleRecords[0].Version != 1 {
		t.Fatalf("expected version 1, got %d", peopleRecords[0].Version)
	}
	if peopleRecords[0].Value.Name != "Alice" {
		t.Fatalf("expected Alice, got %s", peopleRecords[0].Value.Name)
	}
	if !peopleRecords[0].Created.Equal(time.Date(2025, 3, 10, 8, 16, 13, 0, time.UTC)) {
		t.Fatalf("expected 2025-03-10 08:16:13, got %s", peopleRecords[0].Created)
	}
	if peopleRecords[1].Key != "key2" {
		t.Fatalf("expected key2, got %s", peopleRecords[1].Key)
	}
	if peopleRecords[1].Version != 3 {
		t.Fatalf("expected version 3, got %d", peopleRecords[1].Version)
	}
	if peopleRecords[1].Value.Name != "Bob" {
		t.Fatalf("expected Bob, got %s", peopleRecords[1].Value.Name)
	}
	if !peopleRecords[1].Created.Equal(time.Date(2025, 3, 10, 8, 16, 13, 0, time.UTC)) {
		t.Fatalf("expected 2025-03-10 08:16:13, got %s", peopleRecords[1].Created)
	}
}

type Person struct {
	Name         string   `json:"name"`
	PhoneNumbers []string `json:"phone_numbers"`
}

func (p Person) Equals(other Person) bool {
	if p.Name != other.Name {
		return false
	}
	if len(p.PhoneNumbers) != len(other.PhoneNumbers) {
		return false
	}
	for i, number := range p.PhoneNumbers {
		if number != other.PhoneNumbers[i] {
			return false
		}
	}
	return true
}

func personSliceIsEqual(a, b []Person) bool {
	if len(a) != len(b) {
		return false
	}
	for i, p := range a {
		if !p.Equals(b[i]) {
			return false
		}
	}
	return true
}

func Run(t *testing.T, store kv.Store) {
	ctx := context.Background()
	if err := store.Init(ctx); err != nil {
		t.Fatalf("unexpected error initializing store: %v", err)
	}

	// Clear the data before running the tests.
	if _, err := store.DeletePrefix(ctx, "*", 0, -1); err != nil {
		t.Fatalf("unexpected error clearing data: %v", err)
	}

	t.Run("Get", newGetTest(ctx, store))
	t.Run("GetPrefix", newGetPrefixTest(ctx, store))
	t.Run("GetRange", newGetRangeTest(ctx, store))
	t.Run("List", newListTest(ctx, store))
	t.Run("Put", newPutTest(ctx, store))
	t.Run("Delete", newDeleteTest(ctx, store))
	t.Run("DeletePrefix", newDeletePrefixTest(ctx, store))
	t.Run("DeleteRange", newDeleteRangeTest(ctx, store))
	t.Run("Count", newCountTest(ctx, store))
	t.Run("CountPrefix", newCountPrefixTest(ctx, store))
	t.Run("CountRange", newCountRangeTest(ctx, store))
	t.Run("Patch", newPatchTest(ctx, store))
	t.Run("MutateAll", newMutateAllTest(ctx, store))
	t.Run("PutPatches", newPutPatchesTest(ctx, store))
	t.Run("Stream", newStreamTest(ctx, store))
	t.Run("Consumer", newConsumerTest(ctx, store))
	t.Run("Locks", newLockTest(ctx, store))

	deleted, err := store.DeletePrefix(ctx, "*", 0, -1)
	if err != nil {
		t.Fatalf("unexpected error clearing data after tests: %v", err)
	}
	if deleted > 0 {
		t.Fatalf("expected all data to be deleted after tests, got %d items", deleted)
	}
}

func expectRowsAffectedEqual(t *testing.T, expected, actual []int) {
	if len(expected) != len(actual) {
		t.Errorf("expected %d rows affected records, got %d", len(expected), len(actual))
	}
	for i, e := range expected {
		if e != actual[i] {
			t.Errorf("index: %d: expected %d rows affected, got %d", i, e, actual[i])
		}
	}
}
