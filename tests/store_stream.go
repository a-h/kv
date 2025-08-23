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
			actual, err := store.Stream(ctx, kv.TypeAll, startSeq, -1)
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
			actual, err := store.Stream(ctx, kv.TypeAll, startSeq, 2)
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
			actual, err := store.Stream(ctx, kv.TypeAll, startSeq+2, -1)
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
		t.Run("Can filter stream records by type", func(t *testing.T) {
			if err := store.StreamTrim(ctx, -1); err != nil {
				t.Fatalf("could not trim stream: %v", err)
			}

			type Weapon struct {
				Name   string
				Damage int
			}
			type Health struct {
				Current int
				Max     int
			}
			type Position struct {
				X, Y float64
			}

			startSeq, err := store.StreamSeq(ctx)
			if err != nil {
				t.Fatalf("could not get stream sequence: %v", err)
			}

			weapons := []Weapon{
				{Name: "Sword", Damage: 10},
				{Name: "Axe", Damage: 15},
			}
			healths := []Health{
				{Current: 80, Max: 100},
				{Current: 60, Max: 100},
			}
			positions := []Position{
				{X: 10.5, Y: 20.3},
			}

			for i, weapon := range weapons {
				if err := store.Put(ctx, "entity/"+string(rune('1'+i))+"/weapon", -1, weapon); err != nil {
					t.Fatalf("failed to put weapon: %v", err)
				}
			}
			for i, health := range healths {
				if err := store.Put(ctx, "entity/"+string(rune('1'+i))+"/health", -1, health); err != nil {
					t.Fatalf("failed to put health: %v", err)
				}
			}
			for i, position := range positions {
				if err := store.Put(ctx, "entity/"+string(rune('1'+i))+"/position", -1, position); err != nil {
					t.Fatalf("failed to put position: %v", err)
				}
			}

			allResults, err := store.Stream(ctx, kv.TypeAll, startSeq, -1)
			if err != nil {
				t.Fatalf("failed to get all stream records: %v", err)
			}
			if len(allResults) != 5 {
				t.Fatalf("expected 5 total records, got %d", len(allResults))
			}

			weaponResults, err := store.Stream(ctx, "Weapon", startSeq, -1)
			if err != nil {
				t.Fatalf("failed to get weapon stream records: %v", err)
			}
			if len(weaponResults) != 2 {
				t.Fatalf("expected 2 weapon records, got %d", len(weaponResults))
			}
			for _, record := range weaponResults {
				if record.Record.Type != "Weapon" {
					t.Fatalf("expected weapon type, got %s", record.Record.Type)
				}
			}

			healthResults, err := store.Stream(ctx, "Health", startSeq, -1)
			if err != nil {
				t.Fatalf("failed to get health stream records: %v", err)
			}
			if len(healthResults) != 2 {
				t.Fatalf("expected 2 health records, got %d", len(healthResults))
			}
			for _, record := range healthResults {
				if record.Record.Type != "Health" {
					t.Fatalf("expected health type, got %s", record.Record.Type)
				}
			}

			positionResults, err := store.Stream(ctx, "Position", startSeq, -1)
			if err != nil {
				t.Fatalf("failed to get position stream records: %v", err)
			}
			if len(positionResults) != 1 {
				t.Fatalf("expected 1 position record, got %d", len(positionResults))
			}
			if positionResults[0].Record.Type != "Position" {
				t.Fatalf("expected position type, got %s", positionResults[0].Record.Type)
			}
		})
		t.Run("Returns empty results for non-existent type", func(t *testing.T) {
			startSeq, err := store.StreamSeq(ctx)
			if err != nil {
				t.Fatalf("could not get stream sequence: %v", err)
			}

			noResults, err := store.Stream(ctx, "NonExistent", startSeq, -1)
			if err != nil {
				t.Fatalf("failed to get non-existent type stream records: %v", err)
			}
			if len(noResults) != 0 {
				t.Fatalf("expected 0 records for non-existent type, got %d", len(noResults))
			}
		})
	}
}
