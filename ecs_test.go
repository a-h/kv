package kv_test

import (
	"context"
	"testing"

	"github.com/a-h/kv"
	"github.com/a-h/kv/sqlitekv"
	"zombiezen.com/go/sqlite/sqlitex"
)

type Weapon struct {
	Name   string `json:"name"`
	Damage int    `json:"damage"`
}

type Health struct {
	Current int `json:"current"`
	Max     int `json:"max"`
}

type Position struct {
	X int `json:"x"`
	Y int `json:"y"`
}

func TestEntityComponentSystem(t *testing.T) {
	ctx := context.Background()

	pool, err := sqlitex.NewPool("file::memory:?mode=memory&cache=shared", sqlitex.PoolOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	store := sqlitekv.NewStore(pool)
	if err := store.Init(ctx); err != nil {
		t.Fatalf("failed to init store: %v", err)
	}

	// Entity 1 - has weapon and position.
	if err := store.Put(ctx, "entity/1/weapon", -1, Weapon{Name: "Sword", Damage: 10}); err != nil {
		t.Fatalf("failed to put weapon: %v", err)
	}
	if err := store.Put(ctx, "entity/1/position", -1, Position{X: 5, Y: 10}); err != nil {
		t.Fatalf("failed to put position: %v", err)
	}

	// Entity 2 - has weapon, health, and position.
	if err := store.Put(ctx, "entity/2/weapon", -1, Weapon{Name: "Bow", Damage: 8}); err != nil {
		t.Fatalf("failed to put weapon: %v", err)
	}
	if err := store.Put(ctx, "entity/2/health", -1, Health{Current: 80, Max: 100}); err != nil {
		t.Fatalf("failed to put health: %v", err)
	}
	if err := store.Put(ctx, "entity/2/position", -1, Position{X: 15, Y: 20}); err != nil {
		t.Fatalf("failed to put position: %v", err)
	}

	// Entity 3 - only has health.
	if err := store.Put(ctx, "entity/3/health", -1, Health{Current: 100, Max: 100}); err != nil {
		t.Fatalf("failed to put health: %v", err)
	}

	t.Run("Can get entity by prefix", func(t *testing.T) {
		records, err := store.GetPrefix(ctx, "entity/1/", 0, -1)
		if err != nil {
			t.Fatalf("failed to get entity/1/ prefix: %v", err)
		}
		if len(records) != 2 {
			t.Fatalf("expected 2 components for entity/1, got %d", len(records))
		}

		var foundWeapon, foundPosition bool
		for _, record := range records {
			switch record.Type {
			case "Weapon":
				foundWeapon = true
			case "Position":
				foundPosition = true
			}
		}

		if !foundWeapon || !foundPosition {
			t.Fatalf("expected to find Weapon and Position components, found weapon=%v, position=%v", foundWeapon, foundPosition)
		}
	})
	t.Run("Can get all weapons by type", func(t *testing.T) {
		records, err := store.GetType(ctx, "Weapon", 0, -1)
		if err != nil {
			t.Fatalf("failed to get weapons by type: %v", err)
		}
		if len(records) != 2 {
			t.Fatalf("expected 2 weapon components, got %d", len(records))
		}

		weapons, err := kv.ValuesOf[Weapon](records)
		if err != nil {
			t.Fatalf("failed to convert to weapons: %v", err)
		}

		weaponNames := make(map[string]bool)
		for _, weapon := range weapons {
			weaponNames[weapon.Name] = true
		}

		if !weaponNames["Sword"] || !weaponNames["Bow"] {
			t.Fatalf("expected to find Sword and Bow weapons, got: %v", weaponNames)
		}
	})
	t.Run("Can get all health components", func(t *testing.T) {
		records, err := store.GetType(ctx, "Health", 0, -1)
		if err != nil {
			t.Fatalf("failed to get health by type: %v", err)
		}

		if len(records) != 2 {
			t.Fatalf("expected 2 health components, got %d", len(records))
		}
	})
	t.Run("Can reflect on types", func(t *testing.T) {
		tests := []struct {
			name     string
			value    any
			expected string
		}{
			{
				name:     "struct value",
				value:    Weapon{Name: "Axe", Damage: 12},
				expected: "Weapon",
			},
			{
				name:     "struct pointer",
				value:    &Health{Current: 50, Max: 100},
				expected: "Health",
			},
			{
				name:     "string",
				value:    "hello",
				expected: "string",
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				typeName := kv.TypeOf(tt.value)
				if typeName != tt.expected {
					t.Fatalf("expected type name '%s', got '%s'", tt.expected, typeName)
				}
			})
		}
	})
}
