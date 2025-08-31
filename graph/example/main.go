package main

import (
	"context"
	"fmt"
	"log"

	"github.com/a-h/kv"
	"github.com/a-h/kv/graph"
	"github.com/a-h/kv/sqlitekv"
	"zombiezen.com/go/sqlite/sqlitex"
)

// ECS components.
type Position struct {
	X int `json:"x"`
	Y int `json:"y"`
}

type Health struct {
	Current int `json:"current"`
	Max     int `json:"max"`
}

type Weapon struct {
	Name   string `json:"name"`
	Damage int    `json:"damage"`
}

// Game entities.
type Player struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type Team struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type Guild struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

func main() {
	ctx := context.Background()

	// Initialize store.
	pool, err := sqlitex.NewPool("file:example.db?mode=rwc", sqlitex.PoolOptions{})
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	store := sqlitekv.NewStore(pool)
	if err := store.Init(ctx); err != nil {
		log.Fatal(err)
	}

	// Initialize graph.
	g := graph.New(store)

	// Create some players.
	players := []Player{
		{ID: "player1", Name: "Alice"},
		{ID: "player2", Name: "Bob"},
		{ID: "player3", Name: "Charlie"},
	}

	// Create teams and guilds.
	team := Team{ID: "team1", Name: "Red Team"}
	guild := Guild{ID: "guild1", Name: "Dragon Slayers"}

	// Store entities.
	for _, player := range players {
		if err := store.Put(ctx, "player/"+player.ID, -1, player); err != nil {
			log.Fatal(err)
		}
	}
	if err := store.Put(ctx, "team/"+team.ID, -1, team); err != nil {
		log.Fatal(err)
	}
	if err := store.Put(ctx, "guild/"+guild.ID, -1, guild); err != nil {
		log.Fatal(err)
	}

	// ECS: Add components to entities.
	components := map[string]any{
		"entity/player1/position": Position{X: 10, Y: 20},
		"entity/player1/health":   Health{Current: 100, Max: 100},
		"entity/player1/weapon":   Weapon{Name: "Sword", Damage: 15},
		"entity/player2/position": Position{X: 15, Y: 25},
		"entity/player2/health":   Health{Current: 80, Max: 100},
		"entity/player2/weapon":   Weapon{Name: "Bow", Damage: 12},
		"entity/player3/position": Position{X: 5, Y: 30},
		"entity/player3/health":   Health{Current: 90, Max: 100},
	}

	for key, component := range components {
		if err := store.Put(ctx, key, -1, component); err != nil {
			log.Fatal(err)
		}
	}

	// Graph: Add relationships between entities.
	relationships := []graph.Edge{
		// Team memberships.
		{
			FromEntityType: "Player", FromEntityID: "player1",
			ToEntityType: "Team", ToEntityID: "team1",
			Type: "member_of",
		},
		{
			FromEntityType: "Player", FromEntityID: "player2",
			ToEntityType: "Team", ToEntityID: "team1",
			Type: "member_of",
		},
		// Guild memberships.
		{
			FromEntityType: "Player", FromEntityID: "player1",
			ToEntityType: "Guild", ToEntityID: "guild1",
			Type: "member_of",
		},
		{
			FromEntityType: "Player", FromEntityID: "player3",
			ToEntityType: "Guild", ToEntityID: "guild1",
			Type: "member_of",
		},
		// Friendships.
		{
			FromEntityType: "Player", FromEntityID: "player1",
			ToEntityType: "Player", ToEntityID: "player2",
			Type: "friends_with",
			Properties: map[string]any{"since": "2024-01-01"},
		},
		{
			FromEntityType: "Player", FromEntityID: "player2",
			ToEntityType: "Player", ToEntityID: "player1",
			Type: "friends_with",
			Properties: map[string]any{"since": "2024-01-01"},
		},
		// Rivalries.
		{
			FromEntityType: "Player", FromEntityID: "player1",
			ToEntityType: "Player", ToEntityID: "player3",
			Type: "rival_of",
			Properties: map[string]any{"intensity": 7},
		},
	}

	for _, rel := range relationships {
		if err := g.AddEdge(ctx, rel); err != nil {
			log.Fatal(err)
		}
	}

	// Now demonstrate combined ECS + Graph queries.
	fmt.Println("=== ECS + Graph Example ===")

	// 1. Get all team members and their components.
	fmt.Println("\n1. Team Red Team members and their stats:")
	teamMembers, err := g.GetIncomingEdges(ctx, "Team", "team1", "member_of")
	if err != nil {
		log.Fatal(err)
	}

	for _, member := range teamMembers {
		playerID := member.FromEntityID
		fmt.Printf("  Player: %s\n", playerID)

		// Get player's components.
		playerComponents, err := store.GetPrefix(ctx, "entity/"+playerID+"/", 0, -1)
		if err != nil {
			log.Fatal(err)
		}

		for _, component := range playerComponents {
			switch component.Type {
			case "Position":
				var pos Position
				if pos, err = kv.ValueOf[Position](component); err == nil {
					fmt.Printf("    Position: (%d, %d)\n", pos.X, pos.Y)
				}
			case "Health":
				var health Health
				if health, err = kv.ValueOf[Health](component); err == nil {
					fmt.Printf("    Health: %d/%d\n", health.Current, health.Max)
				}
			case "Weapon":
				var weapon Weapon
				if weapon, err = kv.ValueOf[Weapon](component); err == nil {
					fmt.Printf("    Weapon: %s (Damage: %d)\n", weapon.Name, weapon.Damage)
				}
			}
		}
	}

	// 2. Find players who are friends and check if they're close in position.
	fmt.Println("\n2. Friend proximity analysis:")
	friendships, err := g.GetOutgoingEdges(ctx, "Player", "player1", "friends_with")
	if err != nil {
		log.Fatal(err)
	}

	for _, friendship := range friendships {
		friendID := friendship.ToEntityID
		fmt.Printf("  %s is friends with %s\n", "player1", friendID)

		// Get both players' positions.
		var pos1, pos2 Position
		_, ok1, err1 := store.Get(ctx, "entity/player1/position", &pos1)
		_, ok2, err2 := store.Get(ctx, "entity/"+friendID+"/position", &pos2)

		if err1 == nil && err2 == nil && ok1 && ok2 {
			distance := abs(pos1.X-pos2.X) + abs(pos1.Y-pos2.Y) // Manhattan distance
			fmt.Printf("    Distance: %d units\n", distance)
			if distance < 10 {
				fmt.Printf("    They are close enough to help each other!\n")
			}
		}
	}

	// 3. Find guild members who might form alliances (not rivals).
	fmt.Println("\n3. Potential guild alliances:")
	guildMembers, err := g.GetIncomingEdges(ctx, "Guild", "guild1", "member_of")
	if err != nil {
		log.Fatal(err)
	}

	for i, member1 := range guildMembers {
		for j, member2 := range guildMembers {
			if i >= j {
				continue // Avoid duplicates and self-comparison
			}

			player1ID := member1.FromEntityID
			player2ID := member2.FromEntityID

			// Check if they're rivals.
			_, isRival, err := g.GetEdge(ctx, "Player", player1ID, "rival_of", "Player", player2ID)
			if err != nil {
				continue
			}

			if !isRival {
				// Check reverse direction too.
				_, isRivalReverse, err := g.GetEdge(ctx, "Player", player2ID, "rival_of", "Player", player1ID)
				if err != nil {
					continue
				}

				if !isRivalReverse {
					fmt.Printf("  %s and %s could form an alliance (no rivalry detected)\n", player1ID, player2ID)
				}
			}
		}
	}

	// 4. Combat effectiveness calculation using both ECS and relationships.
	fmt.Println("\n4. Combat effectiveness (Health + Weapon + Team bonus):")
	for _, player := range players {
		playerID := player.ID
		
		var health Health
		var weapon Weapon
		var effectiveness int

		// Get health component.
		if _, ok, err := store.Get(ctx, "entity/"+playerID+"/health", &health); err == nil && ok {
			effectiveness += health.Current
		}

		// Get weapon component.
		if _, ok, err := store.Get(ctx, "entity/"+playerID+"/weapon", &weapon); err == nil && ok {
			effectiveness += weapon.Damage * 2 // Weapon damage counts double
		}

		// Team bonus - get team members and add team size bonus.
		playerTeams, err := g.GetOutgoingEdges(ctx, "Player", playerID, "member_of")
		if err == nil {
			for _, teamEdge := range playerTeams {
				if teamEdge.ToEntityType == "Team" {
					// Count team members.
					teamMembers, err := g.GetIncomingEdges(ctx, "Team", teamEdge.ToEntityID, "member_of")
					if err == nil {
						teamBonus := len(teamMembers) * 5 // 5 points per team member
						effectiveness += teamBonus
						fmt.Printf("  %s: Base: %d, Team bonus: %d, Total: %d\n", 
							player.Name, effectiveness-teamBonus, teamBonus, effectiveness)
						break
					}
				}
			}
		}

		if len(playerTeams) == 0 {
			fmt.Printf("  %s: Base: %d, Team bonus: 0, Total: %d (no team)\n", 
				player.Name, effectiveness, effectiveness)
		}
	}

	fmt.Println("\n=== Example Complete ===")
}

func abs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}
