package main

import (
	"context"
	"encoding/json"
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
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx := context.Background()

	// Initialize store.
	pool, err := sqlitex.NewPool("file:example.db?mode=rwc", sqlitex.PoolOptions{})
	if err != nil {
		return fmt.Errorf("failed to create pool: %w", err)
	}
	defer pool.Close()

	store := sqlitekv.NewStore(pool)
	if err := store.Init(ctx); err != nil {
		return fmt.Errorf("failed to init store: %w", err)
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
			return fmt.Errorf("failed to store player %s: %w", player.ID, err)
		}
	}
	if err := store.Put(ctx, "team/"+team.ID, -1, team); err != nil {
		return fmt.Errorf("failed to store team %s: %w", team.ID, err)
	}
	if err := store.Put(ctx, "guild/"+guild.ID, -1, guild); err != nil {
		return fmt.Errorf("failed to store guild %s: %w", guild.ID, err)
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
			return fmt.Errorf("failed to store component %s: %w", key, err)
		}
	}

	// Graph: Add relationships between entities.
	relationships := []graph.Edge{
		// Team memberships.
		graph.NewEdge(
			graph.NewNodeRef("player1", "Player"),
			graph.NewNodeRef("team1", "Team"),
			"member_of",
			nil,
		),
		graph.NewEdge(
			graph.NewNodeRef("player2", "Player"),
			graph.NewNodeRef("team1", "Team"),
			"member_of",
			nil,
		),
		// Guild memberships.
		graph.NewEdge(
			graph.NewNodeRef("player1", "Player"),
			graph.NewNodeRef("guild1", "Guild"),
			"member_of",
			nil,
		),
		graph.NewEdge(
			graph.NewNodeRef("player3", "Player"),
			graph.NewNodeRef("guild1", "Guild"),
			"member_of",
			nil,
		),
		// Friendships.
		graph.NewEdge(
			graph.NewNodeRef("player1", "Player"),
			graph.NewNodeRef("player2", "Player"),
			"friends_with",
			json.RawMessage(`{"since": "2024-01-01"}`),
		),
		graph.NewEdge(
			graph.NewNodeRef("player2", "Player"),
			graph.NewNodeRef("player1", "Player"),
			"friends_with",
			json.RawMessage(`{"since": "2024-01-01"}`),
		),
		// Rivalries.
		graph.NewEdge(
			graph.NewNodeRef("player1", "Player"),
			graph.NewNodeRef("player3", "Player"),
			"rival_of",
			json.RawMessage(`{"intensity": 7}`),
		),
	}

	for _, rel := range relationships {
		if err := g.AddEdge(ctx, rel); err != nil {
			return fmt.Errorf("failed to add edge: %w", err)
		}
	}

	// Now demonstrate combined ECS + Graph queries.
	fmt.Println("=== ECS + Graph Example ===")

	// 1. Get all team members and their components.
	fmt.Println("\n1. Team Red Team members and their stats:")
	var teamMembers []graph.Edge
	for edge, err := range g.GetIncoming(ctx, graph.NewNodeRef("team1", "Team"), "member_of") {
		if err != nil {
			return fmt.Errorf("failed to get team members: %w", err)
		}
		teamMembers = append(teamMembers, edge)
	}

	for _, member := range teamMembers {
		playerID := member.From.ID
		fmt.Printf("  Player: %s\n", playerID)

		// Get player's components.
		playerComponents, err := store.GetPrefix(ctx, "entity/"+playerID+"/", 0, -1)
		if err != nil {
			return fmt.Errorf("failed to get player components for %s: %w", playerID, err)
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
	var friendships []graph.Edge
	for edge, err := range g.GetOutgoing(ctx, graph.NewNodeRef("player1", "Player"), "friends_with") {
		if err != nil {
			return fmt.Errorf("failed to get friendships: %w", err)
		}
		friendships = append(friendships, edge)
	}

	for _, friendship := range friendships {
		friendID := friendship.To.ID
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
	var guildMembers []graph.Edge
	for edge, err := range g.GetIncoming(ctx, graph.NewNodeRef("guild1", "Guild"), "member_of") {
		if err != nil {
			return fmt.Errorf("failed to get guild members: %w", err)
		}
		guildMembers = append(guildMembers, edge)
	}

	for i, member1 := range guildMembers {
		for j, member2 := range guildMembers {
			if i >= j {
				continue // Avoid duplicates and self-comparison
			}

			player1ID := member1.From.ID
			player2ID := member2.From.ID

			// Check if they're rivals.
			player1Node := graph.NewNodeRef(player1ID, "Player")
			player2Node := graph.NewNodeRef(player2ID, "Player")
			_, isRival, err := g.GetEdge(ctx, player1Node, "rival_of", player2Node)
			if err != nil {
				continue
			}

			if !isRival {
				// Check reverse direction too.
				_, isRivalReverse, err := g.GetEdge(ctx, player2Node, "rival_of", player1Node)
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
		var playerTeams []graph.Edge
		for edge, err := range g.GetOutgoing(ctx, graph.NewNodeRef(playerID, "Player"), "member_of") {
			if err == nil {
				playerTeams = append(playerTeams, edge)
			}
		}
		if len(playerTeams) > 0 {
			for _, teamEdge := range playerTeams {
				if teamEdge.To.Type == "Team" {
					// Count team members.
					var teamMembers []graph.Edge
					for edge, err := range g.GetIncoming(ctx, graph.NewNodeRef(teamEdge.To.ID, "Team"), "member_of") {
						if err == nil {
							teamMembers = append(teamMembers, edge)
						}
					}
					if len(teamMembers) > 0 {
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

	// Note: The range-over patterns used throughout this example (e.g., g.GetIncoming, g.GetOutgoing)
	// demonstrate the streaming graph APIs that provide real-time iteration over graph data.

	return nil
}

func abs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}
