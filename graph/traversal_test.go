package graph_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/a-h/kv/graph"
	"github.com/a-h/kv/sqlitekv"
	"zombiezen.com/go/sqlite/sqlitex"
)

func TestGraphTraversalAlgorithms(t *testing.T) {
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

	g := graph.New(store)

	// Create a test social network:
	// Alice -> Bob -> Charlie
	//   |              ^
	//   +-> Diana -----+
	//
	// Alice also likes some posts.
	users := []string{"alice", "bob", "charlie", "diana"}
	posts := []string{"post1", "post2"}

	for _, userID := range users {
		user := User{ID: userID, Name: userID}
		if err := store.Put(ctx, "user/"+userID, -1, user); err != nil {
			t.Fatalf("failed to put user %s: %v", userID, err)
		}
	}

	for _, postID := range posts {
		post := Post{ID: postID, Title: postID + " title", Content: postID + " content"}
		if err := store.Put(ctx, "post/"+postID, -1, post); err != nil {
			t.Fatalf("failed to put post %s: %v", postID, err)
		}
	}

	// Create follow relationships.
	followRelationships := []struct {
		from, to string
	}{
		{"alice", "bob"},
		{"bob", "charlie"},
		{"alice", "diana"},
		{"diana", "charlie"},
	}

	for _, rel := range followRelationships {
		edge := graph.NewEdge(
			graph.NewNodeRef("User", rel.from),
			graph.NewNodeRef("User", rel.to),
			"follows",
			nil,
		)
		if err := g.AddEdge(ctx, edge); err != nil {
			t.Fatalf("failed to add follow edge %s -> %s: %v", rel.from, rel.to, err)
		}
	}

	// Alice likes some posts.
	likeRelationships := []struct {
		user, post string
		score      int
	}{
		{"alice", "post1", 5},
		{"alice", "post2", 3},
		{"bob", "post1", 4},
	}

	for _, rel := range likeRelationships {
		edgeData, _ := json.Marshal(map[string]any{"score": rel.score})
		edge := graph.NewEdge(
			graph.NewNodeRef("User", rel.user),
			graph.NewNodeRef("Post", rel.post),
			"likes",
			json.RawMessage(edgeData),
		)
		if err := g.AddEdge(ctx, edge); err != nil {
			t.Fatalf("failed to add like edge %s -> %s: %v", rel.user, rel.post, err)
		}
	}

	t.Run("BreadthFirstSearch", func(t *testing.T) {
		paths, err := g.BreadthFirstSearch(ctx, "User", "alice", graph.TraversalOptions{
			MaxDepth:  2,
			EdgeTypes: []string{"follows"},
		})
		if err != nil {
			t.Fatalf("failed to perform BFS: %v", err)
		}

		// Should find: alice, bob, diana, charlie (through bob and diana)
		if len(paths) < 4 {
			t.Fatalf("expected at least 4 paths, got %d", len(paths))
		}

		// First path should be just alice.
		if len(paths[0].Nodes) != 1 || paths[0].Nodes[0].ID != "alice" {
			t.Fatalf("first path should be alice, got %+v", paths[0])
		}

		// Check that charlie is reachable within 2 hops.
		var foundCharlie bool
		for _, path := range paths {
			if len(path.Nodes) > 0 && path.Nodes[len(path.Nodes)-1].ID == "charlie" {
				foundCharlie = true
				if path.Depth > 2 {
					t.Fatalf("charlie should be reachable within 2 hops, got depth %d", path.Depth)
				}
			}
		}
		if !foundCharlie {
			t.Fatalf("charlie should be reachable from alice")
		}
	})

	t.Run("FindShortestPath", func(t *testing.T) {
		path, err := g.FindShortestPath(ctx, "User", "alice", "User", "charlie", graph.TraversalOptions{
			EdgeTypes: []string{"follows"},
		})
		if err != nil {
			t.Fatalf("failed to find shortest path: %v", err)
		}
		if path == nil {
			t.Fatalf("expected to find a path from alice to charlie")
		}

		// Should be alice -> bob -> charlie (depth 2) or alice -> diana -> charlie (depth 2).
		if path.Depth != 2 {
			t.Fatalf("expected shortest path depth of 2, got %d", path.Depth)
		}

		if len(path.Nodes) != 3 {
			t.Fatalf("expected 3 nodes in path, got %d", len(path.Nodes))
		}

		if path.Nodes[0].ID != "alice" {
			t.Fatalf("path should start with alice, got %s", path.Nodes[0].ID)
		}

		if path.Nodes[2].ID != "charlie" {
			t.Fatalf("path should end with charlie, got %s", path.Nodes[2].ID)
		}
	})

	t.Run("FindMutualConnections", func(t *testing.T) {
		mutual, err := g.FindMutualConnections(ctx, "User", "bob", "User", "diana", "follows")
		if err != nil {
			t.Fatalf("failed to find mutual connections: %v", err)
		}

		// Both bob and diana follow charlie.
		if len(mutual) != 1 {
			t.Fatalf("expected 1 mutual connection, got %d", len(mutual))
		}

		if mutual[0].ID != "charlie" {
			t.Fatalf("expected mutual connection to be charlie, got %s", mutual[0].ID)
		}
	})

	t.Run("GetDegree", func(t *testing.T) {
		inDegree, outDegree, err := g.GetDegree(ctx, "User", "charlie", "follows")
		if err != nil {
			t.Fatalf("failed to get degree: %v", err)
		}

		// Charlie is followed by bob and diana (in-degree = 2).
		// Charlie follows no one (out-degree = 0).
		if inDegree != 2 {
			t.Fatalf("expected in-degree of 2 for charlie, got %d", inDegree)
		}

		if outDegree != 0 {
			t.Fatalf("expected out-degree of 0 for charlie, got %d", outDegree)
		}
	})

	t.Run("GetNeighbors", func(t *testing.T) {
		neighbors, err := g.GetNeighbors(ctx, "User", "alice", graph.TraversalOptions{
			EdgeTypes: []string{"follows", "likes"},
		})
		if err != nil {
			t.Fatalf("failed to get neighbors: %v", err)
		}

		// Alice follows bob and diana, likes post1 and post2.
		if len(neighbors) != 4 {
			t.Fatalf("expected 4 neighbors, got %d", len(neighbors))
		}

		neighborIDs := make(map[string]bool)
		for _, neighbor := range neighbors {
			neighborIDs[neighbor.ID] = true
		}

		expected := []string{"bob", "diana", "post1", "post2"}
		for _, expectedID := range expected {
			if !neighborIDs[expectedID] {
				t.Fatalf("expected neighbor %s not found", expectedID)
			}
		}
	})

	t.Run("FilterByProperties", func(t *testing.T) {
		scoreFilter := func(edge graph.Edge) bool {
			if len(edge.Data) == 0 {
				return false
			}
			var data map[string]any
			if err := json.Unmarshal(edge.Data, &data); err != nil {
				return false
			}
			if score, ok := data["score"].(float64); ok {
				return score == 5.0 // JSON marshaling converts int to float64
			}
			return false
		}

		paths, err := g.BreadthFirstSearch(ctx, "User", "alice", graph.TraversalOptions{
			MaxDepth:  1,
			EdgeTypes: []string{"likes"},
			Filter:    scoreFilter,
		})
		if err != nil {
			t.Fatalf("failed to perform BFS with property filter: %v", err)
		}

		// Should only find alice and post1 (score = 5), not post2 (score = 3).
		var foundPost1, foundPost2 bool
		for _, path := range paths {
			for _, node := range path.Nodes {
				if node.ID == "post1" {
					foundPost1 = true
				}
				if node.ID == "post2" {
					foundPost2 = true
				}
			}
		}

		if !foundPost1 {
			t.Fatalf("expected to find post1 with score filter")
		}

		if foundPost2 {
			t.Fatalf("should not find post2 with score filter")
		}
	})

	t.Run("NoPathExists", func(t *testing.T) {
		// Try to find path from charlie to alice (no such path exists).
		path, err := g.FindShortestPath(ctx, "User", "charlie", "User", "alice", graph.TraversalOptions{
			EdgeTypes: []string{"follows"},
		})
		if err != nil {
			t.Fatalf("failed to search for non-existent path: %v", err)
		}

		if path != nil {
			t.Fatalf("expected no path from charlie to alice, but found one")
		}
	})
}

func TestGraphQueries(t *testing.T) {
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

	g := graph.New(store)

	// Create a product recommendation scenario.
	// Users -> buy -> Products
	// Users -> view -> Products
	// Products -> category -> Categories

	users := []string{"user1", "user2", "user3"}
	products := []string{"laptop", "mouse", "keyboard", "monitor"}
	categories := []string{"computers", "accessories"}

	// Add entities.
	for _, userID := range users {
		if err := store.Put(ctx, "user/"+userID, -1, map[string]any{"id": userID}); err != nil {
			t.Fatalf("failed to put user: %v", err)
		}
	}

	for _, productID := range products {
		if err := store.Put(ctx, "product/"+productID, -1, map[string]any{"id": productID}); err != nil {
			t.Fatalf("failed to put product: %v", err)
		}
	}

	for _, categoryID := range categories {
		if err := store.Put(ctx, "category/"+categoryID, -1, map[string]any{"id": categoryID}); err != nil {
			t.Fatalf("failed to put category: %v", err)
		}
	}

	// User purchase behaviors.
	purchases := []struct {
		user, product string
		amount        float64
	}{
		{"user1", "laptop", 1200.0},
		{"user1", "mouse", 25.0},
		{"user2", "laptop", 1200.0},
		{"user2", "keyboard", 80.0},
		{"user3", "monitor", 300.0},
	}

	for _, purchase := range purchases {
		edgeData, _ := json.Marshal(map[string]any{"amount": purchase.amount})
		edge := graph.NewEdge(
			graph.NewNodeRef("User", purchase.user),
			graph.NewNodeRef("Product", purchase.product),
			"bought",
			json.RawMessage(edgeData),
		)
		if err := g.AddEdge(ctx, edge); err != nil {
			t.Fatalf("failed to add purchase edge: %v", err)
		}
	}

	// Product categories.
	productCategories := map[string]string{
		"laptop":   "computers",
		"mouse":    "accessories",
		"keyboard": "accessories",
		"monitor":  "computers",
	}

	for product, category := range productCategories {
		edge := graph.NewEdge(
			graph.NewNodeRef("Product", product),
			graph.NewNodeRef("Category", category),
			"in_category",
			nil,
		)
		if err := g.AddEdge(ctx, edge); err != nil {
			t.Fatalf("failed to add category edge: %v", err)
		}
	}

	t.Run("Find users who bought expensive items", func(t *testing.T) {
		// Find all purchase edges with amount > 100.
		paths, err := g.BreadthFirstSearch(ctx, "User", "user1", graph.TraversalOptions{
			MaxDepth:  1,
			EdgeTypes: []string{"bought"},
		})
		if err != nil {
			t.Fatalf("failed to find purchases: %v", err)
		}

		var expensivePurchases []graph.Path
		for _, path := range paths {
			if len(path.Edges) > 0 {
				var edgeData map[string]any
				if err := json.Unmarshal(path.Edges[0].Data, &edgeData); err == nil {
					if amount, ok := edgeData["amount"].(float64); ok && amount > 100 {
						expensivePurchases = append(expensivePurchases, path)
					}
				}
			}
		}

		// user1 bought laptop (1200.0) which is > 100.
		if len(expensivePurchases) != 1 {
			t.Fatalf("expected 1 expensive purchase, got %d", len(expensivePurchases))
		}
	})

	t.Run("Find products in same category", func(t *testing.T) {
		// Find what category laptop is in.
		var laptopCategories []graph.Edge
		for edge, err := range g.GetOutgoing(ctx, graph.NewNodeRef("Product", "laptop"), "in_category") {
			if err != nil {
				t.Fatalf("failed to get laptop categories: %v", err)
			}
			laptopCategories = append(laptopCategories, edge)
		}

		if len(laptopCategories) != 1 {
			t.Fatalf("expected laptop to be in 1 category, got %d", len(laptopCategories))
		}

		category := laptopCategories[0].To.ID

		// Find all products in the same category.
		var categoryProducts []graph.Edge
		for edge, err := range g.GetIncoming(ctx, graph.NewNodeRef("Category", category), "in_category") {
			if err != nil {
				t.Fatalf("failed to get products in category: %v", err)
			}
			categoryProducts = append(categoryProducts, edge)
		}

		// Should find laptop and monitor (both in computers category).
		if len(categoryProducts) != 2 {
			t.Fatalf("expected 2 products in computers category, got %d", len(categoryProducts))
		}

		productIDs := make(map[string]bool)
		for _, edge := range categoryProducts {
			productIDs[edge.From.ID] = true
		}

		if !productIDs["laptop"] || !productIDs["monitor"] {
			t.Fatalf("expected laptop and monitor in computers category")
		}
	})

	t.Run("Find similar users", func(t *testing.T) {
		// Find users who bought the same product as user1.
		var user1Purchases []graph.Edge
		for edge, err := range g.GetOutgoing(ctx, graph.NewNodeRef("User", "user1"), "bought") {
			if err != nil {
				t.Fatalf("failed to get user1 purchases: %v", err)
			}
			user1Purchases = append(user1Purchases, edge)
		}

		similarUsers := make(map[string]int) // userID -> number of common products

		for _, purchase := range user1Purchases {
			// Find who else bought this product.
			var otherBuyers []graph.Edge
			for edge, err := range g.GetIncoming(ctx, graph.NewNodeRef("Product", purchase.To.ID), "bought") {
				if err != nil {
					t.Fatalf("failed to get other buyers: %v", err)
				}
				otherBuyers = append(otherBuyers, edge)
			}

			for _, buyer := range otherBuyers {
				if buyer.From.ID != "user1" { // Exclude user1 themselves
					similarUsers[buyer.From.ID]++
				}
			}
		}

		// user2 also bought laptop, so should be similar to user1.
		if similarUsers["user2"] != 1 {
			t.Fatalf("expected user2 to have 1 common product with user1, got %d", similarUsers["user2"])
		}
	})
}
