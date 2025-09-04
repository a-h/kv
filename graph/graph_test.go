package graph_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/a-h/kv/graph"
	"github.com/a-h/kv/sqlitekv"
	"zombiezen.com/go/sqlite/sqlitex"
)

// Example entities for testing.
type User struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type Post struct {
	ID      string `json:"id"`
	Title   string `json:"title"`
	Content string `json:"content"`
}

type Comment struct {
	ID      string `json:"id"`
	Content string `json:"content"`
}

func TestGraph(t *testing.T) {
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

	// Create some test entities.
	user1 := User{ID: "user1", Name: "Alice"}
	user2 := User{ID: "user2", Name: "Bob"}
	post1 := Post{ID: "post1", Title: "Hello World", Content: "First post"}
	comment1 := Comment{ID: "comment1", Content: "Great post!"}

	// Store entities in the KV store.
	if err := store.Put(ctx, "user/user1", -1, user1); err != nil {
		t.Fatalf("failed to put user1: %v", err)
	}
	if err := store.Put(ctx, "user/user2", -1, user2); err != nil {
		t.Fatalf("failed to put user2: %v", err)
	}
	if err := store.Put(ctx, "post/post1", -1, post1); err != nil {
		t.Fatalf("failed to put post1: %v", err)
	}
	if err := store.Put(ctx, "comment/comment1", -1, comment1); err != nil {
		t.Fatalf("failed to put comment1: %v", err)
	}

	t.Run("Create and retrieve edges", func(t *testing.T) {
		// User1 follows User2.
		followEdge := graph.NewEdge(
			graph.NewNodeRef("User", "user1"),
			graph.NewNodeRef("User", "user2"),
			"follows",
			json.RawMessage(`{"since": "2024-01-01"}`),
		)

		if err := g.AddEdge(ctx, followEdge); err != nil {
			t.Fatalf("failed to add follow edge: %v", err)
		}

		// User1 authored Post1.
		authorEdge := graph.NewEdge(
			graph.NewNodeRef("User", "user1"),
			graph.NewNodeRef("Post", "post1"),
			"authored",
			nil,
		)

		if err := g.AddEdge(ctx, authorEdge); err != nil {
			t.Fatalf("failed to add author edge: %v", err)
		}

		// User2 commented on Post1.
		commentedEdge := graph.NewEdge(
			graph.NewNodeRef("User", "user2"),
			graph.NewNodeRef("Comment", "comment1"),
			"authored",
			nil,
		)

		if err := g.AddEdge(ctx, commentedEdge); err != nil {
			t.Fatalf("failed to add commented edge: %v", err)
		}

		// Comment1 is on Post1.
		commentOnPostEdge := graph.NewEdge(
			graph.NewNodeRef("Comment", "comment1"),
			graph.NewNodeRef("Post", "post1"),
			"on",
			nil,
		)

		if err := g.AddEdge(ctx, commentOnPostEdge); err != nil {
			t.Fatalf("failed to add comment-on-post edge: %v", err)
		}

		// Retrieve specific edge.
		user1Node := graph.NewNodeRef("User", "user1")
		user2Node := graph.NewNodeRef("User", "user2")
		retrievedEdge, ok, err := g.GetEdge(ctx, user1Node, "follows", user2Node)
		if err != nil {
			t.Fatalf("failed to get edge: %v", err)
		}
		if !ok {
			t.Fatalf("expected edge to exist")
		}
		// Check edge data.
		var edgeData map[string]any
		if err := json.Unmarshal(retrievedEdge.Data, &edgeData); err != nil {
			t.Fatalf("failed to unmarshal edge data: %v", err)
		}
		if edgeData["since"] != "2024-01-01" {
			t.Fatalf("expected property 'since' to be '2024-01-01', got %v", edgeData["since"])
		}
	})

	t.Run("Get outgoing edges", func(t *testing.T) {
		user1Node := graph.NewNodeRef("User", "user1")
		// Get all users that user1 follows.
		var followEdges []graph.Edge
		for edge, err := range g.GetOutgoing(ctx, user1Node, "follows") {
			if err != nil {
				t.Fatalf("failed to get outgoing follow edges: %v", err)
			}
			followEdges = append(followEdges, edge)
		}
		if len(followEdges) != 1 {
			t.Fatalf("expected 1 follow edge, got %d", len(followEdges))
		}
		if followEdges[0].To.ID != "user2" {
			t.Fatalf("expected to follow user2, got %s", followEdges[0].To.ID)
		}

		// Get all posts authored by user1.
		var authoredEdges []graph.Edge
		for edge, err := range g.GetOutgoing(ctx, user1Node, "authored") {
			if err != nil {
				t.Fatalf("failed to get outgoing authored edges: %v", err)
			}
			authoredEdges = append(authoredEdges, edge)
		}
		if len(authoredEdges) != 1 {
			t.Fatalf("expected 1 authored edge, got %d", len(authoredEdges))
		}
		if authoredEdges[0].To.ID != "post1" {
			t.Fatalf("expected to author post1, got %s", authoredEdges[0].To.ID)
		}
	})

	t.Run("Get incoming edges", func(t *testing.T) {
		user2Node := graph.NewNodeRef("User", "user2")
		// Get all users who follow user2.
		var followersEdges []graph.Edge
		for edge, err := range g.GetIncoming(ctx, user2Node, "follows") {
			if err != nil {
				t.Fatalf("failed to get incoming follow edges: %v", err)
			}
			followersEdges = append(followersEdges, edge)
		}
		if len(followersEdges) != 1 {
			t.Fatalf("expected 1 follower edge, got %d", len(followersEdges))
		}
		if followersEdges[0].From.ID != "user1" {
			t.Fatalf("expected follower to be user1, got %s", followersEdges[0].From.ID)
		}

		// Get who authored post1.
		var authorEdges []graph.Edge
		post1Node := graph.NewNodeRef("Post", "post1")
		for edge, err := range g.GetIncoming(ctx, post1Node, "authored") {
			if err != nil {
				t.Fatalf("failed to get incoming authored edges: %v", err)
			}
			authorEdges = append(authorEdges, edge)
		}
		if len(authorEdges) != 1 {
			t.Fatalf("expected 1 author edge, got %d", len(authorEdges))
		}
		if authorEdges[0].From.ID != "user1" {
			t.Fatalf("expected author to be user1, got %s", authorEdges[0].From.ID)
		}
	})

	t.Run("Get all edges for entity", func(t *testing.T) {
		// Get all outgoing edges from user1.
		var allOutgoing []graph.Edge
		user1Node := graph.NewNodeRef("User", "user1")
		for edge, err := range g.GetAllOutgoing(ctx, user1Node) {
			if err != nil {
				t.Fatalf("failed to get all outgoing edges: %v", err)
			}
			allOutgoing = append(allOutgoing, edge)
		}
		if len(allOutgoing) != 2 { // follows and authored
			t.Fatalf("expected 2 outgoing edges, got %d", len(allOutgoing))
		}

		// Get all incoming edges to post1.
		var allIncoming []graph.Edge
		post1Node := graph.NewNodeRef("Post", "post1")
		for edge, err := range g.GetAllIncoming(ctx, post1Node) {
			if err != nil {
				t.Fatalf("failed to get all incoming edges: %v", err)
			}
			allIncoming = append(allIncoming, edge)
		}
		if len(allIncoming) != 2 { // authored by user1, comment from comment1
			t.Fatalf("expected 2 incoming edges, got %d", len(allIncoming))
		}
	})

	t.Run("Remove edges", func(t *testing.T) {
		// Remove the follow relationship.
		user1Node := graph.NewNodeRef("User", "user1")
		user2Node := graph.NewNodeRef("User", "user2")
		if err := g.RemoveEdge(ctx, user1Node, "follows", user2Node); err != nil {
			t.Fatalf("failed to remove follow edge: %v", err)
		}

		// Verify it's gone.
		_, ok, err := g.GetEdge(ctx, user1Node, "follows", user2Node)
		if err != nil {
			t.Fatalf("failed to check edge existence: %v", err)
		}
		if ok {
			t.Fatalf("expected edge to be removed")
		}

		// Verify outgoing edges list is updated.
		var followEdges []graph.Edge
		for edge, err := range g.GetOutgoing(ctx, user1Node, "follows") {
			if err != nil {
				t.Fatalf("failed to get outgoing follow edges: %v", err)
			}
			followEdges = append(followEdges, edge)
		}
		if len(followEdges) != 0 {
			t.Fatalf("expected 0 follow edges after removal, got %d", len(followEdges))
		}

		// Verify incoming edges list is updated.
		var followersEdges []graph.Edge
		for edge, err := range g.GetIncoming(ctx, user2Node, "follows") {
			if err != nil {
				t.Fatalf("failed to get incoming follow edges: %v", err)
			}
			followersEdges = append(followersEdges, edge)
		}
		if len(followersEdges) != 0 {
			t.Fatalf("expected 0 follower edges after removal, got %d", len(followersEdges))
		}
	})
}

func TestGraphTraversal(t *testing.T) {
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

	// Create a small social network.
	users := []User{
		{ID: "alice", Name: "Alice"},
		{ID: "bob", Name: "Bob"},
		{ID: "charlie", Name: "Charlie"},
		{ID: "diana", Name: "Diana"},
	}

	for _, user := range users {
		if err := store.Put(ctx, "user/"+user.ID, -1, user); err != nil {
			t.Fatalf("failed to put user %s: %v", user.ID, err)
		}
	}

	// Create follow relationships: Alice -> Bob -> Charlie -> Diana -> Alice (cycle).
	relationships := []struct {
		from, to string
	}{
		{"alice", "bob"},
		{"bob", "charlie"},
		{"charlie", "diana"},
		{"diana", "alice"},
		{"alice", "charlie"}, // Alice also follows Charlie directly
	}

	for _, rel := range relationships {
		edge := graph.NewEdge(
			graph.NewNodeRef("User", rel.from),
			graph.NewNodeRef("User", rel.to),
			"follows",
			nil,
		)
		if err := g.AddEdge(ctx, edge); err != nil {
			t.Fatalf("failed to add edge %s -> %s: %v", rel.from, rel.to, err)
		}
	}

	t.Run("Find who Alice follows", func(t *testing.T) {
		var following []graph.Edge
		aliceNode := graph.NewNodeRef("User", "alice")
		for edge, err := range g.GetOutgoing(ctx, aliceNode, "follows") {
			if err != nil {
				t.Fatalf("failed to get Alice's following: %v", err)
			}
			following = append(following, edge)
		}

		if len(following) != 2 {
			t.Fatalf("expected Alice to follow 2 people, got %d", len(following))
		}

		followingIDs := make(map[string]bool)
		for _, edge := range following {
			followingIDs[edge.To.ID] = true
		}

		if !followingIDs["bob"] || !followingIDs["charlie"] {
			t.Fatalf("expected Alice to follow Bob and Charlie, got %v", followingIDs)
		}
	})

	t.Run("Find Alice's followers", func(t *testing.T) {
		var followers []graph.Edge
		aliceNode := graph.NewNodeRef("User", "alice")
		for edge, err := range g.GetIncoming(ctx, aliceNode, "follows") {
			if err != nil {
				t.Fatalf("failed to get Alice's followers: %v", err)
			}
			followers = append(followers, edge)
		}

		if len(followers) != 1 {
			t.Fatalf("expected Alice to have 1 follower, got %d", len(followers))
		}

		if followers[0].From.ID != "diana" {
			t.Fatalf("expected Diana to follow Alice, got %s", followers[0].From.ID)
		}
	})
}

func TestStreamingMethods(t *testing.T) {
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

	// Create test edges.
	edges := []graph.Edge{
		graph.NewEdge(
			graph.NewNodeRef("User", "alice"),
			graph.NewNodeRef("User", "bob"),
			"follows",
			json.RawMessage(`{"since": "2024-01-01"}`),
		),
		graph.NewEdge(
			graph.NewNodeRef("User", "alice"),
			graph.NewNodeRef("Post", "post1"),
			"authored",
			nil,
		),
		graph.NewEdge(
			graph.NewNodeRef("User", "bob"),
			graph.NewNodeRef("User", "alice"),
			"follows",
			nil,
		),
		graph.NewEdge(
			graph.NewNodeRef("User", "bob"),
			graph.NewNodeRef("Post", "post2"),
			"authored",
			nil,
		),
	}

	for _, edge := range edges {
		if err := g.AddEdge(ctx, edge); err != nil {
			t.Fatalf("failed to add edge: %v", err)
		}
	}

	t.Run("returns only edges of specified type when filtering outgoing edges", func(t *testing.T) {
		var collectedEdges []graph.Edge
		var collectedErrors []error

		aliceNode := graph.NewNodeRef("User", "alice")
		for edge, err := range g.GetOutgoing(ctx, aliceNode, "follows") {
			if err != nil {
				collectedErrors = append(collectedErrors, err)
				continue
			}
			collectedEdges = append(collectedEdges, edge)
		}

		if len(collectedErrors) > 0 {
			t.Fatalf("unexpected errors: %v", collectedErrors)
		}

		if len(collectedEdges) != 1 {
			t.Fatalf("expected 1 follow edge, got %d", len(collectedEdges))
		}

		if collectedEdges[0].To.ID != "bob" {
			t.Fatalf("expected edge to bob, got %s", collectedEdges[0].To.ID)
		}
	})

	t.Run("returns only edges of specified type when filtering incoming edges", func(t *testing.T) {
		var collectedEdges []graph.Edge
		var collectedErrors []error

		aliceNode := graph.NewNodeRef("User", "alice")
		for edge, err := range g.GetIncoming(ctx, aliceNode, "follows") {
			if err != nil {
				collectedErrors = append(collectedErrors, err)
				continue
			}
			collectedEdges = append(collectedEdges, edge)
		}

		if len(collectedErrors) > 0 {
			t.Fatalf("unexpected errors: %v", collectedErrors)
		}

		if len(collectedEdges) != 1 {
			t.Fatalf("expected 1 incoming follow edge, got %d", len(collectedEdges))
		}

		if collectedEdges[0].From.ID != "bob" {
			t.Fatalf("expected edge from bob, got %s", collectedEdges[0].From.ID)
		}
	})

	t.Run("returns all outgoing edges regardless of type", func(t *testing.T) {
		var collectedEdges []graph.Edge
		var collectedErrors []error

		aliceNode := graph.NewNodeRef("User", "alice")
		for edge, err := range g.GetAllOutgoing(ctx, aliceNode) {
			if err != nil {
				collectedErrors = append(collectedErrors, err)
				continue
			}
			collectedEdges = append(collectedEdges, edge)
		}

		if len(collectedErrors) > 0 {
			t.Fatalf("unexpected errors: %v", collectedErrors)
		}

		if len(collectedEdges) != 2 {
			t.Fatalf("expected 2 outgoing edges from alice, got %d", len(collectedEdges))
		}

		// Check that we have one follow and one authored edge.
		followCount := 0
		authoredCount := 0
		for _, edge := range collectedEdges {
			switch edge.Type {
			case "follows":
				followCount++
			case "authored":
				authoredCount++
			}
		}

		if followCount != 1 || authoredCount != 1 {
			t.Fatalf("expected 1 follow and 1 authored edge, got %d follows, %d authored", followCount, authoredCount)
		}
	})

	t.Run("returns all incoming edges regardless of type", func(t *testing.T) {
		var collectedEdges []graph.Edge
		var collectedErrors []error

		aliceNode := graph.NewNodeRef("User", "alice")
		for edge, err := range g.GetAllIncoming(ctx, aliceNode) {
			if err != nil {
				collectedErrors = append(collectedErrors, err)
				continue
			}
			collectedEdges = append(collectedEdges, edge)
		}

		if len(collectedErrors) > 0 {
			t.Fatalf("unexpected errors: %v", collectedErrors)
		}

		if len(collectedEdges) != 1 {
			t.Fatalf("expected 1 incoming edge to alice, got %d", len(collectedEdges))
		}

		if collectedEdges[0].Type != "follows" {
			t.Fatalf("expected follow edge, got %s", collectedEdges[0].Type)
		}
	})

	t.Run("streams all edges when using wildcard edge type", func(t *testing.T) {
		var collectedEdges []graph.Edge
		var collectedErrors []error

		for edge, err := range g.All(ctx) {
			if err != nil {
				collectedErrors = append(collectedErrors, err)
				continue
			}
			collectedEdges = append(collectedEdges, edge)
		}

		if len(collectedErrors) > 0 {
			t.Fatalf("unexpected errors: %v", collectedErrors)
		}

		if len(collectedEdges) != 4 {
			t.Fatalf("expected 4 total edges, got %d", len(collectedEdges))
		}
	})

	t.Run("Streaming with context cancellation", func(t *testing.T) {
		cancelCtx, cancel := context.WithCancel(ctx)
		cancel() // Cancel immediately.

		var collectedErrors []error
		aliceNode := graph.NewNodeRef("User", "alice")
		for _, err := range g.GetOutgoing(cancelCtx, aliceNode, "follows") {
			if err != nil {
				collectedErrors = append(collectedErrors, err)
				break // Exit on first error.
			}
		}

		if len(collectedErrors) == 0 {
			t.Fatal("expected context cancellation error")
		}

		// The error could be context.Canceled directly or wrapped by the underlying store.
		errStr := collectedErrors[0].Error()
		if collectedErrors[0] != context.Canceled && !strings.Contains(errStr, "context canceled") && !strings.Contains(errStr, "interrupted") {
			t.Fatalf("expected context cancellation error, got %v", collectedErrors[0])
		}
	})
}

func TestFunctionBasedFiltering(t *testing.T) {
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

	// Create test edges with different scores.
	edges := []graph.Edge{
		graph.NewEdge(
			graph.NewNodeRef("User", "alice"),
			graph.NewNodeRef("Post", "post1"),
			"likes",
			json.RawMessage(`{"score": 10}`),
		),
		graph.NewEdge(
			graph.NewNodeRef("User", "alice"),
			graph.NewNodeRef("Post", "post2"),
			"likes",
			json.RawMessage(`{"score": 3}`),
		),
		graph.NewEdge(
			graph.NewNodeRef("User", "alice"),
			graph.NewNodeRef("Post", "post3"),
			"likes",
			json.RawMessage(`{"score": 8}`),
		),
		graph.NewEdge(
			graph.NewNodeRef("User", "alice"),
			graph.NewNodeRef("Post", "post4"),
			"likes",
			nil, // No data
		),
	}

	for _, edge := range edges {
		if err := g.AddEdge(ctx, edge); err != nil {
			t.Fatalf("failed to add edge: %v", err)
		}
	}

	t.Run("filters edges by custom function", func(t *testing.T) {
		// Filter for edges with score > 5.
		scoreFilter := func(edge graph.Edge) bool {
			if len(edge.Data) == 0 {
				return false
			}
			var data map[string]any
			if err := json.Unmarshal(edge.Data, &data); err != nil {
				return false
			}
			score, ok := data["score"]
			if !ok {
				return false
			}
			if scoreVal, ok := score.(float64); ok {
				return scoreVal > 5
			}
			return false
		}

		paths, err := g.BreadthFirstSearch(ctx, "User", "alice", graph.TraversalOptions{
			EdgeTypes: []string{"likes"},
			Filter:    scoreFilter,
			MaxDepth:  1,
		})
		if err != nil {
			t.Fatalf("failed to perform BFS with filter: %v", err)
		}

		// Should find 2 posts (post1 with score 10, post3 with score 8).
		if len(paths) != 3 { // 1 for alice + 2 for filtered posts
			t.Fatalf("expected 3 paths (alice + 2 posts), got %d", len(paths))
		}

		// Check that only high-score posts are in the paths.
		highScorePosts := make(map[string]bool)
		for _, path := range paths {
			if path.Depth == 1 { // Posts are at depth 1
				postID := path.Nodes[1].ID
				highScorePosts[postID] = true
			}
		}

		if len(highScorePosts) != 2 {
			t.Fatalf("expected 2 high-score posts, got %d", len(highScorePosts))
		}

		if !highScorePosts["post1"] || !highScorePosts["post3"] {
			t.Fatalf("expected post1 and post3 to be in results, got %v", highScorePosts)
		}
	})

	t.Run("combines score and target filtering", func(t *testing.T) {
		// Use function filtering that combines both score and target filtering.
		combinedFilter := func(edge graph.Edge) bool {
			// Check score = 10.
			if len(edge.Data) == 0 {
				return false
			}
			var data map[string]any
			if err := json.Unmarshal(edge.Data, &data); err != nil {
				return false
			}
			score, ok := data["score"].(float64)
			if !ok || score != 10 {
				return false
			}
			// Only allow edges to post1 or post3.
			return edge.To.ID == "post1" || edge.To.ID == "post3"
		}

		paths, err := g.BreadthFirstSearch(ctx, "User", "alice", graph.TraversalOptions{
			EdgeTypes: []string{"likes"},
			Filter:    combinedFilter,
			MaxDepth:  1,
		})
		if err != nil {
			t.Fatalf("failed to perform BFS with combined filters: %v", err)
		}

		// Should find only post1 (score=10 AND allowed by custom filter).
		foundPosts := make(map[string]bool)
		for _, path := range paths {
			if path.Depth == 1 {
				foundPosts[path.Nodes[1].ID] = true
			}
		}

		if len(foundPosts) != 1 || !foundPosts["post1"] {
			t.Fatalf("expected only post1, got %v", foundPosts)
		}
	})
}
