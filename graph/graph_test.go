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
		followEdge := graph.Edge{
			FromEntityType: "User",
			FromEntityID:   "user1",
			ToEntityType:   "User",
			ToEntityID:     "user2",
			Type:           "follows",
			Data:           json.RawMessage(`{"since": "2024-01-01"}`),
		}

		if err := g.AddEdge(ctx, followEdge); err != nil {
			t.Fatalf("failed to add follow edge: %v", err)
		}

		// User1 authored Post1.
		authorEdge := graph.Edge{
			FromEntityType: "User",
			FromEntityID:   "user1",
			ToEntityType:   "Post",
			ToEntityID:     "post1",
			Type:           "authored",
		}

		if err := g.AddEdge(ctx, authorEdge); err != nil {
			t.Fatalf("failed to add author edge: %v", err)
		}

		// User2 commented on Post1.
		commentedEdge := graph.Edge{
			FromEntityType: "User",
			FromEntityID:   "user2",
			ToEntityType:   "Comment",
			ToEntityID:     "comment1",
			Type:           "authored",
		}

		if err := g.AddEdge(ctx, commentedEdge); err != nil {
			t.Fatalf("failed to add commented edge: %v", err)
		}

		// Comment1 is on Post1.
		commentOnPostEdge := graph.Edge{
			FromEntityType: "Comment",
			FromEntityID:   "comment1",
			ToEntityType:   "Post",
			ToEntityID:     "post1",
			Type:           "on",
		}

		if err := g.AddEdge(ctx, commentOnPostEdge); err != nil {
			t.Fatalf("failed to add comment-on-post edge: %v", err)
		}

		// Retrieve specific edge.
		retrievedEdge, ok, err := g.GetEdge(ctx, "User", "user1", "follows", "User", "user2")
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
		// Get all users that user1 follows.
		var followEdges []graph.Edge
		for edge, err := range g.GetOutgoing(ctx, "User", "user1", "follows") {
			if err != nil {
				t.Fatalf("failed to get outgoing follow edges: %v", err)
			}
			followEdges = append(followEdges, edge)
		}
		if len(followEdges) != 1 {
			t.Fatalf("expected 1 follow edge, got %d", len(followEdges))
		}
		if followEdges[0].ToEntityID != "user2" {
			t.Fatalf("expected to follow user2, got %s", followEdges[0].ToEntityID)
		}

		// Get all posts authored by user1.
		var authoredEdges []graph.Edge
		for edge, err := range g.GetOutgoing(ctx, "User", "user1", "authored") {
			if err != nil {
				t.Fatalf("failed to get outgoing authored edges: %v", err)
			}
			authoredEdges = append(authoredEdges, edge)
		}
		if len(authoredEdges) != 1 {
			t.Fatalf("expected 1 authored edge, got %d", len(authoredEdges))
		}
		if authoredEdges[0].ToEntityID != "post1" {
			t.Fatalf("expected to author post1, got %s", authoredEdges[0].ToEntityID)
		}
	})

	t.Run("Get incoming edges", func(t *testing.T) {
		// Get all users who follow user2.
		var followersEdges []graph.Edge
		for edge, err := range g.GetIncoming(ctx, "User", "user2", "follows") {
			if err != nil {
				t.Fatalf("failed to get incoming follow edges: %v", err)
			}
			followersEdges = append(followersEdges, edge)
		}
		if len(followersEdges) != 1 {
			t.Fatalf("expected 1 follower edge, got %d", len(followersEdges))
		}
		if followersEdges[0].FromEntityID != "user1" {
			t.Fatalf("expected follower to be user1, got %s", followersEdges[0].FromEntityID)
		}

		// Get who authored post1.
		var authorEdges []graph.Edge
		for edge, err := range g.GetIncoming(ctx, "Post", "post1", "authored") {
			if err != nil {
				t.Fatalf("failed to get incoming authored edges: %v", err)
			}
			authorEdges = append(authorEdges, edge)
		}
		if len(authorEdges) != 1 {
			t.Fatalf("expected 1 author edge, got %d", len(authorEdges))
		}
		if authorEdges[0].FromEntityID != "user1" {
			t.Fatalf("expected author to be user1, got %s", authorEdges[0].FromEntityID)
		}
	})

	t.Run("Get all edges for entity", func(t *testing.T) {
		// Get all outgoing edges from user1.
		var allOutgoing []graph.Edge
		for edge, err := range g.GetAllOutgoing(ctx, "User", "user1") {
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
		for edge, err := range g.GetAllIncoming(ctx, "Post", "post1") {
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
		if err := g.RemoveEdge(ctx, "User", "user1", "follows", "User", "user2"); err != nil {
			t.Fatalf("failed to remove follow edge: %v", err)
		}

		// Verify it's gone.
		_, ok, err := g.GetEdge(ctx, "User", "user1", "follows", "User", "user2")
		if err != nil {
			t.Fatalf("failed to check edge existence: %v", err)
		}
		if ok {
			t.Fatalf("expected edge to be removed")
		}

		// Verify outgoing edges list is updated.
		var followEdges []graph.Edge
		for edge, err := range g.GetOutgoing(ctx, "User", "user1", "follows") {
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
		for edge, err := range g.GetIncoming(ctx, "User", "user2", "follows") {
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
		edge := graph.Edge{
			FromEntityType: "User",
			FromEntityID:   rel.from,
			ToEntityType:   "User",
			ToEntityID:     rel.to,
			Type:           "follows",
		}
		if err := g.AddEdge(ctx, edge); err != nil {
			t.Fatalf("failed to add edge %s -> %s: %v", rel.from, rel.to, err)
		}
	}

	t.Run("Find who Alice follows", func(t *testing.T) {
		var following []graph.Edge
		for edge, err := range g.GetOutgoing(ctx, "User", "alice", "follows") {
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
			followingIDs[edge.ToEntityID] = true
		}

		if !followingIDs["bob"] || !followingIDs["charlie"] {
			t.Fatalf("expected Alice to follow Bob and Charlie, got %v", followingIDs)
		}
	})

	t.Run("Find Alice's followers", func(t *testing.T) {
		var followers []graph.Edge
		for edge, err := range g.GetIncoming(ctx, "User", "alice", "follows") {
			if err != nil {
				t.Fatalf("failed to get Alice's followers: %v", err)
			}
			followers = append(followers, edge)
		}

		if len(followers) != 1 {
			t.Fatalf("expected Alice to have 1 follower, got %d", len(followers))
		}

		if followers[0].FromEntityID != "diana" {
			t.Fatalf("expected Diana to follow Alice, got %s", followers[0].FromEntityID)
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
		{
			FromEntityType: "User",
			FromEntityID:   "alice",
			ToEntityType:   "User",
			ToEntityID:     "bob",
			Type:           "follows",
			Data:           json.RawMessage(`{"since": "2024-01-01"}`),
		},
		{
			FromEntityType: "User",
			FromEntityID:   "alice",
			ToEntityType:   "Post",
			ToEntityID:     "post1",
			Type:           "authored",
		},
		{
			FromEntityType: "User",
			FromEntityID:   "bob",
			ToEntityType:   "User",
			ToEntityID:     "alice",
			Type:           "follows",
		},
		{
			FromEntityType: "User",
			FromEntityID:   "bob",
			ToEntityType:   "Post",
			ToEntityID:     "post2",
			Type:           "authored",
		},
	}

	for _, edge := range edges {
		if err := g.AddEdge(ctx, edge); err != nil {
			t.Fatalf("failed to add edge: %v", err)
		}
	}

	t.Run("GetOutgoing specific type", func(t *testing.T) {
		var collectedEdges []graph.Edge
		var collectedErrors []error

		for edge, err := range g.GetOutgoing(ctx, "User", "alice", "follows") {
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

		if collectedEdges[0].ToEntityID != "bob" {
			t.Fatalf("expected edge to bob, got %s", collectedEdges[0].ToEntityID)
		}
	})

	t.Run("GetIncoming specific type", func(t *testing.T) {
		var collectedEdges []graph.Edge
		var collectedErrors []error

		for edge, err := range g.GetIncoming(ctx, "User", "alice", "follows") {
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

		if collectedEdges[0].FromEntityID != "bob" {
			t.Fatalf("expected edge from bob, got %s", collectedEdges[0].FromEntityID)
		}
	})

	t.Run("GetAllOutgoing", func(t *testing.T) {
		var collectedEdges []graph.Edge
		var collectedErrors []error

		for edge, err := range g.GetAllOutgoing(ctx, "User", "alice") {
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

	t.Run("GetAllIncoming", func(t *testing.T) {
		var collectedEdges []graph.Edge
		var collectedErrors []error

		for edge, err := range g.GetAllIncoming(ctx, "User", "alice") {
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

	t.Run("All", func(t *testing.T) {
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
		for _, err := range g.GetOutgoing(cancelCtx, "User", "alice", "follows") {
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
