package graph_test

import (
	"context"
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
			Properties:     map[string]any{"since": "2024-01-01"},
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
		if retrievedEdge.Properties["since"] != "2024-01-01" {
			t.Fatalf("expected property 'since' to be '2024-01-01', got %v", retrievedEdge.Properties["since"])
		}
	})

	t.Run("Get outgoing edges", func(t *testing.T) {
		// Get all users that user1 follows.
		followEdges, err := g.GetOutgoingEdges(ctx, "User", "user1", "follows")
		if err != nil {
			t.Fatalf("failed to get outgoing follow edges: %v", err)
		}
		if len(followEdges) != 1 {
			t.Fatalf("expected 1 follow edge, got %d", len(followEdges))
		}
		if followEdges[0].ToEntityID != "user2" {
			t.Fatalf("expected to follow user2, got %s", followEdges[0].ToEntityID)
		}

		// Get all posts authored by user1.
		authoredEdges, err := g.GetOutgoingEdges(ctx, "User", "user1", "authored")
		if err != nil {
			t.Fatalf("failed to get outgoing authored edges: %v", err)
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
		followersEdges, err := g.GetIncomingEdges(ctx, "User", "user2", "follows")
		if err != nil {
			t.Fatalf("failed to get incoming follow edges: %v", err)
		}
		if len(followersEdges) != 1 {
			t.Fatalf("expected 1 follower edge, got %d", len(followersEdges))
		}
		if followersEdges[0].FromEntityID != "user1" {
			t.Fatalf("expected follower to be user1, got %s", followersEdges[0].FromEntityID)
		}

		// Get who authored post1.
		authorEdges, err := g.GetIncomingEdges(ctx, "Post", "post1", "authored")
		if err != nil {
			t.Fatalf("failed to get incoming authored edges: %v", err)
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
		allOutgoing, err := g.GetAllOutgoingEdges(ctx, "User", "user1")
		if err != nil {
			t.Fatalf("failed to get all outgoing edges: %v", err)
		}
		if len(allOutgoing) != 2 { // follows and authored
			t.Fatalf("expected 2 outgoing edges, got %d", len(allOutgoing))
		}

		// Get all incoming edges to post1.
		allIncoming, err := g.GetAllIncomingEdges(ctx, "Post", "post1")
		if err != nil {
			t.Fatalf("failed to get all incoming edges: %v", err)
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
		followEdges, err := g.GetOutgoingEdges(ctx, "User", "user1", "follows")
		if err != nil {
			t.Fatalf("failed to get outgoing follow edges: %v", err)
		}
		if len(followEdges) != 0 {
			t.Fatalf("expected 0 follow edges after removal, got %d", len(followEdges))
		}

		// Verify incoming edges list is updated.
		followersEdges, err := g.GetIncomingEdges(ctx, "User", "user2", "follows")
		if err != nil {
			t.Fatalf("failed to get incoming follow edges: %v", err)
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
		following, err := g.GetOutgoingEdges(ctx, "User", "alice", "follows")
		if err != nil {
			t.Fatalf("failed to get Alice's following: %v", err)
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
		followers, err := g.GetIncomingEdges(ctx, "User", "alice", "follows")
		if err != nil {
			t.Fatalf("failed to get Alice's followers: %v", err)
		}

		if len(followers) != 1 {
			t.Fatalf("expected Alice to have 1 follower, got %d", len(followers))
		}

		if followers[0].FromEntityID != "diana" {
			t.Fatalf("expected Diana to follow Alice, got %s", followers[0].FromEntityID)
		}
	})
}
