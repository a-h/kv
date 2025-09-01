package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/a-h/kv/graph"
	"github.com/a-h/kv/sqlitekv"
	"zombiezen.com/go/sqlite/sqlitex"
)

// StreamingDemo demonstrates how to use the streaming graph APIs for real-time UI updates.
func StreamingDemo() {
	ctx := context.Background()

	// Initialize store.
	pool, err := sqlitex.NewPool("file::memory:?mode=memory&cache=shared", sqlitex.PoolOptions{})
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	store := sqlitekv.NewStore(pool)
	if err := store.Init(ctx); err != nil {
		log.Fatalf("failed to init store: %v", err)
	}

	g := graph.New(store)

	// Create some sample data for demonstration.
	setupSampleData(ctx, g)

	fmt.Println("=== Streaming Graph Demo ===")
	fmt.Println()

	// Demo 1: Stream outgoing edges for real-time UI updates.
	fmt.Println("1. Streaming outgoing 'follows' edges from alice:")
	simulateUIUpdates("StreamOutgoingEdges", func() {
		for edge, err := range g.StreamOutgoingEdges(ctx, "User", "alice", "follows") {
			if err != nil {
				fmt.Printf("Error: %v\n", err)
				continue
			}
			// Simulate UI update with each edge.
			fmt.Printf("   -> Alice follows %s (since: %v)\n",
				edge.ToEntityID, edge.Properties["since"])
		}
	})

	// Demo 2: Stream all outgoing edges with type information.
	fmt.Println("\n2. Streaming all outgoing edges from alice:")
	simulateUIUpdates("StreamAllOutgoingEdges", func() {
		for edge, err := range g.StreamAllOutgoingEdges(ctx, "User", "alice") {
			if err != nil {
				fmt.Printf("Error: %v\n", err)
				continue
			}
			// Simulate UI update showing different edge types.
			fmt.Printf("   -> Alice %s %s:%s\n",
				edge.Type, edge.ToEntityType, edge.ToEntityID)
		}
	})

	// Demo 3: Stream incoming edges to show followers.
	fmt.Println("\n3. Streaming incoming 'follows' edges to bob:")
	simulateUIUpdates("StreamIncomingEdges", func() {
		for edge, err := range g.StreamIncomingEdges(ctx, "User", "bob", "follows") {
			if err != nil {
				fmt.Printf("Error: %v\n", err)
				continue
			}
			// Simulate UI update for followers.
			fmt.Printf("   -> %s follows Bob\n", edge.FromEntityID)
		}
	})

	// Demo 4: Stream all edges for analytics dashboard.
	fmt.Println("\n4. Streaming all edges for analytics dashboard:")
	simulateUIUpdates("StreamAllEdges", func() {
		edgeTypeCounts := make(map[string]int)
		for edge, err := range g.StreamAllEdges(ctx) {
			if err != nil {
				fmt.Printf("Error: %v\n", err)
				continue
			}
			edgeTypeCounts[edge.Type]++
			// Simulate dashboard update.
			fmt.Printf("   -> Processing %s edge: %s:%s -> %s:%s\n",
				edge.Type, edge.FromEntityType, edge.FromEntityID,
				edge.ToEntityType, edge.ToEntityID)
		}
		fmt.Printf("   Summary - Edge type counts: %v\n", edgeTypeCounts)
	})

	// Demo 5: Context cancellation for responsive UI.
	fmt.Println("\n5. Demonstrating context cancellation:")
	cancelCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()

	fmt.Println("   Starting stream with 100ms timeout...")
	edgeCount := 0
	for edge, err := range g.StreamAllEdges(cancelCtx) {
		if err != nil {
			fmt.Printf("   Stream cancelled due to: %v\n", err)
			break
		}
		edgeCount++
		// Simulate slow processing.
		time.Sleep(50 * time.Millisecond)
		fmt.Printf("   Processed edge %d: %s\n", edgeCount, edge.Type)
	}
	fmt.Printf("   Processed %d edges before cancellation\n", edgeCount)
}

func setupSampleData(ctx context.Context, g *graph.Graph) {
	// Create sample social network edges.
	edges := []graph.Edge{
		{
			FromEntityType: "User",
			FromEntityID:   "alice",
			ToEntityType:   "User",
			ToEntityID:     "bob",
			Type:           "follows",
			Properties:     map[string]any{"since": "2024-01-01"},
		},
		{
			FromEntityType: "User",
			FromEntityID:   "alice",
			ToEntityType:   "User",
			ToEntityID:     "charlie",
			Type:           "follows",
			Properties:     map[string]any{"since": "2024-02-15"},
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
			Properties:     map[string]any{"since": "2024-01-10"},
		},
		{
			FromEntityType: "User",
			FromEntityID:   "charlie",
			ToEntityType:   "Post",
			ToEntityID:     "post1",
			Type:           "liked",
			Properties:     map[string]any{"timestamp": "2024-03-01T10:30:00Z"},
		},
		{
			FromEntityType: "User",
			FromEntityID:   "bob",
			ToEntityType:   "Comment",
			ToEntityID:     "comment1",
			Type:           "authored",
		},
	}

	for _, edge := range edges {
		if err := g.AddEdge(ctx, edge); err != nil {
			log.Fatalf("failed to add edge: %v", err)
		}
	}
}

func simulateUIUpdates(operation string, streamFunc func()) {
	fmt.Printf("   [%s] Starting stream...\n", operation)
	start := time.Now()

	streamFunc()

	duration := time.Since(start)
	fmt.Printf("   [%s] Stream completed in %v\n", operation, duration)
}

func init() {
	// Add this demo to the main function if run directly.
	if len(fmt.Sprintf("")) == 0 { // Always false, just to make this compilable
		StreamingDemo()
	}
}
