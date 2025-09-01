# Graph Package

The `graph` package provides graph database functionality on top of the KV store, allowing you to create and query relationships between entities.

## Overview

This package implements a directed graph where:

- **Nodes** are entities stored in your KV store (identified by entity type and ID)
- **Edges** represent relationships between entities with optional properties
- **Edge Types** categorize different kinds of relationships (e.g., "follows", "likes", "authored")

## Key Features

- **Directed Edges**: Relationships have a clear direction (from → to)
- **Typed Relationships**: Each edge has a type (e.g., "follows", "likes", "owns")
- **Edge Properties**: Store metadata on relationships (e.g., timestamps, weights, scores)
- **Efficient Lookups**: Fast queries for incoming/outgoing edges
- **Graph Traversal**: BFS, shortest path, neighbor finding
- **Atomic Operations**: All edge operations are transactional
- **Pagination Support**: Memory-efficient streaming for large graphs
- **Batch Retrieval**: Optimized performance with reduced database round trips

## Performance Optimizations

The graph package is optimized for large-scale graphs with the following features:

### Pagination

Instead of loading all edges at once, the graph uses the KV store's paginator to stream results in configurable batches. This prevents memory issues when dealing with nodes that have thousands of edges.

### Batch Retrieval

Rather than making individual database calls for each edge (N+1 query pattern), the graph collects edge keys and uses the `GetBatch()` API to retrieve multiple edges simultaneously, significantly reducing database round trips.

### Configurable Batch Size

```go
// Default batch size (1000)
g := graph.New(store)

// Custom batch size for your workload
g := graph.NewWithBatchSize(store, 500)
```

Choose batch size based on:

- Available memory
- Average node degree (number of edges per node)
- Database performance characteristics

## Basic Usage

### Creating a Graph

```go
import (
    "github.com/a-h/kv/graph"
    "github.com/a-h/kv/sqlitekv"
)

// Initialize your KV store
store := sqlitekv.NewStore(pool)
store.Init(ctx)

// Create graph instance
g := graph.New(store)
```

### Adding Entities and Relationships

```go
// Store entities in KV store first
user1 := User{ID: "alice", Name: "Alice"}
user2 := User{ID: "bob", Name: "Bob"}
post := Post{ID: "post1", Title: "Hello World"}

store.Put(ctx, "user/alice", -1, user1)
store.Put(ctx, "user/bob", -1, user2)
store.Put(ctx, "post/post1", -1, post)

// Create relationships
followEdge := graph.Edge{
    FromEntityType: "User",
    FromEntityID:   "alice",
    ToEntityType:   "User",
    ToEntityID:     "bob",
    Type:           "follows",
    Properties:     map[string]any{"since": "2024-01-01"},
}

authorEdge := graph.Edge{
    FromEntityType: "User",
    FromEntityID:   "alice",
    ToEntityType:   "Post",
    ToEntityID:     "post1",
    Type:           "authored",
    Properties:     map[string]any{"timestamp": time.Now()},
}

g.AddEdge(ctx, followEdge)
g.AddEdge(ctx, authorEdge)
```

### Querying Relationships

```go
// Get specific edge
edge, exists, err := g.GetEdge(ctx, "User", "alice", "follows", "User", "bob")

// Get all users alice follows
following, err := g.GetOutgoingEdges(ctx, "User", "alice", "follows")

// Get all followers of bob
followers, err := g.GetIncomingEdges(ctx, "User", "bob", "follows")

// Get all relationships from alice (any type)
allOutgoing, err := g.GetAllOutgoingEdges(ctx, "User", "alice")

// Get all relationships to bob (any type)
allIncoming, err := g.GetAllIncomingEdges(ctx, "User", "bob")
```

## Graph Traversal

### Breadth-First Search

```go
// Traverse the graph starting from alice, max 3 hops
paths, err := g.BreadthFirstSearch(ctx, "User", "alice", graph.TraversalOptions{
    MaxDepth:   3,
    EdgeTypes:  []string{"follows"}, // Only follow edges
    VisitLimit: 100,                 // Max 100 nodes
})

// Each path contains the nodes and edges traversed
for _, path := range paths {
    fmt.Printf("Depth %d: %v\n", path.Depth, path.Nodes)
}
```

### Shortest Path

```go
// Find shortest path from alice to charlie
path, err := g.FindShortestPath(ctx, "User", "alice", "User", "charlie", 
    graph.TraversalOptions{
        EdgeTypes: []string{"follows"},
    })

if path != nil {
    fmt.Printf("Shortest path: %d hops\n", path.Depth)
    for i, node := range path.Nodes {
        fmt.Printf("  %d: %s/%s\n", i, node.EntityType, node.EntityID)
    }
}
```

### Finding Common Connections

```go
// Find mutual follows between alice and bob
mutual, err := g.FindMutualConnections(ctx, "User", "alice", "User", "bob", "follows")

for _, connection := range mutual {
    fmt.Printf("Both alice and bob follow: %s\n", connection.EntityID)
}
```

### Graph Analytics

```go
// Get node degree (number of connections)
inDegree, outDegree, err := g.GetDegree(ctx, "User", "alice", "follows")
fmt.Printf("Alice follows %d users and is followed by %d users\n", outDegree, inDegree)

// Get all neighbors (both directions)
neighbors, err := g.GetNeighbors(ctx, "User", "alice", graph.TraversalOptions{
    EdgeTypes: []string{"follows", "likes"},
})
```

## Advanced Features

### Property Filtering

Filter edges based on properties during traversal:

```go
// Only traverse edges with high scores
paths, err := g.BreadthFirstSearch(ctx, "User", "alice", graph.TraversalOptions{
    EdgeTypes:  []string{"likes"},
    Properties: map[string]any{"score": 5}, // Only edges with score = 5
})
```

### Multi-Type Traversal

Traverse multiple edge types in a single query:

```go
neighbors, err := g.GetNeighbors(ctx, "User", "alice", graph.TraversalOptions{
    EdgeTypes: []string{"follows", "likes", "mentions"},
})
```

## Key Patterns

The graph package uses consistent key patterns for efficient storage and retrieval:

```text
# Edge storage
graph/edge/{fromType}/{fromID}/{edgeType}/{toType}/{toID}

# Outgoing edge indexes (for fast lookups)
graph/node/{entityType}/{entityID}/outgoing/{edgeType}

# Incoming edge indexes (for reverse lookups)  
graph/node/{entityType}/{entityID}/incoming/{edgeType}
```

## Use Cases

### Social Networks

```go
// User follows another user
g.AddEdge(ctx, graph.Edge{
    FromEntityType: "User", FromEntityID: "alice",
    ToEntityType: "User", ToEntityID: "bob",
    Type: "follows",
})

// Find followers, following, mutual connections
followers, _ := g.GetIncomingEdges(ctx, "User", "alice", "follows")
following, _ := g.GetOutgoingEdges(ctx, "User", "alice", "follows")
mutual, _ := g.FindMutualConnections(ctx, "User", "alice", "User", "bob", "follows")
```

### Content Management

```go
// User authored a post
g.AddEdge(ctx, graph.Edge{
    FromEntityType: "User", FromEntityID: "alice",
    ToEntityType: "Post", ToEntityID: "post1",
    Type: "authored",
    Properties: map[string]any{"timestamp": time.Now()},
})

// User commented on a post
g.AddEdge(ctx, graph.Edge{
    FromEntityType: "User", FromEntityID: "bob", 
    ToEntityType: "Post", ToEntityID: "post1",
    Type: "commented",
    Properties: map[string]any{"comment_id": "comment123"},
})

// Find all posts by a user
posts, _ := g.GetOutgoingEdges(ctx, "User", "alice", "authored")

// Find all comments on a post
comments, _ := g.GetIncomingEdges(ctx, "Post", "post1", "commented")
```

### Product Recommendations

```go
// User viewed/bought products
g.AddEdge(ctx, graph.Edge{
    FromEntityType: "User", FromEntityID: "alice",
    ToEntityType: "Product", ToEntityID: "laptop",
    Type: "bought",
    Properties: map[string]any{"price": 1200.0, "date": "2024-01-01"},
})

// Products belong to categories
g.AddEdge(ctx, graph.Edge{
    FromEntityType: "Product", FromEntityID: "laptop",
    ToEntityType: "Category", ToEntityID: "electronics",
    Type: "in_category",
})

// Find similar users (who bought same products)
alicePurchases, _ := g.GetOutgoingEdges(ctx, "User", "alice", "bought")
for _, purchase := range alicePurchases {
    similarUsers, _ := g.GetIncomingEdges(ctx, "Product", purchase.ToEntityID, "bought")
    // similarUsers contains other buyers of the same product
}
```

### Entity Component System (ECS) with Relationships

Combine with the existing ECS pattern to add relationships between entities:

```go
// Traditional ECS: entity has components
store.Put(ctx, "entity/player1/position", -1, Position{X: 10, Y: 20})
store.Put(ctx, "entity/player1/health", -1, Health{Current: 100, Max: 100})

// Graph: entities have relationships
g.AddEdge(ctx, graph.Edge{
    FromEntityType: "Entity", FromEntityID: "player1",
    ToEntityType: "Entity", ToEntityID: "weapon1", 
    Type: "equipped",
})

g.AddEdge(ctx, graph.Edge{
    FromEntityType: "Entity", FromEntityID: "player1",
    ToEntityType: "Entity", ToEntityID: "team1",
    Type: "member_of",
})

// Query: find all team members
teamMembers, _ := g.GetIncomingEdges(ctx, "Entity", "team1", "member_of")

// Query: find what player has equipped
equipment, _ := g.GetOutgoingEdges(ctx, "Entity", "player1", "equipped")
```

## Performance Considerations

1. **Indexing**: The package maintains separate indexes for incoming and outgoing edges for O(1) lookups
2. **Batch Operations**: Use `MutateAll` for atomic multi-edge operations
3. **Property Filtering**: Properties are stored as JSON, so complex property queries may be slower
4. **Traversal Limits**: Always set reasonable limits on depth and visit count for large graphs
5. **Memory Usage**: BFS loads paths into memory, so limit `VisitLimit` for large graphs

## Integration with Existing Features

The graph package integrates seamlessly with other KV store features:

- **Streaming**: Edge operations appear in the change stream
- **Tasks**: Schedule graph maintenance tasks
- **Locks**: Use distributed locks for graph-wide operations
- **State Machines**: Model state transitions as graph edges
- **ECS**: Add relationships between entities and components

## Migration from Other Graph Databases

The key patterns make it easy to migrate from other graph databases:

```go
// From Neo4j: CREATE (a:User)-[:FOLLOWS]->(b:User)
g.AddEdge(ctx, graph.Edge{
    FromEntityType: "User", FromEntityID: "a",
    ToEntityType: "User", ToEntityID: "b",
    Type: "follows",
})

// From AWS Neptune/Property Graph
g.AddEdge(ctx, graph.Edge{
    FromEntityType: "Person", FromEntityID: "person1",
    ToEntityType: "Company", ToEntityID: "company1", 
    Type: "works_at",
    Properties: map[string]any{
        "start_date": "2020-01-01",
        "position": "Engineer",
    },
})
```
