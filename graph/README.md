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
- **Edge Properties**: Store metadata on relationships using JSON data (e.g., timestamps, weights, scores)
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
// Default batch size (1000).
g := graph.New(store)

// Custom batch size for your workload.
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

// Initialize your KV store.
store := sqlitekv.NewStore(pool)
store.Init(ctx)

// Create graph instance.
g := graph.New(store)
```

### Adding Entities and Relationships

```go
// Store entities in KV store first.
user1 := User{ID: "alice", Name: "Alice"}
user2 := User{ID: "bob", Name: "Bob"}
post := Post{ID: "post1", Title: "Hello World"}

store.Put(ctx, "user/alice", -1, user1)
store.Put(ctx, "user/bob", -1, user2)
store.Put(ctx, "post/post1", -1, post)

// Create node references.
aliceNode := graph.NewNodeRef("alice", "User")
bobNode := graph.NewNodeRef("bob", "User")
postNode := graph.NewNodeRef("post1", "Post")

// Create relationships.
followEdge := graph.NewEdge(
    aliceNode,
    bobNode,
    "follows",
    json.RawMessage(`{"since": "2024-01-01"}`),
)

authorEdge := graph.NewEdge(
    aliceNode,
    postNode,
    "authored",
    json.RawMessage(`{"timestamp": "2024-01-01T10:00:00Z"}`),
)

g.AddEdge(ctx, followEdge)
g.AddEdge(ctx, authorEdge)
```

### Querying Relationships

```go
// Create node references for queries.
aliceNode := graph.NewNodeRef("alice", "User")
bobNode := graph.NewNodeRef("bob", "User")

// Get specific edge.
edge, exists, err := g.GetEdge(ctx, aliceNode, "follows", bobNode)

// Get all users alice follows.
for edge, err := range g.GetOutgoing(ctx, aliceNode, "follows") {
    if err != nil {
        // handle error
        break
    }
    fmt.Printf("Alice follows: %s\n", edge.To.ID)
}

// Get all followers of bob.
for edge, err := range g.GetIncoming(ctx, bobNode, "follows") {
    if err != nil {
        // handle error
        break
    }
    fmt.Printf("Bob is followed by: %s\n", edge.From.ID)
}

// Get all relationships from alice (any type).
for edge, err := range g.GetAllOutgoing(ctx, aliceNode) {
    if err != nil {
        // handle error
        break
    }
    fmt.Printf("Alice -> %s via %s\n", edge.To.ID, edge.Type)
}

// Get all relationships to bob (any type).
for edge, err := range g.GetAllIncoming(ctx, bobNode) {
    if err != nil {
        // handle error
        break
    }
    fmt.Printf("%s -> Bob via %s\n", edge.From.ID, edge.Type)
}
```

## Graph Traversal

### Breadth-First Search

```go
// Traverse the graph starting from alice, max 3 hops.
paths, err := g.BreadthFirstSearch(ctx, "User", "alice", graph.TraversalOptions{
    MaxDepth:   3,
    EdgeTypes:  []string{"follows"}, // Only follow "follows" edges.
    VisitLimit: 100,                 // Maximum of 100 nodes.
})

// Each path contains the nodes and edges traversed.
for _, path := range paths {
    fmt.Printf("Depth %d: %v\n", path.Depth, path.Nodes)
}
```

### Shortest Path

```go
// Find shortest path from alice to charlie.
path, err := g.FindShortestPath(ctx, "User", "alice", "User", "charlie", 
    graph.TraversalOptions{
        EdgeTypes: []string{"follows"},
    })

if path != nil {
    fmt.Printf("Shortest path: %d hops\n", path.Depth)
    for i, node := range path.Nodes {
        fmt.Printf("  %d: %s/%s\n", i, node.Type, node.ID)
    }
}
```

### Finding Common Connections

```go
// Find mutual follows between alice and bob.
mutual, err := g.FindMutualConnections(ctx, "User", "alice", "User", "bob", "follows")

for _, connection := range mutual {
    fmt.Printf("Both alice and bob follow: %s\n", connection.ID)
}
```

### Graph Analytics

```go
// Get node degree (number of connections).
inDegree, outDegree, err := g.GetDegree(ctx, "User", "alice", "follows")
fmt.Printf("Alice follows %d users and is followed by %d users\n", outDegree, inDegree)

// Get all neighbors (both directions).
neighbors, err := g.GetNeighbors(ctx, "User", "alice", graph.TraversalOptions{
    EdgeTypes: []string{"follows", "likes"},
})
```

## Advanced Features

### Edge Filtering

Filter edges during traversal using custom functions:

```go
// Filter edges with score > 5.
scoreFilter := func(edge graph.Edge) bool {
    if len(edge.Data) == 0 {
        return false
    }
    var data map[string]any
    if err := json.Unmarshal(edge.Data, &data); err != nil {
        return false
    }
    if score, ok := data["score"].(float64); ok {
        return score > 5
    }
    return false
}

paths, err := g.BreadthFirstSearch(ctx, "User", "alice", graph.TraversalOptions{
    EdgeTypes: []string{"likes"},
    Filter:    scoreFilter,
})

// Combine multiple filtering criteria.
combinedFilter := func(edge graph.Edge) bool {
    if len(edge.Data) == 0 {
        return false
    }
    var data map[string]any
    if err := json.Unmarshal(edge.Data, &data); err != nil {
        return false
    }
    // Check category AND score.
    category, hasCategory := data["category"].(string)
    score, hasScore := data["score"].(float64)
    return hasCategory && category == "tech" && hasScore && score > 5
}

paths, err = g.BreadthFirstSearch(ctx, "User", "alice", graph.TraversalOptions{
    EdgeTypes: []string{"likes"},
    Filter:    combinedFilter,
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
// User follows another user.
aliceNode := graph.NewNodeRef("alice", "User")
bobNode := graph.NewNodeRef("bob", "User")

followEdge := graph.NewEdge(aliceNode, bobNode, "follows", nil)
g.AddEdge(ctx, followEdge)

// Find followers, following, mutual connections.
var followers []graph.Edge
for edge, err := range g.GetIncoming(ctx, aliceNode, "follows") {
    if err != nil {
        // handle error
        break
    }
    followers = append(followers, edge)
}

var following []graph.Edge
for edge, err := range g.GetOutgoing(ctx, aliceNode, "follows") {
    if err != nil {
        // handle error
        break
    }
    following = append(following, edge)
}

mutual, _ := g.FindMutualConnections(ctx, "User", "alice", "User", "bob", "follows")
```

### Content Management

```go
// User authored a post
aliceNode := graph.NewNodeRef("alice", "User")
postNode := graph.NewNodeRef("post1", "Post")

authorEdge := graph.NewEdge(
    aliceNode,
    postNode,
    "authored",
    json.RawMessage(`{"timestamp": "2024-01-01T10:00:00Z"}`),
)
g.AddEdge(ctx, authorEdge)

// User commented on a post
bobNode := graph.NewNodeRef("bob", "User")
commentEdge := graph.NewEdge(
    bobNode,
    postNode,
    "commented",
    json.RawMessage(`{"comment_id": "comment123"}`),
)
g.AddEdge(ctx, commentEdge)

// Find all posts by a user
var posts []graph.Edge
for edge, err := range g.GetOutgoing(ctx, aliceNode, "authored") {
    if err != nil {
        // handle error
        break
    }
    posts = append(posts, edge)
}

// Find all comments on a post
var comments []graph.Edge
for edge, err := range g.GetIncoming(ctx, postNode, "commented") {
    if err != nil {
        // handle error
        break
    }
    comments = append(comments, edge)
}
```

### Product Recommendations

```go
// User viewed/bought products
aliceNode := graph.NewNodeRef("alice", "User")
laptopNode := graph.NewNodeRef("laptop", "Product")

buyEdge := graph.NewEdge(
    aliceNode,
    laptopNode,
    "bought",
    json.RawMessage(`{"price": 1200.0, "date": "2024-01-01"}`),
)
g.AddEdge(ctx, buyEdge)

// Products belong to categories
electronicsNode := graph.NewNodeRef("electronics", "Category")
categoryEdge := graph.NewEdge(
    laptopNode,
    electronicsNode,
    "in_category",
    nil,
)
g.AddEdge(ctx, categoryEdge)

// Find similar users (who bought same products)
var alicePurchases []graph.Edge
for edge, err := range g.GetOutgoing(ctx, aliceNode, "bought") {
    if err != nil {
        // handle error
        break
    }
    alicePurchases = append(alicePurchases, edge)
}

for _, purchase := range alicePurchases {
    productNode := graph.NewNodeRef(purchase.To.ID, "Product")
    for edge, err := range g.GetIncoming(ctx, productNode, "bought") {
        if err != nil {
            // handle error
            continue
        }
        // edge.From contains other buyers of the same product
        fmt.Printf("Similar user: %s\n", edge.From.ID)
    }
}
```

### Entity Component System (ECS) with Relationships

Combine with the existing ECS pattern to add relationships between entities:

```go
// Traditional ECS: entity has components
store.Put(ctx, "entity/player1/position", -1, Position{X: 10, Y: 20})
store.Put(ctx, "entity/player1/health", -1, Health{Current: 100, Max: 100})

// Graph: entities have relationships
player1Node := graph.NewNodeRef("player1", "Entity")
weapon1Node := graph.NewNodeRef("weapon1", "Entity")
team1Node := graph.NewNodeRef("team1", "Entity")

equipEdge := graph.NewEdge(player1Node, weapon1Node, "equipped", nil)
g.AddEdge(ctx, equipEdge)

memberEdge := graph.NewEdge(player1Node, team1Node, "member_of", nil)
g.AddEdge(ctx, memberEdge)

// Query: find all team members
var teamMembers []graph.Edge
for edge, err := range g.GetIncoming(ctx, team1Node, "member_of") {
    if err != nil {
        // handle error
        break
    }
    teamMembers = append(teamMembers, edge)
}

// Query: find what player has equipped
var equipment []graph.Edge
for edge, err := range g.GetOutgoing(ctx, player1Node, "equipped") {
    if err != nil {
        // handle error
        break
    }
    equipment = append(equipment, edge)
}
```

## Performance Considerations

1. **Indexing**: The package maintains separate indexes for incoming and outgoing edges for O(1) lookups
2. **Batch Operations**: Use `MutateAll` for atomic multi-edge operations
3. **Data Filtering**: Edge data is stored as JSON, so complex data queries may be slower
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
aNode := graph.NewNodeRef("a", "User")
bNode := graph.NewNodeRef("b", "User")
followEdge := graph.NewEdge(aNode, bNode, "follows", nil)
g.AddEdge(ctx, followEdge)

// From AWS Neptune/Property Graph
personNode := graph.NewNodeRef("person1", "Person")
companyNode := graph.NewNodeRef("company1", "Company")
workEdge := graph.NewEdge(
    personNode,
    companyNode,
    "works_at",
    json.RawMessage(`{
        "start_date": "2020-01-01",
        "position": "Engineer"
    }`),
)
g.AddEdge(ctx, workEdge)
```
