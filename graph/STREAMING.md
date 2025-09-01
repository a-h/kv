# Streaming Graph API

This document describes the streaming graph API additions that provide improved performance characteristics for scenarios where graph data needs to be consumed incrementally or displayed in real-time UIs.

## Performance Issues with Original API

The original `Get*Edges` functions had several performance issues:

1. **N+1 Database Operations**: Functions like `GetOutgoingEdges()` would:
   - Call `GetPrefix()` to get all edge references
   - For each reference, call `GetEdge()` individually
   - This resulted in `1 + N` database operations for `N` edges

2. **Memory Usage**: All edges were loaded into memory before returning, which could be problematic for large graphs

3. **Blocking Operations**: The entire operation had to complete before any results could be displayed

4. **Poor Scalability**: `GetAllOutgoingEdges()` and `GetAllIncomingEdges()` multiplied the problem by calling the individual edge functions for each edge type

## Streaming API Solution

The new streaming API addresses these issues using Go 1.22+ iterators (`iter.Seq2[Edge, error]`):

### New Functions

- `StreamOutgoingEdges(ctx, entityType, entityID, edgeType)` - Stream outgoing edges of specific type
- `StreamIncomingEdges(ctx, entityType, entityID, edgeType)` - Stream incoming edges of specific type  
- `StreamAllOutgoingEdges(ctx, entityType, entityID)` - Stream all outgoing edges
- `StreamAllIncomingEdges(ctx, entityType, entityID)` - Stream all incoming edges
- `StreamAllEdges(ctx)` - Stream all edges in the graph

### Architecture Improvements

All existing `Get*Edges` and `ListAllEdges` functions have been refactored to use the streaming functions internally:

- `GetOutgoingEdges()` → uses `StreamOutgoingEdges()`
- `GetIncomingEdges()` → uses `StreamIncomingEdges()`
- `GetAllOutgoingEdges()` → uses `StreamAllOutgoingEdges()`
- `GetAllIncomingEdges()` → uses `StreamAllIncomingEdges()`
- `ListAllEdges()` → uses `StreamAllEdges()`

This eliminates code duplication and ensures consistency between streaming and non-streaming APIs.

### Benefits

1. **Memory Efficiency**: Edges are processed one at a time, reducing memory usage
2. **Responsive UI**: Results start appearing immediately as they're found
3. **Cancellation Support**: Proper context cancellation with `ctx.Err()` checks
4. **Same Database Operations**: The streaming versions don't reduce database calls, but allow processing to begin immediately
5. **Backward Compatibility**: Original functions remain unchanged

## Usage Examples

### Basic Streaming

```go
for edge, err := range g.StreamOutgoingEdges(ctx, "User", "alice", "follows") {
    if err != nil {
        log.Printf("Error: %v", err)
        continue
    }
    // Update UI immediately with this edge
    updateUI(edge)
}
```

### Context Cancellation

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

for edge, err := range g.StreamAllEdges(ctx) {
    if err != nil {
        // Handle cancellation or other errors
        break
    }
    processEdge(edge)
}
```

### Real-time Analytics Dashboard

```go
edgeTypeCounts := make(map[string]int)
for edge, err := range g.StreamAllEdges(ctx) {
    if err != nil {
        log.Printf("Error: %v", err)
        continue
    }
    edgeTypeCounts[edge.Type]++
    // Update dashboard in real-time
    updateDashboard(edgeTypeCounts)
}
```

## When to Use Each API

### Use Original API When:
- You need all results before proceeding
- Working with small datasets
- Building simple batch operations

### Use Streaming API When:
- Building responsive UIs that show results as they arrive
- Working with large graphs
- Need cancellation support for long-running operations
- Building real-time analytics or monitoring dashboards
- Memory usage is a concern

## Implementation Details

The streaming functions use Go 1.22+ iterator pattern with proper:
- Context cancellation checking (`ctx.Err()`)
- Error handling via `iter.Seq2[Edge, error]`
- Early termination when `yield()` returns false
- Efficient deduplication in `StreamAll*` functions

## Future Optimizations

While the current implementation maintains the same database operation count, future optimizations could include:

1. **Batch Processing**: Fetch multiple edge references and resolve them in batches
2. **Parallel Resolution**: Resolve edge references concurrently
3. **Connection Pooling**: Better utilize database connection pools
4. **Caching**: Add optional caching layer for frequently accessed edges

The streaming API provides the foundation for these optimizations without breaking the user interface.
