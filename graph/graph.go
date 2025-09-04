package graph

import (
	"context"
	"encoding/json"
	"fmt"
	"iter"
	"strings"

	"github.com/a-h/kv"
)

// Edge represents a relationship between two entities.
type Edge struct {
	From NodeRef         `json:"from"`
	To   NodeRef         `json:"to"`
	Type string          `json:"type"`
	Data json.RawMessage `json:"data,omitempty"`
}

// Key returns a unique string key for this edge.
func (e Edge) Key() string {
	return fmt.Sprintf("graph/edge/%s/%s/%s/%s/%s", e.From.Type, e.From.ID, e.Type, e.To.Type, e.To.ID)
}

// NodeRef represents a reference to a node in the graph.
type NodeRef struct {
	Type string `json:"type"`
	ID   string `json:"id"`
}

// Key returns a unique string key for this node reference.
func (n NodeRef) Key() string {
	return fmt.Sprintf("%s:%s", n.Type, n.ID)
}

// NewNodeRef creates a new NodeRef with the given type and ID.
func NewNodeRef(nodeType, id string) NodeRef {
	return NodeRef{Type: nodeType, ID: id}
}

// NewEdge creates a new Edge with the given parameters.
func NewEdge(from, to NodeRef, edgeType string, data json.RawMessage) Edge {
	return Edge{
		From: from,
		To:   to,
		Type: edgeType,
		Data: data,
	}
}

// EdgeRef is a lightweight reference to an edge, stored as individual keys.
type EdgeRef struct {
	Node     NodeRef `json:"node"`
	EdgeType string  `json:"edgeType"`
}

// Graph provides graph operations on top of a KV store.
// This struct is safe for concurrent use as it only wraps the underlying store and paginator.
type Graph struct {
	store     kv.Store
	paginator *kv.Paginator
	// Maximum nodes to visit during traversal (prevents runaway operations).
	MaxTraversalNodes int
}

// New creates a new graph instance.
func New(store kv.Store) *Graph {
	return &Graph{
		store:             store,
		paginator:         kv.NewPaginator(store, 1000), // Default batch size of 1000
		MaxTraversalNodes: 100000,                       // Default max traversal nodes
	}
}

// NewWithBatchSize creates a new graph instance with a custom batch size for pagination.
func NewWithBatchSize(store kv.Store, batchSize int) *Graph {
	if batchSize <= 0 {
		batchSize = 1000
	}
	return &Graph{
		store:             store,
		paginator:         kv.NewPaginator(store, batchSize),
		MaxTraversalNodes: 100000,
	}
}

// NewWithConfig creates a new graph instance with custom configuration.
func NewWithConfig(store kv.Store, batchSize, maxTraversalNodes int) *Graph {
	if batchSize <= 0 {
		batchSize = 1000
	}
	if maxTraversalNodes <= 0 {
		maxTraversalNodes = 100000
	}
	return &Graph{
		store:             store,
		paginator:         kv.NewPaginator(store, batchSize),
		MaxTraversalNodes: maxTraversalNodes,
	}
}

// AddEdge creates a directed edge from one entity to another.
func (g *Graph) AddEdge(ctx context.Context, edge Edge) error {
	if edge.From.Type == "" || edge.From.ID == "" || edge.To.Type == "" || edge.To.ID == "" || edge.Type == "" {
		return fmt.Errorf("edge fields cannot be empty")
	}

	// Store the edge itself.
	edgeKey := edge.Key()

	// Store outgoing edge reference.
	outgoingRefKey := outgoingEdgeRefKey(edge.From, edge.Type, edge.To)

	// Store incoming edge reference.
	incomingRefKey := incomingEdgeRefKey(edge.To, edge.Type, edge.From)

	edgeRef := EdgeRef{
		Node:     edge.To,
		EdgeType: edge.Type,
	}

	incomingEdgeRef := EdgeRef{
		Node:     edge.From,
		EdgeType: edge.Type,
	}

	mutations := []kv.Mutation{
		kv.Put(edgeKey, 0, edge),
		kv.Put(outgoingRefKey, 0, edgeRef),
		kv.Put(incomingRefKey, 0, incomingEdgeRef),
	}

	_, err := g.store.MutateAll(ctx, mutations...)
	return err
}

// RemoveEdge removes a directed edge.
func (g *Graph) RemoveEdge(ctx context.Context, from NodeRef, edgeType string, to NodeRef) error {
	edge := NewEdge(from, to, edgeType, nil)
	edgeKey := edge.Key()
	outgoingRefKey := outgoingEdgeRefKey(from, edgeType, to)
	incomingRefKey := incomingEdgeRefKey(to, edgeType, from)

	mutations := []kv.Mutation{
		kv.Delete(edgeKey),
		kv.Delete(outgoingRefKey),
		kv.Delete(incomingRefKey),
	}

	_, err := g.store.MutateAll(ctx, mutations...)
	return err
}

// GetEdge retrieves a specific edge.
func (g *Graph) GetEdge(ctx context.Context, from NodeRef, edgeType string, to NodeRef) (edge Edge, ok bool, err error) {
	searchEdge := Edge{From: from, Type: edgeType, To: to}
	key := searchEdge.Key()
	_, ok, err = g.store.Get(ctx, key, &edge)
	if err != nil {
		return edge, false, err
	}
	return edge, ok, nil
}

// GetOutgoing returns an iterator that streams outgoing edges of a specific type from a node.
func (g *Graph) GetOutgoing(ctx context.Context, node NodeRef, edgeType string) iter.Seq2[Edge, error] {
	return func(yield func(Edge, error) bool) {
		var prefix string
		if edgeType == "*" {
			// Get all outgoing edges regardless of type.
			prefix = fmt.Sprintf("graph/node/%s/%s/outgoing/", node.Type, node.ID)
		} else {
			// Get edges of specific type.
			prefix = fmt.Sprintf("graph/node/%s/%s/outgoing/%s/", node.Type, node.ID, edgeType)
		}

		// Use paginator to stream edge references in batches.
		var edgeKeys []string
		var edgeRefs []EdgeRef

		for record, err := range g.paginator.GetPrefix(ctx, prefix) {
			if err != nil {
				yield(Edge{}, err)
				return
			}

			// Check context cancellation.
			if err := ctx.Err(); err != nil {
				yield(Edge{}, err)
				return
			}

			var edgeRef EdgeRef
			if err := json.Unmarshal(record.Value, &edgeRef); err != nil {
				continue // Skip malformed references
			}

			// Extract edge type from the key if we're in wildcard mode.
			actualEdgeType := edgeType
			if edgeType == "*" {
				actualEdgeType = extractEdgeTypeFromKey(record.Key)
			}

			// Build the edge key for batch retrieval.
			edge := Edge{From: node, Type: actualEdgeType, To: edgeRef.Node}
			edgeKey := edge.Key()
			edgeKeys = append(edgeKeys, edgeKey)
			edgeRefs = append(edgeRefs, edgeRef)

			// Process batch when we reach the paginator limit.
			if len(edgeKeys) >= g.paginator.Limit {
				if !g.processBatchedEdges(ctx, edgeKeys, yield) {
					return
				}
				edgeKeys = edgeKeys[:0]
				edgeRefs = edgeRefs[:0]
			}
		}

		// Process remaining edges.
		if len(edgeKeys) > 0 {
			g.processBatchedEdges(ctx, edgeKeys, yield)
		}
	}
}

// GetIncoming returns an iterator that streams incoming edges of a specific type to a node.
func (g *Graph) GetIncoming(ctx context.Context, node NodeRef, edgeType string) iter.Seq2[Edge, error] {
	return func(yield func(Edge, error) bool) {
		var prefix string
		if edgeType == "*" {
			// Get all incoming edges regardless of type.
			prefix = fmt.Sprintf("graph/node/%s/%s/incoming/", node.Type, node.ID)
		} else {
			// Get edges of specific type.
			prefix = fmt.Sprintf("graph/node/%s/%s/incoming/%s/", node.Type, node.ID, edgeType)
		}

		// Use paginator to stream edge references in batches.
		var edgeKeys []string

		for record, err := range g.paginator.GetPrefix(ctx, prefix) {
			if err != nil {
				yield(Edge{}, err)
				return
			}

			// Check context cancellation.
			if err := ctx.Err(); err != nil {
				yield(Edge{}, err)
				return
			}

			var edgeRef EdgeRef
			if err := json.Unmarshal(record.Value, &edgeRef); err != nil {
				// Skip malformed references.
				continue
			}

			// Extract edge type from the key if we're in wildcard mode.
			actualEdgeType := edgeType
			if edgeType == "*" {
				actualEdgeType = extractEdgeTypeFromKey(record.Key)
			}

			// Build the edge key for batch retrieval.
			edge := Edge{From: edgeRef.Node, Type: actualEdgeType, To: node}
			edgeKey := edge.Key()
			edgeKeys = append(edgeKeys, edgeKey)

			// Process batch when we reach the paginator limit.
			if len(edgeKeys) >= g.paginator.Limit {
				if !g.processBatchedEdges(ctx, edgeKeys, yield) {
					return
				}
				edgeKeys = edgeKeys[:0]
			}
		}

		// Process remaining edges.
		if len(edgeKeys) > 0 {
			g.processBatchedEdges(ctx, edgeKeys, yield)
		}
	}
}

// GetAllOutgoing returns an iterator that streams all outgoing edges from a node (all types).
func (g *Graph) GetAllOutgoing(ctx context.Context, node NodeRef) iter.Seq2[Edge, error] {
	return func(yield func(Edge, error) bool) {
		prefix := fmt.Sprintf("graph/node/%s/%s/outgoing/", node.Type, node.ID)

		// Use paginator to stream edge references in batches without grouping by type.
		var edgeKeys []string

		for record, err := range g.paginator.GetPrefix(ctx, prefix) {
			if err != nil {
				yield(Edge{}, err)
				return
			}

			// Check context cancellation.
			if err := ctx.Err(); err != nil {
				yield(Edge{}, err)
				return
			}

			var edgeRef EdgeRef
			if err := json.Unmarshal(record.Value, &edgeRef); err != nil {
				continue // Skip malformed references
			}

			// Extract edge type from the key.
			edgeType := extractEdgeTypeFromKey(record.Key)

			// Build the edge key for batch retrieval.
			edge := Edge{From: node, Type: edgeType, To: edgeRef.Node}
			edgeKey := edge.Key()
			edgeKeys = append(edgeKeys, edgeKey)

			// Process batch when we reach the paginator limit.
			if len(edgeKeys) >= g.paginator.Limit {
				if !g.processBatchedEdges(ctx, edgeKeys, yield) {
					return
				}
				edgeKeys = edgeKeys[:0]
			}
		}

		// Process remaining edges.
		if len(edgeKeys) > 0 {
			g.processBatchedEdges(ctx, edgeKeys, yield)
		}
	}
}

// GetAllIncoming returns an iterator that streams all incoming edges to a node (all types).
func (g *Graph) GetAllIncoming(ctx context.Context, node NodeRef) iter.Seq2[Edge, error] {
	return func(yield func(Edge, error) bool) {
		prefix := fmt.Sprintf("graph/node/%s/%s/incoming/", node.Type, node.ID)

		// Use paginator to stream edge references in batches without grouping by type.
		var edgeKeys []string

		for record, err := range g.paginator.GetPrefix(ctx, prefix) {
			if err != nil {
				yield(Edge{}, err)
				return
			}

			// Check context cancellation.
			if err := ctx.Err(); err != nil {
				yield(Edge{}, err)
				return
			}

			var edgeRef EdgeRef
			if err := json.Unmarshal(record.Value, &edgeRef); err != nil {
				continue // Skip malformed references
			}

			// Extract edge type from the key.
			edgeType := extractEdgeTypeFromKey(record.Key)

			// Build the edge key for batch retrieval.
			edge := Edge{From: edgeRef.Node, Type: edgeType, To: node}
			edgeKey := edge.Key()
			edgeKeys = append(edgeKeys, edgeKey)

			// Process batch when we reach the paginator limit.
			if len(edgeKeys) >= g.paginator.Limit {
				if !g.processBatchedEdges(ctx, edgeKeys, yield) {
					return
				}
				edgeKeys = edgeKeys[:0]
			}
		}

		// Process remaining edges.
		if len(edgeKeys) > 0 {
			g.processBatchedEdges(ctx, edgeKeys, yield)
		}
	}
}

// All returns an iterator that streams all edges in the graph.
func (g *Graph) All(ctx context.Context) iter.Seq2[Edge, error] {
	return func(yield func(Edge, error) bool) {
		// Use paginator to stream all edge keys.
		for record, err := range g.paginator.GetPrefix(ctx, "graph/edge/") {
			if err != nil {
				yield(Edge{}, err)
				return
			}

			// Check context cancellation.
			if err := ctx.Err(); err != nil {
				yield(Edge{}, err)
				return
			}

			var edge Edge
			if err := json.Unmarshal(record.Value, &edge); err != nil {
				continue // Skip malformed edges.
			}
			if !yield(edge, nil) {
				return
			}
		}
	}
}

// Helper functions.

// processBatchedEdges retrieves edges in batch and yields them.
func (g *Graph) processBatchedEdges(ctx context.Context, edgeKeys []string, yield func(Edge, error) bool) bool {
	if len(edgeKeys) == 0 {
		return true
	}

	// Use GetBatch to retrieve all edges at once.
	records, err := g.store.GetBatch(ctx, edgeKeys...)
	if err != nil {
		yield(Edge{}, err)
		return false
	}

	// Process each retrieved edge.
	for _, record := range records {
		// Check context cancellation.
		if err := ctx.Err(); err != nil {
			yield(Edge{}, err)
			return false
		}

		var edge Edge
		if err := json.Unmarshal(record.Value, &edge); err != nil {
			continue // Skip malformed edges
		}

		if !yield(edge, nil) {
			return false
		}
	}

	return true
}

// Individual edge reference keys for scalability.
func outgoingEdgeRefKey(from NodeRef, edgeType string, to NodeRef) string {
	return fmt.Sprintf("graph/node/%s/%s/outgoing/%s/%s/%s", from.Type, from.ID, edgeType, to.Type, to.ID)
}

func incomingEdgeRefKey(to NodeRef, edgeType string, from NodeRef) string {
	return fmt.Sprintf("graph/node/%s/%s/incoming/%s/%s/%s", to.Type, to.ID, edgeType, from.Type, from.ID)
}

func extractEdgeTypeFromKey(key string) string {
	parts := strings.Split(key, "/")
	// Key structure: graph/node/{type}/{id}/outgoing/{edgeType}/{targetType}/{targetID}
	// or: graph/node/{type}/{id}/incoming/{edgeType}/{sourceType}/{sourceID}
	if len(parts) < 6 {
		return ""
	}
	return parts[5] // The edge type is at index 5
}
