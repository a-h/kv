package graph

import (
	"context"
	"encoding/json"
	"fmt"
	"iter"
	"net/url"
	"strings"

	"github.com/a-h/kv"
)

// Edge represents a relationship between two nodes.
type Edge struct {
	From NodeRef         `json:"from"`
	To   NodeRef         `json:"to"`
	Type string          `json:"type"`
	Data json.RawMessage `json:"data,omitempty"`
}

// Key returns a unique string key for this edge.
func (e Edge) Key() string {
	return buildEdgeKey(e.From, e.Type, e.To)
}

// NodeRef represents a reference to a node in the graph.
type NodeRef struct {
	Type string `json:"type"`
	ID   string `json:"id"`
}

// Key returns a unique string key for this node reference.
func (n NodeRef) Key() string {
	return buildNodeRefKey(n.Type, n.ID)
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

// New creates a new graph instance with default settings.
// Uses a default paginator with batch size 1000 and max traversal nodes 100000.
func New(store kv.Store) *Graph {
	return NewWithPaginator(kv.NewPaginator(store))
}

// NewWithPaginator creates a new graph instance with a custom paginator.
// This allows fine-grained control over batch sizes and pagination behavior.
// The MaxTraversalNodes can be set on the returned graph if needed.
func NewWithPaginator(paginator *kv.Paginator) *Graph {
	return &Graph{
		store:             paginator.Store,
		paginator:         paginator,
		MaxTraversalNodes: 100000, // Default max traversal nodes
	}
}

// AddEdge creates a directed edge from one node to another.
func (g *Graph) AddEdge(ctx context.Context, edge Edge) error {
	if edge.From.Type == "" || edge.From.ID == "" || edge.To.Type == "" || edge.To.ID == "" || edge.Type == "" {
		return fmt.Errorf("edge fields cannot be empty")
	}

	// Store the edge itself.
	edgeKey := edge.Key()

	// Store outgoing edge reference.
	outgoingRefKey := buildOutgoingEdgeRefKey(edge.From, edge.Type, edge.To)

	// Store incoming edge reference.
	incomingRefKey := buildIncomingEdgeRefKey(edge.To, edge.Type, edge.From)

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
	outgoingRefKey := buildOutgoingEdgeRefKey(from, edgeType, to)
	incomingRefKey := buildIncomingEdgeRefKey(to, edgeType, from)

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
		prefix := buildOutgoingEdgeRefPrefix(node, edgeType)

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
				// Skip malformed references.
				continue
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
		prefix := buildIncomingEdgeRefPrefix(node, edgeType)

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
	return g.GetOutgoing(ctx, node, "*")
}

// GetAllIncoming returns an iterator that streams all incoming edges to a node (all types).
func (g *Graph) GetAllIncoming(ctx context.Context, node NodeRef) iter.Seq2[Edge, error] {
	return g.GetIncoming(ctx, node, "*")
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
				// Skip malformed edges.
				continue
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
			// Skip malformed edges.
			continue
		}

		if !yield(edge, nil) {
			return false
		}
	}

	return true
}

func buildEdgeKey(from NodeRef, edgeType string, to NodeRef) string {
	return fmt.Sprintf("graph/edge/%s/%s/%s/%s/%s",
		url.PathEscape(from.Type),
		url.PathEscape(from.ID),
		url.PathEscape(edgeType),
		url.PathEscape(to.Type),
		url.PathEscape(to.ID))
}

func buildNodeRefKey(nodeType, nodeID string) string {
	return fmt.Sprintf("%s:%s", url.PathEscape(nodeType), url.PathEscape(nodeID))
}

func buildOutgoingEdgeRefKey(from NodeRef, edgeType string, to NodeRef) string {
	return fmt.Sprintf("graph/node/%s/%s/outgoing/%s/%s/%s",
		url.PathEscape(from.Type),
		url.PathEscape(from.ID),
		url.PathEscape(edgeType),
		url.PathEscape(to.Type),
		url.PathEscape(to.ID))
}

func buildIncomingEdgeRefKey(to NodeRef, edgeType string, from NodeRef) string {
	return fmt.Sprintf("graph/node/%s/%s/incoming/%s/%s/%s",
		url.PathEscape(to.Type),
		url.PathEscape(to.ID),
		url.PathEscape(edgeType),
		url.PathEscape(from.Type),
		url.PathEscape(from.ID))
}

func buildOutgoingEdgeRefPrefix(node NodeRef, edgeType string) string {
	if edgeType == "*" {
		return fmt.Sprintf("graph/node/%s/%s/outgoing/",
			url.PathEscape(node.Type),
			url.PathEscape(node.ID))
	}
	return fmt.Sprintf("graph/node/%s/%s/outgoing/%s/",
		url.PathEscape(node.Type),
		url.PathEscape(node.ID),
		url.PathEscape(edgeType))
}

func buildIncomingEdgeRefPrefix(node NodeRef, edgeType string) string {
	if edgeType == "*" {
		return fmt.Sprintf("graph/node/%s/%s/incoming/",
			url.PathEscape(node.Type),
			url.PathEscape(node.ID))
	}
	return fmt.Sprintf("graph/node/%s/%s/incoming/%s/",
		url.PathEscape(node.Type),
		url.PathEscape(node.ID),
		url.PathEscape(edgeType))
}

func extractEdgeTypeFromKey(key string) string {
	// Key structure: graph/node/{type}/{id}/outgoing/{edgeType}/{targetType}/{targetID}
	// or: graph/node/{type}/{id}/incoming/{edgeType}/{sourceType}/{sourceID}
	// All components are URL path escaped.

	// Find the position of "/outgoing/" or "/incoming/"
	outgoingPos := strings.Index(key, "/outgoing/")
	incomingPos := strings.Index(key, "/incoming/")

	var startPos int
	if outgoingPos != -1 {
		startPos = outgoingPos + len("/outgoing/")
	} else if incomingPos != -1 {
		startPos = incomingPos + len("/incoming/")
	} else {
		return "" // Invalid key format
	}

	// Find the next slash after the edge type
	endPos := strings.Index(key[startPos:], "/")
	if endPos == -1 {
		return "" // Invalid key format - should have target type after edge type
	}

	// Extract the URL-encoded edge type and unescape it
	encodedEdgeType := key[startPos : startPos+endPos]
	edgeType, err := url.PathUnescape(encodedEdgeType)
	if err != nil {
		return "" // Invalid URL encoding
	}

	return edgeType
}
