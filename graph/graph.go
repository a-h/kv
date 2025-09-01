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
	FromEntityType string         `json:"fromEntityType"`
	FromEntityID   string         `json:"fromEntityID"`
	ToEntityType   string         `json:"toEntityType"`
	ToEntityID     string         `json:"toEntityID"`
	Type           string         `json:"type"`
	Properties     map[string]any `json:"properties,omitempty"`
}

// EdgeRef is a lightweight reference to an edge, stored as individual keys.
type EdgeRef struct {
	EntityType string `json:"entityType"`
	EntityID   string `json:"entityID"`
	EdgeType   string `json:"edgeType"`
}

// Graph provides graph operations on top of a KV store.
type Graph struct {
	store kv.Store
}

// New creates a new graph instance.
func New(store kv.Store) *Graph {
	return &Graph{store: store}
}

// AddEdge creates a directed edge from one entity to another.
func (g *Graph) AddEdge(ctx context.Context, edge Edge) error {
	// Store the edge itself.
	edgeKey := edgeKey(edge.FromEntityType, edge.FromEntityID, edge.Type, edge.ToEntityType, edge.ToEntityID)

	// Store outgoing edge reference.
	outgoingRefKey := outgoingEdgeRefKey(edge.FromEntityType, edge.FromEntityID, edge.Type, edge.ToEntityType, edge.ToEntityID)

	// Store incoming edge reference.
	incomingRefKey := incomingEdgeRefKey(edge.ToEntityType, edge.ToEntityID, edge.Type, edge.FromEntityType, edge.FromEntityID)

	edgeRef := EdgeRef{
		EntityType: edge.ToEntityType,
		EntityID:   edge.ToEntityID,
		EdgeType:   edge.Type,
	}

	incomingEdgeRef := EdgeRef{
		EntityType: edge.FromEntityType,
		EntityID:   edge.FromEntityID,
		EdgeType:   edge.Type,
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
func (g *Graph) RemoveEdge(ctx context.Context, fromEntityType, fromEntityID, edgeType, toEntityType, toEntityID string) error {
	edgeKey := edgeKey(fromEntityType, fromEntityID, edgeType, toEntityType, toEntityID)
	outgoingRefKey := outgoingEdgeRefKey(fromEntityType, fromEntityID, edgeType, toEntityType, toEntityID)
	incomingRefKey := incomingEdgeRefKey(toEntityType, toEntityID, edgeType, fromEntityType, fromEntityID)

	mutations := []kv.Mutation{
		kv.Delete(edgeKey),
		kv.Delete(outgoingRefKey),
		kv.Delete(incomingRefKey),
	}

	_, err := g.store.MutateAll(ctx, mutations...)
	return err
}

// GetEdge retrieves a specific edge.
func (g *Graph) GetEdge(ctx context.Context, fromEntityType, fromEntityID, edgeType, toEntityType, toEntityID string) (Edge, bool, error) {
	var edge Edge
	key := edgeKey(fromEntityType, fromEntityID, edgeType, toEntityType, toEntityID)
	_, ok, err := g.store.Get(ctx, key, &edge)
	if err != nil {
		return edge, false, err
	}
	return edge, ok, nil
}

// GetOutgoingEdges returns all outgoing edges of a specific type from an entity.
func (g *Graph) GetOutgoingEdges(ctx context.Context, entityType, entityID, edgeType string) (edges []Edge, err error) {
	for edge, err := range g.StreamOutgoingEdges(ctx, entityType, entityID, edgeType) {
		if err != nil {
			return nil, err
		}
		edges = append(edges, edge)
	}
	return edges, nil
}

// GetIncomingEdges returns all incoming edges of a specific type to an entity.
func (g *Graph) GetIncomingEdges(ctx context.Context, entityType, entityID, edgeType string) (edges []Edge, err error) {
	for edge, err := range g.StreamIncomingEdges(ctx, entityType, entityID, edgeType) {
		if err != nil {
			return nil, err
		}
		edges = append(edges, edge)
	}
	return edges, nil
}

// GetAllOutgoingEdges returns all outgoing edges from an entity (all types).
func (g *Graph) GetAllOutgoingEdges(ctx context.Context, entityType, entityID string) (allEdges []Edge, err error) {
	for edge, err := range g.StreamAllOutgoingEdges(ctx, entityType, entityID) {
		if err != nil {
			return nil, err
		}
		allEdges = append(allEdges, edge)
	}
	return allEdges, nil
}

// GetAllIncomingEdges returns all incoming edges to an entity (all types).
func (g *Graph) GetAllIncomingEdges(ctx context.Context, entityType, entityID string) (allEdges []Edge, err error) {
	for edge, err := range g.StreamAllIncomingEdges(ctx, entityType, entityID) {
		if err != nil {
			return nil, err
		}
		allEdges = append(allEdges, edge)
	}
	return allEdges, nil
}

// StreamOutgoingEdges returns an iterator that streams outgoing edges of a specific type from an entity.
func (g *Graph) StreamOutgoingEdges(ctx context.Context, entityType, entityID, edgeType string) iter.Seq2[Edge, error] {
	return func(yield func(Edge, error) bool) {
		var prefix string
		if edgeType == "*" {
			// Get all outgoing edges regardless of type.
			prefix = fmt.Sprintf("graph/node/%s/%s/outgoing/", entityType, entityID)
		} else {
			// Get edges of specific type.
			prefix = fmt.Sprintf("graph/node/%s/%s/outgoing/%s/", entityType, entityID, edgeType)
		}

		records, err := g.store.GetPrefix(ctx, prefix, 0, -1)
		if err != nil {
			yield(Edge{}, err)
			return
		}

		for _, record := range records {
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

			// Get the full edge.
			edge, exists, err := g.GetEdge(ctx, entityType, entityID, actualEdgeType, edgeRef.EntityType, edgeRef.EntityID)
			if err != nil {
				if !yield(Edge{}, err) {
					return
				}
				continue
			}
			if exists {
				if !yield(edge, nil) {
					return
				}
			}
		}
	}
}

// StreamIncomingEdges returns an iterator that streams incoming edges of a specific type to an entity.
func (g *Graph) StreamIncomingEdges(ctx context.Context, entityType, entityID, edgeType string) iter.Seq2[Edge, error] {
	return func(yield func(Edge, error) bool) {
		var prefix string
		if edgeType == "*" {
			// Get all incoming edges regardless of type.
			prefix = fmt.Sprintf("graph/node/%s/%s/incoming/", entityType, entityID)
		} else {
			// Get edges of specific type.
			prefix = fmt.Sprintf("graph/node/%s/%s/incoming/%s/", entityType, entityID, edgeType)
		}

		records, err := g.store.GetPrefix(ctx, prefix, 0, -1)
		if err != nil {
			yield(Edge{}, err)
			return
		}

		for _, record := range records {
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

			// Get the full edge.
			edge, exists, err := g.GetEdge(ctx, edgeRef.EntityType, edgeRef.EntityID, actualEdgeType, entityType, entityID)
			if err != nil {
				if !yield(Edge{}, err) {
					return
				}
				continue
			}
			if exists {
				if !yield(edge, nil) {
					return
				}
			}
		}
	}
}

// StreamAllOutgoingEdges returns an iterator that streams all outgoing edges from an entity (all types).
func (g *Graph) StreamAllOutgoingEdges(ctx context.Context, entityType, entityID string) iter.Seq2[Edge, error] {
	return func(yield func(Edge, error) bool) {
		prefix := fmt.Sprintf("graph/node/%s/%s/outgoing/", entityType, entityID)
		records, err := g.store.GetPrefix(ctx, prefix, 0, -1)
		if err != nil {
			yield(Edge{}, err)
			return
		}

		// Group records by edge type to avoid duplicate processing.
		edgeTypes := make(map[string]bool)
		for _, record := range records {
			edgeType := extractEdgeTypeFromKey(record.Key)
			if edgeType != "" {
				edgeTypes[edgeType] = true
			}
		}

		for edgeType := range edgeTypes {
			// Check context cancellation.
			if err := ctx.Err(); err != nil {
				yield(Edge{}, err)
				return
			}

			for edge, err := range g.StreamOutgoingEdges(ctx, entityType, entityID, edgeType) {
				if err != nil {
					if !yield(Edge{}, err) {
						return
					}
					continue
				}
				if !yield(edge, nil) {
					return
				}
			}
		}
	}
}

// StreamAllIncomingEdges returns an iterator that streams all incoming edges to an entity (all types).
func (g *Graph) StreamAllIncomingEdges(ctx context.Context, entityType, entityID string) iter.Seq2[Edge, error] {
	return func(yield func(Edge, error) bool) {
		prefix := fmt.Sprintf("graph/node/%s/%s/incoming/", entityType, entityID)
		records, err := g.store.GetPrefix(ctx, prefix, 0, -1)
		if err != nil {
			yield(Edge{}, err)
			return
		}

		// Group records by edge type to avoid duplicate processing.
		edgeTypes := make(map[string]bool)
		for _, record := range records {
			edgeType := extractEdgeTypeFromKey(record.Key)
			if edgeType != "" {
				edgeTypes[edgeType] = true
			}
		}

		for edgeType := range edgeTypes {
			// Check context cancellation.
			if err := ctx.Err(); err != nil {
				yield(Edge{}, err)
				return
			}

			for edge, err := range g.StreamIncomingEdges(ctx, entityType, entityID, edgeType) {
				if err != nil {
					if !yield(Edge{}, err) {
						return
					}
					continue
				}
				if !yield(edge, nil) {
					return
				}
			}
		}
	}
}

// StreamAllEdges returns an iterator that streams all edges in the graph.
func (g *Graph) StreamAllEdges(ctx context.Context) iter.Seq2[Edge, error] {
	return func(yield func(Edge, error) bool) {
		// List all edge keys by using the "graph/edge/" prefix.
		records, err := g.store.GetPrefix(ctx, "graph/edge/", 0, -1)
		if err != nil {
			yield(Edge{}, err)
			return
		}

		for _, record := range records {
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

func edgeKey(fromEntityType, fromEntityID, edgeType, toEntityType, toEntityID string) string {
	return fmt.Sprintf("graph/edge/%s/%s/%s/%s/%s", fromEntityType, fromEntityID, edgeType, toEntityType, toEntityID)
}

// Individual edge reference keys for scalability.
func outgoingEdgeRefKey(fromEntityType, fromEntityID, edgeType, toEntityType, toEntityID string) string {
	return fmt.Sprintf("graph/node/%s/%s/outgoing/%s/%s/%s", fromEntityType, fromEntityID, edgeType, toEntityType, toEntityID)
}

func incomingEdgeRefKey(toEntityType, toEntityID, edgeType, fromEntityType, fromEntityID string) string {
	return fmt.Sprintf("graph/node/%s/%s/incoming/%s/%s/%s", toEntityType, toEntityID, edgeType, fromEntityType, fromEntityID)
}

// ListAllEdges returns all edges in the graph.
func (g *Graph) ListAllEdges(ctx context.Context) ([]Edge, error) {
	var edges []Edge
	for edge, err := range g.StreamAllEdges(ctx) {
		if err != nil {
			return nil, err
		}
		edges = append(edges, edge)
	}
	return edges, nil
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
