package graph

import (
	"context"
	"fmt"
)

// TraversalOptions configure graph traversal behavior.
type TraversalOptions struct {
	MaxDepth   int            // Maximum depth to traverse (0 = unlimited)
	EdgeTypes  []string       // Filter by specific edge types
	VisitLimit int            // Maximum nodes to visit (0 = unlimited)
	Properties map[string]any // Filter edges by properties
}

// Path represents a path through the graph.
type Path struct {
	Nodes []NodeRef `json:"nodes"`
	Edges []Edge    `json:"edges"`
	Depth int       `json:"depth"`
}

// NodeRef represents a reference to a graph node.
type NodeRef struct {
	EntityType string `json:"entityType"`
	EntityID   string `json:"entityID"`
}

// BreadthFirstSearch performs BFS traversal from a starting node.
func (g *Graph) BreadthFirstSearch(ctx context.Context, startEntityType, startEntityID string, opts TraversalOptions) ([]Path, error) {
	var paths []Path
	visited := make(map[string]bool)
	queue := []Path{{
		Nodes: []NodeRef{{EntityType: startEntityType, EntityID: startEntityID}},
		Edges: []Edge{},
		Depth: 0,
	}}

	visited[nodeKey(startEntityType, startEntityID)] = true

	for len(queue) > 0 && (opts.VisitLimit == 0 || len(paths) < opts.VisitLimit) {
		currentPath := queue[0]
		queue = queue[1:]

		paths = append(paths, currentPath)

		// Stop if we've reached max depth.
		if opts.MaxDepth > 0 && currentPath.Depth >= opts.MaxDepth {
			continue
		}

		currentNode := currentPath.Nodes[len(currentPath.Nodes)-1]

		// Get all outgoing edges from current node.
		var edges []Edge
		var err error

		if len(opts.EdgeTypes) == 0 {
			edges, err = g.GetAllOutgoingEdges(ctx, currentNode.EntityType, currentNode.EntityID)
		} else {
			for _, edgeType := range opts.EdgeTypes {
				typeEdges, err := g.GetOutgoingEdges(ctx, currentNode.EntityType, currentNode.EntityID, edgeType)
				if err != nil {
					return nil, err
				}
				edges = append(edges, typeEdges...)
			}
		}

		if err != nil {
			return nil, err
		}

		// Filter edges by properties if specified.
		edges = g.filterEdgesByProperties(edges, opts.Properties)

		// Add unvisited neighbors to queue.
		for _, edge := range edges {
			neighborKey := nodeKey(edge.ToEntityType, edge.ToEntityID)
			if !visited[neighborKey] {
				visited[neighborKey] = true

				newPath := Path{
					Nodes: append(currentPath.Nodes, NodeRef{
						EntityType: edge.ToEntityType,
						EntityID:   edge.ToEntityID,
					}),
					Edges: append(currentPath.Edges, edge),
					Depth: currentPath.Depth + 1,
				}

				queue = append(queue, newPath)
			}
		}
	}

	return paths, nil
}

// FindShortestPath finds the shortest path between two nodes using BFS.
func (g *Graph) FindShortestPath(ctx context.Context, fromEntityType, fromEntityID, toEntityType, toEntityID string, opts TraversalOptions) (*Path, error) {
	targetKey := nodeKey(toEntityType, toEntityID)
	visited := make(map[string]bool)
	queue := []Path{{
		Nodes: []NodeRef{{EntityType: fromEntityType, EntityID: fromEntityID}},
		Edges: []Edge{},
		Depth: 0,
	}}

	visited[nodeKey(fromEntityType, fromEntityID)] = true

	for len(queue) > 0 {
		currentPath := queue[0]
		queue = queue[1:]

		currentNode := currentPath.Nodes[len(currentPath.Nodes)-1]
		currentKey := nodeKey(currentNode.EntityType, currentNode.EntityID)

		// Check if we've reached the target.
		if currentKey == targetKey {
			return &currentPath, nil
		}

		// Stop if we've reached max depth.
		if opts.MaxDepth > 0 && currentPath.Depth >= opts.MaxDepth {
			continue
		}

		// Get all outgoing edges from current node.
		var edges []Edge
		var err error

		if len(opts.EdgeTypes) == 0 {
			edges, err = g.GetAllOutgoingEdges(ctx, currentNode.EntityType, currentNode.EntityID)
			if err != nil {
				return nil, err
			}
		} else {
			for _, edgeType := range opts.EdgeTypes {
				typeEdges, err := g.GetOutgoingEdges(ctx, currentNode.EntityType, currentNode.EntityID, edgeType)
				if err != nil {
					return nil, err
				}
				edges = append(edges, typeEdges...)
			}
		}

		// Filter edges by properties if specified.
		edges = g.filterEdgesByProperties(edges, opts.Properties)

		// Add unvisited neighbors to queue.
		for _, edge := range edges {
			neighborKey := nodeKey(edge.ToEntityType, edge.ToEntityID)
			if !visited[neighborKey] {
				visited[neighborKey] = true

				newPath := Path{
					Nodes: append(currentPath.Nodes, NodeRef{
						EntityType: edge.ToEntityType,
						EntityID:   edge.ToEntityID,
					}),
					Edges: append(currentPath.Edges, edge),
					Depth: currentPath.Depth + 1,
				}

				queue = append(queue, newPath)
			}
		}
	}

	return nil, nil // No path found
}

// FindMutualConnections finds entities that are connected to both given entities.
func (g *Graph) FindMutualConnections(ctx context.Context, entity1Type, entity1ID, entity2Type, entity2ID, connectionType string) ([]NodeRef, error) {
	// Get all entities that entity1 connects to.
	edges1, err := g.GetOutgoingEdges(ctx, entity1Type, entity1ID, connectionType)
	if err != nil {
		return nil, err
	}

	// Get all entities that entity2 connects to.
	edges2, err := g.GetOutgoingEdges(ctx, entity2Type, entity2ID, connectionType)
	if err != nil {
		return nil, err
	}

	// Find intersection.
	connected1 := make(map[string]NodeRef)
	for _, edge := range edges1 {
		key := nodeKey(edge.ToEntityType, edge.ToEntityID)
		connected1[key] = NodeRef{
			EntityType: edge.ToEntityType,
			EntityID:   edge.ToEntityID,
		}
	}

	var mutual []NodeRef
	for _, edge := range edges2 {
		key := nodeKey(edge.ToEntityType, edge.ToEntityID)
		if node, exists := connected1[key]; exists {
			mutual = append(mutual, node)
		}
	}

	return mutual, nil
}

// GetDegree returns the in-degree and out-degree of a node for a specific edge type.
func (g *Graph) GetDegree(ctx context.Context, entityType, entityID, edgeType string) (inDegree, outDegree int, err error) {
	outgoingEdges, err := g.GetOutgoingEdges(ctx, entityType, entityID, edgeType)
	if err != nil {
		return 0, 0, err
	}
	outDegree = len(outgoingEdges)

	incomingEdges, err := g.GetIncomingEdges(ctx, entityType, entityID, edgeType)
	if err != nil {
		return 0, 0, err
	}
	inDegree = len(incomingEdges)

	return inDegree, outDegree, nil
}

// GetNeighbors returns all neighbors (both incoming and outgoing) of a node.
func (g *Graph) GetNeighbors(ctx context.Context, entityType, entityID string, opts TraversalOptions) ([]NodeRef, error) {
	neighbors := make(map[string]NodeRef)

	// Get outgoing neighbors.
	var outgoingEdges []Edge
	var err error

	if len(opts.EdgeTypes) == 0 {
		outgoingEdges, err = g.GetAllOutgoingEdges(ctx, entityType, entityID)
	} else {
		for _, edgeType := range opts.EdgeTypes {
			typeEdges, err := g.GetOutgoingEdges(ctx, entityType, entityID, edgeType)
			if err != nil {
				return nil, err
			}
			outgoingEdges = append(outgoingEdges, typeEdges...)
		}
	}

	if err != nil {
		return nil, err
	}

	outgoingEdges = g.filterEdgesByProperties(outgoingEdges, opts.Properties)
	for _, edge := range outgoingEdges {
		key := nodeKey(edge.ToEntityType, edge.ToEntityID)
		neighbors[key] = NodeRef{
			EntityType: edge.ToEntityType,
			EntityID:   edge.ToEntityID,
		}
	}

	// Get incoming neighbors.
	var incomingEdges []Edge

	if len(opts.EdgeTypes) == 0 {
		incomingEdges, err = g.GetAllIncomingEdges(ctx, entityType, entityID)
	} else {
		for _, edgeType := range opts.EdgeTypes {
			typeEdges, err := g.GetIncomingEdges(ctx, entityType, entityID, edgeType)
			if err != nil {
				return nil, err
			}
			incomingEdges = append(incomingEdges, typeEdges...)
		}
	}

	if err != nil {
		return nil, err
	}

	incomingEdges = g.filterEdgesByProperties(incomingEdges, opts.Properties)
	for _, edge := range incomingEdges {
		key := nodeKey(edge.FromEntityType, edge.FromEntityID)
		neighbors[key] = NodeRef{
			EntityType: edge.FromEntityType,
			EntityID:   edge.FromEntityID,
		}
	}

	// Convert map to slice.
	var result []NodeRef
	for _, neighbor := range neighbors {
		result = append(result, neighbor)
	}

	return result, nil
}

// Helper functions.

func nodeKey(entityType, entityID string) string {
	return fmt.Sprintf("%s:%s", entityType, entityID)
}

func (g *Graph) filterEdgesByProperties(edges []Edge, properties map[string]any) []Edge {
	if len(properties) == 0 {
		return edges
	}

	var filtered []Edge
	for _, edge := range edges {
		matches := true
		for key, value := range properties {
			if edge.Properties == nil {
				matches = false
				break
			}
			if edge.Properties[key] != value {
				matches = false
				break
			}
		}
		if matches {
			filtered = append(filtered, edge)
		}
	}

	return filtered
}
