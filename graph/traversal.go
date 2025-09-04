package graph

import (
	"context"
	"encoding/json"
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

// BreadthFirstSearch performs BFS traversal from a starting node.
func (g *Graph) BreadthFirstSearch(ctx context.Context, startEntityType, startEntityID string, opts TraversalOptions) ([]Path, error) {
	if startEntityType == "" || startEntityID == "" {
		return nil, fmt.Errorf("start entity type and ID cannot be empty")
	}
	if opts.MaxDepth < 0 {
		return nil, fmt.Errorf("max depth cannot be negative")
	}
	if opts.VisitLimit < 0 {
		return nil, fmt.Errorf("visit limit cannot be negative")
	}

	var paths []Path
	visited := make(map[string]bool)
	queue := []Path{{
		Nodes: []NodeRef{{Type: startEntityType, ID: startEntityID}},
		Edges: []Edge{},
		Depth: 0,
	}}

	visited[NewNodeRef(startEntityType, startEntityID).Key()] = true
	nodesVisited := 1 // Track for observability

	for len(queue) > 0 && (opts.VisitLimit == 0 || len(paths) < opts.VisitLimit) {
		// Check for runaway traversals.
		if nodesVisited > g.MaxTraversalNodes {
			return nil, fmt.Errorf("traversal exceeded maximum nodes (%d) - consider using MaxDepth or VisitLimit", g.MaxTraversalNodes)
		}
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

		if len(opts.EdgeTypes) == 0 {
			for edge, err := range g.GetAllOutgoing(ctx, currentNode) {
				if err != nil {
					return nil, err
				}
				edges = append(edges, edge)
			}
		} else {
			for _, edgeType := range opts.EdgeTypes {
				for edge, err := range g.GetOutgoing(ctx, currentNode, edgeType) {
					if err != nil {
						return nil, err
					}
					edges = append(edges, edge)
				}
			}
		}

		// Filter edges by properties if specified.
		edges = g.filterEdgesByProperties(edges, opts.Properties)

		// Add unvisited neighbors to queue.
		for _, edge := range edges {
			neighborKey := edge.To.Key()
			if !visited[neighborKey] {
				visited[neighborKey] = true
				nodesVisited++

				newPath := Path{
					Nodes: append(currentPath.Nodes, edge.To),
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
	if fromEntityType == "" || fromEntityID == "" || toEntityType == "" || toEntityID == "" {
		return nil, fmt.Errorf("entity types and IDs cannot be empty")
	}
	if opts.MaxDepth < 0 {
		return nil, fmt.Errorf("max depth cannot be negative")
	}

	targetKey := NewNodeRef(toEntityType, toEntityID).Key()
	visited := make(map[string]bool)
	queue := []Path{{
		Nodes: []NodeRef{{Type: fromEntityType, ID: fromEntityID}},
		Edges: []Edge{},
		Depth: 0,
	}}

	visited[NewNodeRef(fromEntityType, fromEntityID).Key()] = true
	nodesVisited := 1 // Track for observability

	for len(queue) > 0 {
		// Check for runaway traversals.
		if nodesVisited > g.MaxTraversalNodes {
			return nil, fmt.Errorf("traversal exceeded maximum nodes (%d) - consider using MaxDepth", g.MaxTraversalNodes)
		}

		currentPath := queue[0]
		queue = queue[1:]

		currentNode := currentPath.Nodes[len(currentPath.Nodes)-1]
		currentKey := currentNode.Key()

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

		if len(opts.EdgeTypes) == 0 {
			for edge, err := range g.GetAllOutgoing(ctx, currentNode) {
				if err != nil {
					return nil, err
				}
				edges = append(edges, edge)
			}
		} else {
			for _, edgeType := range opts.EdgeTypes {
				for edge, err := range g.GetOutgoing(ctx, currentNode, edgeType) {
					if err != nil {
						return nil, err
					}
					edges = append(edges, edge)
				}
			}
		}

		// Filter edges by properties if specified.
		edges = g.filterEdgesByProperties(edges, opts.Properties)

		// Add unvisited neighbors to queue.
		for _, edge := range edges {
			neighborKey := edge.To.Key()
			if !visited[neighborKey] {
				visited[neighborKey] = true
				nodesVisited++

				newPath := Path{
					Nodes: append(currentPath.Nodes, edge.To),
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
	entity1 := NewNodeRef(entity1Type, entity1ID)
	entity2 := NewNodeRef(entity2Type, entity2ID)
	
	// Get all entities that entity1 connects to.
	var edges1 []Edge
	for edge, err := range g.GetOutgoing(ctx, entity1, connectionType) {
		if err != nil {
			return nil, err
		}
		edges1 = append(edges1, edge)
	}

	// Get all entities that entity2 connects to.
	var edges2 []Edge
	for edge, err := range g.GetOutgoing(ctx, entity2, connectionType) {
		if err != nil {
			return nil, err
		}
		edges2 = append(edges2, edge)
	}

	// Find intersection.
	connected1 := make(map[string]NodeRef)
	for _, edge := range edges1 {
		key := edge.To.Key()
		connected1[key] = edge.To
	}

	var mutual []NodeRef
	for _, edge := range edges2 {
		key := edge.To.Key()
		if node, exists := connected1[key]; exists {
			mutual = append(mutual, node)
		}
	}

	return mutual, nil
}

// GetDegree returns the in-degree and out-degree of a node for a specific edge type.
func (g *Graph) GetDegree(ctx context.Context, entityType, entityID, edgeType string) (inDegree, outDegree int, err error) {
	node := NewNodeRef(entityType, entityID)
	
	for _, err := range g.GetOutgoing(ctx, node, edgeType) {
		if err != nil {
			return 0, 0, err
		}
		outDegree++
	}

	for _, err := range g.GetIncoming(ctx, node, edgeType) {
		if err != nil {
			return 0, 0, err
		}
		inDegree++
	}

	return inDegree, outDegree, nil
}

// GetNeighbors returns all neighbors (both incoming and outgoing) of a node.
func (g *Graph) GetNeighbors(ctx context.Context, entityType, entityID string, opts TraversalOptions) ([]NodeRef, error) {
	node := NewNodeRef(entityType, entityID)
	neighbors := make(map[string]NodeRef)

	// Get outgoing neighbors.
	var outgoingEdges []Edge

	if len(opts.EdgeTypes) == 0 {
		for edge, err := range g.GetAllOutgoing(ctx, node) {
			if err != nil {
				return nil, err
			}
			outgoingEdges = append(outgoingEdges, edge)
		}
	} else {
		for _, edgeType := range opts.EdgeTypes {
			for edge, err := range g.GetOutgoing(ctx, node, edgeType) {
				if err != nil {
					return nil, err
				}
				outgoingEdges = append(outgoingEdges, edge)
			}
		}
	}

	outgoingEdges = g.filterEdgesByProperties(outgoingEdges, opts.Properties)
	for _, edge := range outgoingEdges {
		key := edge.To.Key()
		neighbors[key] = edge.To
	}

	// Get incoming neighbors.
	var incomingEdges []Edge

	if len(opts.EdgeTypes) == 0 {
		for edge, err := range g.GetAllIncoming(ctx, node) {
			if err != nil {
				return nil, err
			}
			incomingEdges = append(incomingEdges, edge)
		}
	} else {
		for _, edgeType := range opts.EdgeTypes {
			for edge, err := range g.GetIncoming(ctx, node, edgeType) {
				if err != nil {
					return nil, err
				}
				incomingEdges = append(incomingEdges, edge)
			}
		}
	}

	incomingEdges = g.filterEdgesByProperties(incomingEdges, opts.Properties)
	for _, edge := range incomingEdges {
		key := edge.From.Key()
		neighbors[key] = edge.From
	}

	// Convert map to slice.
	var result []NodeRef
	for _, neighbor := range neighbors {
		result = append(result, neighbor)
	}

	return result, nil
}

// Helper functions.

func (g *Graph) filterEdgesByProperties(edges []Edge, properties map[string]any) []Edge {
	if len(properties) == 0 {
		return edges
	}

	var filtered []Edge
	for _, edge := range edges {
		matches := true
		for key, value := range properties {
			if len(edge.Data) == 0 {
				matches = false
				break
			}
			// Unmarshal edge data to check properties.
			var edgeData map[string]any
			if err := json.Unmarshal(edge.Data, &edgeData); err != nil {
				matches = false
				break
			}
			if edgeData[key] != value {
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
