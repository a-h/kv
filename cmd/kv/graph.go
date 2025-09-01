package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/a-h/kv/graph"
)

type GraphCommand struct {
	AddEdge     GraphAddEdgeCommand     `cmd:"add-edge" help:"Add an edge between two entities"`
	GetEdge     GraphGetEdgeCommand     `cmd:"get-edge" help:"Get a specific edge"`
	GetOutgoing GraphGetOutgoingCommand `cmd:"get-outgoing" help:"Get outgoing edges from an entity"`
	GetIncoming GraphGetIncomingCommand `cmd:"get-incoming" help:"Get incoming edges to an entity"`
	RemoveEdge  GraphRemoveEdgeCommand  `cmd:"remove-edge" help:"Remove an edge between two entities"`
	FindPath    GraphFindPathCommand    `cmd:"find-path" help:"Find shortest path between two entities"`
	View        GraphViewCommand        `cmd:"view" help:"Generate graph visualization output"`
}

type GraphAddEdgeCommand struct {
	FromType   string `arg:"" help:"Source entity type"`
	FromID     string `arg:"" help:"Source entity ID"`
	EdgeType   string `arg:"" help:"Edge type"`
	ToType     string `arg:"" help:"Target entity type"`
	ToID       string `arg:"" help:"Target entity ID"`
	Properties string `help:"Edge properties as JSON" default:""`
}

func (c *GraphAddEdgeCommand) Run(ctx context.Context, g GlobalFlags) error {
	if c.FromType == "" || c.FromID == "" || c.EdgeType == "" || c.ToType == "" || c.ToID == "" {
		return fmt.Errorf("all edge components are required")
	}

	store, err := g.Store()
	if err != nil {
		return fmt.Errorf("failed to create store: %w", err)
	}

	gr := graph.New(store)

	edge := graph.Edge{
		FromEntityType: c.FromType,
		FromEntityID:   c.FromID,
		ToEntityType:   c.ToType,
		ToEntityID:     c.ToID,
		Type:           c.EdgeType,
	}

	if c.Properties != "" {
		var properties map[string]any
		if err := json.Unmarshal([]byte(c.Properties), &properties); err != nil {
			return fmt.Errorf("invalid properties JSON: %w", err)
		}
		edge.Properties = properties
	}

	if err := gr.AddEdge(ctx, edge); err != nil {
		return fmt.Errorf("failed to add edge: %w", err)
	}

	fmt.Printf("Added edge: %s/%s -[%s]-> %s/%s\n", c.FromType, c.FromID, c.EdgeType, c.ToType, c.ToID)
	return nil
}

type GraphGetEdgeCommand struct {
	FromType string `arg:"" help:"Source entity type"`
	FromID   string `arg:"" help:"Source entity ID"`
	EdgeType string `arg:"" help:"Edge type"`
	ToType   string `arg:"" help:"Target entity type"`
	ToID     string `arg:"" help:"Target entity ID"`
}

func (c *GraphGetEdgeCommand) Run(ctx context.Context, g GlobalFlags) error {
	store, err := g.Store()
	if err != nil {
		return fmt.Errorf("failed to create store: %w", err)
	}

	gr := graph.New(store)

	edge, exists, err := gr.GetEdge(ctx, c.FromType, c.FromID, c.EdgeType, c.ToType, c.ToID)
	if err != nil {
		return fmt.Errorf("failed to get edge: %w", err)
	}

	if !exists {
		return fmt.Errorf("edge not found")
	}

	output, err := json.MarshalIndent(edge, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal edge: %w", err)
	}

	fmt.Println(string(output))
	return nil
}

type GraphGetOutgoingCommand struct {
	EntityType string `arg:"" help:"Entity type"`
	EntityID   string `arg:"" help:"Entity ID"`
	EdgeType   string `arg:"" help:"Edge type (or '*' for all types)"`
}

func (c *GraphGetOutgoingCommand) Run(ctx context.Context, g GlobalFlags) error {
	store, err := g.Store()
	if err != nil {
		return fmt.Errorf("failed to create store: %w", err)
	}

	gr := graph.New(store)

	var edges []graph.Edge
	if c.EdgeType == "*" {
		for edge, err := range gr.GetAllOutgoing(ctx, c.EntityType, c.EntityID) {
			if err != nil {
				return fmt.Errorf("failed to get outgoing edges: %w", err)
			}
			edges = append(edges, edge)
		}
	} else {
		for edge, err := range gr.GetOutgoing(ctx, c.EntityType, c.EntityID, c.EdgeType) {
			if err != nil {
				return fmt.Errorf("failed to get outgoing edges: %w", err)
			}
			edges = append(edges, edge)
		}
	}

	if len(edges) == 0 {
		return fmt.Errorf("no outgoing edges found")
	}

	for _, edge := range edges {
		output, err := json.MarshalIndent(edge, "", "  ")
		if err != nil {
			return fmt.Errorf("failed to marshal edge: %w", err)
		}
		fmt.Println(string(output))
	}

	return nil
}

type GraphGetIncomingCommand struct {
	EntityType string `arg:"" help:"Entity type"`
	EntityID   string `arg:"" help:"Entity ID"`
	EdgeType   string `arg:"" help:"Edge type (or '*' for all types)"`
}

func (c *GraphGetIncomingCommand) Run(ctx context.Context, g GlobalFlags) error {
	store, err := g.Store()
	if err != nil {
		return fmt.Errorf("failed to create store: %w", err)
	}

	gr := graph.New(store)

	var edges []graph.Edge
	if c.EdgeType == "*" {
		for edge, err := range gr.GetAllIncoming(ctx, c.EntityType, c.EntityID) {
			if err != nil {
				return fmt.Errorf("failed to get incoming edges: %w", err)
			}
			edges = append(edges, edge)
		}
	} else {
		for edge, err := range gr.GetIncoming(ctx, c.EntityType, c.EntityID, c.EdgeType) {
			if err != nil {
				return fmt.Errorf("failed to get incoming edges: %w", err)
			}
			edges = append(edges, edge)
		}
	}

	if len(edges) == 0 {
		return fmt.Errorf("no incoming edges found")
	}

	for _, edge := range edges {
		output, err := json.MarshalIndent(edge, "", "  ")
		if err != nil {
			return fmt.Errorf("failed to marshal edge: %w", err)
		}
		fmt.Println(string(output))
	}

	return nil
}

type GraphRemoveEdgeCommand struct {
	FromType string `arg:"" help:"Source entity type"`
	FromID   string `arg:"" help:"Source entity ID"`
	EdgeType string `arg:"" help:"Edge type"`
	ToType   string `arg:"" help:"Target entity type"`
	ToID     string `arg:"" help:"Target entity ID"`
}

func (c *GraphRemoveEdgeCommand) Run(ctx context.Context, g GlobalFlags) error {
	store, err := g.Store()
	if err != nil {
		return fmt.Errorf("failed to create store: %w", err)
	}

	gr := graph.New(store)

	if err := gr.RemoveEdge(ctx, c.FromType, c.FromID, c.EdgeType, c.ToType, c.ToID); err != nil {
		return fmt.Errorf("failed to remove edge: %w", err)
	}

	fmt.Printf("Removed edge: %s/%s -[%s]-> %s/%s\n", c.FromType, c.FromID, c.EdgeType, c.ToType, c.ToID)
	return nil
}

type GraphFindPathCommand struct {
	FromType  string   `arg:"" help:"Source entity type"`
	FromID    string   `arg:"" help:"Source entity ID"`
	ToType    string   `arg:"" help:"Target entity type"`
	ToID      string   `arg:"" help:"Target entity ID"`
	MaxDepth  int      `help:"Maximum search depth (0 = unlimited)" default:"0"`
	EdgeTypes []string `help:"Edge types to traverse"`
}

func (c *GraphFindPathCommand) Run(ctx context.Context, g GlobalFlags) error {
	store, err := g.Store()
	if err != nil {
		return fmt.Errorf("failed to create store: %w", err)
	}

	gr := graph.New(store)

	opts := graph.TraversalOptions{}
	if c.MaxDepth > 0 {
		opts.MaxDepth = c.MaxDepth
	}
	if len(c.EdgeTypes) > 0 {
		opts.EdgeTypes = c.EdgeTypes
	}

	path, err := gr.FindShortestPath(ctx, c.FromType, c.FromID, c.ToType, c.ToID, opts)
	if err != nil {
		return fmt.Errorf("failed to find path: %w", err)
	}

	if path == nil {
		return fmt.Errorf("no path found")
	}

	fmt.Printf("Shortest path (depth %d):\n", path.Depth)
	for i, node := range path.Nodes {
		if i > 0 {
			edge := path.Edges[i-1]
			fmt.Printf(" -[%s]-> ", edge.Type)
		}
		fmt.Printf("%s/%s", node.EntityType, node.EntityID)
	}
	fmt.Println()

	return nil
}

type GraphViewCommand struct {
	EntityType string `arg:"" help:"Entity type to visualize (or '*' for all types)"`
	EntityID   string `arg:"" help:"Entity ID to visualize (or '*' for all entities of the type)" optional:""`
	Format     string `help:"Output format: dot, mermaid" default:"dot"`
	MaxDepth   int    `help:"Maximum depth for traversal (0 for unlimited)" default:"2"`
	EdgeType   string `help:"Edge type filter (or '*' for all types)" default:"*"`
}

func (c *GraphViewCommand) Run(ctx context.Context, g GlobalFlags) error {
	store, err := g.Store()
	if err != nil {
		return fmt.Errorf("failed to create store: %w", err)
	}

	gr := graph.New(store)

	switch c.Format {
	case "dot":
		return c.outputDot(ctx, gr)
	case "mermaid":
		return c.outputMermaid(ctx, gr)
	default:
		return fmt.Errorf("unsupported format: %s (supported: dot, mermaid)", c.Format)
	}
}

func (c *GraphViewCommand) outputDot(ctx context.Context, gr *graph.Graph) error {
	fmt.Println("digraph G {")
	fmt.Println("  rankdir=LR;")
	fmt.Println("  node [shape=box, style=rounded];")
	fmt.Println()

	edges, err := c.collectEdges(ctx, gr)
	if err != nil {
		return err
	}

	// Collect unique nodes.
	nodes := make(map[string]bool)
	for _, edge := range edges {
		fromNode := fmt.Sprintf("%s_%s", edge.FromEntityType, edge.FromEntityID)
		toNode := fmt.Sprintf("%s_%s", edge.ToEntityType, edge.ToEntityID)
		nodes[fromNode] = true
		nodes[toNode] = true
	}

	// Output node definitions.
	for node := range nodes {
		fmt.Printf("  \"%s\";\n", node)
	}
	fmt.Println()

	// Output edges.
	for _, edge := range edges {
		fromNode := fmt.Sprintf("%s_%s", edge.FromEntityType, edge.FromEntityID)
		toNode := fmt.Sprintf("%s_%s", edge.ToEntityType, edge.ToEntityID)

		label := edge.Type
		if len(edge.Properties) > 0 {
			// Add properties to label.
			propStr := ""
			for k, v := range edge.Properties {
				if propStr != "" {
					propStr += ", "
				}
				propStr += fmt.Sprintf("%s=%v", k, v)
			}
			label += fmt.Sprintf("\\n{%s}", propStr)
		}

		fmt.Printf("  \"%s\" -> \"%s\" [label=\"%s\"];\n", fromNode, toNode, label)
	}

	fmt.Println("}")
	return nil
}

func (c *GraphViewCommand) outputMermaid(ctx context.Context, gr *graph.Graph) error {
	fmt.Println("graph LR")

	edges, err := c.collectEdges(ctx, gr)
	if err != nil {
		return err
	}

	// Output edges in Mermaid format.
	for _, edge := range edges {
		fromNode := fmt.Sprintf("%s_%s", edge.FromEntityType, edge.FromEntityID)
		toNode := fmt.Sprintf("%s_%s", edge.ToEntityType, edge.ToEntityID)

		label := edge.Type
		if len(edge.Properties) > 0 {
			// Add a subset of properties to keep it readable.
			for k, v := range edge.Properties {
				label += fmt.Sprintf(" %s=%v", k, v)
				break // Only show first property to keep it clean.
			}
		}

		fmt.Printf("  %s -->|%s| %s\n", fromNode, label, toNode)
	}

	return nil
}

func (c *GraphViewCommand) collectEdges(ctx context.Context, gr *graph.Graph) ([]graph.Edge, error) {
	var allEdges []graph.Edge
	const maxEdges = 10000 // Prevent memory issues

	if c.EntityType == "*" {
		// Get all edges in the graph by scanning all graph keys.
		var edgeCount int
		for edge, err := range gr.All(ctx) {
			if err != nil {
				return nil, fmt.Errorf("failed to list all edges: %w", err)
			}
			if edgeCount >= maxEdges {
				return nil, fmt.Errorf("too many edges (%d+) for visualization - please filter by entity type", maxEdges)
			}
			allEdges = append(allEdges, edge)
			edgeCount++
		}

		// Apply edge type filter if specified.
		if c.EdgeType != "*" {
			var filtered []graph.Edge
			for _, edge := range allEdges {
				if edge.Type == c.EdgeType {
					filtered = append(filtered, edge)
				}
			}
			allEdges = filtered
		}
	} else {
		if c.EntityID == "" || c.EntityID == "*" {
			// Get all entities of the specified type by scanning the store.
			// This is a limitation - we'd need to maintain an entity index for this to work efficiently.
			return nil, fmt.Errorf("getting all entities of a type is not yet supported - please specify both entity type and ID, or use '*' for both")
		} else {
			// Get edges for specific entity.
			for edge, err := range gr.GetOutgoing(ctx, c.EntityType, c.EntityID, c.EdgeType) {
				if err != nil {
					return nil, fmt.Errorf("failed to get outgoing edges: %w", err)
				}
				allEdges = append(allEdges, edge)
			}

			// For visualization, also include incoming edges to show the full picture.
			for edge, err := range gr.GetIncoming(ctx, c.EntityType, c.EntityID, c.EdgeType) {
				if err != nil {
					return nil, fmt.Errorf("failed to get incoming edges: %w", err)
				}
				allEdges = append(allEdges, edge)
			}
		}
	}

	// Apply depth filtering if needed.
	if c.MaxDepth > 0 {
		allEdges = c.filterByDepth(allEdges, c.EntityType, c.EntityID, c.MaxDepth)
	}

	return allEdges, nil
}

func (c *GraphViewCommand) filterByDepth(edges []graph.Edge, rootType, rootID string, maxDepth int) []graph.Edge {
	if rootType == "*" || rootID == "*" || rootID == "" {
		// Can't filter by depth without a specific starting node.
		return edges
	}

	visited := make(map[string]int)
	var filtered []graph.Edge

	// BFS to find edges within the depth limit.
	queue := []struct {
		nodeType string
		nodeID   string
		depth    int
	}{{rootType, rootID, 0}}
	visited[fmt.Sprintf("%s/%s", rootType, rootID)] = 0

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		if current.depth >= maxDepth {
			continue
		}

		// Find edges from this node.
		for _, edge := range edges {
			if edge.FromEntityType == current.nodeType && edge.FromEntityID == current.nodeID {
				filtered = append(filtered, edge)

				// Add target to queue if not visited or found at greater depth.
				targetKey := fmt.Sprintf("%s/%s", edge.ToEntityType, edge.ToEntityID)
				if prevDepth, ok := visited[targetKey]; !ok || prevDepth > current.depth+1 {
					visited[targetKey] = current.depth + 1
					queue = append(queue, struct {
						nodeType string
						nodeID   string
						depth    int
					}{edge.ToEntityType, edge.ToEntityID, current.depth + 1})
				}
			}
		}
	}

	return filtered
}
