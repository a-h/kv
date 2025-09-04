package main

import (
	"context"
	"fmt"

	"github.com/a-h/kv/graph"
)

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
		if len(edge.Data) > 0 {
			// Add raw JSON data to label.
			label += fmt.Sprintf("\\n%s", string(edge.Data))
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
		if len(edge.Data) > 0 {
			// Add raw JSON data to label (truncated for readability).
			dataStr := string(edge.Data)
			if len(dataStr) > 50 {
				dataStr = dataStr[:47] + "..."
			}
			label += fmt.Sprintf(" %s", dataStr)
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
