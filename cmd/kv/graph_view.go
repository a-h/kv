package main

import (
	"context"
	"fmt"
	"iter"
	"maps"
	"slices"

	"github.com/a-h/kv/graph"
)

type GraphViewCommand struct {
	EntityType string `arg:"" help:"Entity type to visualize (or '*' for all types)"`
	EntityID   string `arg:"" help:"Entity ID to visualize (or '*' for all entities of the type)" optional:""`
	Format     string `help:"Output format: dot, mermaid" default:"dot"`
	MaxDepth   int    `help:"Maximum depth for traversal (0 for unlimited)" default:"2"`
	EdgeType   string `help:"Edge type filter" default:"*"`
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

	// Get the edge iterator based on entity type.
	edgeIterator, err := c.getEdgeIterator(ctx, gr)
	if err != nil {
		return err
	}

	// Track unique nodes as we stream edges.
	nodes := make(map[string]bool)
	const maxEdges = 10000 // Prevent memory issues
	var edgeCount int

	// First pass: collect nodes.
	for edge, err := range edgeIterator() {
		if err != nil {
			return fmt.Errorf("failed to iterate edges: %w", err)
		}
		if edgeCount >= maxEdges {
			return fmt.Errorf("too many edges (%d+) for visualization - please filter by entity type", maxEdges)
		}

		// Apply edge type filter if specified (only for "all" mode).
		if c.EntityType == "*" && c.EdgeType != "*" && edge.Type != c.EdgeType {
			continue
		}

		nodes[edge.From.Key()] = true
		nodes[edge.To.Key()] = true
		edgeCount++
	}

	// Output node definitions.
	nodeKeys := slices.Collect(maps.Keys(nodes))
	slices.Sort(nodeKeys)
	for _, node := range nodeKeys {
		fmt.Printf("  \"%s\";\n", node)
	}
	fmt.Println()

	// Second pass: output edges.
	for edge, err := range edgeIterator() {
		if err != nil {
			return fmt.Errorf("failed to iterate edges: %w", err)
		}

		// Apply edge type filter if specified (only for "all" mode).
		if c.EntityType == "*" && c.EdgeType != "*" && edge.Type != c.EdgeType {
			continue
		}

		fromNode := edge.From.Key()
		toNode := edge.To.Key()

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

	// Get the edge iterator based on entity type.
	edgeIterator, err := c.getEdgeIterator(ctx, gr)
	if err != nil {
		return err
	}

	const maxEdges = 10000 // Prevent memory issues
	var edgeCount int

	// Stream and output edges directly.
	for edge, err := range edgeIterator() {
		if err != nil {
			return fmt.Errorf("failed to iterate edges: %w", err)
		}
		if edgeCount >= maxEdges {
			return fmt.Errorf("too many edges (%d+) for visualization - please filter by entity type", maxEdges)
		}

		// Apply edge type filter if specified (only for "all" mode).
		if c.EntityType == "*" && c.EdgeType != "*" && edge.Type != c.EdgeType {
			continue
		}

		fromNode := edge.From.Key()
		toNode := edge.To.Key()

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
		edgeCount++
	}

	return nil
}

func (c *GraphViewCommand) getEdgeIterator(ctx context.Context, gr *graph.Graph) (func() iter.Seq2[graph.Edge, error], error) {
	if c.EntityType == "*" {
		return func() iter.Seq2[graph.Edge, error] {
			return gr.All(ctx)
		}, nil
	}

	if c.EntityID == "" || c.EntityID == "*" {
		return nil, fmt.Errorf("getting all entities of a type is not yet supported - please specify both entity type and ID, or use '*' for both")
	}

	node := graph.NewNodeRef(c.EntityType, c.EntityID)
	return func() iter.Seq2[graph.Edge, error] {
		return func(yield func(graph.Edge, error) bool) {
			// Stream outgoing edges.
			for edge, err := range gr.GetOutgoing(ctx, node, c.EdgeType) {
				if err != nil {
					yield(graph.Edge{}, err)
					return
				}
				if !yield(edge, nil) {
					return
				}
			}

			// Stream incoming edges for visualization.
			for edge, err := range gr.GetIncoming(ctx, node, c.EdgeType) {
				if err != nil {
					yield(graph.Edge{}, err)
					return
				}
				if !yield(edge, nil) {
					return
				}
			}
		}
	}, nil
}
