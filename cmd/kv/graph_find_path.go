package main

import (
	"context"
	"fmt"

	"github.com/a-h/kv/graph"
)

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
		fmt.Print(node.Key())
	}
	fmt.Println()

	return nil
}
