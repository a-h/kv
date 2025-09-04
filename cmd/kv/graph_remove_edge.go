package main

import (
	"context"
	"fmt"

	"github.com/a-h/kv/graph"
)

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
