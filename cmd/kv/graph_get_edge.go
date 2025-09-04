package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/a-h/kv/graph"
)

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
