package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/a-h/kv/graph"
)

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

	node := graph.NewNodeRef(c.EntityType, c.EntityID)

	var edges []graph.Edge
	if c.EdgeType == "*" {
		for edge, err := range gr.GetAllIncoming(ctx, node) {
			if err != nil {
				return fmt.Errorf("failed to get incoming edges: %w", err)
			}
			edges = append(edges, edge)
		}
	} else {
		for edge, err := range gr.GetIncoming(ctx, node, c.EdgeType) {
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
