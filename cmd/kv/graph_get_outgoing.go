package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/a-h/kv/graph"
)

type GraphGetOutgoingCommand struct {
	EntityType string `arg:"" help:"Entity type"`
	EntityID   string `arg:"" help:"Entity ID"`
	EdgeType   string `arg:"" help:"Edge type" default:"*"`
}

func (c *GraphGetOutgoingCommand) Run(ctx context.Context, g GlobalFlags) error {
	store, err := g.Store()
	if err != nil {
		return fmt.Errorf("failed to create store: %w", err)
	}

	gr := graph.New(store)

	node := graph.NewNodeRef(c.EntityID, c.EntityType)

	// Get the appropriate edge iterator based on edge type.
	edgeIterator := gr.GetAllOutgoing(ctx, node)
	if c.EdgeType != "*" {
		edgeIterator = gr.GetOutgoing(ctx, node, c.EdgeType)
	}

	var edgeCount int
	for edge, err := range edgeIterator {
		if err != nil {
			return fmt.Errorf("failed to get outgoing edges: %w", err)
		}

		output, err := json.MarshalIndent(edge, "", "  ")
		if err != nil {
			return fmt.Errorf("failed to marshal edge: %w", err)
		}
		fmt.Println(string(output))
		edgeCount++
	}

	if edgeCount == 0 {
		return fmt.Errorf("no outgoing edges found")
	}

	return nil
}
