package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/a-h/kv/graph"
)

type GraphAddEdgeCommand struct {
	FromType string `arg:"" help:"Source entity type"`
	FromID   string `arg:"" help:"Source entity ID"`
	EdgeType string `arg:"" help:"Edge type"`
	ToType   string `arg:"" help:"Target entity type"`
	ToID     string `arg:"" help:"Target entity ID"`
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

	edge := graph.NewEdge(
		graph.NewNodeRef(c.ToID, c.ToType),
		graph.NewNodeRef(c.FromID, c.FromType),
		c.EdgeType,
		nil,
	)

	// Read properties from stdin if available.
	stat, err := os.Stdin.Stat()
	if err == nil && (stat.Mode()&os.ModeCharDevice) == 0 {
		// Stdin has data (not a terminal).
		dataBytes, err := io.ReadAll(os.Stdin)
		if err != nil {
			return fmt.Errorf("failed to read edge data from stdin: %w", err)
		}
		if len(dataBytes) > 0 {
			// Validate JSON before storing as RawMessage.
			var temp any
			if err := json.Unmarshal(dataBytes, &temp); err != nil {
				return fmt.Errorf("invalid JSON data from stdin: %w", err)
			}
			edge.Data = json.RawMessage(dataBytes)
		}
	}

	if err := gr.AddEdge(ctx, edge); err != nil {
		return fmt.Errorf("failed to add edge: %w", err)
	}

	fmt.Printf("Added edge: %s/%s -[%s]-> %s/%s\n", c.FromType, c.FromID, c.EdgeType, c.ToType, c.ToID)
	return nil
}
