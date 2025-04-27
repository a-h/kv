package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
)

type GetCommand struct {
	Key          string `arg:"" help:"The key to get." required:""`
	PrintVersion bool   `help:"Print the version of the key."`
}

func (c *GetCommand) Run(ctx context.Context, g GlobalFlags) error {
	store, err := g.Store()
	if err != nil {
		return fmt.Errorf("failed to create store: %w", err)
	}

	var data map[string]any
	r, ok, err := store.Get(ctx, c.Key, &data)
	if err != nil {
		return fmt.Errorf("failed to get data: %w", err)
	}
	if !ok {
		return fmt.Errorf("%q not found", c.Key)
	}

	if c.PrintVersion {
		fmt.Printf("%d", r.Version)
		return nil
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(data)
}
