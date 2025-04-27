package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
)

type PutCommand struct {
	Key     string `arg:"" help:"Key name" required:""`
	Version int64  `help:"The version of the key to overwrite, or -1 if no version check is required." default:"-1"`
}

func (c *PutCommand) Run(ctx context.Context, g GlobalFlags) error {
	store, err := g.Store()
	if err != nil {
		return fmt.Errorf("failed to create store: %w", err)
	}

	var data map[string]any
	err = json.NewDecoder(os.Stdin).Decode(&data)
	if err != nil {
		return fmt.Errorf("failed to decode data: %w", err)
	}

	return store.Put(ctx, c.Key, c.Version, data)
}
