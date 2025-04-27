package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
)

type PatchCommand struct {
	Key     string `arg:"" help:"The key to patch in the KV store." required:""`
	Version int64  `help:"The version of the key to patch, or -1 if no version check is required." default:"-1"`
}

func (c *PatchCommand) Run(ctx context.Context, g GlobalFlags) error {
	store, err := g.Store()
	if err != nil {
		return fmt.Errorf("failed to create store: %w", err)
	}

	var data map[string]any
	err = json.NewDecoder(os.Stdin).Decode(&data)
	if err != nil {
		return fmt.Errorf("failed to decode patch: %w", err)
	}

	return store.Patch(ctx, c.Key, c.Version, data)
}
