package main

import (
	"context"
	"fmt"
)

type DeleteCommand struct {
	Key string `arg:"" help:"The key to delete from the KV store." required:""`
}

func (c *DeleteCommand) Run(ctx context.Context, g GlobalFlags) error {
	store, err := g.Store()
	if err != nil {
		return fmt.Errorf("failed to create store: %w", err)
	}

	_, err = store.Delete(ctx, c.Key)
	return err
}
