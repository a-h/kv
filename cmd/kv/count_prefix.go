package main

import (
	"context"
	"fmt"
)

type CountPrefixCommand struct {
	Prefix string `arg:"" help:"The prefix to count." required:""`
}

func (c *CountPrefixCommand) Run(ctx context.Context, g GlobalFlags) error {
	store, err := g.Store()
	if err != nil {
		return fmt.Errorf("failed to create store: %w", err)
	}

	count, err := store.CountPrefix(ctx, c.Prefix)
	if err != nil {
		return fmt.Errorf("failed to get count: %w", err)
	}

	fmt.Println(count)
	return nil
}
