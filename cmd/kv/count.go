package main

import (
	"context"
	"fmt"
)

type CountCommand struct {
}

func (c *CountCommand) Run(ctx context.Context, g GlobalFlags) error {
	store, err := g.Store()
	if err != nil {
		return fmt.Errorf("failed to create store: %w", err)
	}

	count, err := store.Count(ctx)
	if err != nil {
		return fmt.Errorf("failed to get count: %w", err)
	}

	fmt.Println(count)
	return nil
}
