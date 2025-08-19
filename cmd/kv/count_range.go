package main

import (
	"context"
	"fmt"
)

type CountRangeCommand struct {
	From string `arg:"" help:"Start of the range." required:""`
	To   string `arg:"" help:"End of the range (exclusive)." required:""`
}

func (c *CountRangeCommand) Run(ctx context.Context, g GlobalFlags) error {
	store, err := g.Store()
	if err != nil {
		return fmt.Errorf("failed to create store: %w", err)
	}

	count, err := store.CountRange(ctx, c.From, c.To)
	if err != nil {
		return fmt.Errorf("failed to get count: %w", err)
	}

	fmt.Println(count)
	return nil
}
