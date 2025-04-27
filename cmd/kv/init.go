package main

import (
	"context"
	"fmt"
)

type InitCommand struct {
}

func (c *InitCommand) Run(ctx context.Context, g GlobalFlags) error {
	store, err := g.Store()
	if err != nil {
		return fmt.Errorf("failed to create store: %w", err)
	}

	return store.Init(ctx)
}
