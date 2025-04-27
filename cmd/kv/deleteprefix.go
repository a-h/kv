package main

import (
	"context"
	"fmt"
)

type DeletePrefixCommand struct {
	Prefix string `arg:"" help:"The prefix to delete." required:""`
	Offset int    `arg:"-o,--offset" help:"The offset to start deleting from." default:"0"`
	Limit  int    `arg:"-l,--limit" help:"Maximum number of records to delete, or -1 for all." default:"1000"`
}

func (c *DeletePrefixCommand) Run(ctx context.Context, g GlobalFlags) error {
	store, err := g.Store()
	if err != nil {
		return fmt.Errorf("failed to create store: %w", err)
	}

	deleted, err := store.DeletePrefix(ctx, c.Prefix, c.Offset, c.Limit)
	if err != nil {
		return err
	}

	fmt.Printf("Deleted %d records (limit of %d)\n", deleted, c.Limit)
	return nil
}
