package main

import (
	"context"
	"fmt"
)

type DeleteRangeCommand struct {
	From   string `arg:"" help:"Start of the range." required:""`
	To     string `arg:"" help:"End of the range (exclusive)." required:""`
	Offset int    `arg:"-o,--offset" help:"Range offset." default:"0"`
	Limit  int    `arg:"-l,--limit" help:"The maximum number of records to return, or -1 for no limit." default:"1000"`
}

func (c *DeleteRangeCommand) Run(ctx context.Context, g GlobalFlags) error {
	store, err := g.Store()
	if err != nil {
		return fmt.Errorf("failed to create store: %w", err)
	}

	deleted, err := store.DeleteRange(ctx, c.From, c.To, c.Offset, c.Limit)
	if err != nil {
		return err
	}

	fmt.Printf("Deleted %d records\n", deleted)
	return nil
}
