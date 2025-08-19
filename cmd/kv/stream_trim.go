package main

import (
	"context"
)

type StreamTrimCommand struct {
	Seq int `arg:"" help:"Trim the stream to this sequence number." required:"true"`
}

func (c *StreamTrimCommand) Run(ctx context.Context, g GlobalFlags) error {
	store, err := g.Store()
	if err != nil {
		return err
	}
	if err := store.StreamTrim(ctx, c.Seq); err != nil {
		return err
	}
	return nil
}
