package main

import (
	"context"
	"fmt"
)

type StreamTrimCommand struct {
	Seq int `help:"Trim the stream to this sequence number." required:"true"`
}

func (c *StreamTrimCommand) Run(ctx context.Context, g GlobalFlags) error {
	store, err := g.Store()
	if err != nil {
		return err
	}
	if err := store.StreamTrim(ctx, c.Seq); err != nil {
		return err
	}
	fmt.Println("stream trimmed")
	return nil
}
