package main

import (
	"context"

	"github.com/a-h/kv"
)

type ConsumerDeleteCommand struct {
	Name string `arg:"" help:"The consumer name to delete." required:"true"`
}

func (c *ConsumerDeleteCommand) Run(ctx context.Context, g GlobalFlags) error {
	store, err := g.Store()
	if err != nil {
		return err
	}
	if err := kv.DeleteStreamConsumer(ctx, store, c.Name); err != nil {
		return err
	}
	return nil
}
