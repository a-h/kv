package main

import (
	"context"

	"github.com/a-h/kv"
)

type ConsumerCommitCommand struct {
	Name    string `arg:"" help:"The consumer name." required:"true"`
	LastSeq int    `arg:"" help:"The last sequence number to commit." required:"true"`
}

func (c *ConsumerCommitCommand) Run(ctx context.Context, g GlobalFlags) error {
	store, err := g.Store()
	if err != nil {
		return err
	}
	consumer, err := kv.NewStreamConsumer(ctx, store, c.Name)
	if err != nil {
		return err
	}
	consumer.LastSeq = c.LastSeq
	return consumer.Commit(ctx)
}
