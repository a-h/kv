package main

import (
	"context"
	"encoding/json"
	"os"

	"github.com/a-h/kv"
)

type ConsumerGetCommand struct {
	Name  string `help:"The consumer name." required:"true"`
	Limit int    `help:"The maximum number of records to fetch." default:"10"`
}

func (c *ConsumerGetCommand) Run(ctx context.Context, g GlobalFlags) error {
	store, err := g.Store()
	if err != nil {
		return err
	}
	consumer, err := kv.NewStreamConsumer(ctx, store, c.Name)
	if err != nil {
		return err
	}
	if c.Limit > 0 {
		consumer.Limit = c.Limit
	}
	records, err := consumer.Get(ctx)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(records)
}
