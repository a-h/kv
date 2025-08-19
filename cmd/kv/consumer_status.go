package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"

	"github.com/a-h/kv"
)

type ConsumerStatusCommand struct {
	Name string `help:"The consumer name." required:"true"`
}

func (c *ConsumerStatusCommand) Run(ctx context.Context, g GlobalFlags) error {
	store, err := g.Store()
	if err != nil {
		return err
	}
	consumer, err := kv.NewStreamConsumer(ctx, store, c.Name)
	if err != nil {
		return err
	}
	status, ok, err := consumer.Status(ctx)
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("consumer not found")
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(status)
}
