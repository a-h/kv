package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
)

type LockStatusCommand struct {
	Name string `arg:"" help:"The lock name." required:"true"`
}

func (c *LockStatusCommand) Run(ctx context.Context, g GlobalFlags) error {
	store, err := g.Store()
	if err != nil {
		return err
	}
	status, ok, err := store.LockStatus(ctx, c.Name)
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("lock not found")
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(status)
}
