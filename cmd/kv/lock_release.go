package main

import (
	"context"
)

type LockReleaseCommand struct {
	Name     string `arg:"" help:"The lock name." required:"true"`
	LockedBy string `arg:"" help:"The owner of the lock." required:"true"`
}

func (c *LockReleaseCommand) Run(ctx context.Context, g GlobalFlags) error {
	store, err := g.Store()
	if err != nil {
		return err
	}
	return store.LockRelease(ctx, c.Name, c.LockedBy)
}
